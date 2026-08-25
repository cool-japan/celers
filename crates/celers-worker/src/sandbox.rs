//! Task execution sandboxing for isolation and security
//!
//! # What this module actually enforces
//!
//! Sandboxing in a library that runs tasks as **in-process async futures** is
//! necessarily limited, and this module is deliberate about the boundary
//! between what it *enforces* and what it merely *records*.  Nothing here
//! pretends to contain hostile native code.
//!
//! | Control | Status |
//! |---|---|
//! | Execution timeout | **Enforced** — [`Sandbox::execute`] drives the future under [`tokio::time::timeout`] and drops it on expiry |
//! | Filesystem allowlist | **Enforced** for callers that route their path access through [`Sandbox::check_path`] / [`Sandbox::is_path_allowed`] (symlink- and `..`-resistant, see [`Sandbox::is_path_allowed`]) |
//! | Read-only filesystem | **Enforced** at the same choke point ([`Sandbox::check_path`] with `write = true`) |
//! | Network policy | **Advisory** — [`Sandbox::check_network`] is a gate the caller must consult; no syscall filtering |
//! | Environment scrubbing | **Enforced** by [`Sandbox::sanitize_env`] for callers that build a task environment through it |
//! | Address space / file descriptor / CPU-time limits | **Enforced process-wide** via `setrlimit` from [`Sandbox::enforce_process_limits`], and only when the crate's off-by-default `rlimit` feature is enabled on a Unix target |
//! | `max_cpu_percent` | **Advisory only** — a scheduling share is not expressible as an rlimit; [`Sandbox::validate_resources`] records violations reported by the caller |
//! | seccomp-bpf syscall filtering | **Enforced process-wide** as a deny-list of syscalls a worker never makes, installed by [`Sandbox::enforce_process_limits`] on Linux with the crate's off-by-default `seccomp` feature at [`IsolationLevel::Process`]. In any other configuration requesting it is a hard error — never a silent no-op |
//! | [`IsolationLevel::Container`] / [`IsolationLevel::Full`] | **Not implemented** — requesting them is a hard error |
//!
//! Because unimplemented containment is worse than no containment, a
//! [`Sandbox`] refuses to be constructed at all when the configuration asks
//! for something this build cannot deliver: [`Sandbox::new`] returns
//! [`SandboxError::Unsupported`] instead of handing back an object that looks
//! like a jail and is not one.  Use [`Sandbox::enforcement`] to inspect, at
//! runtime, exactly which controls are live.
//!
//! # Example
//!
//! ```
//! use celers_worker::sandbox::{Sandbox, SandboxConfig};
//!
//! let config = SandboxConfig::default()
//!     .with_max_memory_mb(512)
//!     .with_max_cpu_percent(80)
//!     .with_timeout_secs(300);
//!
//! let sandbox = Sandbox::new(config).expect("Basic isolation is always supported");
//! assert!(sandbox.enforcement().timeout);
//! ```

mod config;
mod error;
mod stats;

#[cfg(all(unix, feature = "rlimit"))]
mod rlimit_impl;
#[cfg(all(target_os = "linux", feature = "seccomp"))]
mod seccomp_impl;

#[cfg(test)]
mod tests;

pub use config::{IsolationLevel, SandboxConfig};
pub use error::{SandboxError, SandboxViolation};
pub use stats::SandboxStats;

use std::fmt;
use std::path::{Component, Path, PathBuf};

/// A runtime report of which sandbox controls are actually active.
///
/// Returned by [`Sandbox::enforcement`] so that callers (and operators
/// reading logs) can tell enforcement from bookkeeping without reading this
/// module's source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnforcementReport {
    /// Execution timeout is enforced by [`Sandbox::execute`].
    pub timeout: bool,
    /// A non-empty filesystem allowlist is in effect.
    pub path_allowlist: bool,
    /// Filesystem writes are refused by [`Sandbox::check_path`].
    pub read_only_filesystem: bool,
    /// Network access is refused by [`Sandbox::check_network`] (advisory:
    /// only callers that consult the gate are constrained).
    pub network_gate: bool,
    /// [`Sandbox::enforce_process_limits`] can apply real OS limits here.
    pub os_resource_limits: bool,
    /// Syscall filtering is active. Always `false` in this build.
    pub syscall_filter: bool,
}

impl fmt::Display for EnforcementReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EnforcementReport(timeout={}, path_allowlist={}, read_only_fs={}, network_gate={}, os_limits={}, syscall_filter={})",
            self.timeout,
            self.path_allowlist,
            self.read_only_filesystem,
            self.network_gate,
            self.os_resource_limits,
            self.syscall_filter
        )
    }
}

/// Environment variable name fragments that mark a value as credential-like.
///
/// Matching is case-insensitive and substring-based, which deliberately errs
/// towards dropping too much rather than leaking a secret into task code.
const SENSITIVE_ENV_FRAGMENTS: &[&str] = &[
    "SECRET",
    "TOKEN",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "PRIVATE_KEY",
    "API_KEY",
    "ACCESS_KEY",
    "SESSION_KEY",
    "AUTH",
];

/// Sandbox for executing tasks with the isolation described in the module
/// documentation.
pub struct Sandbox {
    config: SandboxConfig,
    /// Allowlist entries pre-normalised once at construction so that every
    /// check is a component-wise comparison against a canonical path.
    allowed_roots: Vec<PathBuf>,
    stats: tokio::sync::RwLock<SandboxStats>,
}

impl Sandbox {
    /// Create a new sandbox.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::InvalidConfig`] for a self-inconsistent
    /// configuration, and [`SandboxError::Unsupported`] when the
    /// configuration requests containment this build cannot enforce
    /// ([`IsolationLevel::Container`], [`IsolationLevel::Full`], seccomp
    /// filtering, or [`IsolationLevel::Process`] off Unix).  Failing loudly
    /// is intentional: a sandbox that silently drops a requested control is
    /// more dangerous than no sandbox at all.
    pub fn new(config: SandboxConfig) -> Result<Self, SandboxError> {
        Self::check_supported(&config)?;

        let allowed_roots = config
            .allowed_paths()
            .iter()
            .filter_map(|p| normalize_path(Path::new(p)))
            .collect();

        Ok(Self {
            config,
            allowed_roots,
            stats: tokio::sync::RwLock::new(SandboxStats::default()),
        })
    }

    /// Check whether a configuration can be enforced on this build without
    /// constructing a sandbox.
    ///
    /// # Errors
    ///
    /// Same conditions as [`Sandbox::new`].
    pub fn check_supported(config: &SandboxConfig) -> Result<(), SandboxError> {
        if !config.is_valid() {
            return Err(SandboxError::InvalidConfig(
                "max_cpu_percent must be within 1..=100 and max_memory_mb must be non-zero"
                    .to_string(),
            ));
        }

        if config.is_seccomp_enabled() {
            if !cfg!(all(target_os = "linux", feature = "seccomp")) {
                return Err(SandboxError::Unsupported {
                    control: "seccomp".to_string(),
                    reason: "seccomp-bpf syscall filtering is not compiled into this build: it \
                             requires a Linux target and the celers-worker `seccomp` feature, \
                             which is off by default. Rebuild with `--features seccomp` on \
                             Linux, or disable it with SandboxConfig::with_seccomp(false)"
                        .to_string(),
                });
            }
            // The filter is installed by `enforce_process_limits`, which only
            // runs at `IsolationLevel::Process`. Accepting the configuration
            // at any other level would hand back a sandbox that reports
            // success while never installing the filter — exactly the
            // silently-dropped control this module refuses to produce.
            if config.isolation_level() != IsolationLevel::Process {
                return Err(SandboxError::Unsupported {
                    control: "seccomp".to_string(),
                    reason: format!(
                        "seccomp filtering is installed by Sandbox::enforce_process_limits, \
                         which requires IsolationLevel::Process (configured: {}). Set \
                         .with_isolation_level(IsolationLevel::Process), or disable seccomp \
                         with .with_seccomp(false)",
                        config.isolation_level()
                    ),
                });
            }
        }

        match config.isolation_level() {
            IsolationLevel::None | IsolationLevel::Basic => Ok(()),
            IsolationLevel::Process => {
                if cfg!(unix) {
                    Ok(())
                } else {
                    Err(SandboxError::Unsupported {
                        control: "IsolationLevel::Process".to_string(),
                        reason: "process resource limits require a Unix target".to_string(),
                    })
                }
            }
            IsolationLevel::Container => Err(SandboxError::Unsupported {
                control: "IsolationLevel::Container".to_string(),
                reason: "namespace isolation requires spawning tasks in a child process, \
                         which this in-process executor does not do"
                    .to_string(),
            }),
            IsolationLevel::Full => Err(SandboxError::Unsupported {
                control: "IsolationLevel::Full".to_string(),
                reason: "VM-level isolation is not implemented".to_string(),
            }),
        }
    }

    /// Report which controls are actually active for this sandbox.
    pub fn enforcement(&self) -> EnforcementReport {
        EnforcementReport {
            timeout: self.config.timeout().is_some(),
            path_allowlist: !self.allowed_roots.is_empty(),
            read_only_filesystem: !self.config.is_fs_write_allowed(),
            network_gate: !self.config.is_network_allowed(),
            os_resource_limits: cfg!(all(unix, feature = "rlimit"))
                && self.config.isolation_level() == IsolationLevel::Process,
            // Like `os_resource_limits`, this reports that the control is
            // both requested and compilable in this build; it is actually
            // installed by `enforce_process_limits`.
            syscall_filter: cfg!(all(target_os = "linux", feature = "seccomp"))
                && self.config.is_seccomp_enabled()
                && self.config.isolation_level() == IsolationLevel::Process,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &SandboxConfig {
        &self.config
    }

    /// Get statistics
    pub async fn stats(&self) -> SandboxStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        self.stats.write().await.reset();
    }

    /// Run a future under the configured execution timeout.
    ///
    /// This is the module's one *real* runtime control: the future is polled
    /// under [`tokio::time::timeout`] and dropped (cancelled at its next
    /// suspension point) when the budget expires.  A CPU-bound future that
    /// never yields cannot be interrupted this way — run such work on
    /// [`tokio::task::spawn_blocking`] and give the sandbox the join handle's
    /// future instead.
    ///
    /// Execution statistics (count, average duration, timeout violations) are
    /// updated automatically.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxViolation::Timeout`] if the future did not complete
    /// within [`SandboxConfig::timeout`].
    pub async fn execute<F>(&self, fut: F) -> Result<F::Output, SandboxViolation>
    where
        F: std::future::Future,
    {
        let started = std::time::Instant::now();
        match self.config.timeout() {
            Some(limit) => match tokio::time::timeout(limit, fut).await {
                Ok(output) => {
                    let elapsed = started.elapsed();
                    self.record_execution(true, elapsed.as_millis() as u64, 0)
                        .await;
                    Ok(output)
                }
                Err(_) => {
                    let elapsed = started.elapsed();
                    self.record_execution(false, elapsed.as_millis() as u64, 0)
                        .await;
                    self.record_timeout_violation().await;
                    Err(SandboxViolation::Timeout { elapsed, limit })
                }
            },
            None => {
                let output = fut.await;
                self.record_execution(true, started.elapsed().as_millis() as u64, 0)
                    .await;
                Ok(output)
            }
        }
    }

    /// Check if a path is allowed by the filesystem allowlist.
    ///
    /// The check is resistant to the two classic bypasses:
    ///
    /// * **Traversal** — the path is lexically normalised first, so
    ///   `/data/../etc/passwd` is compared as `/etc/passwd` and denied for an
    ///   allowlist of `["/data"]`.
    /// * **Prefix collision** — comparison is component-wise
    ///   ([`Path::starts_with`]), so `/data-secret/keys` does *not* match the
    ///   `/data` entry.
    ///
    /// Symlinks are resolved for the longest existing ancestor of both the
    /// candidate and the allowlist entries, so a symlink planted inside an
    /// allowed directory cannot be used to reach outside it.
    ///
    /// An empty allowlist means "no filesystem containment configured" and
    /// allows everything.
    pub fn is_path_allowed(&self, path: &str) -> bool {
        self.is_path_allowed_path(Path::new(path))
    }

    /// [`Sandbox::is_path_allowed`] for an already-typed [`Path`].
    pub fn is_path_allowed_path(&self, path: &Path) -> bool {
        if self.allowed_roots.is_empty() {
            // If no paths specified, all paths are allowed
            return true;
        }

        match normalize_path(path) {
            Some(normalized) => self
                .allowed_roots
                .iter()
                .any(|allowed| normalized.starts_with(allowed)),
            // A path that cannot be normalised (e.g. one escaping above the
            // filesystem root) is never inside the allowlist.
            None => false,
        }
    }

    /// Gate a filesystem access against the configured policy.
    ///
    /// # Errors
    ///
    /// * [`SandboxViolation::FilesystemWriteDenied`] when `write` is set and
    ///   the configuration is read-only.
    /// * [`SandboxViolation::PathDenied`] when the path is outside the
    ///   allowlist.
    pub async fn check_path(&self, path: &Path, write: bool) -> Result<(), SandboxViolation> {
        if write && !self.config.is_fs_write_allowed() {
            self.stats.write().await.record_path_violation();
            return Err(SandboxViolation::FilesystemWriteDenied);
        }

        if !self.is_path_allowed_path(path) {
            self.stats.write().await.record_path_violation();
            return Err(SandboxViolation::PathDenied {
                path: path.display().to_string(),
            });
        }

        Ok(())
    }

    /// Gate a network access against the configured policy.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxViolation::NetworkDenied`] when the configuration
    /// disallows network access.
    pub fn check_network(&self) -> Result<(), SandboxViolation> {
        if self.config.is_network_allowed() {
            Ok(())
        } else {
            Err(SandboxViolation::NetworkDenied)
        }
    }

    /// Build a task environment with credential-like variables removed.
    ///
    /// At [`IsolationLevel::None`] the input is passed through unchanged; at
    /// every other level any variable whose *name* looks credential-bearing
    /// (see [`Sandbox::is_sensitive_env_key`]) is dropped, so task code cannot
    /// read the worker's own broker/backend/cloud credentials out of the
    /// process environment it inherits.
    pub fn sanitize_env<I, K, V>(&self, vars: I) -> Vec<(String, String)>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let scrub = self.config.isolation_level() != IsolationLevel::None;
        vars.into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .filter(|(k, _)| !(scrub && Self::is_sensitive_env_key(k)))
            .collect()
    }

    /// Whether an environment variable name looks credential-bearing.
    pub fn is_sensitive_env_key(key: &str) -> bool {
        let upper = key.to_ascii_uppercase();
        SENSITIVE_ENV_FRAGMENTS
            .iter()
            .any(|fragment| upper.contains(fragment))
    }

    /// Apply the configured resource limits to the **current process**.
    ///
    /// # Warning
    ///
    /// `setrlimit` is process-wide, not per-task: calling this constrains the
    /// whole worker process, and an unprivileged process can never raise a
    /// hard limit afterwards.  Call it once during worker start-up, before
    /// any task runs — never per task.
    ///
    /// Requested values are clamped to the current hard limit rather than
    /// attempting (and failing) to raise it.  `max_cpu_percent` is *not*
    /// applied: a CPU share is a scheduler property with no rlimit
    /// equivalent; [`SandboxConfig::timeout`] is mapped to `RLIMIT_CPU`
    /// (CPU-seconds) instead.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Unsupported`] when the sandbox is not at
    /// [`IsolationLevel::Process`], when the target is not Unix, or when the
    /// crate's off-by-default `rlimit` feature is not enabled.  Returns
    /// [`SandboxError::LimitFailed`] if the OS refuses a limit.
    pub fn enforce_process_limits(&self) -> Result<(), SandboxError> {
        if self.config.isolation_level() != IsolationLevel::Process {
            return Err(SandboxError::Unsupported {
                control: "enforce_process_limits".to_string(),
                reason: format!(
                    "OS resource limits require IsolationLevel::Process (configured: {})",
                    self.config.isolation_level()
                ),
            });
        }

        // Tracks whether this build could apply *anything*, so a build with
        // every OS-level feature switched off still fails loudly instead of
        // reporting success for controls it never installed.
        #[allow(unused_mut, unused_assignments)]
        let mut applied_any = false;

        #[cfg(all(unix, feature = "rlimit"))]
        {
            rlimit_impl::apply(&self.config)?;
            applied_any = true;
        }

        // The seccomp filter is installed last, because it is irreversible:
        // if an rlimit is going to be refused, fail before narrowing the
        // process's syscall surface for the rest of its life.
        #[cfg(all(target_os = "linux", feature = "seccomp"))]
        if self.config.is_seccomp_enabled() {
            seccomp_impl::install()?;
            applied_any = true;
        }

        if !applied_any {
            return Err(SandboxError::Unsupported {
                control: "enforce_process_limits".to_string(),
                reason: "this build cannot apply OS resource limits: the `rlimit` feature is \
                         disabled or the target is not Unix"
                    .to_string(),
            });
        }
        Ok(())
    }

    /// Validate resource usage reported by the caller.
    ///
    /// This is advisory accounting, not enforcement: the numbers come from
    /// the caller.  See [`Sandbox::enforce_process_limits`] for the enforcing
    /// counterpart.
    ///
    /// # Errors
    ///
    /// Returns the corresponding [`SandboxViolation`] when a configured limit
    /// is exceeded.
    pub async fn validate_resources(
        &self,
        memory_mb: usize,
        cpu_percent: u8,
    ) -> Result<(), SandboxViolation> {
        if let Some(max_memory) = self.config.max_memory_mb() {
            if memory_mb > max_memory {
                self.stats.write().await.record_memory_violation();
                return Err(SandboxViolation::MemoryLimit {
                    used: memory_mb,
                    limit: max_memory,
                });
            }
        }

        if let Some(max_cpu) = self.config.max_cpu_percent() {
            if cpu_percent > max_cpu {
                self.stats.write().await.record_cpu_violation();
                return Err(SandboxViolation::CpuLimit {
                    used: cpu_percent,
                    limit: max_cpu,
                });
            }
        }

        Ok(())
    }

    /// Record execution result
    pub async fn record_execution(&self, success: bool, duration_ms: u64, memory_mb: usize) {
        self.stats
            .write()
            .await
            .record_execution(success, duration_ms, memory_mb);
    }

    /// Record timeout violation
    pub async fn record_timeout_violation(&self) {
        self.stats.write().await.record_timeout_violation();
    }

    /// Record file descriptor violation
    pub async fn record_fd_violation(&self) {
        self.stats.write().await.record_fd_violation();
    }
}

impl fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sandbox")
            .field("config", &self.config)
            .field("allowed_roots", &self.allowed_roots)
            .finish()
    }
}

/// Normalise a path for containment comparison.
///
/// The result is absolute, free of `.`/`..` components, and has symlinks
/// resolved for its longest existing ancestor (so an existing path is fully
/// canonical, and a not-yet-created path is canonical up to the deepest
/// directory that does exist).
///
/// Returns `None` when the path escapes above the filesystem root, which is
/// never a containable location.
fn normalize_path(path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };

    // 1. Purely lexical cleanup: drop `.`, resolve `..` against what we have
    //    accumulated so far, and refuse to walk above the root.
    let mut lexical = PathBuf::new();
    let mut depth = 0usize;
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => lexical.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if depth == 0 {
                    // `/..` escapes the root; there is nothing to contain.
                    return None;
                }
                lexical.pop();
                depth -= 1;
            }
            Component::Normal(part) => {
                lexical.push(part);
                depth += 1;
            }
        }
    }

    // 2. Resolve symlinks for the longest existing ancestor and re-apply the
    //    remaining (not-yet-existing) components on top of it.
    let mut trailing: Vec<std::ffi::OsString> = Vec::new();
    let mut candidate = lexical.clone();
    loop {
        if let Ok(canonical) = candidate.canonicalize() {
            let mut resolved = canonical;
            for part in trailing.iter().rev() {
                resolved.push(part);
            }
            return Some(resolved);
        }
        match candidate.file_name() {
            Some(name) => {
                trailing.push(name.to_os_string());
                if !candidate.pop() {
                    return Some(lexical);
                }
            }
            // Reached a root (or a prefix) that does not canonicalize; fall
            // back to the lexically-cleaned path.
            None => return Some(lexical),
        }
    }
}
