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

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

/// Sandbox configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Maximum memory usage in MB
    max_memory_mb: Option<usize>,
    /// Maximum CPU usage percentage (0-100)
    max_cpu_percent: Option<u8>,
    /// Execution timeout
    timeout: Option<Duration>,
    /// Maximum number of file descriptors
    max_file_descriptors: Option<usize>,
    /// Enable network access
    allow_network: bool,
    /// Enable filesystem write access
    allow_fs_write: bool,
    /// Allowed filesystem paths
    allowed_paths: Vec<String>,
    /// Enable system call filtering
    enable_seccomp: bool,
    /// Isolation level
    isolation_level: IsolationLevel,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: Some(1024),
            max_cpu_percent: Some(100),
            timeout: Some(Duration::from_secs(300)),
            max_file_descriptors: Some(1024),
            allow_network: true,
            allow_fs_write: true,
            allowed_paths: Vec::new(),
            enable_seccomp: false,
            isolation_level: IsolationLevel::Basic,
        }
    }
}

impl SandboxConfig {
    /// Create a new sandbox configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum memory in MB
    pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
        self.max_memory_mb = Some(mb);
        self
    }

    /// Set maximum CPU percentage
    pub fn with_max_cpu_percent(mut self, percent: u8) -> Self {
        self.max_cpu_percent = Some(percent.min(100));
        self
    }

    /// Set execution timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set execution timeout in seconds
    pub fn with_timeout_secs(mut self, secs: u64) -> Self {
        self.timeout = Some(Duration::from_secs(secs));
        self
    }

    /// Set maximum file descriptors
    pub fn with_max_file_descriptors(mut self, count: usize) -> Self {
        self.max_file_descriptors = Some(count);
        self
    }

    /// Enable or disable network access
    pub fn with_network_access(mut self, allow: bool) -> Self {
        self.allow_network = allow;
        self
    }

    /// Enable or disable filesystem write access
    pub fn with_fs_write_access(mut self, allow: bool) -> Self {
        self.allow_fs_write = allow;
        self
    }

    /// Add allowed filesystem path
    pub fn with_allowed_path(mut self, path: String) -> Self {
        self.allowed_paths.push(path);
        self
    }

    /// Enable or disable seccomp-bpf syscall filtering.
    ///
    /// # Availability
    ///
    /// Filtering exists only on Linux, only when `celers-worker` is built
    /// with its off-by-default `seccomp` feature, and only at
    /// [`IsolationLevel::Process`] — the level at which
    /// [`Sandbox::enforce_process_limits`] runs and installs it. In any other
    /// configuration setting this to `true` makes [`Sandbox::new`] fail with
    /// [`SandboxError::Unsupported`] rather than silently ignore the request.
    ///
    /// # Warning: process-wide and irreversible
    ///
    /// The filter applies to the **whole worker process**, not to one task,
    /// and can never be removed once installed — a seccomp filter has no
    /// per-future or per-task granularity, because tasks here are in-process
    /// async futures sharing the runtime's threads. It is therefore a
    /// deny-list of syscalls the worker itself never makes (module loading,
    /// `ptrace`, mount/namespace manipulation, `bpf`, keyring access, …),
    /// answered with `EPERM` rather than a `SIGSYS` kill. It is installed by
    /// [`Sandbox::enforce_process_limits`], which should be called once at
    /// worker start-up.
    pub fn with_seccomp(mut self, enable: bool) -> Self {
        self.enable_seccomp = enable;
        self
    }

    /// Set isolation level
    ///
    /// See [`IsolationLevel`] for which levels this build can actually
    /// enforce; unsupported levels are rejected by [`Sandbox::new`].
    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Get maximum memory in MB
    pub fn max_memory_mb(&self) -> Option<usize> {
        self.max_memory_mb
    }

    /// Get maximum CPU percentage
    pub fn max_cpu_percent(&self) -> Option<u8> {
        self.max_cpu_percent
    }

    /// Get timeout
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Get maximum file descriptors
    pub fn max_file_descriptors(&self) -> Option<usize> {
        self.max_file_descriptors
    }

    /// Check if network access is allowed
    pub fn is_network_allowed(&self) -> bool {
        self.allow_network
    }

    /// Check if filesystem write access is allowed
    pub fn is_fs_write_allowed(&self) -> bool {
        self.allow_fs_write
    }

    /// Get allowed paths
    pub fn allowed_paths(&self) -> &[String] {
        &self.allowed_paths
    }

    /// Check if seccomp is enabled
    pub fn is_seccomp_enabled(&self) -> bool {
        self.enable_seccomp
    }

    /// Get isolation level
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        if let Some(percent) = self.max_cpu_percent {
            if percent == 0 || percent > 100 {
                return false;
            }
        }

        if let Some(mb) = self.max_memory_mb {
            if mb == 0 {
                return false;
            }
        }

        true
    }

    /// Create a strict configuration (minimal permissions)
    ///
    /// The preset uses [`IsolationLevel::Basic`] and leaves seccomp off so
    /// that it stays constructible on every platform: asking for containment
    /// this build cannot provide would only turn [`Sandbox::new`] into a hard
    /// error.  Combine with [`SandboxConfig::with_isolation_level`] and the
    /// `rlimit` feature when running on Unix and real limits are wanted.
    pub fn strict() -> Self {
        Self {
            max_memory_mb: Some(512),
            max_cpu_percent: Some(50),
            timeout: Some(Duration::from_secs(60)),
            max_file_descriptors: Some(64),
            allow_network: false,
            allow_fs_write: false,
            allowed_paths: Vec::new(),
            enable_seccomp: false,
            isolation_level: IsolationLevel::Basic,
        }
    }

    /// Create a lenient configuration (most permissions)
    pub fn lenient() -> Self {
        Self {
            max_memory_mb: Some(4096),
            max_cpu_percent: Some(100),
            timeout: Some(Duration::from_secs(3600)),
            max_file_descriptors: Some(4096),
            allow_network: true,
            allow_fs_write: true,
            allowed_paths: Vec::new(),
            enable_seccomp: false,
            isolation_level: IsolationLevel::Basic,
        }
    }

    /// Create a balanced configuration
    pub fn balanced() -> Self {
        Self::default()
    }
}

impl fmt::Display for SandboxConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SandboxConfig(mem={:?}MB, cpu={:?}%, timeout={:?}s, isolation={})",
            self.max_memory_mb,
            self.max_cpu_percent,
            self.timeout.map(|d| d.as_secs()),
            self.isolation_level
        )
    }
}

/// Isolation level
///
/// Only [`IsolationLevel::None`] and [`IsolationLevel::Basic`] are supported
/// on every platform.  [`IsolationLevel::Process`] additionally requires a
/// Unix target (it is what unlocks [`Sandbox::enforce_process_limits`]).
/// [`IsolationLevel::Container`] and [`IsolationLevel::Full`] are **not
/// implemented**; [`Sandbox::new`] rejects them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// No isolation (use with caution)
    None,
    /// Basic isolation: timeout enforcement, path allowlist, advisory
    /// resource accounting.  Supported everywhere.
    #[default]
    Basic,
    /// Everything `Basic` provides plus process-wide OS resource limits
    /// applied through [`Sandbox::enforce_process_limits`].  Unix only; the
    /// limits themselves additionally require the crate's `rlimit` feature.
    Process,
    /// Container isolation (namespace isolation) — not implemented.
    Container,
    /// Full isolation (VM-level) — not implemented.
    Full,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Basic => write!(f, "Basic"),
            Self::Process => write!(f, "Process"),
            Self::Container => write!(f, "Container"),
            Self::Full => write!(f, "Full"),
        }
    }
}

/// Error returned when a sandbox cannot be created or a control cannot be
/// applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxError {
    /// The configuration is internally inconsistent (see
    /// [`SandboxConfig::is_valid`]).
    InvalidConfig(String),
    /// The configuration asks for containment this build/platform cannot
    /// provide.  Returned instead of pretending the control is active.
    Unsupported {
        /// The control that was requested (e.g. `"IsolationLevel::Full"`).
        control: String,
        /// Why it cannot be honoured here.
        reason: String,
    },
    /// An OS-level limit could not be applied.
    LimitFailed(String),
}

impl fmt::Display for SandboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(msg) => write!(f, "Invalid sandbox configuration: {}", msg),
            Self::Unsupported { control, reason } => {
                write!(
                    f,
                    "Sandbox control '{}' is not supported: {}",
                    control, reason
                )
            }
            Self::LimitFailed(msg) => write!(f, "Failed to apply resource limit: {}", msg),
        }
    }
}

impl std::error::Error for SandboxError {}

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

/// Sandbox execution statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxStats {
    /// Total executions
    total_executions: u64,
    /// Successful executions
    successful: u64,
    /// Failed executions
    failed: u64,
    /// Timeout violations
    timeout_violations: u64,
    /// Memory limit violations
    memory_violations: u64,
    /// CPU limit violations
    cpu_violations: u64,
    /// File descriptor limit violations
    fd_violations: u64,
    /// Path access denials
    path_violations: u64,
    /// Average execution time (milliseconds)
    avg_execution_time_ms: u64,
    /// Peak memory usage (MB)
    peak_memory_mb: usize,
}

impl SandboxStats {
    /// Create new sandbox statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total executions
    pub fn total_executions(&self) -> u64 {
        self.total_executions
    }

    /// Get successful executions
    pub fn successful(&self) -> u64 {
        self.successful
    }

    /// Get failed executions
    pub fn failed(&self) -> u64 {
        self.failed
    }

    /// Get timeout violations
    pub fn timeout_violations(&self) -> u64 {
        self.timeout_violations
    }

    /// Get memory violations
    pub fn memory_violations(&self) -> u64 {
        self.memory_violations
    }

    /// Get CPU violations
    pub fn cpu_violations(&self) -> u64 {
        self.cpu_violations
    }

    /// Get file descriptor violations
    pub fn fd_violations(&self) -> u64 {
        self.fd_violations
    }

    /// Get path access denials
    pub fn path_violations(&self) -> u64 {
        self.path_violations
    }

    /// Get average execution time
    pub fn avg_execution_time_ms(&self) -> u64 {
        self.avg_execution_time_ms
    }

    /// Get peak memory usage
    pub fn peak_memory_mb(&self) -> usize {
        self.peak_memory_mb
    }

    /// Calculate success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_executions == 0 {
            return 0.0;
        }
        self.successful as f64 / self.total_executions as f64
    }

    /// Get total violations
    pub fn total_violations(&self) -> u64 {
        self.timeout_violations
            + self.memory_violations
            + self.cpu_violations
            + self.fd_violations
            + self.path_violations
    }

    /// Calculate violation rate (0.0 - 1.0)
    pub fn violation_rate(&self) -> f64 {
        if self.total_executions == 0 {
            return 0.0;
        }
        self.total_violations() as f64 / self.total_executions as f64
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for SandboxStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SandboxStats(total={}, success_rate={:.2}%, violations={})",
            self.total_executions,
            self.success_rate() * 100.0,
            self.total_violations()
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
            .allowed_paths
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

        if config.enable_seccomp {
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
            if config.isolation_level != IsolationLevel::Process {
                return Err(SandboxError::Unsupported {
                    control: "seccomp".to_string(),
                    reason: format!(
                        "seccomp filtering is installed by Sandbox::enforce_process_limits, \
                         which requires IsolationLevel::Process (configured: {}). Set \
                         .with_isolation_level(IsolationLevel::Process), or disable seccomp \
                         with .with_seccomp(false)",
                        config.isolation_level
                    ),
                });
            }
        }

        match config.isolation_level {
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
            timeout: self.config.timeout.is_some(),
            path_allowlist: !self.allowed_roots.is_empty(),
            read_only_filesystem: !self.config.allow_fs_write,
            network_gate: !self.config.allow_network,
            os_resource_limits: cfg!(all(unix, feature = "rlimit"))
                && self.config.isolation_level == IsolationLevel::Process,
            // Like `os_resource_limits`, this reports that the control is
            // both requested and compilable in this build; it is actually
            // installed by `enforce_process_limits`.
            syscall_filter: cfg!(all(target_os = "linux", feature = "seccomp"))
                && self.config.enable_seccomp
                && self.config.isolation_level == IsolationLevel::Process,
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
        match self.config.timeout {
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
        if write && !self.config.allow_fs_write {
            self.stats.write().await.path_violations += 1;
            return Err(SandboxViolation::FilesystemWriteDenied);
        }

        if !self.is_path_allowed_path(path) {
            self.stats.write().await.path_violations += 1;
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
        if self.config.allow_network {
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
        let scrub = self.config.isolation_level != IsolationLevel::None;
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
        if self.config.isolation_level != IsolationLevel::Process {
            return Err(SandboxError::Unsupported {
                control: "enforce_process_limits".to_string(),
                reason: format!(
                    "OS resource limits require IsolationLevel::Process (configured: {})",
                    self.config.isolation_level
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
        if self.config.enable_seccomp {
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
        if let Some(max_memory) = self.config.max_memory_mb {
            if memory_mb > max_memory {
                self.stats.write().await.memory_violations += 1;
                return Err(SandboxViolation::MemoryLimit {
                    used: memory_mb,
                    limit: max_memory,
                });
            }
        }

        if let Some(max_cpu) = self.config.max_cpu_percent {
            if cpu_percent > max_cpu {
                self.stats.write().await.cpu_violations += 1;
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
        let mut stats = self.stats.write().await;
        stats.total_executions += 1;

        if success {
            stats.successful += 1;
        } else {
            stats.failed += 1;
        }

        // Update average execution time
        let total_time = stats.avg_execution_time_ms * (stats.total_executions - 1) + duration_ms;
        stats.avg_execution_time_ms = total_time / stats.total_executions;

        // Update peak memory
        if memory_mb > stats.peak_memory_mb {
            stats.peak_memory_mb = memory_mb;
        }
    }

    /// Record timeout violation
    pub async fn record_timeout_violation(&self) {
        self.stats.write().await.timeout_violations += 1;
    }

    /// Record file descriptor violation
    pub async fn record_fd_violation(&self) {
        self.stats.write().await.fd_violations += 1;
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

/// `setrlimit` bridge, compiled only for Unix targets with the crate's
/// off-by-default `rlimit` feature enabled.
#[cfg(all(unix, feature = "rlimit"))]
mod rlimit_impl {
    use super::{SandboxConfig, SandboxError};

    /// The integer type `getrlimit`/`setrlimit` take for the resource id
    /// differs between platforms: glibc uses `__rlimit_resource_t`, every
    /// other Unix libc uses a plain `c_int`.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    type RlimitResource = libc::__rlimit_resource_t;
    /// See the glibc variant above.
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    type RlimitResource = libc::c_int;

    /// `rlim_t` is `u64` on Linux/macOS but `i64` on the BSDs, so the two
    /// conversions below are load-bearing on some targets and a no-op cast on
    /// others.
    #[allow(clippy::unnecessary_cast)]
    fn to_rlim(value: u64) -> libc::rlim_t {
        value as libc::rlim_t
    }

    /// See [`to_rlim`].
    #[allow(clippy::unnecessary_cast)]
    fn from_rlim(value: libc::rlim_t) -> u64 {
        value as u64
    }

    /// The platform's "no limit" sentinel, widened to `u64`.
    pub(super) fn rlim_infinity() -> u64 {
        from_rlim(libc::RLIM_INFINITY)
    }

    /// Clamp a requested soft limit to the process's current hard limit.
    ///
    /// Lowering is always permitted; raising above the hard limit requires
    /// privileges we deliberately do not assume, so the request is capped
    /// instead of failing the whole call.
    pub(super) fn clamp_to_hard(requested: u64, hard: u64) -> u64 {
        if hard == rlim_infinity() {
            requested
        } else {
            requested.min(hard)
        }
    }

    fn set_limit(resource: RlimitResource, requested: u64) -> Result<(), SandboxError> {
        let mut current = libc::rlimit {
            rlim_cur: to_rlim(0),
            rlim_max: to_rlim(0),
        };
        // SAFETY: `getrlimit` writes into a caller-owned, fully-initialised
        // `rlimit` POD struct, and `resource` is a libc constant.
        let rc = unsafe { libc::getrlimit(resource, &mut current) };
        if rc != 0 {
            return Err(SandboxError::LimitFailed(format!(
                "getrlimit failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        let target = clamp_to_hard(requested, from_rlim(current.rlim_max));
        let new = libc::rlimit {
            rlim_cur: to_rlim(target),
            rlim_max: current.rlim_max,
        };
        // SAFETY: `new` is a fully-initialised `rlimit` POD struct owned by
        // this frame; `setrlimit` only reads through the pointer.
        let rc = unsafe { libc::setrlimit(resource, &new) };
        if rc != 0 {
            return Err(SandboxError::LimitFailed(format!(
                "setrlimit failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }

    pub(super) fn apply(config: &SandboxConfig) -> Result<(), SandboxError> {
        if let Some(mb) = config.max_memory_mb() {
            let bytes = (mb as u64).saturating_mul(1024 * 1024);
            set_limit(libc::RLIMIT_AS, bytes)?;
        }
        if let Some(fds) = config.max_file_descriptors() {
            set_limit(libc::RLIMIT_NOFILE, fds as u64)?;
        }
        if let Some(timeout) = config.timeout() {
            // RLIMIT_CPU is CPU-seconds, a strictly weaker bound than the
            // wall-clock timeout, but the closest honest mapping available.
            set_limit(libc::RLIMIT_CPU, timeout.as_secs().max(1))?;
        }
        Ok(())
    }
}

/// seccomp-bpf bridge, compiled only for Linux with the crate's
/// off-by-default `seccomp` feature enabled.
///
/// # What this filter is, and is not
///
/// It is a **process-wide hardening deny-list**, not a per-task jail. A
/// seccomp filter applies to the thread that installs it (and, with
/// `TSYNC`, its siblings) and can never be removed; tasks here are
/// in-process async futures multiplexed across the runtime's worker threads,
/// so there is no thread or process boundary that corresponds to "one task".
/// Filtering per task is therefore impossible by construction, and a filter
/// that blocked, say, `socket` would break the worker's own broker
/// connection rather than the task's.
///
/// What *is* both safe and useful is denying the syscalls a task worker
/// never legitimately makes — module loading, `ptrace`, mount/namespace
/// manipulation, `bpf`, `perf_event_open`, keyring access, `reboot` and
/// friends. Blocking them shrinks the kernel attack surface reachable from
/// exploited task code without touching anything the worker itself does.
///
/// The action is `SECCOMP_RET_ERRNO(EPERM)`, deliberately **not**
/// `SECCOMP_RET_KILL_*`: if this list is ever wrong, the caller sees
/// `EPERM` instead of the whole worker dying on `SIGSYS`.
///
/// # Verification status
///
/// This module is type-checked against both `x86_64-unknown-linux-gnu` and
/// `aarch64-unknown-linux-gnu`, and [`build_program`]'s jump encoding is
/// covered by a unit test. Neither has been *executed* on a Linux host — the
/// workspace's build machine is macOS, where the whole module is `cfg`'d out.
/// Run `cargo test -p celers-worker --features seccomp` on Linux before
/// relying on it in production.
#[cfg(all(target_os = "linux", feature = "seccomp"))]
mod seccomp_impl {
    use super::SandboxError;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Classic-BPF instruction, layout-compatible with `struct sock_filter`
    /// from `<linux/filter.h>`. Declared here rather than taken from `libc`
    /// so the exact ABI this code relies on is visible at the use site.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SockFilter {
        code: u16,
        jt: u8,
        jf: u8,
        k: u32,
    }

    /// `struct sock_fprog` from `<linux/filter.h>`.
    #[repr(C)]
    struct SockFprog {
        len: libc::c_ushort,
        filter: *const SockFilter,
    }

    // --- BPF opcodes (<linux/bpf_common.h>) ---
    /// `BPF_LD | BPF_W | BPF_ABS`
    const LD_W_ABS: u16 = 0x00 | 0x00 | 0x20;
    /// `BPF_JMP | BPF_JEQ | BPF_K`
    const JEQ_K: u16 = 0x05 | 0x10 | 0x00;
    /// `BPF_RET | BPF_K`
    const RET_K: u16 = 0x06 | 0x00;

    // --- seccomp constants (<linux/seccomp.h>, <linux/prctl.h>) ---
    /// Byte offset of `seccomp_data.nr`.
    const OFFSET_NR: u32 = 0;
    /// Byte offset of `seccomp_data.arch`.
    const OFFSET_ARCH: u32 = 4;
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
    const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
    const SECCOMP_MODE_FILTER: libc::c_int = 2;
    const PR_SET_SECCOMP: libc::c_int = 22;
    const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;

    /// `AUDIT_ARCH_*` from `<linux/audit.h>`, for the arch check that stops
    /// a 32-bit compat entry point from bypassing the syscall-number list.
    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH: u32 = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH: u32 = 0xc000_00b7;

    /// Installed at most once per process; a second call is a no-op success
    /// because the filter is already in force and cannot be removed.
    static INSTALLED: AtomicBool = AtomicBool::new(false);

    /// The syscalls a CeleRS worker never makes and that an exploited task
    /// would want. Everything here is available on both Linux targets this
    /// crate is type-checked against (`x86_64` and `aarch64`).
    fn denied_syscalls() -> Vec<libc::c_long> {
        // `mut` is used only on x86_64, which appends three arch-specific
        // entries below; aarch64 has no equivalent syscalls.
        #[allow(unused_mut)]
        let mut denied: Vec<libc::c_long> = vec![
            // Debugging / cross-process memory access
            libc::SYS_ptrace,
            libc::SYS_process_vm_readv,
            libc::SYS_process_vm_writev,
            // Kernel module and kernel image manipulation
            libc::SYS_init_module,
            libc::SYS_finit_module,
            libc::SYS_delete_module,
            libc::SYS_kexec_load,
            libc::SYS_kexec_file_load,
            // Mount / namespace manipulation
            libc::SYS_mount,
            libc::SYS_umount2,
            libc::SYS_pivot_root,
            libc::SYS_chroot,
            libc::SYS_setns,
            libc::SYS_unshare,
            // Tracing / eBPF subsystems
            libc::SYS_bpf,
            libc::SYS_perf_event_open,
            // Kernel keyring
            libc::SYS_add_key,
            libc::SYS_keyctl,
            libc::SYS_request_key,
            // Filesystem handles that bypass path resolution
            libc::SYS_name_to_handle_at,
            libc::SYS_open_by_handle_at,
            // Whole-machine state
            libc::SYS_swapon,
            libc::SYS_swapoff,
            libc::SYS_reboot,
            libc::SYS_acct,
            libc::SYS_quotactl,
            // Misc privilege / memory-management escape hatches
            libc::SYS_personality,
            libc::SYS_userfaultfd,
        ];
        #[cfg(target_arch = "x86_64")]
        {
            // x86-only I/O port and LDT access.
            denied.push(libc::SYS_iopl);
            denied.push(libc::SYS_ioperm);
            denied.push(libc::SYS_modify_ldt);
        }
        denied
    }

    /// Build the filter program.
    ///
    /// Layout (indices are instruction slots):
    ///
    /// ```text
    ///   0            load seccomp_data.arch
    ///   1            if arch != AUDIT_ARCH -> deny
    ///   2            load seccomp_data.nr
    ///   3 .. 3+n-1   if nr == denied[i]    -> deny
    ///   3+n          return ALLOW
    ///   4+n          deny: return ERRNO(EPERM)
    /// ```
    ///
    /// Jump offsets are relative to the *following* instruction and are
    /// single bytes, which caps the list at 253 entries — far above the ~30
    /// used here, and asserted below so a future addition cannot silently
    /// produce a mis-encoded program.
    fn build_program(denied: &[libc::c_long]) -> Vec<SockFilter> {
        let n = denied.len();
        assert!(
            n <= 253,
            "seccomp deny-list must stay within the 8-bit BPF jump range"
        );
        let n_u8 = n as u8;

        let mut prog = Vec::with_capacity(n + 5);
        // 0: A = seccomp_data.arch
        prog.push(SockFilter {
            code: LD_W_ABS,
            jt: 0,
            jf: 0,
            k: OFFSET_ARCH,
        });
        // 1: if A == AUDIT_ARCH fall through, else jump to deny.
        prog.push(SockFilter {
            code: JEQ_K,
            jt: 0,
            jf: n_u8 + 2,
            k: AUDIT_ARCH,
        });
        // 2: A = seccomp_data.nr
        prog.push(SockFilter {
            code: LD_W_ABS,
            jt: 0,
            jf: 0,
            k: OFFSET_NR,
        });
        // 3..: one equality test per denied syscall.
        for (i, nr) in denied.iter().enumerate() {
            prog.push(SockFilter {
                code: JEQ_K,
                jt: n_u8 - i as u8,
                jf: 0,
                k: *nr as u32,
            });
        }
        // 3+n: nothing matched -> allow.
        prog.push(SockFilter {
            code: RET_K,
            jt: 0,
            jf: 0,
            k: SECCOMP_RET_ALLOW,
        });
        // 4+n: deny -> EPERM (never KILL; see the module docs).
        prog.push(SockFilter {
            code: RET_K,
            jt: 0,
            jf: 0,
            k: SECCOMP_RET_ERRNO | (libc::EPERM as u32 & 0x0000_ffff),
        });
        prog
    }

    /// Install the filter for this process.
    ///
    /// Idempotent: the second and later calls succeed without touching the
    /// kernel, because a seccomp filter can never be uninstalled.
    pub(super) fn install() -> Result<(), SandboxError> {
        if INSTALLED.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // `PR_SET_NO_NEW_PRIVS` is mandatory for an unprivileged
        // `PR_SET_SECCOMP`, and is what stops a set-uid binary from being
        // used to shed the filter.
        //
        // The trailing arguments are cast to `c_ulong` rather than left as
        // untyped integer literals: `prctl` is variadic and the C library
        // reads arg2..arg5 as `unsigned long`, so passing 32-bit `int`s would
        // leave the upper half of each 8-byte variadic slot undefined — and
        // the kernel rejects `PR_SET_NO_NEW_PRIVS` with `EINVAL` unless
        // arg3..arg5 are exactly zero.
        //
        // SAFETY: `prctl` with these constants takes no pointers.
        let rc = unsafe {
            libc::prctl(
                PR_SET_NO_NEW_PRIVS,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        };
        if rc != 0 {
            INSTALLED.store(false, Ordering::SeqCst);
            return Err(SandboxError::LimitFailed(format!(
                "prctl(PR_SET_NO_NEW_PRIVS) failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        let program = build_program(&denied_syscalls());
        let fprog = SockFprog {
            len: program.len() as libc::c_ushort,
            filter: program.as_ptr(),
        };
        // SAFETY: `fprog` points at `program`, which outlives this call, and
        // `len` is exactly its length. The kernel copies the program in.
        // See the `c_ulong` note above for why the trailing zeros are typed.
        let rc = unsafe {
            libc::prctl(
                PR_SET_SECCOMP,
                SECCOMP_MODE_FILTER as libc::c_ulong,
                &fprog as *const SockFprog,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        };
        if rc != 0 {
            INSTALLED.store(false, Ordering::SeqCst);
            return Err(SandboxError::LimitFailed(format!(
                "prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER) failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn program_layout_is_wellformed() {
            let denied = denied_syscalls();
            let prog = build_program(&denied);
            let n = denied.len();
            assert_eq!(prog.len(), n + 5);

            // The arch mismatch branch must land exactly on the deny slot.
            let deny_index = n + 4;
            assert_eq!(1 + 1 + prog[1].jf as usize, deny_index);
            // Every syscall test must land exactly on the deny slot too.
            for (i, insn) in prog[3..3 + n].iter().enumerate() {
                assert_eq!(3 + i + 1 + insn.jt as usize, deny_index);
                assert_eq!(insn.jf, 0);
            }
            assert_eq!(prog[n + 3].k, SECCOMP_RET_ALLOW);
            assert_eq!(prog[deny_index].k, SECCOMP_RET_ERRNO | libc::EPERM as u32);
        }

        #[test]
        fn deny_list_has_no_duplicates() {
            let denied = denied_syscalls();
            let mut sorted = denied.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), denied.len(), "duplicate syscall in deny-list");
        }
    }
}

/// Sandbox violation error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxViolation {
    /// Memory limit exceeded
    MemoryLimit {
        /// Memory actually used, in MB.
        used: usize,
        /// Configured limit, in MB.
        limit: usize,
    },
    /// CPU limit exceeded
    CpuLimit {
        /// CPU percentage actually used.
        used: u8,
        /// Configured limit, as a percentage.
        limit: u8,
    },
    /// Timeout exceeded
    Timeout {
        /// Time the execution actually took.
        elapsed: Duration,
        /// Configured limit.
        limit: Duration,
    },
    /// File descriptor limit exceeded
    FileDescriptorLimit {
        /// Descriptors actually open.
        used: usize,
        /// Configured limit.
        limit: usize,
    },
    /// Network access denied
    NetworkDenied,
    /// Filesystem write denied
    FilesystemWriteDenied,
    /// Path access denied
    PathDenied {
        /// The path that was refused.
        path: String,
    },
}

impl fmt::Display for SandboxViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryLimit { used, limit } => {
                write!(f, "Memory limit exceeded: {}MB > {}MB", used, limit)
            }
            Self::CpuLimit { used, limit } => {
                write!(f, "CPU limit exceeded: {}% > {}%", used, limit)
            }
            Self::Timeout { elapsed, limit } => {
                write!(f, "Timeout exceeded: {:?} > {:?}", elapsed, limit)
            }
            Self::FileDescriptorLimit { used, limit } => {
                write!(f, "File descriptor limit exceeded: {} > {}", used, limit)
            }
            Self::NetworkDenied => write!(f, "Network access denied"),
            Self::FilesystemWriteDenied => write!(f, "Filesystem write access denied"),
            Self::PathDenied { path } => write!(f, "Path access denied: {}", path),
        }
    }
}

impl std::error::Error for SandboxViolation {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sandbox(config: SandboxConfig) -> Sandbox {
        Sandbox::new(config).expect("configuration should be supported")
    }

    #[test]
    fn test_sandbox_config_default() {
        let config = SandboxConfig::default();
        assert_eq!(config.max_memory_mb(), Some(1024));
        assert_eq!(config.max_cpu_percent(), Some(100));
        assert!(config.is_network_allowed());
        assert!(config.is_valid());
    }

    #[test]
    fn test_sandbox_config_builder() {
        let config = SandboxConfig::new()
            .with_max_memory_mb(512)
            .with_max_cpu_percent(80)
            .with_timeout_secs(60)
            .with_network_access(false);

        assert_eq!(config.max_memory_mb(), Some(512));
        assert_eq!(config.max_cpu_percent(), Some(80));
        assert_eq!(config.timeout(), Some(Duration::from_secs(60)));
        assert!(!config.is_network_allowed());
    }

    #[test]
    fn test_sandbox_config_presets() {
        let strict = SandboxConfig::strict();
        assert_eq!(strict.max_memory_mb(), Some(512));
        assert!(!strict.is_network_allowed());
        // The strict preset must stay constructible: it may only ask for
        // controls this build can actually enforce.
        assert_eq!(strict.isolation_level(), IsolationLevel::Basic);
        assert!(!strict.is_seccomp_enabled());
        assert!(Sandbox::new(strict).is_ok());

        let lenient = SandboxConfig::lenient();
        assert_eq!(lenient.max_memory_mb(), Some(4096));
        assert!(lenient.is_network_allowed());

        let balanced = SandboxConfig::balanced();
        assert_eq!(balanced.max_memory_mb(), Some(1024));
    }

    #[test]
    fn test_sandbox_config_validation() {
        let config = SandboxConfig::new().with_max_cpu_percent(150);
        assert_eq!(config.max_cpu_percent(), Some(100)); // Clamped to 100

        let config = SandboxConfig::new().with_max_cpu_percent(0);
        assert!(!config.is_valid());
        assert!(matches!(
            Sandbox::new(config),
            Err(SandboxError::InvalidConfig(_))
        ));
    }

    #[test]
    fn test_isolation_level_display() {
        assert_eq!(format!("{}", IsolationLevel::None), "None");
        assert_eq!(format!("{}", IsolationLevel::Full), "Full");
    }

    // --- Regression: unsupported isolation must be a hard error (idx 169) ---

    #[test]
    fn test_unsupported_isolation_levels_are_rejected() {
        for level in [IsolationLevel::Container, IsolationLevel::Full] {
            let config = SandboxConfig::new().with_isolation_level(level);
            match Sandbox::new(config) {
                Err(SandboxError::Unsupported { control, .. }) => {
                    assert!(control.contains(&level.to_string()), "control: {}", control);
                }
                other => panic!("{:?} must be rejected, got {:?}", level, other.map(|_| ())),
            }
        }
    }

    /// Without a Linux target *and* the `seccomp` feature there is no filter
    /// to install, so asking for one must be a hard error rather than a
    /// silently-dropped control. This is what the default build compiles.
    #[cfg(not(all(target_os = "linux", feature = "seccomp")))]
    #[test]
    fn test_seccomp_request_is_rejected_when_not_compiled_in() {
        let config = SandboxConfig::new().with_seccomp(true);
        match Sandbox::new(config) {
            Err(SandboxError::Unsupported { control, reason }) => {
                assert_eq!(control, "seccomp");
                assert!(reason.contains("seccomp"), "reason: {}", reason);
                assert!(reason.contains("feature"), "reason: {}", reason);
            }
            other => panic!("seccomp must be rejected, got {:?}", other.map(|_| ())),
        }
    }

    /// With the filter compiled in, a configuration that can actually receive
    /// it must construct — and one that cannot must still be refused.
    #[cfg(all(target_os = "linux", feature = "seccomp"))]
    #[test]
    fn test_seccomp_request_is_accepted_only_at_isolation_level_process() {
        // Installed by `enforce_process_limits`, which requires
        // IsolationLevel::Process — so a Basic-level sandbox asking for
        // seccomp would silently never get it, and must be refused.
        let basic = SandboxConfig::new().with_seccomp(true);
        match Sandbox::new(basic) {
            Err(SandboxError::Unsupported { control, reason }) => {
                assert_eq!(control, "seccomp");
                assert!(
                    reason.contains("IsolationLevel::Process"),
                    "reason: {reason}"
                );
            }
            other => panic!(
                "seccomp below IsolationLevel::Process must be rejected, got {:?}",
                other.map(|_| ())
            ),
        }

        let process = SandboxConfig::new()
            .with_seccomp(true)
            .with_isolation_level(IsolationLevel::Process);
        let sandbox = Sandbox::new(process).expect("seccomp is supported in this build");
        assert!(sandbox.config().is_seccomp_enabled());
        assert!(sandbox.enforcement().syscall_filter);
    }

    #[test]
    fn test_supported_levels_are_accepted() {
        for level in [IsolationLevel::None, IsolationLevel::Basic] {
            let config = SandboxConfig::new().with_isolation_level(level);
            assert!(
                Sandbox::new(config).is_ok(),
                "{:?} must be supported",
                level
            );
        }

        let process = SandboxConfig::new().with_isolation_level(IsolationLevel::Process);
        assert_eq!(Sandbox::new(process).is_ok(), cfg!(unix));
    }

    #[test]
    fn test_enforcement_report_is_honest() {
        let sandbox = make_sandbox(
            SandboxConfig::new()
                .with_timeout_secs(5)
                .with_network_access(false)
                .with_fs_write_access(false)
                .with_allowed_path("/var".to_string()),
        );

        let report = sandbox.enforcement();
        assert!(report.timeout);
        assert!(report.path_allowlist);
        assert!(report.read_only_filesystem);
        assert!(report.network_gate);
        // Never claim syscall filtering; it is not implemented.
        assert!(!report.syscall_filter);
        // Basic isolation never applies OS limits.
        assert!(!report.os_resource_limits);
    }

    #[test]
    fn test_process_limits_require_process_level() {
        let sandbox = make_sandbox(SandboxConfig::new());
        assert!(matches!(
            sandbox.enforce_process_limits(),
            Err(SandboxError::Unsupported { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn test_process_limits_report_capability_truthfully() {
        let sandbox = make_sandbox(
            SandboxConfig::new()
                .with_isolation_level(IsolationLevel::Process)
                .with_network_access(false),
        );
        assert_eq!(
            sandbox.enforcement().os_resource_limits,
            cfg!(feature = "rlimit")
        );

        // Without the feature the call must fail loudly rather than pretend.
        #[cfg(not(feature = "rlimit"))]
        assert!(matches!(
            sandbox.enforce_process_limits(),
            Err(SandboxError::Unsupported { .. })
        ));
    }

    #[cfg(all(unix, feature = "rlimit"))]
    #[test]
    fn test_rlimit_clamps_to_hard_limit() {
        use super::rlimit_impl::{clamp_to_hard, rlim_infinity};

        assert_eq!(clamp_to_hard(100, 50), 50);
        assert_eq!(clamp_to_hard(10, 50), 10);
        assert_eq!(clamp_to_hard(u64::MAX - 1, rlim_infinity()), u64::MAX - 1);
    }

    #[cfg(all(unix, feature = "rlimit"))]
    #[test]
    fn test_enforce_process_limits_with_no_limits_is_a_noop() {
        // Deliberately configures *no* limits: applying a real RLIMIT_AS to
        // the test process would break the test runner itself.
        let config = SandboxConfig {
            max_memory_mb: None,
            max_cpu_percent: None,
            timeout: None,
            max_file_descriptors: None,
            allow_network: true,
            allow_fs_write: true,
            allowed_paths: Vec::new(),
            enable_seccomp: false,
            isolation_level: IsolationLevel::Process,
        };
        let sandbox = make_sandbox(config);
        assert!(sandbox.enforce_process_limits().is_ok());
    }

    #[test]
    fn test_sandbox_stats_default() {
        let stats = SandboxStats::default();
        assert_eq!(stats.total_executions(), 0);
        assert_eq!(stats.successful(), 0);
        assert_eq!(stats.success_rate(), 0.0);
        assert_eq!(stats.total_violations(), 0);
    }

    #[test]
    fn test_sandbox_stats_rates() {
        let stats = SandboxStats {
            total_executions: 100,
            successful: 80,
            timeout_violations: 10,
            memory_violations: 5,
            ..Default::default()
        };

        assert_eq!(stats.success_rate(), 0.8);
        assert_eq!(stats.total_violations(), 15);
        assert_eq!(stats.violation_rate(), 0.15);
    }

    #[test]
    fn test_sandbox_creation() {
        let config = SandboxConfig::default();
        let sandbox = make_sandbox(config);
        assert!(sandbox.config().is_valid());
    }

    #[test]
    fn test_sandbox_path_allowed() {
        let temp_dir = std::env::temp_dir().to_string_lossy().to_string();
        let config = SandboxConfig::new()
            .with_allowed_path(temp_dir.clone())
            .with_allowed_path("/var".to_string());

        let sandbox = make_sandbox(config);
        let temp_file = std::env::temp_dir().join("file.txt");
        assert!(sandbox.is_path_allowed_path(&temp_file));
        assert!(sandbox.is_path_allowed("/var/log/app.log"));
        assert!(!sandbox.is_path_allowed("/etc/passwd"));
    }

    #[test]
    fn test_sandbox_path_allowed_empty() {
        let config = SandboxConfig::new();
        let sandbox = make_sandbox(config);

        // When no paths specified, all paths are allowed
        assert!(sandbox.is_path_allowed("/any/path"));
    }

    // --- Regression tests for the allowlist bypasses (idx 170) ---

    #[test]
    fn test_path_traversal_is_denied() {
        let config = SandboxConfig::new().with_allowed_path("/data".to_string());
        let sandbox = make_sandbox(config);

        assert!(sandbox.is_path_allowed("/data/report.csv"));
        assert!(
            !sandbox.is_path_allowed("/data/../etc/passwd"),
            "`..` must be resolved before the allowlist comparison"
        );
        assert!(!sandbox.is_path_allowed("/data/sub/../../etc/shadow"));
    }

    #[test]
    fn test_prefix_collision_is_denied() {
        let config = SandboxConfig::new().with_allowed_path("/data".to_string());
        let sandbox = make_sandbox(config);

        assert!(
            !sandbox.is_path_allowed("/data-secret/keys"),
            "comparison must be component-wise, not string-prefix"
        );
        assert!(!sandbox.is_path_allowed("/database/dump.sql"));
    }

    #[test]
    fn test_escape_above_root_is_denied() {
        let config = SandboxConfig::new().with_allowed_path("/data".to_string());
        let sandbox = make_sandbox(config);
        assert!(!sandbox.is_path_allowed("/../../etc/passwd"));
    }

    #[cfg(unix)]
    #[test]
    fn test_symlink_escaping_allowlist_is_denied() {
        use std::fs;

        let root = std::env::temp_dir().join(format!("celers-sandbox-{}", uuid::Uuid::new_v4()));
        let allowed = root.join("allowed");
        let outside = root.join("outside");
        fs::create_dir_all(&allowed).expect("create allowed dir");
        fs::create_dir_all(&outside).expect("create outside dir");
        let secret = outside.join("secret.txt");
        fs::write(&secret, b"secret").expect("write secret");

        let link = allowed.join("escape.txt");
        std::os::unix::fs::symlink(&secret, &link).expect("create symlink");

        let config = SandboxConfig::new().with_allowed_path(allowed.to_string_lossy().to_string());
        let sandbox = make_sandbox(config);

        let inside = allowed.join("ok.txt");
        fs::write(&inside, b"ok").expect("write inside file");
        assert!(sandbox.is_path_allowed_path(&inside));
        assert!(
            !sandbox.is_path_allowed_path(&link),
            "a symlink pointing outside the allowlist must be denied"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn test_check_path_gates_writes_and_paths() {
        let temp_dir = std::env::temp_dir();
        let config = SandboxConfig::new()
            .with_allowed_path(temp_dir.to_string_lossy().to_string())
            .with_fs_write_access(false);
        let sandbox = make_sandbox(config);

        let inside = temp_dir.join("celers-sandbox-check.txt");
        assert!(sandbox.check_path(&inside, false).await.is_ok());
        assert_eq!(
            sandbox.check_path(&inside, true).await,
            Err(SandboxViolation::FilesystemWriteDenied)
        );
        assert!(matches!(
            sandbox.check_path(Path::new("/etc/passwd"), false).await,
            Err(SandboxViolation::PathDenied { .. })
        ));

        let stats = sandbox.stats().await;
        assert_eq!(stats.path_violations(), 2);
    }

    #[test]
    fn test_check_network_gate() {
        let open = make_sandbox(SandboxConfig::new());
        assert!(open.check_network().is_ok());

        let closed = make_sandbox(SandboxConfig::new().with_network_access(false));
        assert_eq!(closed.check_network(), Err(SandboxViolation::NetworkDenied));
    }

    #[test]
    fn test_sanitize_env_drops_credentials() {
        let sandbox = make_sandbox(SandboxConfig::new());
        let scrubbed = sandbox.sanitize_env([
            ("PATH", "/usr/bin"),
            ("AWS_SECRET_ACCESS_KEY", "shhh"),
            ("celers_broker_password", "shhh"),
            ("HOME", "/home/worker"),
            ("GITHUB_TOKEN", "shhh"),
        ]);

        let keys: Vec<&str> = scrubbed.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["PATH", "HOME"]);

        // IsolationLevel::None explicitly opts out of scrubbing.
        let passthrough =
            make_sandbox(SandboxConfig::new().with_isolation_level(IsolationLevel::None));
        assert_eq!(
            passthrough.sanitize_env([("GITHUB_TOKEN", "shhh")]).len(),
            1
        );
    }

    #[tokio::test]
    async fn test_execute_enforces_timeout() {
        tokio::time::pause();

        let sandbox = make_sandbox(SandboxConfig::new().with_timeout(Duration::from_secs(30)));

        let ok = sandbox.execute(async { 21 * 2 }).await;
        assert_eq!(ok, Ok(42));

        let timed_out = sandbox
            .execute(async {
                std::future::pending::<()>().await;
            })
            .await;
        assert!(matches!(timed_out, Err(SandboxViolation::Timeout { .. })));

        let stats = sandbox.stats().await;
        assert_eq!(stats.total_executions(), 2);
        assert_eq!(stats.successful(), 1);
        assert_eq!(stats.failed(), 1);
        assert_eq!(stats.timeout_violations(), 1);
    }

    #[tokio::test]
    async fn test_execute_without_timeout_runs_to_completion() {
        let config = SandboxConfig {
            timeout: None,
            ..SandboxConfig::new()
        };
        let sandbox = make_sandbox(config);
        assert_eq!(sandbox.execute(async { "done" }).await, Ok("done"));
        assert!(!sandbox.enforcement().timeout);
    }

    #[tokio::test]
    async fn test_sandbox_validate_resources() {
        let config = SandboxConfig::new()
            .with_max_memory_mb(1024)
            .with_max_cpu_percent(80);

        let sandbox = make_sandbox(config);

        // Within limits
        assert!(sandbox.validate_resources(512, 50).await.is_ok());

        // Exceed memory limit
        let result = sandbox.validate_resources(2048, 50).await;
        assert!(matches!(result, Err(SandboxViolation::MemoryLimit { .. })));

        // Exceed CPU limit
        let result = sandbox.validate_resources(512, 90).await;
        assert!(matches!(result, Err(SandboxViolation::CpuLimit { .. })));
    }

    #[tokio::test]
    async fn test_sandbox_record_execution() {
        let config = SandboxConfig::default();
        let sandbox = make_sandbox(config);

        sandbox.record_execution(true, 100, 256).await;
        sandbox.record_execution(false, 200, 512).await;

        let stats = sandbox.stats().await;
        assert_eq!(stats.total_executions(), 2);
        assert_eq!(stats.successful(), 1);
        assert_eq!(stats.failed(), 1);
        assert_eq!(stats.avg_execution_time_ms(), 150);
        assert_eq!(stats.peak_memory_mb(), 512);
    }

    #[tokio::test]
    async fn test_sandbox_record_violations() {
        let config = SandboxConfig::default();
        let sandbox = make_sandbox(config);

        sandbox.record_timeout_violation().await;
        sandbox.record_fd_violation().await;

        let stats = sandbox.stats().await;
        assert_eq!(stats.timeout_violations(), 1);
        assert_eq!(stats.fd_violations(), 1);
        assert_eq!(stats.total_violations(), 2);
    }

    #[tokio::test]
    async fn test_sandbox_reset_stats() {
        let config = SandboxConfig::default();
        let sandbox = make_sandbox(config);

        sandbox.record_execution(true, 100, 256).await;
        sandbox.reset_stats().await;

        let stats = sandbox.stats().await;
        assert_eq!(stats.total_executions(), 0);
    }

    #[test]
    fn test_sandbox_violation_display() {
        let violation = SandboxViolation::MemoryLimit {
            used: 2048,
            limit: 1024,
        };
        assert_eq!(
            format!("{}", violation),
            "Memory limit exceeded: 2048MB > 1024MB"
        );

        let violation = SandboxViolation::NetworkDenied;
        assert_eq!(format!("{}", violation), "Network access denied");
    }

    #[test]
    fn test_sandbox_error_display() {
        let err = SandboxError::Unsupported {
            control: "seccomp".to_string(),
            reason: "not implemented".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "Sandbox control 'seccomp' is not supported: not implemented"
        );
    }

    #[test]
    fn test_normalize_path_resolves_relative_paths() {
        let normalized = normalize_path(Path::new("."));
        assert!(normalized.is_some_and(|p| p.is_absolute()));
    }
}
