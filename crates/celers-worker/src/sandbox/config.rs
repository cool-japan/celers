//! Sandbox configuration: [`SandboxConfig`] and [`IsolationLevel`].
//!
//! Split out of `sandbox.rs` so the file stays under the workspace's
//! per-file line cap; see [`super`] for what this module's types actually
//! enforce.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// Sandbox configuration
///
/// Every field is `pub(super)` rather than private: [`super::Sandbox`] (in
/// the parent `sandbox` module) reads them through the accessors below, and
/// `sandbox`'s own test module builds ad hoc configurations (e.g. every
/// limit deliberately `None`) via struct-literal syntax that the builder
/// methods do not cover — the same access a single-file `sandbox.rs` gave
/// for free before it was split. Nothing outside `sandbox` can see these
/// fields either way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Maximum memory usage in MB
    pub(super) max_memory_mb: Option<usize>,
    /// Maximum CPU usage percentage (0-100)
    pub(super) max_cpu_percent: Option<u8>,
    /// Execution timeout
    pub(super) timeout: Option<Duration>,
    /// Maximum number of file descriptors
    pub(super) max_file_descriptors: Option<usize>,
    /// Enable network access
    pub(super) allow_network: bool,
    /// Enable filesystem write access
    pub(super) allow_fs_write: bool,
    /// Allowed filesystem paths
    pub(super) allowed_paths: Vec<String>,
    /// Enable system call filtering
    pub(super) enable_seccomp: bool,
    /// Isolation level
    pub(super) isolation_level: IsolationLevel,
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
