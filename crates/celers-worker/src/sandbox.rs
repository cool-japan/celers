//! Task execution sandboxing for isolation and security
//!
//! This module provides sandboxing capabilities for task execution to improve
//! security and stability. Features include:
//! - Resource limits (CPU, memory, file descriptors)
//! - Execution timeouts
//! - Filesystem isolation
//! - Network restrictions
//! - System call filtering (where supported)
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
//! let sandbox = Sandbox::new(config);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
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

    /// Enable or disable seccomp filtering
    pub fn with_seccomp(mut self, enable: bool) -> Self {
        self.enable_seccomp = enable;
        self
    }

    /// Set isolation level
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
    pub fn strict() -> Self {
        Self {
            max_memory_mb: Some(512),
            max_cpu_percent: Some(50),
            timeout: Some(Duration::from_secs(60)),
            max_file_descriptors: Some(64),
            allow_network: false,
            allow_fs_write: false,
            allowed_paths: Vec::new(),
            enable_seccomp: true,
            isolation_level: IsolationLevel::Full,
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// No isolation (use with caution)
    None,
    /// Basic isolation (resource limits only)
    #[default]
    Basic,
    /// Process isolation (separate process)
    Process,
    /// Container isolation (namespace isolation)
    Container,
    /// Full isolation (VM-level)
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
        self.timeout_violations + self.memory_violations + self.cpu_violations + self.fd_violations
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

/// Sandbox for executing tasks with isolation
pub struct Sandbox {
    config: SandboxConfig,
    stats: tokio::sync::RwLock<SandboxStats>,
}

impl Sandbox {
    /// Create a new sandbox
    pub fn new(config: SandboxConfig) -> Self {
        Self {
            config,
            stats: tokio::sync::RwLock::new(SandboxStats::default()),
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

    /// Check if a path is allowed
    pub fn is_path_allowed(&self, path: &str) -> bool {
        if self.config.allowed_paths.is_empty() {
            // If no paths specified, all paths are allowed
            return true;
        }

        self.config
            .allowed_paths
            .iter()
            .any(|allowed| path.starts_with(allowed))
    }

    /// Validate resource usage
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
            .finish()
    }
}

/// Sandbox violation error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxViolation {
    /// Memory limit exceeded
    MemoryLimit { used: usize, limit: usize },
    /// CPU limit exceeded
    CpuLimit { used: u8, limit: u8 },
    /// Timeout exceeded
    Timeout { elapsed: Duration, limit: Duration },
    /// File descriptor limit exceeded
    FileDescriptorLimit { used: usize, limit: usize },
    /// Network access denied
    NetworkDenied,
    /// Filesystem write denied
    FilesystemWriteDenied,
    /// Path access denied
    PathDenied { path: String },
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
        assert_eq!(strict.isolation_level(), IsolationLevel::Full);

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
    }

    #[test]
    fn test_isolation_level_display() {
        assert_eq!(format!("{}", IsolationLevel::None), "None");
        assert_eq!(format!("{}", IsolationLevel::Full), "Full");
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
        let sandbox = Sandbox::new(config);
        assert!(sandbox.config().is_valid());
    }

    #[test]
    fn test_sandbox_path_allowed() {
        let temp_dir = std::env::temp_dir().to_string_lossy().to_string();
        let config = SandboxConfig::new()
            .with_allowed_path(temp_dir.clone())
            .with_allowed_path("/var".to_string());

        let sandbox = Sandbox::new(config);
        let temp_file = std::env::temp_dir().join("file.txt");
        assert!(sandbox.is_path_allowed(temp_file.to_str().unwrap()));
        assert!(sandbox.is_path_allowed("/var/log/app.log"));
        assert!(!sandbox.is_path_allowed("/etc/passwd"));
    }

    #[test]
    fn test_sandbox_path_allowed_empty() {
        let config = SandboxConfig::new();
        let sandbox = Sandbox::new(config);

        // When no paths specified, all paths are allowed
        assert!(sandbox.is_path_allowed("/any/path"));
    }

    #[tokio::test]
    async fn test_sandbox_validate_resources() {
        let config = SandboxConfig::new()
            .with_max_memory_mb(1024)
            .with_max_cpu_percent(80);

        let sandbox = Sandbox::new(config);

        // Within limits
        assert!(sandbox.validate_resources(512, 50).await.is_ok());

        // Exceed memory limit
        let result = sandbox.validate_resources(2048, 50).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SandboxViolation::MemoryLimit { .. }
        ));

        // Exceed CPU limit
        let result = sandbox.validate_resources(512, 90).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SandboxViolation::CpuLimit { .. }
        ));
    }

    #[tokio::test]
    async fn test_sandbox_record_execution() {
        let config = SandboxConfig::default();
        let sandbox = Sandbox::new(config);

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
        let sandbox = Sandbox::new(config);

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
        let sandbox = Sandbox::new(config);

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
}
