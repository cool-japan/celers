//! Sandbox error types: [`SandboxError`] (construction/enforcement failures)
//! and [`SandboxViolation`] (runtime policy violations).
//!
//! Split out of `sandbox.rs` so the file stays under the workspace's
//! per-file line cap.

use std::fmt;
use std::time::Duration;

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
