//! Sandbox execution statistics: [`SandboxStats`].
//!
//! Split out of `sandbox.rs` so the file stays under the workspace's
//! per-file line cap.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Sandbox execution statistics
///
/// Every field is `pub(super)` rather than private: `sandbox`'s own test
/// module seeds fixture values via struct-literal syntax, the same access a
/// single-file `sandbox.rs` gave for free before it was split. The mutator
/// methods below are still how [`super::Sandbox`] updates a live instance;
/// nothing outside `sandbox` can see these fields either way.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxStats {
    /// Total executions
    pub(super) total_executions: u64,
    /// Successful executions
    pub(super) successful: u64,
    /// Failed executions
    pub(super) failed: u64,
    /// Timeout violations
    pub(super) timeout_violations: u64,
    /// Memory limit violations
    pub(super) memory_violations: u64,
    /// CPU limit violations
    pub(super) cpu_violations: u64,
    /// File descriptor limit violations
    pub(super) fd_violations: u64,
    /// Path access denials
    pub(super) path_violations: u64,
    /// Average execution time (milliseconds)
    pub(super) avg_execution_time_ms: u64,
    /// Peak memory usage (MB)
    pub(super) peak_memory_mb: usize,
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

    /// Record a path (filesystem) policy violation.
    ///
    /// `pub(super)` rather than public: mutation is `sandbox`'s job, done
    /// from behind the [`super::Sandbox`]-held write lock; outside callers
    /// only ever read through the getters above.
    pub(super) fn record_path_violation(&mut self) {
        self.path_violations += 1;
    }

    /// Record a memory-limit violation. See
    /// [`SandboxStats::record_path_violation`] for the visibility rationale.
    pub(super) fn record_memory_violation(&mut self) {
        self.memory_violations += 1;
    }

    /// Record a CPU-limit violation. See
    /// [`SandboxStats::record_path_violation`] for the visibility rationale.
    pub(super) fn record_cpu_violation(&mut self) {
        self.cpu_violations += 1;
    }

    /// Record a timeout violation. See
    /// [`SandboxStats::record_path_violation`] for the visibility rationale.
    pub(super) fn record_timeout_violation(&mut self) {
        self.timeout_violations += 1;
    }

    /// Record a file-descriptor violation. See
    /// [`SandboxStats::record_path_violation`] for the visibility rationale.
    pub(super) fn record_fd_violation(&mut self) {
        self.fd_violations += 1;
    }

    /// Record one execution outcome: the running total, the success/failure
    /// split, the rolling average duration and the observed peak memory.
    /// See [`SandboxStats::record_path_violation`] for the visibility
    /// rationale.
    pub(super) fn record_execution(&mut self, success: bool, duration_ms: u64, memory_mb: usize) {
        self.total_executions += 1;

        if success {
            self.successful += 1;
        } else {
            self.failed += 1;
        }

        // Update average execution time
        let total_time = self.avg_execution_time_ms * (self.total_executions - 1) + duration_ms;
        self.avg_execution_time_ms = total_time / self.total_executions;

        // Update peak memory
        if memory_mb > self.peak_memory_mb {
            self.peak_memory_mb = memory_mb;
        }
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
