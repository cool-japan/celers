//! Statistics, batch operation results, and constant configurations
//!
//! This module contains:
//! - [`BatchOperationResult`] for detailed success/failure tracking
//! - [`PoolStats`] for connection pool statistics
//! - [`StateCount`] and [`StatePercentages`] for task state distribution
//! - [`TaskSummary`] for collection-level task statistics
//! - [`BackendStats`] for backend-level statistics
//! - [`ttl`] module with recommended TTL durations
//! - [`batch_size`] module with recommended batch sizes

use uuid::Uuid;

/// Batch operation result with detailed success/failure tracking
#[derive(Debug, Clone)]
pub struct BatchOperationResult {
    /// Total number of operations attempted
    pub total: usize,
    /// Number of successful operations
    pub successful: usize,
    /// Number of failed operations
    pub failed: usize,
    /// Task IDs that succeeded
    pub succeeded_ids: Vec<Uuid>,
    /// Task IDs that failed with error messages
    pub failed_ids: Vec<(Uuid, String)>,
}

impl BatchOperationResult {
    /// Create a new empty batch result
    pub fn new() -> Self {
        Self {
            total: 0,
            successful: 0,
            failed: 0,
            succeeded_ids: Vec::new(),
            failed_ids: Vec::new(),
        }
    }

    /// Record a successful operation
    pub fn record_success(&mut self, task_id: Uuid) {
        self.total += 1;
        self.successful += 1;
        self.succeeded_ids.push(task_id);
    }

    /// Record a failed operation
    pub fn record_failure(&mut self, task_id: Uuid, error: String) {
        self.total += 1;
        self.failed += 1;
        self.failed_ids.push((task_id, error));
    }

    /// Check if all operations succeeded
    pub fn all_succeeded(&self) -> bool {
        self.failed == 0 && self.total > 0
    }

    /// Check if any operations failed
    pub fn has_failures(&self) -> bool {
        self.failed > 0
    }

    /// Get success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.successful as f64 / self.total as f64
        }
    }

    /// Get failure rate (0.0 to 1.0)
    pub fn failure_rate(&self) -> f64 {
        1.0 - self.success_rate()
    }
}

impl Default for BatchOperationResult {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for BatchOperationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Batch Operation Result:")?;
        writeln!(f, "  Total: {}", self.total)?;
        writeln!(
            f,
            "  Successful: {} ({:.1}%)",
            self.successful,
            self.success_rate() * 100.0
        )?;
        writeln!(
            f,
            "  Failed: {} ({:.1}%)",
            self.failed,
            self.failure_rate() * 100.0
        )?;
        if !self.failed_ids.is_empty() {
            writeln!(f, "  Failed IDs:")?;
            for (id, error) in self.failed_ids.iter().take(5) {
                writeln!(f, "    {} - {}", &id.to_string()[..8], error)?;
            }
            if self.failed_ids.len() > 5 {
                writeln!(f, "    ... and {} more", self.failed_ids.len() - 5)?;
            }
        }
        Ok(())
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Backend type
    pub backend_type: String,
    /// Connection mode (multiplexed, pooled, etc.)
    pub connection_mode: String,
    /// Whether connected to Redis
    pub is_connected: bool,
}

impl std::fmt::Display for PoolStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PoolStats: type={}, mode={}, connected={}",
            self.backend_type, self.connection_mode, self.is_connected
        )
    }
}

/// Count of tasks by state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCount {
    /// Total number of tasks queried
    pub total: usize,
    /// Number of pending tasks
    pub pending: usize,
    /// Number of started tasks
    pub started: usize,
    /// Number of successful tasks
    pub success: usize,
    /// Number of failed tasks
    pub failure: usize,
    /// Number of tasks being retried
    pub retry: usize,
    /// Number of revoked tasks
    pub revoked: usize,
    /// Number of tasks not found
    pub not_found: usize,
}

impl StateCount {
    /// Get the percentage of tasks in each state
    pub fn percentages(&self) -> StatePercentages {
        let total = self.total as f64;
        if total == 0.0 {
            return StatePercentages::default();
        }

        StatePercentages {
            pending: (self.pending as f64 / total) * 100.0,
            started: (self.started as f64 / total) * 100.0,
            success: (self.success as f64 / total) * 100.0,
            failure: (self.failure as f64 / total) * 100.0,
            retry: (self.retry as f64 / total) * 100.0,
            revoked: (self.revoked as f64 / total) * 100.0,
            not_found: (self.not_found as f64 / total) * 100.0,
        }
    }
}

impl std::fmt::Display for StateCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "StateCount (Total: {}):", self.total)?;
        writeln!(f, "  Pending: {}", self.pending)?;
        writeln!(f, "  Started: {}", self.started)?;
        writeln!(f, "  Success: {}", self.success)?;
        writeln!(f, "  Failure: {}", self.failure)?;
        writeln!(f, "  Retry: {}", self.retry)?;
        writeln!(f, "  Revoked: {}", self.revoked)?;
        write!(f, "  Not Found: {}", self.not_found)?;
        Ok(())
    }
}

/// Percentage breakdown of task states
#[derive(Debug, Clone, PartialEq)]
pub struct StatePercentages {
    pub pending: f64,
    pub started: f64,
    pub success: f64,
    pub failure: f64,
    pub retry: f64,
    pub revoked: f64,
    pub not_found: f64,
}

impl Default for StatePercentages {
    fn default() -> Self {
        Self {
            pending: 0.0,
            started: 0.0,
            success: 0.0,
            failure: 0.0,
            retry: 0.0,
            revoked: 0.0,
            not_found: 0.0,
        }
    }
}

impl std::fmt::Display for StatePercentages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "State Percentages:")?;
        writeln!(f, "  Pending: {:.1}%", self.pending)?;
        writeln!(f, "  Started: {:.1}%", self.started)?;
        writeln!(f, "  Success: {:.1}%", self.success)?;
        writeln!(f, "  Failure: {:.1}%", self.failure)?;
        writeln!(f, "  Retry: {:.1}%", self.retry)?;
        writeln!(f, "  Revoked: {:.1}%", self.revoked)?;
        write!(f, "  Not Found: {:.1}%", self.not_found)?;
        Ok(())
    }
}

/// Summary statistics for a collection of tasks
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSummary {
    /// Total number of tasks queried
    pub total: usize,
    /// Number of tasks found in backend
    pub found: usize,
    /// Number of tasks not found
    pub not_found: usize,
    /// Number of pending tasks
    pub pending: usize,
    /// Number of started tasks
    pub started: usize,
    /// Number of successful tasks
    pub success: usize,
    /// Number of failed tasks
    pub failure: usize,
    /// Number of tasks being retried
    pub retry: usize,
    /// Number of revoked tasks
    pub revoked: usize,
}

impl TaskSummary {
    /// Get the completion rate (0.0 to 1.0)
    pub fn completion_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.success + self.failure + self.revoked) as f64 / self.total as f64
        }
    }

    /// Get the success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.success as f64 / self.total as f64
        }
    }

    /// Get the failure rate (0.0 to 1.0)
    pub fn failure_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.failure as f64 / self.total as f64
        }
    }

    /// Check if all tasks are complete
    pub fn all_complete(&self) -> bool {
        self.pending == 0 && self.started == 0 && self.retry == 0
    }

    /// Check if any tasks have failed
    pub fn has_failures(&self) -> bool {
        self.failure > 0
    }
}

impl std::fmt::Display for TaskSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TaskSummary:")?;
        writeln!(f, "  Total: {}", self.total)?;
        writeln!(f, "  Found: {} | Not Found: {}", self.found, self.not_found)?;
        writeln!(
            f,
            "  Pending: {} | Started: {} | Retry: {}",
            self.pending, self.started, self.retry
        )?;
        writeln!(
            f,
            "  Success: {} | Failure: {} | Revoked: {}",
            self.success, self.failure, self.revoked
        )?;
        writeln!(
            f,
            "  Completion Rate: {:.1}%",
            self.completion_rate() * 100.0
        )?;
        writeln!(f, "  Success Rate: {:.1}%", self.success_rate() * 100.0)?;
        write!(f, "  Failure Rate: {:.1}%", self.failure_rate() * 100.0)?;
        Ok(())
    }
}

/// Backend statistics
#[derive(Debug, Clone)]
pub struct BackendStats {
    /// Number of task result keys in Redis
    pub task_key_count: usize,

    /// Number of chord state keys in Redis
    pub chord_key_count: usize,

    /// Total number of backend keys
    pub total_keys: usize,

    /// Memory used by Redis (bytes)
    pub used_memory_bytes: u64,
}

impl std::fmt::Display for BackendStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BackendStats: {} task keys, {} chord keys, {:.2} MB memory",
            self.task_key_count,
            self.chord_key_count,
            self.used_memory_bytes as f64 / 1024.0 / 1024.0
        )
    }
}

/// Common TTL (Time-To-Live) durations for task results
///
/// These constants provide recommended TTL values for different use cases.
///
/// # Example
/// ```no_run
/// use celers_backend_redis::{RedisResultBackend, ResultBackend, TaskMeta, ttl};
/// use uuid::Uuid;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut backend = RedisResultBackend::new("redis://localhost")?;
/// let task_id = Uuid::new_v4();
/// let meta = TaskMeta::new(task_id, "my_task".to_string());
///
/// backend.store_result(task_id, &meta).await?;
///
/// // Use recommended TTL for success results
/// backend.set_expiration(task_id, ttl::SUCCESS).await?;
/// # Ok(())
/// # }
/// ```
pub mod ttl {
    use std::time::Duration;

    /// 1 hour - for temporary/transient results
    pub const TEMPORARY: Duration = Duration::from_secs(3600);

    /// 6 hours - for short-lived task results
    pub const SHORT: Duration = Duration::from_secs(6 * 3600);

    /// 24 hours - recommended for successful task results
    pub const SUCCESS: Duration = Duration::from_secs(86400);

    /// 3 days - for important results that need to be kept longer
    pub const MEDIUM: Duration = Duration::from_secs(3 * 86400);

    /// 7 days - recommended for failed tasks (useful for debugging)
    pub const FAILURE: Duration = Duration::from_secs(7 * 86400);

    /// 30 days - for archival/long-term storage
    pub const LONG: Duration = Duration::from_secs(30 * 86400);

    /// 90 days - maximum recommended retention
    pub const MAXIMUM: Duration = Duration::from_secs(90 * 86400);
}

/// Recommended batch sizes for optimal performance
///
/// These constants provide recommended batch sizes for different operations.
///
/// # Example
/// ```no_run
/// use celers_backend_redis::{RedisResultBackend, ResultBackend, batch_size};
/// use uuid::Uuid;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut backend = RedisResultBackend::new("redis://localhost")?;
///
/// // Use recommended batch size for operations
/// let task_ids: Vec<Uuid> = (0..batch_size::SMALL).map(|_| Uuid::new_v4()).collect();
/// let results = backend.get_results_batch(&task_ids).await?;
/// # Ok(())
/// # }
/// ```
pub mod batch_size {
    /// Small batch (10 items) - for low-latency requirements
    pub const SMALL: usize = 10;

    /// Medium batch (50 items) - recommended default for most operations
    pub const MEDIUM: usize = 50;

    /// Large batch (100 items) - for high-throughput scenarios
    pub const LARGE: usize = 100;

    /// Extra large batch (500 items) - for bulk operations with relaxed latency
    pub const EXTRA_LARGE: usize = 500;

    /// Maximum recommended batch (1000 items) - use with caution
    ///
    /// Batches larger than this may cause Redis to block or consume
    /// excessive memory. Consider using streaming instead.
    pub const MAXIMUM: usize = 1000;
}
