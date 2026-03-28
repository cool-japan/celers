//! Statistics and diagnostics type definitions
//!
//! Types used for task execution stats, queue saturation monitoring,
//! latency percentiles, state transitions, priority queue stats,
//! migration verification, and query performance profiling.

use celers_core::TaskId;
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Task execution time statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionStats {
    /// Number of completed tasks measured
    pub task_count: i64,
    /// Minimum execution time in seconds
    pub min_execution_secs: f64,
    /// Maximum execution time in seconds
    pub max_execution_secs: f64,
    /// Average execution time in seconds
    pub avg_execution_secs: f64,
    /// Standard deviation of execution time in seconds
    pub stddev_execution_secs: f64,
    /// P95 execution time in seconds
    pub p95_execution_secs: f64,
}

/// Queue saturation monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSaturation {
    /// Number of pending tasks
    pub pending_count: i64,
    /// Number of processing tasks
    pub processing_count: i64,
    /// Total tasks in all states
    pub total_tasks: i64,
    /// Configured capacity threshold
    pub capacity_threshold: i64,
    /// Utilization percentage (0-100)
    pub utilization_percent: f64,
    /// Whether queue is saturated (>= 80% of capacity)
    pub is_saturated: bool,
    /// Whether queue is critical (>= 95% of capacity)
    pub is_critical: bool,
    /// Status: healthy, warning, or critical
    pub status: String,
}

/// Task latency percentiles for SLA monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLatencyPercentiles {
    /// Number of tasks measured
    pub task_count: i64,
    /// P50 (median) latency in seconds
    pub p50_latency_secs: f64,
    /// P95 latency in seconds
    pub p95_latency_secs: f64,
    /// P99 latency in seconds
    pub p99_latency_secs: f64,
}

/// Task state transition record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStateTransition {
    /// Task ID
    pub task_id: TaskId,
    /// Previous state (None if this is initial state)
    pub from_state: Option<String>,
    /// New state
    pub to_state: String,
    /// When the transition occurred
    pub transitioned_at: chrono::DateTime<Utc>,
}

/// Task latency statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLatencyStats {
    /// Number of tasks measured
    pub task_count: i64,
    /// Minimum latency in seconds
    pub min_latency_secs: f64,
    /// Maximum latency in seconds
    pub max_latency_secs: f64,
    /// Average latency in seconds
    pub avg_latency_secs: f64,
    /// Standard deviation of latency in seconds
    pub stddev_latency_secs: f64,
}

/// Priority queue statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityQueueStats {
    /// Priority level
    pub priority: i32,
    /// Number of pending tasks at this priority
    pub pending_count: i64,
    /// Number of processing tasks at this priority
    pub processing_count: i64,
    /// Number of completed tasks at this priority
    pub completed_count: i64,
    /// Number of failed tasks at this priority
    pub failed_count: i64,
    /// Average wait time in seconds for this priority
    pub avg_wait_time_secs: f64,
}

/// Migration verification report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationVerification {
    /// Whether all required migrations are applied
    pub is_complete: bool,
    /// Number of migrations applied
    pub applied_count: usize,
    /// Number of migrations missing
    pub missing_count: usize,
    /// List of applied migration versions
    pub applied_migrations: Vec<String>,
    /// List of missing migration versions
    pub missing_migrations: Vec<String>,
    /// Whether core database schema is valid
    pub schema_valid: bool,
}

/// Query performance profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPerformanceProfile {
    /// Query digest (normalized query text)
    pub query_digest: String,
    /// Number of times executed
    pub execution_count: i64,
    /// Average execution time in milliseconds
    pub avg_execution_time_ms: f64,
    /// Total rows examined
    pub total_rows_examined: i64,
    /// Total rows sent
    pub total_rows_sent: i64,
    /// Number of executions without index
    pub no_index_used_count: i64,
    /// Number of executions with suboptimal index
    pub no_good_index_used_count: i64,
    /// Whether query needs optimization
    pub needs_optimization: bool,
}
