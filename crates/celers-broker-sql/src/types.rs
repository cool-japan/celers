//! Core type definitions for the MySQL broker
//!
//! This module contains all public types, enums, and data structures
//! used throughout the celers-broker-sql crate.

use celers_core::{CelersError, SerializedTask};
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Task state in the database
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DbTaskState {
    Pending,
    Processing,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for DbTaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbTaskState::Pending => write!(f, "pending"),
            DbTaskState::Processing => write!(f, "processing"),
            DbTaskState::Completed => write!(f, "completed"),
            DbTaskState::Failed => write!(f, "failed"),
            DbTaskState::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for DbTaskState {
    type Err = CelersError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(DbTaskState::Pending),
            "processing" => Ok(DbTaskState::Processing),
            "completed" => Ok(DbTaskState::Completed),
            "failed" => Ok(DbTaskState::Failed),
            "cancelled" => Ok(DbTaskState::Cancelled),
            _ => Err(CelersError::Other(format!("Unknown task state: {}", s))),
        }
    }
}

/// Information about a task in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: Uuid,
    pub task_name: String,
    pub state: DbTaskState,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub worker_id: Option<String>,
    pub error_message: Option<String>,
}

/// Information about a dead-lettered task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqTaskInfo {
    pub id: Uuid,
    pub task_id: Uuid,
    pub task_name: String,
    pub retry_count: i32,
    pub error_message: Option<String>,
    pub failed_at: DateTime<Utc>,
}

/// Database health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub connection_pool_size: u32,
    pub idle_connections: u32,
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub dlq_tasks: i64,
    pub database_version: String,
}

/// Queue statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStatistics {
    pub pending: i64,
    pub processing: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub dlq: i64,
    pub total: i64,
}

/// Task result stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskResultStatus,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub traceback: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub runtime_ms: Option<i64>,
}

/// Task result status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskResultStatus {
    Pending,
    Started,
    Success,
    Failure,
    Retry,
    Revoked,
}

impl std::fmt::Display for TaskResultStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskResultStatus::Pending => write!(f, "PENDING"),
            TaskResultStatus::Started => write!(f, "STARTED"),
            TaskResultStatus::Success => write!(f, "SUCCESS"),
            TaskResultStatus::Failure => write!(f, "FAILURE"),
            TaskResultStatus::Retry => write!(f, "RETRY"),
            TaskResultStatus::Revoked => write!(f, "REVOKED"),
        }
    }
}

impl std::str::FromStr for TaskResultStatus {
    type Err = CelersError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(TaskResultStatus::Pending),
            "STARTED" => Ok(TaskResultStatus::Started),
            "SUCCESS" => Ok(TaskResultStatus::Success),
            "FAILURE" => Ok(TaskResultStatus::Failure),
            "RETRY" => Ok(TaskResultStatus::Retry),
            "REVOKED" => Ok(TaskResultStatus::Revoked),
            _ => Err(CelersError::Other(format!("Unknown result status: {}", s))),
        }
    }
}

/// Table size information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSizeInfo {
    pub table_name: String,
    pub row_count: i64,
    pub data_size_bytes: i64,
    pub index_size_bytes: i64,
}

/// Task count by task name
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNameCount {
    pub task_name: String,
    pub pending: i64,
    pub processing: i64,
    pub completed: i64,
    pub failed: i64,
    pub total: i64,
}

/// Scheduled task information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTaskInfo {
    pub id: Uuid,
    pub task_name: String,
    pub priority: i32,
    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub delay_remaining_secs: i64,
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Minimum number of idle connections
    pub min_connections: u32,
    /// Connection timeout in seconds
    pub acquire_timeout_secs: u64,
    /// Maximum lifetime of a connection in seconds
    pub max_lifetime_secs: Option<u64>,
    /// Idle timeout for connections in seconds
    pub idle_timeout_secs: Option<u64>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
            min_connections: 2,
            acquire_timeout_secs: 5,
            max_lifetime_secs: Some(1800), // 30 minutes
            idle_timeout_secs: Some(600),  // 10 minutes
        }
    }
}

/// Query performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub query_name: String,
    pub execution_count: i64,
    pub total_time_ms: i64,
    pub avg_time_ms: f64,
    pub min_time_ms: i64,
    pub max_time_ms: i64,
}

/// Index usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub table_name: String,
    pub index_name: String,
    pub cardinality: i64,
    pub unique_values: bool,
}

/// Query execution plan information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub id: i32,
    pub select_type: String,
    pub table: Option<String>,
    pub query_type: Option<String>,
    pub possible_keys: Option<String>,
    pub key_used: Option<String>,
    pub key_length: Option<String>,
    pub rows_examined: Option<i64>,
    pub filtered: Option<f64>,
    pub extra: Option<String>,
}

/// Migration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationInfo {
    pub version: String,
    pub name: String,
    pub applied_at: DateTime<Utc>,
}

/// Connection pool diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionDiagnostics {
    pub total_connections: u32,
    pub idle_connections: u32,
    pub active_connections: u32,
    pub max_connections: u32,
    pub connection_wait_time_ms: Option<i64>,
    pub pool_utilization_percent: f64,
}

/// Performance metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub timestamp: DateTime<Utc>,
    pub tasks_per_second: f64,
    pub avg_dequeue_time_ms: f64,
    pub avg_enqueue_time_ms: f64,
    pub queue_depth: i64,
    pub processing_tasks: i64,
    pub dlq_size: i64,
    pub connection_pool: ConnectionDiagnostics,
}

/// Task chain builder for creating dependent task sequences
#[derive(Debug, Clone)]
pub struct TaskChain {
    tasks: Vec<SerializedTask>,
    delay_between_secs: Option<u64>,
}

impl TaskChain {
    /// Create a new task chain
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            delay_between_secs: None,
        }
    }

    /// Add a task to the chain
    pub fn then(mut self, task: SerializedTask) -> Self {
        self.tasks.push(task);
        self
    }

    /// Set delay between tasks in the chain (in seconds)
    pub fn with_delay(mut self, delay_secs: u64) -> Self {
        self.delay_between_secs = Some(delay_secs);
        self
    }

    /// Get the tasks in the chain
    pub fn tasks(&self) -> &[SerializedTask] {
        &self.tasks
    }

    /// Get the delay between tasks
    pub fn delay_between_secs(&self) -> Option<u64> {
        self.delay_between_secs
    }
}

impl Default for TaskChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker statistics for monitoring distributed workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatistics {
    pub worker_id: String,
    pub active_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub last_seen: DateTime<Utc>,
    pub avg_task_duration_secs: f64,
}

/// Task age distribution for queue health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgeDistribution {
    pub bucket_label: String,
    pub task_count: i64,
    pub oldest_task_age_secs: i64,
}

/// Retry statistics for understanding task failure patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryStatistics {
    pub task_name: String,
    pub total_retries: i64,
    pub unique_tasks: i64,
    pub avg_retries_per_task: f64,
    pub max_retries_observed: i32,
}

/// Queue health summary combining multiple metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealth {
    pub overall_status: String, // "healthy", "degraded", "critical"
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub oldest_pending_age_secs: i64,
    pub active_workers: i64,
    pub queue_backlog_minutes: f64,
}

/// Task throughput metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskThroughput {
    pub completed_last_minute: i64,
    pub completed_last_hour: i64,
    pub failed_last_minute: i64,
    pub failed_last_hour: i64,
    pub tasks_per_second: f64,
}

/// Dead Letter Queue statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStatistics {
    pub total_tasks: i64,
    pub by_task_name: Vec<DlqTaskStats>,
}

/// DLQ statistics per task name
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqTaskStats {
    pub task_name: String,
    pub count: i64,
    pub avg_retries: Option<f64>,
    pub max_retries: i32,
}

/// Task progress information for long-running tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgress {
    pub task_id: Uuid,
    pub progress_percent: f64,
    pub current_step: Option<String>,
    pub total_steps: Option<i32>,
    pub updated_at: DateTime<Utc>,
}

/// Rate limit configuration per task type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    pub task_name: String,
    pub max_per_second: f64,
    pub max_per_minute: i64,
    pub max_per_hour: i64,
}

/// Rate limit status showing current usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitStatus {
    pub task_name: String,
    pub current_per_second: f64,
    pub current_per_minute: i64,
    pub current_per_hour: i64,
    pub limit_exceeded: bool,
}

/// Recurring task schedule configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecurringTaskConfig {
    pub task_name: String,
    pub schedule: RecurringSchedule,
    pub payload: Vec<u8>,
    pub priority: i32,
    pub enabled: bool,
    pub last_run: Option<DateTime<Utc>>,
    pub next_run: DateTime<Utc>,
}

/// Recurring schedule types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecurringSchedule {
    /// Run every N seconds
    EverySeconds(u64),
    /// Run every N minutes
    EveryMinutes(u64),
    /// Run every N hours
    EveryHours(u64),
    /// Run every N days at specific time (hour, minute)
    EveryDays(u64, u32, u32),
    /// Run on specific day of week (0=Sunday) at specific time
    Weekly(u32, u32, u32),
    /// Run on specific day of month at specific time
    Monthly(u32, u32, u32),
}

impl RecurringSchedule {
    /// Calculate next run time from a given timestamp
    pub fn next_run_from(&self, from: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            RecurringSchedule::EverySeconds(secs) => from + chrono::Duration::seconds(*secs as i64),
            RecurringSchedule::EveryMinutes(mins) => from + chrono::Duration::minutes(*mins as i64),
            RecurringSchedule::EveryHours(hours) => from + chrono::Duration::hours(*hours as i64),
            RecurringSchedule::EveryDays(days, hour, minute) => {
                let mut next = from + chrono::Duration::days(*days as i64);
                next = next
                    .with_hour(*hour)
                    .and_then(|dt| dt.with_minute(*minute))
                    .and_then(|dt| dt.with_second(0))
                    .unwrap_or(next);
                if next <= from {
                    next += chrono::Duration::days(1);
                }
                next
            }
            RecurringSchedule::Weekly(day_of_week, hour, minute) => {
                let mut next = from;
                let current_weekday = from.weekday().num_days_from_sunday();
                let days_until = ((*day_of_week + 7 - current_weekday) % 7) as i64;
                next += chrono::Duration::days(if days_until == 0 { 7 } else { days_until });
                next = next
                    .with_hour(*hour)
                    .and_then(|dt| dt.with_minute(*minute))
                    .and_then(|dt| dt.with_second(0))
                    .unwrap_or(next);
                next
            }
            RecurringSchedule::Monthly(day, hour, minute) => {
                let mut next = from;
                if let Some(dt) = next
                    .with_day(*day)
                    .and_then(|dt| dt.with_hour(*hour))
                    .and_then(|dt| dt.with_minute(*minute))
                    .and_then(|dt| dt.with_second(0))
                {
                    next = dt;
                    if next <= from {
                        // Move to next month
                        next += chrono::Duration::days(30);
                        next = next
                            .with_day(*day)
                            .and_then(|dt| dt.with_hour(*hour))
                            .and_then(|dt| dt.with_minute(*minute))
                            .and_then(|dt| dt.with_second(0))
                            .unwrap_or(next);
                    }
                }
                next
            }
        }
    }
}

/// Advanced retry policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Retry strategy
    pub strategy: RetryStrategy,
}

/// Retry strategy types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryStrategy {
    /// Fixed delay between retries (seconds)
    Fixed(u64),
    /// Linear backoff: delay = attempt * base_delay
    Linear { base_delay_secs: u64 },
    /// Exponential backoff: delay = base * (multiplier ^ attempt)
    Exponential {
        base_delay_secs: u64,
        multiplier: f64,
        max_delay_secs: u64,
    },
    /// Exponential backoff with jitter to avoid thundering herd
    ExponentialWithJitter {
        base_delay_secs: u64,
        multiplier: f64,
        max_delay_secs: u64,
    },
}

impl RetryStrategy {
    /// Calculate delay in seconds for a given retry attempt
    pub fn calculate_delay(&self, attempt: u32) -> u64 {
        match self {
            RetryStrategy::Fixed(delay) => *delay,
            RetryStrategy::Linear { base_delay_secs } => base_delay_secs * (attempt as u64 + 1),
            RetryStrategy::Exponential {
                base_delay_secs,
                multiplier,
                max_delay_secs,
            } => {
                let delay = (*base_delay_secs as f64) * multiplier.powi(attempt as i32);
                delay.min(*max_delay_secs as f64) as u64
            }
            RetryStrategy::ExponentialWithJitter {
                base_delay_secs,
                multiplier,
                max_delay_secs,
            } => {
                let delay = (*base_delay_secs as f64) * multiplier.powi(attempt as i32);
                let max_delay = delay.min(*max_delay_secs as f64);
                // Add random jitter (0-25% of delay)
                let jitter = (max_delay * 0.25 * (attempt as f64 % 1.0).abs()) as u64;
                (max_delay as u64).saturating_sub(jitter)
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            strategy: RetryStrategy::ExponentialWithJitter {
                base_delay_secs: 1,
                multiplier: 2.0,
                max_delay_secs: 300, // 5 minutes max
            },
        }
    }
}
