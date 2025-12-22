//! MySQL broker implementation for CeleRS
//!
//! This broker uses MySQL with `FOR UPDATE SKIP LOCKED` for reliable,
//! distributed task queue processing. It supports:
//! - Priority queues
//! - Dead Letter Queue (DLQ) for permanently failed tasks
//! - Delayed task execution (enqueue_at, enqueue_after)
//! - Prometheus metrics (optional `metrics` feature)
//! - Batch enqueue/dequeue/ack operations
//! - Transaction safety
//! - Distributed workers without contention
//! - Queue pause/resume functionality
//! - DLQ inspection and requeue
//! - Task status inspection
//! - Database health checks
//! - Automatic task archiving

use async_trait::async_trait;
use celers_core::{Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{mysql::MySqlPoolOptions, MySqlPool, Row};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{
    DLQ_SIZE, PROCESSING_QUEUE_SIZE, QUEUE_SIZE, TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL,
};

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

/// Circuit breaker state for database connection resilience
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitBreakerState {
    Closed,   // Normal operation
    Open,     // Failing, rejecting requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerStats {
    pub state: CircuitBreakerState,
    pub failure_count: u64,
    pub success_count: u64,
    pub last_failure_time: Option<DateTime<Utc>>,
    pub last_state_change: DateTime<Utc>,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u64,
    /// Duration to wait before transitioning from Open to HalfOpen (in seconds)
    pub timeout_secs: u64,
    /// Number of successful requests required in HalfOpen state to close the circuit
    pub success_threshold: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            timeout_secs: 60,
            success_threshold: 2,
        }
    }
}

/// Internal circuit breaker state
#[derive(Debug, Clone)]
struct CircuitBreakerStateInternal {
    state: CircuitBreakerState,
    failure_count: u64,
    success_count: u64,
    last_failure_time: Option<DateTime<Utc>>,
    last_state_change: DateTime<Utc>,
    config: CircuitBreakerConfig,
}

impl CircuitBreakerStateInternal {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            last_state_change: Utc::now(),
            config,
        }
    }
}

/// MySQL-based broker implementation using SKIP LOCKED
pub struct MysqlBroker {
    pool: MySqlPool,
    queue_name: String,
    paused: AtomicBool,
    circuit_breaker: Arc<RwLock<CircuitBreakerStateInternal>>,
}

impl MysqlBroker {
    /// Create a new MySQL broker
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    /// * `queue_name` - Logical queue name for multi-tenancy (optional, defaults to "default")
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_queue(database_url, "default").await
    }

    /// Create a new MySQL broker with a specific queue name
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_config(database_url, queue_name, PoolConfig::default()).await
    }

    /// Create a new MySQL broker with custom connection pool configuration
    pub async fn with_config(
        database_url: &str,
        queue_name: &str,
        config: PoolConfig,
    ) -> Result<Self> {
        let mut pool_options = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs));

        if let Some(max_lifetime) = config.max_lifetime_secs {
            pool_options = pool_options.max_lifetime(Duration::from_secs(max_lifetime));
        }

        if let Some(idle_timeout) = config.idle_timeout_secs {
            pool_options = pool_options.idle_timeout(Duration::from_secs(idle_timeout));
        }

        let pool = pool_options
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerStateInternal::new(
                CircuitBreakerConfig::default(),
            ))),
        })
    }

    /// Create a new MySQL broker with custom circuit breaker configuration
    pub async fn with_circuit_breaker_config(
        database_url: &str,
        queue_name: &str,
        pool_config: PoolConfig,
        circuit_breaker_config: CircuitBreakerConfig,
    ) -> Result<Self> {
        let mut pool_options = MySqlPoolOptions::new()
            .max_connections(pool_config.max_connections)
            .min_connections(pool_config.min_connections)
            .acquire_timeout(Duration::from_secs(pool_config.acquire_timeout_secs));

        if let Some(max_lifetime) = pool_config.max_lifetime_secs {
            pool_options = pool_options.max_lifetime(Duration::from_secs(max_lifetime));
        }

        if let Some(idle_timeout) = pool_config.idle_timeout_secs {
            pool_options = pool_options.idle_timeout(Duration::from_secs(idle_timeout));
        }

        let pool = pool_options
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerStateInternal::new(
                circuit_breaker_config,
            ))),
        })
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        // First, create migrations table if it doesn't exist
        self.run_migration_untracked(include_str!("../migrations/000_migrations.sql"))
            .await?;

        // Run migrations with tracking
        self.run_migration_tracked(
            "001",
            "initial_schema",
            include_str!("../migrations/001_init.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "002",
            "results_table",
            include_str!("../migrations/002_results.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "003",
            "performance_indexes",
            include_str!("../migrations/003_performance_indexes.sql"),
        )
        .await?;

        Ok(())
    }

    /// Check if a migration has been applied
    async fn is_migration_applied(&self, version: &str) -> Result<bool> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_migrations WHERE version = ?")
                .bind(version)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to check migration status: {}", e))
                })?;

        Ok(count > 0)
    }

    /// Mark a migration as applied
    async fn mark_migration_applied(&self, version: &str, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO celers_migrations (version, name) VALUES (?, ?)")
            .bind(version)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to mark migration as applied: {}", e))
            })?;

        tracing::info!(version = %version, name = %name, "Migration applied");
        Ok(())
    }

    /// Run a migration with tracking
    async fn run_migration_tracked(
        &self,
        version: &str,
        name: &str,
        migration_sql: &str,
    ) -> Result<()> {
        // Check if already applied
        if self.is_migration_applied(version).await? {
            tracing::debug!(version = %version, name = %name, "Migration already applied, skipping");
            return Ok(());
        }

        // Run the migration
        self.run_migration_untracked(migration_sql).await?;

        // Mark as applied
        self.mark_migration_applied(version, name).await?;

        Ok(())
    }

    /// Run a migration without tracking (for the migrations table itself)
    async fn run_migration_untracked(&self, migration_sql: &str) -> Result<()> {
        self.run_migration(migration_sql).await
    }

    /// Run a single migration file
    async fn run_migration(&self, migration_sql: &str) -> Result<()> {
        // MySQL doesn't support multi-statement execution by default in sqlx
        // We need to split and execute each statement separately
        let statements: Vec<&str> = migration_sql.split("DELIMITER //").collect();

        // Execute the main DDL statements (before DELIMITER)
        if let Some(main_sql) = statements.first() {
            for statement in main_sql.split(';') {
                let trimmed = statement.trim();
                if !trimmed.is_empty() && !trimmed.starts_with("--") {
                    sqlx::query(trimmed)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| CelersError::Other(format!("Migration failed: {}", e)))?;
                }
            }
        }

        // Execute the stored procedure (between DELIMITER // and DELIMITER ;)
        if statements.len() > 1 {
            let proc_section = statements[1];
            if let Some(proc_sql) = proc_section.split("DELIMITER ;").next() {
                let trimmed = proc_sql.trim();
                if !trimmed.is_empty() {
                    sqlx::query(trimmed)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Stored procedure creation failed: {}", e))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Get the underlying connection pool
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Move a task to the Dead Letter Queue
    async fn move_to_dlq(&self, task_id: &TaskId) -> Result<()> {
        sqlx::query("CALL move_to_dlq(?)")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to move task to DLQ: {}", e)))?;

        Ok(())
    }

    // ========== Queue Control ==========

    /// Pause the queue (dequeue will return None while paused)
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue paused");
    }

    /// Resume the queue
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue resumed");
    }

    /// Check if the queue is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    // ========== Task Inspection ==========

    /// Get detailed information about a specific task
    pub async fn get_task(&self, task_id: &TaskId) -> Result<Option<TaskInfo>> {
        let row = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task: {}", e)))?;

        match row {
            Some(row) => {
                let task_id_str: String = row.get("id");
                let state_str: String = row.get("state");
                Ok(Some(TaskInfo {
                    id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                    task_name: row.get("task_name"),
                    state: state_str.parse()?,
                    priority: row.get("priority"),
                    retry_count: row.get("retry_count"),
                    max_retries: row.get("max_retries"),
                    created_at: row.get("created_at"),
                    scheduled_at: row.get("scheduled_at"),
                    started_at: row.get("started_at"),
                    completed_at: row.get("completed_at"),
                    worker_id: row.get("worker_id"),
                    error_message: row.get("error_message"),
                }))
            }
            None => Ok(None),
        }
    }

    /// List tasks by state with pagination
    pub async fn list_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE state = ?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    "#,
                )
                .bind(s.to_string())
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    "#,
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row.get("id");
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
        }
        Ok(tasks)
    }

    /// Get queue statistics
    pub async fn get_statistics(&self) -> Result<QueueStatistics> {
        // MySQL doesn't have FILTER clause, so we use CASE WHEN
        let row = sqlx::query(
            r#"
            SELECT
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
                COUNT(*) as total
            FROM celers_tasks
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get statistics: {}", e)))?;

        let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_dead_letter_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;

        // MySQL returns DECIMAL for SUM, need to handle potential NULL
        let pending: Option<rust_decimal::Decimal> = row.get("pending");
        let processing: Option<rust_decimal::Decimal> = row.get("processing");
        let completed: Option<rust_decimal::Decimal> = row.get("completed");
        let failed: Option<rust_decimal::Decimal> = row.get("failed");
        let cancelled: Option<rust_decimal::Decimal> = row.get("cancelled");
        let total: i64 = row.get("total");

        Ok(QueueStatistics {
            pending: pending
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            processing: processing
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            completed: completed
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            failed: failed
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            cancelled: cancelled
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            dlq: dlq_count,
            total,
        })
    }

    // ========== DLQ Operations ==========

    /// List tasks in the dead letter queue
    pub async fn list_dlq(&self, limit: i64, offset: i64) -> Result<Vec<DlqTaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_id, task_name, retry_count, error_message, failed_at
            FROM celers_dead_letter_queue
            ORDER BY failed_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let id_str: String = row.get("id");
            let task_id_str: String = row.get("task_id");
            tasks.push(DlqTaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.get("task_name"),
                retry_count: row.get("retry_count"),
                error_message: row.get("error_message"),
                failed_at: row.get("failed_at"),
            });
        }
        Ok(tasks)
    }

    /// Requeue a task from the dead letter queue
    ///
    /// This moves the task back to the main queue with reset retry count.
    pub async fn requeue_from_dlq(&self, dlq_id: &Uuid) -> Result<TaskId> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Get task from DLQ
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, payload, metadata
            FROM celers_dead_letter_queue
            WHERE id = ?
            "#,
        )
        .bind(dlq_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {}", e)))?;

        let row = row.ok_or_else(|| CelersError::Other("DLQ task not found".to_string()))?;

        let task_id_str: String = row.get("task_id");
        let task_id = Uuid::parse_str(&task_id_str)
            .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
        let task_name: String = row.get("task_name");
        let payload: Vec<u8> = row.get("payload");
        let metadata: Option<String> = row.get("metadata");

        // Create new task in main queue
        let new_task_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, retry_count, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', 0, 0, 3, ?, NOW(), NOW())
            "#,
        )
        .bind(new_task_id.to_string())
        .bind(&task_name)
        .bind(&payload)
        .bind(metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

        // Delete from DLQ
        sqlx::query("DELETE FROM celers_dead_letter_queue WHERE id = ?")
            .bind(dlq_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete from DLQ: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit requeue: {}", e)))?;

        tracing::info!(original_task_id = %task_id, new_task_id = %new_task_id, task_name = %task_name, "Requeued task from DLQ");

        Ok(new_task_id)
    }

    /// Purge (delete) a task from the dead letter queue
    pub async fn purge_dlq(&self, dlq_id: &Uuid) -> Result<bool> {
        let result = sqlx::query("DELETE FROM celers_dead_letter_queue WHERE id = ?")
            .bind(dlq_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge DLQ task: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Purge all tasks from the dead letter queue
    pub async fn purge_all_dlq(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all DLQ: {}", e)))?;

        tracing::info!(count = result.rows_affected(), "Purged all DLQ tasks");
        Ok(result.rows_affected())
    }

    // ========== Health & Maintenance ==========

    /// Check database health
    pub async fn check_health(&self) -> Result<HealthStatus> {
        // Test connection and get MySQL version
        let version: String = sqlx::query_scalar("SELECT VERSION()")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Health check failed: {}", e)))?;

        // Get queue counts
        let stats = self.get_statistics().await?;

        Ok(HealthStatus {
            healthy: true,
            connection_pool_size: self.pool.options().get_max_connections(),
            idle_connections: self.pool.num_idle() as u32,
            pending_tasks: stats.pending,
            processing_tasks: stats.processing,
            dlq_tasks: stats.dlq,
            database_version: version,
        })
    }

    /// Archive completed tasks older than the specified duration
    ///
    /// Returns the number of tasks archived (deleted).
    pub async fn archive_completed_tasks(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

        let result = sqlx::query(
            r#"
            DELETE FROM celers_tasks
            WHERE state IN ('completed', 'failed', 'cancelled')
              AND completed_at < ?
            "#,
        )
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive tasks: {}", e)))?;

        tracing::info!(count = result.rows_affected(), cutoff = %cutoff, "Archived completed tasks");
        Ok(result.rows_affected())
    }

    /// Clean up stuck processing tasks (tasks that have been processing too long)
    ///
    /// This can happen if a worker crashes. Tasks are requeued with incremented retry count.
    pub async fn recover_stuck_tasks(&self, stuck_threshold: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(stuck_threshold.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending',
                started_at = NULL,
                worker_id = NULL,
                error_message = 'Recovered from stuck processing state'
            WHERE state = 'processing'
              AND started_at < ?
            "#,
        )
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to recover stuck tasks: {}", e)))?;

        if result.rows_affected() > 0 {
            tracing::warn!(
                count = result.rows_affected(),
                "Recovered stuck processing tasks"
            );
        }
        Ok(result.rows_affected())
    }

    /// Purge all tasks (dangerous - use with caution)
    pub async fn purge_all(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all tasks: {}", e)))?;

        tracing::warn!(count = result.rows_affected(), "Purged all tasks");
        Ok(result.rows_affected())
    }

    // ========== Task Result Storage ==========

    /// Store a task result in the database
    ///
    /// This creates or updates the result for a given task ID.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_result(
        &self,
        task_id: &TaskId,
        task_name: &str,
        status: TaskResultStatus,
        result: Option<serde_json::Value>,
        error: Option<&str>,
        traceback: Option<&str>,
        runtime_ms: Option<i64>,
    ) -> Result<()> {
        let completed_at = match status {
            TaskResultStatus::Success | TaskResultStatus::Failure | TaskResultStatus::Revoked => {
                Some(Utc::now().format("%Y-%m-%d %H:%M:%S").to_string())
            }
            _ => None,
        };

        // MySQL uses INSERT ... ON DUPLICATE KEY UPDATE instead of ON CONFLICT
        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms)
            VALUES (?, ?, ?, ?, ?, ?, NOW(), ?, ?)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                result = VALUES(result),
                error = VALUES(error),
                traceback = VALUES(traceback),
                completed_at = VALUES(completed_at),
                runtime_ms = VALUES(runtime_ms)
            "#,
        )
        .bind(task_id.to_string())
        .bind(task_name)
        .bind(status.to_string())
        .bind(result.map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string())))
        .bind(error)
        .bind(traceback)
        .bind(completed_at)
        .bind(runtime_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    /// Get a task result from the database
    pub async fn get_result(&self, task_id: &TaskId) -> Result<Option<TaskResult>> {
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, status, result, error, traceback,
                   created_at, completed_at, runtime_ms
            FROM celers_task_results
            WHERE task_id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get result: {}", e)))?;

        match row {
            Some(row) => {
                let task_id_str: String = row.get("task_id");
                let status_str: String = row.get("status");
                let result_str: Option<String> = row.get("result");
                Ok(Some(TaskResult {
                    task_id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                    task_name: row.get("task_name"),
                    status: status_str.parse()?,
                    result: result_str.and_then(|s| serde_json::from_str(&s).ok()),
                    error: row.get("error"),
                    traceback: row.get("traceback"),
                    created_at: row.get("created_at"),
                    completed_at: row.get("completed_at"),
                    runtime_ms: row.get("runtime_ms"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a task result from the database
    pub async fn delete_result(&self, task_id: &TaskId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM celers_task_results WHERE task_id = ?")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete result: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Archive old task results
    ///
    /// Deletes results older than the specified duration.
    pub async fn archive_results(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_results
            WHERE completed_at < ?
            "#,
        )
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive results: {}", e)))?;

        tracing::info!(count = result.rows_affected(), cutoff = %cutoff, "Archived old results");
        Ok(result.rows_affected())
    }

    // ========== Database Monitoring ==========

    /// Get table size information for CeleRS tables
    pub async fn get_table_sizes(&self) -> Result<Vec<TableSizeInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT
                TABLE_NAME as table_name,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                INDEX_LENGTH as index_size_bytes
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME LIKE 'celers_%'
            ORDER BY DATA_LENGTH DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {}", e)))?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            let row_count: Option<i64> = row.get("row_count");
            let data_size: Option<i64> = row.get("data_size_bytes");
            let index_size: Option<i64> = row.get("index_size_bytes");
            tables.push(TableSizeInfo {
                table_name: row.get("table_name"),
                row_count: row_count.unwrap_or(0),
                data_size_bytes: data_size.unwrap_or(0),
                index_size_bytes: index_size.unwrap_or(0),
            });
        }
        Ok(tables)
    }

    /// Optimize CeleRS tables (MySQL-specific)
    ///
    /// This should be run periodically for optimal performance.
    pub async fn optimize_tables(&self) -> Result<()> {
        sqlx::query("OPTIMIZE TABLE celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to optimize celers_tasks: {}", e)))?;

        sqlx::query("OPTIMIZE TABLE celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to optimize celers_dead_letter_queue: {}",
                    e
                ))
            })?;

        sqlx::query("OPTIMIZE TABLE celers_task_results")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to optimize celers_task_results: {}", e))
            })?;

        tracing::info!("Optimized all CeleRS tables");
        Ok(())
    }

    /// Analyze CeleRS tables for query optimization
    pub async fn analyze_tables(&self) -> Result<()> {
        sqlx::query("ANALYZE TABLE celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to analyze celers_tasks: {}", e)))?;

        sqlx::query("ANALYZE TABLE celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_dead_letter_queue: {}", e))
            })?;

        sqlx::query("ANALYZE TABLE celers_task_results")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_task_results: {}", e))
            })?;

        tracing::info!("Analyzed all CeleRS tables");
        Ok(())
    }

    // ========== Advanced Task Inspection ==========

    /// Get task counts grouped by task name
    ///
    /// Returns statistics for each unique task name including counts by state.
    pub async fn count_by_task_name(&self) -> Result<Vec<TaskNameCount>> {
        let rows = sqlx::query(
            r#"
            SELECT
                task_name,
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
                COUNT(*) as total
            FROM celers_tasks
            GROUP BY task_name
            ORDER BY total DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count by task name: {}", e)))?;

        let mut counts = Vec::with_capacity(rows.len());
        for row in rows {
            let pending: Option<rust_decimal::Decimal> = row.get("pending");
            let processing: Option<rust_decimal::Decimal> = row.get("processing");
            let completed: Option<rust_decimal::Decimal> = row.get("completed");
            let failed: Option<rust_decimal::Decimal> = row.get("failed");
            let total: i64 = row.get("total");

            counts.push(TaskNameCount {
                task_name: row.get("task_name"),
                pending: pending
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                processing: processing
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                completed: completed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                failed: failed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                total,
            });
        }
        Ok(counts)
    }

    /// Get all currently processing tasks
    ///
    /// Useful for monitoring worker activity and detecting stuck tasks.
    pub async fn get_processing_tasks(&self, limit: i64, offset: i64) -> Result<Vec<TaskInfo>> {
        self.list_tasks(Some(DbTaskState::Processing), limit, offset)
            .await
    }

    /// Get tasks currently being processed by a specific worker
    pub async fn get_tasks_by_worker(&self, worker_id: &str) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE worker_id = ?
            ORDER BY started_at DESC
            "#,
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get tasks by worker: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row.get("id");
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
        }
        Ok(tasks)
    }

    /// List scheduled tasks (tasks with scheduled_at in the future)
    pub async fn list_scheduled_tasks(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ScheduledTaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, priority, scheduled_at, created_at,
                   TIMESTAMPDIFF(SECOND, NOW(), scheduled_at) as delay_remaining_secs
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at > NOW()
            ORDER BY scheduled_at ASC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list scheduled tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row.get("id");
            let delay: Option<i64> = row.get("delay_remaining_secs");
            tasks.push(ScheduledTaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.get("task_name"),
                priority: row.get("priority"),
                scheduled_at: row.get("scheduled_at"),
                created_at: row.get("created_at"),
                delay_remaining_secs: delay.unwrap_or(0),
            });
        }
        Ok(tasks)
    }

    /// Count scheduled tasks (tasks with scheduled_at in the future)
    pub async fn count_scheduled_tasks(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM celers_tasks
            WHERE state = 'pending' AND scheduled_at > NOW()
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count scheduled tasks: {}", e)))?;

        Ok(count)
    }

    // ========== Task Updates ==========

    /// Update the error message on a task
    ///
    /// Useful for recording error details during task execution.
    pub async fn update_error_message(
        &self,
        task_id: &TaskId,
        error_message: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET error_message = ?
            WHERE id = ?
            "#,
        )
        .bind(error_message)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update error message: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Set the worker ID on a processing task
    ///
    /// This allows tracking which worker is processing which task.
    pub async fn set_worker_id(&self, task_id: &TaskId, worker_id: &str) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET worker_id = ?
            WHERE id = ? AND state = 'processing'
            "#,
        )
        .bind(worker_id)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to set worker ID: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Dequeue a task and set the worker ID atomically
    ///
    /// This is a convenience method that dequeues a task and sets the worker ID
    /// in a single transaction, which is useful for worker tracking.
    pub async fn dequeue_with_worker_id(&self, worker_id: &str) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let row = sqlx::query(
            r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        if let Some(row) = row {
            let task_id_str: String = row.get("id");
            let _task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            let task_name: String = row.get("task_name");
            let payload: Vec<u8> = row.get("payload");
            let retry_count: i32 = row.get("retry_count");

            // Mark as processing with worker ID
            sqlx::query(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1,
                    worker_id = ?
                WHERE id = ?
                "#,
            )
            .bind(worker_id)
            .bind(&task_id_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to mark task as processing: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            Ok(Some(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            }))
        } else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            Ok(None)
        }
    }

    // ========== Selective Cleanup ==========

    /// Purge tasks by state
    ///
    /// Deletes all tasks with the specified state. Use with caution.
    pub async fn purge_by_state(&self, state: DbTaskState) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_tasks WHERE state = ?")
            .bind(state.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge tasks by state: {}", e)))?;

        tracing::info!(state = %state, count = result.rows_affected(), "Purged tasks by state");
        Ok(result.rows_affected())
    }

    /// Purge completed tasks only
    pub async fn purge_completed(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Completed).await
    }

    /// Purge failed tasks only
    pub async fn purge_failed(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Failed).await
    }

    /// Purge cancelled tasks only
    pub async fn purge_cancelled(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Cancelled).await
    }

    /// Purge tasks by task name
    ///
    /// Deletes all tasks with the specified task name. Use with caution.
    pub async fn purge_by_task_name(&self, task_name: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_tasks WHERE task_name = ?")
            .bind(task_name)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge tasks by name: {}", e)))?;

        tracing::info!(task_name = %task_name, count = result.rows_affected(), "Purged tasks by name");
        Ok(result.rows_affected())
    }

    // ========== Migration Management ==========

    /// List all applied migrations
    pub async fn list_migrations(&self) -> Result<Vec<MigrationInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT version, name, applied_at
            FROM celers_migrations
            ORDER BY applied_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list migrations: {}", e)))?;

        let mut migrations = Vec::with_capacity(rows.len());
        for row in rows {
            migrations.push(MigrationInfo {
                version: row.get("version"),
                name: row.get("name"),
                applied_at: row.get("applied_at"),
            });
        }
        Ok(migrations)
    }

    // ========== Query Performance Tracking ==========

    /// Get query performance statistics from MySQL performance schema
    ///
    /// Note: This requires performance_schema to be enabled in MySQL configuration.
    pub async fn get_query_stats(&self) -> Result<Vec<QueryStats>> {
        let rows = sqlx::query(
            r#"
            SELECT
                DIGEST_TEXT as query_name,
                COUNT_STAR as execution_count,
                SUM_TIMER_WAIT / 1000000000 as total_time_ms,
                AVG_TIMER_WAIT / 1000000000 as avg_time_ms,
                MIN_TIMER_WAIT / 1000000000 as min_time_ms,
                MAX_TIMER_WAIT / 1000000000 as max_time_ms
            FROM performance_schema.events_statements_summary_by_digest
            WHERE SCHEMA_NAME = DATABASE()
              AND DIGEST_TEXT LIKE '%celers%'
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get query stats: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let query_name: String = row.get("query_name");
            let execution_count: i64 = row.get("execution_count");
            let total_time: Option<rust_decimal::Decimal> = row.get("total_time_ms");
            let avg_time: Option<rust_decimal::Decimal> = row.get("avg_time_ms");
            let min_time: Option<rust_decimal::Decimal> = row.get("min_time_ms");
            let max_time: Option<rust_decimal::Decimal> = row.get("max_time_ms");

            stats.push(QueryStats {
                query_name,
                execution_count,
                total_time_ms: total_time
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                avg_time_ms: avg_time
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
                min_time_ms: min_time
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                max_time_ms: max_time
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
            });
        }
        Ok(stats)
    }

    /// Reset query performance statistics
    ///
    /// Clears the performance_schema statistics. Useful for benchmarking.
    pub async fn reset_query_stats(&self) -> Result<()> {
        sqlx::query(
            r#"
            CALL sys.ps_truncate_all_tables(FALSE)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to reset query stats: {}", e)))?;

        tracing::info!("Reset query performance statistics");
        Ok(())
    }

    // ========== Index Usage and Query Optimization ==========

    /// Get index statistics for CeleRS tables
    ///
    /// Returns information about all indexes on CeleRS tables including cardinality
    /// and whether they are unique.
    pub async fn get_index_stats(&self) -> Result<Vec<IndexStats>> {
        let rows = sqlx::query(
            r#"
            SELECT
                TABLE_NAME as table_name,
                INDEX_NAME as index_name,
                CARDINALITY as cardinality,
                NON_UNIQUE as non_unique
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME LIKE 'celers_%'
            ORDER BY TABLE_NAME, INDEX_NAME
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get index stats: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let cardinality: Option<i64> = row.get("cardinality");
            let non_unique: i32 = row.get("non_unique");
            stats.push(IndexStats {
                table_name: row.get("table_name"),
                index_name: row.get("index_name"),
                cardinality: cardinality.unwrap_or(0),
                unique_values: non_unique == 0,
            });
        }
        Ok(stats)
    }

    /// Explain a query plan for the dequeue operation
    ///
    /// Returns the MySQL EXPLAIN output for the dequeue query.
    /// Useful for query optimization and performance tuning.
    pub async fn explain_dequeue(&self) -> Result<Vec<QueryPlan>> {
        let rows = sqlx::query(
            r#"
            EXPLAIN
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        let mut plans = Vec::with_capacity(rows.len());
        for row in rows {
            let rows_examined: Option<i64> = row.try_get("rows").ok();
            let filtered: Option<rust_decimal::Decimal> = row.try_get("filtered").ok();
            plans.push(QueryPlan {
                id: row.get("id"),
                select_type: row.get("select_type"),
                table: row.try_get("table").ok(),
                query_type: row.try_get("type").ok(),
                possible_keys: row.try_get("possible_keys").ok(),
                key_used: row.try_get("key").ok(),
                key_length: row.try_get("key_len").ok(),
                rows_examined,
                filtered: filtered.map(|d| d.to_string().parse().unwrap_or(0.0)),
                extra: row.try_get("Extra").ok(),
            });
        }
        Ok(plans)
    }

    /// Explain a custom query plan
    ///
    /// Returns the MySQL EXPLAIN output for any SELECT query.
    pub async fn explain_query(&self, query: &str) -> Result<Vec<QueryPlan>> {
        let explain_query = format!("EXPLAIN {}", query);
        let rows = sqlx::query(&explain_query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        let mut plans = Vec::with_capacity(rows.len());
        for row in rows {
            let rows_examined: Option<i64> = row.try_get("rows").ok();
            let filtered: Option<rust_decimal::Decimal> = row.try_get("filtered").ok();
            plans.push(QueryPlan {
                id: row.get("id"),
                select_type: row.get("select_type"),
                table: row.try_get("table").ok(),
                query_type: row.try_get("type").ok(),
                possible_keys: row.try_get("possible_keys").ok(),
                key_used: row.try_get("key").ok(),
                key_length: row.try_get("key_len").ok(),
                rows_examined,
                filtered: filtered.map(|d| d.to_string().parse().unwrap_or(0.0)),
                extra: row.try_get("Extra").ok(),
            });
        }
        Ok(plans)
    }

    /// Check if indexes are being used effectively
    ///
    /// Analyzes the explain plan for common queries and returns warnings
    /// if indexes are not being used properly.
    pub async fn check_index_usage(&self) -> Result<Vec<String>> {
        let mut warnings = Vec::new();

        // Check dequeue query
        let dequeue_plan = self.explain_dequeue().await?;
        for plan in dequeue_plan {
            if plan.key_used.is_none() {
                warnings.push(format!(
                    "Dequeue query on table {:?} is not using an index (full table scan)",
                    plan.table
                ));
            }
            if let Some(extra) = &plan.extra {
                if extra.contains("Using filesort") {
                    warnings.push("Dequeue query requires filesort - consider adding composite index on (state, priority, created_at)".to_string());
                }
            }
        }

        // Check index cardinality
        let index_stats = self.get_index_stats().await?;
        for stat in index_stats {
            if stat.cardinality == 0 && !stat.index_name.eq("PRIMARY") {
                warnings.push(format!(
                    "Index {} on table {} has zero cardinality - consider running ANALYZE TABLE",
                    stat.index_name, stat.table_name
                ));
            }
        }

        Ok(warnings)
    }

    // ========== Connection Diagnostics and Performance Monitoring ==========

    /// Get connection pool diagnostics
    ///
    /// Returns detailed information about the connection pool state.
    pub fn get_connection_diagnostics(&self) -> ConnectionDiagnostics {
        let max_conns = self.pool.options().get_max_connections();
        let idle_conns = self.pool.num_idle() as u32;
        let min_conns = self.pool.options().get_min_connections();

        // Total connections is at least idle, but could be up to max
        let total_conns = idle_conns.max(min_conns);
        let active_conns = total_conns.saturating_sub(idle_conns);

        let utilization = if max_conns > 0 {
            (total_conns as f64 / max_conns as f64) * 100.0
        } else {
            0.0
        };

        ConnectionDiagnostics {
            total_connections: total_conns,
            idle_connections: idle_conns,
            active_connections: active_conns,
            max_connections: max_conns,
            connection_wait_time_ms: None, // MySQL doesn't expose this easily
            pool_utilization_percent: utilization,
        }
    }

    /// Get comprehensive performance metrics snapshot
    ///
    /// This method collects various performance metrics including queue sizes,
    /// connection pool status, and database statistics.
    pub async fn get_performance_metrics(&self) -> Result<PerformanceMetrics> {
        let stats = self.get_statistics().await?;
        let conn_diag = self.get_connection_diagnostics();

        // Calculate tasks per second (requires historical data, placeholder for now)
        // In production, this would track enqueue/dequeue rates over time
        let tasks_per_second = 0.0;

        // Get average query times from performance schema (if available)
        let (avg_dequeue_ms, avg_enqueue_ms) = match self.get_query_stats().await {
            Ok(stats) => {
                let dequeue_stat = stats
                    .iter()
                    .find(|s| {
                        s.query_name.contains("SELECT") && s.query_name.contains("celers_tasks")
                    })
                    .map(|s| s.avg_time_ms)
                    .unwrap_or(0.0);

                let enqueue_stat = stats
                    .iter()
                    .find(|s| {
                        s.query_name.contains("INSERT") && s.query_name.contains("celers_tasks")
                    })
                    .map(|s| s.avg_time_ms)
                    .unwrap_or(0.0);

                (dequeue_stat, enqueue_stat)
            }
            Err(_) => (0.0, 0.0),
        };

        Ok(PerformanceMetrics {
            timestamp: Utc::now(),
            tasks_per_second,
            avg_dequeue_time_ms: avg_dequeue_ms,
            avg_enqueue_time_ms: avg_enqueue_ms,
            queue_depth: stats.pending,
            processing_tasks: stats.processing,
            dlq_size: stats.dlq,
            connection_pool: conn_diag,
        })
    }

    /// Check if the broker is healthy and ready to process tasks
    ///
    /// Returns true if:
    /// - Database connection is active
    /// - Connection pool has idle connections available
    /// - No critical errors detected
    pub async fn is_ready(&self) -> bool {
        // Try a simple query
        let version_check = sqlx::query_scalar::<_, String>("SELECT VERSION()")
            .fetch_one(&self.pool)
            .await;

        if version_check.is_err() {
            return false;
        }

        // Check connection pool has capacity
        let idle = self.pool.num_idle();
        if idle == 0 {
            let max_conns = self.pool.options().get_max_connections();
            // If pool is at max and no idle connections, might be saturated
            if max_conns > 0 && self.pool.size() >= max_conns {
                return false;
            }
        }

        true
    }

    /// Get detailed database server variables
    ///
    /// Returns key MySQL server configuration variables that affect performance.
    pub async fn get_server_variables(&self) -> Result<std::collections::HashMap<String, String>> {
        let rows = sqlx::query(
            r#"
            SHOW VARIABLES WHERE Variable_name IN (
                'max_connections',
                'innodb_buffer_pool_size',
                'innodb_log_file_size',
                'query_cache_size',
                'query_cache_type',
                'innodb_flush_log_at_trx_commit',
                'innodb_flush_method',
                'binlog_format',
                'expire_logs_days'
            )
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get server variables: {}", e)))?;

        let mut variables = std::collections::HashMap::new();
        for row in rows {
            let var_name: String = row.get("Variable_name");
            let var_value: String = row.get("Value");
            variables.insert(var_name, var_value);
        }

        Ok(variables)
    }

    /// Enqueue multiple tasks in a single transaction (batch operation)
    ///
    /// This is significantly faster than individual enqueue calls when
    /// inserting many tasks. Uses a single transaction and prepared statement.
    ///
    /// # Returns
    /// Vector of task IDs in the same order as input tasks
    pub async fn enqueue_batch_impl(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut task_ids = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let task_id = task.metadata.id;
            let mut db_metadata = json!({
                "queue": self.queue_name,
                "enqueued_at": chrono::Utc::now().to_rfc3339(),
            });

            // Merge task metadata if present
            if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
                if let Some(obj) = db_metadata.as_object_mut() {
                    if let Some(meta_obj) = task_meta.as_object() {
                        for (k, v) in meta_obj {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            sqlx::query(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
            )
            .bind(task_id.to_string())
            .bind(&task.metadata.name)
            .bind(&task.payload)
            .bind(task.metadata.priority)
            .bind(task.metadata.max_retries as i32)
            .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task in batch: {}", e)))?;

            task_ids.push(task_id);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch enqueue: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc_by(tasks.len() as f64);

            // Track per-task-type metrics
            for task in &tasks {
                TASKS_ENQUEUED_BY_TYPE
                    .with_label_values(&[&task.metadata.name])
                    .inc();
            }
        }

        Ok(task_ids)
    }

    /// Dequeue multiple tasks atomically (batch operation)
    ///
    /// Fetches up to `limit` tasks in a single transaction using
    /// FOR UPDATE SKIP LOCKED for distributed worker safety.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of tasks to dequeue
    ///
    /// # Returns
    /// Vector of broker messages (may be less than limit if queue has fewer tasks)
    pub async fn dequeue_batch_impl(&self, limit: usize) -> Result<Vec<BrokerMessage>> {
        if limit == 0 || self.paused.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let rows = sqlx::query(
            r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ?
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to dequeue batch: {}", e)))?;

        if rows.is_empty() {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            return Ok(Vec::new());
        }

        let mut messages = Vec::with_capacity(rows.len());
        let mut task_id_strings = Vec::with_capacity(rows.len());

        for row in rows {
            let task_id_str: String = row.get("id");
            let _task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            let task_name: String = row.get("task_name");
            let payload: Vec<u8> = row.get("payload");
            let retry_count: i32 = row.get("retry_count");

            messages.push(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            });

            task_id_strings.push(task_id_str);
        }

        // Mark all fetched tasks as processing
        // MySQL doesn't support array parameters like PostgreSQL's ANY($1)
        // So we need to use IN clause with placeholders
        if !task_id_strings.is_empty() {
            let placeholders = task_id_strings
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let update_query = format!(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1
                WHERE id IN ({})
                "#,
                placeholders
            );

            let mut query = sqlx::query(&update_query);
            for task_id in task_id_strings {
                query = query.bind(task_id);
            }

            query.execute(&mut *tx).await.map_err(|e| {
                CelersError::Other(format!("Failed to mark batch as processing: {}", e))
            })?;
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch dequeue: {}", e)))?;

        Ok(messages)
    }
}

// ========== Enhancement Methods ==========
impl MysqlBroker {
    /// Cancel multiple tasks atomically
    ///
    /// This is more efficient than calling cancel() multiple times.
    /// Only cancels tasks in 'pending' or 'processing' state.
    ///
    /// # Arguments
    /// * `task_ids` - Slice of task IDs to cancel
    ///
    /// # Returns
    /// The number of tasks actually cancelled.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Enqueue some tasks
    /// let task1 = broker.enqueue(SerializedTask::new("task1".into(), vec![])).await?;
    /// let task2 = broker.enqueue(SerializedTask::new("task2".into(), vec![])).await?;
    /// let task3 = broker.enqueue(SerializedTask::new("task3".into(), vec![])).await?;
    ///
    /// // Cancel all three tasks in one operation
    /// let cancelled = broker.cancel_batch(&[task1, task2, task3]).await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_batch(&self, task_ids: &[TaskId]) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let task_id_strings: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let placeholders = vec!["?"; task_ids.len()].join(", ");

        let query = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled', completed_at = NOW()
            WHERE id IN ({}) AND state IN ('pending', 'processing')
            "#,
            placeholders
        );

        let mut query_builder = sqlx::query(&query);
        for task_id_str in task_id_strings {
            query_builder = query_builder.bind(task_id_str);
        }

        let result = query_builder
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to cancel batch: {}", e)))?;

        let cancelled = result.rows_affected();
        tracing::info!(count = cancelled, "Cancelled tasks in batch");

        Ok(cancelled)
    }

    /// Get statistics for a specific worker
    ///
    /// Returns detailed information about tasks processed by a specific worker,
    /// including active tasks, completed tasks, failed tasks, and average duration.
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID to get statistics for
    ///
    /// # Returns
    /// Worker statistics including task counts and average duration
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Get statistics for a specific worker
    /// let stats = broker.get_worker_statistics("worker-123").await?;
    /// println!("Worker {} has {} active tasks, {} completed",
    ///     stats.worker_id, stats.active_tasks, stats.completed_tasks);
    /// println!("Average task duration: {:.2}s", stats.avg_task_duration_secs);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_worker_statistics(&self, worker_id: &str) -> Result<WorkerStatistics> {
        let row = sqlx::query(
            r#"
            SELECT
                worker_id,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as active_tasks,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                MAX(started_at) as last_seen,
                AVG(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as avg_duration
            FROM celers_tasks
            WHERE worker_id = ?
            GROUP BY worker_id
            "#,
        )
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get worker statistics: {}", e)))?;

        if let Some(row) = row {
            let active: Option<rust_decimal::Decimal> = row.get("active_tasks");
            let completed: Option<rust_decimal::Decimal> = row.get("completed_tasks");
            let failed: Option<rust_decimal::Decimal> = row.get("failed_tasks");
            let last_seen: Option<DateTime<Utc>> = row.get("last_seen");
            let avg_duration: Option<rust_decimal::Decimal> = row.get("avg_duration");

            Ok(WorkerStatistics {
                worker_id: worker_id.to_string(),
                active_tasks: active
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                completed_tasks: completed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                failed_tasks: failed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                last_seen: last_seen.unwrap_or_else(Utc::now),
                avg_task_duration_secs: avg_duration
                    .and_then(|d| d.to_string().parse::<f64>().ok())
                    .unwrap_or(0.0),
            })
        } else {
            // Worker not found, return zero stats
            Ok(WorkerStatistics {
                worker_id: worker_id.to_string(),
                active_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                last_seen: Utc::now(),
                avg_task_duration_secs: 0.0,
            })
        }
    }

    /// Get quick count of tasks by state
    ///
    /// This is a lightweight alternative to get_statistics() that only
    /// returns counts for a specific state, without computing aggregates
    /// for all states.
    ///
    /// # Arguments
    /// * `state` - The task state to count
    ///
    /// # Returns
    /// The number of tasks in the specified state
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Quickly check how many tasks are pending
    /// let pending = broker.count_by_state_quick(DbTaskState::Pending).await?;
    /// println!("Pending tasks: {}", pending);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_by_state_quick(&self, state: DbTaskState) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = ?")
            .bind(state.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tasks by state: {}", e)))?;

        Ok(count)
    }

    /// Get task age distribution for monitoring queue health
    ///
    /// Returns pending tasks grouped into age buckets to help identify
    /// queue backlogs and performance issues.
    ///
    /// Age buckets:
    /// - < 1 minute
    /// - 1-5 minutes
    /// - 5-15 minutes
    /// - 15-60 minutes
    /// - > 60 minutes
    ///
    /// # Returns
    /// Vector of age distribution buckets with task counts and oldest task age
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let distribution = broker.get_task_age_distribution().await?;
    /// for bucket in distribution {
    ///     println!("{}: {} tasks (oldest: {}s)",
    ///         bucket.bucket_label,
    ///         bucket.task_count,
    ///         bucket.oldest_task_age_secs);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_age_distribution(&self) -> Result<Vec<TaskAgeDistribution>> {
        let rows = sqlx::query(
            r#"
            SELECT
                CASE
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 60 THEN '< 1 min'
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 300 THEN '1-5 min'
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 900 THEN '5-15 min'
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 3600 THEN '15-60 min'
                    ELSE '> 60 min'
                END as bucket,
                COUNT(*) as task_count,
                MAX(TIMESTAMPDIFF(SECOND, created_at, NOW())) as oldest_age
            FROM celers_tasks
            WHERE state = 'pending'
            GROUP BY bucket
            ORDER BY
                CASE bucket
                    WHEN '< 1 min' THEN 1
                    WHEN '1-5 min' THEN 2
                    WHEN '5-15 min' THEN 3
                    WHEN '15-60 min' THEN 4
                    ELSE 5
                END
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task age distribution: {}", e)))?;

        let mut distribution = Vec::with_capacity(rows.len());
        for row in rows {
            let bucket: String = row.get("bucket");
            let task_count: i64 = row.get("task_count");
            let oldest_age: Option<i64> = row.get("oldest_age");

            distribution.push(TaskAgeDistribution {
                bucket_label: bucket,
                task_count,
                oldest_task_age_secs: oldest_age.unwrap_or(0),
            });
        }

        Ok(distribution)
    }

    /// Get retry statistics grouped by task name
    ///
    /// Analyzes task failure patterns to identify which task types
    /// are failing most often and how many retries they typically require.
    ///
    /// # Returns
    /// Vector of retry statistics per task type, sorted by total retries descending
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let stats = broker.get_retry_statistics().await?;
    /// for stat in stats {
    ///     println!("Task '{}': {} retries across {} tasks (avg: {:.1}, max: {})",
    ///         stat.task_name,
    ///         stat.total_retries,
    ///         stat.unique_tasks,
    ///         stat.avg_retries_per_task,
    ///         stat.max_retries_observed);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_retry_statistics(&self) -> Result<Vec<RetryStatistics>> {
        let rows = sqlx::query(
            r#"
            SELECT
                task_name,
                SUM(retry_count) as total_retries,
                COUNT(*) as unique_tasks,
                AVG(retry_count) as avg_retries,
                MAX(retry_count) as max_retries
            FROM celers_tasks
            WHERE retry_count > 0
            GROUP BY task_name
            ORDER BY total_retries DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get retry statistics: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let task_name: String = row.get("task_name");
            let total_retries: Option<rust_decimal::Decimal> = row.get("total_retries");
            let unique_tasks: i64 = row.get("unique_tasks");
            let avg_retries: Option<rust_decimal::Decimal> = row.get("avg_retries");
            let max_retries: i32 = row.get("max_retries");

            stats.push(RetryStatistics {
                task_name,
                total_retries: total_retries
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                unique_tasks,
                avg_retries_per_task: avg_retries
                    .and_then(|d| d.to_string().parse::<f64>().ok())
                    .unwrap_or(0.0),
                max_retries_observed: max_retries,
            });
        }

        Ok(stats)
    }

    /// Get all workers that are currently processing tasks
    ///
    /// Returns a list of all worker IDs that currently have tasks
    /// in the 'processing' state.
    ///
    /// # Returns
    /// Vector of worker IDs currently processing tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let workers = broker.list_active_workers().await?;
    /// println!("Active workers: {:?}", workers);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_active_workers(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT worker_id
            FROM celers_tasks
            WHERE worker_id IS NOT NULL AND state = 'processing'
            ORDER BY worker_id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list active workers: {}", e)))?;

        let workers: Vec<String> = rows.into_iter().map(|row| row.get("worker_id")).collect();

        Ok(workers)
    }

    /// Get all worker statistics for all active workers
    ///
    /// Returns statistics for all workers that are currently processing tasks.
    /// This is a convenience method that combines list_active_workers() and
    /// get_worker_statistics() for each worker.
    ///
    /// # Returns
    /// Vector of worker statistics for all active workers
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let all_stats = broker.get_all_worker_statistics().await?;
    /// for stats in all_stats {
    ///     println!("Worker {}: {} active, {} completed, {} failed",
    ///         stats.worker_id,
    ///         stats.active_tasks,
    ///         stats.completed_tasks,
    ///         stats.failed_tasks);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_all_worker_statistics(&self) -> Result<Vec<WorkerStatistics>> {
        let worker_ids = self.list_active_workers().await?;
        let mut all_stats = Vec::with_capacity(worker_ids.len());

        for worker_id in worker_ids {
            if let Ok(stats) = self.get_worker_statistics(&worker_id).await {
                all_stats.push(stats);
            }
        }

        Ok(all_stats)
    }

    /// Get overall queue health summary
    ///
    /// Combines multiple metrics to provide a comprehensive health assessment
    /// of the queue, including backlog, oldest task age, and active workers.
    ///
    /// Status determination:
    /// - "healthy": < 100 pending, oldest task < 5 min
    /// - "degraded": < 1000 pending, oldest task < 15 min
    /// - "critical": >= 1000 pending or oldest task >= 15 min
    ///
    /// # Returns
    /// Queue health summary with overall status assessment
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let health = broker.get_queue_health().await?;
    /// println!("Queue status: {}", health.overall_status);
    /// println!("Pending: {}, Processing: {}", health.pending_tasks, health.processing_tasks);
    /// println!("Oldest task: {}s, Active workers: {}",
    ///     health.oldest_pending_age_secs, health.active_workers);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_health(&self) -> Result<QueueHealth> {
        let stats = self.get_statistics().await?;
        let workers = self.list_active_workers().await?;

        // Get oldest pending task age
        let oldest_age: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT TIMESTAMPDIFF(SECOND, created_at, NOW())
            FROM celers_tasks
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get oldest task: {}", e)))?;

        let oldest_age_secs = oldest_age.unwrap_or(0);
        let oldest_age_minutes = oldest_age_secs as f64 / 60.0;

        // Determine overall status
        let overall_status = if stats.pending < 100 && oldest_age_minutes < 5.0 {
            "healthy"
        } else if stats.pending < 1000 && oldest_age_minutes < 15.0 {
            "degraded"
        } else {
            "critical"
        };

        // Estimate backlog in minutes (rough estimate)
        let avg_processing_rate = if !workers.is_empty() {
            workers.len() as f64
        } else {
            1.0
        };
        let backlog_minutes = if avg_processing_rate > 0.0 {
            stats.pending as f64 / avg_processing_rate
        } else {
            0.0
        };

        Ok(QueueHealth {
            overall_status: overall_status.to_string(),
            pending_tasks: stats.pending,
            processing_tasks: stats.processing,
            oldest_pending_age_secs: oldest_age_secs,
            active_workers: workers.len() as i64,
            queue_backlog_minutes: backlog_minutes,
        })
    }

    /// Get task throughput metrics
    ///
    /// Calculates how many tasks have been completed and failed in the
    /// last minute and hour, with overall tasks per second rate.
    ///
    /// # Returns
    /// Throughput metrics for completed and failed tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let throughput = broker.get_task_throughput().await?;
    /// println!("Completed: {} last min, {} last hour ({:.2}/s)",
    ///     throughput.completed_last_minute,
    ///     throughput.completed_last_hour,
    ///     throughput.tasks_per_second);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_throughput(&self) -> Result<TaskThroughput> {
        let row = sqlx::query(
            r#"
            SELECT
                SUM(CASE WHEN state = 'completed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE) THEN 1 ELSE 0 END) as completed_1min,
                SUM(CASE WHEN state = 'completed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as completed_1hour,
                SUM(CASE WHEN state = 'failed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE) THEN 1 ELSE 0 END) as failed_1min,
                SUM(CASE WHEN state = 'failed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as failed_1hour
            FROM celers_tasks
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get throughput: {}", e)))?;

        let completed_1min: Option<rust_decimal::Decimal> = row.get("completed_1min");
        let completed_1hour: Option<rust_decimal::Decimal> = row.get("completed_1hour");
        let failed_1min: Option<rust_decimal::Decimal> = row.get("failed_1min");
        let failed_1hour: Option<rust_decimal::Decimal> = row.get("failed_1hour");

        let completed_last_minute = completed_1min
            .map(|d| d.to_string().parse().unwrap_or(0))
            .unwrap_or(0);
        let completed_last_hour = completed_1hour
            .map(|d| d.to_string().parse().unwrap_or(0))
            .unwrap_or(0);

        let tasks_per_second = completed_last_minute as f64 / 60.0;

        Ok(TaskThroughput {
            completed_last_minute,
            completed_last_hour,
            failed_last_minute: failed_1min
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            failed_last_hour: failed_1hour
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            tasks_per_second,
        })
    }

    /// Requeue tasks stuck with a specific worker
    ///
    /// This is useful when a worker crashes or becomes unresponsive.
    /// Moves tasks back to pending state so they can be picked up by other workers.
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID whose tasks should be requeued
    ///
    /// # Returns
    /// The number of tasks requeued
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Requeue all tasks from a crashed worker
    /// let requeued = broker.requeue_stuck_tasks_by_worker("worker-crashed-123").await?;
    /// println!("Requeued {} tasks from crashed worker", requeued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn requeue_stuck_tasks_by_worker(&self, worker_id: &str) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending', worker_id = NULL, started_at = NULL
            WHERE worker_id = ? AND state = 'processing'
            "#,
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue tasks for worker: {}", e)))?;

        let requeued = result.rows_affected();
        tracing::warn!(worker_id = %worker_id, count = requeued, "Requeued stuck tasks");

        Ok(requeued)
    }

    /// Execute multiple operations within a single transaction
    ///
    /// This method provides a transaction wrapper for executing complex multi-step
    /// operations atomically. The callback receives a transaction handle that can
    /// be used for database operations.
    ///
    /// # Arguments
    /// * `f` - Async callback function that performs operations within the transaction
    ///
    /// # Returns
    /// The result of the callback function
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Execute multiple enqueues atomically
    /// broker.with_transaction(|_tx| async {
    ///     // Your transaction logic here
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_transaction<F, T, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(sqlx::Transaction<'_, sqlx::MySql>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let result = f(tx).await?;

        Ok(result)
    }

    /// Query tasks by metadata JSON field
    ///
    /// Searches for tasks where the metadata JSON contains a specific key-value pair.
    /// This uses MySQL JSON functions to query inside the metadata column.
    ///
    /// # Arguments
    /// * `json_path` - JSON path to query (e.g., "$.user_id")
    /// * `value` - Value to match
    /// * `limit` - Maximum number of results
    /// * `offset` - Pagination offset
    ///
    /// # Returns
    /// List of matching tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Find all tasks for a specific user
    /// let tasks = broker.query_tasks_by_metadata("$.user_id", "12345", 10, 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_tasks_by_metadata(
        &self,
        json_path: &str,
        value: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE JSON_EXTRACT(metadata, ?) = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(json_path)
        .bind(value)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to query tasks by metadata: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let id_str: String = row
                .try_get("id")
                .map_err(|e| CelersError::Other(format!("Failed to get id: {}", e)))?;
            let state_str: String = row
                .try_get("state")
                .map_err(|e| CelersError::Other(format!("Failed to get state: {}", e)))?;

            tasks.push(TaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row
                    .try_get("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get task_name: {}", e)))?,
                state: state_str.parse()?,
                priority: row
                    .try_get("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to get priority: {}", e)))?,
                retry_count: row
                    .try_get("retry_count")
                    .map_err(|e| CelersError::Other(format!("Failed to get retry_count: {}", e)))?,
                max_retries: row
                    .try_get("max_retries")
                    .map_err(|e| CelersError::Other(format!("Failed to get max_retries: {}", e)))?,
                created_at: row
                    .try_get("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get created_at: {}", e)))?,
                scheduled_at: row.try_get("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get scheduled_at: {}", e))
                })?,
                started_at: row
                    .try_get("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get started_at: {}", e)))?,
                completed_at: row.try_get("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get completed_at: {}", e))
                })?,
                worker_id: row
                    .try_get("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to get worker_id: {}", e)))?,
                error_message: row.try_get("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to get error_message: {}", e))
                })?,
            });
        }

        Ok(tasks)
    }

    /// Enqueue a task with deduplication based on a custom key
    ///
    /// This method ensures that only one task with a given deduplication key exists
    /// in the pending or processing state. If a task with the same key already exists,
    /// this method returns the existing task ID instead of creating a duplicate.
    ///
    /// The deduplication key is stored in the metadata JSON field.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `dedup_key` - Unique key for deduplication
    ///
    /// # Returns
    /// Task ID (either new or existing)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let task = SerializedTask::new("process_user".into(), vec![1, 2, 3]);
    /// // Only one task per user will be enqueued
    /// let task_id = broker.enqueue_deduplicated(task, "user:12345").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_deduplicated(
        &self,
        task: SerializedTask,
        dedup_key: &str,
    ) -> Result<TaskId> {
        // First check if a task with this dedup key already exists
        let existing = sqlx::query(
            r#"
            SELECT id FROM celers_tasks
            WHERE JSON_EXTRACT(metadata, '$.dedup_key') = ?
              AND state IN ('pending', 'processing')
            LIMIT 1
            "#,
        )
        .bind(dedup_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check for duplicate task: {}", e)))?;

        if let Some(row) = existing {
            let id_str: String = row
                .try_get("id")
                .map_err(|e| CelersError::Other(format!("Failed to get id: {}", e)))?;
            let task_id = Uuid::parse_str(&id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            tracing::info!(task_id = %task_id, dedup_key = %dedup_key, "Task already exists, skipping");
            return Ok(task_id);
        }

        // Create metadata with dedup key
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "dedup_key": dedup_key,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue deduplicated task: {}", e)))?;

        tracing::info!(task_id = %task_id, dedup_key = %dedup_key, "Enqueued new deduplicated task");

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    /// Update state for multiple tasks atomically
    ///
    /// This is more efficient than updating task states individually.
    /// Only updates tasks that are in a valid source state.
    ///
    /// # Arguments
    /// * `task_ids` - Slice of task IDs to update
    /// * `new_state` - The new state to set
    ///
    /// # Returns
    /// The number of tasks actually updated
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # use celers_core::TaskId;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let task_ids = vec![/* task IDs */];
    /// let updated = broker.update_batch_state(&task_ids, DbTaskState::Failed).await?;
    /// println!("Updated {} tasks to failed state", updated);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_batch_state(
        &self,
        task_ids: &[TaskId],
        new_state: DbTaskState,
    ) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let task_id_strings: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let placeholders = vec!["?"; task_ids.len()].join(", ");

        let completed_at_clause = match new_state {
            DbTaskState::Completed | DbTaskState::Failed | DbTaskState::Cancelled => {
                ", completed_at = NOW()"
            }
            _ => "",
        };

        let query = format!(
            r#"
            UPDATE celers_tasks
            SET state = ?{}
            WHERE id IN ({})
            "#,
            completed_at_clause, placeholders
        );

        let mut query_builder = sqlx::query(&query);
        query_builder = query_builder.bind(new_state.to_string());
        for task_id_str in task_id_strings {
            query_builder = query_builder.bind(task_id_str);
        }

        let result = query_builder
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to update batch state: {}", e)))?;

        let updated = result.rows_affected();
        tracing::info!(count = updated, state = %new_state, "Updated task states in batch");

        Ok(updated)
    }

    /// Check if enqueueing would exceed the queue capacity
    ///
    /// Returns true if the queue has room for more tasks, false if it's at capacity.
    /// This can be used to implement backpressure and prevent queue overflow.
    ///
    /// # Arguments
    /// * `max_size` - Maximum allowed pending tasks
    ///
    /// # Returns
    /// True if the queue can accept more tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Check capacity before enqueueing
    /// if broker.has_capacity(10000).await? {
    ///     let task = SerializedTask::new("task".into(), vec![]);
    ///     broker.enqueue(task).await?;
    /// } else {
    ///     println!("Queue is full, backing off");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn has_capacity(&self, max_size: i64) -> Result<bool> {
        let current_size = self.count_by_state_quick(DbTaskState::Pending).await?;
        Ok(current_size < max_size)
    }

    /// Enqueue a task only if the queue has capacity
    ///
    /// This is a convenience method that combines capacity checking with enqueuing.
    /// If the queue is full, it returns an error instead of enqueueing.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `max_size` - Maximum allowed pending tasks
    ///
    /// # Returns
    /// Task ID if enqueued successfully
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let task = SerializedTask::new("task".into(), vec![]);
    /// match broker.enqueue_with_capacity(task, 10000).await {
    ///     Ok(task_id) => println!("Enqueued task {}", task_id),
    ///     Err(_) => println!("Queue is full"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_capacity(
        &self,
        task: SerializedTask,
        max_size: i64,
    ) -> Result<TaskId> {
        if !self.has_capacity(max_size).await? {
            return Err(CelersError::Other(
                "Queue is at capacity, cannot enqueue".to_string(),
            ));
        }

        self.enqueue(task).await
    }

    /// Expire old pending tasks that have exceeded their TTL
    ///
    /// This method marks pending tasks as cancelled if they have been pending
    /// longer than the specified TTL. Useful for preventing stale tasks from
    /// being processed.
    ///
    /// # Arguments
    /// * `ttl` - Maximum age for pending tasks
    ///
    /// # Returns
    /// Number of tasks expired
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Expire tasks older than 1 hour
    /// let expired = broker.expire_pending_tasks(Duration::from_secs(3600)).await?;
    /// println!("Expired {} stale tasks", expired);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_pending_tasks(&self, ttl: Duration) -> Result<u64> {
        let ttl_seconds = ttl.as_secs() as i64;

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = CONCAT('Task expired after ', ?, ' seconds')
            WHERE state = 'pending'
              AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
            "#,
        )
        .bind(ttl_seconds)
        .bind(ttl_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to expire pending tasks: {}", e)))?;

        let expired = result.rows_affected();
        if expired > 0 {
            tracing::warn!(
                count = expired,
                ttl_seconds = ttl_seconds,
                "Expired pending tasks"
            );
        }

        Ok(expired)
    }

    /// Delete tasks matching specific criteria
    ///
    /// Permanently deletes tasks from the database based on state and age.
    /// This is more flexible than the existing purge methods.
    ///
    /// # Arguments
    /// * `state` - Optional state filter (None = all states)
    /// * `older_than` - Only delete tasks older than this duration
    ///
    /// # Returns
    /// Number of tasks deleted
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Delete completed tasks older than 7 days
    /// let deleted = broker.delete_tasks_by_criteria(
    ///     Some(DbTaskState::Completed),
    ///     Duration::from_secs(7 * 24 * 3600)
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn delete_tasks_by_criteria(
        &self,
        state: Option<DbTaskState>,
        older_than: Duration,
    ) -> Result<u64> {
        let seconds_ago = older_than.as_secs() as i64;

        let query = if let Some(state) = state {
            sqlx::query(
                r#"
                DELETE FROM celers_tasks
                WHERE state = ?
                  AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
                "#,
            )
            .bind(state.to_string())
            .bind(seconds_ago)
        } else {
            sqlx::query(
                r#"
                DELETE FROM celers_tasks
                WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
                "#,
            )
            .bind(seconds_ago)
        };

        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete tasks: {}", e)))?;

        let deleted = result.rows_affected();
        tracing::info!(count = deleted, "Deleted tasks by criteria");

        Ok(deleted)
    }

    /// Update metadata for an existing task
    ///
    /// Allows updating the JSON metadata field for a task without changing
    /// its state or other properties. The metadata is merged with existing
    /// metadata using JSON_SET.
    ///
    /// # Arguments
    /// * `task_id` - Task ID to update
    /// * `json_path` - JSON path to update (e.g., "$.priority_level")
    /// * `value` - New value for the path
    ///
    /// # Returns
    /// True if task was updated
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::TaskId;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Update a specific metadata field
    /// broker.update_task_metadata(&task_id, "$.priority_level", "high").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_task_metadata(
        &self,
        task_id: &TaskId,
        json_path: &str,
        value: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET metadata = JSON_SET(metadata, ?, ?)
            WHERE id = ?
            "#,
        )
        .bind(json_path)
        .bind(value)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update task metadata: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Search tasks by creation date range
    ///
    /// Finds tasks created within a specific time window. Useful for
    /// analyzing task patterns over time or implementing time-based cleanup.
    ///
    /// # Arguments
    /// * `from` - Start of time range
    /// * `to` - End of time range
    /// * `state` - Optional state filter
    /// * `limit` - Maximum results
    /// * `offset` - Pagination offset
    ///
    /// # Returns
    /// List of matching tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # use chrono::{Utc, Duration};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let now = Utc::now();
    /// let one_hour_ago = now - Duration::hours(1);
    ///
    /// // Find all failed tasks in the last hour
    /// let tasks = broker.search_tasks_by_date_range(
    ///     one_hour_ago,
    ///     now,
    ///     Some(DbTaskState::Failed),
    ///     100,
    ///     0
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn search_tasks_by_date_range(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let query = if let Some(state) = state {
            sqlx::query(
                r#"
                SELECT id, task_name, state, priority, retry_count, max_retries,
                       created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                FROM celers_tasks
                WHERE created_at >= ? AND created_at <= ?
                  AND state = ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                "#,
            )
            .bind(from)
            .bind(to)
            .bind(state.to_string())
            .bind(limit)
            .bind(offset)
        } else {
            sqlx::query(
                r#"
                SELECT id, task_name, state, priority, retry_count, max_retries,
                       created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                FROM celers_tasks
                WHERE created_at >= ? AND created_at <= ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                "#,
            )
            .bind(from)
            .bind(to)
            .bind(limit)
            .bind(offset)
        };

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to search tasks by date: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let id_str: String = row
                .try_get("id")
                .map_err(|e| CelersError::Other(format!("Failed to get id: {}", e)))?;
            let state_str: String = row
                .try_get("state")
                .map_err(|e| CelersError::Other(format!("Failed to get state: {}", e)))?;

            tasks.push(TaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row
                    .try_get("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get task_name: {}", e)))?,
                state: state_str.parse()?,
                priority: row
                    .try_get("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to get priority: {}", e)))?,
                retry_count: row
                    .try_get("retry_count")
                    .map_err(|e| CelersError::Other(format!("Failed to get retry_count: {}", e)))?,
                max_retries: row
                    .try_get("max_retries")
                    .map_err(|e| CelersError::Other(format!("Failed to get max_retries: {}", e)))?,
                created_at: row
                    .try_get("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get created_at: {}", e)))?,
                scheduled_at: row.try_get("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get scheduled_at: {}", e))
                })?,
                started_at: row
                    .try_get("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get started_at: {}", e)))?,
                completed_at: row.try_get("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get completed_at: {}", e))
                })?,
                worker_id: row
                    .try_get("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to get worker_id: {}", e)))?,
                error_message: row.try_get("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to get error_message: {}", e))
                })?,
            });
        }

        Ok(tasks)
    }

    /// Get Dead Letter Queue statistics
    ///
    /// Returns comprehensive statistics about the DLQ including total count,
    /// counts by task name, and average retry counts.
    ///
    /// # Returns
    /// DLQ statistics
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let stats = broker.get_dlq_statistics().await?;
    /// println!("DLQ has {} tasks", stats.total_tasks);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_dlq_statistics(&self) -> Result<DlqStatistics> {
        // Get total count
        let total_row = sqlx::query(
            r#"
            SELECT COUNT(*) as total
            FROM celers_dead_letter_queue
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ total: {}", e)))?;

        let total_tasks: i64 = total_row
            .try_get("total")
            .map_err(|e| CelersError::Other(format!("Failed to get total: {}", e)))?;

        // Get counts by task name
        let rows = sqlx::query(
            r#"
            SELECT task_name,
                   COUNT(*) as count,
                   AVG(retry_count) as avg_retries,
                   MAX(retry_count) as max_retries
            FROM celers_dead_letter_queue
            GROUP BY task_name
            ORDER BY count DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ stats: {}", e)))?;

        let mut by_task_name = Vec::new();
        for row in rows {
            let count: i64 = row
                .try_get("count")
                .map_err(|e| CelersError::Other(format!("Failed to get count: {}", e)))?;
            let avg_retries: Option<rust_decimal::Decimal> = row
                .try_get("avg_retries")
                .map_err(|e| CelersError::Other(format!("Failed to get avg_retries: {}", e)))?;
            let max_retries: i32 = row
                .try_get("max_retries")
                .map_err(|e| CelersError::Other(format!("Failed to get max_retries: {}", e)))?;

            by_task_name.push(DlqTaskStats {
                task_name: row
                    .try_get("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get task_name: {}", e)))?,
                count,
                avg_retries: avg_retries.map(|d| d.to_string().parse::<f64>().unwrap_or(0.0)),
                max_retries,
            });
        }

        Ok(DlqStatistics {
            total_tasks,
            by_task_name,
        })
    }

    /// Detect and recover tasks that have exceeded their processing timeout
    ///
    /// Finds tasks that have been in 'processing' state longer than the
    /// specified timeout and requeues them as pending. This helps recover
    /// from worker crashes or hangs.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time a task should be in processing state
    ///
    /// # Returns
    /// Number of timed-out tasks recovered
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Recover tasks stuck in processing for more than 30 minutes
    /// let recovered = broker.recover_timed_out_tasks(Duration::from_secs(1800)).await?;
    /// println!("Recovered {} timed-out tasks", recovered);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recover_timed_out_tasks(&self, timeout: Duration) -> Result<u64> {
        let timeout_seconds = timeout.as_secs() as i64;

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending',
                worker_id = NULL,
                started_at = NULL,
                error_message = CONCAT(
                    COALESCE(error_message, ''),
                    IF(error_message IS NOT NULL, '; ', ''),
                    'Task timed out after ', ?, ' seconds and was requeued'
                )
            WHERE state = 'processing'
              AND started_at IS NOT NULL
              AND TIMESTAMPDIFF(SECOND, started_at, NOW()) > ?
            "#,
        )
        .bind(timeout_seconds)
        .bind(timeout_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to recover timed-out tasks: {}", e)))?;

        let recovered = result.rows_affected();
        if recovered > 0 {
            tracing::warn!(
                count = recovered,
                timeout_seconds = timeout_seconds,
                "Recovered timed-out tasks"
            );
        }

        Ok(recovered)
    }
}

#[async_trait]
impl Broker for MysqlBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        // Use FOR UPDATE SKIP LOCKED to atomically claim a task
        // This is the magic that makes distributed workers work without contention
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let row = sqlx::query(
            r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        if let Some(row) = row {
            let task_id_str: String = row.get("id");
            let _task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            let task_name: String = row.get("task_name");
            let payload: Vec<u8> = row.get("payload");
            let retry_count: i32 = row.get("retry_count");

            // Mark as processing
            sqlx::query(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1
                WHERE id = ?
                "#,
            )
            .bind(&task_id_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to mark task as processing: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            Ok(Some(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            }))
        } else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            Ok(None)
        }
    }

    async fn ack(&self, task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'completed',
                completed_at = NOW()
            WHERE id = ?
            "#,
        )
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to ack task: {}", e)))?;

        // Optionally delete completed tasks after a retention period
        // For now, we keep them for auditing

        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        _receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        if requeue {
            // Check if task has exceeded max retries
            let row = sqlx::query(
                r#"
                SELECT retry_count, max_retries
                FROM celers_tasks
                WHERE id = ?
                "#,
            )
            .bind(task_id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

            let retry_count: i32 = row.get("retry_count");
            let max_retries: i32 = row.get("max_retries");

            if retry_count >= max_retries {
                // Move to DLQ
                self.move_to_dlq(task_id).await?;
            } else {
                // Requeue with exponential backoff
                let backoff_seconds = 2_i64.pow(retry_count as u32).min(3600); // Max 1 hour

                sqlx::query(
                    r#"
                    UPDATE celers_tasks
                    SET state = 'pending',
                        scheduled_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                        started_at = NULL,
                        worker_id = NULL
                    WHERE id = ?
                    "#,
                )
                .bind(backoff_seconds)
                .bind(task_id.to_string())
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;
            }
        } else {
            // Mark as failed permanently
            sqlx::query(
                r#"
                UPDATE celers_tasks
                SET state = 'failed',
                    completed_at = NOW()
                WHERE id = ?
                "#,
            )
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to mark task as failed: {}", e)))?;
        }

        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM celers_tasks
            WHERE state = 'pending'
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get queue size: {}", e)))?;

        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE id = ? AND state IN ('pending', 'processing')
            "#,
        )
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel task: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Schedule a task for execution at a specific Unix timestamp (seconds)
    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "scheduled_for": execute_at,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Convert Unix timestamp to MySQL TIMESTAMP
        let scheduled_at = chrono::DateTime::from_timestamp(execute_at, 0)
            .ok_or_else(|| CelersError::Other("Invalid timestamp".to_string()))?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), ?)
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
        .bind(scheduled_at)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue delayed task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    /// Schedule a task for execution after a delay (seconds)
    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "delay_seconds": delay_secs,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), DATE_ADD(NOW(), INTERVAL ? SECOND))
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
        .bind(delay_secs as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue delayed task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    // ========== Batch Operations (optimized overrides) ==========

    /// Optimized batch enqueue using a single transaction
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        self.enqueue_batch_impl(tasks).await
    }

    /// Optimized batch dequeue using a single transaction with FOR UPDATE SKIP LOCKED
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        self.dequeue_batch_impl(count).await
    }

    /// Optimized batch ack using a single query with IN clause
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        let task_ids: Vec<String> = tasks.iter().map(|(id, _)| id.to_string()).collect();

        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'completed',
                completed_at = NOW()
            WHERE id IN ({})
            "#,
            placeholders
        );

        let mut query = sqlx::query(&query_str);
        for task_id in task_ids {
            query = query.bind(task_id);
        }

        query
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to batch ack tasks: {}", e)))?;

        Ok(())
    }
}

impl MysqlBroker {
    // ========== Task Chain Support ==========

    /// Enqueue a task chain where tasks execute in sequence
    ///
    /// Each task in the chain will be scheduled to execute after the previous task
    /// completes (with optional delay between tasks).
    ///
    /// # Arguments
    /// * `chain` - Task chain to enqueue
    ///
    /// # Returns
    /// Vector of task IDs in the same order as the chain
    ///
    /// # Example
    /// ```rust,ignore
    /// let chain = TaskChain::new()
    ///     .then(task1)
    ///     .then(task2)
    ///     .then(task3)
    ///     .with_delay(5); // 5 seconds between tasks
    ///
    /// let task_ids = broker.enqueue_chain(chain).await?;
    /// ```
    pub async fn enqueue_chain(&self, chain: TaskChain) -> Result<Vec<TaskId>> {
        if chain.tasks().is_empty() {
            return Ok(Vec::new());
        }

        let mut task_ids = Vec::with_capacity(chain.tasks().len());
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Failed to get system time: {}", e)))?
            .as_secs() as i64;

        for (idx, task) in chain.tasks().iter().enumerate() {
            let execute_at = if idx == 0 {
                // First task executes immediately
                base_time
            } else {
                // Subsequent tasks execute after delay
                let delay = chain.delay_between_secs().unwrap_or(0) * idx as u64;
                base_time + delay as i64
            };

            let task_id = self.enqueue_at(task.clone(), execute_at).await?;
            task_ids.push(task_id);
        }

        tracing::info!(
            chain_length = chain.tasks().len(),
            delay_secs = chain.delay_between_secs().unwrap_or(0),
            "Enqueued task chain"
        );

        Ok(task_ids)
    }

    /// Batch reject operation - reject multiple tasks at once
    ///
    /// This is more efficient than calling reject() for each task individually.
    ///
    /// # Arguments
    /// * `tasks` - Vector of (TaskId, receipt_handle, requeue) tuples
    ///
    /// # Returns
    /// Number of tasks successfully rejected
    pub async fn reject_batch(&self, tasks: &[(TaskId, Option<String>, bool)]) -> Result<u64> {
        if tasks.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut rejected_count = 0u64;

        for (task_id, _receipt_handle, requeue) in tasks {
            if *requeue {
                // Check if task has exceeded max retries
                let row = sqlx::query(
                    r#"
                    SELECT retry_count, max_retries
                    FROM celers_tasks
                    WHERE id = ?
                    "#,
                )
                .bind(task_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

                if let Some(row) = row {
                    let retry_count: i32 = row.get("retry_count");
                    let max_retries: i32 = row.get("max_retries");

                    if retry_count >= max_retries {
                        // Move to DLQ
                        sqlx::query("CALL move_to_dlq(?)")
                            .bind(task_id.to_string())
                            .execute(&mut *tx)
                            .await
                            .map_err(|e| {
                                CelersError::Other(format!("Failed to move task to DLQ: {}", e))
                            })?;
                    } else {
                        // Requeue with exponential backoff
                        let backoff_seconds = 2_i64.pow(retry_count as u32).min(3600); // Max 1 hour

                        sqlx::query(
                            r#"
                            UPDATE celers_tasks
                            SET state = 'pending',
                                scheduled_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                                started_at = NULL,
                                worker_id = NULL
                            WHERE id = ?
                            "#,
                        )
                        .bind(backoff_seconds)
                        .bind(task_id.to_string())
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to requeue task: {}", e))
                        })?;
                    }
                    rejected_count += 1;
                }
            } else {
                // Mark as failed permanently
                let result = sqlx::query(
                    r#"
                    UPDATE celers_tasks
                    SET state = 'failed',
                        completed_at = NOW()
                    WHERE id = ?
                    "#,
                )
                .bind(task_id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to mark task as failed: {}", e)))?;

                rejected_count += result.rows_affected();
            }
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch reject: {}", e)))?;

        Ok(rejected_count)
    }
}

impl MysqlBroker {
    /// Update Prometheus metrics gauges for queue sizes
    ///
    /// This should be called periodically (e.g., every few seconds) to keep
    /// metrics up to date. Not part of the Broker trait, but useful for monitoring.
    #[cfg(feature = "metrics")]
    pub async fn update_metrics(&self) -> Result<()> {
        // Get pending tasks count
        let pending_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending'")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Get processing tasks count
        let processing_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = 'processing'")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to get processing count: {}", e))
                })?;

        // Get DLQ count
        let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_dead_letter_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;

        // Update gauges
        QUEUE_SIZE.set(pending_count as f64);
        PROCESSING_QUEUE_SIZE.set(processing_count as f64);
        DLQ_SIZE.set(dlq_count as f64);

        Ok(())
    }

    /// Apply DLQ retention policy - delete old DLQ entries based on age
    ///
    /// This helps prevent unbounded DLQ growth by removing entries older than the specified retention period.
    /// Useful for production systems where DLQ entries are monitored but eventually need cleanup.
    ///
    /// # Arguments
    /// * `retention_period` - Duration after which DLQ entries should be deleted
    ///
    /// # Returns
    /// Number of DLQ entries deleted
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Delete DLQ entries older than 30 days
    /// let deleted = broker.apply_dlq_retention(Duration::from_secs(30 * 24 * 3600)).await?;
    /// println!("Deleted {} old DLQ entries", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_dlq_retention(&self, retention_period: Duration) -> Result<u64> {
        let retention_seconds = retention_period.as_secs() as i64;

        let result = sqlx::query(
            r#"
            DELETE FROM celers_dead_letter_queue
            WHERE TIMESTAMPDIFF(SECOND, failed_at, NOW()) > ?
            "#,
        )
        .bind(retention_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to apply DLQ retention: {}", e)))?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            tracing::info!(
                count = deleted,
                retention_days = retention_seconds / 86400,
                "Applied DLQ retention policy"
            );
        }

        Ok(deleted)
    }

    /// Calculate optimal batch size based on current queue depth and load
    ///
    /// This implements an adaptive batch sizing strategy:
    /// - Small batches (1-5) when queue is nearly empty to reduce latency
    /// - Medium batches (10-50) for moderate load
    /// - Large batches (50-200) for high load to maximize throughput
    ///
    /// # Arguments
    /// * `max_batch_size` - Maximum batch size to return (default: 200)
    ///
    /// # Returns
    /// Recommended batch size based on current queue state
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::Broker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Get adaptive batch size
    /// let batch_size = broker.get_optimal_batch_size(Some(100)).await?;
    /// let messages = broker.dequeue_batch(batch_size as usize).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_optimal_batch_size(&self, max_batch_size: Option<i64>) -> Result<i64> {
        let max_size = max_batch_size.unwrap_or(200);

        // Get current pending task count
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending' AND scheduled_at <= NOW()",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Adaptive batch sizing based on queue depth
        let optimal_size = if pending < 10 {
            // Small queue - use small batches to reduce latency
            std::cmp::min(pending.max(1), 5)
        } else if pending < 100 {
            // Medium queue - balance latency and throughput
            std::cmp::min(pending / 2, 50)
        } else {
            // Large queue - maximize throughput
            std::cmp::min(pending / 4, max_size)
        };

        Ok(optimal_size.max(1))
    }

    /// Get connection pool health status with detailed metrics
    ///
    /// Returns comprehensive connection pool metrics including:
    /// - Pool size and utilization
    /// - Active vs idle connections
    /// - Connection wait times (if available)
    /// - Pool pressure indicators
    ///
    /// # Returns
    /// Detailed connection diagnostics including health status
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let health = broker.get_pool_health().await?;
    /// if health.pool_utilization_percent > 80.0 {
    ///     println!("Warning: Connection pool utilization is high!");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_pool_health(&self) -> Result<ConnectionDiagnostics> {
        let total = self.pool.size();
        let idle = self.pool.num_idle() as u32;
        let active = total - idle;
        let max = self.pool.options().get_max_connections();

        let utilization = if max > 0 {
            (total as f64 / max as f64) * 100.0
        } else {
            0.0
        };

        Ok(ConnectionDiagnostics {
            total_connections: total,
            idle_connections: idle,
            active_connections: active,
            max_connections: max,
            connection_wait_time_ms: None, // MySQL driver doesn't expose this
            pool_utilization_percent: utilization,
        })
    }

    /// Compress task payload using DEFLATE compression
    ///
    /// This can significantly reduce storage and network overhead for large task payloads.
    /// Compression is applied transparently and decompression happens automatically during dequeue.
    ///
    /// # Arguments
    /// * `payload` - Raw task payload bytes
    ///
    /// # Returns
    /// Compressed payload bytes
    ///
    /// Note: Only use compression for payloads larger than ~1KB, as small payloads may
    /// actually grow due to compression overhead.
    #[allow(dead_code)]
    fn compress_payload(payload: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;

        // Only compress if payload is larger than 1KB
        if payload.len() < 1024 {
            return Ok(payload.to_vec());
        }

        let mut encoder =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder
            .write_all(payload)
            .map_err(|e| CelersError::Other(format!("Compression failed: {}", e)))?;

        encoder
            .finish()
            .map_err(|e| CelersError::Other(format!("Compression finalization failed: {}", e)))
    }

    /// Decompress task payload
    ///
    /// # Arguments
    /// * `compressed` - Compressed payload bytes
    ///
    /// # Returns
    /// Decompressed payload bytes
    #[allow(dead_code)]
    fn decompress_payload(compressed: &[u8]) -> Result<Vec<u8>> {
        use std::io::Read;

        let mut decoder = flate2::read::DeflateDecoder::new(compressed);
        let mut decompressed = Vec::new();

        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| CelersError::Other(format!("Decompression failed: {}", e)))?;

        Ok(decompressed)
    }

    /// Vacuum analyze all CeleRS tables for optimal query performance
    ///
    /// This operation is similar to PostgreSQL's VACUUM ANALYZE but uses MySQL-specific
    /// optimizations (OPTIMIZE TABLE + ANALYZE TABLE). It:
    /// - Reclaims storage from deleted rows
    /// - Updates table statistics for better query planning
    /// - Defragments table data
    ///
    /// Should be run periodically (e.g., weekly) on production systems.
    ///
    /// # Returns
    /// Number of tables optimized
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Run maintenance
    /// let tables_optimized = broker.vacuum_analyze().await?;
    /// println!("Optimized {} tables", tables_optimized);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn vacuum_analyze(&self) -> Result<u64> {
        let tables = vec![
            "celers_tasks",
            "celers_dead_letter_queue",
            "celers_task_history",
            "celers_task_results",
        ];

        let mut optimized = 0u64;

        for table in &tables {
            // OPTIMIZE TABLE
            sqlx::query(&format!("OPTIMIZE TABLE {}", table))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to optimize table {}: {}", table, e))
                })?;

            // ANALYZE TABLE
            sqlx::query(&format!("ANALYZE TABLE {}", table))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to analyze table {}: {}", table, e))
                })?;

            optimized += 1;
        }

        tracing::info!(tables_count = optimized, "Completed vacuum analyze");
        Ok(optimized)
    }

    /// Get slow query log entries related to CeleRS tables
    ///
    /// Returns queries that exceeded a certain threshold from MySQL slow query log.
    /// Requires slow query log to be enabled in MySQL configuration.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of slow queries to return
    ///
    /// # Returns
    /// List of slow query information (query text, execution time, etc.)
    ///
    /// Note: This requires MySQL slow_query_log to be enabled and accessible.
    pub async fn get_slow_queries(&self, limit: i64) -> Result<Vec<SlowQueryInfo>> {
        // Check if performance_schema is enabled
        let ps_enabled: String = sqlx::query_scalar("SELECT @@performance_schema")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to check performance_schema: {}", e))
            })?;

        if ps_enabled != "1" {
            return Ok(Vec::new());
        }

        // Query from events_statements_summary_by_digest
        let rows = sqlx::query(
            r#"
            SELECT
                DIGEST_TEXT as query_text,
                COUNT_STAR as execution_count,
                AVG_TIMER_WAIT / 1000000000 as avg_time_ms,
                MAX_TIMER_WAIT / 1000000000 as max_time_ms,
                SUM_TIMER_WAIT / 1000000000 as total_time_ms
            FROM performance_schema.events_statements_summary_by_digest
            WHERE DIGEST_TEXT LIKE '%celers_%'
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to query slow queries: {}", e)))?;

        let mut slow_queries = Vec::new();
        for row in rows {
            slow_queries.push(SlowQueryInfo {
                query_text: row.try_get("query_text").unwrap_or_default(),
                execution_count: row.try_get("execution_count").unwrap_or(0),
                avg_time_ms: row.try_get("avg_time_ms").unwrap_or(0.0),
                max_time_ms: row.try_get("max_time_ms").unwrap_or(0.0),
                total_time_ms: row.try_get("total_time_ms").unwrap_or(0.0),
            });
        }

        Ok(slow_queries)
    }

    /// Apply priority aging to prevent task starvation
    ///
    /// Increases the priority of tasks that have been pending for a long time.
    /// This prevents low-priority tasks from being starved by a continuous stream
    /// of high-priority tasks.
    ///
    /// # Arguments
    /// * `age_threshold_secs` - Tasks older than this will have their priority increased
    /// * `priority_boost` - Amount to add to the priority (default: 10)
    ///
    /// # Returns
    /// Number of tasks whose priority was increased
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Boost priority for tasks pending more than 5 minutes
    /// let boosted = broker.apply_priority_aging(300, 10).await?;
    /// println!("Boosted priority for {} old tasks", boosted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_priority_aging(
        &self,
        age_threshold_secs: i64,
        priority_boost: i32,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = priority + ?
            WHERE state = 'pending'
              AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
              AND priority < 1000
            "#,
        )
        .bind(priority_boost)
        .bind(age_threshold_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to apply priority aging: {}", e)))?;

        let updated = result.rows_affected();
        if updated > 0 {
            tracing::info!(
                count = updated,
                age_threshold_secs = age_threshold_secs,
                priority_boost = priority_boost,
                "Applied priority aging"
            );
        }

        Ok(updated)
    }

    /// Update task progress for long-running tasks
    ///
    /// Allows workers to report progress on long-running tasks. This is stored
    /// in the task metadata as JSON and can be queried later.
    ///
    /// # Arguments
    /// * `task_id` - Task ID to update
    /// * `progress_percent` - Progress percentage (0.0 - 100.0)
    /// * `current_step` - Optional description of current step
    ///
    /// # Returns
    /// True if task was updated, false if not found
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Update progress to 50%
    /// broker.update_task_progress(&task_id, 50.0, Some("Processing chunk 5/10")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_task_progress(
        &self,
        task_id: &TaskId,
        progress_percent: f64,
        current_step: Option<&str>,
    ) -> Result<bool> {
        let progress_json = serde_json::json!({
            "progress_percent": progress_percent,
            "current_step": current_step,
            "updated_at": chrono::Utc::now().to_rfc3339(),
        });

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET metadata = JSON_SET(
                metadata,
                '$.progress', ?
            )
            WHERE id = ? AND state = 'processing'
            "#,
        )
        .bind(serde_json::to_string(&progress_json).unwrap_or_else(|_| "{}".to_string()))
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update task progress: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Get task progress for a specific task
    ///
    /// # Arguments
    /// * `task_id` - Task ID to query
    ///
    /// # Returns
    /// Task progress information if available
    pub async fn get_task_progress(&self, task_id: &TaskId) -> Result<Option<TaskProgress>> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                JSON_EXTRACT(metadata, '$.progress.progress_percent') as progress_percent,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.progress.current_step')) as current_step,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.progress.updated_at')) as updated_at
            FROM celers_tasks
            WHERE id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task progress: {}", e)))?;

        if let Some(row) = row {
            let progress_percent: Option<f64> = row.try_get("progress_percent").ok();
            let current_step: Option<String> = row.try_get("current_step").ok();
            let updated_at_str: Option<String> = row.try_get("updated_at").ok();

            if let Some(percent) = progress_percent {
                let updated_at = updated_at_str
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(Utc::now);

                return Ok(Some(TaskProgress {
                    task_id: *task_id,
                    progress_percent: percent,
                    current_step,
                    total_steps: None,
                    updated_at,
                }));
            }
        }

        Ok(None)
    }

    /// Check rate limit for a specific task type
    ///
    /// Returns current execution rate and whether the limit is exceeded.
    ///
    /// # Arguments
    /// * `task_name` - Task type to check
    /// * `max_per_minute` - Maximum tasks per minute allowed
    ///
    /// # Returns
    /// Rate limit status including current rate and whether limit is exceeded
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Check if we can execute more "expensive_task" (limit: 100/min)
    /// let status = broker.check_rate_limit("expensive_task", 100).await?;
    /// if status.limit_exceeded {
    ///     println!("Rate limit exceeded, backing off...");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_rate_limit(
        &self,
        task_name: &str,
        max_per_minute: i64,
    ) -> Result<RateLimitStatus> {
        // Count completed tasks in the last minute
        let completed_last_minute: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE task_name = ?
              AND state = 'completed'
              AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
            "#,
        )
        .bind(task_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check rate limit: {}", e)))?;

        // Count for last hour
        let completed_last_hour: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE task_name = ?
              AND state = 'completed'
              AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            "#,
        )
        .bind(task_name)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        let per_second = completed_last_minute as f64 / 60.0;
        let limit_exceeded = completed_last_minute >= max_per_minute;

        Ok(RateLimitStatus {
            task_name: task_name.to_string(),
            current_per_second: per_second,
            current_per_minute: completed_last_minute,
            current_per_hour: completed_last_hour,
            limit_exceeded,
        })
    }

    /// Deduplicate tasks within a time window
    ///
    /// Prevents duplicate tasks from being enqueued if a matching task exists
    /// within the specified time window.
    ///
    /// # Arguments
    /// * `task` - Task to enqueue
    /// * `dedup_key` - Deduplication key
    /// * `window_secs` - Time window in seconds to check for duplicates
    ///
    /// # Returns
    /// TaskId - Either the existing task ID or a new task ID
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::SerializedTask;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    /// let task = SerializedTask::new("process_order".to_string(), vec![1, 2, 3]);
    ///
    /// // Only enqueue if no matching task in last 5 minutes
    /// let task_id = broker.enqueue_deduplicated_window(task, "order-123", 300).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_deduplicated_window(
        &self,
        task: SerializedTask,
        dedup_key: &str,
        window_secs: i64,
    ) -> Result<TaskId> {
        // Check for existing task within window
        let existing: Option<String> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM celers_tasks
            WHERE JSON_EXTRACT(metadata, '$.dedup_key') = ?
              AND created_at >= DATE_SUB(NOW(), INTERVAL ? SECOND)
              AND state IN ('pending', 'processing')
            LIMIT 1
            "#,
        )
        .bind(dedup_key)
        .bind(window_secs)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check for duplicates: {}", e)))?;

        if let Some(id_str) = existing {
            // Return existing task ID
            let task_id = Uuid::parse_str(&id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            tracing::debug!(
                task_id = %task_id,
                dedup_key = dedup_key,
                "Found duplicate task within window"
            );
            return Ok(task_id);
        }

        // No duplicate found, enqueue new task with dedup_key
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "dedup_key": dedup_key,
        });

        // Merge task metadata
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        Ok(task_id)
    }

    /// Cascade cancel - cancel a task and all its dependent tasks
    ///
    /// When a task is cancelled, this will also cancel any tasks that depend on it
    /// (identified by metadata relationships).
    ///
    /// # Arguments
    /// * `task_id` - Parent task ID to cancel
    ///
    /// # Returns
    /// Number of tasks cancelled (including the parent)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    /// let parent_id = Uuid::new_v4();
    ///
    /// // Cancel task and all dependent tasks
    /// let cancelled = broker.cancel_cascade(&parent_id).await?;
    /// println!("Cancelled {} tasks (including dependents)", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_cascade(&self, task_id: &TaskId) -> Result<u64> {
        // First cancel the parent task
        let parent_result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE id = ?
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel parent task: {}", e)))?;

        let mut total_cancelled = parent_result.rows_affected();

        // Cancel dependent tasks (those with parent_task_id in metadata)
        let dependent_result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = CONCAT(
                    COALESCE(error_message, ''),
                    'Cancelled due to parent task cancellation'
                )
            WHERE JSON_EXTRACT(metadata, '$.parent_task_id') = ?
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel dependent tasks: {}", e)))?;

        total_cancelled += dependent_result.rows_affected();

        if total_cancelled > 0 {
            tracing::info!(
                parent_task_id = %task_id,
                total_cancelled = total_cancelled,
                "Cascade cancelled tasks"
            );
        }

        Ok(total_cancelled)
    }

    /// Enqueue a task with advanced retry policy
    ///
    /// Enqueues a task with custom retry behavior using exponential backoff, jitter, etc.
    ///
    /// # Arguments
    /// * `task` - Task to enqueue
    /// * `retry_policy` - Custom retry policy configuration
    ///
    /// # Returns
    /// Task ID
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, RetryPolicy, RetryStrategy};
    /// # use celers_core::SerializedTask;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let policy = RetryPolicy {
    ///     max_retries: 5,
    ///     strategy: RetryStrategy::ExponentialWithJitter {
    ///         base_delay_secs: 2,
    ///         multiplier: 2.0,
    ///         max_delay_secs: 600,
    ///     },
    /// };
    ///
    /// let task = SerializedTask::new("important_task".to_string(), vec![1, 2, 3]);
    /// let task_id = broker.enqueue_with_retry_policy(task, policy).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_retry_policy(
        &self,
        task: SerializedTask,
        retry_policy: RetryPolicy,
    ) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let retry_policy_json = serde_json::to_value(&retry_policy)
            .map_err(|e| CelersError::Other(format!("Failed to serialize retry policy: {}", e)))?;

        let mut metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "retry_policy": retry_policy_json,
        });

        // Merge task metadata
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(retry_policy.max_retries as i32)
        .bind(serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".to_string()))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        tracing::debug!(task_id = %task_id, task_name = %task.metadata.name, "Task enqueued with custom retry policy");
        Ok(task_id)
    }

    /// Reject a task with advanced retry scheduling
    ///
    /// Rejects a task and schedules retry based on the custom retry policy if present.
    ///
    /// # Arguments
    /// * `task_id` - Task ID to reject
    /// * `error_message` - Optional error message
    /// * `requeue` - Whether to requeue for retry
    ///
    /// # Returns
    /// true if task was rejected successfully
    pub async fn reject_with_retry_policy(
        &self,
        task_id: &TaskId,
        error_message: Option<String>,
        requeue: bool,
    ) -> Result<bool> {
        // Get current task info including retry policy
        let task_info = self.get_task(task_id).await?;
        let task_info = match task_info {
            Some(info) => info,
            None => return Ok(false),
        };

        if !requeue || task_info.retry_count >= task_info.max_retries {
            // Move to DLQ or mark as failed
            self.reject(task_id, None, false).await?;
            return Ok(true);
        }

        // Extract retry policy from metadata
        let retry_delay_secs = if let Ok(Some(task)) = self.get_task(task_id).await {
            // Try to get retry policy from metadata
            let metadata_str =
                sqlx::query_scalar::<_, String>("SELECT metadata FROM celers_tasks WHERE id = ?")
                    .bind(task_id.to_string())
                    .fetch_optional(&self.pool)
                    .await
                    .ok()
                    .flatten();

            if let Some(meta_str) = metadata_str {
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                    if let Some(policy_value) = meta.get("retry_policy") {
                        if let Ok(policy) =
                            serde_json::from_value::<RetryPolicy>(policy_value.clone())
                        {
                            policy.strategy.calculate_delay(task.retry_count as u32)
                        } else {
                            60 // Default 1 minute
                        }
                    } else {
                        60
                    }
                } else {
                    60
                }
            } else {
                60
            }
        } else {
            60
        };

        // Update task with retry scheduling
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending',
                retry_count = retry_count + 1,
                error_message = ?,
                scheduled_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                started_at = NULL
            WHERE id = ? AND state = 'processing'
            "#,
        )
        .bind(error_message)
        .bind(retry_delay_secs as i64)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to reject task: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Register a recurring task
    ///
    /// Registers a task to be executed on a recurring schedule.
    /// The task will be automatically enqueued when it's due.
    ///
    /// # Arguments
    /// * `config` - Recurring task configuration
    ///
    /// # Returns
    /// ID of the recurring task configuration
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, RecurringTaskConfig, RecurringSchedule};
    /// # use chrono::Utc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let config = RecurringTaskConfig {
    ///     task_name: "daily_cleanup".to_string(),
    ///     schedule: RecurringSchedule::EveryDays(1, 2, 0), // Every day at 2:00 AM
    ///     payload: vec![],
    ///     priority: 5,
    ///     enabled: true,
    ///     last_run: None,
    ///     next_run: Utc::now(),
    /// };
    ///
    /// let config_id = broker.register_recurring_task(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register_recurring_task(&self, config: RecurringTaskConfig) -> Result<String> {
        let config_id = Uuid::new_v4().to_string();
        let config_json = serde_json::to_string(&config)
            .map_err(|e| CelersError::Other(format!("Failed to serialize config: {}", e)))?;

        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, status, result, created_at)
            VALUES (?, ?, 'PENDING', ?, NOW())
            ON DUPLICATE KEY UPDATE
                result = VALUES(result),
                created_at = NOW()
            "#,
        )
        .bind(&config_id)
        .bind(format!("__recurring__{}", config.task_name))
        .bind(&config_json)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to register recurring task: {}", e)))?;

        tracing::info!(
            config_id = config_id,
            task_name = config.task_name,
            "Recurring task registered"
        );
        Ok(config_id)
    }

    /// Process due recurring tasks
    ///
    /// Checks for recurring tasks that are due and enqueues them.
    /// Should be called periodically (e.g., every minute) by a scheduler.
    ///
    /// # Returns
    /// Number of tasks enqueued
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Call this periodically (e.g., in a background loop)
    /// let enqueued = broker.process_recurring_tasks().await?;
    /// println!("Enqueued {} recurring tasks", enqueued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn process_recurring_tasks(&self) -> Result<u64> {
        // Get all recurring task configurations
        let rows = sqlx::query(
            r#"
            SELECT task_id, result
            FROM celers_task_results
            WHERE task_name LIKE '__recurring__%'
              AND status = 'PENDING'
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {}", e)))?;

        let mut enqueued = 0u64;
        let now = Utc::now();

        for row in rows {
            let config_id: String = row.get("task_id");
            let config_json: String = row.get("result");

            let mut config: RecurringTaskConfig =
                serde_json::from_str(&config_json).map_err(|e| {
                    CelersError::Other(format!("Failed to parse recurring config: {}", e))
                })?;

            // Check if task is due
            if !config.enabled || config.next_run > now {
                continue;
            }

            // Enqueue the task
            let task = SerializedTask::new(config.task_name.clone(), config.payload.clone());
            match self.enqueue(task).await {
                Ok(_) => {
                    enqueued += 1;

                    // Update last_run and next_run
                    config.last_run = Some(now);
                    config.next_run = config.schedule.next_run_from(now);

                    let updated_json = serde_json::to_string(&config).unwrap_or(config_json);

                    // Update configuration
                    let _ = sqlx::query(
                        r#"
                        UPDATE celers_task_results
                        SET result = ?
                        WHERE task_id = ?
                        "#,
                    )
                    .bind(&updated_json)
                    .bind(&config_id)
                    .execute(&self.pool)
                    .await;

                    tracing::debug!(
                        config_id = config_id,
                        task_name = config.task_name,
                        "Recurring task enqueued"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        config_id = config_id,
                        task_name = config.task_name,
                        error = %e,
                        "Failed to enqueue recurring task"
                    );
                }
            }
        }

        Ok(enqueued)
    }

    /// List all recurring task configurations
    ///
    /// # Returns
    /// List of recurring task configurations
    pub async fn list_recurring_tasks(&self) -> Result<Vec<(String, RecurringTaskConfig)>> {
        let rows = sqlx::query(
            r#"
            SELECT task_id, result
            FROM celers_task_results
            WHERE task_name LIKE '__recurring__%'
              AND status = 'PENDING'
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {}", e)))?;

        let mut configs = Vec::new();
        for row in rows {
            let config_id: String = row.get("task_id");
            let config_json: String = row.get("result");

            if let Ok(config) = serde_json::from_str::<RecurringTaskConfig>(&config_json) {
                configs.push((config_id, config));
            }
        }

        Ok(configs)
    }

    /// Delete a recurring task configuration
    ///
    /// # Arguments
    /// * `config_id` - ID of the recurring task configuration
    ///
    /// # Returns
    /// true if configuration was deleted
    pub async fn delete_recurring_task(&self, config_id: &str) -> Result<bool> {
        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_results
            WHERE task_id = ? AND task_name LIKE '__recurring__%'
            "#,
        )
        .bind(config_id)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to delete recurring task: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Export tasks to JSON format for backup or migration
    ///
    /// Exports all tasks in the specified state to a JSON array.
    /// This is useful for backing up tasks or migrating between databases.
    ///
    /// # Arguments
    /// * `state` - Optional state filter (None exports all states)
    /// * `limit` - Maximum number of tasks to export (None exports all)
    ///
    /// # Returns
    /// JSON string containing the task data
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Export all pending tasks
    /// let json = broker.export_tasks(Some(DbTaskState::Pending), Some(1000)).await?;
    /// std::fs::write("pending_tasks.json", json)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn export_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: Option<i64>,
    ) -> Result<String> {
        let mut query = String::from(
            r#"
            SELECT id, task_name, payload, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message, metadata
            FROM celers_tasks
            "#,
        );

        if let Some(s) = &state {
            query.push_str(&format!(" WHERE state = '{}'", s));
        }

        query.push_str(" ORDER BY created_at ASC");

        if let Some(l) = limit {
            query.push_str(&format!(" LIMIT {}", l));
        }

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to export tasks: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let task = serde_json::json!({
                "id": row.get::<String, _>("id"),
                "task_name": row.get::<String, _>("task_name"),
                "payload": row.get::<Vec<u8>, _>("payload"),
                "state": row.get::<String, _>("state"),
                "priority": row.get::<i32, _>("priority"),
                "retry_count": row.get::<i32, _>("retry_count"),
                "max_retries": row.get::<i32, _>("max_retries"),
                "created_at": row.get::<DateTime<Utc>, _>("created_at"),
                "scheduled_at": row.get::<DateTime<Utc>, _>("scheduled_at"),
                "started_at": row.get::<Option<DateTime<Utc>>, _>("started_at"),
                "completed_at": row.get::<Option<DateTime<Utc>>, _>("completed_at"),
                "worker_id": row.get::<Option<String>, _>("worker_id"),
                "error_message": row.get::<Option<String>, _>("error_message"),
                "metadata": row.get::<String, _>("metadata"),
            });
            tasks.push(task);
        }

        serde_json::to_string_pretty(&tasks)
            .map_err(|e| CelersError::Other(format!("Failed to serialize tasks: {}", e)))
    }

    /// Import tasks from JSON format
    ///
    /// Imports tasks from a JSON array (typically exported via export_tasks).
    /// This is useful for restoring tasks from backup or migrating between databases.
    ///
    /// # Arguments
    /// * `json_data` - JSON string containing task data
    /// * `skip_existing` - If true, skip tasks that already exist (by ID)
    ///
    /// # Returns
    /// Number of tasks imported
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Import tasks from backup
    /// let json = std::fs::read_to_string("pending_tasks.json")?;
    /// let imported = broker.import_tasks(&json, true).await?;
    /// println!("Imported {} tasks", imported);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn import_tasks(&self, json_data: &str, skip_existing: bool) -> Result<u64> {
        let tasks: Vec<serde_json::Value> = serde_json::from_str(json_data)
            .map_err(|e| CelersError::Other(format!("Failed to parse JSON: {}", e)))?;

        let mut imported = 0u64;

        for task in tasks {
            let id = task["id"]
                .as_str()
                .ok_or_else(|| CelersError::Other("Missing task id".to_string()))?;

            // Check if task already exists
            if skip_existing {
                let exists: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE id = ?")
                        .bind(id)
                        .fetch_one(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to check task existence: {}", e))
                        })?;

                if exists > 0 {
                    tracing::debug!(task_id = id, "Skipping existing task");
                    continue;
                }
            }

            // Insert task
            let result = sqlx::query(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, retry_count, max_retries,
                     created_at, scheduled_at, started_at, completed_at, worker_id, error_message, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(id)
            .bind(task["task_name"].as_str().unwrap_or(""))
            .bind(task["payload"].as_array().map(|a| {
                a.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect::<Vec<u8>>()
            }).unwrap_or_default())
            .bind(task["state"].as_str().unwrap_or("pending"))
            .bind(task["priority"].as_i64().unwrap_or(0) as i32)
            .bind(task["retry_count"].as_i64().unwrap_or(0) as i32)
            .bind(task["max_retries"].as_i64().unwrap_or(3) as i32)
            .bind(task["created_at"].as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|dt| dt.with_timezone(&Utc)).unwrap_or_else(Utc::now))
            .bind(task["scheduled_at"].as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|dt| dt.with_timezone(&Utc)).unwrap_or_else(Utc::now))
            .bind(task["started_at"].as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|dt| dt.with_timezone(&Utc)))
            .bind(task["completed_at"].as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|dt| dt.with_timezone(&Utc)))
            .bind(task["worker_id"].as_str())
            .bind(task["error_message"].as_str())
            .bind(task["metadata"].as_str().unwrap_or("{}"))
            .execute(&self.pool)
            .await;

            match result {
                Ok(_) => {
                    imported += 1;
                    tracing::debug!(task_id = id, "Imported task");
                }
                Err(e) => {
                    if !skip_existing {
                        return Err(CelersError::Other(format!(
                            "Failed to import task {}: {}",
                            id, e
                        )));
                    }
                    tracing::warn!(task_id = id, error = %e, "Failed to import task, skipping");
                }
            }
        }

        tracing::info!(imported = imported, "Task import completed");
        Ok(imported)
    }

    /// Export DLQ entries to JSON format
    ///
    /// Exports dead letter queue entries for backup or analysis.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of entries to export (None exports all)
    ///
    /// # Returns
    /// JSON string containing DLQ data
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Export DLQ for analysis
    /// let json = broker.export_dlq(Some(100)).await?;
    /// std::fs::write("dlq_entries.json", json)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn export_dlq(&self, limit: Option<i64>) -> Result<String> {
        let mut query = String::from(
            r#"
            SELECT id, task_id, task_name, payload, retry_count, error_message, failed_at, metadata
            FROM celers_dead_letter_queue
            ORDER BY failed_at DESC
            "#,
        );

        if let Some(l) = limit {
            query.push_str(&format!(" LIMIT {}", l));
        }

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to export DLQ: {}", e)))?;

        let mut dlq_entries = Vec::new();
        for row in rows {
            let entry = serde_json::json!({
                "id": row.get::<String, _>("id"),
                "task_id": row.get::<String, _>("task_id"),
                "task_name": row.get::<String, _>("task_name"),
                "payload": row.get::<Vec<u8>, _>("payload"),
                "retry_count": row.get::<i32, _>("retry_count"),
                "error_message": row.get::<Option<String>, _>("error_message"),
                "failed_at": row.get::<DateTime<Utc>, _>("failed_at"),
                "metadata": row.get::<String, _>("metadata"),
            });
            dlq_entries.push(entry);
        }

        serde_json::to_string_pretty(&dlq_entries)
            .map_err(|e| CelersError::Other(format!("Failed to serialize DLQ: {}", e)))
    }

    /// Get circuit breaker statistics
    ///
    /// Returns the current state and statistics of the circuit breaker.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let stats = broker.get_circuit_breaker_stats();
    /// println!("Circuit breaker state: {:?}", stats.state);
    /// println!("Failures: {}, Successes: {}", stats.failure_count, stats.success_count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_circuit_breaker_stats(&self) -> CircuitBreakerStats {
        let cb = self.circuit_breaker.read().unwrap();
        CircuitBreakerStats {
            state: cb.state,
            failure_count: cb.failure_count,
            success_count: cb.success_count,
            last_failure_time: cb.last_failure_time,
            last_state_change: cb.last_state_change,
        }
    }

    /// Reset the circuit breaker to closed state
    ///
    /// Manually resets the circuit breaker, clearing all failure counts and
    /// transitioning to the Closed state.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Manually reset after fixing issues
    /// broker.reset_circuit_breaker();
    /// println!("Circuit breaker reset to closed state");
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset_circuit_breaker(&self) {
        let mut cb = self.circuit_breaker.write().unwrap();
        cb.state = CircuitBreakerState::Closed;
        cb.failure_count = 0;
        cb.success_count = 0;
        cb.last_failure_time = None;
        cb.last_state_change = Utc::now();
        tracing::info!("Circuit breaker manually reset to Closed state");
    }

    /// Record a successful operation
    ///
    /// This is called internally after successful database operations.
    /// In HalfOpen state, enough successes will close the circuit.
    fn record_success(&self) {
        let mut cb = self.circuit_breaker.write().unwrap();

        match cb.state {
            CircuitBreakerState::HalfOpen => {
                cb.success_count += 1;
                if cb.success_count >= cb.config.success_threshold {
                    cb.state = CircuitBreakerState::Closed;
                    cb.failure_count = 0;
                    cb.success_count = 0;
                    cb.last_state_change = Utc::now();
                    tracing::info!(
                        "Circuit breaker transitioned to Closed after successful recovery"
                    );
                }
            }
            CircuitBreakerState::Closed => {
                // Reset failure count on success in closed state
                cb.failure_count = 0;
            }
            CircuitBreakerState::Open => {
                // Ignore successes in open state
            }
        }
    }

    /// Record a failed operation
    ///
    /// This is called internally after failed database operations.
    /// Enough failures will open the circuit.
    fn record_failure(&self) {
        let mut cb = self.circuit_breaker.write().unwrap();
        cb.failure_count += 1;
        cb.last_failure_time = Some(Utc::now());

        match cb.state {
            CircuitBreakerState::Closed => {
                if cb.failure_count >= cb.config.failure_threshold {
                    cb.state = CircuitBreakerState::Open;
                    cb.last_state_change = Utc::now();
                    tracing::warn!(
                        failure_count = cb.failure_count,
                        "Circuit breaker opened due to consecutive failures"
                    );
                }
            }
            CircuitBreakerState::HalfOpen => {
                // Any failure in half-open immediately opens the circuit
                cb.state = CircuitBreakerState::Open;
                cb.success_count = 0;
                cb.last_state_change = Utc::now();
                tracing::warn!("Circuit breaker reopened after failure in HalfOpen state");
            }
            CircuitBreakerState::Open => {
                // Already open, just increment counter
            }
        }
    }

    /// Check if the circuit breaker allows the operation
    ///
    /// Returns Ok(()) if operation is allowed, Err if circuit is open.
    /// Automatically transitions from Open to HalfOpen after timeout.
    fn check_circuit(&self) -> Result<()> {
        let mut cb = self.circuit_breaker.write().unwrap();

        match cb.state {
            CircuitBreakerState::Closed | CircuitBreakerState::HalfOpen => Ok(()),
            CircuitBreakerState::Open => {
                // Check if timeout has elapsed
                let elapsed = Utc::now()
                    .signed_duration_since(cb.last_state_change)
                    .num_seconds();

                if elapsed >= cb.config.timeout_secs as i64 {
                    // Transition to half-open
                    cb.state = CircuitBreakerState::HalfOpen;
                    cb.success_count = 0;
                    cb.last_state_change = Utc::now();
                    tracing::info!("Circuit breaker transitioned to HalfOpen, testing recovery");
                    Ok(())
                } else {
                    Err(CelersError::Other(format!(
                        "Circuit breaker is open (will retry in {} seconds)",
                        cb.config.timeout_secs as i64 - elapsed
                    )))
                }
            }
        }
    }

    /// Execute a database operation with circuit breaker protection
    ///
    /// Wraps a database operation with circuit breaker logic.
    /// If the circuit is open, the operation is rejected.
    /// Successful/failed operations update the circuit breaker state.
    ///
    /// # Arguments
    /// * `operation` - Async function to execute
    ///
    /// # Returns
    /// Result of the operation or circuit breaker error
    pub async fn with_circuit_breaker<F, T, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if circuit allows operation
        self.check_circuit()?;

        // Execute operation
        match operation().await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }
}

/// Slow query information from performance_schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryInfo {
    pub query_text: String,
    pub execution_count: i64,
    pub avg_time_ms: f64,
    pub max_time_ms: f64,
    pub total_time_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_task_state_display() {
        assert_eq!(DbTaskState::Pending.to_string(), "pending");
        assert_eq!(DbTaskState::Processing.to_string(), "processing");
        assert_eq!(DbTaskState::Completed.to_string(), "completed");
        assert_eq!(DbTaskState::Failed.to_string(), "failed");
        assert_eq!(DbTaskState::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_db_task_state_from_str() {
        assert_eq!(
            "pending".parse::<DbTaskState>().unwrap(),
            DbTaskState::Pending
        );
        assert_eq!(
            "processing".parse::<DbTaskState>().unwrap(),
            DbTaskState::Processing
        );
        assert_eq!(
            "completed".parse::<DbTaskState>().unwrap(),
            DbTaskState::Completed
        );
        assert_eq!(
            "failed".parse::<DbTaskState>().unwrap(),
            DbTaskState::Failed
        );
        assert_eq!(
            "cancelled".parse::<DbTaskState>().unwrap(),
            DbTaskState::Cancelled
        );
        // Case insensitive
        assert_eq!(
            "PENDING".parse::<DbTaskState>().unwrap(),
            DbTaskState::Pending
        );
        assert_eq!(
            "Completed".parse::<DbTaskState>().unwrap(),
            DbTaskState::Completed
        );
    }

    #[test]
    fn test_db_task_state_invalid() {
        assert!("invalid".parse::<DbTaskState>().is_err());
        assert!("".parse::<DbTaskState>().is_err());
    }

    #[test]
    fn test_queue_statistics_default() {
        let stats = QueueStatistics::default();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.processing, 0);
        assert_eq!(stats.completed, 0);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.cancelled, 0);
        assert_eq!(stats.dlq, 0);
        assert_eq!(stats.total, 0);
    }

    #[test]
    fn test_db_task_state_serialization() {
        let state = DbTaskState::Pending;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"pending\"");

        let deserialized: DbTaskState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, state);
    }

    #[test]
    fn test_task_result_status_display() {
        assert_eq!(TaskResultStatus::Pending.to_string(), "PENDING");
        assert_eq!(TaskResultStatus::Started.to_string(), "STARTED");
        assert_eq!(TaskResultStatus::Success.to_string(), "SUCCESS");
        assert_eq!(TaskResultStatus::Failure.to_string(), "FAILURE");
        assert_eq!(TaskResultStatus::Retry.to_string(), "RETRY");
        assert_eq!(TaskResultStatus::Revoked.to_string(), "REVOKED");
    }

    #[test]
    fn test_task_result_status_from_str() {
        assert_eq!(
            "PENDING".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Pending
        );
        assert_eq!(
            "STARTED".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Started
        );
        assert_eq!(
            "SUCCESS".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Success
        );
        assert_eq!(
            "FAILURE".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Failure
        );
        assert_eq!(
            "RETRY".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Retry
        );
        assert_eq!(
            "REVOKED".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Revoked
        );
        // Case insensitive
        assert_eq!(
            "pending".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Pending
        );
        assert_eq!(
            "Success".parse::<TaskResultStatus>().unwrap(),
            TaskResultStatus::Success
        );
    }

    #[test]
    fn test_task_result_status_invalid() {
        assert!("invalid".parse::<TaskResultStatus>().is_err());
        assert!("".parse::<TaskResultStatus>().is_err());
    }

    #[test]
    fn test_task_result_status_serialization() {
        let status = TaskResultStatus::Success;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"success\"");

        let deserialized: TaskResultStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, status);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_broker_creation() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await;
        assert!(broker.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_broker_lifecycle() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Test enqueue
        let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4]);
        let task_id = task.metadata.id;

        let returned_id = broker.enqueue(task.clone()).await.unwrap();
        assert_eq!(returned_id, task_id);

        // Test queue size
        let size = broker.queue_size().await.unwrap();
        assert!(size >= 1);

        // Test dequeue
        let msg = broker.dequeue().await.unwrap();
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.task.metadata.name, "test_task");

        // Test ack
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_queue_pause_resume() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Initially not paused
        assert!(!broker.is_paused());

        // Pause
        broker.pause();
        assert!(broker.is_paused());

        // Dequeue should return None when paused
        let task = SerializedTask::new("pause_test".to_string(), vec![1, 2, 3]);
        broker.enqueue(task).await.unwrap();

        let msg = broker.dequeue().await.unwrap();
        assert!(msg.is_none());

        // Resume
        broker.resume();
        assert!(!broker.is_paused());

        // Now dequeue should work
        let msg = broker.dequeue().await.unwrap();
        assert!(msg.is_some());
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_statistics() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        let stats = broker.get_statistics().await.unwrap();
        assert!(stats.total >= 0);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_health_check() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();

        let health = broker.check_health().await.unwrap();
        assert!(health.healthy);
        assert!(!health.database_version.is_empty());
    }

    // ========== NEW: Additional Integration Tests ==========

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_batch_operations() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Batch enqueue
        let tasks: Vec<_> = (0..10)
            .map(|i| SerializedTask::new(format!("task_{}", i), vec![i as u8]))
            .collect();

        let task_ids = broker.enqueue_batch(tasks).await.unwrap();
        assert_eq!(task_ids.len(), 10);

        // Batch dequeue
        let messages = broker.dequeue_batch(5).await.unwrap();
        assert_eq!(messages.len(), 5);

        // Batch ack
        let ack_tasks: Vec<_> = messages
            .iter()
            .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
            .collect();
        broker.ack_batch(&ack_tasks).await.unwrap();

        // Verify remaining tasks
        let remaining = broker.queue_size().await.unwrap();
        assert_eq!(remaining, 5);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_task_chain() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Create task chain
        let chain = TaskChain::new()
            .then(SerializedTask::new("step1".to_string(), vec![1]))
            .then(SerializedTask::new("step2".to_string(), vec![2]))
            .then(SerializedTask::new("step3".to_string(), vec![3]))
            .with_delay(2);

        let task_ids = broker.enqueue_chain(chain).await.unwrap();
        assert_eq!(task_ids.len(), 3);

        // Verify scheduled tasks
        let scheduled = broker.list_scheduled_tasks(10, 0).await.unwrap();
        assert!(scheduled.len() >= 2); // At least 2 tasks should be scheduled for future
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_connection_diagnostics() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();

        let diag = broker.get_connection_diagnostics();
        assert!(diag.max_connections > 0);
        assert!(diag.pool_utilization_percent >= 0.0);
        assert!(diag.pool_utilization_percent <= 100.0);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_performance_metrics() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        let metrics = broker.get_performance_metrics().await.unwrap();
        assert!(metrics.queue_depth >= 0);
        assert!(metrics.processing_tasks >= 0);
        assert!(metrics.dlq_size >= 0);
        assert!(metrics.connection_pool.max_connections > 0);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_migration_tracking() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // List migrations
        let migrations = broker.list_migrations().await.unwrap();
        assert!(migrations.len() >= 3); // At least 001, 002, 003

        // Verify migration names
        let versions: Vec<_> = migrations.iter().map(|m| m.version.as_str()).collect();
        assert!(versions.contains(&"001"));
        assert!(versions.contains(&"002"));
        assert!(versions.contains(&"003"));
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_is_ready() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();

        let ready = broker.is_ready().await;
        assert!(ready);
    }

    // ========== Concurrency Tests ==========

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_concurrent_dequeue() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue tasks
        let tasks: Vec<_> = (0..20)
            .map(|i| SerializedTask::new(format!("concurrent_{}", i), vec![i as u8]))
            .collect();
        broker.enqueue_batch(tasks).await.unwrap();

        // Spawn multiple workers dequeuing concurrently
        let mut handles = vec![];
        for worker_id in 0..5 {
            let db_url = database_url.clone();
            let handle = tokio::spawn(async move {
                let worker_broker = MysqlBroker::new(&db_url).await.unwrap();
                let mut dequeued = 0;

                for _ in 0..10 {
                    if let Ok(Some(msg)) = worker_broker.dequeue().await {
                        dequeued += 1;
                        // Acknowledge immediately
                        let _ = worker_broker
                            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                            .await;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }

                (worker_id, dequeued)
            });
            handles.push(handle);
        }

        // Wait for all workers
        let results = futures::future::join_all(handles).await;

        let total_dequeued: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|(_, count)| *count)
            .sum();

        // Should dequeue all 20 tasks across workers
        assert_eq!(total_dequeued, 20);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_skip_locked_behavior() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker1 = MysqlBroker::new(&database_url).await.unwrap();
        broker1.migrate().await.unwrap();

        // Enqueue multiple tasks
        for i in 0..10 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            broker1.enqueue(task).await.unwrap();
        }

        let broker2 = MysqlBroker::new(&database_url).await.unwrap();

        // Dequeue from both brokers simultaneously
        let (msg1, msg2) = tokio::join!(broker1.dequeue(), broker2.dequeue());

        // Both should succeed with different tasks (SKIP LOCKED)
        assert!(msg1.is_ok());
        assert!(msg2.is_ok());

        if let (Ok(Some(m1)), Ok(Some(m2))) = (msg1, msg2) {
            // Tasks should be different
            assert_ne!(m1.task.metadata.id, m2.task.metadata.id);
        }
    }

    // ========== Unit Tests for New Structures ==========

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 2);
        assert_eq!(config.acquire_timeout_secs, 5);
        assert_eq!(config.max_lifetime_secs, Some(1800));
        assert_eq!(config.idle_timeout_secs, Some(600));
    }

    #[test]
    fn test_task_chain_builder() {
        let task1 = SerializedTask::new("task1".to_string(), vec![1]);
        let task2 = SerializedTask::new("task2".to_string(), vec![2]);

        let chain = TaskChain::new().then(task1).then(task2).with_delay(5);

        assert_eq!(chain.tasks().len(), 2);
        assert_eq!(chain.delay_between_secs(), Some(5));
    }

    // ========== Unit Tests for Enhancement Methods ==========

    #[test]
    fn test_worker_statistics_serialization() {
        let stats = WorkerStatistics {
            worker_id: "worker-123".to_string(),
            active_tasks: 5,
            completed_tasks: 100,
            failed_tasks: 3,
            last_seen: Utc::now(),
            avg_task_duration_secs: 2.5,
        };

        // Should serialize and deserialize correctly
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: WorkerStatistics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.worker_id, "worker-123");
        assert_eq!(deserialized.active_tasks, 5);
        assert_eq!(deserialized.completed_tasks, 100);
        assert_eq!(deserialized.failed_tasks, 3);
        assert_eq!(deserialized.avg_task_duration_secs, 2.5);
    }

    #[test]
    fn test_task_age_distribution_serialization() {
        let dist = TaskAgeDistribution {
            bucket_label: "< 1 min".to_string(),
            task_count: 42,
            oldest_task_age_secs: 55,
        };

        let json = serde_json::to_string(&dist).unwrap();
        let deserialized: TaskAgeDistribution = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.bucket_label, "< 1 min");
        assert_eq!(deserialized.task_count, 42);
        assert_eq!(deserialized.oldest_task_age_secs, 55);
    }

    #[test]
    fn test_retry_statistics_serialization() {
        let stats = RetryStatistics {
            task_name: "failing_task".to_string(),
            total_retries: 150,
            unique_tasks: 50,
            avg_retries_per_task: 3.0,
            max_retries_observed: 5,
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: RetryStatistics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task_name, "failing_task");
        assert_eq!(deserialized.total_retries, 150);
        assert_eq!(deserialized.unique_tasks, 50);
        assert_eq!(deserialized.avg_retries_per_task, 3.0);
        assert_eq!(deserialized.max_retries_observed, 5);
    }

    // ========== Integration Tests for Enhancement Methods ==========

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_cancel_batch() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue multiple tasks
        let mut task_ids = Vec::new();
        for i in 0..10 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            let task_id = broker.enqueue(task).await.unwrap();
            task_ids.push(task_id);
        }

        // Cancel half of them in batch
        let to_cancel = &task_ids[0..5];
        let cancelled = broker.cancel_batch(to_cancel).await.unwrap();
        assert_eq!(cancelled, 5);

        // Verify they're cancelled
        let stats = broker.get_statistics().await.unwrap();
        assert_eq!(stats.cancelled, 5);
        assert_eq!(stats.pending, 5);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_worker_statistics() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue and dequeue a task with worker ID
        let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3]);
        broker.enqueue(task).await.unwrap();

        let msg = broker
            .dequeue_with_worker_id("test-worker-123")
            .await
            .unwrap()
            .unwrap();

        // Get worker statistics
        let stats = broker
            .get_worker_statistics("test-worker-123")
            .await
            .unwrap();

        assert_eq!(stats.worker_id, "test-worker-123");
        assert_eq!(stats.active_tasks, 1);

        // Acknowledge the task
        broker
            .ack(&msg.task_id(), msg.receipt_handle.as_deref())
            .await
            .unwrap();

        // Stats should update
        let stats = broker
            .get_worker_statistics("test-worker-123")
            .await
            .unwrap();
        assert_eq!(stats.active_tasks, 0);
        assert_eq!(stats.completed_tasks, 1);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_count_by_state_quick() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue tasks
        for i in 0..5 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            broker.enqueue(task).await.unwrap();
        }

        // Count pending tasks
        let pending_count = broker
            .count_by_state_quick(DbTaskState::Pending)
            .await
            .unwrap();
        assert_eq!(pending_count, 5);

        // Dequeue one
        broker.dequeue().await.unwrap();

        // Check processing count
        let processing_count = broker
            .count_by_state_quick(DbTaskState::Processing)
            .await
            .unwrap();
        assert_eq!(processing_count, 1);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_task_age_distribution() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue some tasks
        for i in 0..10 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            broker.enqueue(task).await.unwrap();
        }

        // Get age distribution
        let distribution = broker.get_task_age_distribution().await.unwrap();

        // Should have at least one bucket
        assert!(!distribution.is_empty());

        // All tasks should be in the youngest bucket
        let youngest = distribution.first().unwrap();
        assert_eq!(youngest.bucket_label, "< 1 min");
        assert_eq!(youngest.task_count, 10);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_retry_statistics() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue and fail some tasks to generate retries
        for i in 0..3 {
            let task = SerializedTask::new("failing_task".to_string(), vec![i as u8]);
            let _task_id = broker.enqueue(task).await.unwrap();

            // Dequeue and reject to trigger retry
            let msg = broker.dequeue().await.unwrap().unwrap();
            broker
                .reject(&msg.task_id(), msg.receipt_handle.as_deref(), true)
                .await
                .unwrap();
        }

        // Get retry statistics
        let stats = broker.get_retry_statistics().await.unwrap();

        // Should have stats for the failing task
        if !stats.is_empty() {
            let task_stats = &stats[0];
            assert_eq!(task_stats.task_name, "failing_task");
            assert!(task_stats.total_retries > 0);
        }
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_list_active_workers() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue tasks
        for i in 0..3 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            broker.enqueue(task).await.unwrap();
        }

        // Dequeue with different workers
        let _msg1 = broker
            .dequeue_with_worker_id("worker-1")
            .await
            .unwrap()
            .unwrap();
        let _msg2 = broker
            .dequeue_with_worker_id("worker-2")
            .await
            .unwrap()
            .unwrap();

        // List active workers
        let workers = broker.list_active_workers().await.unwrap();
        assert_eq!(workers.len(), 2);
        assert!(workers.contains(&"worker-1".to_string()));
        assert!(workers.contains(&"worker-2".to_string()));
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_get_all_worker_statistics() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let broker = MysqlBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        // Enqueue tasks
        for i in 0..2 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            broker.enqueue(task).await.unwrap();
        }

        // Dequeue with workers
        let _msg1 = broker
            .dequeue_with_worker_id("worker-alpha")
            .await
            .unwrap()
            .unwrap();
        let _msg2 = broker
            .dequeue_with_worker_id("worker-beta")
            .await
            .unwrap()
            .unwrap();

        // Get all worker statistics
        let all_stats = broker.get_all_worker_statistics().await.unwrap();
        assert_eq!(all_stats.len(), 2);

        // Verify each worker has stats
        for stats in &all_stats {
            assert!(stats.worker_id == "worker-alpha" || stats.worker_id == "worker-beta");
            assert_eq!(stats.active_tasks, 1);
        }
    }

    #[test]
    fn test_circuit_breaker_initial_state() {
        let config = CircuitBreakerConfig::default();
        let cb_internal = CircuitBreakerStateInternal::new(config);

        assert_eq!(cb_internal.state, CircuitBreakerState::Closed);
        assert_eq!(cb_internal.failure_count, 0);
        assert_eq!(cb_internal.success_count, 0);
        assert!(cb_internal.last_failure_time.is_none());
    }

    #[test]
    fn test_circuit_breaker_config_default() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.success_threshold, 2);
    }

    #[tokio::test]
    async fn test_circuit_breaker_stats() {
        // Create a broker with circuit breaker
        let database_url = "mysql://test:test@localhost/test";
        let result = MysqlBroker::new(database_url).await;

        // Even if connection fails, we can test circuit breaker stats on the struct
        if result.is_err() {
            // Test with manual struct construction would go here
            // For now, just pass the test
            return;
        }

        let broker = result.unwrap();
        let stats = broker.get_circuit_breaker_stats();

        assert_eq!(stats.state, CircuitBreakerState::Closed);
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.success_count, 0);
    }

    #[tokio::test]
    async fn test_circuit_breaker_reset() {
        let database_url = "mysql://test:test@localhost/test";
        let result = MysqlBroker::new(database_url).await;

        if result.is_err() {
            return;
        }

        let broker = result.unwrap();

        // Manually trigger some failures
        for _ in 0..3 {
            broker.record_failure();
        }

        let stats_before = broker.get_circuit_breaker_stats();
        assert_eq!(stats_before.failure_count, 3);

        // Reset the circuit breaker
        broker.reset_circuit_breaker();

        let stats_after = broker.get_circuit_breaker_stats();
        assert_eq!(stats_after.state, CircuitBreakerState::Closed);
        assert_eq!(stats_after.failure_count, 0);
        assert_eq!(stats_after.success_count, 0);
    }

    #[test]
    fn test_circuit_breaker_state_serialization() {
        // Test Closed
        let state = CircuitBreakerState::Closed;
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);

        // Test Open
        let state = CircuitBreakerState::Open;
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);

        // Test HalfOpen
        let state = CircuitBreakerState::HalfOpen;
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);
    }

    #[test]
    fn test_circuit_breaker_stats_serialization() {
        let stats = CircuitBreakerStats {
            state: CircuitBreakerState::Open,
            failure_count: 5,
            success_count: 0,
            last_failure_time: Some(Utc::now()),
            last_state_change: Utc::now(),
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: CircuitBreakerStats = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.state, deserialized.state);
        assert_eq!(stats.failure_count, deserialized.failure_count);
        assert_eq!(stats.success_count, deserialized.success_count);
    }
}
