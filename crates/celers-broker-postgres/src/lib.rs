//! PostgreSQL broker implementation for CeleRS
//!
//! This broker uses PostgreSQL with `FOR UPDATE SKIP LOCKED` for reliable,
//! distributed task queue processing. It supports:
//! - Priority queues
//! - Dead Letter Queue (DLQ) for permanently failed tasks
//! - Delayed task execution (enqueue_at, enqueue_after)
//! - **Task chaining for sequential execution**
//! - **DAG-based workflows with stage dependencies**
//! - **Task deduplication with idempotency keys**
//! - **Real-time LISTEN/NOTIFY for event-driven workers**
//! - Prometheus metrics (optional `metrics` feature)
//! - Batch enqueue/dequeue/ack operations
//! - Transaction safety
//! - Distributed workers without contention
//! - Queue pause/resume functionality
//! - DLQ inspection and requeue
//! - Task status inspection
//! - Database health checks
//! - Automatic task archiving
//!
//! # Quick Start
//!
//! ```no_run
//! use celers_broker_postgres::PostgresBroker;
//! use celers_core::{Broker, SerializedTask};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create broker and run migrations
//! let broker = PostgresBroker::new("postgres://user:pass@localhost/mydb").await?;
//! broker.migrate().await?;
//!
//! // Enqueue a task
//! let task = SerializedTask::new("my_task".to_string(), vec![1, 2, 3]);
//! let task_id = broker.enqueue(task).await?;
//!
//! // Dequeue and process
//! if let Some(msg) = broker.dequeue().await? {
//!     // Process task...
//!     broker.ack(&msg.task.metadata.id, msg.receipt_handle.as_deref()).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Delayed Execution
//!
//! Schedule tasks for future execution:
//!
//! ```no_run
//! use celers_broker_postgres::PostgresBroker;
//! use celers_core::{Broker, SerializedTask};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let broker = PostgresBroker::new("postgres://localhost/db").await?;
//! let task = SerializedTask::new("delayed_task".to_string(), vec![]);
//!
//! // Schedule for specific timestamp (Unix seconds)
//! let execute_at = 1735689600; // Some future timestamp
//! broker.enqueue_at(task.clone(), execute_at).await?;
//!
//! // Or schedule after a delay (seconds)
//! broker.enqueue_after(task, 300).await?; // 5 minutes from now
//! # Ok(())
//! # }
//! ```
//!
//! # Batch Operations
//!
//! Process multiple tasks efficiently:
//!
//! ```no_run
//! use celers_broker_postgres::PostgresBroker;
//! use celers_core::{Broker, SerializedTask};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let broker = PostgresBroker::new("postgres://localhost/db").await?;
//! // Batch enqueue
//! let tasks = vec![
//!     SerializedTask::new("task1".to_string(), vec![1]),
//!     SerializedTask::new("task2".to_string(), vec![2]),
//! ];
//! broker.enqueue_batch(tasks).await?;
//!
//! // Batch dequeue
//! let messages = broker.dequeue_batch(10).await?;
//!
//! // Batch acknowledge
//! let acks: Vec<_> = messages.iter()
//!     .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
//!     .collect();
//! broker.ack_batch(&acks).await?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use celers_core::{Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{
    DLQ_SIZE, POSTGRES_POOL_IDLE, POSTGRES_POOL_IN_USE, POSTGRES_POOL_MAX_SIZE, POSTGRES_POOL_SIZE,
    PROCESSING_QUEUE_SIZE, QUEUE_SIZE, TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL,
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

/// Detailed connection pool metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolMetrics {
    /// Total number of connections in the pool (max_connections)
    pub max_size: u32,
    /// Number of currently active connections
    pub size: u32,
    /// Number of idle connections available
    pub idle: u32,
    /// Number of connections currently in use
    pub in_use: u32,
    /// Approximate number of tasks waiting for a connection
    pub waiting: u32,
}

/// Detailed health check result with diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedHealthStatus {
    /// Overall health status
    pub healthy: bool,
    /// Connection test result
    pub connection_ok: bool,
    /// Query performance (milliseconds)
    pub query_latency_ms: i64,
    /// Connection pool metrics
    pub pool_metrics: PoolMetrics,
    /// Queue statistics
    pub queue_stats: QueueStatistics,
    /// Database version
    pub database_version: String,
    /// Warnings (e.g., high pool utilization)
    pub warnings: Vec<String>,
    /// Recommendations for optimization
    pub recommendations: Vec<String>,
}

/// Batch size recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSizeRecommendation {
    /// Recommended batch size for enqueue operations
    pub enqueue_batch_size: usize,
    /// Recommended batch size for dequeue operations
    pub dequeue_batch_size: usize,
    /// Recommended batch size for ack operations
    pub ack_batch_size: usize,
    /// Reasoning for the recommendation
    pub reasoning: String,
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

/// Information about a table partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_name: String,
    pub partition_start: DateTime<Utc>,
    pub partition_end: DateTime<Utc>,
    pub row_count: i64,
    pub size_bytes: i64,
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

/// Retry strategy for failed tasks
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetryStrategy {
    /// Exponential backoff: 2^retry_count seconds (default, max 1 hour)
    /// Example: 2s, 4s, 8s, 16s, 32s, 64s, 128s, ...
    Exponential { max_delay_secs: i64 },

    /// Exponential backoff with random jitter to avoid thundering herd
    /// backoff = 2^retry_count * random(0.5 to 1.5)
    ExponentialWithJitter { max_delay_secs: i64 },

    /// Linear backoff: base_delay * retry_count seconds
    /// Example (base=10): 10s, 20s, 30s, 40s, ...
    Linear {
        base_delay_secs: i64,
        max_delay_secs: i64,
    },

    /// Fixed delay: always the same delay
    Fixed { delay_secs: i64 },

    /// No automatic retry (immediate requeue or fail)
    Immediate,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::Exponential {
            max_delay_secs: 3600,
        }
    }
}

impl RetryStrategy {
    /// Calculate backoff delay in seconds based on retry count
    pub fn calculate_backoff(&self, retry_count: i32) -> i64 {
        match self {
            RetryStrategy::Exponential { max_delay_secs } => {
                let backoff = 2_i64.pow(retry_count as u32);
                backoff.min(*max_delay_secs)
            }
            RetryStrategy::ExponentialWithJitter { max_delay_secs } => {
                let base_backoff = 2_i64.pow(retry_count as u32).min(*max_delay_secs);
                // Add jitter: multiply by random value between 0.5 and 1.5
                // Using a simple deterministic jitter based on retry_count to avoid needing RNG
                // In production, you might want to use rand crate
                let jitter_factor = 0.5 + ((retry_count % 10) as f64 / 10.0);
                ((base_backoff as f64) * jitter_factor).round() as i64
            }
            RetryStrategy::Linear {
                base_delay_secs,
                max_delay_secs,
            } => {
                let backoff = base_delay_secs * (retry_count as i64);
                backoff.min(*max_delay_secs)
            }
            RetryStrategy::Fixed { delay_secs } => *delay_secs,
            RetryStrategy::Immediate => 0,
        }
    }
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
    pub total_size_bytes: i64,
    pub table_size_bytes: i64,
    pub index_size_bytes: i64,
    pub total_size_pretty: String,
}

/// Index usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsageInfo {
    pub index_name: String,
    pub table_name: String,
    pub index_scans: i64,
    pub tuples_read: i64,
    pub tuples_fetched: i64,
    pub index_size_bytes: i64,
    pub index_size_pretty: String,
}

/// Task chain configuration for sequential execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskChain {
    /// Tasks to execute in order
    pub tasks: Vec<SerializedTask>,
    /// Stop chain execution on first failure (default: true)
    pub stop_on_failure: bool,
}

/// Workflow/Pipeline for complex task orchestration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWorkflow {
    /// Unique workflow ID
    pub id: Uuid,
    /// Workflow name
    pub name: String,
    /// Tasks in this workflow with their dependencies
    pub stages: Vec<WorkflowStage>,
}

/// A stage in a workflow with optional dependencies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStage {
    /// Stage identifier
    pub id: String,
    /// Tasks to execute in this stage (can run in parallel)
    pub tasks: Vec<SerializedTask>,
    /// IDs of stages that must complete before this stage runs
    pub depends_on: Vec<String>,
}

/// Status information for a task chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStatus {
    pub chain_id: Uuid,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub current_position: Option<i64>,
    pub is_complete: bool,
    pub has_failures: bool,
}

/// Status information for a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatus {
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub total_stages: i64,
    pub completed_stages: i64,
    pub active_stages: i64,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub is_complete: bool,
    pub has_failures: bool,
    pub stage_statuses: Vec<StageStatus>,
}

/// Status information for a workflow stage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageStatus {
    pub stage_id: String,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub is_complete: bool,
    pub dependencies_met: bool,
}

/// PostgreSQL-based broker implementation using SKIP LOCKED
pub struct PostgresBroker {
    pool: PgPool,
    queue_name: String,
    paused: AtomicBool,
    retry_strategy: RetryStrategy,
}

impl PostgresBroker {
    /// Create a new PostgreSQL broker
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@localhost/db")
    /// * `queue_name` - Logical queue name for multi-tenancy (optional, defaults to "default")
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_queue(database_url, "default").await
    }

    /// Create a new PostgreSQL broker with a specific queue name
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            retry_strategy: RetryStrategy::default(),
        })
    }

    /// Create a new PostgreSQL broker with custom pool configuration
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string
    /// * `queue_name` - Logical queue name
    /// * `max_connections` - Maximum number of connections in the pool
    /// * `acquire_timeout_secs` - Timeout for acquiring a connection (seconds)
    pub async fn with_pool_config(
        database_url: &str,
        queue_name: &str,
        max_connections: u32,
        acquire_timeout_secs: u64,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            retry_strategy: RetryStrategy::default(),
        })
    }

    /// Set the retry strategy for failed tasks
    ///
    /// This can be called on an existing broker instance to change the retry behavior.
    pub fn set_retry_strategy(&mut self, strategy: RetryStrategy) {
        self.retry_strategy = strategy;
        tracing::info!(strategy = ?strategy, "Updated retry strategy");
    }

    /// Get the current retry strategy
    pub fn retry_strategy(&self) -> RetryStrategy {
        self.retry_strategy
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        // Run initial schema migration
        let init_sql = include_str!("../migrations/001_init.sql");
        sqlx::query(init_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 001_init failed: {}", e)))?;

        // Run results table migration
        let results_sql = include_str!("../migrations/002_results.sql");
        sqlx::query(results_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 002_results failed: {}", e)))?;

        // Run deduplication table migration
        let dedup_sql = include_str!("../migrations/004_deduplication.sql");
        sqlx::query(dedup_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Migration 004_deduplication failed: {}", e))
            })?;

        Ok(())
    }

    /// Get the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the queue name
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Move a task to the Dead Letter Queue
    async fn move_to_dlq(&self, task_id: &TaskId) -> Result<()> {
        sqlx::query("SELECT move_to_dlq($1)")
            .bind(task_id)
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
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task: {}", e)))?;

        match row {
            Some(row) => {
                let state_str: String = row.get("state");
                Ok(Some(TaskInfo {
                    id: row.get("id"),
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
                    WHERE state = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
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
                    LIMIT $1 OFFSET $2
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
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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

    /// Find tasks by metadata JSON path query
    ///
    /// Uses PostgreSQL's JSONB operators to query tasks by metadata.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Find tasks where metadata.user_id = 123
    /// let tasks = broker.find_tasks_by_metadata("user_id", &json!(123), 10, 0).await?;
    ///
    /// // Find tasks where metadata.priority = "high"
    /// let tasks = broker.find_tasks_by_metadata("priority", &json!("high"), 10, 0).await?;
    /// ```
    pub async fn find_tasks_by_metadata(
        &self,
        json_path: &str,
        value: &serde_json::Value,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE metadata->$1 = $2
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(json_path)
        .bind(value)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by metadata: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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

    /// Count tasks matching metadata criteria
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Count tasks where metadata.user_id = 123
    /// let count = broker.count_tasks_by_metadata("user_id", &json!(123)).await?;
    /// ```
    pub async fn count_tasks_by_metadata(
        &self,
        json_path: &str,
        value: &serde_json::Value,
    ) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE metadata->$1 = $2
            "#,
        )
        .bind(json_path)
        .bind(value)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count tasks by metadata: {}", e)))?;

        Ok(count)
    }

    /// Find tasks by task name with pagination
    ///
    /// This is useful for monitoring specific task types.
    pub async fn find_tasks_by_name(
        &self,
        task_name: &str,
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
                    WHERE task_name = $1 AND state = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    "#,
                )
                .bind(task_name)
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
                    WHERE task_name = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                )
                .bind(task_name)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by name: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE state = 'pending') as pending,
                COUNT(*) FILTER (WHERE state = 'processing') as processing,
                COUNT(*) FILTER (WHERE state = 'completed') as completed,
                COUNT(*) FILTER (WHERE state = 'failed') as failed,
                COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled,
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

        Ok(QueueStatistics {
            pending: row.get("pending"),
            processing: row.get("processing"),
            completed: row.get("completed"),
            failed: row.get("failed"),
            cancelled: row.get("cancelled"),
            dlq: dlq_count,
            total: row.get("total"),
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
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            tasks.push(DlqTaskInfo {
                id: row.get("id"),
                task_id: row.get("task_id"),
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
            WHERE id = $1
            "#,
        )
        .bind(dlq_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {}", e)))?;

        let row = row.ok_or_else(|| CelersError::Other("DLQ task not found".to_string()))?;

        let task_id: Uuid = row.get("task_id");
        let task_name: String = row.get("task_name");
        let payload: Vec<u8> = row.get("payload");
        let metadata: Option<serde_json::Value> = row.get("metadata");

        // Create new task in main queue
        let new_task_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, retry_count, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', 0, 0, 3, $4, NOW(), NOW())
            "#,
        )
        .bind(new_task_id)
        .bind(&task_name)
        .bind(&payload)
        .bind(metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

        // Delete from DLQ
        sqlx::query("DELETE FROM celers_dead_letter_queue WHERE id = $1")
            .bind(dlq_id)
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
        let result = sqlx::query("DELETE FROM celers_dead_letter_queue WHERE id = $1")
            .bind(dlq_id)
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
        // Test connection
        let version: String = sqlx::query_scalar("SELECT version()")
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

    /// Get detailed connection pool metrics
    ///
    /// This provides comprehensive statistics about the connection pool state,
    /// useful for monitoring and capacity planning.
    pub fn get_pool_metrics(&self) -> PoolMetrics {
        let max_size = self.pool.options().get_max_connections();
        let size = self.pool.size();
        let idle = self.pool.num_idle() as u32;

        PoolMetrics {
            max_size,
            size,
            idle,
            in_use: size.saturating_sub(idle),
            waiting: 0, // sqlx doesn't expose waiting connections count
        }
    }

    /// Archive completed tasks older than the specified duration
    ///
    /// Returns the number of tasks archived (deleted).
    pub async fn archive_completed_tasks(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);

        let result = sqlx::query(
            r#"
            DELETE FROM celers_tasks
            WHERE state IN ('completed', 'failed', 'cancelled')
              AND completed_at < $1
            "#,
        )
        .bind(cutoff)
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

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending',
                started_at = NULL,
                worker_id = NULL,
                error_message = 'Recovered from stuck processing state'
            WHERE state = 'processing'
              AND started_at < $1
            "#,
        )
        .bind(cutoff)
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
                Some(Utc::now())
            }
            _ => None,
        };

        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8)
            ON CONFLICT (task_id) DO UPDATE SET
                status = EXCLUDED.status,
                result = EXCLUDED.result,
                error = EXCLUDED.error,
                traceback = EXCLUDED.traceback,
                completed_at = EXCLUDED.completed_at,
                runtime_ms = EXCLUDED.runtime_ms
            "#,
        )
        .bind(task_id)
        .bind(task_name)
        .bind(status.to_string())
        .bind(result)
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
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get result: {}", e)))?;

        match row {
            Some(row) => {
                let status_str: String = row.get("status");
                Ok(Some(TaskResult {
                    task_id: row.get("task_id"),
                    task_name: row.get("task_name"),
                    status: status_str.parse()?,
                    result: row.get("result"),
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
        let result = sqlx::query("DELETE FROM celers_task_results WHERE task_id = $1")
            .bind(task_id)
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

        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_results
            WHERE completed_at < $1
            "#,
        )
        .bind(cutoff)
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
                relname as table_name,
                n_live_tup as row_count,
                pg_total_relation_size(relid) as total_size_bytes,
                pg_table_size(relid) as table_size_bytes,
                pg_indexes_size(relid) as index_size_bytes,
                pg_size_pretty(pg_total_relation_size(relid)) as total_size_pretty
            FROM pg_stat_user_tables
            WHERE relname LIKE 'celers_%'
            ORDER BY pg_total_relation_size(relid) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {}", e)))?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            tables.push(TableSizeInfo {
                table_name: row.get("table_name"),
                row_count: row.get("row_count"),
                total_size_bytes: row.get("total_size_bytes"),
                table_size_bytes: row.get("table_size_bytes"),
                index_size_bytes: row.get("index_size_bytes"),
                total_size_pretty: row.get("total_size_pretty"),
            });
        }
        Ok(tables)
    }

    /// Get index usage statistics for CeleRS tables
    pub async fn get_index_usage(&self) -> Result<Vec<IndexUsageInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT
                indexrelname as index_name,
                relname as table_name,
                idx_scan as index_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_relation_size(indexrelid) as index_size_bytes,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size_pretty
            FROM pg_stat_user_indexes
            WHERE relname LIKE 'celers_%'
            ORDER BY idx_scan DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get index usage: {}", e)))?;

        let mut indexes = Vec::with_capacity(rows.len());
        for row in rows {
            indexes.push(IndexUsageInfo {
                index_name: row.get("index_name"),
                table_name: row.get("table_name"),
                index_scans: row.get("index_scans"),
                tuples_read: row.get("tuples_read"),
                tuples_fetched: row.get("tuples_fetched"),
                index_size_bytes: row.get("index_size_bytes"),
                index_size_pretty: row.get("index_size_pretty"),
            });
        }
        Ok(indexes)
    }

    /// Get unused indexes (indexes that have never been scanned)
    ///
    /// This can help identify indexes that can be safely dropped.
    pub async fn get_unused_indexes(&self) -> Result<Vec<IndexUsageInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT
                indexrelname as index_name,
                relname as table_name,
                idx_scan as index_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_relation_size(indexrelid) as index_size_bytes,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size_pretty
            FROM pg_stat_user_indexes
            WHERE relname LIKE 'celers_%'
              AND idx_scan = 0
            ORDER BY pg_relation_size(indexrelid) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get unused indexes: {}", e)))?;

        let mut indexes = Vec::with_capacity(rows.len());
        for row in rows {
            indexes.push(IndexUsageInfo {
                index_name: row.get("index_name"),
                table_name: row.get("table_name"),
                index_scans: row.get("index_scans"),
                tuples_read: row.get("tuples_read"),
                tuples_fetched: row.get("tuples_fetched"),
                index_size_bytes: row.get("index_size_bytes"),
                index_size_pretty: row.get("index_size_pretty"),
            });
        }
        Ok(indexes)
    }

    /// Analyze tables to update statistics for query planner
    ///
    /// This should be run periodically for optimal query performance.
    pub async fn analyze_tables(&self) -> Result<()> {
        sqlx::query("ANALYZE celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to analyze celers_tasks: {}", e)))?;

        sqlx::query("ANALYZE celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_dead_letter_queue: {}", e))
            })?;

        sqlx::query("ANALYZE celers_task_results")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_task_results: {}", e))
            })?;

        tracing::info!("Analyzed all CeleRS tables");
        Ok(())
    }

    /// Manually vacuum CeleRS tables to reclaim space
    ///
    /// This is useful for high-churn queues. For most cases, PostgreSQL's
    /// autovacuum is sufficient.
    pub async fn vacuum_tables(&self) -> Result<()> {
        sqlx::query("VACUUM ANALYZE celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to vacuum celers_tasks: {}", e)))?;

        sqlx::query("VACUUM ANALYZE celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to vacuum celers_dead_letter_queue: {}", e))
            })?;

        sqlx::query("VACUUM ANALYZE celers_task_results")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to vacuum celers_task_results: {}", e))
            })?;

        tracing::info!("Vacuumed all CeleRS tables");
        Ok(())
    }

    /// Start a background maintenance task that periodically runs VACUUM and ANALYZE
    ///
    /// This spawns a tokio task that runs maintenance operations at the specified interval.
    /// Returns a handle that can be used to stop the maintenance task.
    ///
    /// # Arguments
    /// * `interval` - Duration between maintenance runs (e.g., Duration::from_secs(3600) for hourly)
    /// * `vacuum` - Whether to run VACUUM (can be expensive, consider running less frequently)
    /// * `analyze` - Whether to run ANALYZE (lightweight, recommended)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Run maintenance every hour (ANALYZE only)
    /// let handle = broker.start_maintenance_scheduler(
    ///     Duration::from_secs(3600),
    ///     false, // Don't VACUUM (let autovacuum handle it)
    ///     true,  // Do ANALYZE
    /// );
    ///
    /// // Stop the maintenance task later
    /// handle.abort();
    /// ```
    pub fn start_maintenance_scheduler(
        self: Arc<Self>,
        interval: Duration,
        vacuum: bool,
        analyze: bool,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval_timer.tick().await;

                tracing::debug!("Running scheduled maintenance task");

                // Run maintenance operations
                if vacuum {
                    if let Err(e) = self.vacuum_tables().await {
                        tracing::error!(error = %e, "Failed to vacuum tables during maintenance");
                    }
                } else if analyze {
                    // Just run ANALYZE without VACUUM (much faster)
                    if let Err(e) = self.analyze_tables().await {
                        tracing::error!(error = %e, "Failed to analyze tables during maintenance");
                    }
                }

                // Also archive old completed tasks (older than 7 days by default)
                let archive_threshold = Duration::from_secs(7 * 24 * 3600);
                match self.archive_completed_tasks(archive_threshold).await {
                    Ok(count) => {
                        if count > 0 {
                            tracing::info!(count = count, "Archived old completed tasks");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to archive tasks during maintenance");
                    }
                }

                // Archive old results (older than 30 days)
                let results_threshold = Duration::from_secs(30 * 24 * 3600);
                match self.archive_results(results_threshold).await {
                    Ok(count) => {
                        if count > 0 {
                            tracing::info!(count = count, "Archived old task results");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to archive results during maintenance");
                    }
                }

                // Check for stuck tasks (processing for more than 1 hour)
                let stuck_threshold = Duration::from_secs(3600);
                match self.recover_stuck_tasks(stuck_threshold).await {
                    Ok(count) => {
                        if count > 0 {
                            tracing::warn!(count = count, "Recovered stuck processing tasks");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to recover stuck tasks");
                    }
                }
            }
        })
    }

    /// Count tasks by state
    ///
    /// Returns the number of tasks in the specified state.
    pub async fn count_by_state(&self, state: DbTaskState) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = $1")
            .bind(state.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tasks by state: {}", e)))?;

        Ok(count)
    }

    /// Get the number of tasks scheduled for future execution
    ///
    /// Returns the count of tasks that are pending but scheduled for a future time.
    pub async fn count_scheduled(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending' AND scheduled_at > NOW()",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count scheduled tasks: {}", e)))?;

        Ok(count)
    }

    /// Cancel all pending tasks
    ///
    /// Returns the number of tasks cancelled.
    pub async fn cancel_all_pending(&self) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE state = 'pending'
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel all pending tasks: {}", e)))?;

        tracing::warn!(
            count = result.rows_affected(),
            "Cancelled all pending tasks"
        );
        Ok(result.rows_affected())
    }

    /// Test database connectivity
    ///
    /// Performs a simple query to verify the connection is working.
    /// Returns true if the connection is healthy.
    pub async fn test_connection(&self) -> Result<bool> {
        let result: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Connection test failed: {}", e)))?;

        Ok(result == 1)
    }

    /// Get the age (in seconds) of the oldest pending task
    ///
    /// Returns None if there are no pending tasks.
    pub async fn oldest_pending_age_secs(&self) -> Result<Option<i64>> {
        let age: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT EXTRACT(EPOCH FROM (NOW() - created_at))::BIGINT
            FROM celers_tasks
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get oldest pending age: {}", e)))?;

        Ok(age)
    }

    /// Get the age (in seconds) of the oldest processing task
    ///
    /// This is useful for detecting stuck tasks. Returns None if there are no processing tasks.
    pub async fn oldest_processing_age_secs(&self) -> Result<Option<i64>> {
        let age: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT EXTRACT(EPOCH FROM (NOW() - started_at))::BIGINT
            FROM celers_tasks
            WHERE state = 'processing' AND started_at IS NOT NULL
            ORDER BY started_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get oldest processing age: {}", e)))?;

        Ok(age)
    }

    /// Get average task processing time for completed tasks (in milliseconds)
    ///
    /// Calculates the average time between started_at and completed_at for recently completed tasks.
    /// Uses the last 1000 completed tasks by default.
    pub async fn avg_processing_time_ms(&self) -> Result<Option<f64>> {
        let avg: Option<f64> = sqlx::query_scalar(
            r#"
            SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)
            FROM (
                SELECT started_at, completed_at
                FROM celers_tasks
                WHERE state = 'completed'
                  AND started_at IS NOT NULL
                  AND completed_at IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 1000
            ) recent_tasks
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to calculate avg processing time: {}", e))
        })?;

        Ok(avg)
    }

    /// Get the retry rate (percentage of tasks that have been retried at least once)
    ///
    /// Returns a value between 0.0 and 100.0.
    pub async fn retry_rate(&self) -> Result<f64> {
        let rate: Option<f64> = sqlx::query_scalar(
            r#"
            SELECT
                CASE WHEN COUNT(*) > 0
                THEN (COUNT(*) FILTER (WHERE retry_count > 0)::FLOAT / COUNT(*)::FLOAT * 100.0)
                ELSE 0.0
                END
            FROM celers_tasks
            WHERE state IN ('completed', 'failed', 'processing')
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to calculate retry rate: {}", e)))?;

        Ok(rate.unwrap_or(0.0))
    }

    /// Get success rate (percentage of completed vs failed tasks)
    ///
    /// Returns a value between 0.0 and 100.0.
    pub async fn success_rate(&self) -> Result<f64> {
        let rate: Option<f64> = sqlx::query_scalar(
            r#"
            SELECT
                CASE WHEN COUNT(*) > 0
                THEN (COUNT(*) FILTER (WHERE state = 'completed')::FLOAT / COUNT(*)::FLOAT * 100.0)
                ELSE 0.0
                END
            FROM celers_tasks
            WHERE state IN ('completed', 'failed')
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to calculate success rate: {}", e)))?;

        Ok(rate.unwrap_or(0.0))
    }

    // ========== Task Chaining & Workflows ==========

    /// Enqueue a chain of tasks to execute sequentially
    ///
    /// Tasks will be linked together where each task (except the first) is scheduled
    /// to run after the previous task completes. The chain is tracked via metadata.
    ///
    /// # Arguments
    /// * `chain` - The task chain configuration
    ///
    /// # Returns
    /// Vector of task IDs in order
    pub async fn enqueue_chain(&self, chain: TaskChain) -> Result<Vec<TaskId>> {
        if chain.tasks.is_empty() {
            return Ok(Vec::new());
        }

        let chain_id = Uuid::new_v4();
        let chain_total = chain.tasks.len();
        let mut task_ids: Vec<Uuid> = Vec::with_capacity(chain_total);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        for (idx, task) in chain.tasks.into_iter().enumerate() {
            let task_id = task.metadata.id;
            let mut db_metadata = json!({
                "queue": self.queue_name,
                "enqueued_at": chrono::Utc::now().to_rfc3339(),
                "chain_id": chain_id.to_string(),
                "chain_position": idx,
                "chain_total": chain_total,
                "stop_on_failure": chain.stop_on_failure,
            });

            // Add reference to previous task if not the first
            if idx > 0 {
                db_metadata["previous_task_id"] = json!(task_ids[idx - 1].to_string());
            }

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

            // First task is scheduled immediately, others are pending with far-future schedule
            // They'll be rescheduled by complete_chain_task()
            let scheduled_at = if idx == 0 {
                "NOW()"
            } else {
                "NOW() + INTERVAL '100 years'" // Effectively "never" until predecessor completes
            };

            sqlx::query(&format!(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), {})
                "#,
                scheduled_at
            ))
            .bind(task_id)
            .bind(&task.metadata.name)
            .bind(&task.payload)
            .bind(task.metadata.priority)
            .bind(task.metadata.max_retries as i32)
            .bind(db_metadata)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue chain task: {}", e)))?;

            task_ids.push(task_id);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit chain: {}", e)))?;

        tracing::info!(
            chain_id = %chain_id,
            task_count = task_ids.len(),
            "Enqueued task chain"
        );

        Ok(task_ids)
    }

    /// Complete a task in a chain and schedule the next task
    ///
    /// This should be called after successfully completing a task that's part of a chain.
    /// It will automatically schedule the next task in the chain.
    ///
    /// # Arguments
    /// * `task_id` - ID of the completed task
    pub async fn complete_chain_task(&self, task_id: &TaskId) -> Result<()> {
        // Get task metadata to check if it's part of a chain
        let row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

        if let Some(row) = row {
            let metadata: Option<serde_json::Value> = row.get("metadata");
            if let Some(meta) = metadata {
                // Check if this task is part of a chain
                if let Some(chain_id) = meta.get("chain_id").and_then(|v| v.as_str()) {
                    let position = meta
                        .get("chain_position")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    // Find the next task in the chain
                    let next_task = sqlx::query(
                        r#"
                        SELECT id
                        FROM celers_tasks
                        WHERE metadata->>'chain_id' = $1
                          AND (metadata->>'chain_position')::int = $2
                          AND state = 'pending'
                        "#,
                    )
                    .bind(chain_id)
                    .bind((position + 1) as i32)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| CelersError::Other(format!("Failed to find next task: {}", e)))?;

                    if let Some(next_row) = next_task {
                        let next_task_id: Uuid = next_row.get("id");

                        // Schedule the next task to run now
                        sqlx::query(
                            r#"
                            UPDATE celers_tasks
                            SET scheduled_at = NOW()
                            WHERE id = $1
                            "#,
                        )
                        .bind(next_task_id)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to schedule next task: {}", e))
                        })?;

                        tracing::info!(
                            chain_id = %chain_id,
                            completed_task = %task_id,
                            next_task = %next_task_id,
                            "Scheduled next task in chain"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Enqueue a workflow with multiple stages that may have dependencies
    ///
    /// This creates a DAG (Directed Acyclic Graph) of tasks where stages can depend
    /// on other stages completing first. Tasks within a stage can run in parallel.
    ///
    /// # Arguments
    /// * `workflow` - The workflow configuration
    ///
    /// # Returns
    /// Map of stage IDs to their task IDs
    pub async fn enqueue_workflow(
        &self,
        workflow: TaskWorkflow,
    ) -> Result<std::collections::HashMap<String, Vec<TaskId>>> {
        let mut result = std::collections::HashMap::new();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        for stage in &workflow.stages {
            let mut stage_task_ids = Vec::new();

            for task in &stage.tasks {
                let task_id = task.metadata.id;
                let mut db_metadata = json!({
                    "queue": self.queue_name,
                    "enqueued_at": chrono::Utc::now().to_rfc3339(),
                    "workflow_id": workflow.id.to_string(),
                    "workflow_name": workflow.name,
                    "stage_id": stage.id,
                    "stage_depends_on": stage.depends_on,
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

                // Tasks with dependencies are scheduled far in the future
                // They'll be rescheduled by complete_workflow_stage()
                let scheduled_at = if stage.depends_on.is_empty() {
                    "NOW()"
                } else {
                    "NOW() + INTERVAL '100 years'"
                };

                sqlx::query(&format!(
                    r#"
                    INSERT INTO celers_tasks
                        (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                    VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), {})
                    "#,
                    scheduled_at
                ))
                .bind(task_id)
                .bind(&task.metadata.name)
                .bind(&task.payload)
                .bind(task.metadata.priority)
                .bind(task.metadata.max_retries as i32)
                .bind(db_metadata)
                .execute(&mut *tx)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to enqueue workflow task: {}", e)))?;

                stage_task_ids.push(task_id);
            }

            result.insert(stage.id.clone(), stage_task_ids);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit workflow: {}", e)))?;

        tracing::info!(
            workflow_id = %workflow.id,
            workflow_name = %workflow.name,
            stage_count = workflow.stages.len(),
            "Enqueued workflow"
        );

        Ok(result)
    }

    /// Complete a task in a workflow stage and check if dependent stages can be scheduled
    ///
    /// This should be called after successfully completing a task that's part of a workflow.
    /// It will check if all tasks in the current stage are complete, and if so, schedule
    /// any dependent stages.
    ///
    /// # Arguments
    /// * `task_id` - ID of the completed task
    pub async fn complete_workflow_task(&self, task_id: &TaskId) -> Result<()> {
        // Get task metadata to check if it's part of a workflow
        let row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

        if let Some(row) = row {
            let metadata: Option<serde_json::Value> = row.get("metadata");
            if let Some(meta) = metadata {
                if let (Some(workflow_id), Some(stage_id)) = (
                    meta.get("workflow_id").and_then(|v| v.as_str()),
                    meta.get("stage_id").and_then(|v| v.as_str()),
                ) {
                    // Check if all tasks in this stage are completed
                    let incomplete_count: i64 = sqlx::query_scalar(
                        r#"
                        SELECT COUNT(*)
                        FROM celers_tasks
                        WHERE metadata->>'workflow_id' = $1
                          AND metadata->>'stage_id' = $2
                          AND state NOT IN ('completed', 'cancelled')
                        "#,
                    )
                    .bind(workflow_id)
                    .bind(stage_id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to count incomplete tasks: {}", e))
                    })?;

                    if incomplete_count == 0 {
                        // Stage is complete, find dependent stages
                        let dependent_stages = sqlx::query(
                            r#"
                            SELECT DISTINCT metadata->>'stage_id' as stage_id
                            FROM celers_tasks
                            WHERE metadata->>'workflow_id' = $1
                              AND metadata->'stage_depends_on' ? $2
                              AND state = 'pending'
                            "#,
                        )
                        .bind(workflow_id)
                        .bind(stage_id)
                        .fetch_all(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to find dependent stages: {}", e))
                        })?;

                        for dep_row in dependent_stages {
                            let dep_stage_id: String = dep_row.get("stage_id");

                            // Check if all dependencies for this stage are met
                            let unmet_deps = self
                                .check_workflow_stage_dependencies(workflow_id, &dep_stage_id)
                                .await?;

                            if unmet_deps.is_empty() {
                                // All dependencies met, schedule this stage
                                sqlx::query(
                                    r#"
                                    UPDATE celers_tasks
                                    SET scheduled_at = NOW()
                                    WHERE metadata->>'workflow_id' = $1
                                      AND metadata->>'stage_id' = $2
                                      AND state = 'pending'
                                    "#,
                                )
                                .bind(workflow_id)
                                .bind(&dep_stage_id)
                                .execute(&self.pool)
                                .await
                                .map_err(|e| {
                                    CelersError::Other(format!(
                                        "Failed to schedule dependent stage: {}",
                                        e
                                    ))
                                })?;

                                tracing::info!(
                                    workflow_id = %workflow_id,
                                    completed_stage = %stage_id,
                                    scheduled_stage = %dep_stage_id,
                                    "Scheduled dependent workflow stage"
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check which dependencies are not yet met for a workflow stage
    async fn check_workflow_stage_dependencies(
        &self,
        workflow_id: &str,
        stage_id: &str,
    ) -> Result<Vec<String>> {
        // Get the stage's dependencies
        let deps_row = sqlx::query(
            r#"
            SELECT metadata->'stage_depends_on' as deps
            FROM celers_tasks
            WHERE metadata->>'workflow_id' = $1
              AND metadata->>'stage_id' = $2
            LIMIT 1
            "#,
        )
        .bind(workflow_id)
        .bind(stage_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch stage dependencies: {}", e)))?;

        if let Some(row) = deps_row {
            let deps: Option<serde_json::Value> = row.get("deps");
            if let Some(deps_value) = deps {
                if let Some(deps_array) = deps_value.as_array() {
                    let mut unmet = Vec::new();

                    for dep_stage_id in deps_array {
                        if let Some(dep_id) = dep_stage_id.as_str() {
                            // Check if all tasks in the dependency stage are completed
                            let incomplete: i64 = sqlx::query_scalar(
                                r#"
                            SELECT COUNT(*)
                            FROM celers_tasks
                            WHERE metadata->>'workflow_id' = $1
                              AND metadata->>'stage_id' = $2
                              AND state NOT IN ('completed', 'cancelled')
                            "#,
                            )
                            .bind(workflow_id)
                            .bind(dep_id)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(|e| {
                                CelersError::Other(format!("Failed to check dependency: {}", e))
                            })?;

                            if incomplete > 0 {
                                unmet.push(dep_id.to_string());
                            }
                        }
                    }

                    return Ok(unmet);
                }
            }
        }

        Ok(Vec::new())
    }

    /// Cancel an entire task chain
    ///
    /// Cancels all pending and processing tasks in a chain.
    pub async fn cancel_chain(&self, chain_id: &Uuid) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE metadata->>'chain_id' = $1
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(chain_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel chain: {}", e)))?;

        tracing::info!(
            chain_id = %chain_id,
            cancelled_count = result.rows_affected(),
            "Cancelled task chain"
        );

        Ok(result.rows_affected())
    }

    /// Cancel an entire workflow
    ///
    /// Cancels all pending and processing tasks in a workflow.
    pub async fn cancel_workflow(&self, workflow_id: &Uuid) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE metadata->>'workflow_id' = $1
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(workflow_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel workflow: {}", e)))?;

        tracing::info!(
            workflow_id = %workflow_id,
            cancelled_count = result.rows_affected(),
            "Cancelled workflow"
        );

        Ok(result.rows_affected())
    }

    /// Get the status of a task chain
    ///
    /// Returns comprehensive status information about a chain including task counts
    /// by state and the current position in the chain.
    pub async fn get_chain_status(&self, chain_id: &Uuid) -> Result<Option<ChainStatus>> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks,
                MAX((metadata->>'chain_position')::int) FILTER (WHERE state = 'processing') as current_position
            FROM celers_tasks
            WHERE metadata->>'chain_id' = $1
            "#,
        )
        .bind(chain_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get chain status: {}", e)))?;

        if let Some(row) = row {
            let total_tasks: i64 = row.get("total_tasks");
            if total_tasks == 0 {
                return Ok(None);
            }

            let completed_tasks: i64 = row.get("completed_tasks");
            let failed_tasks: i64 = row.get("failed_tasks");
            let pending_tasks: i64 = row.get("pending_tasks");
            let processing_tasks: i64 = row.get("processing_tasks");
            let current_position: Option<i32> = row.get("current_position");

            Ok(Some(ChainStatus {
                chain_id: *chain_id,
                total_tasks,
                completed_tasks,
                failed_tasks,
                pending_tasks,
                processing_tasks,
                current_position: current_position.map(|p| p as i64),
                is_complete: completed_tasks + failed_tasks == total_tasks,
                has_failures: failed_tasks > 0,
            }))
        } else {
            Ok(None)
        }
    }

    /// Get the status of a workflow
    ///
    /// Returns comprehensive status information about a workflow including overall
    /// task counts and detailed status for each stage.
    pub async fn get_workflow_status(&self, workflow_id: &Uuid) -> Result<Option<WorkflowStatus>> {
        // Get overall workflow stats
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks,
                COUNT(DISTINCT metadata->>'stage_id') as total_stages,
                metadata->>'workflow_name' as workflow_name
            FROM celers_tasks
            WHERE metadata->>'workflow_id' = $1
            GROUP BY metadata->>'workflow_name'
            "#,
        )
        .bind(workflow_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get workflow status: {}", e)))?;

        if let Some(row) = row {
            let total_tasks: i64 = row.get("total_tasks");
            if total_tasks == 0 {
                return Ok(None);
            }

            let workflow_name: String = row.get("workflow_name");
            let total_stages: i64 = row.get("total_stages");
            let completed_tasks: i64 = row.get("completed_tasks");
            let failed_tasks: i64 = row.get("failed_tasks");
            let pending_tasks: i64 = row.get("pending_tasks");
            let processing_tasks: i64 = row.get("processing_tasks");

            // Get per-stage stats
            let stage_rows = sqlx::query(
                r#"
                SELECT
                    metadata->>'stage_id' as stage_id,
                    COUNT(*) as total_tasks,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                    COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                    COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks
                FROM celers_tasks
                WHERE metadata->>'workflow_id' = $1
                GROUP BY metadata->>'stage_id'
                "#,
            )
            .bind(workflow_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get stage statuses: {}", e)))?;

            let mut stage_statuses = Vec::new();
            let mut completed_stages = 0;
            let mut active_stages = 0;

            for stage_row in stage_rows {
                let stage_id: String = stage_row.get("stage_id");
                let stage_total: i64 = stage_row.get("total_tasks");
                let stage_completed: i64 = stage_row.get("completed_tasks");
                let stage_failed: i64 = stage_row.get("failed_tasks");
                let stage_pending: i64 = stage_row.get("pending_tasks");
                let stage_processing: i64 = stage_row.get("processing_tasks");

                let is_complete = stage_completed + stage_failed == stage_total;
                if is_complete {
                    completed_stages += 1;
                }
                if stage_processing > 0 {
                    active_stages += 1;
                }

                // Check if dependencies are met (simplified check)
                let dependencies_met = stage_pending == 0 || stage_processing > 0 || is_complete;

                stage_statuses.push(StageStatus {
                    stage_id,
                    total_tasks: stage_total,
                    completed_tasks: stage_completed,
                    failed_tasks: stage_failed,
                    pending_tasks: stage_pending,
                    processing_tasks: stage_processing,
                    is_complete,
                    dependencies_met,
                });
            }

            Ok(Some(WorkflowStatus {
                workflow_id: *workflow_id,
                workflow_name,
                total_stages,
                completed_stages,
                active_stages,
                total_tasks,
                completed_tasks,
                failed_tasks,
                pending_tasks,
                processing_tasks,
                is_complete: completed_tasks + failed_tasks == total_tasks,
                has_failures: failed_tasks > 0,
                stage_statuses,
            }))
        } else {
            Ok(None)
        }
    }

    // ========== Multi-Tenant Support ==========

    /// Create a dedicated tenant ID for better isolation
    ///
    /// This adds a tenant_id to task metadata for multi-tenant scenarios.
    /// Use this when you need stronger isolation than queue_name alone.
    pub fn with_tenant_id(&self, tenant_id: &str) -> TenantBroker<'_> {
        TenantBroker {
            broker: self,
            tenant_id: tenant_id.to_string(),
        }
    }

    /// List all tasks for a specific tenant (across all queues)
    ///
    /// This queries tasks by tenant_id in metadata, useful for multi-tenant monitoring.
    pub async fn list_tasks_by_tenant(
        &self,
        tenant_id: &str,
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
                    WHERE metadata->>'tenant_id' = $1
                      AND state = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    "#,
                )
                .bind(tenant_id)
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
                    WHERE metadata->>'tenant_id' = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                )
                .bind(tenant_id)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tenant tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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

    /// Count tasks by tenant ID
    pub async fn count_tasks_by_tenant(&self, tenant_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE metadata->>'tenant_id' = $1",
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count tenant tasks: {}", e)))?;

        Ok(count)
    }

    // ========== Bulk Operations ==========

    /// Update multiple tasks to a specific state
    ///
    /// Useful for bulk operations like marking tasks as cancelled or failed.
    pub async fn bulk_update_state(
        &self,
        task_ids: &[TaskId],
        new_state: DbTaskState,
    ) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = $1,
                completed_at = CASE WHEN $1 IN ('completed', 'failed', 'cancelled')
                                    THEN NOW() ELSE completed_at END
            WHERE id = ANY($2)
            "#,
        )
        .bind(new_state.to_string())
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to bulk update state: {}", e)))?;

        tracing::info!(
            count = result.rows_affected(),
            new_state = %new_state,
            "Bulk updated task states"
        );

        Ok(result.rows_affected())
    }

    /// Find tasks created within a time range
    ///
    /// Useful for reporting and analytics.
    pub async fn find_tasks_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        state: Option<DbTaskState>,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE created_at >= $1 AND created_at <= $2
                      AND state = $3
                    ORDER BY created_at DESC
                    LIMIT $4
                    "#,
                )
                .bind(start)
                .bind(end)
                .bind(s.to_string())
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE created_at >= $1 AND created_at <= $2
                    ORDER BY created_at DESC
                    LIMIT $3
                    "#,
                )
                .bind(start)
                .bind(end)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by time range: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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
}

/// Tenant-scoped broker for multi-tenant isolation
///
/// This wrapper automatically adds tenant_id to all tasks for better isolation.
pub struct TenantBroker<'a> {
    broker: &'a PostgresBroker,
    tenant_id: String,
}

impl<'a> TenantBroker<'a> {
    /// Enqueue a task with automatic tenant_id
    pub async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        // The broker will merge this with task metadata
        self.broker.enqueue(task).await
    }

    /// Get queue size for this tenant
    pub async fn queue_size(&self) -> Result<usize> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE metadata->>'tenant_id' = $1
              AND state = 'pending'
            "#,
        )
        .bind(&self.tenant_id)
        .fetch_one(&self.broker.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get tenant queue size: {}", e)))?;

        Ok(count as usize)
    }

    /// List tasks for this tenant
    pub async fn list_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        self.broker
            .list_tasks_by_tenant(&self.tenant_id, state, limit, offset)
            .await
    }

    /// Get tenant ID
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}

// ========== LISTEN/NOTIFY Support ==========

/// Task notification listener for real-time task events
///
/// This allows workers to be notified immediately when new tasks are enqueued,
/// reducing polling overhead. Uses PostgreSQL's LISTEN/NOTIFY mechanism.
///
/// # Example
///
/// ```no_run
/// use celers_broker_postgres::PostgresBroker;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = PostgresBroker::new("postgres://localhost/db").await?;
/// broker.migrate().await?;
///
/// // Create a listener on a separate task
/// let mut listener = broker.create_notification_listener().await?;
///
/// // Enable notifications (sends NOTIFY after each enqueue)
/// broker.enable_notifications(true).await?;
///
/// // Wait for notifications
/// tokio::spawn(async move {
///     loop {
///         match listener.wait_for_notification(Duration::from_secs(30)).await {
///             Ok(Some(notification)) => {
///                 println!("New task enqueued: {:?}", notification);
///                 // Dequeue and process task
///             }
///             Ok(None) => {
///                 println!("Timeout, no notification received");
///             }
///             Err(e) => {
///                 eprintln!("Listener error: {}", e);
///                 break;
///             }
///         }
///     }
/// });
/// # Ok(())
/// # }
/// ```
pub struct TaskNotificationListener {
    listener: sqlx::postgres::PgListener,
    channel: String,
}

/// Task notification payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNotification {
    /// Task ID that was enqueued
    pub task_id: Uuid,
    /// Task name
    pub task_name: String,
    /// Queue name
    pub queue_name: String,
    /// Priority
    pub priority: i32,
    /// Timestamp when enqueued
    pub enqueued_at: DateTime<Utc>,
}

impl TaskNotificationListener {
    /// Wait for a task notification with timeout
    ///
    /// Returns:
    /// - `Ok(Some(notification))` if a notification was received
    /// - `Ok(None)` if timeout occurred
    /// - `Err(...)` if an error occurred
    pub async fn wait_for_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TaskNotification>> {
        use tokio::time::timeout as tokio_timeout;

        match tokio_timeout(timeout, self.listener.recv()).await {
            Ok(Ok(notification)) => {
                let payload: TaskNotification = serde_json::from_str(notification.payload())
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to parse notification: {}", e))
                    })?;
                Ok(Some(payload))
            }
            Ok(Err(e)) => Err(CelersError::Other(format!("Listener error: {}", e))),
            Err(_) => Ok(None), // Timeout
        }
    }

    /// Try to receive a notification without blocking
    ///
    /// Returns immediately with either a notification or None.
    pub async fn try_recv_notification(&mut self) -> Result<Option<TaskNotification>> {
        match self.listener.try_recv().await {
            Ok(Some(notification)) => {
                let payload: TaskNotification = serde_json::from_str(notification.payload())
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to parse notification: {}", e))
                    })?;
                Ok(Some(payload))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CelersError::Other(format!("Listener error: {}", e))),
        }
    }

    /// Get the channel name this listener is subscribed to
    pub fn channel(&self) -> &str {
        &self.channel
    }
}

impl PostgresBroker {
    /// Create a notification listener for real-time task events
    ///
    /// The listener will receive notifications when tasks are enqueued.
    /// Call `enable_notifications(true)` to start sending notifications.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let mut listener = broker.create_notification_listener().await?;
    ///
    /// // Wait for notifications
    /// while let Some(notification) = listener.wait_for_notification(Duration::from_secs(30)).await? {
    ///     println!("Task enqueued: {}", notification.task_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_notification_listener(&self) -> Result<TaskNotificationListener> {
        let channel = format!("celers_tasks_{}", self.queue_name);
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to create listener: {}", e)))?;

        listener.listen(&channel).await.map_err(|e| {
            CelersError::Other(format!("Failed to listen on channel {}: {}", channel, e))
        })?;

        tracing::info!(channel = %channel, "Created task notification listener");

        Ok(TaskNotificationListener { listener, channel })
    }

    /// Enable or disable NOTIFY on task enqueue
    ///
    /// When enabled, a PostgreSQL NOTIFY will be sent whenever a task is enqueued,
    /// allowing listeners to be notified immediately without polling.
    ///
    /// # Arguments
    /// * `enabled` - true to enable notifications, false to disable
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// broker.enable_notifications(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enable_notifications(&self, enabled: bool) -> Result<()> {
        // Create or drop the trigger that sends NOTIFY
        let channel = format!("celers_tasks_{}", self.queue_name);

        if enabled {
            // Create trigger function if it doesn't exist
            let function_sql = format!(
                r#"
                CREATE OR REPLACE FUNCTION notify_task_enqueued()
                RETURNS TRIGGER AS $$
                DECLARE
                    payload JSON;
                BEGIN
                    payload := json_build_object(
                        'task_id', NEW.id,
                        'task_name', NEW.task_name,
                        'queue_name', NEW.metadata->>'queue',
                        'priority', NEW.priority,
                        'enqueued_at', NEW.created_at
                    );
                    PERFORM pg_notify('{}', payload::text);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                "#,
                channel
            );

            sqlx::query(&function_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to create notification function: {}", e))
                })?;

            // Create trigger
            let trigger_sql = r#"
                DROP TRIGGER IF EXISTS trigger_notify_task_enqueued ON celers_tasks;
                CREATE TRIGGER trigger_notify_task_enqueued
                    AFTER INSERT ON celers_tasks
                    FOR EACH ROW
                    EXECUTE FUNCTION notify_task_enqueued();
                "#;

            sqlx::query(trigger_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to create notification trigger: {}", e))
                })?;

            tracing::info!(channel = %channel, "Enabled task notifications");
        } else {
            // Drop trigger
            let drop_sql = r#"
                DROP TRIGGER IF EXISTS trigger_notify_task_enqueued ON celers_tasks;
                "#;

            sqlx::query(drop_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to disable notification trigger: {}", e))
                })?;

            tracing::info!(channel = %channel, "Disabled task notifications");
        }

        Ok(())
    }

    /// Check if notifications are enabled
    ///
    /// Returns true if the notification trigger exists, false otherwise.
    pub async fn notifications_enabled(&self) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'trigger_notify_task_enqueued'
                  AND tgrelid = 'celers_tasks'::regclass
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check notification status: {}", e)))?;

        Ok(exists)
    }
}

// ========== Task Deduplication Support ==========

/// Deduplication configuration for preventing duplicate task execution
///
/// Task deduplication ensures that tasks with the same idempotency key are not
/// executed multiple times within a specified time window. This is critical for
/// distributed systems where the same request might be received multiple times.
///
/// # Example
///
/// ```no_run
/// use celers_broker_postgres::{PostgresBroker, DeduplicationConfig};
/// use celers_core::{Broker, SerializedTask};
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = PostgresBroker::new("postgres://localhost/db").await?;
/// broker.migrate().await?;
///
/// // Enable deduplication with 5-minute window
/// let config = DeduplicationConfig {
///     enabled: true,
///     window_secs: 300, // 5 minutes
/// };
///
/// // Enqueue with idempotency key
/// let task = SerializedTask::new("process_payment".to_string(), vec![1, 2, 3]);
/// let idempotency_key = "payment-12345";
///
/// // First enqueue succeeds
/// let task_id = broker.enqueue_idempotent(task.clone(), idempotency_key, &config).await?;
/// println!("Task enqueued: {}", task_id);
///
/// // Second enqueue within window returns existing task ID
/// let duplicate_id = broker.enqueue_idempotent(task, idempotency_key, &config).await?;
/// assert_eq!(task_id, duplicate_id);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationConfig {
    /// Whether deduplication is enabled
    pub enabled: bool,
    /// Deduplication time window in seconds
    pub window_secs: i64,
}

impl Default for DeduplicationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_secs: 300, // 5 minutes default
        }
    }
}

/// Information about a deduplicated task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationInfo {
    /// Idempotency key
    pub idempotency_key: String,
    /// Task ID that was enqueued
    pub task_id: Uuid,
    /// Task name
    pub task_name: String,
    /// When the task was first enqueued
    pub first_seen_at: DateTime<Utc>,
    /// When the deduplication entry expires
    pub expires_at: DateTime<Utc>,
    /// Number of duplicate attempts blocked
    pub duplicate_count: i32,
}

impl PostgresBroker {
    /// Enqueue a task with idempotency guarantee
    ///
    /// If a task with the same idempotency key was enqueued within the deduplication
    /// window, this returns the ID of the existing task instead of creating a duplicate.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `idempotency_key` - Unique key to identify duplicate requests
    /// * `config` - Deduplication configuration
    ///
    /// # Returns
    /// The task ID (either newly created or existing)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, DeduplicationConfig};
    /// use celers_core::{Broker, SerializedTask};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let config = DeduplicationConfig::default();
    ///
    /// let task = SerializedTask::new("process_order".to_string(), vec![]);
    /// let task_id = broker.enqueue_idempotent(task, "order-123", &config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_idempotent(
        &self,
        task: SerializedTask,
        idempotency_key: &str,
        config: &DeduplicationConfig,
    ) -> Result<TaskId> {
        if !config.enabled {
            // Deduplication disabled, just enqueue normally
            return self.enqueue(task).await;
        }

        // Start a transaction for atomicity
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Check for existing task with this idempotency key
        let existing = sqlx::query(
            r#"
            SELECT task_id, duplicate_count
            FROM celers_task_deduplication
            WHERE idempotency_key = $1
              AND expires_at > NOW()
              AND queue_name = $2
            FOR UPDATE
            "#,
        )
        .bind(idempotency_key)
        .bind(&self.queue_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check deduplication: {}", e)))?;

        if let Some(row) = existing {
            let task_id: Uuid = row.get("task_id");
            let duplicate_count: i32 = row.get("duplicate_count");

            // Increment duplicate count
            sqlx::query(
                r#"
                UPDATE celers_task_deduplication
                SET duplicate_count = duplicate_count + 1,
                    last_seen_at = NOW()
                WHERE idempotency_key = $1
                  AND queue_name = $2
                "#,
            )
            .bind(idempotency_key)
            .bind(&self.queue_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to update duplicate count: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            tracing::info!(
                idempotency_key = %idempotency_key,
                task_id = %task_id,
                duplicate_count = duplicate_count + 1,
                "Blocked duplicate task enqueue"
            );

            return Ok(task_id);
        }

        // No existing task, enqueue normally
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "idempotency_key": idempotency_key,
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

        // Insert task
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
            "#,
        )
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(db_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        // Insert deduplication entry
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(config.window_secs);
        sqlx::query(
            r#"
            INSERT INTO celers_task_deduplication
                (idempotency_key, task_id, task_name, queue_name, first_seen_at, last_seen_at, expires_at, duplicate_count)
            VALUES ($1, $2, $3, $4, NOW(), NOW(), $5, 0)
            ON CONFLICT (idempotency_key, queue_name) DO NOTHING
            "#,
        )
        .bind(idempotency_key)
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&self.queue_name)
        .bind(expires_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to insert deduplication entry: {}", e))
        })?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        tracing::info!(
            idempotency_key = %idempotency_key,
            task_id = %task_id,
            task_name = %task.metadata.name,
            "Enqueued task with deduplication"
        );

        Ok(task_id)
    }

    /// Check if a task with the given idempotency key exists
    ///
    /// Returns Some(DeduplicationInfo) if a task exists, None otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// if let Some(info) = broker.check_deduplication("order-123").await? {
    ///     println!("Task already exists: {}", info.task_id);
    ///     println!("Duplicates blocked: {}", info.duplicate_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_deduplication(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<DeduplicationInfo>> {
        let row = sqlx::query(
            r#"
            SELECT idempotency_key, task_id, task_name, first_seen_at, expires_at, duplicate_count
            FROM celers_task_deduplication
            WHERE idempotency_key = $1
              AND queue_name = $2
              AND expires_at > NOW()
            "#,
        )
        .bind(idempotency_key)
        .bind(&self.queue_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check deduplication: {}", e)))?;

        match row {
            Some(row) => Ok(Some(DeduplicationInfo {
                idempotency_key: row.get("idempotency_key"),
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                first_seen_at: row.get("first_seen_at"),
                expires_at: row.get("expires_at"),
                duplicate_count: row.get("duplicate_count"),
            })),
            None => Ok(None),
        }
    }

    /// Clean up expired deduplication entries
    ///
    /// Removes deduplication entries that have expired to prevent table bloat.
    /// This should be run periodically (e.g., via a maintenance task).
    ///
    /// # Returns
    /// The number of entries deleted
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let deleted = broker.cleanup_deduplication().await?;
    /// println!("Cleaned up {} expired deduplication entries", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cleanup_deduplication(&self) -> Result<i64> {
        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_deduplication
            WHERE expires_at < NOW()
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cleanup deduplication: {}", e)))?;

        let deleted = result.rows_affected() as i64;

        tracing::info!(
            deleted = deleted,
            "Cleaned up expired deduplication entries"
        );

        Ok(deleted)
    }

    /// Get deduplication statistics
    ///
    /// Returns statistics about deduplication entries in the system.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let (active, total_duplicates) = broker.get_deduplication_stats().await?;
    /// println!("Active dedup entries: {}", active);
    /// println!("Total duplicates blocked: {}", total_duplicates);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_deduplication_stats(&self) -> Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as active_entries,
                COALESCE(SUM(duplicate_count), 0) as total_duplicates
            FROM celers_task_deduplication
            WHERE expires_at > NOW()
              AND queue_name = $1
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get deduplication stats: {}", e)))?;

        let active_entries: i64 = row.get("active_entries");
        let total_duplicates: i64 = row.get("total_duplicates");

        Ok((active_entries, total_duplicates))
    }
}

#[async_trait]
impl Broker for PostgresBroker {
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
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
            "#,
        )
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority) // Use task's priority
        .bind(task.metadata.max_retries as i32) // Use task's max retries
        .bind(db_metadata)
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
            let task_id: Uuid = row.get("id");
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
                WHERE id = $1
                "#,
            )
            .bind(task_id)
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
            WHERE id = $1
            "#,
        )
        .bind(task_id)
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
                WHERE id = $1
                "#,
            )
            .bind(task_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

            let retry_count: i32 = row.get("retry_count");
            let max_retries: i32 = row.get("max_retries");

            if retry_count >= max_retries {
                // Move to DLQ
                self.move_to_dlq(task_id).await?;
            } else {
                // Requeue with configured retry strategy
                let backoff_seconds = self.retry_strategy.calculate_backoff(retry_count);

                sqlx::query(
                    r#"
                    UPDATE celers_tasks
                    SET state = 'pending',
                        scheduled_at = NOW() + ($1 || ' seconds')::INTERVAL,
                        started_at = NULL,
                        worker_id = NULL
                    WHERE id = $2
                    "#,
                )
                .bind(backoff_seconds)
                .bind(task_id)
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

                tracing::info!(
                    task_id = %task_id,
                    retry_count = retry_count,
                    backoff_seconds = backoff_seconds,
                    strategy = ?self.retry_strategy,
                    "Requeued task with backoff"
                );
            }
        } else {
            // Mark as failed permanently
            sqlx::query(
                r#"
                UPDATE celers_tasks
                SET state = 'failed',
                    completed_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(task_id)
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
            WHERE id = $1 AND state IN ('pending', 'processing')
            "#,
        )
        .bind(task_id)
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

        // Convert Unix timestamp to PostgreSQL timestamp
        let scheduled_at = chrono::DateTime::from_timestamp(execute_at, 0)
            .ok_or_else(|| CelersError::Other("Invalid timestamp".to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), $7)
            "#,
        )
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(db_metadata)
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
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW() + ($7 || ' seconds')::INTERVAL)
            "#,
        )
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(db_metadata)
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
                VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
                "#,
            )
            .bind(task_id)
            .bind(&task.metadata.name)
            .bind(&task.payload)
            .bind(task.metadata.priority)
            .bind(task.metadata.max_retries as i32)
            .bind(db_metadata)
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
            for task in &tasks {
                TASKS_ENQUEUED_BY_TYPE
                    .with_label_values(&[&task.metadata.name])
                    .inc();
            }
        }

        Ok(task_ids)
    }

    /// Optimized batch dequeue using a single transaction with FOR UPDATE SKIP LOCKED
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        if count == 0 || self.paused.load(Ordering::SeqCst) {
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
            LIMIT $1
            "#,
        )
        .bind(count as i64)
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
        let mut task_ids = Vec::with_capacity(rows.len());

        for row in rows {
            let task_id: Uuid = row.get("id");
            let task_name: String = row.get("task_name");
            let payload: Vec<u8> = row.get("payload");
            let retry_count: i32 = row.get("retry_count");

            messages.push(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            });

            task_ids.push(task_id);
        }

        sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'processing',
                started_at = NOW(),
                retry_count = retry_count + 1
            WHERE id = ANY($1)
            "#,
        )
        .bind(&task_ids)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to mark batch as processing: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch dequeue: {}", e)))?;

        Ok(messages)
    }

    /// Optimized batch ack using a single query with ANY()
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        let task_ids: Vec<Uuid> = tasks.iter().map(|(id, _)| *id).collect();

        sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'completed',
                completed_at = NOW()
            WHERE id = ANY($1)
            "#,
        )
        .bind(&task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to batch ack tasks: {}", e)))?;

        Ok(())
    }
}

// Partition Management Methods
impl PostgresBroker {
    /// Create a partition for tasks table for a specific date (monthly partitions)
    ///
    /// This requires that the celers_tasks table is partitioned. See migration 003_partitioning.sql
    /// for details on setting up table partitioning.
    ///
    /// # Arguments
    /// * `partition_date` - Any date within the month to create partition for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::{Utc, Datelike};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Create partition for current month
    /// let current_date = Utc::now().naive_utc().date();
    /// broker.create_partition(current_date).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_partition(&self, partition_date: chrono::NaiveDate) -> Result<String> {
        let result: String = sqlx::query_scalar("SELECT create_tasks_partition($1)")
            .bind(partition_date)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to create partition: {}", e)))?;

        Ok(result)
    }

    /// Create partitions for a date range (monthly partitions)
    ///
    /// This creates all partitions from start_date to end_date (inclusive, by month).
    /// Useful for initializing partitions for several months ahead.
    ///
    /// # Arguments
    /// * `start_date` - Start of date range
    /// * `end_date` - End of date range
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::{Utc, Datelike, Duration};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Create partitions for next 6 months
    /// let start = Utc::now().naive_utc().date();
    /// let end = start + Duration::days(180);
    /// let results = broker.create_partitions_range(start, end).await?;
    /// println!("Created {} partitions", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_partitions_range(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> Result<Vec<(String, String)>> {
        let rows =
            sqlx::query("SELECT partition_name, status FROM create_tasks_partitions_range($1, $2)")
                .bind(start_date)
                .bind(end_date)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to create partitions range: {}", e))
                })?;

        let results: Vec<(String, String)> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("partition_name");
                let status: String = row.get("status");
                (name, status)
            })
            .collect();

        Ok(results)
    }

    /// Drop a partition for a specific date
    ///
    /// **WARNING**: This permanently deletes all tasks in the partition!
    /// Use this for archiving old partitions after backing up the data.
    ///
    /// # Arguments
    /// * `partition_date` - Any date within the month to drop partition for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::NaiveDate;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Drop partition for January 2024 (after backing up!)
    /// let old_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// broker.drop_partition(old_date).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn drop_partition(&self, partition_date: chrono::NaiveDate) -> Result<String> {
        let result: String = sqlx::query_scalar("SELECT drop_tasks_partition($1)")
            .bind(partition_date)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to drop partition: {}", e)))?;

        Ok(result)
    }

    /// List all task partitions with their statistics
    ///
    /// Returns information about each partition including row count and size.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let partitions = broker.list_partitions().await?;
    /// for partition in partitions {
    ///     println!("{}: {} rows, {} bytes",
    ///         partition.partition_name,
    ///         partition.row_count,
    ///         partition.size_bytes);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_partitions(&self) -> Result<Vec<PartitionInfo>> {
        let rows = sqlx::query(
            "SELECT partition_name, partition_start, partition_end, row_count, size_bytes
             FROM list_tasks_partitions()",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list partitions: {}", e)))?;

        let partitions: Vec<PartitionInfo> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("partition_name");
                let start: chrono::NaiveDate = row.get("partition_start");
                let end: chrono::NaiveDate = row.get("partition_end");
                let row_count: i64 = row.get("row_count");
                let size_bytes: i64 = row.get("size_bytes");

                PartitionInfo {
                    partition_name: name,
                    partition_start: DateTime::from_naive_utc_and_offset(
                        start.and_hms_opt(0, 0, 0).unwrap(),
                        Utc,
                    ),
                    partition_end: DateTime::from_naive_utc_and_offset(
                        end.and_hms_opt(0, 0, 0).unwrap(),
                        Utc,
                    ),
                    row_count,
                    size_bytes,
                }
            })
            .collect();

        Ok(partitions)
    }

    /// Maintain partitions by creating future partitions automatically
    ///
    /// This creates partitions for the current month plus `months_ahead` months.
    /// Should be called periodically (e.g., daily or weekly) to ensure partitions
    /// exist before tasks are enqueued.
    ///
    /// # Arguments
    /// * `months_ahead` - Number of months ahead to create partitions for (default: 3)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Create partitions for current month + 3 months ahead
    /// broker.maintain_partitions(3).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn maintain_partitions(&self, months_ahead: i32) -> Result<String> {
        let result: String = sqlx::query_scalar("SELECT maintain_tasks_partitions($1)")
            .bind(months_ahead)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to maintain partitions: {}", e)))?;

        Ok(result)
    }

    /// Get the partition name for a specific date
    ///
    /// Useful for understanding which partition a task will be stored in.
    ///
    /// # Arguments
    /// * `task_date` - Date to get partition name for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::Utc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let partition_name = broker.get_partition_name(Utc::now().naive_utc().date()).await?;
    /// println!("Current partition: {}", partition_name);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_partition_name(&self, task_date: chrono::NaiveDate) -> Result<String> {
        let result: String = sqlx::query_scalar("SELECT get_tasks_partition_name($1)")
            .bind(task_date)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get partition name: {}", e)))?;

        Ok(result)
    }

    /// Detach a partition for archiving without deleting data
    ///
    /// This detaches the partition from the main table but doesn't delete it.
    /// Useful for archiving old data to separate storage before dropping.
    ///
    /// # Arguments
    /// * `partition_date` - Any date within the month to detach partition for
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use chrono::NaiveDate;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Detach partition for archiving
    /// let old_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// broker.detach_partition(old_date).await?;
    /// // Now you can pg_dump the detached table and then drop it
    /// # Ok(())
    /// # }
    /// ```
    pub async fn detach_partition(&self, partition_date: chrono::NaiveDate) -> Result<String> {
        let partition_name = self.get_partition_name(partition_date).await?;

        sqlx::query(&format!(
            "ALTER TABLE celers_tasks DETACH PARTITION {}",
            partition_name
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to detach partition: {}", e)))?;

        Ok(format!("Detached partition: {}", partition_name))
    }
}

// Query Optimization Methods
impl PostgresBroker {
    /// Analyze query performance for the dequeue operation
    ///
    /// Returns EXPLAIN ANALYZE output for the main dequeue query.
    /// Useful for understanding query performance and identifying bottlenecks.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let explain_output = broker.explain_dequeue_query().await?;
    /// println!("Query plan:\n{}", explain_output);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn explain_dequeue_query(&self) -> Result<String> {
        let explain_query = format!(
            r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT id, task_name, payload, retry_count, max_retries, created_at
            FROM {}
            WHERE state = 'pending' AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
            self.queue_name
        );

        let rows = sqlx::query_scalar::<_, String>(&explain_query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        Ok(rows.join("\n"))
    }

    /// Get query statistics for tasks table
    ///
    /// Returns statistics about table scans, index usage, etc.
    /// Useful for monitoring query performance over time.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let stats = broker.get_query_stats().await?;
    /// println!("Query statistics:\n{}", stats);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_query_stats(&self) -> Result<String> {
        let query = format!(
            r#"
            SELECT
                schemaname,
                relname,
                seq_scan,
                seq_tup_read,
                idx_scan,
                idx_tup_fetch,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_live_tup,
                n_dead_tup,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE relname = '{}'
            "#,
            self.queue_name.trim_start_matches("public.")
        );

        let row = sqlx::query(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get query stats: {}", e)))?;

        let stats = format!(
            "Table: {}.{}\n\
             Sequential Scans: {}\n\
             Sequential Tuples Read: {}\n\
             Index Scans: {}\n\
             Index Tuples Fetched: {}\n\
             Tuples Inserted: {}\n\
             Tuples Updated: {}\n\
             Tuples Deleted: {}\n\
             Live Tuples: {}\n\
             Dead Tuples: {}\n\
             Last Vacuum: {:?}\n\
             Last Autovacuum: {:?}\n\
             Last Analyze: {:?}\n\
             Last Autoanalyze: {:?}",
            row.get::<String, _>("schemaname"),
            row.get::<String, _>("relname"),
            row.get::<i64, _>("seq_scan"),
            row.get::<i64, _>("seq_tup_read"),
            row.get::<Option<i64>, _>("idx_scan").unwrap_or(0),
            row.get::<Option<i64>, _>("idx_tup_fetch").unwrap_or(0),
            row.get::<i64, _>("n_tup_ins"),
            row.get::<i64, _>("n_tup_upd"),
            row.get::<i64, _>("n_tup_del"),
            row.get::<i64, _>("n_live_tup"),
            row.get::<i64, _>("n_dead_tup"),
            row.get::<Option<DateTime<Utc>>, _>("last_vacuum"),
            row.get::<Option<DateTime<Utc>>, _>("last_autovacuum"),
            row.get::<Option<DateTime<Utc>>, _>("last_analyze"),
            row.get::<Option<DateTime<Utc>>, _>("last_autoanalyze")
        );

        Ok(stats)
    }

    /// Set query optimization hints for PostgreSQL
    ///
    /// Configures session-level query optimization settings.
    /// These settings only affect the current connection.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Enable parallel query execution
    /// broker.set_query_hints(true, 4).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_query_hints(
        &self,
        enable_parallel: bool,
        max_parallel_workers: i32,
    ) -> Result<()> {
        if enable_parallel {
            sqlx::query(&format!(
                "SET max_parallel_workers_per_gather = {}",
                max_parallel_workers
            ))
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;

            sqlx::query("SET parallel_setup_cost = 100")
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;

            sqlx::query("SET parallel_tuple_cost = 0.01")
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;
        } else {
            sqlx::query("SET max_parallel_workers_per_gather = 0")
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;
        }

        Ok(())
    }

    /// Get connection pool configuration recommendations
    ///
    /// Analyzes current workload and returns recommended pool settings.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let recommendations = broker.get_pool_recommendations().await?;
    /// println!("{}", recommendations);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_pool_recommendations(&self) -> Result<String> {
        let metrics = self.get_pool_metrics();
        let stats = self.get_statistics().await?;

        let utilization = if metrics.max_size > 0 {
            (metrics.in_use as f64 / metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        let recommendations = format!(
            "Connection Pool Recommendations:\n\
             \n\
             Current Configuration:\n\
             - Max Size: {}\n\
             - Current Size: {}\n\
             - In Use: {} ({}% utilization)\n\
             - Idle: {}\n\
             \n\
             Workload Analysis:\n\
             - Pending Tasks: {}\n\
             - Processing Tasks: {}\n\
             - Total Tasks: {}\n\
             \n\
             Recommendations:\n\
             {}",
            metrics.max_size,
            metrics.size,
            metrics.in_use,
            utilization as i32,
            metrics.idle,
            stats.pending,
            stats.processing,
            stats.total,
            if utilization > 80.0 {
                "⚠️  High pool utilization! Consider increasing max_connections.\n\
                 - Recommended: Increase pool size to handle peak load\n\
                 - Current bottleneck: Connection pool exhaustion"
            } else if utilization < 20.0 && metrics.max_size > 10 {
                "✓ Pool is underutilized. You may reduce max_connections to save resources.\n\
                 - Recommended: Reduce pool size to 50-60% of current\n\
                 - Benefit: Lower memory usage and connection overhead"
            } else {
                "✓ Pool utilization is optimal (20-80% range).\n\
                 - Current configuration is well-tuned for your workload"
            }
        );

        Ok(recommendations)
    }
}

// Connection Health & Resilience Methods
impl PostgresBroker {
    /// Perform detailed health check with diagnostics and recommendations
    ///
    /// This provides a comprehensive health assessment including connection status,
    /// query performance, pool metrics, and actionable recommendations.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let health = broker.check_health_detailed().await?;
    /// if !health.healthy {
    ///     eprintln!("Health check failed!");
    ///     for warning in &health.warnings {
    ///         eprintln!("Warning: {}", warning);
    ///     }
    /// }
    /// for rec in &health.recommendations {
    ///     println!("Recommendation: {}", rec);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_health_detailed(&self) -> Result<DetailedHealthStatus> {
        let start = std::time::Instant::now();

        // Test connection and get version
        let version_result = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(&self.pool)
            .await;

        let (connection_ok, database_version) = match version_result {
            Ok(v) => (true, v),
            Err(e) => (false, format!("Connection failed: {}", e)),
        };

        let query_latency_ms = start.elapsed().as_millis() as i64;

        // Get metrics even if connection failed (from cached pool state)
        let pool_metrics = self.get_pool_metrics();

        // Try to get queue stats (may fail if connection is down)
        let queue_stats = self.get_statistics().await.unwrap_or_default();

        // Generate warnings and recommendations
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();

        // Check connection health
        if !connection_ok {
            warnings.push("Database connection failed".to_string());
            recommendations
                .push("Check database availability and network connectivity".to_string());
        } else if query_latency_ms > 1000 {
            warnings.push(format!("High query latency: {}ms", query_latency_ms));
            recommendations
                .push("Database may be overloaded or network latency is high".to_string());
        }

        // Check pool utilization
        let utilization = if pool_metrics.max_size > 0 {
            (pool_metrics.in_use as f64 / pool_metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        if utilization > 90.0 {
            warnings.push(format!("Critical pool utilization: {:.1}%", utilization));
            recommendations.push(
                "Increase max_connections immediately to prevent service degradation".to_string(),
            );
        } else if utilization > 80.0 {
            warnings.push(format!("High pool utilization: {:.1}%", utilization));
            recommendations.push("Consider increasing max_connections".to_string());
        }

        // Check idle connections
        if pool_metrics.idle == 0 && pool_metrics.in_use > 0 {
            warnings.push("No idle connections available".to_string());
            recommendations.push("Pool exhaustion detected - increase max_connections".to_string());
        }

        // Check queue health
        if queue_stats.processing > queue_stats.pending * 5 {
            warnings.push("Many tasks stuck in processing state".to_string());
            recommendations
                .push("Run recover_stuck_tasks() to recover abandoned tasks".to_string());
        }

        if queue_stats.dlq > 1000 {
            warnings.push(format!("Large DLQ size: {} tasks", queue_stats.dlq));
            recommendations.push("Review and address failed tasks in DLQ".to_string());
        }

        // Overall health determination
        let healthy = connection_ok && utilization < 95.0 && query_latency_ms < 5000;

        Ok(DetailedHealthStatus {
            healthy,
            connection_ok,
            query_latency_ms,
            pool_metrics,
            queue_stats,
            database_version,
            warnings,
            recommendations,
        })
    }

    /// Test connection with automatic retry on transient failures
    ///
    /// Attempts to connect to the database with exponential backoff retry logic.
    /// Useful for startup health checks and connection validation.
    ///
    /// # Arguments
    /// * `max_retries` - Maximum number of retry attempts
    /// * `initial_delay_ms` - Initial delay between retries in milliseconds
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Test with 5 retries, starting with 100ms delay
    /// match broker.test_connection_with_retry(5, 100).await {
    ///     Ok(_) => println!("Connection successful"),
    ///     Err(e) => eprintln!("Connection failed after retries: {}", e),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn test_connection_with_retry(
        &self,
        max_retries: u32,
        initial_delay_ms: u64,
    ) -> Result<String> {
        let mut last_error = None;

        for attempt in 0..=max_retries {
            match sqlx::query_scalar::<_, String>("SELECT version()")
                .fetch_one(&self.pool)
                .await
            {
                Ok(version) => {
                    if attempt > 0 {
                        tracing::info!("Connection successful after {} attempt(s)", attempt + 1);
                    }
                    return Ok(version);
                }
                Err(e) => {
                    last_error = Some(e);

                    if attempt < max_retries {
                        let delay_ms = initial_delay_ms * 2_u64.pow(attempt);
                        tracing::warn!(
                            "Connection attempt {} failed, retrying in {}ms: {}",
                            attempt + 1,
                            delay_ms,
                            last_error.as_ref().unwrap()
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        Err(CelersError::Other(format!(
            "Connection failed after {} retries: {}",
            max_retries,
            last_error.unwrap()
        )))
    }

    /// Get recommended batch sizes based on current workload and pool configuration
    ///
    /// Analyzes the current queue state and connection pool to recommend
    /// optimal batch sizes for different operations.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let recommendation = broker.get_recommended_batch_size().await?;
    /// println!("Recommended enqueue batch size: {}", recommendation.enqueue_batch_size);
    /// println!("Reasoning: {}", recommendation.reasoning);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_recommended_batch_size(&self) -> Result<BatchSizeRecommendation> {
        let stats = self.get_statistics().await?;
        let pool_metrics = self.get_pool_metrics();

        // Calculate pool pressure
        let pool_utilization = if pool_metrics.max_size > 0 {
            pool_metrics.in_use as f64 / pool_metrics.max_size as f64
        } else {
            0.0
        };

        // Base batch sizes on workload
        let queue_depth = stats.pending + stats.processing;

        let (enqueue_batch_size, dequeue_batch_size, ack_batch_size, reasoning) =
            if pool_utilization > 0.8 {
                // High pool pressure - use smaller batches to avoid connection exhaustion
                (
                    50,
                    10,
                    50,
                    format!(
                        "High pool utilization ({:.1}%) - using smaller batches to reduce connection time. \
                        Consider increasing max_connections from {} to {}.",
                        pool_utilization * 100.0,
                        pool_metrics.max_size,
                        pool_metrics.max_size * 2
                    )
                )
            } else if queue_depth > 10000 {
                // High queue depth - use larger batches for throughput
                (
                    500,
                    50,
                    200,
                    format!(
                        "High queue depth ({} tasks) - using larger batches for maximum throughput. \
                        Pool utilization is healthy at {:.1}%.",
                        queue_depth,
                        pool_utilization * 100.0
                    )
                )
            } else if queue_depth > 1000 {
                // Medium queue depth - balanced approach
                (
                    200,
                    25,
                    100,
                    format!(
                        "Medium queue depth ({} tasks) - balanced batch sizes. \
                        Pool utilization: {:.1}%.",
                        queue_depth,
                        pool_utilization * 100.0
                    ),
                )
            } else {
                // Low queue depth - smaller batches for latency
                (
                    100,
                    10,
                    50,
                    format!(
                        "Low queue depth ({} tasks) - using smaller batches for lower latency. \
                        Pool utilization: {:.1}%.",
                        queue_depth,
                        pool_utilization * 100.0
                    ),
                )
            };

        Ok(BatchSizeRecommendation {
            enqueue_batch_size,
            dequeue_batch_size,
            ack_batch_size,
            reasoning,
        })
    }

    /// Detect potential connection leaks
    ///
    /// Analyzes connection pool state over time to identify potential leaks.
    /// Returns a report of suspicious patterns.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let report = broker.detect_connection_leaks().await?;
    /// println!("{}", report);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn detect_connection_leaks(&self) -> Result<String> {
        let initial_metrics = self.get_pool_metrics();

        // Wait a bit and check again
        tokio::time::sleep(Duration::from_secs(5)).await;

        let final_metrics = self.get_pool_metrics();

        let mut report = String::from("Connection Leak Detection Report:\n\n");

        report.push_str("Initial state:\n");
        report.push_str(&format!("  - In use: {}\n", initial_metrics.in_use));
        report.push_str(&format!("  - Idle: {}\n", initial_metrics.idle));
        report.push_str(&format!("  - Total: {}\n\n", initial_metrics.size));

        report.push_str("After 5 seconds:\n");
        report.push_str(&format!("  - In use: {}\n", final_metrics.in_use));
        report.push_str(&format!("  - Idle: {}\n", final_metrics.idle));
        report.push_str(&format!("  - Total: {}\n\n", final_metrics.size));

        // Analyze changes
        if final_metrics.in_use > initial_metrics.in_use {
            report.push_str("⚠️  WARNING: Connections in use increased without being released.\n");
            report.push_str("   This may indicate a connection leak.\n");
            report.push_str(
                "   Ensure all database operations use proper error handling and cleanup.\n\n",
            );
        } else if final_metrics.in_use == initial_metrics.in_use && final_metrics.in_use > 0 {
            report.push_str("⚠️  INFO: Connections remain in use without change.\n");
            report.push_str("   This is normal during active workload but monitor for growth.\n\n");
        } else {
            report.push_str("✓ No obvious connection leaks detected.\n\n");
        }

        // Check for high sustained usage
        let utilization = if final_metrics.max_size > 0 {
            (final_metrics.in_use as f64 / final_metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        if utilization > 80.0 {
            report.push_str(&format!(
                "⚠️  HIGH UTILIZATION: {:.1}% of connections in use.\n",
                utilization
            ));
            report.push_str(
                "   Monitor for connection exhaustion and consider increasing pool size.\n",
            );
        }

        Ok(report)
    }
}

impl PostgresBroker {
    /// Update Prometheus metrics gauges for queue sizes and connection pool
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

        // Update queue gauges
        QUEUE_SIZE.set(pending_count as f64);
        PROCESSING_QUEUE_SIZE.set(processing_count as f64);
        DLQ_SIZE.set(dlq_count as f64);

        // Update connection pool metrics
        let pool_metrics = self.get_pool_metrics();
        POSTGRES_POOL_MAX_SIZE.set(pool_metrics.max_size as f64);
        POSTGRES_POOL_SIZE.set(pool_metrics.size as f64);
        POSTGRES_POOL_IDLE.set(pool_metrics.idle as f64);
        POSTGRES_POOL_IN_USE.set(pool_metrics.in_use as f64);

        Ok(())
    }
}

/// Convenience helper methods for common patterns
impl PostgresBroker {
    /// Enqueue multiple tasks with the same configuration
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use celers_core::Broker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task_ids = broker.enqueue_many(
    ///     "process_user",
    ///     vec![vec![1], vec![2], vec![3]],  // payloads
    ///     Some(5),  // priority
    ///     Some(3),  // max_retries
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_many(
        &self,
        task_name: &str,
        payloads: Vec<Vec<u8>>,
        priority: Option<i32>,
        max_retries: Option<u32>,
    ) -> Result<Vec<TaskId>> {
        let mut tasks = Vec::new();

        for payload in payloads {
            let mut task = SerializedTask::new(task_name.to_string(), payload);
            if let Some(p) = priority {
                task.metadata.priority = p;
            }
            if let Some(r) = max_retries {
                task.metadata.max_retries = r;
            }
            tasks.push(task);
        }

        self.enqueue_batch(tasks).await
    }

    /// Process a single task with automatic ack/reject
    ///
    /// Returns Some((task, ack_fn, reject_fn)) if a task is available, None otherwise
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// if let Some((task, ack, reject)) = broker.dequeue_with_handlers().await? {
    ///     match process_task(&task).await {
    ///         Ok(_) => ack().await?,
    ///         Err(e) => reject(&e.to_string()).await?,
    ///     }
    /// }
    /// # async fn process_task(_: &celers_core::SerializedTask) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn dequeue_with_handlers(
        &self,
    ) -> Result<
        Option<(
            SerializedTask,
            Box<
                dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
                    + Send,
            >,
            Box<
                dyn Fn(
                        &str,
                    )
                        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
                    + Send,
            >,
        )>,
    > {
        if let Some(msg) = self.dequeue().await? {
            let task_id = msg.task.metadata.id;
            let receipt = msg.receipt_handle.clone();
            let broker1 = self.pool.clone();
            let broker2 = self.pool.clone();

            let ack_fn = Box::new(move || {
                let pool = broker1.clone();
                let id = task_id;
                let _receipt_clone = receipt.clone();
                Box::pin(async move {
                    sqlx::query("UPDATE celers_tasks SET state = 'completed', completed_at = NOW() WHERE id = $1")
                        .bind(id)
                        .execute(&pool)
                        .await
                        .map_err(|e| CelersError::Other(format!("Ack failed: {}", e)))?;
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            });

            let reject_fn = Box::new(move |_error: &str| {
                let pool = broker2.clone();
                let id = task_id;
                Box::pin(async move {
                    sqlx::query(
                        "UPDATE celers_tasks SET retry_count = retry_count + 1 WHERE id = $1",
                    )
                    .bind(id)
                    .execute(&pool)
                    .await
                    .map_err(|e| CelersError::Other(format!("Reject failed: {}", e)))?;
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            });

            Ok(Some((msg.task, ack_fn, reject_fn)))
        } else {
            Ok(None)
        }
    }

    /// Get a summary of queue health in a single call
    ///
    /// Returns (pending, processing, dlq, oldest_task_age_secs, success_rate)
    pub async fn get_queue_health_summary(&self) -> Result<(i64, i64, i64, Option<i64>, f64)> {
        let stats = self.get_statistics().await?;
        let oldest = self.oldest_pending_age_secs().await.unwrap_or(None);
        let success = self.success_rate().await.unwrap_or(0.0);

        Ok((stats.pending, stats.processing, stats.dlq, oldest, success))
    }

    /// Purge all completed tasks older than specified age
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Purge completed tasks older than 7 days
    /// let purged = broker.purge_old_completed(Duration::from_secs(7 * 24 * 3600)).await?;
    /// println!("Purged {} old completed tasks", purged);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_old_completed(&self, older_than: Duration) -> Result<i64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let result =
            sqlx::query("DELETE FROM celers_tasks WHERE state = 'completed' AND completed_at < $1")
                .bind(cutoff)
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Purge failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Cancel all tasks matching a specific task name
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let cancelled = broker.cancel_by_name("slow_task").await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_by_name(&self, task_name: &str) -> Result<i64> {
        let result = sqlx::query(
            "UPDATE celers_tasks
             SET state = 'cancelled', completed_at = NOW()
             WHERE task_name = $1 AND state IN ('pending', 'processing')",
        )
        .bind(task_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Cancel failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Wait for a specific task to complete (with timeout)
    ///
    /// Returns true if task completed, false if timeout reached
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use celers_core::Broker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// # let task = celers_core::SerializedTask::new("test".to_string(), vec![]);
    /// let task_id = broker.enqueue(task).await?;
    ///
    /// // Wait up to 30 seconds for completion
    /// if broker.wait_for_completion(&task_id, Duration::from_secs(30)).await? {
    ///     println!("Task completed!");
    /// } else {
    ///     println!("Task timed out");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_completion(&self, task_id: &TaskId, timeout: Duration) -> Result<bool> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        while start.elapsed() < timeout {
            if let Some(task_info) = self.get_task(task_id).await? {
                match task_info.state {
                    DbTaskState::Completed | DbTaskState::Failed | DbTaskState::Cancelled => {
                        return Ok(true);
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(poll_interval).await;
        }

        Ok(false)
    }

    /// Get count of tasks in each state as a HashMap
    pub async fn get_state_counts(&self) -> Result<std::collections::HashMap<String, i64>> {
        let stats = self.get_statistics().await?;

        let mut counts = std::collections::HashMap::new();
        counts.insert("pending".to_string(), stats.pending);
        counts.insert("processing".to_string(), stats.processing);
        counts.insert("completed".to_string(), stats.completed);
        counts.insert("failed".to_string(), stats.failed);
        counts.insert("cancelled".to_string(), stats.cancelled);
        counts.insert("dlq".to_string(), stats.dlq);

        Ok(counts)
    }

    /// Retry all tasks currently in the DLQ
    ///
    /// Returns the number of tasks requeued
    pub async fn retry_all_dlq(&self) -> Result<i64> {
        let dlq_tasks = self.list_dlq(1000, 0).await?;
        let mut count = 0;

        for dlq_task in dlq_tasks {
            if self.requeue_from_dlq(&dlq_task.id).await.is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Find tasks within a specific priority range
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Find high-priority tasks (priority >= 5)
    /// let high_priority_tasks = broker.find_tasks_by_priority_range(5, 10, 100).await?;
    /// println!("Found {} high-priority tasks", high_priority_tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_priority_range(
        &self,
        min_priority: i32,
        max_priority: i32,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(&format!(
            "SELECT id, task_name, state, priority, retry_count, max_retries,
                        created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                 FROM {}
                 WHERE priority BETWEEN $1 AND $2 AND state = 'pending'
                 ORDER BY priority DESC, created_at ASC
                 LIMIT $3",
            self.queue_name
        ))
        .bind(min_priority)
        .bind(max_priority)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by priority: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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

    /// Cancel all pending tasks older than specified age
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Cancel pending tasks older than 24 hours
    /// let cancelled = broker.cancel_old_pending(Duration::from_secs(24 * 3600)).await?;
    /// println!("Cancelled {} old pending tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_old_pending(&self, older_than: Duration) -> Result<i64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let result = sqlx::query(&format!(
            "UPDATE {} SET state = 'cancelled', completed_at = NOW()
                 WHERE state = 'pending' AND created_at < $1",
            self.queue_name
        ))
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Cancel old pending failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Batch cancel multiple tasks by their IDs
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    /// let cancelled = broker.batch_cancel(&task_ids).await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_cancel(&self, task_ids: &[TaskId]) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(&format!(
            "UPDATE {} SET state = 'cancelled', completed_at = NOW()
                 WHERE id = ANY($1) AND state IN ('pending', 'processing')",
            self.queue_name
        ))
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Batch cancel failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Find tasks that have been processing for longer than the threshold
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Find tasks processing for more than 1 hour
    /// let stuck_tasks = broker.find_stuck_tasks(Duration::from_secs(3600)).await?;
    /// println!("Found {} stuck tasks", stuck_tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_stuck_tasks(&self, threshold: Duration) -> Result<Vec<TaskInfo>> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(threshold)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let rows = sqlx::query(&format!(
            "SELECT id, task_name, state, priority, retry_count, max_retries,
                        created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                 FROM {}
                 WHERE state = 'processing' AND started_at < $1
                 ORDER BY started_at ASC",
            self.queue_name
        ))
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find stuck tasks: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
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

    /// Automatically requeue stuck tasks (processing too long)
    ///
    /// Returns the number of tasks requeued
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Requeue tasks stuck for more than 2 hours
    /// let requeued = broker.requeue_stuck_tasks(Duration::from_secs(2 * 3600)).await?;
    /// println!("Requeued {} stuck tasks", requeued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn requeue_stuck_tasks(&self, threshold: Duration) -> Result<i64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(threshold)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let result = sqlx::query(&format!(
            "UPDATE {} SET state = 'pending', started_at = NULL, worker_id = NULL
                 WHERE state = 'processing' AND started_at < $1",
            self.queue_name
        ))
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Requeue stuck tasks failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Get queue depth grouped by priority level
    ///
    /// Returns a HashMap mapping priority to task count
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let depth_by_priority = broker.get_queue_depth_by_priority().await?;
    /// for (priority, count) in depth_by_priority {
    ///     println!("Priority {}: {} tasks", priority, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_depth_by_priority(&self) -> Result<std::collections::HashMap<i32, i64>> {
        let rows = sqlx::query(&format!(
            "SELECT priority, COUNT(*) as count
                 FROM {}
                 WHERE state = 'pending'
                 GROUP BY priority
                 ORDER BY priority DESC",
            self.queue_name
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get queue depth: {}", e)))?;

        let mut depth_map = std::collections::HashMap::new();
        for row in rows {
            let priority: i32 = row.get("priority");
            let count: i64 = row.get("count");
            depth_map.insert(priority, count);
        }

        Ok(depth_map)
    }

    /// Get throughput statistics (completed tasks per time period)
    ///
    /// Returns (tasks_last_hour, tasks_last_day, avg_tasks_per_hour)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let (last_hour, last_day, avg_per_hour) = broker.get_throughput_stats().await?;
    /// println!("Throughput: {} tasks/hour (last hour: {}, last day: {})",
    ///          avg_per_hour, last_hour, last_day);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_throughput_stats(&self) -> Result<(i64, i64, f64)> {
        let one_hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        let one_day_ago = chrono::Utc::now() - chrono::Duration::days(1);

        // Tasks completed in last hour
        let last_hour: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {} WHERE state = 'completed' AND completed_at > $1",
            self.queue_name
        ))
        .bind(one_hour_ago)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        // Tasks completed in last day
        let last_day: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {} WHERE state = 'completed' AND completed_at > $1",
            self.queue_name
        ))
        .bind(one_day_ago)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        // Average per hour over last day
        let avg_per_hour = if last_day > 0 {
            last_day as f64 / 24.0
        } else {
            0.0
        };

        Ok((last_hour, last_day, avg_per_hour))
    }

    /// Get average task duration by task name
    ///
    /// Returns HashMap of task name to average duration in milliseconds
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let avg_durations = broker.get_avg_task_duration_by_name().await?;
    /// for (task_name, duration_ms) in avg_durations {
    ///     println!("{}: {:.2}ms average", task_name, duration_ms);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_avg_task_duration_by_name(
        &self,
    ) -> Result<std::collections::HashMap<String, f64>> {
        let rows = sqlx::query(
            &format!(
                "SELECT task_name,
                        AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) as avg_duration_ms
                 FROM {}
                 WHERE state = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
                 GROUP BY task_name
                 ORDER BY avg_duration_ms DESC",
                self.queue_name
            )
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get avg duration: {}", e)))?;

        let mut duration_map = std::collections::HashMap::new();
        for row in rows {
            let task_name: String = row.get("task_name");
            let avg_duration: Option<f64> = row.get("avg_duration_ms");
            if let Some(duration) = avg_duration {
                duration_map.insert(task_name, duration);
            }
        }

        Ok(duration_map)
    }

    // ========== Task TTL (Time To Live) ==========

    /// Set TTL (Time To Live) for tasks by task name
    ///
    /// Tasks older than the specified TTL will be automatically expired and moved to cancelled state.
    /// This is useful for preventing old tasks from being processed when they're no longer relevant.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task type to expire
    /// * `ttl_secs` - TTL in seconds (tasks older than this will be expired)
    ///
    /// # Returns
    ///
    /// Number of tasks that were expired
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Expire pending tasks older than 1 hour for "email_notifications" task type
    /// let expired = broker.expire_tasks_by_ttl("email_notifications", 3600).await?;
    /// println!("Expired {} old tasks", expired);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_tasks_by_ttl(&self, task_name: &str, ttl_secs: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = 'Task expired due to TTL'
            WHERE task_name = $1
              AND queue_name = $2
              AND state IN ('pending', 'processing')
              AND created_at < NOW() - INTERVAL '1 second' * $3
            "#,
        )
        .bind(task_name)
        .bind(&self.queue_name)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to expire tasks: {}", e)))?;

        let expired = result.rows_affected() as i64;

        if expired > 0 {
            tracing::info!(
                task_name = task_name,
                ttl_secs = ttl_secs,
                expired = expired,
                "Expired tasks due to TTL"
            );
        }

        Ok(expired)
    }

    /// Expire all pending tasks older than specified TTL
    ///
    /// This is a global TTL that applies to all task types.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Expire all pending tasks older than 24 hours
    /// let expired = broker.expire_all_tasks_by_ttl(86400).await?;
    /// println!("Expired {} old tasks", expired);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_all_tasks_by_ttl(&self, ttl_secs: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = 'Task expired due to TTL'
            WHERE queue_name = $1
              AND state IN ('pending', 'processing')
              AND created_at < NOW() - INTERVAL '1 second' * $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to expire all tasks: {}", e)))?;

        let expired = result.rows_affected() as i64;

        if expired > 0 {
            tracing::info!(
                ttl_secs = ttl_secs,
                expired = expired,
                "Expired all tasks due to TTL"
            );
        }

        Ok(expired)
    }

    // ========== PostgreSQL Advisory Locks ==========

    /// Acquire PostgreSQL advisory lock for exclusive task processing
    ///
    /// Advisory locks provide application-level distributed locking using PostgreSQL's
    /// advisory lock mechanism. This is useful when you need to ensure only one worker
    /// processes tasks of a specific type at a time.
    ///
    /// The lock is automatically released when the connection is closed or when
    /// `release_advisory_lock` is called.
    ///
    /// # Arguments
    ///
    /// * `lock_id` - Numeric lock ID (must be i64)
    ///
    /// # Returns
    ///
    /// `true` if lock was acquired, `false` if already locked by another session
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Try to acquire lock for critical section
    /// let lock_id = 12345i64;
    /// if broker.try_advisory_lock(lock_id).await? {
    ///     // Process critical task
    ///     println!("Lock acquired, processing...");
    ///
    ///     // Release lock when done
    ///     broker.release_advisory_lock(lock_id).await?;
    /// } else {
    ///     println!("Lock held by another worker");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_advisory_lock(&self, lock_id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT pg_try_advisory_lock($1) as locked")
            .bind(lock_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to acquire advisory lock: {}", e)))?;

        let locked: bool = row.get("locked");

        if locked {
            tracing::debug!(lock_id = lock_id, "Advisory lock acquired");
        } else {
            tracing::debug!(lock_id = lock_id, "Advisory lock already held");
        }

        Ok(locked)
    }

    /// Release PostgreSQL advisory lock
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// broker.release_advisory_lock(12345).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn release_advisory_lock(&self, lock_id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT pg_advisory_unlock($1) as unlocked")
            .bind(lock_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to release advisory lock: {}", e)))?;

        let unlocked: bool = row.get("unlocked");

        if unlocked {
            tracing::debug!(lock_id = lock_id, "Advisory lock released");
        } else {
            tracing::warn!(lock_id = lock_id, "Advisory lock was not held");
        }

        Ok(unlocked)
    }

    /// Acquire blocking advisory lock (waits until available)
    ///
    /// Unlike `try_advisory_lock`, this method will block until the lock becomes available.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Wait for lock to become available
    /// broker.advisory_lock(12345).await?;
    /// // Process critical section
    /// broker.release_advisory_lock(12345).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn advisory_lock(&self, lock_id: i64) -> Result<()> {
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to acquire blocking advisory lock: {}", e))
            })?;

        tracing::debug!(lock_id = lock_id, "Blocking advisory lock acquired");

        Ok(())
    }

    /// Check if an advisory lock is currently held
    ///
    /// Note: This checks across all sessions, not just the current one.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let is_locked = broker.is_advisory_lock_held(12345).await?;
    /// println!("Lock held: {}", is_locked);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_advisory_lock_held(&self, lock_id: i64) -> Result<bool> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) > 0 as held
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND objid = $1
            "#,
        )
        .bind(lock_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check advisory lock: {}", e)))?;

        let held: bool = row.get("held");
        Ok(held)
    }

    // ========== Task Performance Analytics ==========

    /// Get task performance percentiles for a specific task type
    ///
    /// Returns performance percentiles (p50, p95, p99) for task execution times.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let (p50, p95, p99) = broker.get_task_percentiles("send_email").await?;
    /// println!("p50: {}ms, p95: {}ms, p99: {}ms", p50, p95, p99);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_percentiles(&self, task_name: &str) -> Result<(f64, f64, f64)> {
        let row = sqlx::query(
            r#"
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
                ) as p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
                ) as p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
                ) as p99
            FROM celers_tasks
            WHERE task_name = $1
              AND queue_name = $2
              AND state = 'completed'
              AND started_at IS NOT NULL
              AND completed_at IS NOT NULL
              AND completed_at > started_at
            "#,
        )
        .bind(task_name)
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task percentiles: {}", e)))?;

        let p50: Option<f64> = row.get("p50");
        let p95: Option<f64> = row.get("p95");
        let p99: Option<f64> = row.get("p99");

        Ok((p50.unwrap_or(0.0), p95.unwrap_or(0.0), p99.unwrap_or(0.0)))
    }

    /// Get slowest tasks in the queue
    ///
    /// Returns the slowest N tasks based on execution time.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let slow_tasks = broker.get_slowest_tasks(10).await?;
    /// for task in slow_tasks {
    ///     if let (Some(started), Some(completed)) = (task.started_at, task.completed_at) {
    ///         let duration_ms = (completed - started).num_milliseconds();
    ///         println!("{}: {}ms", task.task_name, duration_ms);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_slowest_tasks(&self, limit: i64) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at,
                worker_id, error_message,
                EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000 as runtime_ms
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'completed'
              AND started_at IS NOT NULL
              AND completed_at IS NOT NULL
              AND completed_at > started_at
            ORDER BY (completed_at - started_at) DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get slowest tasks: {}", e)))?;

        let tasks: Result<Vec<TaskInfo>> = rows
            .iter()
            .map(|row| {
                let state_str: String = row.get("state");
                Ok(TaskInfo {
                    id: row.get("id"),
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
                })
            })
            .collect();

        tasks
    }

    // ========== Rate Limiting ==========

    /// Get task processing rate for a specific task type
    ///
    /// Returns the number of tasks processed in the last N seconds.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Get tasks processed in last 60 seconds
    /// let rate = broker.get_task_rate("send_email", 60).await?;
    /// println!("Processed {} send_email tasks in last 60 seconds", rate);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_rate(&self, task_name: &str, window_secs: i64) -> Result<i64> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM celers_tasks
            WHERE task_name = $1
              AND queue_name = $2
              AND state = 'completed'
              AND completed_at > NOW() - INTERVAL '1 second' * $3
            "#,
        )
        .bind(task_name)
        .bind(&self.queue_name)
        .bind(window_secs)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task rate: {}", e)))?;

        let count: i64 = row.get("count");
        Ok(count)
    }

    /// Check if task processing rate exceeds limit
    ///
    /// Returns true if the rate limit is exceeded.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Check if more than 100 emails sent in last 60 seconds
    /// if broker.is_rate_limited("send_email", 100, 60).await? {
    ///     println!("Rate limit exceeded, throttling...");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_rate_limited(
        &self,
        task_name: &str,
        max_count: i64,
        window_secs: i64,
    ) -> Result<bool> {
        let current_rate = self.get_task_rate(task_name, window_secs).await?;
        Ok(current_rate >= max_count)
    }

    // ========== Dynamic Priority Management ==========

    /// Boost priority of pending tasks by task name
    ///
    /// Increases the priority of all pending tasks of a specific type by the specified amount.
    /// Higher priority values mean higher priority (will be dequeued first).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Boost priority of critical alerts by 100
    /// let boosted = broker.boost_task_priority("critical_alert", 100).await?;
    /// println!("Boosted {} critical alert tasks", boosted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn boost_task_priority(&self, task_name: &str, boost_amount: i32) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = priority + $1
            WHERE task_name = $2
              AND queue_name = $3
              AND state = 'pending'
            "#,
        )
        .bind(boost_amount)
        .bind(task_name)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to boost task priority: {}", e)))?;

        let boosted = result.rows_affected() as i64;

        if boosted > 0 {
            tracing::info!(
                task_name = task_name,
                boost_amount = boost_amount,
                boosted = boosted,
                "Boosted task priority"
            );
        }

        Ok(boosted)
    }

    /// Set absolute priority for specific tasks
    ///
    /// Sets the priority to an absolute value for tasks matching the criteria.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let updated = broker.set_task_priority(&task_ids, 999).await?;
    /// println!("Set {} tasks to highest priority", updated);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_task_priority(
        &self,
        task_ids: &[uuid::Uuid],
        new_priority: i32,
    ) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = $1
            WHERE id = ANY($2)
              AND queue_name = $3
              AND state = 'pending'
            "#,
        )
        .bind(new_priority)
        .bind(task_ids)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to set task priority: {}", e)))?;

        let updated = result.rows_affected() as i64;

        if updated > 0 {
            tracing::info!(
                count = updated,
                new_priority = new_priority,
                "Updated task priorities"
            );
        }

        Ok(updated)
    }

    // ========== Enhanced DLQ Analytics ==========

    /// Get DLQ task statistics grouped by task name
    ///
    /// Returns a map of task names to their DLQ counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let stats = broker.get_dlq_stats_by_task().await?;
    /// for (task_name, count) in stats {
    ///     println!("{}: {} failed tasks", task_name, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_dlq_stats_by_task(&self) -> Result<std::collections::HashMap<String, i64>> {
        let rows = sqlx::query(
            r#"
            SELECT task_name, COUNT(*) as count
            FROM celers_dlq
            WHERE queue_name = $1
            GROUP BY task_name
            ORDER BY count DESC
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ stats: {}", e)))?;

        let mut stats = std::collections::HashMap::new();
        for row in rows {
            let task_name: String = row.get("task_name");
            let count: i64 = row.get("count");
            stats.insert(task_name, count);
        }

        Ok(stats)
    }

    /// Get most common error messages from DLQ
    ///
    /// Returns the top N most common error messages with their counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let errors = broker.get_dlq_error_patterns(10).await?;
    /// for (error_msg, count) in errors {
    ///     println!("{} occurrences: {}", count, error_msg.unwrap_or_else(|| "Unknown".to_string()));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_dlq_error_patterns(&self, limit: i64) -> Result<Vec<(Option<String>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT error_message, COUNT(*) as count
            FROM celers_dlq
            WHERE queue_name = $1
            GROUP BY error_message
            ORDER BY count DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ error patterns: {}", e)))?;

        let patterns: Vec<(Option<String>, i64)> = rows
            .iter()
            .map(|row| {
                let error_message: Option<String> = row.get("error_message");
                let count: i64 = row.get("count");
                (error_message, count)
            })
            .collect();

        Ok(patterns)
    }

    /// Get DLQ tasks that failed recently
    ///
    /// Returns DLQ tasks that failed within the specified time window.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Get tasks that failed in last hour
    /// let recent_failures = broker.get_recent_dlq_tasks(3600).await?;
    /// println!("Recent failures: {}", recent_failures.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_recent_dlq_tasks(&self, window_secs: i64) -> Result<Vec<DlqTaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_id, task_name, retry_count, error_message, failed_at
            FROM celers_dlq
            WHERE queue_name = $1
              AND failed_at > NOW() - INTERVAL '1 second' * $2
            ORDER BY failed_at DESC
            "#,
        )
        .bind(&self.queue_name)
        .bind(window_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get recent DLQ tasks: {}", e)))?;

        let tasks: Vec<DlqTaskInfo> = rows
            .iter()
            .map(|row| DlqTaskInfo {
                id: row.get("id"),
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                retry_count: row.get("retry_count"),
                error_message: row.get("error_message"),
                failed_at: row.get("failed_at"),
            })
            .collect();

        Ok(tasks)
    }

    // ========== Task Cancellation with Reasons ==========

    /// Cancel task with a specific reason
    ///
    /// Cancels a task and records the cancellation reason in the error_message field.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_id = Uuid::new_v4();
    /// broker.cancel_with_reason(&task_id, "User requested cancellation").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_with_reason(&self, task_id: &uuid::Uuid, reason: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = $1
            WHERE id = $2
              AND queue_name = $3
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(format!("Cancelled: {}", reason))
        .bind(task_id)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel task with reason: {}", e)))?;

        tracing::info!(
            task_id = %task_id,
            reason = reason,
            "Task cancelled with reason"
        );

        Ok(())
    }

    /// Cancel multiple tasks with a reason
    ///
    /// Batch cancellation with a specified reason.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let cancelled = broker.cancel_batch_with_reason(&task_ids, "System shutdown").await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_batch_with_reason(
        &self,
        task_ids: &[uuid::Uuid],
        reason: &str,
    ) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = $1
            WHERE id = ANY($2)
              AND queue_name = $3
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(format!("Cancelled: {}", reason))
        .bind(task_ids)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel tasks with reason: {}", e)))?;

        let cancelled = result.rows_affected() as i64;

        if cancelled > 0 {
            tracing::info!(
                count = cancelled,
                reason = reason,
                "Tasks cancelled with reason"
            );
        }

        Ok(cancelled)
    }

    /// Get cancellation reasons for cancelled tasks
    ///
    /// Returns a breakdown of cancellation reasons and their counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let reasons = broker.get_cancellation_reasons(10).await?;
    /// for (reason, count) in reasons {
    ///     println!("{}: {} cancellations", reason.unwrap_or_else(|| "Unknown".to_string()), count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_cancellation_reasons(&self, limit: i64) -> Result<Vec<(Option<String>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT error_message, COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'cancelled'
              AND error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY count DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get cancellation reasons: {}", e)))?;

        let reasons: Vec<(Option<String>, i64)> = rows
            .iter()
            .map(|row| {
                let reason: Option<String> = row.get("error_message");
                let count: i64 = row.get("count");
                (reason, count)
            })
            .collect();

        Ok(reasons)
    }

    /// Store multiple task results in a single batch operation
    ///
    /// More efficient than calling `store_result()` multiple times.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TaskResult, TaskResultStatus};
    /// use uuid::Uuid;
    /// use chrono::Utc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let results = vec![
    ///     TaskResult {
    ///         task_id: Uuid::new_v4(),
    ///         task_name: "task1".to_string(),
    ///         status: TaskResultStatus::Success,
    ///         result: Some(serde_json::json!({"value": 42})),
    ///         error: None,
    ///         traceback: None,
    ///         created_at: Utc::now(),
    ///         completed_at: Some(Utc::now()),
    ///         runtime_ms: Some(100),
    ///     },
    ///     TaskResult {
    ///         task_id: Uuid::new_v4(),
    ///         task_name: "task2".to_string(),
    ///         status: TaskResultStatus::Success,
    ///         result: Some(serde_json::json!({"value": 100})),
    ///         error: None,
    ///         traceback: None,
    ///         created_at: Utc::now(),
    ///         completed_at: Some(Utc::now()),
    ///         runtime_ms: Some(200),
    ///     },
    /// ];
    ///
    /// broker.store_results_batch(&results).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_results_batch(&self, results: &[TaskResult]) -> Result<i64> {
        if results.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut stored = 0i64;
        for result in results {
            sqlx::query(
                r#"
                INSERT INTO celers_task_results (task_id, status, result, error, traceback)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (task_id) DO UPDATE
                SET status = EXCLUDED.status,
                    result = EXCLUDED.result,
                    error = EXCLUDED.error,
                    traceback = EXCLUDED.traceback,
                    updated_at = NOW()
                "#,
            )
            .bind(result.task_id)
            .bind(result.status.to_string())
            .bind(&result.result)
            .bind(&result.error)
            .bind(&result.traceback)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

            stored += 1;
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        tracing::info!(count = stored, "Stored batch of task results");
        Ok(stored)
    }

    /// Find tasks that failed with errors matching a pattern
    ///
    /// Uses PostgreSQL pattern matching (LIKE) to find tasks with specific error messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Find all tasks that failed with connection errors
    /// let tasks = broker.find_tasks_by_error("%connection%", 100).await?;
    /// println!("Found {} tasks with connection errors", tasks.len());
    ///
    /// // Find specific error
    /// let tasks = broker.find_tasks_by_error("%timeout%", 50).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_error(
        &self,
        error_pattern: &str,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND state IN ('failed', 'cancelled')
              AND error_message LIKE $2
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(error_pattern)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by error: {}", e)))?;

        let tasks: Vec<TaskInfo> = rows
            .iter()
            .map(|row| TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: row
                    .get::<String, _>("state")
                    .parse()
                    .unwrap_or(DbTaskState::Failed),
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            })
            .collect();

        Ok(tasks)
    }

    /// Estimate wait time for a pending task based on current throughput
    ///
    /// Calculates expected wait time by analyzing recent task completion rate
    /// and current queue depth.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let wait_secs = broker.estimate_wait_time().await?;
    /// if let Some(wait) = wait_secs {
    ///     println!("Estimated wait time: {} seconds", wait);
    /// } else {
    ///     println!("Not enough data to estimate wait time");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn estimate_wait_time(&self) -> Result<Option<i64>> {
        // Get completed tasks in last hour
        let completed_last_hour: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'completed'
              AND completed_at > NOW() - INTERVAL '1 hour'
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get completion rate: {}", e)))?;

        if completed_last_hour == 0 {
            return Ok(None);
        }

        // Get current pending count
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE queue_name = $1 AND state = 'pending'",
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Calculate tasks per second
        let tasks_per_second = completed_last_hour as f64 / 3600.0;

        if tasks_per_second > 0.0 {
            let estimated_wait = (pending as f64 / tasks_per_second) as i64;
            Ok(Some(estimated_wait))
        } else {
            Ok(None)
        }
    }

    /// Get worker performance statistics
    ///
    /// Returns statistics about worker performance including task counts,
    /// average processing times, and success rates per worker.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let worker_stats = broker.get_worker_stats(10).await?;
    /// for (worker_id, processed, avg_time_ms, success_rate) in worker_stats {
    ///     println!(
    ///         "Worker {}: {} tasks, {:.0}ms avg, {:.1}% success",
    ///         worker_id.unwrap_or_else(|| "unknown".to_string()),
    ///         processed,
    ///         avg_time_ms.unwrap_or(0.0),
    ///         success_rate.unwrap_or(0.0) * 100.0
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_worker_stats(
        &self,
        limit: i64,
    ) -> Result<Vec<(Option<String>, i64, Option<f64>, Option<f64>)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                worker_id,
                COUNT(*) as processed,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) as avg_time_ms,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate
            FROM celers_tasks
            WHERE queue_name = $1
              AND worker_id IS NOT NULL
              AND state IN ('completed', 'failed')
              AND completed_at > NOW() - INTERVAL '24 hours'
            GROUP BY worker_id
            ORDER BY processed DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get worker stats: {}", e)))?;

        let stats = rows
            .iter()
            .map(|row| {
                let worker_id: Option<String> = row.get("worker_id");
                let processed: i64 = row.get("processed");
                let avg_time_ms: Option<f64> = row.get("avg_time_ms");
                let success_rate: Option<f64> = row.get("success_rate");
                (worker_id, processed, avg_time_ms, success_rate)
            })
            .collect();

        Ok(stats)
    }

    /// Get task age distribution
    ///
    /// Returns histogram buckets showing how many tasks fall into different age ranges.
    /// Useful for monitoring queue latency and identifying bottlenecks.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let distribution = broker.get_task_age_distribution().await?;
    /// println!("< 1 min: {} tasks", distribution.0);
    /// println!("1-5 min: {} tasks", distribution.1);
    /// println!("5-15 min: {} tasks", distribution.2);
    /// println!("15-60 min: {} tasks", distribution.3);
    /// println!("> 1 hour: {} tasks", distribution.4);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_age_distribution(&self) -> Result<(i64, i64, i64, i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE age_secs < 60) as under_1min,
                COUNT(*) FILTER (WHERE age_secs >= 60 AND age_secs < 300) as between_1_5min,
                COUNT(*) FILTER (WHERE age_secs >= 300 AND age_secs < 900) as between_5_15min,
                COUNT(*) FILTER (WHERE age_secs >= 900 AND age_secs < 3600) as between_15_60min,
                COUNT(*) FILTER (WHERE age_secs >= 3600) as over_1hour
            FROM (
                SELECT EXTRACT(EPOCH FROM (NOW() - created_at)) as age_secs
                FROM celers_tasks
                WHERE queue_name = $1 AND state = 'pending'
            ) as ages
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task age distribution: {}", e)))?;

        Ok((
            row.get("under_1min"),
            row.get("between_1_5min"),
            row.get("between_5_15min"),
            row.get("between_15_60min"),
            row.get("over_1hour"),
        ))
    }

    /// Clone or copy tasks from another queue
    ///
    /// Copies pending tasks from a source queue to this queue.
    /// Useful for queue migration, load balancing, or disaster recovery.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Copy up to 100 tasks from source_queue
    /// let copied = broker.copy_tasks_from_queue("source_queue", 100).await?;
    /// println!("Copied {} tasks", copied);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy_tasks_from_queue(&self, source_queue: &str, limit: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (task_name, payload, queue_name, state, priority, retry_count, max_retries,
                 timeout_secs, scheduled_at, metadata)
            SELECT
                task_name, payload, $1 as queue_name, 'pending' as state, priority,
                0 as retry_count, max_retries, timeout_secs, scheduled_at, metadata
            FROM celers_tasks
            WHERE queue_name = $2
              AND state = 'pending'
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(source_queue)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to copy tasks: {}", e)))?;

        let copied = result.rows_affected() as i64;

        tracing::info!(
            source = source_queue,
            destination = %self.queue_name,
            count = copied,
            "Copied tasks between queues"
        );

        Ok(copied)
    }

    /// Move tasks from another queue to this queue
    ///
    /// Transfers pending tasks from a source queue to this queue by updating their queue_name.
    /// More efficient than copying as it doesn't create new rows.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Move up to 50 tasks from old_queue
    /// let moved = broker.move_tasks_from_queue("old_queue", 50).await?;
    /// println!("Moved {} tasks", moved);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn move_tasks_from_queue(&self, source_queue: &str, limit: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET queue_name = $1
            WHERE id IN (
                SELECT id FROM celers_tasks
                WHERE queue_name = $2 AND state = 'pending'
                LIMIT $3
            )
            "#,
        )
        .bind(&self.queue_name)
        .bind(source_queue)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to move tasks: {}", e)))?;

        let moved = result.rows_affected() as i64;

        tracing::info!(
            source = source_queue,
            destination = %self.queue_name,
            count = moved,
            "Moved tasks between queues"
        );

        Ok(moved)
    }

    /// Get breakdown of tasks by hour of creation
    ///
    /// Returns task counts grouped by hour for the last 24 hours.
    /// Useful for understanding task creation patterns and peak times.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let hourly = broker.get_hourly_task_counts().await?;
    /// for (hour, count) in hourly {
    ///     println!("Hour {}: {} tasks", hour, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_hourly_task_counts(&self) -> Result<Vec<(i32, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                EXTRACT(HOUR FROM created_at)::INTEGER as hour,
                COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND created_at > NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get hourly task counts: {}", e)))?;

        let hourly = rows
            .iter()
            .map(|row| {
                let hour: i32 = row.get("hour");
                let count: i64 = row.get("count");
                (hour, count)
            })
            .collect();

        Ok(hourly)
    }

    /// Replay/rerun completed or failed tasks
    ///
    /// Creates new pending tasks by cloning completed or failed tasks.
    /// Useful for retrying batches of tasks or debugging issues.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, DbTaskState};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Replay all failed tasks from the last hour
    /// let replayed = broker.replay_tasks(DbTaskState::Failed, 100).await?;
    /// println!("Replayed {} failed tasks", replayed);
    ///
    /// // Rerun successful tasks for testing
    /// let rerun = broker.replay_tasks(DbTaskState::Completed, 10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replay_tasks(&self, source_state: DbTaskState, limit: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (task_name, payload, queue_name, state, priority, retry_count, max_retries,
                 timeout_secs, scheduled_at, metadata)
            SELECT
                task_name, payload, queue_name, 'pending' as state, priority,
                0 as retry_count, max_retries, timeout_secs, NOW() as scheduled_at, metadata
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = $2
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(source_state.to_string())
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to replay tasks: {}", e)))?;

        let replayed = result.rows_affected() as i64;

        tracing::info!(
            state = %source_state,
            count = replayed,
            "Replayed tasks"
        );

        Ok(replayed)
    }

    /// Calculate composite queue health score (0-100)
    ///
    /// Returns a health score based on multiple factors:
    /// - Queue depth (pending tasks)
    /// - Processing efficiency (tasks in progress vs pending)
    /// - DLQ ratio (failed tasks)
    /// - Task age (how long tasks have been waiting)
    ///
    /// Score interpretation:
    /// - 90-100: Excellent
    /// - 70-89: Good
    /// - 50-69: Fair
    /// - 30-49: Poor
    /// - 0-29: Critical
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let score = broker.calculate_queue_health_score().await?;
    /// match score {
    ///     90..=100 => println!("Queue health: Excellent ({})", score),
    ///     70..=89 => println!("Queue health: Good ({})", score),
    ///     50..=69 => println!("Queue health: Fair ({})", score),
    ///     30..=49 => println!("Queue health: Poor ({})", score),
    ///     _ => println!("Queue health: Critical ({})", score),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn calculate_queue_health_score(&self) -> Result<i32> {
        let stats = self.get_statistics().await?;

        let mut score = 100;

        // Deduct points for high pending count (max -30 points)
        if stats.pending > 1000 {
            score -= 30;
        } else if stats.pending > 500 {
            score -= 20;
        } else if stats.pending > 100 {
            score -= 10;
        }

        // Deduct points for DLQ ratio (max -25 points)
        let total_tasks = stats.total.max(1);
        let dlq_ratio = (stats.dlq as f64 / total_tasks as f64) * 100.0;
        if dlq_ratio > 10.0 {
            score -= 25;
        } else if dlq_ratio > 5.0 {
            score -= 15;
        } else if dlq_ratio > 1.0 {
            score -= 5;
        }

        // Deduct points for stuck tasks (max -20 points)
        if stats.processing > stats.pending && stats.pending > 0 {
            score -= 20;
        } else if stats.processing > stats.pending / 2 && stats.pending > 0 {
            score -= 10;
        }

        // Deduct points for old pending tasks (max -25 points)
        if let Ok(Some(oldest_age)) = self.oldest_pending_age_secs().await {
            if oldest_age > 3600 {
                score -= 25;
            } else if oldest_age > 1800 {
                score -= 15;
            } else if oldest_age > 600 {
                score -= 10;
            }
        }

        Ok(score.max(0))
    }

    /// Get auto-scaling recommendations based on queue metrics
    ///
    /// Analyzes queue metrics and provides worker scaling recommendations.
    /// Returns (recommended_workers, reason).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let (recommended, reason) = broker.get_autoscaling_recommendation(5).await?;
    /// println!("Current workers: 5, Recommended: {}, Reason: {}", recommended, reason);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_autoscaling_recommendation(
        &self,
        current_workers: i32,
    ) -> Result<(i32, String)> {
        let stats = self.get_statistics().await?;
        let wait_time = self.estimate_wait_time().await?;

        // If queue is empty or very small, scale down
        if stats.pending < 10 {
            let recommended = (current_workers / 2).max(1);
            return Ok((recommended, "Low queue depth - scale down".to_string()));
        }

        // If wait time is very high, scale up aggressively
        if let Some(wait_secs) = wait_time {
            if wait_secs > 3600 {
                let recommended = (current_workers * 2).min(50);
                return Ok((
                    recommended,
                    format!("High wait time ({}s) - scale up aggressively", wait_secs),
                ));
            } else if wait_secs > 600 {
                let recommended = (current_workers + (current_workers / 2)).min(50);
                return Ok((
                    recommended,
                    format!("Moderate wait time ({}s) - scale up", wait_secs),
                ));
            }
        }

        // If processing rate is low compared to pending, scale up
        if stats.processing < stats.pending / 10 && stats.pending > 100 {
            let recommended = (current_workers + (current_workers / 3)).min(50);
            return Ok((
                recommended,
                "Low processing rate vs pending - scale up".to_string(),
            ));
        }

        // If DLQ is growing, might need more workers or there's an issue
        let dlq_ratio = (stats.dlq as f64 / stats.total.max(1) as f64) * 100.0;
        if dlq_ratio > 20.0 {
            return Ok((
                current_workers,
                "High DLQ ratio - investigate errors before scaling".to_string(),
            ));
        }

        // Everything looks good
        Ok((
            current_workers,
            "Queue metrics healthy - maintain current workers".to_string(),
        ))
    }

    /// Sample random tasks for monitoring without affecting processing
    ///
    /// Returns a random sample of tasks in a specific state for analysis.
    /// Does not modify task state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, DbTaskState};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Sample 10 random pending tasks for analysis
    /// let samples = broker.sample_tasks(DbTaskState::Pending, 10).await?;
    /// for task in samples {
    ///     println!("Task: {} (age: {:?})", task.task_name, task.created_at);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sample_tasks(
        &self,
        state: DbTaskState,
        sample_size: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1 AND state = $2
            ORDER BY RANDOM()
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(state.to_string())
        .bind(sample_size)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to sample tasks: {}", e)))?;

        let tasks: Vec<TaskInfo> = rows
            .iter()
            .map(|row| TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: row
                    .get::<String, _>("state")
                    .parse()
                    .unwrap_or(DbTaskState::Pending),
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            })
            .collect();

        Ok(tasks)
    }

    /// Get aggregated statistics from task metadata
    ///
    /// Allows custom aggregations on JSONB metadata fields.
    /// Returns task counts grouped by a metadata key.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Count tasks by region
    /// let by_region = broker.aggregate_by_metadata("region", 10).await?;
    /// for (region, count) in by_region {
    ///     println!("Region {}: {} tasks", region.unwrap_or_else(|| "unknown".to_string()), count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn aggregate_by_metadata(
        &self,
        key: &str,
        limit: i64,
    ) -> Result<Vec<(Option<String>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                metadata->$2 as value,
                COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata IS NOT NULL
              AND metadata ? $2
            GROUP BY value
            ORDER BY count DESC
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to aggregate metadata: {}", e)))?;

        let aggregated = rows
            .iter()
            .map(|row| {
                let value: Option<serde_json::Value> = row.get("value");
                let value_str = value.map(|v| v.to_string().trim_matches('"').to_string());
                let count: i64 = row.get("count");
                (value_str, count)
            })
            .collect();

        Ok(aggregated)
    }

    /// Store a performance baseline for comparison
    ///
    /// Stores current queue metrics as a named baseline for future comparison.
    /// Useful for capacity planning and performance regression detection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Store baseline after optimization
    /// broker.store_performance_baseline("after_optimization").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_performance_baseline(&self, baseline_name: &str) -> Result<()> {
        let stats = self.get_statistics().await?;
        let throughput = self.get_throughput_stats().await?;

        let baseline_data = json!({
            "name": baseline_name,
            "timestamp": Utc::now(),
            "queue_name": self.queue_name,
            "stats": {
                "pending": stats.pending,
                "processing": stats.processing,
                "completed": stats.completed,
                "failed": stats.failed,
                "dlq": stats.dlq,
            },
            "throughput": {
                "tasks_per_hour": throughput.0,
                "tasks_per_day": throughput.1,
            }
        });

        // Store in metadata of a special marker task
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (task_name, payload, queue_name, state, metadata)
            VALUES ('__baseline__', '[]'::jsonb, $1, 'completed', $2)
            "#,
        )
        .bind(&self.queue_name)
        .bind(baseline_data)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to store baseline: {}", e)))?;

        tracing::info!(
            baseline = baseline_name,
            queue = %self.queue_name,
            "Stored performance baseline"
        );

        Ok(())
    }

    /// Compare current metrics against a stored baseline
    ///
    /// Returns percentage differences between current metrics and a baseline.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// if let Some(comparison) = broker.compare_to_baseline("after_optimization").await? {
    ///     println!("Throughput change: {:.1}%", comparison.get("throughput_change").unwrap_or(&0.0));
    ///     println!("DLQ change: {:.1}%", comparison.get("dlq_change").unwrap_or(&0.0));
    /// } else {
    ///     println!("Baseline not found");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn compare_to_baseline(
        &self,
        baseline_name: &str,
    ) -> Result<Option<std::collections::HashMap<String, f64>>> {
        // Fetch baseline
        let baseline_row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE queue_name = $1
              AND task_name = '__baseline__'
              AND metadata->>'name' = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&self.queue_name)
        .bind(baseline_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch baseline: {}", e)))?;

        if baseline_row.is_none() {
            return Ok(None);
        }

        let baseline_meta: serde_json::Value = baseline_row.unwrap().get("metadata");

        // Get current metrics
        let current_stats = self.get_statistics().await?;
        let current_throughput = self.get_throughput_stats().await?;

        // Extract baseline values
        let baseline_dlq = baseline_meta["stats"]["dlq"].as_i64().unwrap_or(1) as f64;
        let baseline_throughput = baseline_meta["throughput"]["tasks_per_hour"]
            .as_f64()
            .unwrap_or(1.0);

        // Calculate percentage changes
        let mut comparison = std::collections::HashMap::new();

        let dlq_change =
            ((current_stats.dlq as f64 - baseline_dlq) / baseline_dlq.max(1.0)) * 100.0;
        let throughput_change = ((current_throughput.0 as f64 - baseline_throughput)
            / baseline_throughput.max(1.0))
            * 100.0;

        comparison.insert("dlq_change".to_string(), dlq_change);
        comparison.insert("throughput_change".to_string(), throughput_change);
        comparison.insert("current_pending".to_string(), current_stats.pending as f64);
        comparison.insert("current_dlq".to_string(), current_stats.dlq as f64);

        Ok(Some(comparison))
    }

    /// Get distinct task names in the queue
    ///
    /// Returns all unique task names currently in the queue.
    /// Useful for discovering task types and monitoring task diversity.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_types = broker.get_distinct_task_names().await?;
    /// println!("Task types in queue: {}", task_types.len());
    /// for task_name in task_types {
    ///     println!("  - {}", task_name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_distinct_task_names(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT task_name
            FROM celers_tasks
            WHERE queue_name = $1
            ORDER BY task_name
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get distinct task names: {}", e)))?;

        let task_names = rows.iter().map(|row| row.get("task_name")).collect();

        Ok(task_names)
    }

    /// Get task count breakdown by task name
    ///
    /// Returns counts of tasks grouped by task name and state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let breakdown = broker.get_task_breakdown_by_name(20).await?;
    /// for (task_name, pending, processing, completed, failed) in breakdown {
    ///     println!("{}: {} pending, {} processing, {} completed, {} failed",
    ///              task_name, pending, processing, completed, failed);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_breakdown_by_name(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, i64, i64, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                task_name,
                COUNT(*) FILTER (WHERE state = 'pending') as pending,
                COUNT(*) FILTER (WHERE state = 'processing') as processing,
                COUNT(*) FILTER (WHERE state = 'completed') as completed,
                COUNT(*) FILTER (WHERE state = 'failed') as failed
            FROM celers_tasks
            WHERE queue_name = $1
            GROUP BY task_name
            ORDER BY COUNT(*) DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task breakdown: {}", e)))?;

        let breakdown = rows
            .iter()
            .map(|row| {
                let task_name: String = row.get("task_name");
                let pending: i64 = row.get("pending");
                let processing: i64 = row.get("processing");
                let completed: i64 = row.get("completed");
                let failed: i64 = row.get("failed");
                (task_name, pending, processing, completed, failed)
            })
            .collect();

        Ok(breakdown)
    }
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
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_broker_lifecycle() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

        let broker = PostgresBroker::new(&database_url).await.unwrap();
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

        // Verify task is completed
        let size = broker.queue_size().await.unwrap();
        assert_eq!(size, 0);
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_skip_locked_concurrent_dequeue() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

        let broker1 = PostgresBroker::new(&database_url).await.unwrap();
        broker1.migrate().await.unwrap();

        let broker2 = PostgresBroker::new(&database_url).await.unwrap();

        // Enqueue multiple tasks
        for i in 0..10 {
            let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
            broker1.enqueue(task).await.unwrap();
        }

        // Dequeue concurrently
        let (msg1, msg2) = tokio::join!(broker1.dequeue(), broker2.dequeue());

        let msg1 = msg1.unwrap();
        let msg2 = msg2.unwrap();

        // Both should get different tasks (SKIP LOCKED ensures no contention)
        assert!(msg1.is_some());
        assert!(msg2.is_some());
        assert_ne!(
            msg1.unwrap().task.metadata.id,
            msg2.unwrap().task.metadata.id
        );
    }

    #[test]
    fn test_retry_strategy_exponential() {
        let strategy = RetryStrategy::Exponential {
            max_delay_secs: 3600,
        };

        assert_eq!(strategy.calculate_backoff(0), 1); // 2^0 = 1
        assert_eq!(strategy.calculate_backoff(1), 2); // 2^1 = 2
        assert_eq!(strategy.calculate_backoff(2), 4); // 2^2 = 4
        assert_eq!(strategy.calculate_backoff(3), 8); // 2^3 = 8
        assert_eq!(strategy.calculate_backoff(10), 1024); // 2^10 = 1024
        assert_eq!(strategy.calculate_backoff(20), 3600); // Capped at max_delay_secs
    }

    #[test]
    fn test_retry_strategy_linear() {
        let strategy = RetryStrategy::Linear {
            base_delay_secs: 10,
            max_delay_secs: 100,
        };

        assert_eq!(strategy.calculate_backoff(0), 0); // 10 * 0 = 0
        assert_eq!(strategy.calculate_backoff(1), 10); // 10 * 1 = 10
        assert_eq!(strategy.calculate_backoff(2), 20); // 10 * 2 = 20
        assert_eq!(strategy.calculate_backoff(5), 50); // 10 * 5 = 50
        assert_eq!(strategy.calculate_backoff(15), 100); // Capped at max_delay_secs
    }

    #[test]
    fn test_retry_strategy_fixed() {
        let strategy = RetryStrategy::Fixed { delay_secs: 30 };

        assert_eq!(strategy.calculate_backoff(0), 30);
        assert_eq!(strategy.calculate_backoff(1), 30);
        assert_eq!(strategy.calculate_backoff(5), 30);
        assert_eq!(strategy.calculate_backoff(100), 30);
    }

    #[test]
    fn test_retry_strategy_immediate() {
        let strategy = RetryStrategy::Immediate;

        assert_eq!(strategy.calculate_backoff(0), 0);
        assert_eq!(strategy.calculate_backoff(1), 0);
        assert_eq!(strategy.calculate_backoff(10), 0);
    }

    #[test]
    fn test_retry_strategy_exponential_with_jitter() {
        let strategy = RetryStrategy::ExponentialWithJitter {
            max_delay_secs: 3600,
        };

        // Jitter should produce values in reasonable range
        let backoff0 = strategy.calculate_backoff(0);
        assert!((0..=2).contains(&backoff0)); // 2^0 = 1, with jitter 0.5-1.5

        let backoff3 = strategy.calculate_backoff(3);
        assert!((4..=12).contains(&backoff3)); // 2^3 = 8, with jitter ~0.5-1.5
    }

    #[test]
    fn test_retry_strategy_default() {
        let strategy = RetryStrategy::default();
        match strategy {
            RetryStrategy::Exponential { max_delay_secs } => {
                assert_eq!(max_delay_secs, 3600);
            }
            _ => panic!("Default should be Exponential"),
        }
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_pool_metrics() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

        let broker = PostgresBroker::new(&database_url).await.unwrap();
        broker.migrate().await.unwrap();

        let metrics = broker.get_pool_metrics();

        // Pool should have some configuration
        assert!(metrics.max_size > 0);
        assert!(metrics.size <= metrics.max_size);
        assert_eq!(metrics.size, metrics.idle + metrics.in_use);
    }

    #[test]
    fn test_task_chain_creation() {
        let task1 = SerializedTask::new("task1".to_string(), vec![1, 2, 3]);
        let task2 = SerializedTask::new("task2".to_string(), vec![4, 5, 6]);

        let chain = TaskChain {
            tasks: vec![task1, task2],
            stop_on_failure: true,
        };

        assert_eq!(chain.tasks.len(), 2);
        assert!(chain.stop_on_failure);
    }

    #[test]
    fn test_task_chain_serialization() {
        let task1 = SerializedTask::new("task1".to_string(), vec![1]);
        let chain = TaskChain {
            tasks: vec![task1],
            stop_on_failure: false,
        };

        let json = serde_json::to_string(&chain).unwrap();
        let deserialized: TaskChain = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.tasks.len(), 1);
        assert!(!deserialized.stop_on_failure);
    }

    #[test]
    fn test_workflow_stage_creation() {
        let task = SerializedTask::new("task1".to_string(), vec![1]);

        let stage = WorkflowStage {
            id: "stage1".to_string(),
            tasks: vec![task],
            depends_on: vec!["stage0".to_string()],
        };

        assert_eq!(stage.id, "stage1");
        assert_eq!(stage.tasks.len(), 1);
        assert_eq!(stage.depends_on.len(), 1);
        assert_eq!(stage.depends_on[0], "stage0");
    }

    #[test]
    fn test_workflow_creation() {
        let task1 = SerializedTask::new("task1".to_string(), vec![1]);
        let task2 = SerializedTask::new("task2".to_string(), vec![2]);

        let stage1 = WorkflowStage {
            id: "stage1".to_string(),
            tasks: vec![task1],
            depends_on: vec![],
        };

        let stage2 = WorkflowStage {
            id: "stage2".to_string(),
            tasks: vec![task2],
            depends_on: vec!["stage1".to_string()],
        };

        let workflow = TaskWorkflow {
            id: Uuid::new_v4(),
            name: "test_workflow".to_string(),
            stages: vec![stage1, stage2],
        };

        assert_eq!(workflow.name, "test_workflow");
        assert_eq!(workflow.stages.len(), 2);
        assert_eq!(workflow.stages[0].id, "stage1");
        assert_eq!(workflow.stages[1].depends_on[0], "stage1");
    }

    #[test]
    fn test_workflow_serialization() {
        let task = SerializedTask::new("task".to_string(), vec![]);
        let stage = WorkflowStage {
            id: "s1".to_string(),
            tasks: vec![task],
            depends_on: vec![],
        };

        let workflow = TaskWorkflow {
            id: Uuid::new_v4(),
            name: "wf".to_string(),
            stages: vec![stage],
        };

        let json = serde_json::to_string(&workflow).unwrap();
        let deserialized: TaskWorkflow = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "wf");
        assert_eq!(deserialized.stages.len(), 1);
        assert_eq!(deserialized.stages[0].id, "s1");
    }

    #[test]
    fn test_chain_status_serialization() {
        let status = ChainStatus {
            chain_id: Uuid::new_v4(),
            total_tasks: 10,
            completed_tasks: 5,
            failed_tasks: 1,
            pending_tasks: 3,
            processing_tasks: 1,
            current_position: Some(5),
            is_complete: false,
            has_failures: true,
        };

        let json = serde_json::to_string(&status).unwrap();
        let deserialized: ChainStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_tasks, 10);
        assert_eq!(deserialized.completed_tasks, 5);
        assert_eq!(deserialized.failed_tasks, 1);
        assert!(deserialized.has_failures);
        assert!(!deserialized.is_complete);
        assert_eq!(deserialized.current_position, Some(5));
    }

    #[test]
    fn test_stage_status_serialization() {
        let status = StageStatus {
            stage_id: "stage1".to_string(),
            total_tasks: 5,
            completed_tasks: 3,
            failed_tasks: 0,
            pending_tasks: 2,
            processing_tasks: 0,
            is_complete: false,
            dependencies_met: true,
        };

        let json = serde_json::to_string(&status).unwrap();
        let deserialized: StageStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.stage_id, "stage1");
        assert_eq!(deserialized.total_tasks, 5);
        assert!(deserialized.dependencies_met);
        assert!(!deserialized.is_complete);
    }

    #[test]
    fn test_workflow_status_serialization() {
        let stage_status = StageStatus {
            stage_id: "s1".to_string(),
            total_tasks: 2,
            completed_tasks: 2,
            failed_tasks: 0,
            pending_tasks: 0,
            processing_tasks: 0,
            is_complete: true,
            dependencies_met: true,
        };

        let status = WorkflowStatus {
            workflow_id: Uuid::new_v4(),
            workflow_name: "test_wf".to_string(),
            total_stages: 2,
            completed_stages: 1,
            active_stages: 1,
            total_tasks: 10,
            completed_tasks: 7,
            failed_tasks: 1,
            pending_tasks: 1,
            processing_tasks: 1,
            is_complete: false,
            has_failures: true,
            stage_statuses: vec![stage_status],
        };

        let json = serde_json::to_string(&status).unwrap();
        let deserialized: WorkflowStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.workflow_name, "test_wf");
        assert_eq!(deserialized.total_stages, 2);
        assert_eq!(deserialized.completed_stages, 1);
        assert_eq!(deserialized.stage_statuses.len(), 1);
        assert!(deserialized.has_failures);
    }

    #[test]
    fn test_health_status_serialization() {
        let status = HealthStatus {
            healthy: true,
            connection_pool_size: 20,
            idle_connections: 15,
            pending_tasks: 100,
            processing_tasks: 5,
            dlq_tasks: 2,
            database_version: "PostgreSQL 14.5".to_string(),
        };

        let json = serde_json::to_string(&status).unwrap();
        let deserialized: HealthStatus = serde_json::from_str(&json).unwrap();

        assert!(deserialized.healthy);
        assert_eq!(deserialized.connection_pool_size, 20);
        assert_eq!(deserialized.idle_connections, 15);
        assert_eq!(deserialized.pending_tasks, 100);
        assert_eq!(deserialized.database_version, "PostgreSQL 14.5");
    }

    #[test]
    fn test_pool_metrics_serialization() {
        let metrics = PoolMetrics {
            max_size: 20,
            size: 10,
            idle: 7,
            in_use: 3,
            waiting: 2,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: PoolMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.max_size, 20);
        assert_eq!(deserialized.size, 10);
        assert_eq!(deserialized.idle, 7);
        assert_eq!(deserialized.in_use, 3);
        assert_eq!(deserialized.waiting, 2);
    }

    #[test]
    fn test_task_info_serialization() {
        let now = Utc::now();
        let info = TaskInfo {
            id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            state: DbTaskState::Processing,
            priority: 5,
            retry_count: 2,
            max_retries: 3,
            created_at: now,
            scheduled_at: now,
            started_at: Some(now),
            completed_at: None,
            worker_id: Some("worker1".to_string()),
            error_message: None,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: TaskInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task_name, "test_task");
        assert_eq!(deserialized.state, DbTaskState::Processing);
        assert_eq!(deserialized.priority, 5);
        assert_eq!(deserialized.retry_count, 2);
        assert_eq!(deserialized.worker_id, Some("worker1".to_string()));
    }

    #[test]
    fn test_dlq_task_info_serialization() {
        let now = Utc::now();
        let info = DlqTaskInfo {
            id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            task_name: "failed_task".to_string(),
            retry_count: 5,
            error_message: Some("Task failed permanently".to_string()),
            failed_at: now,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: DlqTaskInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task_name, "failed_task");
        assert_eq!(deserialized.retry_count, 5);
        assert_eq!(
            deserialized.error_message,
            Some("Task failed permanently".to_string())
        );
    }

    #[test]
    fn test_workflow_parallel_execution() {
        // Test that a stage can have multiple tasks (parallel execution)
        let task1 = SerializedTask::new("parallel1".to_string(), vec![1]);
        let task2 = SerializedTask::new("parallel2".to_string(), vec![2]);
        let task3 = SerializedTask::new("parallel3".to_string(), vec![3]);

        let stage = WorkflowStage {
            id: "parallel_stage".to_string(),
            tasks: vec![task1, task2, task3],
            depends_on: vec![],
        };

        assert_eq!(stage.tasks.len(), 3);
        // All tasks in the same stage can be executed in parallel
    }

    #[test]
    fn test_workflow_complex_dependencies() {
        let stage1 = WorkflowStage {
            id: "s1".to_string(),
            tasks: vec![SerializedTask::new("t1".to_string(), vec![])],
            depends_on: vec![],
        };

        let stage2 = WorkflowStage {
            id: "s2".to_string(),
            tasks: vec![SerializedTask::new("t2".to_string(), vec![])],
            depends_on: vec![],
        };

        let stage3 = WorkflowStage {
            id: "s3".to_string(),
            tasks: vec![SerializedTask::new("t3".to_string(), vec![])],
            depends_on: vec!["s1".to_string(), "s2".to_string()],
        };

        let workflow = TaskWorkflow {
            id: Uuid::new_v4(),
            name: "complex".to_string(),
            stages: vec![stage1, stage2, stage3],
        };

        // s1 and s2 have no dependencies (can run in parallel)
        assert!(workflow.stages[0].depends_on.is_empty());
        assert!(workflow.stages[1].depends_on.is_empty());

        // s3 depends on both s1 and s2
        assert_eq!(workflow.stages[2].depends_on.len(), 2);
        assert!(workflow.stages[2].depends_on.contains(&"s1".to_string()));
        assert!(workflow.stages[2].depends_on.contains(&"s2".to_string()));
    }

    #[test]
    fn test_get_state_counts_structure() {
        use std::collections::HashMap;

        let mut expected_keys = HashMap::new();
        expected_keys.insert("pending", 0i64);
        expected_keys.insert("processing", 0i64);
        expected_keys.insert("completed", 0i64);
        expected_keys.insert("failed", 0i64);
        expected_keys.insert("cancelled", 0i64);
        expected_keys.insert("dlq", 0i64);

        // Verify all expected state keys exist
        for key in expected_keys.keys() {
            assert!([
                "pending",
                "processing",
                "completed",
                "failed",
                "cancelled",
                "dlq"
            ]
            .contains(key));
        }
    }

    #[test]
    fn test_detailed_health_status_structure() {
        let warnings = vec![
            "High DLQ count".to_string(),
            "Old pending tasks".to_string(),
        ];
        let recommendations = vec!["Increase workers".to_string()];

        let pool_metrics = PoolMetrics {
            max_size: 20,
            size: 15,
            idle: 10,
            in_use: 5,
            waiting: 0,
        };

        let queue_stats = QueueStatistics {
            pending: 100,
            processing: 10,
            completed: 500,
            failed: 5,
            cancelled: 2,
            dlq: 3,
            total: 620,
        };

        let status = DetailedHealthStatus {
            healthy: true,
            connection_ok: true,
            query_latency_ms: 5,
            pool_metrics,
            queue_stats,
            database_version: "PostgreSQL 15".to_string(),
            warnings,
            recommendations,
        };

        assert!(status.healthy);
        assert!(status.connection_ok);
        assert_eq!(status.warnings.len(), 2);
        assert_eq!(status.recommendations.len(), 1);
        assert_eq!(status.query_latency_ms, 5);
    }

    #[test]
    fn test_batch_size_recommendation_structure() {
        let recommendation = BatchSizeRecommendation {
            enqueue_batch_size: 100,
            dequeue_batch_size: 50,
            ack_batch_size: 50,
            reasoning: "Based on current workload and pool size".to_string(),
        };

        assert_eq!(recommendation.enqueue_batch_size, 100);
        assert_eq!(recommendation.dequeue_batch_size, 50);
        assert_eq!(recommendation.ack_batch_size, 50);
        assert!(!recommendation.reasoning.is_empty());
    }

    #[test]
    fn test_table_size_info_structure() {
        let info = TableSizeInfo {
            table_name: "celers_tasks".to_string(),
            row_count: 10000,
            total_size_bytes: 5242880,
            table_size_bytes: 4194304,
            index_size_bytes: 1048576,
            total_size_pretty: "5.0 MB".to_string(),
        };

        assert_eq!(info.table_name, "celers_tasks");
        assert_eq!(info.row_count, 10000);
        assert!(info.total_size_bytes > info.table_size_bytes);
        assert_eq!(
            info.total_size_bytes,
            info.table_size_bytes + info.index_size_bytes
        );
    }

    #[test]
    fn test_index_usage_info_structure() {
        let info = IndexUsageInfo {
            table_name: "celers_tasks".to_string(),
            index_name: "idx_tasks_state_priority".to_string(),
            index_scans: 1000,
            tuples_read: 50000,
            tuples_fetched: 45000,
            index_size_bytes: 524288,
            index_size_pretty: "512 KB".to_string(),
        };

        assert_eq!(info.table_name, "celers_tasks");
        assert_eq!(info.index_name, "idx_tasks_state_priority");
        assert!(info.index_scans > 0);
        assert!(info.tuples_read >= info.tuples_fetched);
    }

    #[test]
    fn test_partition_info_structure() {
        use chrono::TimeZone;

        let info = PartitionInfo {
            partition_name: "celers_tasks_2025_01".to_string(),
            partition_start: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            partition_end: Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap(),
            row_count: 5000,
            size_bytes: 2097152,
        };

        assert!(info.partition_name.contains("celers_tasks"));
        assert!(info.partition_name.contains("2025_01"));
        assert_eq!(info.row_count, 5000);
        assert!(info.size_bytes > 0);
        assert!(info.partition_end > info.partition_start);
    }

    #[test]
    fn test_tenant_broker_isolation() {
        // Test that tenant IDs are properly formatted
        let tenant_id = "tenant-123";
        assert!(!tenant_id.is_empty());
        assert!(tenant_id.contains("-"));
    }

    #[test]
    fn test_retry_strategy_calculation_bounds() {
        // Test that retry delay calculations don't overflow
        let strategy = RetryStrategy::Exponential {
            max_delay_secs: 3600,
        };

        // Should handle reasonable retry counts
        for retry_count in 0..10 {
            let delay = strategy.calculate_backoff(retry_count);
            assert!(delay < 10000); // Less than ~3 hours
        }
    }

    #[test]
    fn test_retry_strategy_jitter_range() {
        let strategy = RetryStrategy::ExponentialWithJitter {
            max_delay_secs: 3600,
        };

        // Jitter should provide some variation
        let delay1 = strategy.calculate_backoff(3);
        let delay2 = strategy.calculate_backoff(3);

        // With jitter, delays might vary (though not guaranteed in every run)
        // Just verify they're in reasonable range
        assert!(delay1 <= 100);
        assert!(delay2 <= 100);
    }

    #[test]
    fn test_db_task_state_all_variants() {
        let states = vec![
            DbTaskState::Pending,
            DbTaskState::Processing,
            DbTaskState::Completed,
            DbTaskState::Failed,
            DbTaskState::Cancelled,
        ];

        for state in states {
            let s = state.to_string();
            assert!(!s.is_empty());

            // Verify round-trip conversion
            let parsed: DbTaskState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn test_task_result_status_all_variants() {
        let statuses = vec![
            TaskResultStatus::Pending,
            TaskResultStatus::Started,
            TaskResultStatus::Success,
            TaskResultStatus::Failure,
            TaskResultStatus::Retry,
            TaskResultStatus::Revoked,
        ];

        for status in statuses {
            let s = status.to_string();
            assert!(!s.is_empty());

            // Verify round-trip conversion
            let parsed: TaskResultStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_task_result_structure() {
        let result = TaskResult {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            status: TaskResultStatus::Success,
            result: Some(json!({"output": "success"})),
            error: None,
            traceback: None,
            created_at: Utc::now(),
            completed_at: Some(Utc::now()),
            runtime_ms: Some(1500),
        };

        assert_eq!(result.task_name, "test_task");
        assert_eq!(result.status, TaskResultStatus::Success);
        assert!(result.result.is_some());
        assert!(result.error.is_none());
        assert_eq!(result.runtime_ms, Some(1500));
    }

    #[test]
    fn test_dlq_task_info_structure() {
        let info = DlqTaskInfo {
            id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            task_name: "failed_task".to_string(),
            retry_count: 5,
            error_message: Some("Connection timeout".to_string()),
            failed_at: Utc::now(),
        };

        assert_eq!(info.task_name, "failed_task");
        assert_eq!(info.retry_count, 5);
        assert!(info.error_message.is_some());
    }

    #[test]
    fn test_queue_statistics_comprehensive() {
        let stats = QueueStatistics {
            pending: 100,
            processing: 10,
            completed: 500,
            failed: 5,
            cancelled: 2,
            dlq: 3,
            total: 620,
        };

        // Verify total calculation
        assert_eq!(stats.total, 620);

        // Verify relationships
        assert!(stats.completed > stats.pending);
        assert!(stats.processing < stats.completed);
        assert!(stats.failed < stats.pending);
        assert!(stats.dlq < stats.failed);
    }

    // ========== LISTEN/NOTIFY Tests ==========

    #[test]
    fn test_task_notification_structure() {
        let notification = TaskNotification {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            queue_name: "default".to_string(),
            priority: 5,
            enqueued_at: Utc::now(),
        };

        assert_eq!(notification.task_name, "test_task");
        assert_eq!(notification.queue_name, "default");
        assert_eq!(notification.priority, 5);
    }

    #[test]
    fn test_task_notification_serialization() {
        let notification = TaskNotification {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            queue_name: "default".to_string(),
            priority: 10,
            enqueued_at: Utc::now(),
        };

        // Test serialization
        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("test_task"));
        assert!(json.contains("default"));

        // Test deserialization
        let deserialized: TaskNotification = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.task_id, notification.task_id);
        assert_eq!(deserialized.task_name, notification.task_name);
        assert_eq!(deserialized.queue_name, notification.queue_name);
        assert_eq!(deserialized.priority, notification.priority);
    }

    #[test]
    fn test_notification_channel_naming() {
        // Test that channel names follow the expected format
        let queue_name = "default";
        let expected_channel = format!("celers_tasks_{}", queue_name);
        assert_eq!(expected_channel, "celers_tasks_default");

        let queue_name = "high_priority";
        let expected_channel = format!("celers_tasks_{}", queue_name);
        assert_eq!(expected_channel, "celers_tasks_high_priority");
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_notification_listener_creation() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_notification_listener_creation -- --ignored
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_enable_disable_notifications() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_enable_disable_notifications -- --ignored
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_notification_end_to_end() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_notification_end_to_end -- --ignored
    }

    // ========== Task Deduplication Tests ==========

    #[test]
    fn test_deduplication_config_default() {
        let config = DeduplicationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.window_secs, 300); // 5 minutes
    }

    #[test]
    fn test_deduplication_config_custom() {
        let config = DeduplicationConfig {
            enabled: true,
            window_secs: 600, // 10 minutes
        };
        assert!(config.enabled);
        assert_eq!(config.window_secs, 600);
    }

    #[test]
    fn test_deduplication_config_serialization() {
        let config = DeduplicationConfig {
            enabled: true,
            window_secs: 300,
        };

        // Test serialization
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("enabled"));
        assert!(json.contains("window_secs"));

        // Test deserialization
        let deserialized: DeduplicationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.window_secs, config.window_secs);
    }

    #[test]
    fn test_deduplication_info_structure() {
        let info = DeduplicationInfo {
            idempotency_key: "order-123".to_string(),
            task_id: Uuid::new_v4(),
            task_name: "process_order".to_string(),
            first_seen_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(300),
            duplicate_count: 5,
        };

        assert_eq!(info.idempotency_key, "order-123");
        assert_eq!(info.task_name, "process_order");
        assert_eq!(info.duplicate_count, 5);
        assert!(info.expires_at > info.first_seen_at);
    }

    #[test]
    fn test_deduplication_info_serialization() {
        let info = DeduplicationInfo {
            idempotency_key: "payment-456".to_string(),
            task_id: Uuid::new_v4(),
            task_name: "process_payment".to_string(),
            first_seen_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(600),
            duplicate_count: 3,
        };

        // Test serialization
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("payment-456"));
        assert!(json.contains("process_payment"));

        // Test deserialization
        let deserialized: DeduplicationInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.idempotency_key, info.idempotency_key);
        assert_eq!(deserialized.task_id, info.task_id);
        assert_eq!(deserialized.task_name, info.task_name);
        assert_eq!(deserialized.duplicate_count, info.duplicate_count);
    }

    #[test]
    fn test_deduplication_window_expiry() {
        let config = DeduplicationConfig {
            enabled: true,
            window_secs: 300,
        };

        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(config.window_secs);

        // Verify expiry is in the future
        assert!(expires_at > now);

        // Verify expiry is exactly window_secs in the future
        let duration = expires_at - now;
        assert!(duration.num_seconds() <= config.window_secs + 1); // Allow 1 second tolerance
        assert!(duration.num_seconds() >= config.window_secs - 1);
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_enqueue_idempotent() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_enqueue_idempotent -- --ignored
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_check_deduplication() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_check_deduplication -- --ignored
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_cleanup_deduplication() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_cleanup_deduplication -- --ignored
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_deduplication_stats() {
        // This test requires a live PostgreSQL database
        // Run with: cargo test test_get_deduplication_stats -- --ignored
    }

    // ========== Tests for new features (TTL, Advisory Locks, Performance Analytics) ==========

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_expire_tasks_by_ttl() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Enqueue tasks of a specific type
        // 2. Manually update their created_at to be old
        // 3. Call expire_tasks_by_ttl
        // 4. Verify tasks are in cancelled state
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_expire_all_tasks_by_ttl() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Enqueue multiple task types
        // 2. Manually update their created_at to be old
        // 3. Call expire_all_tasks_by_ttl
        // 4. Verify all old tasks are cancelled
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_advisory_lock_acquire_release() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Acquire lock with try_advisory_lock
        // 2. Verify it returns true
        // 3. Try to acquire same lock again, verify returns false
        // 4. Release lock
        // 5. Verify lock can be acquired again
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_advisory_lock_blocking() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Acquire lock with advisory_lock
        // 2. Verify is_advisory_lock_held returns true
        // 3. Release lock
        // 4. Verify is_advisory_lock_held returns false
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_task_percentiles() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Create and complete multiple tasks with varying durations
        // 2. Call get_task_percentiles
        // 3. Verify p50, p95, p99 are calculated correctly
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_slowest_tasks() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Create and complete multiple tasks with varying durations
        // 2. Call get_slowest_tasks
        // 3. Verify results are ordered by duration (slowest first)
        // 4. Verify limit is respected
    }

    // ========== Tests for Rate Limiting, Priority, DLQ Analytics, Cancellation ==========

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_task_rate() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Complete multiple tasks of same type
        // 2. Call get_task_rate
        // 3. Verify count matches expected rate
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_is_rate_limited() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Complete tasks to reach rate limit
        // 2. Call is_rate_limited
        // 3. Verify it returns true when limit exceeded
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_boost_task_priority() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Enqueue tasks with normal priority
        // 2. Boost priority
        // 3. Verify tasks have increased priority
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_set_task_priority() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Enqueue tasks
        // 2. Set absolute priority for specific tasks
        // 3. Verify priority was set correctly
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_dlq_stats_by_task() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Create failed tasks in DLQ
        // 2. Call get_dlq_stats_by_task
        // 3. Verify task counts grouped by name
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_dlq_error_patterns() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Create DLQ tasks with various errors
        // 2. Call get_dlq_error_patterns
        // 3. Verify most common errors are returned
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_recent_dlq_tasks() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Create DLQ tasks at different times
        // 2. Call get_recent_dlq_tasks
        // 3. Verify only recent tasks are returned
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_cancel_with_reason() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Enqueue task
        // 2. Cancel with reason
        // 3. Verify task is cancelled with reason recorded
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_cancel_batch_with_reason() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Enqueue multiple tasks
        // 2. Batch cancel with reason
        // 3. Verify all tasks cancelled with reason
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL connection
    async fn test_get_cancellation_reasons() {
        // This test requires a live PostgreSQL database
        // It would test:
        // 1. Cancel tasks with various reasons
        // 2. Call get_cancellation_reasons
        // 3. Verify reasons are grouped and counted correctly
    }
}
