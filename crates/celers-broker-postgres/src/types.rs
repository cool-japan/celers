//! Type definitions for the PostgreSQL broker

use celers_core::{CelersError, Result, SerializedTask};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
    Exponential { max_delay_secs: i64 },
    /// Exponential backoff with random jitter to avoid thundering herd
    ExponentialWithJitter { max_delay_secs: i64 },
    /// Linear backoff: base_delay * retry_count seconds
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

/// Task lifecycle hook types for extensibility
///
/// Lifecycle hooks allow you to inject custom logic at key points in task processing:
/// - Validation: Ensure tasks meet criteria before enqueuing
/// - Enrichment: Add metadata or transform tasks
/// - Logging: Track task lifecycle events
/// - Metrics: Custom instrumentation
/// - Integration: Trigger external systems
///
/// # Example
/// ```no_run
/// use celers_broker_postgres::{PostgresBroker, TaskHook, HookContext};
/// use celers_core::{Result, SerializedTask};
/// use std::sync::Arc;
///
/// fn validate_hook() -> Arc<dyn Fn(&HookContext, &SerializedTask) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync> {
///     Arc::new(|_ctx: &HookContext, task: &SerializedTask| {
///         let payload_len = task.payload.len();
///         Box::pin(async move {
///             // Validate task payload size
///             if payload_len > 1_000_000 {
///                 return Err(celers_core::CelersError::Other(
///                     "Task payload too large".to_string()
///                 ));
///             }
///             Ok(())
///         })
///     })
/// }
///
/// # async fn example() -> Result<()> {
/// let broker = PostgresBroker::new("postgres://localhost/db").await?;
/// broker.add_hook(TaskHook::BeforeEnqueue(validate_hook())).await;
/// # Ok(())
/// # }
/// ```
pub type HookFn = Arc<
    dyn Fn(
            &HookContext,
            &SerializedTask,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Context passed to lifecycle hooks
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Queue name
    pub queue_name: String,
    /// Task ID (if available)
    pub task_id: Option<Uuid>,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
    /// Additional metadata
    pub metadata: serde_json::Value,
}

/// Task lifecycle hook enum
#[derive(Clone)]
pub enum TaskHook {
    /// Called before a task is enqueued
    BeforeEnqueue(HookFn),
    /// Called after a task is successfully enqueued
    AfterEnqueue(HookFn),
    /// Called before a task is dequeued
    BeforeDequeue(HookFn),
    /// Called after a task is dequeued
    AfterDequeue(HookFn),
    /// Called before a task is acknowledged
    BeforeAck(HookFn),
    /// Called after a task is acknowledged
    AfterAck(HookFn),
    /// Called before a task is rejected
    BeforeReject(HookFn),
    /// Called after a task is rejected
    AfterReject(HookFn),
}

/// Container for all registered hooks
#[derive(Clone, Default)]
pub struct TaskHooks {
    pub(crate) before_enqueue: Vec<HookFn>,
    pub(crate) after_enqueue: Vec<HookFn>,
    pub(crate) before_dequeue: Vec<HookFn>,
    pub(crate) after_dequeue: Vec<HookFn>,
    pub(crate) before_ack: Vec<HookFn>,
    pub(crate) after_ack: Vec<HookFn>,
    pub(crate) before_reject: Vec<HookFn>,
    pub(crate) after_reject: Vec<HookFn>,
}

impl TaskHooks {
    /// Create empty hooks container
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a hook
    pub fn add(&mut self, hook: TaskHook) {
        match hook {
            TaskHook::BeforeEnqueue(f) => self.before_enqueue.push(f),
            TaskHook::AfterEnqueue(f) => self.after_enqueue.push(f),
            TaskHook::BeforeDequeue(f) => self.before_dequeue.push(f),
            TaskHook::AfterDequeue(f) => self.after_dequeue.push(f),
            TaskHook::BeforeAck(f) => self.before_ack.push(f),
            TaskHook::AfterAck(f) => self.after_ack.push(f),
            TaskHook::BeforeReject(f) => self.before_reject.push(f),
            TaskHook::AfterReject(f) => self.after_reject.push(f),
        }
    }

    /// Execute before_enqueue hooks
    pub(crate) async fn run_before_enqueue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_enqueue {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_enqueue hooks
    pub(crate) async fn run_after_enqueue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_enqueue {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute before_dequeue hooks
    #[allow(dead_code)]
    pub(crate) async fn run_before_dequeue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_dequeue {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_dequeue hooks
    pub(crate) async fn run_after_dequeue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_dequeue {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute before_ack hooks
    pub(crate) async fn run_before_ack(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_ack {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_ack hooks
    pub(crate) async fn run_after_ack(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_ack {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute before_reject hooks
    pub(crate) async fn run_before_reject(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_reject {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_reject hooks
    pub(crate) async fn run_after_reject(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_reject {
            hook(ctx, task).await?;
        }
        Ok(())
    }
}

/// Distributed tracing context for end-to-end observability
///
/// Compatible with OpenTelemetry W3C Trace Context specification.
/// Allows correlation of task execution across distributed workers and services.
///
/// # Example
/// ```no_run
/// use celers_broker_postgres::{PostgresBroker, TraceContext};
/// use celers_core::{Broker, SerializedTask};
///
/// # async fn example() -> celers_core::Result<()> {
/// let broker = PostgresBroker::new("postgres://localhost/db").await?;
///
/// // Create task with trace context
/// let task = SerializedTask::new("my_task".to_string(), vec![]);
/// let trace_ctx = TraceContext::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
///
/// // Enqueue task with trace context
/// broker.enqueue_with_trace_context(task, trace_ctx).await?;
///
/// // Extract trace context when processing
/// if let Some(msg) = broker.dequeue().await? {
///     if let Some(ctx) = broker.extract_trace_context(&msg.task.metadata.id).await? {
///         println!("Processing task with trace_id: {}", ctx.trace_id);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C Trace ID (32 hex characters, 16 bytes)
    pub trace_id: String,
    /// W3C Span ID (16 hex characters, 8 bytes)
    pub span_id: String,
    /// Trace flags (8-bit field, typically "01" for sampled)
    #[serde(default = "default_trace_flags")]
    pub trace_flags: String,
    /// Optional trace state for vendor-specific data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_state: Option<String>,
}

pub(crate) fn default_trace_flags() -> String {
    "01".to_string()
}

impl TraceContext {
    /// Create a new trace context with trace_id and span_id
    ///
    /// # Arguments
    /// * `trace_id` - 32 hex character trace ID (W3C format)
    /// * `span_id` - 16 hex character span ID (W3C format)
    ///
    /// # Example
    /// ```
    /// use celers_broker_postgres::TraceContext;
    ///
    /// let ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    /// assert_eq!(ctx.trace_flags, "01"); // Sampled by default
    /// ```
    pub fn new(trace_id: impl Into<String>, span_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            trace_flags: default_trace_flags(),
            trace_state: None,
        }
    }

    /// Create trace context from W3C traceparent header value
    ///
    /// Format: "00-{trace_id}-{span_id}-{flags}"
    ///
    /// # Example
    /// ```
    /// use celers_broker_postgres::TraceContext;
    ///
    /// let ctx = TraceContext::from_traceparent(
    ///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    /// ).unwrap();
    /// assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    /// assert_eq!(ctx.span_id, "00f067aa0ba902b7");
    /// ```
    pub fn from_traceparent(traceparent: &str) -> Result<Self> {
        let parts: Vec<&str> = traceparent.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return Err(CelersError::Other(format!(
                "Invalid traceparent format: {}",
                traceparent
            )));
        }

        Ok(Self {
            trace_id: parts[1].to_string(),
            span_id: parts[2].to_string(),
            trace_flags: parts[3].to_string(),
            trace_state: None,
        })
    }

    /// Convert to W3C traceparent header value
    ///
    /// # Example
    /// ```
    /// use celers_broker_postgres::TraceContext;
    ///
    /// let ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    /// assert_eq!(
    ///     ctx.to_traceparent(),
    ///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    /// );
    /// ```
    pub fn to_traceparent(&self) -> String {
        format!("00-{}-{}-{}", self.trace_id, self.span_id, self.trace_flags)
    }

    /// Check if trace is sampled (should be recorded)
    pub fn is_sampled(&self) -> bool {
        self.trace_flags == "01"
    }

    /// Generate a new child span ID for this trace
    ///
    /// # Example
    /// ```
    /// use celers_broker_postgres::TraceContext;
    ///
    /// let parent_ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    /// let child_ctx = parent_ctx.create_child_span();
    ///
    /// // Same trace ID, different span ID
    /// assert_eq!(child_ctx.trace_id, parent_ctx.trace_id);
    /// assert_ne!(child_ctx.span_id, parent_ctx.span_id);
    /// ```
    pub fn create_child_span(&self) -> Self {
        let span_id = format!(
            "{:016x}",
            uuid::Uuid::new_v4().as_u128() & 0xFFFFFFFFFFFFFFFF
        );
        Self {
            trace_id: self.trace_id.clone(),
            span_id,
            trace_flags: self.trace_flags.clone(),
            trace_state: self.trace_state.clone(),
        }
    }
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

/// Deduplication configuration for preventing duplicate task execution
///
/// Task deduplication ensures that tasks with the same idempotency key are not
/// executed multiple times within a specified time window.
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

/// Queue health thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealthThresholds {
    pub max_pending_tasks: i64,
    pub max_processing_tasks: i64,
    pub max_dlq_tasks: i64,
    pub max_oldest_pending_age_secs: i64,
    pub min_success_rate: f64,
}

impl Default for QueueHealthThresholds {
    fn default() -> Self {
        Self {
            max_pending_tasks: 1000,
            max_processing_tasks: 100,
            max_dlq_tasks: 50,
            max_oldest_pending_age_secs: 3600, // 1 hour
            min_success_rate: 0.95,            // 95%
        }
    }
}

/// Queue health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealthCheck {
    pub is_healthy: bool,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
    pub stats: QueueStatistics,
    pub oldest_pending_age_secs: Option<i64>,
    pub success_rate: f64,
    pub checked_at: DateTime<Utc>,
}

/// Task group status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskGroupStatus {
    pub group_id: String,
    pub group_name: String,
    pub total_tasks: i64,
    pub pending_count: i64,
    pub processing_count: i64,
    pub completed_count: i64,
    pub failed_count: i64,
    pub cancelled_count: i64,
    pub completion_percentage: f64,
    pub first_task_created: Option<DateTime<Utc>>,
    pub last_task_completed: Option<DateTime<Utc>>,
}

/// Task search filter
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskSearchFilter {
    /// Filter by task states
    pub states: Option<Vec<String>>,
    /// Minimum priority
    pub priority_min: Option<i32>,
    /// Maximum priority
    pub priority_max: Option<i32>,
    /// Tasks created after this many seconds ago
    pub created_after_secs: Option<i64>,
    /// SQL LIKE pattern for task name
    pub task_name_pattern: Option<String>,
    /// Metadata key-value filters (AND logic)
    pub metadata_filters: Vec<(String, String)>,
    /// Maximum number of results
    pub limit: i64,
}

/// Task lifecycle information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLifecycle {
    pub task_id: Uuid,
    pub task_name: String,
    pub current_state: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub retry_count: i32,
    pub total_lifetime_secs: i64,
    pub time_pending_secs: Option<i64>,
    pub time_processing_secs: Option<i64>,
    pub error_message: Option<String>,
}

/// State transition statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransitionStats {
    pub avg_time_pending_secs: f64,
    pub avg_time_processing_secs: f64,
    pub success_rate: f64,
    pub pending_to_processing_count: i64,
    pub processing_to_completed_count: i64,
    pub completed_count: i64,
    pub failed_count: i64,
    pub cancelled_count: i64,
}

/// Priority adjustment strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityStrategy {
    /// Tasks older than this (in seconds) get age-based priority boost
    pub age_threshold_secs: i64,
    /// Priority increment for age-based boost
    pub age_priority_boost: i32,
    /// Tasks with this many retries or more get retry-based priority boost
    pub retry_threshold: i32,
    /// Priority increment for retry-based boost
    pub retry_priority_boost: i32,
    /// Task type-specific priority boosts (task_name, priority_increment)
    pub task_type_boosts: Vec<(String, i32)>,
}

/// Result of applying a priority strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityStrategyResult {
    /// Number of tasks adjusted by age criteria
    pub age_adjusted_count: i64,
    /// Number of tasks adjusted by retry criteria
    pub retry_adjusted_count: i64,
    /// Number of tasks adjusted by task type criteria
    pub type_adjusted_count: i64,
    /// Total number of unique tasks adjusted
    pub total_adjusted: i64,
}

/// Queue depth forecast
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueForecast {
    pub current_depth: i64,
    pub predicted_depth: i64,
    pub forecast_hours: i64,
    pub avg_arrival_rate: f64,
    pub avg_completion_rate: f64,
    pub net_rate: f64,
    pub trend: String,
    pub confidence: f64,
}

/// Queue trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueTrendAnalysis {
    pub hours_analyzed: i64,
    pub peak_pending: i64,
    pub avg_pending: f64,
    pub peak_hour: i32,
    pub total_tasks: i64,
    pub task_velocity: f64,
    pub success_rate: f64,
    pub total_completed: i64,
    pub total_failed: i64,
}

/// Task completion time estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompletionEstimate {
    pub task_id: Uuid,
    pub estimated_wait_secs: i64,
    pub tasks_ahead: i64,
    pub avg_task_duration_secs: i64,
    pub confidence: f64,
}

/// Queue capacity analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueCapacityAnalysis {
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub estimated_worker_count: i64,
    pub throughput_per_hour: i64,
    pub utilization_percent: f64,
    pub avg_task_duration_secs: i64,
    pub recommendation: String,
}

/// Periodic task schedule (cron-like)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodicTaskSchedule {
    pub schedule_id: String,
    pub task_name: String,
    pub cron_expression: String,
    pub payload: serde_json::Value,
    pub priority: i32,
    pub enabled: bool,
    pub last_run: Option<DateTime<Utc>>,
    pub next_run: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Queue snapshot for backup/restore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub snapshot_id: String,
    pub queue_name: String,
    pub created_at: DateTime<Utc>,
    pub task_count: i64,
    pub total_size_bytes: i64,
    pub includes_results: bool,
}

/// Task retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRetentionPolicy {
    pub policy_name: String,
    pub task_state: String,
    pub retention_days: i32,
    pub archive_before_delete: bool,
    pub enabled: bool,
}

impl Default for TaskRetentionPolicy {
    fn default() -> Self {
        Self {
            policy_name: "default".to_string(),
            task_state: "completed".to_string(),
            retention_days: 30,
            archive_before_delete: true,
            enabled: true,
        }
    }
}

/// Connection pool health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolHealth {
    pub current_size: u32,
    pub idle_count: u32,
    pub active_count: u32,
    pub max_size: u32,
    pub utilization_percent: f64,
    pub avg_wait_time_ms: f64,
    pub recommendation: String,
    pub should_scale: bool,
    pub recommended_size: u32,
}
