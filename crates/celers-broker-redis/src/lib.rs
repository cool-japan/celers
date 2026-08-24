//! Redis broker implementation for CeleRS
//!
//! Provides Kombu-compatible Redis broker with:
//! - Visibility timeout using Lua scripts
//! - Priority queue support
//! - Dead Letter Queue (DLQ)
//! - Task cancellation via Pub/Sub
//! - Health checks and monitoring
//! - Queue pause/resume functionality
//! - Task deduplication
//! - Circuit breaker for resilience
//! - Rate limiting (local and distributed)
//! - Automatic retry with configurable backoff
//! - Geo-distribution with multi-region replication

use async_trait::async_trait;
use celers_core::{Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    AsyncCommands, Client,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};

#[cfg(feature = "metrics")]
use celers_metrics::{
    DLQ_SIZE, PROCESSING_QUEUE_SIZE, QUEUE_SIZE, TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL,
};

pub mod advanced_queue;
pub mod authorization;
pub mod backup_restore;
pub mod batch_ext;
#[cfg(test)]
mod broker_tests;
pub mod bulkhead;
pub mod circuit_breaker;
pub mod cluster;
pub mod compression;
pub mod connection;
pub mod cron_scheduler;
pub mod dedup;
pub mod degradation;
pub mod dlq_analytics;
pub mod dlq_archival;
pub mod dlq_replay;
pub mod encryption;
pub mod geo;
pub mod health;
pub mod hooks;
pub mod hooks_advanced;
pub mod integrity;
pub mod locks;
pub mod lua_scripts;
pub mod metrics_ext;
pub mod monitoring;
pub mod otel_integration;
pub mod partitioning;
pub mod pipeline_advanced;
pub mod pool;
pub mod pool_advanced;
pub mod priority_mgmt;
pub mod queue_control;
pub mod quota_mgmt;
pub mod rate_limit;
pub mod result_backend;
pub mod retry;
pub mod sentinel;
pub mod streams;
pub mod structured_logging;
pub mod task_groups;
pub mod task_query;
pub mod telemetry;
pub mod utilities;
pub mod visibility;

pub use advanced_queue::{
    AdvancedQueueManager, PriorityAgingConfig, PriorityAgingConfigBuilder, QueueWeight,
    StarvationPrevention, StarvationStats, TaskAge, WeightedQueueSelector,
};
pub use authorization::{AuthorizationPolicy, UserPermissions};
pub use backup_restore::{BackupManager, QueueSnapshot, SnapshotComparison};
pub use batch_ext::{BatchOperations, TaskFilter};
pub use bulkhead::{Bulkhead, BulkheadConfig, BulkheadManager, BulkheadPermit, BulkheadStats};
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats, CircuitState,
};
pub use cluster::{
    ClusterConfig, ClusterConfigBuilder, ClusterNode, ClusterNodeRole, ClusterTopology, HashSlot,
    RedisMode,
};
#[allow(deprecated)]
pub use compression::{CompressionAlgorithm, CompressionConfig, CompressionStats, Compressor};
pub use connection::{
    blocking_response_timeout, ConnectionStats, RedisClientExt, RedisConfig, TlsConfig,
    BLOCKING_RESPONSE_MARGIN, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_RESPONSE_TIMEOUT,
};
pub use cron_scheduler::{CronExpression, CronScheduler, ScheduledTask};
pub use dedup::{DedupResult, DedupStrategy, Deduplicator};
pub use degradation::{DegradationManager, DegradationMode, DegradationStats, QueuedOperation};
pub use dlq_analytics::{
    DLQAnalyzer, ErrorCategory, ErrorCluster, FailurePattern, FailureTrend, RootCause,
    TemporalAnalysis,
};
pub use dlq_archival::{
    ArchivalConfig, ArchiveSearchCriteria, ArchiveStats, ArchivedTask, DLQArchivalManager,
    RetentionPolicy, StorageBackend,
};
pub use dlq_replay::{
    ReplayCondition, ReplayPolicy, ReplayPolicyType, ReplayResult, ReplayScheduler, ReplayStats,
};
pub use encryption::{
    EncryptedData, EncryptionAlgorithm, EncryptionConfig, EncryptionManager, EncryptionStats,
};
pub use geo::{
    ConflictResolution, GeoReplicationManager, Region, RegionId, RegionStats, RegionStatsSnapshot,
    RegionalReadRouter, ReplicationConfig, ReplicationConfigBuilder, RoutingStrategy, SyncMode,
};
pub use health::{HealthChecker, KeyspaceStats, QueueStats, RedisHealthStatus, ReplicationInfo};
pub use hooks::{
    CompletionHook, CompletionStatus, DequeueHook, EnqueueHook, HookContext, HookResult, LogLevel,
    LoggingHook, MetricsHook, PayloadSizeValidator, TaskHookRegistry, TimestampEnrichmentHook,
};
pub use hooks_advanced::{
    ConditionalHook, HookCondition, HookErrorStrategy, ParallelHookExecutor, PrioritizedHook,
    RetryableHook, SequentialHooks,
};
pub use integrity::{ChecksumAlgorithm, IntegrityStats, IntegrityValidator, IntegrityWrappedTask};
pub use locks::{DistributedLock, LockConfig, LockGuard, LockToken};
pub use lua_scripts::{ScriptId, ScriptManager, ScriptPerformance, ScriptStats, SCRIPT_VERSION};
pub use metrics_ext::{
    HistogramSnapshot, LatencyStats, MetricsSnapshot, MetricsTracker, SlowOperation,
    SlowOperationLogger, TaskAgeHistogram,
};
pub use monitoring::{
    analyze_performance_trend, analyze_redis_broker_performance, analyze_redis_consumer_lag,
    analyze_redis_dlq_health, analyze_redis_fragmentation, analyze_redis_memory_efficiency,
    analyze_redis_slowlog, analyze_task_completion_patterns,
    calculate_redis_message_age_distribution, calculate_redis_message_velocity,
    calculate_redis_queue_health_score, detect_performance_regression, detect_queue_burst,
    detect_redis_queue_anomaly, detect_redis_queue_saturation, estimate_redis_monthly_cost,
    estimate_redis_processing_capacity, generate_queue_health_report, predict_redis_queue_size,
    recommend_alert_thresholds, recommend_redis_scaling_strategy, suggest_redis_worker_scaling,
    AlertThresholds, AnomalyDetection, BurstDetection, ConsumerLagAnalysis, DLQAnalysis,
    FragmentationAnalysis, MemoryEfficiencyAnalysis, MessageAgeDistribution, MessageVelocity,
    PerformanceTrend, ProcessingCapacity, QueueHealthReport, QueueSaturationAnalysis,
    QueueSizePrediction, QueueTrend, RedisCostEstimate, RegressionDetection, SaturationLevel,
    ScalingRecommendation, ScalingStrategy, SlowlogAnalysis, TaskCompletionAnalysis,
    WorkerScalingSuggestion,
};
pub use otel_integration::{
    OtelBrokerInstrumentation, OtelConfig, SpanInfo, TracingBackend, W3CTraceContext,
};
pub use partitioning::{
    ConsistentHashRing, HashAlgorithm, PartitionManager, PartitionStats, PartitionStrategy,
};
pub use pipeline_advanced::{
    AdvancedPipeline, PipelineConfig, PipelineExecutionResult, PipelineOperation, PipelineStats,
};
pub use pool::{ConnectionPool, PoolConfig, PoolStats};
pub use pool_advanced::{AdaptiveConnectionPool, AdaptivePoolConfig, AdaptivePoolStats};
pub use priority_mgmt::{PriorityAdjustment, PriorityManager};
pub use queue_control::{QueueController, QueueState};
pub use quota_mgmt::{QuotaConfig, QuotaManager, QuotaPeriod, QuotaUsage};
pub use rate_limit::{
    DistributedRateLimiter, QueueRateLimitConfig, QueueRateLimiter, TokenBucketLimiter,
    TrackedRateLimiter,
};
pub use result_backend::{ResultBackend, ResultBackendConfig, TaskResult, TaskStatus};
pub use retry::{BackoffStrategy, RetryConfig, RetryExecutor, RetryResult};
pub use sentinel::{
    MasterAddress, SentinelClient, SentinelConfig, SentinelConfigBuilder, SentinelRole,
};
pub use streams::{
    StreamConfig, StreamConfigBuilder, StreamEntry, StreamMessageId, StreamStats, StreamsClient,
};
pub use structured_logging::{
    CorrelationAnalysis, LogConfig, LogContext, LogEntry, PerformanceAnalysis, StructuredLogLevel,
    StructuredLogger,
};
pub use task_groups::{GroupConfig, GroupMetadata, GroupStatus, TaskGroup};
pub use task_query::{TaskQuery, TaskSearchCriteria, TaskStats};
pub use telemetry::{SpanBuilder, SpanEvent, TracingContext};
pub use utilities::{
    analyze_redis_command_performance, analyze_redis_queue_balance,
    calculate_optimal_redis_batch_size, calculate_optimal_redis_pool_size,
    calculate_redis_capacity_headroom, calculate_redis_key_ttl_by_priority,
    calculate_redis_migration_batch_size, calculate_redis_optimal_shard_count,
    calculate_redis_pipeline_size, calculate_redis_queue_efficiency,
    calculate_redis_sla_compliance, calculate_redis_timeout_values, calculate_worker_distribution,
    estimate_redis_migration_time, estimate_redis_queue_drain_time, estimate_redis_queue_memory,
    estimate_redis_scaling_time, optimize_task_priority, recommend_queue_rebalancing,
    suggest_redis_data_retention, suggest_redis_persistence_strategy,
    suggest_redis_pipeline_strategy, CapacityHeadroom, MigrationStrategy, PriorityOptimization,
    QueueEfficiency, RebalancingRecommendation, SLACompliance, ScalingTimeEstimate,
    WorkerDistribution,
};
pub use visibility::{NackAction, PopOutcome, QueueKeys, VisibilityManager, DEFAULT_SWEEP_BATCH};

/// How many revoked messages a single dequeue will discard before giving up
/// and reporting the queue as empty.
const REVOKED_SKIP_BUDGET: usize = 32;

/// How many pending entries [`RedisBroker::cancel`] scans per structure when
/// looking for copies of the revoked task.
const REVOKE_SCAN_LIMIT: usize = 10_000;

/// How long a revocation is remembered, in seconds (24 hours).
const DEFAULT_REVOCATION_TTL_SECS: u64 = 86_400;

/// How many dead-letter entries are replayed per `EVAL`.
const REPLAY_CHUNK: usize = 500;

/// Default seconds between two background sweeps (delayed-task promotion and
/// visibility-timeout recovery) triggered from `dequeue`.
const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 1;

/// Current Unix time in seconds, saturating to 0 if the clock predates the
/// epoch rather than panicking.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// Queue mode for Redis broker
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueueMode {
    /// Standard FIFO queue using Redis lists
    Fifo,
    /// Priority queue using Redis sorted sets (higher priority = processed first)
    Priority,
}

impl QueueMode {
    /// Check if this is FIFO mode
    pub fn is_fifo(&self) -> bool {
        matches!(self, QueueMode::Fifo)
    }

    /// Check if this is Priority mode
    pub fn is_priority(&self) -> bool {
        matches!(self, QueueMode::Priority)
    }
}

impl std::fmt::Display for QueueMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueMode::Fifo => write!(f, "FIFO"),
            QueueMode::Priority => write!(f, "Priority"),
        }
    }
}

/// Redis-based broker implementation
///
/// # Connections
///
/// The broker holds one auto-reconnecting [`ConnectionManager`], created on
/// first use and shared (cheaply cloned) by every operation. Opening a
/// connection per call — as this used to do — pays a TCP handshake, an
/// AUTH/SELECT round trip, a tokio task spawn and a socket left in
/// `TIME_WAIT` for *every message*, which exhausts the ephemeral port range
/// long before Redis itself is the bottleneck. Blocking pops cannot share a
/// multiplexed connection (they would stall every other command queued behind
/// them on that socket), so they are served from a small dedicated
/// [`ConnectionPool`].
///
/// # Delivery guarantees
///
/// Dequeue is a single `EVAL` that pops the message, stages it on
/// `<queue>:processing` and records its visibility deadline in
/// `<queue>:unacked`. Unacknowledged work is redelivered once that deadline
/// passes, so a crashed worker's task is never stranded. See the
/// [`visibility`] module for the full model.
pub struct RedisBroker {
    client: Client,
    /// Shared multiplexed connection, established on first use.
    ///
    /// Lazily initialised rather than built in the constructor because the
    /// constructors are synchronous and may run outside a tokio runtime.
    conn: OnceCell<ConnectionManager>,
    /// Dedicated connections for blocking pops, which must not share the
    /// multiplexed connection.
    blocking_pool: OnceCell<ConnectionPool>,
    blocking_pool_config: PoolConfig,
    manager_config: ConnectionManagerConfig,
    keys: QueueKeys,
    queue_name: String,
    processing_queue: String,
    dlq_name: String,
    delayed_queue: String,
    cancel_channel: String,
    mode: QueueMode,
    visibility_manager: VisibilityManager,
    visibility_timeout_secs: u64,
    /// How long `dequeue` waits for a message before reporting an empty queue
    block_timeout_secs: f64,
    /// Minimum seconds between two housekeeping sweeps
    sweep_interval_secs: u64,
    /// Unix time of the last housekeeping sweep
    last_sweep: AtomicU64,
    /// How long a revocation is remembered
    revocation_ttl_secs: u64,
    /// The one controller this broker hands out clones of.
    ///
    /// Built once rather than per call: `QueueController` carries a
    /// process-local emergency-stop flag, so constructing a fresh one per
    /// call would give every caller its own immediately-orphaned flag and
    /// `emergency_stop()` would never be observed anywhere.
    queue_controller: QueueController,
}

impl RedisBroker {
    /// Assemble a broker around an already-built client.
    fn from_client(
        client: Client,
        queue_name: &str,
        mode: QueueMode,
        manager_config: ConnectionManagerConfig,
    ) -> Self {
        let queue_controller = QueueController::new(client.clone(), queue_name);
        Self {
            client,
            conn: OnceCell::new(),
            blocking_pool: OnceCell::new(),
            blocking_pool_config: PoolConfig::default(),
            manager_config,
            keys: QueueKeys::new(queue_name),
            queue_name: queue_name.to_string(),
            processing_queue: format!("{}:processing", queue_name),
            dlq_name: format!("{}:dlq", queue_name),
            delayed_queue: format!("{}:delayed", queue_name),
            cancel_channel: format!("{}:cancel", queue_name),
            mode,
            visibility_manager: VisibilityManager::new(),
            visibility_timeout_secs: 300, // 5 minutes default
            block_timeout_secs: 1.0,
            sweep_interval_secs: DEFAULT_SWEEP_INTERVAL_SECS,
            last_sweep: AtomicU64::new(0),
            revocation_ttl_secs: DEFAULT_REVOCATION_TTL_SECS,
            queue_controller,
        }
    }

    /// Create a new Redis broker with FIFO mode
    pub fn new(redis_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_mode(redis_url, queue_name, QueueMode::Fifo)
    }

    /// Create a new Redis broker with specified queue mode
    pub fn with_mode(redis_url: &str, queue_name: &str, mode: QueueMode) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self::from_client(
            client,
            queue_name,
            mode,
            connection::default_manager_config(),
        ))
    }

    /// Create a new Redis broker from a RedisConfig
    pub fn from_config(config: &RedisConfig, queue_name: &str, mode: QueueMode) -> Result<Self> {
        let client = config.build_client()?;

        debug!("Created Redis broker with config: {}", config.describe());

        Ok(Self::from_client(
            client,
            queue_name,
            mode,
            config.manager_config(),
        ))
    }

    /// Create a new Redis broker from a SentinelClient (for high availability)
    pub async fn from_sentinel(
        sentinel: &SentinelClient,
        queue_name: &str,
        mode: QueueMode,
    ) -> Result<Self> {
        let client = sentinel.get_client().await?;

        info!("Created Redis broker with Sentinel support");

        Ok(Self::from_client(
            client,
            queue_name,
            mode,
            connection::default_manager_config(),
        ))
    }

    /// Set visibility timeout (default: 300 seconds)
    ///
    /// A message that is not acknowledged within this window is put back on
    /// the queue by the reaper, so this is the longest a crashed worker can
    /// hold work hostage.
    pub fn with_visibility_timeout(mut self, timeout_secs: u64) -> Self {
        self.visibility_timeout_secs = timeout_secs;
        self
    }

    /// Set how long [`Broker::dequeue`] waits for a message before reporting
    /// an empty queue (default: 1 second).
    pub fn with_block_timeout(mut self, timeout_secs: f64) -> Self {
        self.block_timeout_secs = timeout_secs.max(0.0);
        self
    }

    /// Set the minimum interval between housekeeping sweeps (default: 1
    /// second).
    ///
    /// Each sweep promotes due delayed tasks and reclaims in-flight messages
    /// past their visibility deadline. Sweeps are triggered opportunistically
    /// from `dequeue`, and this interval keeps N polling workers from each
    /// paying for one on every poll.
    pub fn with_sweep_interval(mut self, interval_secs: u64) -> Self {
        self.sweep_interval_secs = interval_secs;
        self
    }

    /// Set how long a revocation recorded by [`Broker::cancel`] is remembered
    /// (default: 24 hours).
    pub fn with_revocation_ttl(mut self, ttl_secs: u64) -> Self {
        self.revocation_ttl_secs = ttl_secs;
        self
    }

    /// Size the pool backing blocking pops (default: [`PoolConfig::default`]).
    ///
    /// A blocking command occupies its connection for the whole wait, so this
    /// caps how many dequeues can be parked at once; beyond it, callers wait
    /// for a free connection instead of opening more sockets.
    pub fn with_blocking_pool(mut self, config: PoolConfig) -> Self {
        self.blocking_pool_config = config;
        self
    }

    /// Get the queue mode
    pub fn mode(&self) -> QueueMode {
        self.mode
    }

    /// How long [`Broker::dequeue`] waits for a message before reporting an
    /// empty queue. Zero disables the blocking wait entirely.
    pub fn block_timeout_secs(&self) -> f64 {
        self.block_timeout_secs
    }

    /// Refuse the enqueue if the queue is paused, draining, or emergency
    /// stopped.
    ///
    /// Refusing loudly rather than dropping the task: a silently discarded
    /// enqueue is data loss the caller never learns about, and "the queue is
    /// closed" is exactly the kind of thing a producer needs to retry or
    /// escalate. (The dequeue side stays quiet — `Ok(None)` — because a
    /// polling worker would otherwise log an error on every poll; its check
    /// lives inside the dequeue script, which already reads the pause key
    /// server-side.)
    ///
    /// Costs one `MGET` on the connection the caller already holds.
    async fn ensure_accepting_work(&self, conn: &mut ConnectionManager) -> Result<()> {
        if self.queue_controller.is_emergency_stopped() {
            return Err(CelersError::Broker(format!(
                "Queue {} is emergency stopped, refusing to enqueue",
                self.queue_name
            )));
        }

        if !queue_control::is_enqueue_allowed(conn, &self.queue_name).await? {
            return Err(CelersError::Broker(format!(
                "Queue {} is paused or draining, refusing to enqueue",
                self.queue_name
            )));
        }

        Ok(())
    }

    /// Get the shared multiplexed connection, establishing it on first use.
    ///
    /// [`ConnectionManager`] is cheap to clone, multiplexes concurrent
    /// commands over one socket and reconnects by itself after a failure.
    async fn get_connection(&self) -> Result<ConnectionManager> {
        self.conn
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager_with_config(self.manager_config.clone())
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))
            })
            .await
            .cloned()
    }

    /// Get the pool backing blocking pops, creating it on first use.
    ///
    /// The pool is told how long its connections will block so it can size
    /// their response timeout accordingly: the `redis` crate's 500 ms default
    /// is shorter than even the default one-second block, which turns an
    /// empty queue into `Err("timed out")` instead of `Ok(None)`.
    async fn blocking_connections(&self) -> Result<&ConnectionPool> {
        self.blocking_pool
            .get_or_try_init(|| async {
                let config = self
                    .blocking_pool_config
                    .clone()
                    .with_max_block(Duration::from_secs_f64(self.block_timeout_secs.max(0.0)));
                ConnectionPool::new(self.client.clone(), config).await
            })
            .await
    }

    /// The Redis keys this broker operates on.
    pub fn keys(&self) -> &QueueKeys {
        &self.keys
    }

    /// Get the number of tasks in the Dead Letter Queue
    pub async fn dlq_size(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let size: usize = conn
            .llen(&self.dlq_name)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ size: {}", e)))?;

        Ok(size)
    }

    /// Inspect tasks in the Dead Letter Queue
    pub async fn inspect_dlq(&self, limit: isize) -> Result<Vec<SerializedTask>> {
        let mut conn = self.get_connection().await?;

        let items: Vec<String> = conn
            .lrange(&self.dlq_name, 0, limit - 1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to inspect DLQ: {}", e)))?;

        let mut tasks = Vec::new();
        for item in items {
            let task: SerializedTask = serde_json::from_str(&item)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    /// Turn a raw dead-letter entry into the `(original, replacement, score)`
    /// triple the replay script consumes, resetting the task to `Pending`.
    fn replay_triple(item: String) -> Result<(String, String, f64)> {
        let mut task: SerializedTask =
            serde_json::from_str(&item).map_err(|e| CelersError::Deserialization(e.to_string()))?;
        task.metadata.state = celers_core::TaskState::Pending;

        let score = -(task.metadata.priority as f64);
        let replacement =
            serde_json::to_string(&task).map_err(|e| CelersError::Serialization(e.to_string()))?;

        Ok((item, replacement, score))
    }

    /// Replay a task from the Dead Letter Queue back to the main queue
    pub async fn replay_from_dlq(&self, task_id: &TaskId) -> Result<bool> {
        let mut conn = self.get_connection().await?;

        // Get all DLQ items
        let items: Vec<String> = conn
            .lrange(&self.dlq_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ items: {}", e)))?;

        // Find the task by ID
        for item in items {
            let task: SerializedTask = serde_json::from_str(&item)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            if &task.metadata.id == task_id {
                let triple = Self::replay_triple(item)?;

                // The claim (LREM) and the re-enqueue happen in one
                // server-side step, so a failure in between can neither
                // duplicate nor lose the task.
                let moved = self
                    .visibility_manager
                    .replay_dlq(
                        &mut conn,
                        &self.keys,
                        self.mode,
                        std::slice::from_ref(&triple),
                    )
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to replay task: {}", e)))?;

                if moved > 0 {
                    info!("Replayed task {} from DLQ", task_id);
                    return Ok(true);
                }

                // Another replayer claimed the same entry first.
                return Ok(false);
            }
        }

        Ok(false)
    }

    /// Clear all tasks from the Dead Letter Queue
    pub async fn clear_dlq(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let count: usize = conn
            .llen(&self.dlq_name)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ size: {}", e)))?;

        conn.del::<_, ()>(&self.dlq_name)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to clear DLQ: {}", e)))?;

        info!("Cleared {} tasks from DLQ", count);
        Ok(count)
    }

    /// Get the cancellation channel name (for workers to subscribe)
    pub fn cancel_channel(&self) -> &str {
        &self.cancel_channel
    }

    /// Create a PubSub connection for listening to cancellation messages
    pub async fn create_pubsub(&self) -> Result<redis::aio::PubSub> {
        let pubsub = self.client.get_async_pubsub().await.map_err(|e| {
            CelersError::Broker(format!("Failed to create PubSub connection: {}", e))
        })?;
        Ok(pubsub)
    }

    /// Create a health checker for monitoring Redis status
    pub fn health_checker(&self) -> HealthChecker {
        HealthChecker::new(self.client.clone())
    }

    /// A handle on this broker's queue controller, for pause/resume/drain.
    ///
    /// Every call returns a clone of the *same* controller, so an emergency
    /// stop triggered through one handle is seen by every other handle and by
    /// the broker itself. (Constructing a controller per call, as this used
    /// to, gave each caller a private flag that nothing else could observe.)
    pub fn queue_controller(&self) -> QueueController {
        self.queue_controller.clone()
    }

    /// Create a task deduplicator
    ///
    /// Shares this broker's connection when one has already been
    /// established, so a deduplicator built per enqueue does not open a
    /// connection of its own.
    pub fn deduplicator(&self) -> Deduplicator {
        let deduplicator = Deduplicator::new(self.client.clone(), &self.queue_name);
        match self.conn.get() {
            Some(manager) => deduplicator.with_connection_manager(manager.clone()),
            None => deduplicator,
        }
    }

    /// Create a script manager for Lua script optimization
    pub fn script_manager(&self) -> ScriptManager {
        ScriptManager::new(self.client.clone())
    }

    /// Create a partition manager for distributed queues
    pub fn partition_manager(
        &self,
        num_partitions: usize,
        strategy: PartitionStrategy,
    ) -> PartitionManager {
        PartitionManager::new(num_partitions, strategy, &self.queue_name)
    }

    /// Create a batch operations handler for advanced batch processing
    ///
    /// Inherits this broker's visibility timeout and, when one has already
    /// been established, its connection.
    pub fn batch_operations(&self) -> BatchOperations {
        let operations =
            BatchOperations::new(self.client.clone(), self.queue_name.clone(), self.mode)
                .with_visibility_timeout(self.visibility_timeout_secs);
        match self.conn.get() {
            Some(manager) => operations.with_connection_manager(manager.clone()),
            None => operations,
        }
    }

    /// Create a priority manager for dynamic priority adjustments
    pub fn priority_manager(&self) -> PriorityManager {
        PriorityManager::new(self.client.clone(), self.queue_name.clone(), self.mode)
    }

    /// Create a task query interface for inspecting tasks
    pub fn task_query(&self) -> TaskQuery {
        TaskQuery::new(
            self.client.clone(),
            self.queue_name.clone(),
            self.processing_queue.clone(),
            self.dlq_name.clone(),
            self.mode,
        )
    }

    /// Create a backup manager for queue backup and restore
    pub fn backup_manager(&self) -> BackupManager {
        BackupManager::new(
            self.client.clone(),
            self.queue_name.clone(),
            self.processing_queue.clone(),
            self.dlq_name.clone(),
            self.delayed_queue.clone(),
            self.mode,
        )
    }

    /// Set TTL (time-to-live) for tasks in all queues
    ///
    /// This helps prevent unbounded queue growth by automatically expiring old tasks.
    /// TTL is set in seconds.
    pub async fn set_queue_ttl(&self, ttl_secs: u64) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // Set TTL on main queue
        let _: () = redis::cmd("EXPIRE")
            .arg(&self.queue_name)
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to set queue TTL: {}", e)))?;

        // Set TTL on processing queue
        let _: () = redis::cmd("EXPIRE")
            .arg(&self.processing_queue)
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to set processing queue TTL: {}", e))
            })?;

        // Set TTL on delayed queue
        let _: () = redis::cmd("EXPIRE")
            .arg(&self.delayed_queue)
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to set delayed queue TTL: {}", e)))?;

        debug!("Set TTL of {} seconds on all queues", ttl_secs);

        Ok(())
    }

    /// Clean up old tasks from DLQ (older than specified age in seconds)
    pub async fn cleanup_dlq(&self, max_age_secs: u64) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let items: Vec<String> = conn
            .lrange(&self.dlq_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ items: {}", e)))?;

        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_secs as i64);
        let mut removed_count = 0;

        for item in items {
            if let Ok(task) = serde_json::from_str::<SerializedTask>(&item) {
                if task.metadata.created_at < cutoff {
                    conn.lrem::<_, _, ()>(&self.dlq_name, 1, &item)
                        .await
                        .map_err(|e| {
                            CelersError::Broker(format!("Failed to remove from DLQ: {}", e))
                        })?;
                    removed_count += 1;
                }
            }
        }

        if removed_count > 0 {
            info!("Cleaned up {} old tasks from DLQ", removed_count);
        }

        Ok(removed_count)
    }

    /// Get queue statistics (pending, processing, DLQ, delayed counts)
    pub async fn get_queue_stats(&self) -> Result<QueueStats> {
        self.health_checker()
            .get_queue_stats(&self.queue_name, self.mode.is_priority())
            .await
    }

    /// Check Redis health status
    pub async fn check_health(&self) -> RedisHealthStatus {
        self.health_checker().check_health().await
    }

    /// Ping Redis and return latency in milliseconds
    pub async fn ping(&self) -> Result<u64> {
        self.health_checker().ping().await
    }

    /// Get the visibility timeout in seconds
    pub fn visibility_timeout(&self) -> u64 {
        self.visibility_timeout_secs
    }

    /// Get the Redis client (for advanced operations)
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the delayed queue name
    pub fn delayed_queue_name(&self) -> &str {
        &self.delayed_queue
    }

    /// Get the processing queue name
    pub fn processing_queue_name(&self) -> &str {
        &self.processing_queue
    }

    /// Get the DLQ name
    pub fn dlq_name(&self) -> &str {
        &self.dlq_name
    }

    /// Get the main queue name
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Get the unacked set name (in-flight messages and their deadlines)
    pub fn unacked_set_name(&self) -> &str {
        &self.keys.unacked
    }

    /// Get all queue names managed by this broker
    ///
    /// Includes the unacked set: it holds in-flight bookkeeping, so leaving
    /// it out of [`Self::purge_all_queues`] would strand deadlines whose
    /// messages no longer exist and have the reaper resurrect ghosts.
    pub fn queue_names(&self) -> Vec<String> {
        vec![
            self.queue_name.clone(),
            self.processing_queue.clone(),
            self.dlq_name.clone(),
            self.delayed_queue.clone(),
            self.keys.unacked.clone(),
        ]
    }

    /// Check if a specific queue exists and has tasks
    pub async fn queue_exists(&self, queue_name: &str) -> Result<bool> {
        let mut conn = self.get_connection().await?;

        let exists: bool = redis::cmd("EXISTS")
            .arg(queue_name)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check queue existence: {}", e)))?;

        Ok(exists)
    }

    /// Get the size of a specific queue by name
    pub async fn get_queue_size_by_name(&self, queue_name: &str) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        // Try both list and sorted set
        let list_size: usize = conn.llen(queue_name).await.unwrap_or(0);
        if list_size > 0 {
            return Ok(list_size);
        }

        let zset_size: usize = conn.zcard(queue_name).await.unwrap_or(0);
        Ok(zset_size)
    }

    /// Delete a specific queue (use with caution!)
    pub async fn delete_queue(&self, queue_name: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;

        conn.del::<_, ()>(queue_name)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to delete queue: {}", e)))?;

        warn!("Deleted queue: {}", queue_name);
        Ok(())
    }

    /// Purge all queues (main, processing, DLQ, delayed)
    pub async fn purge_all_queues(&self) -> Result<usize> {
        let mut total_removed = 0;

        for queue_name in self.queue_names() {
            if let Ok(size) = self.get_queue_size_by_name(&queue_name).await {
                total_removed += size;
                self.delete_queue(&queue_name).await?;
            }
        }

        warn!("Purged all queues, removed {} tasks total", total_removed);
        Ok(total_removed)
    }

    /// Get a summary of all queue sizes
    pub async fn get_all_queue_sizes(&self) -> Result<std::collections::HashMap<String, usize>> {
        let mut sizes = std::collections::HashMap::new();

        for queue_name in self.queue_names() {
            if let Ok(size) = self.get_queue_size_by_name(&queue_name).await {
                sizes.insert(queue_name, size);
            }
        }

        Ok(sizes)
    }

    /// Requeue in-flight tasks whose visibility timeout has expired.
    ///
    /// This is the crash-recovery path: a worker that dies mid-task stops
    /// acknowledging, its deadline lapses, and the message goes back on the
    /// queue. Tasks a *live* worker is still executing keep their deadline
    /// and are left alone — the old blanket "move everything back" behaviour
    /// duplicated exactly those tasks, so it now lives under the explicit
    /// name [`Self::force_recover_all_processing_tasks`].
    ///
    /// One call moves at most [`DEFAULT_SWEEP_BATCH`] messages so a huge
    /// backlog cannot stall the (single-threaded) Redis server; call it again
    /// while it keeps returning that many.
    ///
    /// Returns the number of tasks requeued.
    pub async fn recover_processing_tasks(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let recovered = self
            .visibility_manager
            .reap(
                &mut conn,
                &self.keys,
                self.mode,
                self.visibility_timeout_secs,
                DEFAULT_SWEEP_BATCH,
            )
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to recover timed-out tasks: {}", e))
            })?;

        if recovered > 0 {
            info!("Recovered {} timed-out tasks from processing", recovered);
        }

        Ok(recovered.max(0) as usize)
    }

    /// Move **every** in-flight task back to the main queue, whether or not
    /// its visibility timeout has expired.
    ///
    /// # Warning
    ///
    /// Tasks that a live worker is executing right now are moved too, so they
    /// will run a second time. Use this only when no worker is running (for
    /// example after an unclean shutdown of the whole fleet); during normal
    /// operation use [`Self::recover_processing_tasks`], which only reclaims
    /// work whose owner has gone silent past the visibility timeout.
    ///
    /// Returns the number of tasks moved.
    pub async fn force_recover_all_processing_tasks(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let items: Vec<String> = conn
            .lrange(&self.processing_queue, 0, -1)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to get processing queue items: {}", e))
            })?;

        if items.is_empty() {
            return Ok(0);
        }

        let count = items.len();
        let mut pipe = redis::pipe();
        // MULTI/EXEC: a bare pipeline is only batching, so a failure halfway
        // through would leave tasks in both structures.
        pipe.atomic();

        for item in &items {
            // Parse to get task
            if let Ok(task) = serde_json::from_str::<SerializedTask>(item) {
                // Add back to the *back* of the main queue based on mode
                match self.mode {
                    QueueMode::Fifo => {
                        pipe.lpush(&self.queue_name, item);
                    }
                    QueueMode::Priority => {
                        let score = -(task.metadata.priority as f64);
                        pipe.zadd(&self.queue_name, item, score);
                    }
                }
                // Clear both in-flight structures
                pipe.lrem(&self.processing_queue, 1, item);
                pipe.zrem(&self.keys.unacked, item);
            }
        }

        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to recover processing tasks: {}", e))
            })?;

        info!("Recovered {} tasks from processing queue", count);
        Ok(count)
    }

    /// Get the total number of tasks across all queues (main, processing, DLQ, delayed)
    pub async fn total_task_count(&self) -> Result<usize> {
        let sizes = self.get_all_queue_sizes().await?;
        Ok(sizes.values().sum())
    }

    /// Check if the broker is idle (all queues empty)
    pub async fn is_idle(&self) -> Result<bool> {
        let total = self.total_task_count().await?;
        Ok(total == 0)
    }

    /// Estimate memory usage of tasks in all queues (in bytes)
    ///
    /// This is an approximation based on serialized task sizes.
    pub async fn estimate_memory_usage(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let mut total_bytes = 0usize;

        // Check each queue
        for queue_name in self.queue_names() {
            let memory: usize = redis::cmd("MEMORY")
                .arg("USAGE")
                .arg(&queue_name)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            total_bytes += memory;
        }

        Ok(total_bytes)
    }

    /// Move tasks from DLQ back to main queue in bulk
    ///
    /// Single pass: one `LRANGE` to read the candidates, then one `EVAL` per
    /// [`REPLAY_CHUNK`] entries to claim and re-enqueue them. Replaying `n`
    /// tasks by calling [`Self::replay_from_dlq`] in a loop would instead read
    /// and deserialize the *whole* dead letter queue once per task.
    ///
    /// Returns the number of tasks moved.
    pub async fn bulk_replay_from_dlq(&self, max_count: Option<usize>) -> Result<usize> {
        let limit = max_count.unwrap_or(usize::MAX).min(isize::MAX as usize) as isize;
        if limit == 0 {
            return Ok(0);
        }

        let mut conn = self.get_connection().await?;

        let items: Vec<String> = conn
            .lrange(&self.dlq_name, 0, limit - 1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ items: {}", e)))?;

        let mut triples = Vec::with_capacity(items.len());
        for item in items {
            match Self::replay_triple(item) {
                Ok(triple) => triples.push(triple),
                // A malformed entry cannot be re-enqueued; leave it in the
                // DLQ for inspection rather than failing the whole batch.
                Err(e) => warn!("Skipping undeserializable DLQ entry: {}", e),
            }
        }

        let mut moved = 0usize;
        for chunk in triples.chunks(REPLAY_CHUNK) {
            let chunk_moved = self
                .visibility_manager
                .replay_dlq(&mut conn, &self.keys, self.mode, chunk)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to replay DLQ batch: {}", e)))?;
            moved += chunk_moved.max(0) as usize;
        }

        if moved > 0 {
            info!("Replayed {} tasks from DLQ", moved);
        }

        Ok(moved)
    }

    /// Peek at the next task without dequeueing it
    ///
    /// Useful for inspecting what will be processed next without removing it from the queue.
    pub async fn peek_next(&self) -> Result<Option<SerializedTask>> {
        let mut conn = self.get_connection().await?;

        let data: Option<String> = match self.mode {
            QueueMode::Fifo => {
                // The tail is the oldest message and therefore the next one
                // out: producers push to the head, consumers pop the tail.
                conn.lindex(&self.queue_name, -1)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to peek: {}", e)))?
            }
            QueueMode::Priority => {
                // Get first item in sorted set (lowest score = highest priority)
                let items: Vec<String> = conn
                    .zrange(&self.queue_name, 0, 0)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to peek: {}", e)))?;
                items.into_iter().next()
            }
        };

        if let Some(serialized) = data {
            let task: SerializedTask = serde_json::from_str(&serialized)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    /// Get queue depth percentage (current size / max recommended size)
    ///
    /// Returns a value between 0.0 and 1.0+ where > 0.8 suggests the queue is getting full.
    /// max_recommended_size defaults to 10000 if not specified.
    pub async fn queue_depth_percentage(&self, max_recommended_size: Option<usize>) -> Result<f64> {
        let max_size = max_recommended_size.unwrap_or(10000) as f64;
        let current_size = self.queue_size().await? as f64;
        Ok(current_size / max_size)
    }

    /// Check if the queue is approaching capacity (> 80% of recommended max)
    pub async fn is_near_capacity(&self, max_recommended_size: Option<usize>) -> Result<bool> {
        let percentage = self.queue_depth_percentage(max_recommended_size).await?;
        Ok(percentage > 0.8)
    }

    /// Move ready delayed tasks to the main queue
    ///
    /// A single `EVAL` claims each due message with `ZREM` before pushing it,
    /// so concurrent sweepers are mutually exclusive. The previous
    /// read-then-write pipeline let every worker read the same ready set and
    /// re-enqueue it before any of them removed it, delivering each delayed
    /// task once per concurrent poller; a bare `redis::pipe()` is command
    /// batching, not `MULTI`/`EXEC`, so it never made that safe.
    ///
    /// One call promotes at most [`DEFAULT_SWEEP_BATCH`] tasks; call it again
    /// while it keeps returning that many.
    ///
    /// Returns the number of tasks promoted.
    pub async fn promote_delayed_tasks(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let moved = self
            .visibility_manager
            .promote_delayed(&mut conn, &self.keys, self.mode, DEFAULT_SWEEP_BATCH)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to move delayed tasks: {}", e)))?;

        if moved > 0 {
            debug!("Moved {} delayed tasks to main queue", moved);
        }

        Ok(moved.max(0) as usize)
    }

    /// Whether enough time has passed to run another housekeeping sweep.
    ///
    /// Claiming the slot with a compare-exchange means that when N workers
    /// poll simultaneously exactly one of them pays for the sweep.
    fn claim_sweep(&self) -> bool {
        let now = now_secs();
        let last = self.last_sweep.load(Ordering::Relaxed);

        if now.saturating_sub(last) < self.sweep_interval_secs {
            return false;
        }

        self.last_sweep
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Promote due delayed tasks and reclaim timed-out in-flight messages.
    ///
    /// Runs at most once every [`Self::with_sweep_interval`] seconds. Both
    /// steps are bounded, atomic scripts, so this costs two round trips —
    /// never the unbounded `ZRANGEBYSCORE` + deserialize-everything pass that
    /// used to run on *every* dequeue of *every* worker.
    ///
    /// Failures are logged rather than propagated: housekeeping must not turn
    /// a healthy dequeue into an error, but it must not be silent either.
    async fn run_maintenance(&self, conn: &mut ConnectionManager) {
        if !self.claim_sweep() {
            return;
        }

        match self
            .visibility_manager
            .promote_delayed(conn, &self.keys, self.mode, DEFAULT_SWEEP_BATCH)
            .await
        {
            Ok(moved) if moved > 0 => debug!("Promoted {} delayed tasks", moved),
            Ok(_) => {}
            Err(e) => warn!("Failed to promote delayed tasks: {}", e),
        }

        match self
            .visibility_manager
            .reap(
                conn,
                &self.keys,
                self.mode,
                self.visibility_timeout_secs,
                DEFAULT_SWEEP_BATCH,
            )
            .await
        {
            Ok(recovered) if recovered > 0 => {
                warn!(
                    "Recovered {} tasks whose visibility timeout expired",
                    recovered
                )
            }
            Ok(_) => {}
            Err(e) => warn!("Failed to recover timed-out tasks: {}", e),
        }
    }

    /// Blocking tail of the dequeue path.
    ///
    /// Runs on a dedicated pooled connection: a blocking command issued on
    /// the shared multiplexed connection would park every other command
    /// queued behind it on that socket.
    async fn blocking_pop(&self, conn: &mut ConnectionManager) -> Result<Option<String>> {
        // Priority mode has no blocking primitive (there is no
        // `BZPOPMIN`-into-list), so the atomic script is the whole story.
        if !self.mode.is_fifo() || self.block_timeout_secs <= 0.0 {
            return Ok(None);
        }

        let mut pooled = self.blocking_connections().await?.get().await?;
        let data: Option<String> = pooled
            .get_mut()
            .brpoplpush(
                &self.queue_name,
                &self.processing_queue,
                self.block_timeout_secs,
            )
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to dequeue task: {}", e)))?;
        drop(pooled);

        let Some(message) = data else {
            return Ok(None);
        };

        // Record the visibility deadline for the message we just staged. If
        // this write is lost (crash, connection drop) the reaper adopts the
        // orphan on its next pass, so recovery never depends on it landing.
        self.visibility_manager
            .register_unacked(conn, &self.keys, &message, self.visibility_timeout_secs)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to record visibility deadline: {}", e))
            })?;

        // The atomic script filters revoked messages itself; this path has to
        // ask, or a revoked task would still be delivered.
        if let Ok(Some(id)) = message_task_id(&message) {
            if self
                .visibility_manager
                .is_revoked(conn, &self.keys, &id)
                .await
                .unwrap_or(false)
            {
                self.visibility_manager
                    .ack_unacked(conn, &self.keys, &message)
                    .await
                    .map_err(|e| {
                        CelersError::Broker(format!("Failed to drop revoked task: {}", e))
                    })?;
                debug!("Dropped revoked task {} during dequeue", id);
                return Ok(None);
            }
        }

        Ok(Some(message))
    }

    /// Move a message that cannot be deserialized out of the in-flight
    /// structures and into the dead letter queue.
    ///
    /// Without this a poison message is redelivered forever: the reaper puts
    /// it back on the queue every visibility timeout, and every worker that
    /// picks it up fails on it again.
    async fn quarantine_poison_message(&self, conn: &mut ConnectionManager, message: &str) {
        if let Err(e) = self
            .visibility_manager
            .nack_unacked(conn, &self.keys, message, NackAction::DeadLetter, self.mode)
            .await
        {
            error!("Failed to quarantine undeserializable message: {}", e);
        }
    }
}

/// Extract the task id from a raw serialized message.
fn message_task_id(message: &str) -> Result<Option<String>> {
    let task: SerializedTask =
        serde_json::from_str(message).map_err(|e| CelersError::Deserialization(e.to_string()))?;
    Ok(Some(task.metadata.id.to_string()))
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let mut conn = self.get_connection().await?;
        self.ensure_accepting_work(&mut conn).await?;

        let task_id = task.metadata.id;
        let priority = task.metadata.priority;
        let serialized =
            serde_json::to_string(&task).map_err(|e| CelersError::Serialization(e.to_string()))?;

        match self.mode {
            QueueMode::Fifo => {
                // Push to the head: consumers pop the tail (`BRPOPLPUSH` /
                // `RPOP`), so head-push + tail-pop is FIFO. Appending with
                // `RPUSH` instead would make the queue a *stack* — the newest
                // task delivered first, the oldest starving indefinitely —
                // and would disagree with Kombu, whose Redis transport also
                // produces with `LPUSH` and consumes with `RPOP`.
                conn.lpush::<_, _, ()>(&self.queue_name, &serialized)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to enqueue task: {}", e)))?;
            }
            QueueMode::Priority => {
                // Use sorted set with priority as score (negate for descending order)
                // Higher priority values should be processed first
                let score = -priority as f64;
                conn.zadd::<_, _, _, ()>(&self.queue_name, &serialized, score)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to enqueue task: {}", e)))?;
            }
        }

        debug!(
            "Enqueued task {} to queue {} (priority: {})",
            task_id, self.queue_name, priority
        );

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();

            // Track per-task-type metrics
            let task_name = &task.metadata.name;
            TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_name]).inc();
        }

        Ok(task_id)
    }

    /// Take the next message, staging it as in-flight under a visibility
    /// timeout.
    ///
    /// The pop, the staging and the deadline are one atomic `EVAL`, so a
    /// message can never be lost between "removed from the queue" and
    /// "recorded somewhere recoverable" — the old Priority path did
    /// `ZPOPMIN` and then a *separate* `LPUSH`, and a crash in between
    /// destroyed the task with no trace anywhere in Redis.
    ///
    /// Returns `None` when the queue is empty, paused, or the only available
    /// messages were revoked.
    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        let mut conn = self.get_connection().await?;

        // Promote due delayed tasks and reclaim expired in-flight work.
        // Throttled, bounded and atomic (see `run_maintenance`).
        self.run_maintenance(&mut conn).await;

        let outcome = self
            .visibility_manager
            .pop_to_unacked(
                &mut conn,
                &self.keys,
                self.mode,
                self.visibility_timeout_secs,
                REVOKED_SKIP_BUDGET,
            )
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to dequeue task: {}", e)))?;

        let result = match outcome {
            PopOutcome::Message(data) => Some(data),
            PopOutcome::Paused => {
                debug!("Queue {} is paused, not dequeuing", self.queue_name);
                return Ok(None);
            }
            // Nothing was ready; wait for an arrival instead of spinning.
            PopOutcome::Empty => self.blocking_pop(&mut conn).await?,
        };

        match result {
            Some(data) => {
                let task: SerializedTask = match serde_json::from_str(&data) {
                    Ok(task) => task,
                    Err(e) => {
                        // Do not leave it in-flight: the reaper would put it
                        // back on the queue every visibility timeout forever.
                        error!("Undeserializable message moved to DLQ: {}", e);
                        self.quarantine_poison_message(&mut conn, &data).await;
                        return Err(CelersError::Deserialization(e.to_string()));
                    }
                };

                debug!(
                    "Dequeued task {} (priority: {})",
                    task.metadata.id, task.metadata.priority
                );
                Ok(Some(BrokerMessage {
                    task,
                    receipt_handle: Some(data),
                }))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()> {
        if let Some(handle) = receipt_handle {
            let mut conn = self.get_connection().await?;

            // Clears both in-flight structures: the visibility deadline and
            // the processing-list entry.
            self.visibility_manager
                .ack_unacked(&mut conn, &self.keys, handle)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to ack task: {}", e)))?;

            info!("Acknowledged task {}", task_id);
        }
        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        if let Some(handle) = receipt_handle {
            let mut conn = self.get_connection().await?;

            if requeue {
                // Re-add to main queue (increment retry count)
                let mut task: SerializedTask = serde_json::from_str(handle)
                    .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                // Update task state to Retrying
                let retry_count = match task.metadata.state {
                    celers_core::TaskState::Retrying(count) => count + 1,
                    _ => 1,
                };
                task.metadata.state = celers_core::TaskState::Retrying(retry_count);

                let serialized = serde_json::to_string(&task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                // One EVAL clears both in-flight structures and requeues the
                // rewritten payload at the *back* of the queue, so a retry
                // cannot spin ahead of tasks that were already waiting.
                self.visibility_manager
                    .nack_unacked(
                        &mut conn,
                        &self.keys,
                        handle,
                        NackAction::Requeue {
                            payload: &serialized,
                            score: -(task.metadata.priority as f64),
                        },
                        self.mode,
                    )
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to requeue task: {}", e)))?;

                info!(
                    "Rejected and requeued task {} (retry {})",
                    task_id, retry_count
                );
            } else {
                // Move to Dead Letter Queue
                self.visibility_manager
                    .nack_unacked(
                        &mut conn,
                        &self.keys,
                        handle,
                        NackAction::DeadLetter,
                        self.mode,
                    )
                    .await
                    .map_err(|e| {
                        CelersError::Broker(format!("Failed to move task to DLQ: {}", e))
                    })?;
                error!("Task {} failed permanently, moved to DLQ", task_id);
            }
        }
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let size: usize = match self.mode {
            QueueMode::Fifo => conn
                .llen(&self.queue_name)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get queue size: {}", e)))?,
            QueueMode::Priority => conn
                .zcard(&self.queue_name)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get queue size: {}", e)))?,
        };

        #[cfg(feature = "metrics")]
        {
            QUEUE_SIZE.set(size as f64);

            // Also update processing and DLQ sizes
            let processing_size: usize = conn.llen(&self.processing_queue).await.unwrap_or(0);
            PROCESSING_QUEUE_SIZE.set(processing_size as f64);

            let dlq_size: usize = conn.llen(&self.dlq_name).await.unwrap_or(0);
            DLQ_SIZE.set(dlq_size as f64);
        }

        Ok(size)
    }

    /// Revoke a task.
    ///
    /// The revocation is **durable**: the id is recorded in
    /// `<queue>:revoked` (scored by its expiry, pruned on every call) and
    /// every dequeue path drops messages whose id is listed there, so a task
    /// that is still sitting in the queue really is cancelled. Pending copies
    /// are also removed from the main and delayed queues on a best-effort,
    /// bounded scan.
    ///
    /// Pub/Sub notification is kept for workers that are already executing
    /// the task, but it is *not* the mechanism: it is fire-and-forget, so a
    /// worker that is restarting or momentarily disconnected never sees it —
    /// which is exactly why the old implementation, which only published,
    /// cancelled nothing at all.
    ///
    /// Returns `true` once the revocation is recorded.
    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let cancel_msg = task_id.to_string();

        let removed = self
            .visibility_manager
            .revoke(
                &mut conn,
                &self.keys,
                &cancel_msg,
                self.mode,
                self.revocation_ttl_secs,
                REVOKE_SCAN_LIMIT,
            )
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to record revocation: {}", e)))?;

        // Notify workers that may already be executing the task.
        let subscribers: i32 = conn
            .publish(&self.cancel_channel, &cancel_msg)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to publish cancel message: {}", e)))?;

        info!(
            "Revoked task {} (removed {} pending copies, notified {} subscriber(s))",
            task_id, removed, subscribers
        );

        Ok(true)
    }

    // Optimized batch operations using Redis pipelining

    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.get_connection().await?;
        self.ensure_accepting_work(&mut conn).await?;

        let mut task_ids = Vec::with_capacity(tasks.len());
        let mut pipe = redis::pipe();
        // MULTI/EXEC so a batch enqueue is all-or-nothing rather than a
        // partially applied batch after a mid-flight failure.
        pipe.atomic();

        // Build pipeline with all enqueue operations
        for task in &tasks {
            let task_id = task.metadata.id;
            task_ids.push(task_id);

            let serialized = serde_json::to_string(&task)
                .map_err(|e| CelersError::Serialization(e.to_string()))?;

            match self.mode {
                QueueMode::Fifo => {
                    // Head-push, tail-pop: see `enqueue`.
                    pipe.lpush(&self.queue_name, &serialized);
                }
                QueueMode::Priority => {
                    let score = -(task.metadata.priority as f64);
                    pipe.zadd(&self.queue_name, &serialized, score);
                }
            }
        }

        // Execute all operations in a single round-trip
        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to enqueue batch: {}", e)))?;

        debug!(
            "Enqueued batch of {} tasks to queue {}",
            tasks.len(),
            self.queue_name
        );

        #[cfg(feature = "metrics")]
        {
            use celers_metrics::{
                BATCH_ENQUEUE_TOTAL, BATCH_SIZE, TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL,
            };
            TASKS_ENQUEUED_TOTAL.inc_by(tasks.len() as f64);
            BATCH_ENQUEUE_TOTAL.inc();
            BATCH_SIZE.observe(tasks.len() as f64);

            // Track per-task-type metrics
            for task in &tasks {
                let task_name = &task.metadata.name;
                TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_name]).inc();
            }
        }

        Ok(task_ids)
    }

    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.get_connection().await?;

        self.run_maintenance(&mut conn).await;

        // One EVAL pops the whole batch, stages it and records a visibility
        // deadline per message. The old Priority path popped the batch with
        // `ZPOPMIN` and only then pipelined the `LPUSH`es, so a failure in
        // between destroyed up to `count` tasks at once.
        let items = self
            .visibility_manager
            .pop_batch_to_unacked(
                &mut conn,
                &self.keys,
                self.mode,
                self.visibility_timeout_secs,
                count,
            )
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to dequeue batch: {}", e)))?;

        let mut messages = Vec::with_capacity(items.len());
        for data in items {
            match serde_json::from_str::<SerializedTask>(&data) {
                Ok(task) => messages.push(BrokerMessage {
                    task,
                    receipt_handle: Some(data),
                }),
                Err(e) => {
                    // Quarantine rather than poison-loop the batch forever.
                    error!("Undeserializable message moved to DLQ: {}", e);
                    self.quarantine_poison_message(&mut conn, &data).await;
                }
            }
        }

        debug!(
            "Dequeued batch of {} tasks from queue {}",
            messages.len(),
            self.queue_name
        );

        #[cfg(feature = "metrics")]
        if !messages.is_empty() {
            use celers_metrics::{BATCH_DEQUEUE_TOTAL, BATCH_SIZE};
            BATCH_DEQUEUE_TOTAL.inc();
            BATCH_SIZE.observe(messages.len() as f64);
        }

        Ok(messages)
    }

    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;

        let mut pipe = redis::pipe();
        // MULTI/EXEC: acknowledging half a batch would leave the rest
        // in-flight and have the reaper redeliver already-finished work.
        pipe.atomic();
        let mut ack_count = 0;

        for (task_id, receipt_handle) in tasks {
            if let Some(handle) = receipt_handle {
                // Clear both in-flight structures, as `ack` does.
                pipe.zrem(&self.keys.unacked, handle);
                pipe.lrem(&self.processing_queue, 1, handle);
                ack_count += 1;
            } else {
                warn!("No receipt handle for task {}, skipping ack", task_id);
            }
        }

        if ack_count > 0 {
            pipe.query_async::<redis::Value>(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to ack batch: {}", e)))?;

            debug!("Acknowledged batch of {} tasks", ack_count);
        }

        Ok(())
    }

    // Delayed Task Execution

    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> Result<TaskId> {
        let mut conn = self.get_connection().await?;

        let task_id = task.metadata.id;
        let serialized =
            serde_json::to_string(&task).map_err(|e| CelersError::Serialization(e.to_string()))?;

        // Add to delayed queue (sorted set with execute_at as score)
        conn.zadd::<_, _, _, ()>(&self.delayed_queue, &serialized, execute_at as f64)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to schedule delayed task: {}", e)))?;

        debug!(
            "Scheduled task {} for execution at Unix timestamp {} (queue: {})",
            task_id, execute_at, self.delayed_queue
        );

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();

            // Track per-task-type metrics
            let task_name = &task.metadata.name;
            TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_name]).inc();
        }

        Ok(task_id)
    }

    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Failed to get current time: {}", e)))?
            .as_secs() as i64;

        let execute_at = now + delay_secs as i64;
        self.enqueue_at(task, execute_at).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_mode_is_fifo() {
        assert!(QueueMode::Fifo.is_fifo());
        assert!(!QueueMode::Priority.is_fifo());
    }

    #[test]
    fn test_queue_mode_is_priority() {
        assert!(!QueueMode::Fifo.is_priority());
        assert!(QueueMode::Priority.is_priority());
    }

    #[test]
    fn test_queue_mode_display() {
        assert_eq!(QueueMode::Fifo.to_string(), "FIFO");
        assert_eq!(QueueMode::Priority.to_string(), "Priority");
    }

    #[test]
    fn test_redis_broker_new() {
        let broker = RedisBroker::new("redis://localhost:6379", "test_queue");
        assert!(broker.is_ok());

        let broker = broker.unwrap();
        assert_eq!(broker.queue_name(), "test_queue");
        assert_eq!(broker.mode(), QueueMode::Fifo);
    }

    #[test]
    fn test_redis_broker_with_mode() {
        let broker =
            RedisBroker::with_mode("redis://localhost:6379", "test_queue", QueueMode::Priority);
        assert!(broker.is_ok());

        let broker = broker.unwrap();
        assert_eq!(broker.mode(), QueueMode::Priority);
    }

    #[test]
    fn test_redis_broker_with_visibility_timeout() {
        let broker = RedisBroker::new("redis://localhost:6379", "test_queue")
            .unwrap()
            .with_visibility_timeout(600);

        assert_eq!(broker.visibility_timeout(), 600);
    }

    #[test]
    fn test_queue_names() {
        let broker = RedisBroker::new("redis://localhost:6379", "my_queue").unwrap();

        assert_eq!(broker.queue_name(), "my_queue");
        assert_eq!(broker.processing_queue_name(), "my_queue:processing");
        assert_eq!(broker.dlq_name(), "my_queue:dlq");
        assert_eq!(broker.delayed_queue_name(), "my_queue:delayed");
        assert_eq!(broker.cancel_channel(), "my_queue:cancel");
    }

    #[test]
    fn test_broker_accessors() {
        let broker = RedisBroker::new("redis://localhost:6379", "test_queue").unwrap();

        // Test health_checker creation
        let _ = broker.health_checker();

        // Test queue_controller creation
        let _ = broker.queue_controller();

        // Test deduplicator creation
        let _ = broker.deduplicator();

        // Test client access
        let _ = broker.client();
    }

    #[test]
    fn test_invalid_redis_url() {
        let result = RedisBroker::new("invalid://not-a-redis-url", "test_queue");
        assert!(result.is_err());
    }

    #[test]
    fn test_cleanup_dlq_age_logic() {
        // Verify the age-filtering logic: only items older than cutoff should be removed.
        // We test the filtering math rather than full async Redis integration.
        use chrono::{Duration, Utc};

        let now = Utc::now();
        let old_created = now - Duration::seconds(3700); // older than 1 hour
        let new_created = now - Duration::seconds(30); // very recent

        let cutoff = now - Duration::seconds(3600); // 1-hour max age

        // Old task should be removed
        assert!(
            old_created < cutoff,
            "Old task created before cutoff should be removed"
        );
        // Recent task should be kept
        assert!(
            new_created >= cutoff,
            "Recent task created after cutoff should be kept"
        );
    }
}
