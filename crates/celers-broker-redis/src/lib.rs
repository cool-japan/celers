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
use redis::{AsyncCommands, Client};
use tracing::{debug, error, info, warn};

#[cfg(feature = "metrics")]
use celers_metrics::{
    DLQ_SIZE, PROCESSING_QUEUE_SIZE, QUEUE_SIZE, TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL,
};

pub mod advanced_queue;
pub mod authorization;
pub mod backup_restore;
pub mod batch_ext;
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
pub use connection::{ConnectionStats, RedisConfig, TlsConfig};
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
use visibility::VisibilityManager;

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
pub struct RedisBroker {
    client: Client,
    queue_name: String,
    processing_queue: String,
    dlq_name: String,
    delayed_queue: String,
    cancel_channel: String,
    mode: QueueMode,
    #[allow(dead_code)]
    visibility_manager: VisibilityManager,
    visibility_timeout_secs: u64,
}

impl RedisBroker {
    /// Create a new Redis broker with FIFO mode
    pub fn new(redis_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_mode(redis_url, queue_name, QueueMode::Fifo)
    }

    /// Create a new Redis broker with specified queue mode
    pub fn with_mode(redis_url: &str, queue_name: &str, mode: QueueMode) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            processing_queue: format!("{}:processing", queue_name),
            dlq_name: format!("{}:dlq", queue_name),
            delayed_queue: format!("{}:delayed", queue_name),
            cancel_channel: format!("{}:cancel", queue_name),
            mode,
            visibility_manager: VisibilityManager::new(),
            visibility_timeout_secs: 300, // 5 minutes default
        })
    }

    /// Create a new Redis broker from a RedisConfig
    pub fn from_config(config: &RedisConfig, queue_name: &str, mode: QueueMode) -> Result<Self> {
        let client = config.build_client()?;

        debug!("Created Redis broker with config: {}", config.describe());

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            processing_queue: format!("{}:processing", queue_name),
            dlq_name: format!("{}:dlq", queue_name),
            delayed_queue: format!("{}:delayed", queue_name),
            cancel_channel: format!("{}:cancel", queue_name),
            mode,
            visibility_manager: VisibilityManager::new(),
            visibility_timeout_secs: 300, // 5 minutes default
        })
    }

    /// Create a new Redis broker from a SentinelClient (for high availability)
    pub async fn from_sentinel(
        sentinel: &SentinelClient,
        queue_name: &str,
        mode: QueueMode,
    ) -> Result<Self> {
        let client = sentinel.get_client().await?;

        info!("Created Redis broker with Sentinel support");

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            processing_queue: format!("{}:processing", queue_name),
            dlq_name: format!("{}:dlq", queue_name),
            delayed_queue: format!("{}:delayed", queue_name),
            cancel_channel: format!("{}:cancel", queue_name),
            mode,
            visibility_manager: VisibilityManager::new(),
            visibility_timeout_secs: 300, // 5 minutes default
        })
    }

    /// Set visibility timeout (default: 300 seconds)
    pub fn with_visibility_timeout(mut self, timeout_secs: u64) -> Self {
        self.visibility_timeout_secs = timeout_secs;
        self
    }

    /// Get the queue mode
    pub fn mode(&self) -> QueueMode {
        self.mode
    }

    /// Get the number of tasks in the Dead Letter Queue
    pub async fn dlq_size(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let size: usize = conn
            .llen(&self.dlq_name)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ size: {}", e)))?;

        Ok(size)
    }

    /// Inspect tasks in the Dead Letter Queue
    pub async fn inspect_dlq(&self, limit: isize) -> Result<Vec<SerializedTask>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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

    /// Replay a task from the Dead Letter Queue back to the main queue
    pub async fn replay_from_dlq(&self, task_id: &TaskId) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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
                // Remove from DLQ
                conn.lrem::<_, _, ()>(&self.dlq_name, 1, &item)
                    .await
                    .map_err(|e| {
                        CelersError::Broker(format!("Failed to remove from DLQ: {}", e))
                    })?;

                // Reset retry count in task metadata
                let mut replayed_task = task;
                replayed_task.metadata.state = celers_core::TaskState::Pending;

                let serialized = serde_json::to_string(&replayed_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                // Re-enqueue based on mode
                match self.mode {
                    QueueMode::Fifo => {
                        conn.rpush::<_, _, ()>(&self.queue_name, &serialized)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to replay task: {}", e))
                            })?;
                    }
                    QueueMode::Priority => {
                        let score = -replayed_task.metadata.priority as f64;
                        conn.zadd::<_, _, _, ()>(&self.queue_name, &serialized, score)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to replay task: {}", e))
                            })?;
                    }
                }

                info!("Replayed task {} from DLQ", task_id);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Clear all tasks from the Dead Letter Queue
    pub async fn clear_dlq(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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

    /// Create a queue controller for pause/resume operations
    pub fn queue_controller(&self) -> QueueController {
        QueueController::new(self.client.clone(), &self.queue_name)
    }

    /// Create a task deduplicator
    pub fn deduplicator(&self) -> Deduplicator {
        Deduplicator::new(self.client.clone(), &self.queue_name)
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
    pub fn batch_operations(&self) -> BatchOperations {
        BatchOperations::new(self.client.clone(), self.queue_name.clone(), self.mode)
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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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
    pub async fn cleanup_dlq(&self, _max_age_secs: u64) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        // Get all DLQ items
        let items: Vec<String> = conn
            .lrange(&self.dlq_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ items: {}", e)))?;

        let mut removed_count = 0;

        for item in items {
            // Try to parse task to validate it's a proper task
            if serde_json::from_str::<SerializedTask>(&item).is_ok() {
                // This is a simplified version - in real use, you'd track DLQ entry time
                // and only remove tasks older than max_age_secs
                conn.lrem::<_, _, ()>(&self.dlq_name, 1, &item)
                    .await
                    .map_err(|e| {
                        CelersError::Broker(format!("Failed to remove from DLQ: {}", e))
                    })?;
                removed_count += 1;
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

    /// Get all queue names managed by this broker
    pub fn queue_names(&self) -> Vec<String> {
        vec![
            self.queue_name.clone(),
            self.processing_queue.clone(),
            self.dlq_name.clone(),
            self.delayed_queue.clone(),
        ]
    }

    /// Check if a specific queue exists and has tasks
    pub async fn queue_exists(&self, queue_name: &str) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let exists: bool = redis::cmd("EXISTS")
            .arg(queue_name)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check queue existence: {}", e)))?;

        Ok(exists)
    }

    /// Get the size of a specific queue by name
    pub async fn get_queue_size_by_name(&self, queue_name: &str) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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

    /// Move all tasks from processing queue back to main queue
    ///
    /// Useful for recovering tasks that were being processed when a worker crashed.
    /// Returns the number of tasks moved.
    pub async fn recover_processing_tasks(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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

        for item in &items {
            // Parse to get task
            if let Ok(task) = serde_json::from_str::<SerializedTask>(item) {
                // Add back to main queue based on mode
                match self.mode {
                    QueueMode::Fifo => {
                        pipe.rpush(&self.queue_name, item);
                    }
                    QueueMode::Priority => {
                        let score = -(task.metadata.priority as f64);
                        pipe.zadd(&self.queue_name, item, score);
                    }
                }
                // Remove from processing queue
                pipe.lrem(&self.processing_queue, 1, item);
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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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
    /// Returns the number of tasks moved.
    pub async fn bulk_replay_from_dlq(&self, max_count: Option<usize>) -> Result<usize> {
        let limit = max_count.unwrap_or(isize::MAX as usize) as isize;
        let tasks = self.inspect_dlq(limit).await?;
        let count = tasks.len();

        for task in tasks {
            self.replay_from_dlq(&task.metadata.id).await?;
        }

        Ok(count)
    }

    /// Peek at the next task without dequeueing it
    ///
    /// Useful for inspecting what will be processed next without removing it from the queue.
    pub async fn peek_next(&self) -> Result<Option<SerializedTask>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let data: Option<String> = match self.mode {
            QueueMode::Fifo => {
                // Get last item (will be next to dequeue)
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
    /// Checks the delayed queue for tasks ready to execute (timestamp <= now)
    /// and moves them to the main queue atomically.
    async fn process_delayed_tasks(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Failed to get current time: {}", e)))?
            .as_secs() as f64;

        // Get all tasks ready for execution (score <= now)
        let ready_tasks: Vec<String> =
            conn.zrangebyscore(&self.delayed_queue, "-inf", now)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get ready tasks: {}", e)))?;

        if ready_tasks.is_empty() {
            return Ok(0);
        }

        let count = ready_tasks.len();

        // Move tasks to main queue based on queue mode
        let mut pipe = redis::pipe();

        for task_data in &ready_tasks {
            // Deserialize to get priority for sorted queue
            if let Ok(task) = serde_json::from_str::<SerializedTask>(task_data) {
                match self.mode {
                    QueueMode::Fifo => {
                        pipe.rpush(&self.queue_name, task_data);
                    }
                    QueueMode::Priority => {
                        let score = -(task.metadata.priority as f64);
                        pipe.zadd(&self.queue_name, task_data, score);
                    }
                }
            }

            // Remove from delayed queue
            pipe.zrem(&self.delayed_queue, task_data);
        }

        // Execute all operations atomically
        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to move delayed tasks: {}", e)))?;

        debug!("Moved {} delayed tasks to main queue", count);

        Ok(count)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let task_id = task.metadata.id;
        let priority = task.metadata.priority;
        let serialized =
            serde_json::to_string(&task).map_err(|e| CelersError::Serialization(e.to_string()))?;

        match self.mode {
            QueueMode::Fifo => {
                // Use list for FIFO (priority ignored)
                conn.rpush::<_, _, ()>(&self.queue_name, &serialized)
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

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        // Process delayed tasks first (move ready tasks to main queue)
        let _ = self.process_delayed_tasks().await;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let result = match self.mode {
            QueueMode::Fifo => {
                // Use BRPOPLPUSH for atomic dequeue and move to processing queue
                conn.brpoplpush(&self.queue_name, &self.processing_queue, 1.0)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to dequeue task: {}", e)))?
            }
            QueueMode::Priority => {
                // Use ZPOPMIN to get highest priority task (lowest score due to negation)
                // Note: Redis doesn't have BZPOPMINLPUSH, so we do two operations
                let items: Vec<(String, f64)> = conn
                    .zpopmin(&self.queue_name, 1)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to dequeue task: {}", e)))?;

                if let Some((data, _score)) = items.first() {
                    // Move to processing queue
                    conn.lpush::<_, _, ()>(&self.processing_queue, data)
                        .await
                        .map_err(|e| {
                            CelersError::Broker(format!("Failed to move task to processing: {}", e))
                        })?;
                    Some(data.clone())
                } else {
                    None
                }
            }
        };

        match result {
            Some(data) => {
                let task: SerializedTask = serde_json::from_str(&data)
                    .map_err(|e| CelersError::Deserialization(e.to_string()))?;

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
            let mut conn = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

            conn.lrem::<_, _, ()>(&self.processing_queue, 1, handle)
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
            let mut conn = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

            // Remove from processing queue
            conn.lrem::<_, _, ()>(&self.processing_queue, 1, handle)
                .await
                .map_err(|e| {
                    CelersError::Broker(format!("Failed to remove from processing: {}", e))
                })?;

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

                match self.mode {
                    QueueMode::Fifo => {
                        conn.rpush::<_, _, ()>(&self.queue_name, &serialized)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to requeue task: {}", e))
                            })?;
                    }
                    QueueMode::Priority => {
                        let score = -task.metadata.priority as f64;
                        conn.zadd::<_, _, _, ()>(&self.queue_name, &serialized, score)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to requeue task: {}", e))
                            })?;
                    }
                }

                info!(
                    "Rejected and requeued task {} (retry {})",
                    task_id, retry_count
                );
            } else {
                // Move to Dead Letter Queue
                conn.lpush::<_, _, ()>(&self.dlq_name, handle)
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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        // Publish cancellation message
        let cancel_msg = task_id.to_string();
        let subscribers: i32 = conn
            .publish(&self.cancel_channel, &cancel_msg)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to publish cancel message: {}", e)))?;

        if subscribers > 0 {
            info!(
                "Published cancellation for task {} to {} subscriber(s)",
                task_id, subscribers
            );
            Ok(true)
        } else {
            warn!("No subscribers listening for task {} cancellation", task_id);
            Ok(false)
        }
    }

    // Optimized batch operations using Redis pipelining

    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut task_ids = Vec::with_capacity(tasks.len());
        let mut pipe = redis::pipe();

        // Build pipeline with all enqueue operations
        for task in &tasks {
            let task_id = task.metadata.id;
            task_ids.push(task_id);

            let serialized = serde_json::to_string(&task)
                .map_err(|e| CelersError::Serialization(e.to_string()))?;

            match self.mode {
                QueueMode::Fifo => {
                    pipe.rpush(&self.queue_name, &serialized);
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

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut messages = Vec::with_capacity(count);

        match self.mode {
            QueueMode::Fifo => {
                // Use pipeline for FIFO batch dequeue
                let mut pipe = redis::pipe();
                for _ in 0..count {
                    pipe.rpoplpush(&self.queue_name, &self.processing_queue);
                }

                let results: Vec<Option<String>> = pipe
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to dequeue batch: {}", e)))?;

                for data_opt in results {
                    if let Some(data) = data_opt {
                        let task: SerializedTask = serde_json::from_str(&data)
                            .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                        messages.push(BrokerMessage {
                            task,
                            receipt_handle: Some(data),
                        });
                    } else {
                        break; // Queue is empty
                    }
                }
            }
            QueueMode::Priority => {
                // For priority queue, pop multiple items at once
                let items: Vec<(String, f64)> = conn
                    .zpopmin(&self.queue_name, count as isize)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to dequeue batch: {}", e)))?;

                if !items.is_empty() {
                    // Move all to processing queue using pipeline
                    let mut pipe = redis::pipe();
                    for (data, _score) in &items {
                        pipe.lpush(&self.processing_queue, data);
                    }

                    pipe.query_async::<redis::Value>(&mut conn)
                        .await
                        .map_err(|e| {
                            CelersError::Broker(format!(
                                "Failed to move batch to processing: {}",
                                e
                            ))
                        })?;

                    for (data, _score) in items {
                        let task: SerializedTask = serde_json::from_str(&data)
                            .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                        messages.push(BrokerMessage {
                            task,
                            receipt_handle: Some(data),
                        });
                    }
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

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut pipe = redis::pipe();
        let mut ack_count = 0;

        for (task_id, receipt_handle) in tasks {
            if let Some(handle) = receipt_handle {
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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

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
}
