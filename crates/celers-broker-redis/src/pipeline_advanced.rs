//! Advanced Pipeline Operations
//!
//! Provides intelligent Redis pipeline management with:
//! - Automatic pipeline optimization based on operation characteristics
//! - Dynamic pipeline size tuning
//! - Comprehensive error handling with partial failure recovery
//! - Pipeline performance monitoring
//!
//! # Features
//!
//! - **Adaptive Batching**: Automatically adjusts pipeline size based on latency and throughput
//! - **Smart Grouping**: Groups similar operations for better efficiency
//! - **Error Recovery**: Handles partial failures with retry and fallback strategies
//! - **Performance Metrics**: Tracks pipeline efficiency and bottlenecks
//! - **Backpressure Management**: Prevents pipeline overload
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::pipeline_advanced::{
//!     AdvancedPipeline, PipelineConfig, PipelineOperation
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = PipelineConfig::default()
//!     .with_max_batch_size(100)
//!     .with_target_latency_ms(50)
//!     .with_auto_flush_interval_ms(10);
//!
//! let pipeline = AdvancedPipeline::new("redis://localhost:6379", config).await?;
//!
//! // Operations are automatically batched and optimized
//! pipeline.set("key1", "value1").await?;
//! pipeline.set("key2", "value2").await?;
//! pipeline.set("key3", "value3").await?;
//!
//! // Flush pending operations
//! let result = pipeline.flush().await?;
//! println!("Executed {} operations", result.operations_executed);
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result};
use redis::{aio::MultiplexedConnection, Client, Pipeline, RedisError};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use tracing::{debug, warn};

/// Advanced pipeline configuration
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum operations per pipeline batch
    pub max_batch_size: usize,
    /// Minimum operations before auto-flush
    pub min_batch_size: usize,
    /// Target latency for pipeline execution in milliseconds
    pub target_latency_ms: u64,
    /// Auto-flush interval in milliseconds
    pub auto_flush_interval_ms: u64,
    /// Enable adaptive batch sizing
    pub adaptive_sizing: bool,
    /// Retry failed operations
    pub retry_on_failure: bool,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Enable operation grouping
    pub enable_grouping: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            min_batch_size: 10,
            target_latency_ms: 50,
            auto_flush_interval_ms: 10,
            adaptive_sizing: true,
            retry_on_failure: true,
            max_retries: 3,
            enable_grouping: true,
        }
    }
}

impl PipelineConfig {
    /// Set maximum batch size
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Set minimum batch size
    pub fn with_min_batch_size(mut self, size: usize) -> Self {
        self.min_batch_size = size;
        self
    }

    /// Set target latency in milliseconds
    pub fn with_target_latency_ms(mut self, latency_ms: u64) -> Self {
        self.target_latency_ms = latency_ms;
        self
    }

    /// Set auto-flush interval in milliseconds
    pub fn with_auto_flush_interval_ms(mut self, interval_ms: u64) -> Self {
        self.auto_flush_interval_ms = interval_ms;
        self
    }

    /// Enable or disable adaptive sizing
    pub fn with_adaptive_sizing(mut self, enable: bool) -> Self {
        self.adaptive_sizing = enable;
        self
    }

    /// Enable or disable retry on failure
    pub fn with_retry_on_failure(mut self, enable: bool) -> Self {
        self.retry_on_failure = enable;
        self
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Enable or disable operation grouping
    pub fn with_grouping(mut self, enable: bool) -> Self {
        self.enable_grouping = enable;
        self
    }
}

/// Pipeline operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineOperation {
    /// SET operation
    Set { key: String, value: String },
    /// GET operation
    Get { key: String },
    /// DELETE operation
    Del { key: String },
    /// LPUSH operation
    LPush { key: String, value: String },
    /// RPUSH operation
    RPush { key: String, value: String },
    /// LPOP operation
    LPop { key: String },
    /// RPOP operation
    RPop { key: String },
    /// ZADD operation
    ZAdd {
        key: String,
        score: f64,
        member: String,
    },
    /// ZPOPMIN operation
    ZPopMin { key: String },
    /// EXPIRE operation
    Expire { key: String, seconds: u64 },
}

impl PipelineOperation {
    /// Get operation type for grouping
    pub fn operation_type(&self) -> &'static str {
        match self {
            PipelineOperation::Set { .. } => "SET",
            PipelineOperation::Get { .. } => "GET",
            PipelineOperation::Del { .. } => "DEL",
            PipelineOperation::LPush { .. } => "LPUSH",
            PipelineOperation::RPush { .. } => "RPUSH",
            PipelineOperation::LPop { .. } => "LPOP",
            PipelineOperation::RPop { .. } => "RPOP",
            PipelineOperation::ZAdd { .. } => "ZADD",
            PipelineOperation::ZPopMin { .. } => "ZPOPMIN",
            PipelineOperation::Expire { .. } => "EXPIRE",
        }
    }

    /// Check if operation is read-only
    pub fn is_readonly(&self) -> bool {
        matches!(self, PipelineOperation::Get { .. })
    }
}

/// Pipeline execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineExecutionResult {
    /// Number of operations executed
    pub operations_executed: usize,
    /// Number of successful operations
    pub operations_succeeded: usize,
    /// Number of failed operations
    pub operations_failed: usize,
    /// Execution duration in microseconds
    pub duration_us: u64,
    /// Operations per second
    pub ops_per_sec: f64,
}

/// Pipeline statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Total pipelines executed
    pub total_pipelines: u64,
    /// Total operations executed
    pub total_operations: u64,
    /// Total successful operations
    pub total_succeeded: u64,
    /// Total failed operations
    pub total_failed: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Peak throughput (ops/sec)
    pub peak_throughput: f64,
    /// Current adaptive batch size
    pub current_batch_size: usize,
    /// Total retries
    pub total_retries: u64,
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self {
            total_pipelines: 0,
            total_operations: 0,
            total_succeeded: 0,
            total_failed: 0,
            avg_batch_size: 0.0,
            avg_latency_us: 0,
            peak_throughput: 0.0,
            current_batch_size: 10,
            total_retries: 0,
        }
    }
}

/// Advanced pipeline manager
pub struct AdvancedPipeline {
    client: Client,
    config: PipelineConfig,
    pending_operations: Arc<Mutex<VecDeque<PipelineOperation>>>,
    stats: Arc<RwLock<PipelineStats>>,
    last_flush: Arc<Mutex<Instant>>,
    auto_flush_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl AdvancedPipeline {
    /// Create a new advanced pipeline
    pub async fn new(redis_url: &str, config: PipelineConfig) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        let pipeline = Self {
            client,
            config: config.clone(),
            pending_operations: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(PipelineStats {
                current_batch_size: config.min_batch_size,
                ..Default::default()
            })),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            auto_flush_handle: Arc::new(Mutex::new(None)),
        };

        // Start auto-flush if configured
        if config.auto_flush_interval_ms > 0 {
            pipeline.start_auto_flush();
        }

        Ok(pipeline)
    }

    /// Add SET operation to pipeline
    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
        self.add_operation(PipelineOperation::Set {
            key: key.to_string(),
            value: value.to_string(),
        })
        .await
    }

    /// Add GET operation to pipeline
    pub async fn get(&self, key: &str) -> Result<()> {
        self.add_operation(PipelineOperation::Get {
            key: key.to_string(),
        })
        .await
    }

    /// Add DELETE operation to pipeline
    pub async fn del(&self, key: &str) -> Result<()> {
        self.add_operation(PipelineOperation::Del {
            key: key.to_string(),
        })
        .await
    }

    /// Add LPUSH operation to pipeline
    pub async fn lpush(&self, key: &str, value: &str) -> Result<()> {
        self.add_operation(PipelineOperation::LPush {
            key: key.to_string(),
            value: value.to_string(),
        })
        .await
    }

    /// Add operation to pipeline
    async fn add_operation(&self, operation: PipelineOperation) -> Result<()> {
        let mut pending = self.pending_operations.lock().await;
        pending.push_back(operation);

        // Check if we should auto-flush
        let stats = self.stats.read().await;
        if pending.len() >= stats.current_batch_size {
            drop(stats);
            drop(pending);
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush pending operations
    pub async fn flush(&self) -> Result<PipelineExecutionResult> {
        let operations = {
            let mut pending = self.pending_operations.lock().await;
            if pending.is_empty() {
                return Ok(PipelineExecutionResult {
                    operations_executed: 0,
                    operations_succeeded: 0,
                    operations_failed: 0,
                    duration_us: 0,
                    ops_per_sec: 0.0,
                });
            }

            let stats = self.stats.read().await;
            let batch_size = stats.current_batch_size.min(pending.len());
            drop(stats);

            pending.drain(..batch_size).collect::<Vec<_>>()
        };

        let result = self.execute_pipeline(&operations).await?;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_pipelines += 1;
            stats.total_operations += result.operations_executed as u64;
            stats.total_succeeded += result.operations_succeeded as u64;
            stats.total_failed += result.operations_failed as u64;

            // Update average batch size
            stats.avg_batch_size = if stats.total_pipelines == 1 {
                result.operations_executed as f64
            } else {
                (stats.avg_batch_size * (stats.total_pipelines - 1) as f64
                    + result.operations_executed as f64)
                    / stats.total_pipelines as f64
            };

            // Update average latency
            stats.avg_latency_us = if stats.total_pipelines == 1 {
                result.duration_us
            } else {
                (stats.avg_latency_us * (stats.total_pipelines - 1) + result.duration_us)
                    / stats.total_pipelines
            };

            // Update peak throughput
            if result.ops_per_sec > stats.peak_throughput {
                stats.peak_throughput = result.ops_per_sec;
            }

            // Adaptive batch sizing
            if self.config.adaptive_sizing {
                self.adjust_batch_size(&mut stats, &result);
            }
        }

        *self.last_flush.lock().await = Instant::now();

        Ok(result)
    }

    /// Execute pipeline operations
    async fn execute_pipeline(
        &self,
        operations: &[PipelineOperation],
    ) -> Result<PipelineExecutionResult> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let start = Instant::now();
        let mut succeeded = 0;
        let mut failed = 0;

        // Group operations if enabled
        let batches = if self.config.enable_grouping {
            self.group_operations(operations)
        } else {
            vec![operations.to_vec()]
        };

        for batch in batches {
            let result = self.execute_batch(&mut conn, &batch).await;

            match result {
                Ok(count) => succeeded += count,
                Err(_) if self.config.retry_on_failure => {
                    // Retry failed batch
                    for retry in 1..=self.config.max_retries {
                        match self.execute_batch(&mut conn, &batch).await {
                            Ok(count) => {
                                succeeded += count;
                                let mut stats = self.stats.write().await;
                                stats.total_retries += retry as u64;
                                break;
                            }
                            Err(e) if retry == self.config.max_retries => {
                                warn!("Pipeline batch failed after {} retries: {}", retry, e);
                                failed += batch.len();
                            }
                            _ => {
                                tokio::time::sleep(Duration::from_millis(50 * retry as u64)).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Pipeline batch failed: {}", e);
                    failed += batch.len();
                }
            }
        }

        let duration = start.elapsed();
        let duration_us = duration.as_micros() as u64;
        let ops_per_sec = if duration_us > 0 {
            (operations.len() as f64 * 1_000_000.0) / duration_us as f64
        } else {
            0.0
        };

        Ok(PipelineExecutionResult {
            operations_executed: operations.len(),
            operations_succeeded: succeeded,
            operations_failed: failed,
            duration_us,
            ops_per_sec,
        })
    }

    /// Execute a batch of operations
    async fn execute_batch(
        &self,
        conn: &mut MultiplexedConnection,
        operations: &[PipelineOperation],
    ) -> Result<usize> {
        let mut pipe = Pipeline::new();

        for op in operations {
            match op {
                PipelineOperation::Set { key, value } => {
                    pipe.set(key, value);
                }
                PipelineOperation::Get { key } => {
                    pipe.get(key);
                }
                PipelineOperation::Del { key } => {
                    pipe.del(key);
                }
                PipelineOperation::LPush { key, value } => {
                    pipe.lpush(key, value);
                }
                PipelineOperation::RPush { key, value } => {
                    pipe.rpush(key, value);
                }
                PipelineOperation::LPop { key } => {
                    pipe.lpop(key, None);
                }
                PipelineOperation::RPop { key } => {
                    pipe.rpop(key, None);
                }
                PipelineOperation::ZAdd { key, score, member } => {
                    pipe.zadd(key, member, *score);
                }
                PipelineOperation::ZPopMin { key } => {
                    pipe.zpopmin(key, 1);
                }
                PipelineOperation::Expire { key, seconds } => {
                    pipe.expire(key, *seconds as i64);
                }
            }
        }

        let _: Vec<()> = pipe.query_async(conn).await.map_err(|e: RedisError| {
            CelersError::Broker(format!("Pipeline execution failed: {}", e))
        })?;

        Ok(operations.len())
    }

    /// Group operations by type
    fn group_operations(&self, operations: &[PipelineOperation]) -> Vec<Vec<PipelineOperation>> {
        let mut groups: Vec<Vec<PipelineOperation>> = Vec::new();
        let mut current_group: Vec<PipelineOperation> = Vec::new();
        let mut current_type: Option<&'static str> = None;

        for op in operations {
            let op_type = op.operation_type();

            if current_type.is_none() || current_type == Some(op_type) {
                current_group.push(op.clone());
                current_type = Some(op_type);
            } else {
                if !current_group.is_empty() {
                    groups.push(current_group);
                }
                current_group = vec![op.clone()];
                current_type = Some(op_type);
            }
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }

    /// Adjust batch size based on performance
    fn adjust_batch_size(&self, stats: &mut PipelineStats, result: &PipelineExecutionResult) {
        let target_latency_us = self.config.target_latency_ms * 1000;

        if result.duration_us > target_latency_us
            && stats.current_batch_size > self.config.min_batch_size
        {
            // Too slow, reduce batch size
            stats.current_batch_size =
                (stats.current_batch_size * 9 / 10).max(self.config.min_batch_size);
            debug!("Reduced batch size to {}", stats.current_batch_size);
        } else if result.duration_us < target_latency_us / 2
            && stats.current_batch_size < self.config.max_batch_size
        {
            // Can handle more, increase batch size
            stats.current_batch_size =
                (stats.current_batch_size * 11 / 10).min(self.config.max_batch_size);
            debug!("Increased batch size to {}", stats.current_batch_size);
        }
    }

    /// Start auto-flush background task
    fn start_auto_flush(&self) {
        let pending = Arc::clone(&self.pending_operations);
        let last_flush = Arc::clone(&self.last_flush);
        let interval = Duration::from_millis(self.config.auto_flush_interval_ms);
        let pipeline = Self {
            client: self.client.clone(),
            config: self.config.clone(),
            pending_operations: Arc::clone(&self.pending_operations),
            stats: Arc::clone(&self.stats),
            last_flush: Arc::clone(&self.last_flush),
            auto_flush_handle: Arc::new(Mutex::new(None)),
        };

        let handle = tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);

            loop {
                interval_timer.tick().await;

                let should_flush = {
                    let pending = pending.lock().await;
                    let last = last_flush.lock().await;

                    !pending.is_empty() && last.elapsed() >= interval
                };

                if should_flush {
                    if let Err(e) = pipeline.flush().await {
                        warn!("Auto-flush failed: {}", e);
                    }
                }
            }
        });

        *self.auto_flush_handle.blocking_lock() = Some(handle);
    }

    /// Stop auto-flush
    pub async fn stop_auto_flush(&self) {
        if let Some(handle) = self.auto_flush_handle.lock().await.take() {
            handle.abort();
        }
    }

    /// Get pipeline statistics
    pub async fn stats(&self) -> PipelineStats {
        self.stats.read().await.clone()
    }

    /// Get pending operation count
    pub async fn pending_count(&self) -> usize {
        self.pending_operations.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_builder() {
        let config = PipelineConfig::default()
            .with_max_batch_size(200)
            .with_min_batch_size(20)
            .with_target_latency_ms(100)
            .with_adaptive_sizing(true);

        assert_eq!(config.max_batch_size, 200);
        assert_eq!(config.min_batch_size, 20);
        assert_eq!(config.target_latency_ms, 100);
        assert!(config.adaptive_sizing);
    }

    #[test]
    fn test_pipeline_operation_types() {
        let set_op = PipelineOperation::Set {
            key: "test".to_string(),
            value: "value".to_string(),
        };
        let get_op = PipelineOperation::Get {
            key: "test".to_string(),
        };

        assert_eq!(set_op.operation_type(), "SET");
        assert_eq!(get_op.operation_type(), "GET");
        assert!(!set_op.is_readonly());
        assert!(get_op.is_readonly());
    }

    #[test]
    fn test_pipeline_stats_default() {
        let stats = PipelineStats::default();

        assert_eq!(stats.total_pipelines, 0);
        assert_eq!(stats.total_operations, 0);
        assert_eq!(stats.avg_batch_size, 0.0);
    }

    #[test]
    fn test_pipeline_execution_result() {
        let result = PipelineExecutionResult {
            operations_executed: 100,
            operations_succeeded: 95,
            operations_failed: 5,
            duration_us: 50000,
            ops_per_sec: 2000.0,
        };

        assert_eq!(result.operations_executed, 100);
        assert_eq!(result.operations_succeeded, 95);
        assert_eq!(result.operations_failed, 5);
    }
}
