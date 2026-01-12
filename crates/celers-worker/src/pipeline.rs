//! Task execution pipeline for improved throughput
//!
//! This module provides a pipeline for task execution that allows overlapping
//! different stages of task processing:
//! - Fetching tasks from the broker
//! - Deserializing task payloads
//! - Executing task logic
//! - Serializing results
//! - Storing results in the backend
//!
//! By pipelining these stages, we can improve throughput by reducing idle time.
//!
//! # Example
//!
//! ```
//! use celers_worker::pipeline::{Pipeline, PipelineConfig, PipelineStage};
//!
//! let config = PipelineConfig::default()
//!     .with_fetch_buffer_size(10)
//!     .with_execute_buffer_size(5)
//!     .with_result_buffer_size(10);
//!
//! // Pipeline stages:
//! // Fetch -> Deserialize -> Execute -> Serialize -> Store
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

/// Pipeline stage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStage {
    /// Fetch stage - retrieving tasks from broker
    Fetch,
    /// Deserialize stage - converting raw data to task objects
    Deserialize,
    /// Execute stage - running task logic
    Execute,
    /// Serialize stage - converting results to raw data
    Serialize,
    /// Store stage - saving results to backend
    Store,
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fetch => write!(f, "Fetch"),
            Self::Deserialize => write!(f, "Deserialize"),
            Self::Execute => write!(f, "Execute"),
            Self::Serialize => write!(f, "Serialize"),
            Self::Store => write!(f, "Store"),
        }
    }
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Buffer size between fetch and deserialize stages
    fetch_buffer_size: usize,
    /// Buffer size between deserialize and execute stages
    execute_buffer_size: usize,
    /// Buffer size between execute and serialize stages
    serialize_buffer_size: usize,
    /// Buffer size between serialize and store stages
    result_buffer_size: usize,
    /// Maximum concurrent fetch operations
    max_concurrent_fetch: usize,
    /// Maximum concurrent deserialize operations
    max_concurrent_deserialize: usize,
    /// Maximum concurrent execute operations
    max_concurrent_execute: usize,
    /// Maximum concurrent serialize operations
    max_concurrent_serialize: usize,
    /// Maximum concurrent store operations
    max_concurrent_store: usize,
    /// Enable pipeline statistics
    enable_stats: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            fetch_buffer_size: 10,
            execute_buffer_size: 5,
            serialize_buffer_size: 10,
            result_buffer_size: 10,
            max_concurrent_fetch: 5,
            max_concurrent_deserialize: 10,
            max_concurrent_execute: 10,
            max_concurrent_serialize: 10,
            max_concurrent_store: 5,
            enable_stats: true,
        }
    }
}

impl PipelineConfig {
    /// Create a new pipeline configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set fetch buffer size
    pub fn with_fetch_buffer_size(mut self, size: usize) -> Self {
        self.fetch_buffer_size = size;
        self
    }

    /// Set execute buffer size
    pub fn with_execute_buffer_size(mut self, size: usize) -> Self {
        self.execute_buffer_size = size;
        self
    }

    /// Set serialize buffer size
    pub fn with_serialize_buffer_size(mut self, size: usize) -> Self {
        self.serialize_buffer_size = size;
        self
    }

    /// Set result buffer size
    pub fn with_result_buffer_size(mut self, size: usize) -> Self {
        self.result_buffer_size = size;
        self
    }

    /// Set maximum concurrent fetch operations
    pub fn with_max_concurrent_fetch(mut self, count: usize) -> Self {
        self.max_concurrent_fetch = count;
        self
    }

    /// Set maximum concurrent deserialize operations
    pub fn with_max_concurrent_deserialize(mut self, count: usize) -> Self {
        self.max_concurrent_deserialize = count;
        self
    }

    /// Set maximum concurrent execute operations
    pub fn with_max_concurrent_execute(mut self, count: usize) -> Self {
        self.max_concurrent_execute = count;
        self
    }

    /// Set maximum concurrent serialize operations
    pub fn with_max_concurrent_serialize(mut self, count: usize) -> Self {
        self.max_concurrent_serialize = count;
        self
    }

    /// Set maximum concurrent store operations
    pub fn with_max_concurrent_store(mut self, count: usize) -> Self {
        self.max_concurrent_store = count;
        self
    }

    /// Enable or disable statistics
    pub fn with_stats(mut self, enable: bool) -> Self {
        self.enable_stats = enable;
        self
    }

    /// Get fetch buffer size
    pub fn fetch_buffer_size(&self) -> usize {
        self.fetch_buffer_size
    }

    /// Get execute buffer size
    pub fn execute_buffer_size(&self) -> usize {
        self.execute_buffer_size
    }

    /// Get serialize buffer size
    pub fn serialize_buffer_size(&self) -> usize {
        self.serialize_buffer_size
    }

    /// Get result buffer size
    pub fn result_buffer_size(&self) -> usize {
        self.result_buffer_size
    }

    /// Get max concurrent fetch operations
    pub fn max_concurrent_fetch(&self) -> usize {
        self.max_concurrent_fetch
    }

    /// Get max concurrent deserialize operations
    pub fn max_concurrent_deserialize(&self) -> usize {
        self.max_concurrent_deserialize
    }

    /// Get max concurrent execute operations
    pub fn max_concurrent_execute(&self) -> usize {
        self.max_concurrent_execute
    }

    /// Get max concurrent serialize operations
    pub fn max_concurrent_serialize(&self) -> usize {
        self.max_concurrent_serialize
    }

    /// Get max concurrent store operations
    pub fn max_concurrent_store(&self) -> usize {
        self.max_concurrent_store
    }

    /// Check if stats are enabled
    pub fn is_stats_enabled(&self) -> bool {
        self.enable_stats
    }

    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        self.fetch_buffer_size > 0
            && self.execute_buffer_size > 0
            && self.serialize_buffer_size > 0
            && self.result_buffer_size > 0
            && self.max_concurrent_fetch > 0
            && self.max_concurrent_deserialize > 0
            && self.max_concurrent_execute > 0
            && self.max_concurrent_serialize > 0
            && self.max_concurrent_store > 0
    }

    /// Create a high-throughput configuration
    pub fn high_throughput() -> Self {
        Self {
            fetch_buffer_size: 50,
            execute_buffer_size: 20,
            serialize_buffer_size: 50,
            result_buffer_size: 50,
            max_concurrent_fetch: 20,
            max_concurrent_deserialize: 30,
            max_concurrent_execute: 20,
            max_concurrent_serialize: 30,
            max_concurrent_store: 20,
            enable_stats: true,
        }
    }

    /// Create a low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            fetch_buffer_size: 5,
            execute_buffer_size: 2,
            serialize_buffer_size: 5,
            result_buffer_size: 5,
            max_concurrent_fetch: 2,
            max_concurrent_deserialize: 5,
            max_concurrent_execute: 5,
            max_concurrent_serialize: 5,
            max_concurrent_store: 2,
            enable_stats: false,
        }
    }

    /// Create a balanced configuration
    pub fn balanced() -> Self {
        Self::default()
    }
}

impl fmt::Display for PipelineConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PipelineConfig(fetch_buf={}, exec_buf={}, ser_buf={}, res_buf={}, stats={})",
            self.fetch_buffer_size,
            self.execute_buffer_size,
            self.serialize_buffer_size,
            self.result_buffer_size,
            self.enable_stats
        )
    }
}

/// Pipeline statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Total tasks processed through the pipeline
    total_tasks: u64,
    /// Tasks currently in fetch stage
    in_fetch: u64,
    /// Tasks currently in deserialize stage
    in_deserialize: u64,
    /// Tasks currently in execute stage
    in_execute: u64,
    /// Tasks currently in serialize stage
    in_serialize: u64,
    /// Tasks currently in store stage
    in_store: u64,
    /// Average time spent in fetch stage (microseconds)
    avg_fetch_time_us: u64,
    /// Average time spent in deserialize stage (microseconds)
    avg_deserialize_time_us: u64,
    /// Average time spent in execute stage (microseconds)
    avg_execute_time_us: u64,
    /// Average time spent in serialize stage (microseconds)
    avg_serialize_time_us: u64,
    /// Average time spent in store stage (microseconds)
    avg_store_time_us: u64,
}

impl PipelineStats {
    /// Create new pipeline statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total tasks processed
    pub fn total_tasks(&self) -> u64 {
        self.total_tasks
    }

    /// Get tasks in fetch stage
    pub fn in_fetch(&self) -> u64 {
        self.in_fetch
    }

    /// Get tasks in deserialize stage
    pub fn in_deserialize(&self) -> u64 {
        self.in_deserialize
    }

    /// Get tasks in execute stage
    pub fn in_execute(&self) -> u64 {
        self.in_execute
    }

    /// Get tasks in serialize stage
    pub fn in_serialize(&self) -> u64 {
        self.in_serialize
    }

    /// Get tasks in store stage
    pub fn in_store(&self) -> u64 {
        self.in_store
    }

    /// Get total tasks in pipeline
    pub fn total_in_pipeline(&self) -> u64 {
        self.in_fetch + self.in_deserialize + self.in_execute + self.in_serialize + self.in_store
    }

    /// Get average fetch time in microseconds
    pub fn avg_fetch_time_us(&self) -> u64 {
        self.avg_fetch_time_us
    }

    /// Get average deserialize time in microseconds
    pub fn avg_deserialize_time_us(&self) -> u64 {
        self.avg_deserialize_time_us
    }

    /// Get average execute time in microseconds
    pub fn avg_execute_time_us(&self) -> u64 {
        self.avg_execute_time_us
    }

    /// Get average serialize time in microseconds
    pub fn avg_serialize_time_us(&self) -> u64 {
        self.avg_serialize_time_us
    }

    /// Get average store time in microseconds
    pub fn avg_store_time_us(&self) -> u64 {
        self.avg_store_time_us
    }

    /// Get the bottleneck stage (stage with highest average time)
    pub fn bottleneck_stage(&self) -> PipelineStage {
        let times = [
            (PipelineStage::Fetch, self.avg_fetch_time_us),
            (PipelineStage::Deserialize, self.avg_deserialize_time_us),
            (PipelineStage::Execute, self.avg_execute_time_us),
            (PipelineStage::Serialize, self.avg_serialize_time_us),
            (PipelineStage::Store, self.avg_store_time_us),
        ];

        times
            .iter()
            .max_by_key(|(_, time)| time)
            .map(|(stage, _)| *stage)
            .unwrap_or(PipelineStage::Execute)
    }

    /// Get total pipeline time (sum of all stage times)
    pub fn total_pipeline_time_us(&self) -> u64 {
        self.avg_fetch_time_us
            + self.avg_deserialize_time_us
            + self.avg_execute_time_us
            + self.avg_serialize_time_us
            + self.avg_store_time_us
    }

    /// Calculate pipeline efficiency (1.0 = perfect pipelining)
    /// Lower values indicate more time wasted in serial execution
    pub fn pipeline_efficiency(&self) -> f64 {
        let total_time = self.total_pipeline_time_us();
        if total_time == 0 {
            return 1.0;
        }

        let bottleneck_time = self.bottleneck_stage_time();
        if bottleneck_time == 0 {
            return 1.0;
        }

        bottleneck_time as f64 / total_time as f64
    }

    /// Get the time of the bottleneck stage
    fn bottleneck_stage_time(&self) -> u64 {
        match self.bottleneck_stage() {
            PipelineStage::Fetch => self.avg_fetch_time_us,
            PipelineStage::Deserialize => self.avg_deserialize_time_us,
            PipelineStage::Execute => self.avg_execute_time_us,
            PipelineStage::Serialize => self.avg_serialize_time_us,
            PipelineStage::Store => self.avg_store_time_us,
        }
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for PipelineStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PipelineStats(total={}, in_pipeline={}, bottleneck={}, efficiency={:.2}%)",
            self.total_tasks,
            self.total_in_pipeline(),
            self.bottleneck_stage(),
            self.pipeline_efficiency() * 100.0
        )
    }
}

/// Pipeline handle for managing pipeline execution
pub struct Pipeline {
    config: PipelineConfig,
    stats: Arc<tokio::sync::RwLock<PipelineStats>>,
    fetch_semaphore: Arc<Semaphore>,
    deserialize_semaphore: Arc<Semaphore>,
    execute_semaphore: Arc<Semaphore>,
    serialize_semaphore: Arc<Semaphore>,
    store_semaphore: Arc<Semaphore>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            fetch_semaphore: Arc::new(Semaphore::new(config.max_concurrent_fetch)),
            deserialize_semaphore: Arc::new(Semaphore::new(config.max_concurrent_deserialize)),
            execute_semaphore: Arc::new(Semaphore::new(config.max_concurrent_execute)),
            serialize_semaphore: Arc::new(Semaphore::new(config.max_concurrent_serialize)),
            store_semaphore: Arc::new(Semaphore::new(config.max_concurrent_store)),
            stats: Arc::new(tokio::sync::RwLock::new(PipelineStats::default())),
            config,
        }
    }

    /// Get pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Get pipeline statistics
    pub async fn stats(&self) -> PipelineStats {
        self.stats.read().await.clone()
    }

    /// Reset pipeline statistics
    pub async fn reset_stats(&self) {
        self.stats.write().await.reset();
    }

    /// Acquire a permit for a specific stage
    pub async fn acquire_permit(&self, stage: PipelineStage) -> Result<(), String> {
        let semaphore = match stage {
            PipelineStage::Fetch => &self.fetch_semaphore,
            PipelineStage::Deserialize => &self.deserialize_semaphore,
            PipelineStage::Execute => &self.execute_semaphore,
            PipelineStage::Serialize => &self.serialize_semaphore,
            PipelineStage::Store => &self.store_semaphore,
        };

        match semaphore.acquire().await {
            Ok(_permit) => {
                // Permit will be released when dropped
                debug!("Acquired permit for stage: {}", stage);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to acquire permit for stage {}: {}", stage, e);
                Err(format!("Failed to acquire permit: {}", e))
            }
        }
    }

    /// Get available permits for a stage
    pub fn available_permits(&self, stage: PipelineStage) -> usize {
        let semaphore = match stage {
            PipelineStage::Fetch => &self.fetch_semaphore,
            PipelineStage::Deserialize => &self.deserialize_semaphore,
            PipelineStage::Execute => &self.execute_semaphore,
            PipelineStage::Serialize => &self.serialize_semaphore,
            PipelineStage::Store => &self.store_semaphore,
        };

        semaphore.available_permits()
    }

    /// Check if a stage is at capacity
    pub fn is_stage_full(&self, stage: PipelineStage) -> bool {
        self.available_permits(stage) == 0
    }

    /// Get the least busy stage (most available permits)
    pub fn least_busy_stage(&self) -> PipelineStage {
        let stages = [
            (
                PipelineStage::Fetch,
                self.available_permits(PipelineStage::Fetch),
            ),
            (
                PipelineStage::Deserialize,
                self.available_permits(PipelineStage::Deserialize),
            ),
            (
                PipelineStage::Execute,
                self.available_permits(PipelineStage::Execute),
            ),
            (
                PipelineStage::Serialize,
                self.available_permits(PipelineStage::Serialize),
            ),
            (
                PipelineStage::Store,
                self.available_permits(PipelineStage::Store),
            ),
        ];

        stages
            .iter()
            .max_by_key(|(_, permits)| permits)
            .map(|(stage, _)| *stage)
            .unwrap_or(PipelineStage::Execute)
    }
}

impl fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipeline")
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_stage_display() {
        assert_eq!(format!("{}", PipelineStage::Fetch), "Fetch");
        assert_eq!(format!("{}", PipelineStage::Execute), "Execute");
    }

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.fetch_buffer_size(), 10);
        assert_eq!(config.execute_buffer_size(), 5);
        assert!(config.is_valid());
    }

    #[test]
    fn test_pipeline_config_builder() {
        let config = PipelineConfig::new()
            .with_fetch_buffer_size(20)
            .with_execute_buffer_size(10)
            .with_max_concurrent_fetch(5);

        assert_eq!(config.fetch_buffer_size(), 20);
        assert_eq!(config.execute_buffer_size(), 10);
        assert_eq!(config.max_concurrent_fetch(), 5);
    }

    #[test]
    fn test_pipeline_config_presets() {
        let high_throughput = PipelineConfig::high_throughput();
        assert!(
            high_throughput.fetch_buffer_size() > PipelineConfig::default().fetch_buffer_size()
        );

        let low_latency = PipelineConfig::low_latency();
        assert!(low_latency.fetch_buffer_size() < PipelineConfig::default().fetch_buffer_size());

        let balanced = PipelineConfig::balanced();
        assert_eq!(
            balanced.fetch_buffer_size(),
            PipelineConfig::default().fetch_buffer_size()
        );
    }

    #[test]
    fn test_pipeline_stats_default() {
        let stats = PipelineStats::default();
        assert_eq!(stats.total_tasks(), 0);
        assert_eq!(stats.total_in_pipeline(), 0);
    }

    #[test]
    fn test_pipeline_stats_bottleneck() {
        let stats = PipelineStats {
            avg_execute_time_us: 1000,
            avg_fetch_time_us: 100,
            avg_deserialize_time_us: 200,
            avg_serialize_time_us: 150,
            avg_store_time_us: 100,
            ..Default::default()
        };

        assert_eq!(stats.bottleneck_stage(), PipelineStage::Execute);
    }

    #[test]
    fn test_pipeline_stats_total_time() {
        let stats = PipelineStats {
            avg_fetch_time_us: 100,
            avg_deserialize_time_us: 200,
            avg_execute_time_us: 1000,
            avg_serialize_time_us: 150,
            avg_store_time_us: 100,
            ..Default::default()
        };

        assert_eq!(stats.total_pipeline_time_us(), 1550);
    }

    #[test]
    fn test_pipeline_stats_efficiency() {
        let stats = PipelineStats {
            avg_fetch_time_us: 100,
            avg_deserialize_time_us: 100,
            avg_execute_time_us: 100,
            avg_serialize_time_us: 100,
            avg_store_time_us: 100,
            ..Default::default()
        };

        // Perfect balance means efficiency = 1/5 = 0.2
        assert!((stats.pipeline_efficiency() - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_pipeline_stats_reset() {
        let mut stats = PipelineStats {
            total_tasks: 100,
            in_execute: 10,
            ..Default::default()
        };

        stats.reset();
        assert_eq!(stats.total_tasks(), 0);
        assert_eq!(stats.in_execute(), 0);
    }

    #[tokio::test]
    async fn test_pipeline_creation() {
        let config = PipelineConfig::default();
        let pipeline = Pipeline::new(config);

        assert_eq!(pipeline.config().fetch_buffer_size(), 10);
        assert_eq!(pipeline.available_permits(PipelineStage::Fetch), 5);
    }

    #[tokio::test]
    async fn test_pipeline_available_permits() {
        let config = PipelineConfig::default().with_max_concurrent_fetch(10);
        let pipeline = Pipeline::new(config);

        assert_eq!(pipeline.available_permits(PipelineStage::Fetch), 10);
        assert!(!pipeline.is_stage_full(PipelineStage::Fetch));
    }

    #[tokio::test]
    async fn test_pipeline_least_busy_stage() {
        let config = PipelineConfig::default()
            .with_max_concurrent_fetch(20)
            .with_max_concurrent_execute(5);
        let pipeline = Pipeline::new(config);

        let least_busy = pipeline.least_busy_stage();
        assert_eq!(least_busy, PipelineStage::Fetch);
    }

    #[tokio::test]
    async fn test_pipeline_stats_access() {
        let config = PipelineConfig::default();
        let pipeline = Pipeline::new(config);

        let stats = pipeline.stats().await;
        assert_eq!(stats.total_tasks(), 0);

        pipeline.reset_stats().await;
        let stats = pipeline.stats().await;
        assert_eq!(stats.total_tasks(), 0);
    }
}
