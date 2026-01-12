//! Batch operation streaming utilities
//!
//! This module provides efficient streaming of large batch operations
//! to avoid loading all results into memory at once.

use crate::{BackendError, RedisResultBackend, ResultBackend, TaskMeta};
use uuid::Uuid;

/// Configuration for batch streaming operations
#[derive(Debug, Clone)]
pub struct BatchStreamConfig {
    /// Number of items to fetch per batch
    pub chunk_size: usize,
    /// Maximum number of concurrent fetch operations
    pub max_concurrent: usize,
    /// Whether to skip errors and continue processing
    pub skip_errors: bool,
}

impl Default for BatchStreamConfig {
    fn default() -> Self {
        Self {
            chunk_size: 100,
            max_concurrent: 4,
            skip_errors: false,
        }
    }
}

impl BatchStreamConfig {
    /// Create a new batch stream configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the chunk size
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Set the maximum number of concurrent operations
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent = max;
        self
    }

    /// Enable or disable error skipping
    pub fn with_skip_errors(mut self, skip: bool) -> Self {
        self.skip_errors = skip;
        self
    }
}

/// Batch stream result item
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum BatchStreamItem {
    /// Successful result
    Success { task_id: Uuid, meta: TaskMeta },
    /// Error occurred
    Error { task_id: Uuid, error: String },
    /// Task not found
    NotFound { task_id: Uuid },
}

impl BatchStreamItem {
    /// Check if this is a successful result
    pub fn is_success(&self) -> bool {
        matches!(self, BatchStreamItem::Success { .. })
    }

    /// Check if this is an error
    pub fn is_error(&self) -> bool {
        matches!(self, BatchStreamItem::Error { .. })
    }

    /// Check if task was not found
    pub fn is_not_found(&self) -> bool {
        matches!(self, BatchStreamItem::NotFound { .. })
    }

    /// Get the task ID
    pub fn task_id(&self) -> Uuid {
        match self {
            BatchStreamItem::Success { task_id, .. } => *task_id,
            BatchStreamItem::Error { task_id, .. } => *task_id,
            BatchStreamItem::NotFound { task_id } => *task_id,
        }
    }

    /// Get the metadata if this is a successful result
    pub fn meta(&self) -> Option<&TaskMeta> {
        match self {
            BatchStreamItem::Success { meta, .. } => Some(meta),
            _ => None,
        }
    }

    /// Consume and get the metadata if this is a successful result
    pub fn into_meta(self) -> Option<TaskMeta> {
        match self {
            BatchStreamItem::Success { meta, .. } => Some(meta),
            _ => None,
        }
    }
}

/// Batch result stream helper
pub struct BatchStream;

impl BatchStream {
    /// Fetch batch get results with the given configuration
    ///
    /// This is a simplified batch fetching utility that processes results in chunks.
    pub async fn fetch_batch(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<Vec<BatchStreamItem>, BackendError> {
        let mut all_results = Vec::new();

        for chunk in task_ids.chunks(config.chunk_size) {
            let chunk_results =
                Self::fetch_chunk(backend, chunk.to_vec(), config.skip_errors).await;
            for result in chunk_results {
                match result {
                    Ok(item) => all_results.push(item),
                    Err(_) if config.skip_errors => {
                        // Skip error
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(all_results)
    }

    async fn fetch_chunk(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        skip_errors: bool,
    ) -> Vec<Result<BatchStreamItem, BackendError>> {
        let mut results = Vec::new();

        // Fetch all results for this chunk
        match backend.get_results_batch(&task_ids).await {
            Ok(metas) => {
                for (task_id, meta_opt) in task_ids.iter().zip(metas.iter()) {
                    match meta_opt {
                        Some(meta) => {
                            results.push(Ok(BatchStreamItem::Success {
                                task_id: *task_id,
                                meta: meta.clone(),
                            }));
                        }
                        None => {
                            results.push(Ok(BatchStreamItem::NotFound { task_id: *task_id }));
                        }
                    }
                }
            }
            Err(e) => {
                if skip_errors {
                    for task_id in task_ids {
                        results.push(Ok(BatchStreamItem::Error {
                            task_id,
                            error: e.to_string(),
                        }));
                    }
                } else {
                    results.push(Err(e));
                }
            }
        }

        results
    }

    /// Fetch and filter batch results
    pub async fn fetch_filtered<F>(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
        filter: F,
    ) -> Result<Vec<BatchStreamItem>, BackendError>
    where
        F: Fn(&BatchStreamItem) -> bool,
    {
        let items = Self::fetch_batch(backend, task_ids, config).await?;
        Ok(items.into_iter().filter(|item| filter(item)).collect())
    }

    /// Fetch only successful results
    pub async fn fetch_successes(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<Vec<(Uuid, TaskMeta)>, BackendError> {
        let items = Self::fetch_batch(backend, task_ids, config).await?;
        Ok(items
            .into_iter()
            .filter_map(|item| match item {
                BatchStreamItem::Success { task_id, meta } => Some((task_id, meta)),
                _ => None,
            })
            .collect())
    }

    /// Count items in a batch
    pub async fn count_batch(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<usize, BackendError> {
        let items = Self::fetch_batch(backend, task_ids, config).await?;
        Ok(items.len())
    }
}

/// Statistics for batch streaming operations
#[derive(Debug, Clone, Default)]
pub struct BatchStreamStats {
    /// Total items processed
    pub total: usize,
    /// Successful items
    pub success: usize,
    /// Items with errors
    pub errors: usize,
    /// Items not found
    pub not_found: usize,
}

impl BatchStreamStats {
    /// Create empty statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Update statistics with a batch item
    pub fn update(&mut self, item: &BatchStreamItem) {
        self.total += 1;
        match item {
            BatchStreamItem::Success { .. } => self.success += 1,
            BatchStreamItem::Error { .. } => self.errors += 1,
            BatchStreamItem::NotFound { .. } => self.not_found += 1,
        }
    }

    /// Get success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.success as f64 / self.total as f64
        }
    }

    /// Get error rate (0.0 - 1.0)
    pub fn error_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.errors as f64 / self.total as f64
        }
    }
}

impl std::fmt::Display for BatchStreamStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Batch Stream Statistics")?;
        writeln!(f, "  Total: {}", self.total)?;
        writeln!(f, "  Success: {}", self.success)?;
        writeln!(f, "  Errors: {}", self.errors)?;
        writeln!(f, "  Not Found: {}", self.not_found)?;
        writeln!(f, "  Success Rate: {:.1}%", self.success_rate() * 100.0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_stream_config_default() {
        let config = BatchStreamConfig::default();
        assert_eq!(config.chunk_size, 100);
        assert_eq!(config.max_concurrent, 4);
        assert!(!config.skip_errors);
    }

    #[test]
    fn test_batch_stream_config_builder() {
        let config = BatchStreamConfig::new()
            .with_chunk_size(50)
            .with_max_concurrent(8)
            .with_skip_errors(true);

        assert_eq!(config.chunk_size, 50);
        assert_eq!(config.max_concurrent, 8);
        assert!(config.skip_errors);
    }

    #[test]
    fn test_batch_stream_item_success() {
        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());
        let item = BatchStreamItem::Success {
            task_id,
            meta: meta.clone(),
        };

        assert!(item.is_success());
        assert!(!item.is_error());
        assert!(!item.is_not_found());
        assert_eq!(item.task_id(), task_id);
        assert!(item.meta().is_some());
    }

    #[test]
    fn test_batch_stream_item_error() {
        let task_id = Uuid::new_v4();
        let item = BatchStreamItem::Error {
            task_id,
            error: "test error".to_string(),
        };

        assert!(!item.is_success());
        assert!(item.is_error());
        assert!(!item.is_not_found());
        assert_eq!(item.task_id(), task_id);
        assert!(item.meta().is_none());
    }

    #[test]
    fn test_batch_stream_item_not_found() {
        let task_id = Uuid::new_v4();
        let item = BatchStreamItem::NotFound { task_id };

        assert!(!item.is_success());
        assert!(!item.is_error());
        assert!(item.is_not_found());
        assert_eq!(item.task_id(), task_id);
        assert!(item.meta().is_none());
    }

    #[test]
    fn test_batch_stream_stats() {
        let mut stats = BatchStreamStats::new();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.success, 0);

        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());

        stats.update(&BatchStreamItem::Success { task_id, meta });
        stats.update(&BatchStreamItem::Error {
            task_id,
            error: "err".to_string(),
        });
        stats.update(&BatchStreamItem::NotFound { task_id });

        assert_eq!(stats.total, 3);
        assert_eq!(stats.success, 1);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.not_found, 1);
    }

    #[test]
    fn test_batch_stream_stats_rates() {
        let mut stats = BatchStreamStats::new();
        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());

        stats.update(&BatchStreamItem::Success {
            task_id,
            meta: meta.clone(),
        });
        stats.update(&BatchStreamItem::Success { task_id, meta });
        stats.update(&BatchStreamItem::Error {
            task_id,
            error: "err".to_string(),
        });

        assert_eq!(stats.success_rate(), 2.0 / 3.0);
        assert_eq!(stats.error_rate(), 1.0 / 3.0);
    }

    #[test]
    fn test_batch_stream_stats_display() {
        let mut stats = BatchStreamStats::new();
        let task_id = Uuid::new_v4();
        let meta = TaskMeta::new(task_id, "test".to_string());

        stats.update(&BatchStreamItem::Success { task_id, meta });

        let display = format!("{}", stats);
        assert!(display.contains("Batch Stream Statistics"));
        assert!(display.contains("Total: 1"));
        assert!(display.contains("Success: 1"));
    }
}
