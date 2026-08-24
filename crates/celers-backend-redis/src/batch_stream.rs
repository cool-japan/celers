//! Batch operation streaming utilities
//!
//! Streams large batch reads chunk by chunk so callers never have to hold every
//! result in memory at once, and so several chunks can be in flight
//! concurrently.
//!
//! [`BatchStream::stream`] is the streaming entry point: it yields
//! [`BatchStreamItem`]s as chunks come back, fetching up to
//! [`BatchStreamConfig::max_concurrent`] chunks in parallel. The `fetch_*`
//! helpers are eager convenience wrappers built on top of it for callers that
//! genuinely want a `Vec`.

use std::pin::Pin;

use futures_util::stream::{Stream, StreamExt};
use uuid::Uuid;

use crate::{BackendError, RedisResultBackend, ResultBackend, TaskMeta};

/// Stream of batch items produced by [`BatchStream::stream`].
pub type BatchItemStream =
    Pin<Box<dyn Stream<Item = Result<BatchStreamItem, BackendError>> + Send>>;

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
    /// Stream batch results chunk by chunk.
    ///
    /// Chunks of [`BatchStreamConfig::chunk_size`] task IDs are fetched with up
    /// to [`BatchStreamConfig::max_concurrent`] requests in flight, and items
    /// are yielded as soon as their chunk lands — nothing accumulates in memory
    /// beyond the in-flight chunks.
    ///
    /// With `skip_errors` set, a failing chunk yields one
    /// [`BatchStreamItem::Error`] per task instead of terminating the stream.
    pub fn stream(
        backend: &RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> BatchItemStream {
        let chunk_size = config.chunk_size.max(1);
        let max_concurrent = config.max_concurrent.max(1);
        let skip_errors = config.skip_errors;

        let chunks: Vec<Vec<Uuid>> = task_ids
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let backend = backend.clone();

        let stream = futures_util::stream::iter(chunks)
            .map(move |chunk| {
                let mut backend = backend.clone();
                async move { Self::fetch_chunk(&mut backend, chunk, skip_errors).await }
            })
            .buffered(max_concurrent)
            .flat_map(futures_util::stream::iter);

        Box::pin(stream)
    }

    /// Fetch batch get results with the given configuration
    ///
    /// Eager wrapper around [`Self::stream`] for callers that want the whole
    /// result set at once.
    pub async fn fetch_batch(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<Vec<BatchStreamItem>, BackendError> {
        let skip_errors = config.skip_errors;
        let mut stream = Self::stream(backend, task_ids, config);
        let mut all_results = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => all_results.push(item),
                Err(_) if skip_errors => {
                    // Skip error
                }
                Err(e) => return Err(e),
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
                for (task_id, meta_opt) in task_ids.iter().zip(metas) {
                    match meta_opt {
                        Some(meta) => {
                            results.push(Ok(BatchStreamItem::Success {
                                task_id: *task_id,
                                meta,
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
        let skip_errors = config.skip_errors;
        let mut stream = Self::stream(backend, task_ids, config);
        let mut kept = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => {
                    if filter(&item) {
                        kept.push(item);
                    }
                }
                Err(_) if skip_errors => {}
                Err(e) => return Err(e),
            }
        }

        Ok(kept)
    }

    /// Fetch only successful results
    pub async fn fetch_successes(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<Vec<(Uuid, TaskMeta)>, BackendError> {
        let skip_errors = config.skip_errors;
        let mut stream = Self::stream(backend, task_ids, config);
        let mut found = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(BatchStreamItem::Success { task_id, meta }) => found.push((task_id, meta)),
                Ok(_) => {}
                Err(_) if skip_errors => {}
                Err(e) => return Err(e),
            }
        }

        Ok(found)
    }

    /// Count items in a batch
    ///
    /// Streams the batch and counts as it goes, so nothing is retained.
    pub async fn count_batch(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<usize, BackendError> {
        let skip_errors = config.skip_errors;
        let mut stream = Self::stream(backend, task_ids, config);
        let mut count = 0;

        while let Some(result) = stream.next().await {
            match result {
                Ok(_) => count += 1,
                Err(_) if skip_errors => {}
                Err(e) => return Err(e),
            }
        }

        Ok(count)
    }

    /// Collect statistics for a batch without retaining the results
    pub async fn collect_stats(
        backend: &mut RedisResultBackend,
        task_ids: Vec<Uuid>,
        config: BatchStreamConfig,
    ) -> Result<BatchStreamStats, BackendError> {
        let skip_errors = config.skip_errors;
        let mut stream = Self::stream(backend, task_ids, config);
        let mut stats = BatchStreamStats::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => stats.update(&item),
                Err(_) if skip_errors => {}
                Err(e) => return Err(e),
            }
        }

        Ok(stats)
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
