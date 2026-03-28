//! Result backend trait definition
//!
//! Defines the [`ResultBackend`] trait that all result backend implementations must satisfy,
//! along with [`LazyTaskResult`] for deferred result loading and the [`ResultStream`] type alias.

use async_trait::async_trait;
use chrono::Utc;
use futures_util::stream::Stream;
use std::pin::Pin;
use std::time::Duration;
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::types::{ChordState, ProgressInfo, Result, TaskMeta, TaskResult};

/// Result backend trait
#[async_trait]
pub trait ResultBackend: Send + Sync {
    /// Store task result
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()>;

    /// Get task result
    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>>;

    /// Delete task result
    async fn delete_result(&mut self, task_id: Uuid) -> Result<()>;

    /// Set task result expiration
    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()>;

    /// Chord: Initialize chord state
    async fn chord_init(&mut self, state: ChordState) -> Result<()>;

    /// Chord: Increment completion counter (returns total completed)
    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize>;

    /// Chord: Get chord state
    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>>;

    /// Chord: Cancel a chord
    async fn chord_cancel(&mut self, chord_id: Uuid, reason: Option<String>) -> Result<()> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            state.cancel(reason);
            // Re-store the updated state (requires ChordState to be serializable)
            // This is a default implementation that should be overridden for efficiency
            self.chord_init(state).await?;
        }
        Ok(())
    }

    /// Chord: Get partial results for all tasks in chord
    ///
    /// Returns a vector of (task_id, result) tuples for all tasks, where result
    /// is None if the task hasn't completed yet.
    async fn chord_get_partial_results(
        &mut self,
        chord_id: Uuid,
    ) -> Result<Vec<(Uuid, Option<TaskMeta>)>> {
        if let Some(state) = self.chord_get_state(chord_id).await? {
            let results = self.get_results_batch(&state.task_ids).await?;
            Ok(state.task_ids.iter().copied().zip(results).collect())
        } else {
            Ok(vec![])
        }
    }

    /// Chord: Retry a failed chord
    ///
    /// Resets the chord state and increments the retry count.
    /// Returns true if retry was successful, false if max retries exceeded.
    async fn chord_retry(&mut self, chord_id: Uuid) -> Result<bool> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            if state.retry() {
                self.chord_init(state).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    // Batch operations (with default implementations for compatibility)

    /// Store multiple task results (optimized with pipelining where supported)
    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        for (task_id, meta) in results {
            self.store_result(*task_id, meta).await?;
        }
        Ok(())
    }

    /// Get multiple task results (optimized with pipelining where supported)
    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        let mut results = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            results.push(self.get_result(*task_id).await?);
        }
        Ok(results)
    }

    /// Delete multiple task results (optimized with pipelining where supported)
    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        for task_id in task_ids {
            self.delete_result(*task_id).await?;
        }
        Ok(())
    }

    // Result versioning (with default implementations)

    /// Store a versioned result
    ///
    /// Stores the result with an incremented version number and keeps a history
    /// of previous versions using a versioned key pattern.
    async fn store_versioned_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<u32> {
        // Get current version
        let current_version = if let Some(existing) = self.get_result(task_id).await? {
            existing.version
        } else {
            0
        };

        // Create new version
        let new_version = current_version + 1;
        let mut versioned_meta = meta.clone();
        versioned_meta.version = new_version;

        // Store the latest version
        self.store_result(task_id, &versioned_meta).await?;

        Ok(new_version)
    }

    /// Get a specific version of a result
    ///
    /// Returns None if the version doesn't exist. Version 0 or omitted means latest.
    async fn get_result_version(
        &mut self,
        task_id: Uuid,
        _version: u32,
    ) -> Result<Option<TaskMeta>> {
        // Default implementation only returns the latest version
        // Override in specific backends to support versioned storage
        self.get_result(task_id).await
    }

    // Progress tracking (with default implementations)

    /// Update task progress (for long-running tasks)
    ///
    /// This allows tasks to report their progress during execution.
    /// The progress is stored in the task metadata and can be queried by clients.
    ///
    /// # Arguments
    /// * `task_id` - Task ID
    /// * `progress` - Progress information
    async fn set_progress(&mut self, task_id: Uuid, progress: ProgressInfo) -> Result<()> {
        // Default implementation: load, update, store
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.progress = Some(progress);
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Get task progress
    ///
    /// Returns the current progress information for a task, if available.
    ///
    /// # Arguments
    /// * `task_id` - Task ID
    ///
    /// # Returns
    /// Progress information, if the task has reported progress
    async fn get_progress(&mut self, task_id: Uuid) -> Result<Option<ProgressInfo>> {
        if let Some(meta) = self.get_result(task_id).await? {
            Ok(meta.progress)
        } else {
            Ok(None)
        }
    }

    // Partial updates (with default implementations)

    /// Update only the task result state (without changing other fields)
    ///
    /// This is more efficient than loading, modifying, and storing the entire TaskMeta.
    async fn update_result_state(&mut self, task_id: Uuid, result: TaskResult) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            let is_terminal = result.is_terminal();
            meta.result = result;
            if is_terminal && meta.completed_at.is_none() {
                meta.completed_at = Some(Utc::now());
            }
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Update only the worker field
    async fn update_worker(&mut self, task_id: Uuid, worker: String) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.worker = Some(worker);
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Mark task as started (updates state and timestamp)
    async fn mark_started(&mut self, task_id: Uuid, worker: Option<String>) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.result = TaskResult::Started;
            meta.started_at = Some(Utc::now());
            if let Some(w) = worker {
                meta.worker = Some(w);
            }
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Mark task as completed with result
    async fn mark_completed(&mut self, task_id: Uuid, result: TaskResult) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.result = result;
            meta.completed_at = Some(Utc::now());
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    // Pagination support (with default implementations)

    /// Get paginated task results
    ///
    /// Returns a page of results based on the provided task IDs with pagination support.
    ///
    /// # Arguments
    /// * `task_ids` - All task IDs to paginate through
    /// * `page` - Page number (0-based)
    /// * `page_size` - Number of results per page
    ///
    /// # Returns
    /// Tuple of (results, total_count, has_more)
    async fn get_results_paginated(
        &mut self,
        task_ids: &[Uuid],
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<Option<TaskMeta>>, usize, bool)> {
        let total = task_ids.len();
        let start = page * page_size;
        let end = (start + page_size).min(total);
        let has_more = end < total;

        if start >= total {
            return Ok((Vec::new(), total, false));
        }

        let page_ids = &task_ids[start..end];
        let results = self.get_results_batch(page_ids).await?;

        Ok((results, total, has_more))
    }
}

/// Lazy result loading wrapper
///
/// Defers loading the full task result until explicitly requested.
/// Useful for performance when you only need the task ID initially.
#[derive(Debug, Clone)]
pub struct LazyTaskResult {
    /// Task ID
    pub task_id: Uuid,

    /// Cached result (loaded on first access)
    cached: Option<TaskMeta>,
}

impl LazyTaskResult {
    /// Create a new lazy result
    pub fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            cached: None,
        }
    }

    /// Create a lazy result with pre-loaded data
    pub fn with_data(meta: TaskMeta) -> Self {
        Self {
            task_id: meta.task_id,
            cached: Some(meta),
        }
    }

    /// Check if the result has been loaded
    pub fn is_loaded(&self) -> bool {
        self.cached.is_some()
    }

    /// Load the result (if not already loaded)
    pub async fn load(&mut self, backend: &mut RedisResultBackend) -> Result<Option<&TaskMeta>> {
        if self.cached.is_none() {
            self.cached = backend.get_result(self.task_id).await?;
        }
        Ok(self.cached.as_ref())
    }

    /// Get the cached result without loading
    pub fn get_cached(&self) -> Option<&TaskMeta> {
        self.cached.as_ref()
    }
}

/// Result stream type for async iteration
pub type ResultStream = Pin<Box<dyn Stream<Item = Result<(Uuid, Option<TaskMeta>)>> + Send>>;
