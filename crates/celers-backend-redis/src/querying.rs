//! Task querying and tag-based bulk operations
//!
//! These helpers scan the backend's key space and filter task metadata
//! client-side. All of them go through
//! [`find_tasks_by_pattern`](RedisResultBackend::find_tasks_by_pattern), which
//! supplies the key prefix itself — callers pass a bare suffix pattern
//! (`RedisResultBackend::ALL_TASKS_PATTERN` for "every task").

use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::query::TaskQuery;
use crate::result_backend_trait::ResultBackend;
use crate::types::{Result, TaskResult};

impl RedisResultBackend {
    /// Query tasks by state
    ///
    /// Returns task IDs that match the specified state.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, TaskResult};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// // Find all failed tasks
    /// let failed_ids = backend.query_tasks_by_state(TaskResult::Failure("".to_string())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_tasks_by_state(&mut self, target_state: TaskResult) -> Result<Vec<Uuid>> {
        let task_ids = self.find_tasks_by_pattern(Self::ALL_TASKS_PATTERN).await?;

        let mut matching_ids = Vec::new();

        // Filter by state
        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if meta.result.same_variant(&target_state) {
                    matching_ids.push(task_id);
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks by worker name
    ///
    /// Returns task IDs that were executed by the specified worker.
    pub async fn query_tasks_by_worker(&mut self, worker_name: &str) -> Result<Vec<Uuid>> {
        let task_ids = self.find_tasks_by_pattern(Self::ALL_TASKS_PATTERN).await?;

        let mut matching_ids = Vec::new();

        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if let Some(ref worker) = meta.worker {
                    if worker == worker_name {
                        matching_ids.push(task_id);
                    }
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks created within a time range
    ///
    /// # Arguments
    /// * `start` - Start time (inclusive)
    /// * `end` - End time (inclusive)
    ///
    /// # Returns
    /// Vector of task IDs created within the specified time range
    pub async fn query_tasks_by_time_range(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Uuid>> {
        let task_ids = self.find_tasks_by_pattern(Self::ALL_TASKS_PATTERN).await?;

        let mut matching_ids = Vec::new();

        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if meta.created_at >= start && meta.created_at <= end {
                    matching_ids.push(task_id);
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks by multiple criteria
    ///
    /// # Arguments
    /// * `criteria` - Query criteria to filter tasks
    ///
    /// # Returns
    /// Vector of task IDs that match all specified criteria
    pub async fn query_tasks(&mut self, criteria: TaskQuery) -> Result<Vec<Uuid>> {
        let task_ids = self.find_tasks_by_pattern(Self::ALL_TASKS_PATTERN).await?;

        let mut matching_ids = Vec::new();

        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if criteria.matches(&meta) {
                    matching_ids.push(task_id);
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks by tags (task must have all specified tags)
    ///
    /// This is a convenience method that wraps `query_tasks` with a tag filter.
    pub async fn query_tasks_by_tags(&mut self, tags: Vec<String>) -> Result<Vec<Uuid>> {
        let criteria = TaskQuery::new().with_tags(tags);
        self.query_tasks(criteria).await
    }

    /// Count tasks with specific tags
    ///
    /// Returns the number of tasks that have all specified tags.
    pub async fn count_tasks_by_tags(&mut self, tags: Vec<String>) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        Ok(task_ids.len())
    }

    /// Bulk delete tasks with specific tags
    ///
    /// Deletes all tasks that have all specified tags.
    /// Returns the number of tasks deleted.
    pub async fn bulk_delete_by_tags(&mut self, tags: Vec<String>) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        if task_ids.is_empty() {
            return Ok(0);
        }
        self.delete_results_batch(&task_ids).await?;
        Ok(task_ids.len())
    }

    /// Bulk revoke tasks with specific tags
    ///
    /// Revokes all tasks that have all specified tags.
    /// Returns the number of tasks revoked.
    pub async fn bulk_revoke_by_tags(&mut self, tags: Vec<String>) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        if task_ids.is_empty() {
            return Ok(0);
        }
        self.bulk_transition_state(&task_ids, TaskResult::Revoked)
            .await
    }

    /// Bulk set TTL for tasks with specific tags
    ///
    /// Sets expiration time for all tasks that have all specified tags.
    /// Returns the number of tasks updated.
    pub async fn bulk_set_ttl_by_tags(
        &mut self,
        tags: Vec<String>,
        ttl: Duration,
    ) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        if task_ids.is_empty() {
            return Ok(0);
        }
        self.set_multiple_expirations(&task_ids, ttl).await?;
        Ok(task_ids.len())
    }
}
