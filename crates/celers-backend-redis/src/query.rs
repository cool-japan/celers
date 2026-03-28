//! Task query criteria for filtering and searching tasks
//!
//! Provides the [`TaskQuery`] struct for building multi-criteria task filters.

use chrono::{DateTime, Utc};

use crate::types::{TaskMeta, TaskResult};

/// Query criteria for filtering tasks
#[derive(Debug, Clone, Default)]
pub struct TaskQuery {
    /// Filter by task state
    pub state: Option<TaskResult>,
    /// Filter by worker name
    pub worker: Option<String>,
    /// Filter by creation time (start of range)
    pub created_after: Option<DateTime<Utc>>,
    /// Filter by creation time (end of range)
    pub created_before: Option<DateTime<Utc>>,
    /// Filter by task name pattern
    pub task_name_pattern: Option<String>,
    /// Filter by tags (task must have all specified tags)
    pub tags: Vec<String>,
    /// Filter by metadata key-value pairs (task must have all specified key-value pairs)
    pub metadata: std::collections::HashMap<String, serde_json::Value>,
}

impl TaskQuery {
    /// Create a new empty query
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by state
    pub fn with_state(mut self, state: TaskResult) -> Self {
        self.state = Some(state);
        self
    }

    /// Filter by worker
    pub fn with_worker(mut self, worker: impl Into<String>) -> Self {
        self.worker = Some(worker.into());
        self
    }

    /// Filter by creation time range
    pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.created_after = Some(start);
        self.created_before = Some(end);
        self
    }

    /// Filter by task name pattern (simple substring match)
    pub fn with_task_name_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.task_name_pattern = Some(pattern.into());
        self
    }

    /// Filter by tag (task must have this tag)
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Filter by multiple tags (task must have all specified tags)
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Filter by metadata field (task must have this exact key-value pair)
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Check if a task matches all criteria
    pub fn matches(&self, meta: &TaskMeta) -> bool {
        // Check state
        if let Some(ref target_state) = self.state {
            if !meta.result.same_variant(target_state) {
                return false;
            }
        }

        // Check worker
        if let Some(ref target_worker) = self.worker {
            if meta.worker.as_ref() != Some(target_worker) {
                return false;
            }
        }

        // Check creation time range
        if let Some(start) = self.created_after {
            if meta.created_at < start {
                return false;
            }
        }
        if let Some(end) = self.created_before {
            if meta.created_at > end {
                return false;
            }
        }

        // Check task name pattern
        if let Some(ref pattern) = self.task_name_pattern {
            if !meta.task_name.contains(pattern) {
                return false;
            }
        }

        // Check tags (task must have all specified tags)
        if !self.tags.is_empty() && !meta.has_all_tags(&self.tags) {
            return false;
        }

        // Check metadata (task must have all specified key-value pairs)
        for (key, value) in &self.metadata {
            match meta.get_metadata(key) {
                Some(meta_value) if meta_value == value => {}
                _ => return false,
            }
        }

        true
    }
}
