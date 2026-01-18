//! Task inspection and search capabilities
//!
//! Provides tools to inspect and query tasks in queues without dequeueing:
//! - Search tasks by various criteria
//! - Peek at queue contents
//! - Count tasks matching conditions
//! - Task metadata extraction

use celers_core::{CelersError, Result, SerializedTask, TaskId, TaskState};
use redis::{AsyncCommands, Client};
use tracing::debug;

use crate::QueueMode;

/// Task search criteria
#[derive(Debug, Clone)]
pub struct TaskSearchCriteria {
    /// Filter by task name (exact match)
    pub name: Option<String>,
    /// Filter by minimum priority
    pub min_priority: Option<i32>,
    /// Filter by maximum priority
    pub max_priority: Option<i32>,
    /// Filter by task state
    pub state: Option<TaskState>,
    /// Filter by task ID
    pub task_id: Option<TaskId>,
    /// Maximum number of results
    pub limit: Option<usize>,
}

impl TaskSearchCriteria {
    /// Create default search criteria (no filters)
    pub fn new() -> Self {
        Self {
            name: None,
            min_priority: None,
            max_priority: None,
            state: None,
            task_id: None,
            limit: None,
        }
    }

    /// Set task name filter
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set priority range filter
    pub fn with_priority_range(mut self, min: i32, max: i32) -> Self {
        self.min_priority = Some(min);
        self.max_priority = Some(max);
        self
    }

    /// Set minimum priority filter
    pub fn with_min_priority(mut self, min: i32) -> Self {
        self.min_priority = Some(min);
        self
    }

    /// Set maximum priority filter
    pub fn with_max_priority(mut self, max: i32) -> Self {
        self.max_priority = Some(max);
        self
    }

    /// Set task state filter
    pub fn with_state(mut self, state: TaskState) -> Self {
        self.state = Some(state);
        self
    }

    /// Set task ID filter
    pub fn with_task_id(mut self, id: TaskId) -> Self {
        self.task_id = Some(id);
        self
    }

    /// Set result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Check if a task matches the criteria
    pub fn matches(&self, task: &SerializedTask) -> bool {
        if let Some(ref name) = self.name {
            if &task.metadata.name != name {
                return false;
            }
        }

        if let Some(min) = self.min_priority {
            if task.metadata.priority < min {
                return false;
            }
        }

        if let Some(max) = self.max_priority {
            if task.metadata.priority > max {
                return false;
            }
        }

        if let Some(ref state) = self.state {
            if &task.metadata.state != state {
                return false;
            }
        }

        if let Some(ref id) = self.task_id {
            if &task.metadata.id != id {
                return false;
            }
        }

        true
    }
}

impl Default for TaskSearchCriteria {
    fn default() -> Self {
        Self::new()
    }
}

/// Task query and inspection interface
pub struct TaskQuery {
    client: Client,
    queue_name: String,
    processing_queue: String,
    dlq_name: String,
    mode: QueueMode,
}

impl TaskQuery {
    /// Create a new task query interface
    pub fn new(
        client: Client,
        queue_name: String,
        processing_queue: String,
        dlq_name: String,
        mode: QueueMode,
    ) -> Self {
        Self {
            client,
            queue_name,
            processing_queue,
            dlq_name,
            mode,
        }
    }

    /// Search for tasks in the main queue matching criteria
    pub async fn search_queue(&self, criteria: &TaskSearchCriteria) -> Result<Vec<SerializedTask>> {
        self.search_in_queue(&self.queue_name, criteria).await
    }

    /// Search for tasks in the processing queue
    pub async fn search_processing(
        &self,
        criteria: &TaskSearchCriteria,
    ) -> Result<Vec<SerializedTask>> {
        self.search_in_queue(&self.processing_queue, criteria).await
    }

    /// Search for tasks in the DLQ
    pub async fn search_dlq(&self, criteria: &TaskSearchCriteria) -> Result<Vec<SerializedTask>> {
        self.search_in_queue(&self.dlq_name, criteria).await
    }

    /// Search for tasks in a specific queue
    async fn search_in_queue(
        &self,
        queue_name: &str,
        criteria: &TaskSearchCriteria,
    ) -> Result<Vec<SerializedTask>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let items: Vec<String> =
            if self.mode == QueueMode::Priority && queue_name == self.queue_name {
                // For priority queue, use ZRANGE
                conn.zrange(queue_name, 0, -1)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to get tasks: {}", e)))?
            } else {
                // For FIFO or non-main queues, use LRANGE
                conn.lrange(queue_name, 0, -1)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to get tasks: {}", e)))?
            };

        let mut results = Vec::new();
        let mut count = 0;

        for data in items {
            let task: SerializedTask = serde_json::from_str(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            if criteria.matches(&task) {
                results.push(task);
                count += 1;

                if let Some(limit) = criteria.limit {
                    if count >= limit {
                        break;
                    }
                }
            }
        }

        debug!("Found {} tasks matching criteria", results.len());
        Ok(results)
    }

    /// Peek at the next N tasks without dequeueing them
    pub async fn peek(&self, count: usize) -> Result<Vec<SerializedTask>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let items: Vec<String> = if self.mode == QueueMode::Priority {
            // Get highest priority items (lowest scores)
            conn.zrange(&self.queue_name, 0, (count as isize) - 1)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to peek tasks: {}", e)))?
        } else {
            // Get from the right (FIFO)
            conn.lrange(&self.queue_name, -(count as isize), -1)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to peek tasks: {}", e)))?
        };

        let mut tasks = Vec::new();
        for data in items {
            let task: SerializedTask = serde_json::from_str(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    /// Count tasks matching criteria in the main queue
    pub async fn count_matching(&self, criteria: &TaskSearchCriteria) -> Result<usize> {
        let tasks = self.search_queue(criteria).await?;
        Ok(tasks.len())
    }

    /// Find a specific task by ID across all queues
    pub async fn find_task_by_id(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<(String, SerializedTask)>> {
        let criteria = TaskSearchCriteria::new()
            .with_task_id(*task_id)
            .with_limit(1);

        // Search in main queue
        if let Ok(tasks) = self.search_queue(&criteria).await {
            if let Some(task) = tasks.first() {
                return Ok(Some(("main".to_string(), task.clone())));
            }
        }

        // Search in processing queue
        if let Ok(tasks) = self.search_processing(&criteria).await {
            if let Some(task) = tasks.first() {
                return Ok(Some(("processing".to_string(), task.clone())));
            }
        }

        // Search in DLQ
        if let Ok(tasks) = self.search_dlq(&criteria).await {
            if let Some(task) = tasks.first() {
                return Ok(Some(("dlq".to_string(), task.clone())));
            }
        }

        Ok(None)
    }

    /// Get task statistics summary
    pub async fn get_task_stats(&self) -> Result<TaskStats> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let main_items: Vec<String> = if self.mode == QueueMode::Priority {
            conn.zrange(&self.queue_name, 0, -1)
                .await
                .unwrap_or_default()
        } else {
            conn.lrange(&self.queue_name, 0, -1)
                .await
                .unwrap_or_default()
        };

        let mut priority_sum = 0i64;
        let mut priority_count = 0usize;
        let mut name_counts = std::collections::HashMap::new();

        for data in main_items {
            if let Ok(task) = serde_json::from_str::<SerializedTask>(&data) {
                priority_sum += task.metadata.priority as i64;
                priority_count += 1;
                *name_counts.entry(task.metadata.name.clone()).or_insert(0) += 1;
            }
        }

        let avg_priority = if priority_count > 0 {
            (priority_sum / priority_count as i64) as f64
        } else {
            0.0
        };

        Ok(TaskStats {
            total_tasks: priority_count,
            avg_priority,
            task_types: name_counts.len(),
            name_distribution: name_counts,
        })
    }
}

/// Task statistics summary
#[derive(Debug, Clone)]
pub struct TaskStats {
    pub total_tasks: usize,
    pub avg_priority: f64,
    pub task_types: usize,
    pub name_distribution: std::collections::HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::TaskMetadata;

    #[test]
    fn test_search_criteria_matches_name() {
        let criteria = TaskSearchCriteria::new().with_name("test_task");

        let metadata = TaskMetadata::new("test_task".to_string());
        let task = SerializedTask {
            metadata,
            payload: vec![],
        };
        assert!(criteria.matches(&task));

        let metadata2 = TaskMetadata::new("other_task".to_string());
        let task2 = SerializedTask {
            metadata: metadata2,
            payload: vec![],
        };
        assert!(!criteria.matches(&task2));
    }

    #[test]
    fn test_search_criteria_matches_priority() {
        let criteria = TaskSearchCriteria::new().with_priority_range(5, 10);

        let mut metadata = TaskMetadata::new("test".to_string());
        metadata.priority = 7;
        let task = SerializedTask {
            metadata,
            payload: vec![],
        };
        assert!(criteria.matches(&task));

        let mut metadata2 = TaskMetadata::new("test".to_string());
        metadata2.priority = 3;
        let task2 = SerializedTask {
            metadata: metadata2,
            payload: vec![],
        };
        assert!(!criteria.matches(&task2));
    }

    #[test]
    fn test_search_criteria_builder() {
        let criteria = TaskSearchCriteria::new()
            .with_name("test")
            .with_min_priority(5)
            .with_max_priority(10)
            .with_limit(100);

        assert_eq!(criteria.name, Some("test".to_string()));
        assert_eq!(criteria.min_priority, Some(5));
        assert_eq!(criteria.max_priority, Some(10));
        assert_eq!(criteria.limit, Some(100));
    }

    #[test]
    fn test_task_query_creation() {
        let client = Client::open("redis://localhost:6379").unwrap();
        let query = TaskQuery::new(
            client,
            "queue".to_string(),
            "queue:processing".to_string(),
            "queue:dlq".to_string(),
            QueueMode::Fifo,
        );
        assert_eq!(query.queue_name, "queue");
    }
}
