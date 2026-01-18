//! Task priority management with dynamic boost and decay
//!
//! Provides mechanisms to adjust task priorities dynamically:
//! - Priority boost for aging tasks (prevent starvation)
//! - Priority decay for demoting tasks
//! - Batch priority adjustments
//! - Priority histogram analysis

use celers_core::{CelersError, Result, SerializedTask};
use redis::{AsyncCommands, Client};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use crate::QueueMode;

/// Priority adjustment strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriorityAdjustment {
    /// Add to current priority
    Boost(i32),
    /// Subtract from current priority
    Decay(i32),
    /// Set to absolute value
    SetAbsolute(i32),
    /// Multiply priority by factor (clamped to i32 range)
    Multiply(f64),
}

impl PriorityAdjustment {
    /// Apply adjustment to a priority value
    pub fn apply(&self, current: i32) -> i32 {
        match self {
            PriorityAdjustment::Boost(delta) => (current + delta).clamp(0, 255),
            PriorityAdjustment::Decay(delta) => (current - delta).clamp(0, 255),
            PriorityAdjustment::SetAbsolute(value) => (*value).clamp(0, 255),
            PriorityAdjustment::Multiply(factor) => {
                ((current as f64 * factor) as i32).clamp(0, 255)
            }
        }
    }
}

/// Priority manager for dynamic task priority adjustments
pub struct PriorityManager {
    client: Client,
    queue_name: String,
    mode: QueueMode,
}

impl PriorityManager {
    /// Create a new priority manager
    pub fn new(client: Client, queue_name: String, mode: QueueMode) -> Self {
        Self {
            client,
            queue_name,
            mode,
        }
    }

    /// Boost priority of tasks waiting longer than threshold
    ///
    /// Scans the queue and boosts priority of tasks that have been waiting
    /// longer than the specified threshold. Only works in Priority mode.
    ///
    /// # Arguments
    /// * `age_threshold_secs` - Age threshold in seconds
    /// * `boost_amount` - Amount to boost priority by
    ///
    /// # Returns
    /// Number of tasks that had their priority boosted
    pub async fn boost_aging_tasks(
        &self,
        age_threshold_secs: u64,
        boost_amount: i32,
    ) -> Result<usize> {
        if self.mode != QueueMode::Priority {
            return Err(CelersError::Broker(
                "Priority boost only works in Priority mode".to_string(),
            ));
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        // Get all tasks from the priority queue
        let items: Vec<(String, f64)> = conn
            .zrange_withscores(&self.queue_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get tasks: {}", e)))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Failed to get current time: {}", e)))?
            .as_secs();

        let mut boosted_count = 0;
        let mut pipe = redis::pipe();

        for (data, _) in items {
            let task: SerializedTask = serde_json::from_str(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            // Calculate task age (now - created_at timestamp)
            let task_age = now.saturating_sub(task.metadata.id.as_u128() as u64 / 1_000_000);

            if task_age >= age_threshold_secs {
                // Boost priority
                let mut boosted_task = task;
                boosted_task.metadata.priority =
                    (boosted_task.metadata.priority + boost_amount).clamp(0, 255);

                let serialized = serde_json::to_string(&boosted_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                // Remove old entry and add with new priority
                pipe.zrem(&self.queue_name, &data);
                pipe.zadd(
                    &self.queue_name,
                    &serialized,
                    -(boosted_task.metadata.priority as f64),
                );

                boosted_count += 1;
            }
        }

        if boosted_count > 0 {
            pipe.query_async::<redis::Value>(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to boost priorities: {}", e)))?;

            info!("Boosted priority of {} aging tasks", boosted_count);
        }

        Ok(boosted_count)
    }

    /// Apply priority adjustment to tasks matching a filter
    ///
    /// # Arguments
    /// * `filter` - Function to determine which tasks to adjust
    /// * `adjustment` - Priority adjustment to apply
    ///
    /// # Returns
    /// Number of tasks adjusted
    pub async fn adjust_priorities<F>(
        &self,
        filter: F,
        adjustment: PriorityAdjustment,
    ) -> Result<usize>
    where
        F: Fn(&SerializedTask) -> bool,
    {
        if self.mode != QueueMode::Priority {
            return Err(CelersError::Broker(
                "Priority adjustment only works in Priority mode".to_string(),
            ));
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let items: Vec<(String, f64)> = conn
            .zrange_withscores(&self.queue_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get tasks: {}", e)))?;

        let mut adjusted_count = 0;
        let mut pipe = redis::pipe();

        for (data, _) in items {
            let task: SerializedTask = serde_json::from_str(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            if filter(&task) {
                let mut adjusted_task = task;
                adjusted_task.metadata.priority = adjustment.apply(adjusted_task.metadata.priority);

                let serialized = serde_json::to_string(&adjusted_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                // Remove old entry and add with new priority
                pipe.zrem(&self.queue_name, &data);
                pipe.zadd(
                    &self.queue_name,
                    &serialized,
                    -(adjusted_task.metadata.priority as f64),
                );

                adjusted_count += 1;
            }
        }

        if adjusted_count > 0 {
            pipe.query_async::<redis::Value>(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to adjust priorities: {}", e)))?;

            debug!("Adjusted priority of {} tasks", adjusted_count);
        }

        Ok(adjusted_count)
    }

    /// Get priority distribution histogram
    ///
    /// Returns a histogram showing how many tasks are at each priority level.
    /// Useful for monitoring queue balance and detecting priority skew.
    pub async fn get_priority_histogram(&self) -> Result<Vec<(i32, usize)>> {
        if self.mode != QueueMode::Priority {
            return Err(CelersError::Broker(
                "Priority histogram only works in Priority mode".to_string(),
            ));
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let items: Vec<String> = conn
            .zrange(&self.queue_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get tasks: {}", e)))?;

        let mut histogram = std::collections::HashMap::new();

        for data in items {
            let task: SerializedTask = serde_json::from_str(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            *histogram.entry(task.metadata.priority).or_insert(0) += 1;
        }

        let mut result: Vec<(i32, usize)> = histogram.into_iter().collect();
        result.sort_by_key(|(priority, _)| *priority);

        Ok(result)
    }

    /// Normalize all priorities to a 0-255 range based on current distribution
    ///
    /// Rescales all task priorities proportionally to use the full priority range.
    /// Useful when priorities have drifted or become clustered.
    pub async fn normalize_priorities(&self) -> Result<usize> {
        if self.mode != QueueMode::Priority {
            return Err(CelersError::Broker(
                "Priority normalization only works in Priority mode".to_string(),
            ));
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let items: Vec<String> = conn
            .zrange(&self.queue_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get tasks: {}", e)))?;

        if items.is_empty() {
            return Ok(0);
        }

        // Parse all tasks and find min/max priorities
        let mut tasks: Vec<SerializedTask> = Vec::new();
        let mut min_priority = i32::MAX;
        let mut max_priority = i32::MIN;

        for data in &items {
            let task: SerializedTask = serde_json::from_str(data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            min_priority = min_priority.min(task.metadata.priority);
            max_priority = max_priority.max(task.metadata.priority);
            tasks.push(task);
        }

        // If all tasks have the same priority, nothing to normalize
        if min_priority == max_priority {
            return Ok(0);
        }

        let range = max_priority - min_priority;
        let mut pipe = redis::pipe();
        let mut normalized_count = 0;

        for (task, old_data) in tasks.iter().zip(items.iter()) {
            // Normalize to 0-255 range
            let normalized_priority = if range > 0 {
                (((task.metadata.priority - min_priority) as f64 / range as f64) * 255.0) as i32
            } else {
                128 // Middle value if range is 0
            };

            if normalized_priority != task.metadata.priority {
                let mut normalized_task = task.clone();
                normalized_task.metadata.priority = normalized_priority;

                let serialized = serde_json::to_string(&normalized_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                pipe.zrem(&self.queue_name, old_data);
                pipe.zadd(&self.queue_name, &serialized, -(normalized_priority as f64));

                normalized_count += 1;
            }
        }

        if normalized_count > 0 {
            pipe.query_async::<redis::Value>(&mut conn)
                .await
                .map_err(|e| {
                    CelersError::Broker(format!("Failed to normalize priorities: {}", e))
                })?;

            info!(
                "Normalized priorities for {} tasks (range: {}-{} -> 0-255)",
                normalized_count, min_priority, max_priority
            );
        }

        Ok(normalized_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_adjustment_boost() {
        let adj = PriorityAdjustment::Boost(10);
        assert_eq!(adj.apply(5), 15);
        assert_eq!(adj.apply(250), 255); // Clamped
    }

    #[test]
    fn test_priority_adjustment_decay() {
        let adj = PriorityAdjustment::Decay(10);
        assert_eq!(adj.apply(15), 5);
        assert_eq!(adj.apply(5), 0); // Clamped
    }

    #[test]
    fn test_priority_adjustment_absolute() {
        let adj = PriorityAdjustment::SetAbsolute(42);
        assert_eq!(adj.apply(100), 42);
        assert_eq!(adj.apply(0), 42);
    }

    #[test]
    fn test_priority_adjustment_multiply() {
        let adj = PriorityAdjustment::Multiply(2.0);
        assert_eq!(adj.apply(10), 20);
        assert_eq!(adj.apply(200), 255); // Clamped

        let adj2 = PriorityAdjustment::Multiply(0.5);
        assert_eq!(adj2.apply(100), 50);
    }

    #[test]
    fn test_priority_manager_creation() {
        let client = Client::open("redis://localhost:6379").unwrap();
        let manager = PriorityManager::new(client, "test_queue".to_string(), QueueMode::Priority);
        assert_eq!(manager.queue_name, "test_queue");
        assert_eq!(manager.mode, QueueMode::Priority);
    }
}
