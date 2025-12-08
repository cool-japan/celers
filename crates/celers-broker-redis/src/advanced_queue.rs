//! Advanced queue features for task scheduling
//!
//! Provides weighted queue selection, priority aging, and starvation prevention.

use crate::{CelersError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Queue weight configuration for weighted selection
#[derive(Debug, Clone, PartialEq)]
pub struct QueueWeight {
    /// Queue name
    pub queue_name: String,
    /// Weight (higher = higher probability of selection)
    pub weight: u32,
}

impl QueueWeight {
    /// Create a new queue weight
    pub fn new(queue_name: impl Into<String>, weight: u32) -> Self {
        Self {
            queue_name: queue_name.into(),
            weight,
        }
    }
}

/// Weighted queue selector using round-robin with weights
#[derive(Debug, Clone)]
pub struct WeightedQueueSelector {
    queues: Vec<QueueWeight>,
    current_index: usize,
    current_weight_count: u32,
}

impl WeightedQueueSelector {
    /// Create a new weighted queue selector
    pub fn new(queues: Vec<QueueWeight>) -> Result<Self> {
        if queues.is_empty() {
            return Err(CelersError::Configuration(
                "At least one queue is required".to_string(),
            ));
        }

        Ok(Self {
            queues,
            current_index: 0,
            current_weight_count: 0,
        })
    }

    /// Select the next queue based on weights
    pub fn next_queue(&mut self) -> &str {
        let queue = &self.queues[self.current_index];

        self.current_weight_count += 1;

        // Move to next queue if we've exhausted the current queue's weight
        if self.current_weight_count >= queue.weight {
            self.current_weight_count = 0;
            self.current_index = (self.current_index + 1) % self.queues.len();
        }

        &queue.queue_name
    }

    /// Get total weight of all queues
    pub fn total_weight(&self) -> u32 {
        self.queues.iter().map(|q| q.weight).sum()
    }

    /// Get the number of queues
    pub fn queue_count(&self) -> usize {
        self.queues.len()
    }

    /// Get queue weights
    pub fn queues(&self) -> &[QueueWeight] {
        &self.queues
    }

    /// Reset selection state
    pub fn reset(&mut self) {
        self.current_index = 0;
        self.current_weight_count = 0;
    }
}

/// Priority aging configuration
#[derive(Debug, Clone)]
pub struct PriorityAgingConfig {
    /// Enable priority aging
    pub enabled: bool,

    /// Age increment per aging interval
    pub age_increment: i32,

    /// Aging interval in seconds
    pub aging_interval_secs: u64,

    /// Maximum priority (cap for aging)
    pub max_priority: i32,

    /// Minimum priority (floor for aging)
    pub min_priority: i32,
}

impl Default for PriorityAgingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            age_increment: 1,
            aging_interval_secs: 60,
            max_priority: 255,
            min_priority: 0,
        }
    }
}

impl PriorityAgingConfig {
    /// Create a new builder
    pub fn builder() -> PriorityAgingConfigBuilder {
        PriorityAgingConfigBuilder::default()
    }
}

/// Builder for PriorityAgingConfig
#[derive(Default)]
pub struct PriorityAgingConfigBuilder {
    enabled: bool,
    age_increment: Option<i32>,
    aging_interval_secs: Option<u64>,
    max_priority: Option<i32>,
    min_priority: Option<i32>,
}

impl PriorityAgingConfigBuilder {
    /// Enable or disable priority aging
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set age increment
    pub fn age_increment(mut self, increment: i32) -> Self {
        self.age_increment = Some(increment);
        self
    }

    /// Set aging interval in seconds
    pub fn aging_interval_secs(mut self, secs: u64) -> Self {
        self.aging_interval_secs = Some(secs);
        self
    }

    /// Set maximum priority
    pub fn max_priority(mut self, max: i32) -> Self {
        self.max_priority = Some(max);
        self
    }

    /// Set minimum priority
    pub fn min_priority(mut self, min: i32) -> Self {
        self.min_priority = Some(min);
        self
    }

    /// Build the configuration
    pub fn build(self) -> PriorityAgingConfig {
        PriorityAgingConfig {
            enabled: self.enabled,
            age_increment: self.age_increment.unwrap_or(1),
            aging_interval_secs: self.aging_interval_secs.unwrap_or(60),
            max_priority: self.max_priority.unwrap_or(255),
            min_priority: self.min_priority.unwrap_or(0),
        }
    }
}

/// Task age tracking for priority aging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAge {
    /// Task ID
    pub task_id: String,

    /// Original priority
    pub original_priority: i32,

    /// Current priority (after aging)
    pub current_priority: i32,

    /// Creation timestamp (Unix epoch)
    pub created_at: u64,

    /// Last aging timestamp (Unix epoch)
    pub last_aged_at: u64,
}

impl TaskAge {
    /// Create a new task age tracker
    pub fn new(task_id: String, priority: i32) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            task_id,
            original_priority: priority,
            current_priority: priority,
            created_at: now,
            last_aged_at: now,
        }
    }

    /// Calculate the age in seconds
    pub fn age_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now.saturating_sub(self.created_at)
    }

    /// Check if task should be aged based on interval
    pub fn should_age(&self, interval_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now.saturating_sub(self.last_aged_at) >= interval_secs
    }

    /// Apply aging to the task
    pub fn apply_aging(&mut self, config: &PriorityAgingConfig) {
        if !config.enabled {
            return;
        }

        if !self.should_age(config.aging_interval_secs) {
            return;
        }

        // Increase priority (lower value = higher priority in some systems)
        self.current_priority = (self.current_priority + config.age_increment)
            .clamp(config.min_priority, config.max_priority);

        self.last_aged_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

/// Starvation prevention statistics
#[derive(Debug, Clone, Default)]
pub struct StarvationStats {
    /// Number of tasks promoted
    pub tasks_promoted: u64,

    /// Number of tasks starved (waited too long)
    pub tasks_starved: u64,

    /// Average wait time before promotion (seconds)
    pub avg_wait_time_secs: f64,

    /// Maximum wait time observed (seconds)
    pub max_wait_time_secs: u64,
}

/// Starvation prevention manager
#[derive(Debug, Clone)]
pub struct StarvationPrevention {
    /// Maximum wait time before promotion (seconds)
    pub max_wait_secs: u64,

    /// Priority boost on promotion
    pub promotion_boost: i32,

    /// Task age tracking
    task_ages: HashMap<String, TaskAge>,

    /// Statistics
    stats: StarvationStats,
}

impl StarvationPrevention {
    /// Create a new starvation prevention manager
    pub fn new(max_wait_secs: u64, promotion_boost: i32) -> Self {
        Self {
            max_wait_secs,
            promotion_boost,
            task_ages: HashMap::new(),
            stats: StarvationStats::default(),
        }
    }

    /// Track a new task
    pub fn track_task(&mut self, task_id: String, priority: i32) {
        self.task_ages
            .insert(task_id.clone(), TaskAge::new(task_id, priority));
    }

    /// Check if a task is starving and needs promotion
    pub fn check_starvation(&mut self, task_id: &str) -> Option<i32> {
        if let Some(task_age) = self.task_ages.get(task_id) {
            let age = task_age.age_secs();

            if age >= self.max_wait_secs {
                // Task is starving, promote it
                let new_priority = task_age.current_priority + self.promotion_boost;

                self.stats.tasks_starved += 1;
                self.stats.tasks_promoted += 1;
                self.stats.max_wait_time_secs = self.stats.max_wait_time_secs.max(age);

                // Update average wait time
                let total_wait = self.stats.avg_wait_time_secs
                    * (self.stats.tasks_promoted.saturating_sub(1)) as f64;
                self.stats.avg_wait_time_secs =
                    (total_wait + age as f64) / self.stats.tasks_promoted as f64;

                return Some(new_priority);
            }
        }

        None
    }

    /// Remove a task from tracking (when processed)
    pub fn remove_task(&mut self, task_id: &str) {
        self.task_ages.remove(task_id);
    }

    /// Get statistics
    pub fn stats(&self) -> &StarvationStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = StarvationStats::default();
    }

    /// Get number of tracked tasks
    pub fn tracked_count(&self) -> usize {
        self.task_ages.len()
    }

    /// Clear all tracked tasks
    pub fn clear(&mut self) {
        self.task_ages.clear();
    }
}

/// Combined advanced queue manager
pub struct AdvancedQueueManager {
    selector: WeightedQueueSelector,
    aging_config: PriorityAgingConfig,
    starvation: StarvationPrevention,
}

impl AdvancedQueueManager {
    /// Create a new advanced queue manager
    pub fn new(
        queues: Vec<QueueWeight>,
        aging_config: PriorityAgingConfig,
        max_wait_secs: u64,
        promotion_boost: i32,
    ) -> Result<Self> {
        Ok(Self {
            selector: WeightedQueueSelector::new(queues)?,
            aging_config,
            starvation: StarvationPrevention::new(max_wait_secs, promotion_boost),
        })
    }

    /// Select the next queue
    pub fn select_queue(&mut self) -> &str {
        self.selector.next_queue()
    }

    /// Get aging configuration
    pub fn aging_config(&self) -> &PriorityAgingConfig {
        &self.aging_config
    }

    /// Get starvation prevention
    pub fn starvation_prevention(&mut self) -> &mut StarvationPrevention {
        &mut self.starvation
    }

    /// Get queue selector
    pub fn queue_selector(&self) -> &WeightedQueueSelector {
        &self.selector
    }

    /// Get mutable queue selector
    pub fn queue_selector_mut(&mut self) -> &mut WeightedQueueSelector {
        &mut self.selector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_weight_new() {
        let weight = QueueWeight::new("queue1", 10);
        assert_eq!(weight.queue_name, "queue1");
        assert_eq!(weight.weight, 10);
    }

    #[test]
    fn test_weighted_queue_selector_new() {
        let queues = vec![QueueWeight::new("queue1", 3), QueueWeight::new("queue2", 1)];

        let selector = WeightedQueueSelector::new(queues).unwrap();
        assert_eq!(selector.queue_count(), 2);
        assert_eq!(selector.total_weight(), 4);
    }

    #[test]
    fn test_weighted_queue_selector_empty() {
        let result = WeightedQueueSelector::new(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_weighted_queue_selector_next() {
        let queues = vec![QueueWeight::new("queue1", 2), QueueWeight::new("queue2", 1)];

        let mut selector = WeightedQueueSelector::new(queues).unwrap();

        // Should select queue1 twice, then queue2 once, then repeat
        assert_eq!(selector.next_queue(), "queue1");
        assert_eq!(selector.next_queue(), "queue1");
        assert_eq!(selector.next_queue(), "queue2");
        assert_eq!(selector.next_queue(), "queue1");
        assert_eq!(selector.next_queue(), "queue1");
        assert_eq!(selector.next_queue(), "queue2");
    }

    #[test]
    fn test_weighted_queue_selector_reset() {
        let queues = vec![QueueWeight::new("queue1", 2), QueueWeight::new("queue2", 1)];

        let mut selector = WeightedQueueSelector::new(queues).unwrap();

        assert_eq!(selector.next_queue(), "queue1");
        assert_eq!(selector.next_queue(), "queue1");

        selector.reset();

        assert_eq!(selector.next_queue(), "queue1");
        assert_eq!(selector.next_queue(), "queue1");
    }

    #[test]
    fn test_priority_aging_config_default() {
        let config = PriorityAgingConfig::default();

        assert!(config.enabled);
        assert_eq!(config.age_increment, 1);
        assert_eq!(config.aging_interval_secs, 60);
        assert_eq!(config.max_priority, 255);
        assert_eq!(config.min_priority, 0);
    }

    #[test]
    fn test_priority_aging_config_builder() {
        let config = PriorityAgingConfig::builder()
            .enabled(false)
            .age_increment(5)
            .aging_interval_secs(30)
            .max_priority(100)
            .min_priority(10)
            .build();

        assert!(!config.enabled);
        assert_eq!(config.age_increment, 5);
        assert_eq!(config.aging_interval_secs, 30);
        assert_eq!(config.max_priority, 100);
        assert_eq!(config.min_priority, 10);
    }

    #[test]
    fn test_task_age_new() {
        let task_age = TaskAge::new("task1".to_string(), 50);

        assert_eq!(task_age.task_id, "task1");
        assert_eq!(task_age.original_priority, 50);
        assert_eq!(task_age.current_priority, 50);
        assert!(task_age.age_secs() < 1); // Just created
    }

    #[test]
    fn test_task_age_should_age() {
        let mut task_age = TaskAge::new("task1".to_string(), 50);
        task_age.last_aged_at -= 120; // 2 minutes ago

        assert!(task_age.should_age(60)); // Should age after 1 minute
        assert!(!task_age.should_age(180)); // Should not age if interval is 3 minutes
    }

    #[test]
    fn test_task_age_apply_aging() {
        let config = PriorityAgingConfig {
            enabled: true,
            age_increment: 10,
            aging_interval_secs: 60,
            max_priority: 255,
            min_priority: 0,
        };

        let mut task_age = TaskAge::new("task1".to_string(), 50);
        task_age.last_aged_at -= 120; // 2 minutes ago

        task_age.apply_aging(&config);

        assert_eq!(task_age.current_priority, 60);
    }

    #[test]
    fn test_task_age_apply_aging_disabled() {
        let config = PriorityAgingConfig {
            enabled: false,
            age_increment: 10,
            aging_interval_secs: 60,
            max_priority: 255,
            min_priority: 0,
        };

        let mut task_age = TaskAge::new("task1".to_string(), 50);
        task_age.last_aged_at -= 120;

        task_age.apply_aging(&config);

        assert_eq!(task_age.current_priority, 50); // No change
    }

    #[test]
    fn test_task_age_apply_aging_clamp() {
        let config = PriorityAgingConfig {
            enabled: true,
            age_increment: 50,
            aging_interval_secs: 60,
            max_priority: 100,
            min_priority: 0,
        };

        let mut task_age = TaskAge::new("task1".to_string(), 90);
        task_age.last_aged_at -= 120;

        task_age.apply_aging(&config);

        assert_eq!(task_age.current_priority, 100); // Clamped to max
    }

    #[test]
    fn test_starvation_prevention_new() {
        let sp = StarvationPrevention::new(300, 20);

        assert_eq!(sp.max_wait_secs, 300);
        assert_eq!(sp.promotion_boost, 20);
        assert_eq!(sp.tracked_count(), 0);
    }

    #[test]
    fn test_starvation_prevention_track_task() {
        let mut sp = StarvationPrevention::new(300, 20);

        sp.track_task("task1".to_string(), 50);

        assert_eq!(sp.tracked_count(), 1);
    }

    #[test]
    fn test_starvation_prevention_check_no_starvation() {
        let mut sp = StarvationPrevention::new(300, 20);

        sp.track_task("task1".to_string(), 50);

        let result = sp.check_starvation("task1");
        assert!(result.is_none()); // Not starving yet
    }

    #[test]
    fn test_starvation_prevention_remove_task() {
        let mut sp = StarvationPrevention::new(300, 20);

        sp.track_task("task1".to_string(), 50);
        assert_eq!(sp.tracked_count(), 1);

        sp.remove_task("task1");
        assert_eq!(sp.tracked_count(), 0);
    }

    #[test]
    fn test_starvation_prevention_clear() {
        let mut sp = StarvationPrevention::new(300, 20);

        sp.track_task("task1".to_string(), 50);
        sp.track_task("task2".to_string(), 60);
        assert_eq!(sp.tracked_count(), 2);

        sp.clear();
        assert_eq!(sp.tracked_count(), 0);
    }

    #[test]
    fn test_starvation_prevention_reset_stats() {
        let mut sp = StarvationPrevention::new(300, 20);

        sp.track_task("task1".to_string(), 50);
        sp.stats.tasks_promoted = 10;

        sp.reset_stats();

        assert_eq!(sp.stats().tasks_promoted, 0);
    }

    #[test]
    fn test_advanced_queue_manager_new() {
        let queues = vec![QueueWeight::new("queue1", 3), QueueWeight::new("queue2", 1)];

        let aging_config = PriorityAgingConfig::default();

        let manager = AdvancedQueueManager::new(queues, aging_config, 300, 20).unwrap();

        assert_eq!(manager.queue_selector().queue_count(), 2);
        assert_eq!(manager.queue_selector().total_weight(), 4);
    }

    #[test]
    fn test_advanced_queue_manager_select_queue() {
        let queues = vec![QueueWeight::new("queue1", 2), QueueWeight::new("queue2", 1)];

        let aging_config = PriorityAgingConfig::default();

        let mut manager = AdvancedQueueManager::new(queues, aging_config, 300, 20).unwrap();

        assert_eq!(manager.select_queue(), "queue1");
        assert_eq!(manager.select_queue(), "queue1");
        assert_eq!(manager.select_queue(), "queue2");
    }
}
