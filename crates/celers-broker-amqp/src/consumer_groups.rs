//! Consumer group management utilities for AMQP
//!
//! This module provides utilities for managing consumer groups, load balancing,
//! and coordinating multiple consumers working on the same queue.
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::consumer_groups::{ConsumerGroup, ConsumerInfo, LoadBalancingStrategy};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut group = ConsumerGroup::new("my_consumer_group".to_string(), LoadBalancingStrategy::RoundRobin);
//!
//! // Add consumers to the group
//! group.add_consumer(ConsumerInfo {
//!     consumer_id: "consumer-1".to_string(),
//!     queue_name: "task_queue".to_string(),
//!     prefetch_count: 10,
//!     priority: 0,
//!     active: true,
//!     messages_processed: 0,
//! });
//!
//! // Get next consumer for message routing
//! let consumer = group.next_consumer();
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Load balancing strategy for consumer groups
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoadBalancingStrategy {
    /// Round-robin distribution
    RoundRobin,
    /// Least-connections (route to consumer with fewest active messages)
    LeastConnections,
    /// Priority-based (route to highest priority consumer)
    Priority,
    /// Random distribution
    Random,
}

impl std::fmt::Display for LoadBalancingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadBalancingStrategy::RoundRobin => write!(f, "round-robin"),
            LoadBalancingStrategy::LeastConnections => write!(f, "least-connections"),
            LoadBalancingStrategy::Priority => write!(f, "priority"),
            LoadBalancingStrategy::Random => write!(f, "random"),
        }
    }
}

/// Consumer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerInfo {
    /// Unique consumer ID
    pub consumer_id: String,
    /// Queue name this consumer is listening to
    pub queue_name: String,
    /// Prefetch count (QoS)
    pub prefetch_count: u16,
    /// Consumer priority (higher = more priority)
    pub priority: i32,
    /// Whether consumer is active
    pub active: bool,
    /// Number of messages processed
    pub messages_processed: usize,
}

impl ConsumerInfo {
    /// Create a new consumer info
    pub fn new(consumer_id: String, queue_name: String) -> Self {
        Self {
            consumer_id,
            queue_name,
            prefetch_count: 10,
            priority: 0,
            active: true,
            messages_processed: 0,
        }
    }

    /// Set prefetch count
    pub fn with_prefetch(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Mark consumer as inactive
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    /// Mark consumer as active
    pub fn activate(&mut self) {
        self.active = true;
    }

    /// Record message processed
    pub fn record_message_processed(&mut self) {
        self.messages_processed += 1;
    }
}

/// Consumer group for coordinating multiple consumers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroup {
    /// Group name
    pub group_name: String,
    /// Load balancing strategy
    pub strategy: LoadBalancingStrategy,
    /// Consumers in this group
    consumers: HashMap<String, ConsumerInfo>,
    /// Current round-robin index
    #[serde(skip)]
    round_robin_index: usize,
}

impl ConsumerGroup {
    /// Create a new consumer group
    pub fn new(group_name: String, strategy: LoadBalancingStrategy) -> Self {
        Self {
            group_name,
            strategy,
            consumers: HashMap::new(),
            round_robin_index: 0,
        }
    }

    /// Add consumer to the group
    pub fn add_consumer(&mut self, consumer: ConsumerInfo) {
        self.consumers
            .insert(consumer.consumer_id.clone(), consumer);
    }

    /// Remove consumer from the group
    pub fn remove_consumer(&mut self, consumer_id: &str) -> Option<ConsumerInfo> {
        self.consumers.remove(consumer_id)
    }

    /// Get consumer by ID
    pub fn get_consumer(&self, consumer_id: &str) -> Option<&ConsumerInfo> {
        self.consumers.get(consumer_id)
    }

    /// Get mutable consumer by ID
    pub fn get_consumer_mut(&mut self, consumer_id: &str) -> Option<&mut ConsumerInfo> {
        self.consumers.get_mut(consumer_id)
    }

    /// Get all consumers
    pub fn get_all_consumers(&self) -> &HashMap<String, ConsumerInfo> {
        &self.consumers
    }

    /// Get active consumers
    pub fn get_active_consumers(&self) -> Vec<&ConsumerInfo> {
        self.consumers.values().filter(|c| c.active).collect()
    }

    /// Get next consumer ID based on load balancing strategy
    pub fn next_consumer(&mut self) -> Option<String> {
        let active_consumers: Vec<ConsumerInfo> = self
            .consumers
            .values()
            .filter(|c| c.active)
            .cloned()
            .collect();

        if active_consumers.is_empty() {
            return None;
        }

        let selected = match self.strategy {
            LoadBalancingStrategy::RoundRobin => {
                let consumer = &active_consumers[self.round_robin_index % active_consumers.len()];
                self.round_robin_index = (self.round_robin_index + 1) % active_consumers.len();
                consumer.clone()
            }
            LoadBalancingStrategy::LeastConnections => active_consumers
                .into_iter()
                .min_by_key(|c| c.messages_processed)?,
            LoadBalancingStrategy::Priority => {
                active_consumers.into_iter().max_by_key(|c| c.priority)?
            }
            LoadBalancingStrategy::Random => {
                use rand::Rng;
                let mut rng = rand::rng();
                let idx = rng.random_range(0..active_consumers.len());
                active_consumers[idx].clone()
            }
        };

        Some(selected.consumer_id)
    }

    /// Get consumer count
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// Get active consumer count
    pub fn active_consumer_count(&self) -> usize {
        self.consumers.values().filter(|c| c.active).count()
    }

    /// Get total messages processed by the group
    pub fn total_messages_processed(&self) -> usize {
        self.consumers.values().map(|c| c.messages_processed).sum()
    }

    /// Get group statistics
    pub fn get_statistics(&self) -> ConsumerGroupStats {
        let total_consumers = self.consumer_count();
        let active_consumers = self.active_consumer_count();
        let total_messages_processed = self.total_messages_processed();

        let avg_messages_per_consumer = if total_consumers > 0 {
            total_messages_processed as f64 / total_consumers as f64
        } else {
            0.0
        };

        ConsumerGroupStats {
            group_name: self.group_name.clone(),
            total_consumers,
            active_consumers,
            inactive_consumers: total_consumers - active_consumers,
            total_messages_processed,
            avg_messages_per_consumer,
            strategy: self.strategy,
        }
    }
}

/// Consumer group statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupStats {
    /// Group name
    pub group_name: String,
    /// Total number of consumers
    pub total_consumers: usize,
    /// Number of active consumers
    pub active_consumers: usize,
    /// Number of inactive consumers
    pub inactive_consumers: usize,
    /// Total messages processed
    pub total_messages_processed: usize,
    /// Average messages per consumer
    pub avg_messages_per_consumer: f64,
    /// Load balancing strategy
    pub strategy: LoadBalancingStrategy,
}

impl ConsumerGroupStats {
    /// Check if group is healthy (has active consumers)
    pub fn is_healthy(&self) -> bool {
        self.active_consumers > 0
    }

    /// Get consumer utilization (active / total)
    pub fn utilization(&self) -> f64 {
        if self.total_consumers == 0 {
            return 0.0;
        }
        (self.active_consumers as f64 / self.total_consumers as f64) * 100.0
    }
}

/// Consumer group manager for managing multiple consumer groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupManager {
    groups: HashMap<String, ConsumerGroup>,
}

impl ConsumerGroupManager {
    /// Create a new consumer group manager
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Add consumer group
    pub fn add_group(&mut self, group: ConsumerGroup) {
        self.groups.insert(group.group_name.clone(), group);
    }

    /// Remove consumer group
    pub fn remove_group(&mut self, group_name: &str) -> Option<ConsumerGroup> {
        self.groups.remove(group_name)
    }

    /// Get consumer group
    pub fn get_group(&self, group_name: &str) -> Option<&ConsumerGroup> {
        self.groups.get(group_name)
    }

    /// Get mutable consumer group
    pub fn get_group_mut(&mut self, group_name: &str) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(group_name)
    }

    /// Get all groups
    pub fn get_all_groups(&self) -> &HashMap<String, ConsumerGroup> {
        &self.groups
    }

    /// Get group count
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Get total consumer count across all groups
    pub fn total_consumer_count(&self) -> usize {
        self.groups.values().map(|g| g.consumer_count()).sum()
    }

    /// Get total active consumer count across all groups
    pub fn total_active_consumer_count(&self) -> usize {
        self.groups
            .values()
            .map(|g| g.active_consumer_count())
            .sum()
    }

    /// Get manager statistics
    pub fn get_statistics(&self) -> ConsumerGroupManagerStats {
        let group_stats: Vec<ConsumerGroupStats> =
            self.groups.values().map(|g| g.get_statistics()).collect();

        ConsumerGroupManagerStats {
            total_groups: self.group_count(),
            total_consumers: self.total_consumer_count(),
            total_active_consumers: self.total_active_consumer_count(),
            group_stats,
        }
    }
}

impl Default for ConsumerGroupManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumer group manager statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupManagerStats {
    /// Total number of groups
    pub total_groups: usize,
    /// Total number of consumers
    pub total_consumers: usize,
    /// Total number of active consumers
    pub total_active_consumers: usize,
    /// Statistics for each group
    pub group_stats: Vec<ConsumerGroupStats>,
}

impl ConsumerGroupManagerStats {
    /// Get overall utilization
    pub fn overall_utilization(&self) -> f64 {
        if self.total_consumers == 0 {
            return 0.0;
        }
        (self.total_active_consumers as f64 / self.total_consumers as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_info_creation() {
        let consumer = ConsumerInfo::new("consumer-1".to_string(), "queue1".to_string());
        assert_eq!(consumer.consumer_id, "consumer-1");
        assert_eq!(consumer.queue_name, "queue1");
        assert_eq!(consumer.prefetch_count, 10);
        assert_eq!(consumer.priority, 0);
        assert!(consumer.active);
    }

    #[test]
    fn test_consumer_info_with_prefetch() {
        let consumer =
            ConsumerInfo::new("consumer-1".to_string(), "queue1".to_string()).with_prefetch(20);
        assert_eq!(consumer.prefetch_count, 20);
    }

    #[test]
    fn test_consumer_info_with_priority() {
        let consumer =
            ConsumerInfo::new("consumer-1".to_string(), "queue1".to_string()).with_priority(5);
        assert_eq!(consumer.priority, 5);
    }

    #[test]
    fn test_consumer_group_creation() {
        let group = ConsumerGroup::new("group1".to_string(), LoadBalancingStrategy::RoundRobin);
        assert_eq!(group.group_name, "group1");
        assert_eq!(group.consumer_count(), 0);
    }

    #[test]
    fn test_consumer_group_add_remove() {
        let mut group = ConsumerGroup::new("group1".to_string(), LoadBalancingStrategy::RoundRobin);

        let consumer = ConsumerInfo::new("consumer-1".to_string(), "queue1".to_string());
        group.add_consumer(consumer);

        assert_eq!(group.consumer_count(), 1);

        let removed = group.remove_consumer("consumer-1");
        assert!(removed.is_some());
        assert_eq!(group.consumer_count(), 0);
    }

    #[test]
    fn test_consumer_group_round_robin() {
        let mut group = ConsumerGroup::new("group1".to_string(), LoadBalancingStrategy::RoundRobin);

        group.add_consumer(ConsumerInfo::new(
            "consumer-1".to_string(),
            "queue1".to_string(),
        ));
        group.add_consumer(ConsumerInfo::new(
            "consumer-2".to_string(),
            "queue1".to_string(),
        ));

        let c1_id = group.next_consumer().unwrap();
        let c2_id = group.next_consumer().unwrap();
        let c3_id = group.next_consumer().unwrap();

        // Should rotate
        assert_ne!(c1_id, c2_id);
        assert_eq!(c1_id, c3_id);
    }

    #[test]
    fn test_consumer_group_stats() {
        let mut group = ConsumerGroup::new("group1".to_string(), LoadBalancingStrategy::RoundRobin);

        let mut consumer1 = ConsumerInfo::new("consumer-1".to_string(), "queue1".to_string());
        consumer1.messages_processed = 10;
        group.add_consumer(consumer1);

        let mut consumer2 = ConsumerInfo::new("consumer-2".to_string(), "queue1".to_string());
        consumer2.messages_processed = 20;
        group.add_consumer(consumer2);

        let stats = group.get_statistics();
        assert_eq!(stats.total_consumers, 2);
        assert_eq!(stats.active_consumers, 2);
        assert_eq!(stats.total_messages_processed, 30);
        assert_eq!(stats.avg_messages_per_consumer, 15.0);
    }

    #[test]
    fn test_consumer_group_manager() {
        let mut manager = ConsumerGroupManager::new();

        let group = ConsumerGroup::new("group1".to_string(), LoadBalancingStrategy::RoundRobin);
        manager.add_group(group);

        assert_eq!(manager.group_count(), 1);
        assert!(manager.get_group("group1").is_some());
    }

    #[test]
    fn test_load_balancing_strategy_display() {
        assert_eq!(LoadBalancingStrategy::RoundRobin.to_string(), "round-robin");
        assert_eq!(
            LoadBalancingStrategy::LeastConnections.to_string(),
            "least-connections"
        );
        assert_eq!(LoadBalancingStrategy::Priority.to_string(), "priority");
        assert_eq!(LoadBalancingStrategy::Random.to_string(), "random");
    }

    #[test]
    fn test_consumer_group_stats_utilization() {
        let stats = ConsumerGroupStats {
            group_name: "group1".to_string(),
            total_consumers: 10,
            active_consumers: 8,
            inactive_consumers: 2,
            total_messages_processed: 100,
            avg_messages_per_consumer: 10.0,
            strategy: LoadBalancingStrategy::RoundRobin,
        };

        assert_eq!(stats.utilization(), 80.0);
        assert!(stats.is_healthy());
    }
}
