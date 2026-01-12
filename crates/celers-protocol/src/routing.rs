//! Message routing helpers
//!
//! This module provides utilities for routing messages to different queues based on
//! task names, priorities, and custom rules.

use crate::Message;
use std::collections::HashMap;

/// Routing rule for directing messages to queues
#[derive(Debug, Clone)]
pub struct RoutingRule {
    /// Task name pattern (supports prefix matching with '*')
    pub pattern: String,
    /// Target queue name
    pub queue: String,
    /// Optional routing key
    pub routing_key: Option<String>,
    /// Optional exchange name
    pub exchange: Option<String>,
}

impl RoutingRule {
    /// Create a new routing rule
    pub fn new(pattern: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            queue: queue.into(),
            routing_key: None,
            exchange: None,
        }
    }

    /// Set the routing key
    pub fn with_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = Some(routing_key.into());
        self
    }

    /// Set the exchange
    pub fn with_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = Some(exchange.into());
        self
    }

    /// Check if this rule matches a task name
    #[inline]
    pub fn matches(&self, task_name: &str) -> bool {
        if self.pattern.ends_with('*') {
            let prefix = &self.pattern[..self.pattern.len() - 1];
            task_name.starts_with(prefix)
        } else {
            task_name == self.pattern
        }
    }
}

/// Router for directing messages to queues
#[derive(Debug, Clone)]
pub struct MessageRouter {
    rules: Vec<RoutingRule>,
    default_queue: String,
}

impl MessageRouter {
    /// Create a new message router with a default queue
    pub fn new(default_queue: impl Into<String>) -> Self {
        Self {
            rules: Vec::new(),
            default_queue: default_queue.into(),
        }
    }

    /// Add a routing rule
    pub fn add_rule(&mut self, rule: RoutingRule) {
        self.rules.push(rule);
    }

    /// Add a simple routing rule (pattern -> queue)
    pub fn route(&mut self, pattern: impl Into<String>, queue: impl Into<String>) {
        self.rules.push(RoutingRule::new(pattern, queue));
    }

    /// Get the queue name for a message
    #[inline]
    pub fn get_queue(&self, message: &Message) -> &str {
        self.get_queue_for_task(&message.headers.task)
    }

    /// Get the queue name for a task name
    #[inline]
    pub fn get_queue_for_task(&self, task_name: &str) -> &str {
        for rule in &self.rules {
            if rule.matches(task_name) {
                return &rule.queue;
            }
        }
        &self.default_queue
    }

    /// Get the routing key for a message
    #[inline]
    pub fn get_routing_key(&self, message: &Message) -> Option<&str> {
        for rule in &self.rules {
            if rule.matches(&message.headers.task) {
                return rule.routing_key.as_deref();
            }
        }
        None
    }

    /// Get the exchange for a message
    #[inline]
    pub fn get_exchange(&self, message: &Message) -> Option<&str> {
        for rule in &self.rules {
            if rule.matches(&message.headers.task) {
                return rule.exchange.as_deref();
            }
        }
        None
    }

    /// Group messages by their target queues
    pub fn group_by_queue(&self, messages: Vec<Message>) -> HashMap<String, Vec<Message>> {
        let mut groups = HashMap::new();
        for msg in messages {
            let queue = self.get_queue(&msg).to_string();
            groups.entry(queue).or_insert_with(Vec::new).push(msg);
        }
        groups
    }
}

/// Priority-based router
pub struct PriorityRouter {
    high_priority_queue: String,
    normal_priority_queue: String,
    low_priority_queue: String,
    threshold_high: u8,
    threshold_low: u8,
}

impl PriorityRouter {
    /// Create a new priority-based router
    pub fn new(
        high_priority_queue: impl Into<String>,
        normal_priority_queue: impl Into<String>,
        low_priority_queue: impl Into<String>,
    ) -> Self {
        Self {
            high_priority_queue: high_priority_queue.into(),
            normal_priority_queue: normal_priority_queue.into(),
            low_priority_queue: low_priority_queue.into(),
            threshold_high: 7,
            threshold_low: 3,
        }
    }

    /// Set priority thresholds
    pub fn with_thresholds(mut self, high: u8, low: u8) -> Self {
        self.threshold_high = high;
        self.threshold_low = low;
        self
    }

    /// Get the queue for a message based on priority
    #[inline]
    pub fn get_queue(&self, message: &Message) -> &str {
        let priority = message.properties.priority.unwrap_or(5);

        if priority >= self.threshold_high {
            &self.high_priority_queue
        } else if priority <= self.threshold_low {
            &self.low_priority_queue
        } else {
            &self.normal_priority_queue
        }
    }

    /// Group messages by priority queue
    pub fn group_by_priority(&self, messages: Vec<Message>) -> HashMap<String, Vec<Message>> {
        let mut groups = HashMap::new();
        for msg in messages {
            let queue = self.get_queue(&msg).to_string();
            groups.entry(queue).or_insert_with(Vec::new).push(msg);
        }
        groups
    }
}

/// Round-robin router for load balancing
pub struct RoundRobinRouter {
    queues: Vec<String>,
    current_index: std::sync::atomic::AtomicUsize,
}

impl RoundRobinRouter {
    /// Create a new round-robin router
    pub fn new(queues: Vec<String>) -> Self {
        Self {
            queues,
            current_index: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get the next queue in round-robin order
    #[inline]
    pub fn next_queue(&self) -> &str {
        if self.queues.is_empty() {
            return "default";
        }

        let index = self
            .current_index
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            % self.queues.len();
        &self.queues[index]
    }

    /// Distribute messages across queues in round-robin fashion
    pub fn distribute(&self, messages: Vec<Message>) -> HashMap<String, Vec<Message>> {
        let mut groups = HashMap::new();
        for msg in messages {
            let queue = self.next_queue().to_string();
            groups.entry(queue).or_insert_with(Vec::new).push(msg);
        }
        groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MessageBuilder;

    fn create_test_message(task: &str) -> Message {
        MessageBuilder::new(task).build().unwrap()
    }

    #[test]
    fn test_routing_rule_matches() {
        let rule = RoutingRule::new("tasks.add", "math_queue");
        assert!(rule.matches("tasks.add"));
        assert!(!rule.matches("tasks.subtract"));

        let prefix_rule = RoutingRule::new("tasks.*", "task_queue");
        assert!(prefix_rule.matches("tasks.add"));
        assert!(prefix_rule.matches("tasks.subtract"));
        assert!(!prefix_rule.matches("email.send"));
    }

    #[test]
    fn test_message_router() {
        let mut router = MessageRouter::new("default_queue");
        router.route("tasks.*", "task_queue");
        router.route("email.*", "email_queue");
        router.route("tasks.priority", "priority_queue");

        assert_eq!(router.get_queue_for_task("tasks.add"), "task_queue");
        assert_eq!(router.get_queue_for_task("email.send"), "email_queue");
        assert_eq!(router.get_queue_for_task("tasks.priority"), "task_queue"); // First match wins
        assert_eq!(router.get_queue_for_task("unknown.task"), "default_queue");
    }

    #[test]
    fn test_message_router_with_message() {
        let mut router = MessageRouter::new("default");
        router.route("tasks.*", "task_queue");

        let msg = create_test_message("tasks.add");
        assert_eq!(router.get_queue(&msg), "task_queue");
    }

    #[test]
    fn test_message_router_group_by_queue() {
        let mut router = MessageRouter::new("default");
        router.route("tasks.*", "task_queue");
        router.route("email.*", "email_queue");

        let messages = vec![
            create_test_message("tasks.add"),
            create_test_message("tasks.subtract"),
            create_test_message("email.send"),
            create_test_message("other.task"),
        ];

        let groups = router.group_by_queue(messages);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.get("task_queue").unwrap().len(), 2);
        assert_eq!(groups.get("email_queue").unwrap().len(), 1);
        assert_eq!(groups.get("default").unwrap().len(), 1);
    }

    #[test]
    fn test_priority_router() {
        let router =
            PriorityRouter::new("high_queue", "normal_queue", "low_queue").with_thresholds(7, 3);

        let mut high_msg = create_test_message("task");
        high_msg.properties.priority = Some(9);
        assert_eq!(router.get_queue(&high_msg), "high_queue");

        let mut normal_msg = create_test_message("task");
        normal_msg.properties.priority = Some(5);
        assert_eq!(router.get_queue(&normal_msg), "normal_queue");

        let mut low_msg = create_test_message("task");
        low_msg.properties.priority = Some(1);
        assert_eq!(router.get_queue(&low_msg), "low_queue");
    }

    #[test]
    fn test_priority_router_default() {
        let router = PriorityRouter::new("high", "normal", "low");

        let msg = create_test_message("task"); // No priority set
        assert_eq!(router.get_queue(&msg), "normal");
    }

    #[test]
    fn test_round_robin_router() {
        let router = RoundRobinRouter::new(vec![
            "queue1".to_string(),
            "queue2".to_string(),
            "queue3".to_string(),
        ]);

        assert_eq!(router.next_queue(), "queue1");
        assert_eq!(router.next_queue(), "queue2");
        assert_eq!(router.next_queue(), "queue3");
        assert_eq!(router.next_queue(), "queue1"); // Wraps around
    }

    #[test]
    fn test_round_robin_distribute() {
        let router = RoundRobinRouter::new(vec!["queue1".to_string(), "queue2".to_string()]);

        let messages = vec![
            create_test_message("task1"),
            create_test_message("task2"),
            create_test_message("task3"),
            create_test_message("task4"),
        ];

        let groups = router.distribute(messages);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.get("queue1").unwrap().len(), 2);
        assert_eq!(groups.get("queue2").unwrap().len(), 2);
    }

    #[test]
    fn test_routing_rule_with_routing_key() {
        let rule = RoutingRule::new("tasks.*", "task_queue")
            .with_routing_key("tasks.#")
            .with_exchange("celery");

        assert_eq!(rule.routing_key.as_deref(), Some("tasks.#"));
        assert_eq!(rule.exchange.as_deref(), Some("celery"));
    }
}
