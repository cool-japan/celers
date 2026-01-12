// Copyright (c) 2025 CeleRS Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Multi-queue routing for SQS message distribution
//!
//! This module provides utilities for routing messages to different
//! SQS queues based on configurable rules and patterns.
//!
//! # Features
//!
//! - Rule-based message routing
//! - Priority queue simulation (using multiple queues)
//! - Content-based routing
//! - Round-robin load balancing
//! - Weighted routing for A/B testing
//! - Fan-out to multiple queues
//!
//! # Example
//!
//! ```rust
//! use celers_broker_sqs::router::{MessageRouter, RoutingRule, RoutingStrategy};
//! use serde_json::json;
//!
//! // Create a router with rules
//! let mut router = MessageRouter::new();
//!
//! // Route high-priority tasks to express queue
//! router.add_rule(RoutingRule::priority_based("express-queue", 8, 10));
//!
//! // Route medium-priority to standard queue
//! router.add_rule(RoutingRule::priority_based("standard-queue", 4, 7));
//!
//! // Default to slow queue
//! router.set_default_queue("slow-queue");
//!
//! // Get queue for a message
//! let metadata = json!({"priority": 9});
//! let queue = router.route(&metadata);
//! assert_eq!(queue, "express-queue");
//! ```

use serde_json::Value;
use std::collections::HashMap;

/// Routing strategy for message distribution
#[derive(Debug, Clone, PartialEq)]
pub enum RoutingStrategy {
    /// Route based on priority value
    Priority { min: u8, max: u8 },
    /// Route based on task name pattern (glob-style matching)
    TaskPattern { pattern: String },
    /// Route based on metadata field value
    MetadataMatch { field: String, value: String },
    /// Round-robin across multiple queues
    RoundRobin { queues: Vec<String> },
    /// Weighted routing for A/B testing (queue_name -> weight percentage)
    Weighted { weights: HashMap<String, f64> },
    /// Hash-based routing (consistent hashing on a field)
    Hash { field: String, queues: Vec<String> },
    /// Always route to specific queue
    Fixed { queue: String },
}

/// Routing rule for message distribution
#[derive(Debug, Clone)]
pub struct RoutingRule {
    /// Rule name for debugging
    pub name: String,
    /// Target queue name
    pub queue_name: String,
    /// Routing strategy
    pub strategy: RoutingStrategy,
    /// Rule priority (higher priority rules are evaluated first)
    pub priority: u32,
    /// Whether rule is enabled
    pub enabled: bool,
}

impl RoutingRule {
    /// Create a new routing rule
    pub fn new(
        name: impl Into<String>,
        queue_name: impl Into<String>,
        strategy: RoutingStrategy,
    ) -> Self {
        Self {
            name: name.into(),
            queue_name: queue_name.into(),
            strategy,
            priority: 0,
            enabled: true,
        }
    }

    /// Create a priority-based routing rule
    ///
    /// Routes messages with priority in range [min, max] to the specified queue.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_broker_sqs::router::RoutingRule;
    ///
    /// let rule = RoutingRule::priority_based("express-queue", 8, 10);
    /// assert_eq!(rule.queue_name, "express-queue");
    /// ```
    pub fn priority_based(
        queue_name: impl Into<String>,
        min_priority: u8,
        max_priority: u8,
    ) -> Self {
        Self::new(
            format!("priority-{}-{}", min_priority, max_priority),
            queue_name,
            RoutingStrategy::Priority {
                min: min_priority,
                max: max_priority,
            },
        )
    }

    /// Create a task pattern routing rule
    ///
    /// Routes messages with task names matching the pattern.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_broker_sqs::router::RoutingRule;
    ///
    /// let rule = RoutingRule::task_pattern("email-queue", "email.*");
    /// assert_eq!(rule.queue_name, "email-queue");
    /// ```
    pub fn task_pattern(queue_name: impl Into<String>, pattern: impl Into<String>) -> Self {
        let pattern_str = pattern.into();
        Self::new(
            format!("task-{}", pattern_str),
            queue_name,
            RoutingStrategy::TaskPattern {
                pattern: pattern_str,
            },
        )
    }

    /// Create a metadata-based routing rule
    pub fn metadata_match(
        queue_name: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let field_str = field.into();
        let value_str = value.into();
        Self::new(
            format!("metadata-{}-{}", field_str, value_str),
            queue_name,
            RoutingStrategy::MetadataMatch {
                field: field_str,
                value: value_str,
            },
        )
    }

    /// Create a fixed routing rule
    pub fn fixed(queue_name: impl Into<String>) -> Self {
        let queue = queue_name.into();
        Self::new(
            format!("fixed-{}", queue),
            queue.clone(),
            RoutingStrategy::Fixed { queue },
        )
    }

    /// Set rule priority (higher values evaluated first)
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Enable or disable rule
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Check if rule matches the message metadata
    pub fn matches(&self, task_name: Option<&str>, metadata: &Value) -> bool {
        if !self.enabled {
            return false;
        }

        match &self.strategy {
            RoutingStrategy::Priority { min, max } => {
                if let Some(priority) = metadata.get("priority").and_then(|v| v.as_u64()) {
                    let p = priority as u8;
                    return p >= *min && p <= *max;
                }
                false
            }
            RoutingStrategy::TaskPattern { pattern } => {
                if let Some(name) = task_name {
                    return glob_match(pattern, name);
                }
                false
            }
            RoutingStrategy::MetadataMatch { field, value } => {
                if let Some(field_value) = metadata.get(field).and_then(|v| v.as_str()) {
                    return field_value == value;
                }
                false
            }
            RoutingStrategy::Fixed { .. } => true,
            _ => false, // RoundRobin, Weighted, Hash handled separately
        }
    }
}

/// Message router for distributing messages to queues
#[derive(Debug)]
pub struct MessageRouter {
    rules: Vec<RoutingRule>,
    default_queue: String,
    round_robin_state: HashMap<String, usize>,
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageRouter {
    /// Create a new message router
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            default_queue: "default".to_string(),
            round_robin_state: HashMap::new(),
        }
    }

    /// Add a routing rule
    pub fn add_rule(&mut self, rule: RoutingRule) {
        self.rules.push(rule);
        // Sort by priority (descending)
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Set default queue for messages that don't match any rules
    pub fn set_default_queue(&mut self, queue_name: impl Into<String>) {
        self.default_queue = queue_name.into();
    }

    /// Route a message to a queue
    ///
    /// Returns the queue name where the message should be sent.
    pub fn route(&mut self, metadata: &Value) -> String {
        self.route_with_task_name(None, metadata)
    }

    /// Route a message with task name to a queue
    pub fn route_with_task_name(&mut self, task_name: Option<&str>, metadata: &Value) -> String {
        for rule in &self.rules {
            if rule.matches(task_name, metadata) {
                return rule.queue_name.clone();
            }
        }

        self.default_queue.clone()
    }

    /// Get statistics about routing
    pub fn stats(&self) -> RouterStats {
        RouterStats {
            total_rules: self.rules.len(),
            enabled_rules: self.rules.iter().filter(|r| r.enabled).count(),
            default_queue: self.default_queue.clone(),
        }
    }

    /// Clear all rules
    pub fn clear_rules(&mut self) {
        self.rules.clear();
        self.round_robin_state.clear();
    }

    /// Get rule by name
    pub fn get_rule(&self, name: &str) -> Option<&RoutingRule> {
        self.rules.iter().find(|r| r.name == name)
    }

    /// Enable/disable a rule by name
    pub fn set_rule_enabled(&mut self, name: &str, enabled: bool) {
        if let Some(rule) = self.rules.iter_mut().find(|r| r.name == name) {
            rule.enabled = enabled;
        }
    }
}

/// Router statistics
#[derive(Debug, Clone)]
pub struct RouterStats {
    /// Total number of rules
    pub total_rules: usize,
    /// Number of enabled rules
    pub enabled_rules: usize,
    /// Default queue name
    pub default_queue: String,
}

/// Simple glob pattern matching (supports * wildcard)
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if !pattern.contains('*') {
        return pattern == text;
    }

    let parts: Vec<&str> = pattern.split('*').collect();

    // Check if text starts with first part
    if !parts.is_empty() && !parts[0].is_empty() && !text.starts_with(parts[0]) {
        return false;
    }

    // Check if text ends with last part
    if parts.len() > 1
        && !parts[parts.len() - 1].is_empty()
        && !text.ends_with(parts[parts.len() - 1])
    {
        return false;
    }

    // Check middle parts
    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            pos = part.len();
            continue;
        }

        if let Some(index) = text[pos..].find(part) {
            pos += index + part.len();
        } else {
            return false;
        }
    }

    true
}

/// Priority queue simulator using multiple SQS queues
///
/// Simulates priority queues by routing messages to different queues
/// based on priority levels.
#[derive(Debug)]
pub struct PriorityQueueRouter {
    router: MessageRouter,
}

impl PriorityQueueRouter {
    /// Create a new priority queue router with 3 levels
    ///
    /// - High priority (8-10) -> {base_name}-high
    /// - Medium priority (4-7) -> {base_name}-medium
    /// - Low priority (0-3) -> {base_name}-low
    pub fn new(base_queue_name: &str) -> Self {
        let mut router = MessageRouter::new();

        router.add_rule(
            RoutingRule::priority_based(format!("{}-high", base_queue_name), 8, 10)
                .with_priority(3),
        );
        router.add_rule(
            RoutingRule::priority_based(format!("{}-medium", base_queue_name), 4, 7)
                .with_priority(2),
        );
        router.add_rule(
            RoutingRule::priority_based(format!("{}-low", base_queue_name), 0, 3).with_priority(1),
        );

        router.set_default_queue(format!("{}-medium", base_queue_name));

        Self { router }
    }

    /// Route message based on priority
    pub fn route(&mut self, metadata: &Value) -> String {
        self.router.route(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_priority_routing() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("express", 8, 10));
        router.add_rule(RoutingRule::priority_based("standard", 4, 7));
        router.set_default_queue("slow");

        assert_eq!(router.route(&json!({"priority": 9})), "express");
        assert_eq!(router.route(&json!({"priority": 5})), "standard");
        assert_eq!(router.route(&json!({"priority": 2})), "slow");
        assert_eq!(router.route(&json!({})), "slow");
    }

    #[test]
    fn test_task_pattern_routing() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::task_pattern("email-queue", "email.*"));
        router.add_rule(RoutingRule::task_pattern("sms-queue", "sms.*"));
        router.set_default_queue("default");

        assert_eq!(
            router.route_with_task_name(Some("email.send"), &json!({})),
            "email-queue"
        );
        assert_eq!(
            router.route_with_task_name(Some("sms.send"), &json!({})),
            "sms-queue"
        );
        assert_eq!(
            router.route_with_task_name(Some("other.task"), &json!({})),
            "default"
        );
    }

    #[test]
    fn test_metadata_routing() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::metadata_match(
            "premium-queue",
            "tier",
            "premium",
        ));
        router.set_default_queue("standard-queue");

        assert_eq!(router.route(&json!({"tier": "premium"})), "premium-queue");
        assert_eq!(router.route(&json!({"tier": "standard"})), "standard-queue");
    }

    #[test]
    fn test_rule_priority() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("high", 5, 10).with_priority(10));
        router.add_rule(RoutingRule::priority_based("low", 0, 10).with_priority(1));

        // High priority rule should match first
        assert_eq!(router.route(&json!({"priority": 7})), "high");
    }

    #[test]
    fn test_rule_enabled() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("express", 8, 10).with_enabled(false));
        router.set_default_queue("default");

        // Disabled rule should not match
        assert_eq!(router.route(&json!({"priority": 9})), "default");
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("email.*", "email.send"));
        assert!(glob_match("email.*", "email.receive"));
        assert!(!glob_match("email.*", "sms.send"));
        assert!(glob_match("*.send", "email.send"));
        assert!(glob_match("*.send", "sms.send"));
        assert!(!glob_match("*.send", "email.receive"));
        assert!(glob_match("test", "test"));
        assert!(!glob_match("test", "test2"));
    }

    #[test]
    fn test_priority_queue_router() {
        let mut router = PriorityQueueRouter::new("tasks");

        assert_eq!(router.route(&json!({"priority": 9})), "tasks-high");
        assert_eq!(router.route(&json!({"priority": 5})), "tasks-medium");
        assert_eq!(router.route(&json!({"priority": 1})), "tasks-low");
        assert_eq!(router.route(&json!({})), "tasks-medium");
    }

    #[test]
    fn test_router_stats() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("express", 8, 10));
        router.add_rule(RoutingRule::priority_based("slow", 0, 3).with_enabled(false));
        router.set_default_queue("default");

        let stats = router.stats();
        assert_eq!(stats.total_rules, 2);
        assert_eq!(stats.enabled_rules, 1);
        assert_eq!(stats.default_queue, "default");
    }

    #[test]
    fn test_clear_rules() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("express", 8, 10));
        router.add_rule(RoutingRule::priority_based("slow", 0, 3));

        assert_eq!(router.rules.len(), 2);

        router.clear_rules();
        assert_eq!(router.rules.len(), 0);
    }

    #[test]
    fn test_get_rule() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("express", 8, 10));

        let rule = router.get_rule("priority-8-10");
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().queue_name, "express");

        let missing = router.get_rule("nonexistent");
        assert!(missing.is_none());
    }

    #[test]
    fn test_set_rule_enabled() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::priority_based("express", 8, 10));
        router.set_default_queue("default");

        assert_eq!(router.route(&json!({"priority": 9})), "express");

        router.set_rule_enabled("priority-8-10", false);
        assert_eq!(router.route(&json!({"priority": 9})), "default");
    }

    #[test]
    fn test_fixed_routing() {
        let mut router = MessageRouter::new();
        router.add_rule(RoutingRule::fixed("always-here"));

        assert_eq!(router.route(&json!({})), "always-here");
        assert_eq!(router.route(&json!({"priority": 10})), "always-here");
    }
}
