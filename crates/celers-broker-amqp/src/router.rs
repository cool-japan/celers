//! Advanced message routing patterns for AMQP
//!
//! Provides sophisticated routing strategies beyond basic AMQP exchange types,
//! including content-based routing, priority routing, and conditional routing.
//!
//! # Features
//!
//! - **Content-Based Routing** - Route based on message payload
//! - **Priority-Based Routing** - Route high-priority messages differently
//! - **Conditional Routing** - Apply rules and conditions
//! - **Round-Robin Routing** - Distribute across multiple queues
//! - **Weighted Routing** - Route based on weights
//! - **Fallback Routes** - Handle routing failures gracefully
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::router::{MessageRouter, RoutingRule, RouteCondition};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut router = MessageRouter::new("default_queue");
//!
//! // Add routing rules
//! router.add_rule(RoutingRule {
//!     name: "high_priority".to_string(),
//!     condition: RouteCondition::Priority(9),
//!     target_queue: "priority_queue".to_string(),
//!     weight: 1.0,
//! });
//!
//! router.add_rule(RoutingRule {
//!     name: "large_messages".to_string(),
//!     condition: RouteCondition::PayloadSize { min: Some(1024 * 1024), max: None },
//!     target_queue: "large_message_queue".to_string(),
//!     weight: 1.0,
//! });
//!
//! // Determine routing
//! let queue = router.route("test_message".as_bytes(), Some(9));
//! println!("Route to: {}", queue);
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Routing condition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RouteCondition {
    /// Route based on message priority
    Priority(u8),
    /// Route based on priority range
    PriorityRange { min: u8, max: u8 },
    /// Route based on payload size
    PayloadSize {
        min: Option<usize>,
        max: Option<usize>,
    },
    /// Route based on content type
    ContentType(String),
    /// Route based on routing key pattern
    RoutingKeyPattern(String),
    /// Route based on header value
    Header { key: String, value: String },
    /// Route if payload contains pattern
    PayloadContains(String),
    /// Always match (catch-all)
    Always,
}

/// Routing rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Rule name
    pub name: String,
    /// Condition to match
    pub condition: RouteCondition,
    /// Target queue name
    pub target_queue: String,
    /// Weight for weighted routing (0.0 - 1.0)
    pub weight: f64,
}

/// Routing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    /// First matching rule wins
    FirstMatch,
    /// Route to all matching queues
    Multicast,
    /// Round-robin among matching queues
    RoundRobin,
    /// Weighted random selection
    WeightedRandom,
}

/// Routing statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutingStats {
    /// Total messages routed
    pub total_routed: u64,
    /// Routes per queue
    pub queue_counts: HashMap<String, u64>,
    /// Routes per rule
    pub rule_matches: HashMap<String, u64>,
    /// Fallback routes used
    pub fallback_count: u64,
}

/// Message router
#[derive(Debug)]
pub struct MessageRouter {
    rules: Vec<RoutingRule>,
    default_queue: String,
    strategy: RoutingStrategy,
    stats: RoutingStats,
    round_robin_index: usize,
}

impl MessageRouter {
    /// Create a new message router
    pub fn new(default_queue: &str) -> Self {
        Self {
            rules: Vec::new(),
            default_queue: default_queue.to_string(),
            strategy: RoutingStrategy::FirstMatch,
            stats: RoutingStats::default(),
            round_robin_index: 0,
        }
    }

    /// Create a router with a specific strategy
    pub fn with_strategy(default_queue: &str, strategy: RoutingStrategy) -> Self {
        Self {
            rules: Vec::new(),
            default_queue: default_queue.to_string(),
            strategy,
            stats: RoutingStats::default(),
            round_robin_index: 0,
        }
    }

    /// Add a routing rule
    pub fn add_rule(&mut self, rule: RoutingRule) {
        self.rules.push(rule);
    }

    /// Remove a routing rule by name
    pub fn remove_rule(&mut self, name: &str) -> Option<RoutingRule> {
        if let Some(pos) = self.rules.iter().position(|r| r.name == name) {
            Some(self.rules.remove(pos))
        } else {
            None
        }
    }

    /// Route a message to the appropriate queue(s)
    pub fn route(&mut self, payload: &[u8], priority: Option<u8>) -> String {
        let matching_rules = self.find_matching_rules(payload, priority);

        // Collect info before mutating stats to avoid borrow checker issues
        let (target, rule_names) = if matching_rules.is_empty() {
            (self.default_queue.clone(), Vec::new())
        } else {
            let target = match self.strategy {
                RoutingStrategy::FirstMatch => matching_rules[0].target_queue.clone(),
                RoutingStrategy::RoundRobin => {
                    let rule = &matching_rules[self.round_robin_index % matching_rules.len()];

                    rule.target_queue.clone()
                }
                RoutingStrategy::WeightedRandom => {
                    self.weighted_select(&matching_rules).target_queue.clone()
                }
                RoutingStrategy::Multicast => {
                    // For multicast, return first queue (caller should handle multiple routes)
                    matching_rules[0].target_queue.clone()
                }
            };
            let rule_names: Vec<String> = matching_rules.iter().map(|r| r.name.clone()).collect();
            (target, rule_names)
        };

        // Update round robin index if needed
        if !matching_rules.is_empty() && matches!(self.strategy, RoutingStrategy::RoundRobin) {
            self.round_robin_index += 1;
        }

        // Update statistics
        if rule_names.is_empty() {
            self.stats.fallback_count += 1;
        }
        self.stats.total_routed += 1;
        *self.stats.queue_counts.entry(target.clone()).or_insert(0) += 1;

        for rule_name in rule_names {
            *self.stats.rule_matches.entry(rule_name).or_insert(0) += 1;
        }

        target
    }

    /// Route a message and return all matching queues (for multicast)
    pub fn route_multicast(&mut self, payload: &[u8], priority: Option<u8>) -> Vec<String> {
        let matching_rules = self.find_matching_rules(payload, priority);

        // Collect info before mutating stats to avoid borrow checker issues
        let (queues, rule_names) = if matching_rules.is_empty() {
            (vec![self.default_queue.clone()], Vec::new())
        } else {
            let queues: Vec<String> = matching_rules
                .iter()
                .map(|r| r.target_queue.clone())
                .collect();
            let rule_names: Vec<String> = matching_rules.iter().map(|r| r.name.clone()).collect();
            (queues, rule_names)
        };

        // Update statistics
        if rule_names.is_empty() {
            self.stats.fallback_count += 1;
        }
        self.stats.total_routed += 1;
        for queue in &queues {
            *self.stats.queue_counts.entry(queue.clone()).or_insert(0) += 1;
        }
        for rule_name in rule_names {
            *self.stats.rule_matches.entry(rule_name).or_insert(0) += 1;
        }

        queues
    }

    /// Find rules matching the message
    fn find_matching_rules(&self, payload: &[u8], priority: Option<u8>) -> Vec<&RoutingRule> {
        self.rules
            .iter()
            .filter(|rule| self.matches_condition(&rule.condition, payload, priority))
            .collect()
    }

    /// Check if a condition matches
    fn matches_condition(
        &self,
        condition: &RouteCondition,
        payload: &[u8],
        priority: Option<u8>,
    ) -> bool {
        match condition {
            RouteCondition::Priority(p) => priority == Some(*p),
            RouteCondition::PriorityRange { min, max } => {
                if let Some(prio) = priority {
                    prio >= *min && prio <= *max
                } else {
                    false
                }
            }
            RouteCondition::PayloadSize { min, max } => {
                let size = payload.len();
                let min_ok = min.map(|m| size >= m).unwrap_or(true);
                let max_ok = max.map(|m| size <= m).unwrap_or(true);
                min_ok && max_ok
            }
            RouteCondition::PayloadContains(pattern) => {
                if let Ok(text) = std::str::from_utf8(payload) {
                    text.contains(pattern)
                } else {
                    false
                }
            }
            RouteCondition::Always => true,
            // These would require additional context
            RouteCondition::ContentType(_) => false,
            RouteCondition::RoutingKeyPattern(_) => false,
            RouteCondition::Header { .. } => false,
        }
    }

    /// Weighted random selection
    fn weighted_select<'a>(&self, rules: &[&'a RoutingRule]) -> &'a RoutingRule {
        let total_weight: f64 = rules.iter().map(|r| r.weight).sum();
        let mut random_value = rand::random::<f64>() * total_weight;

        for rule in rules {
            random_value -= rule.weight;
            if random_value <= 0.0 {
                return rule;
            }
        }

        rules[0] // Fallback to first rule
    }

    /// Get routing statistics
    pub fn stats(&self) -> &RoutingStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = RoutingStats::default();
        self.round_robin_index = 0;
    }

    /// Get number of rules
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Set routing strategy
    pub fn set_strategy(&mut self, strategy: RoutingStrategy) {
        self.strategy = strategy;
    }

    /// Get current strategy
    pub fn strategy(&self) -> RoutingStrategy {
        self.strategy
    }
}

/// Route builder for fluent API
pub struct RouteBuilder {
    name: String,
    condition: Option<RouteCondition>,
    target_queue: Option<String>,
    weight: f64,
}

impl RouteBuilder {
    /// Create a new route builder
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            condition: None,
            target_queue: None,
            weight: 1.0,
        }
    }

    /// Set routing condition
    pub fn condition(mut self, condition: RouteCondition) -> Self {
        self.condition = Some(condition);
        self
    }

    /// Set target queue
    pub fn to_queue(mut self, queue: &str) -> Self {
        self.target_queue = Some(queue.to_string());
        self
    }

    /// Set weight
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// Build the routing rule
    pub fn build(self) -> Result<RoutingRule, String> {
        Ok(RoutingRule {
            name: self.name,
            condition: self.condition.ok_or("Condition is required")?,
            target_queue: self.target_queue.ok_or("Target queue is required")?,
            weight: self.weight,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_creation() {
        let router = MessageRouter::new("default");
        assert_eq!(router.default_queue, "default");
        assert_eq!(router.rule_count(), 0);
    }

    #[test]
    fn test_add_remove_rules() {
        let mut router = MessageRouter::new("default");

        let rule = RoutingRule {
            name: "test_rule".to_string(),
            condition: RouteCondition::Always,
            target_queue: "test_queue".to_string(),
            weight: 1.0,
        };

        router.add_rule(rule);
        assert_eq!(router.rule_count(), 1);

        let removed = router.remove_rule("test_rule");
        assert!(removed.is_some());
        assert_eq!(router.rule_count(), 0);
    }

    #[test]
    fn test_priority_routing() {
        let mut router = MessageRouter::new("default");

        router.add_rule(RoutingRule {
            name: "high_priority".to_string(),
            condition: RouteCondition::Priority(9),
            target_queue: "priority_queue".to_string(),
            weight: 1.0,
        });

        let queue = router.route(b"test", Some(9));
        assert_eq!(queue, "priority_queue");

        let queue = router.route(b"test", Some(5));
        assert_eq!(queue, "default");
    }

    #[test]
    fn test_priority_range_routing() {
        let mut router = MessageRouter::new("default");

        router.add_rule(RoutingRule {
            name: "mid_priority".to_string(),
            condition: RouteCondition::PriorityRange { min: 5, max: 7 },
            target_queue: "mid_queue".to_string(),
            weight: 1.0,
        });

        assert_eq!(router.route(b"test", Some(6)), "mid_queue");
        assert_eq!(router.route(b"test", Some(4)), "default");
        assert_eq!(router.route(b"test", Some(8)), "default");
    }

    #[test]
    fn test_payload_size_routing() {
        let mut router = MessageRouter::new("default");

        router.add_rule(RoutingRule {
            name: "large_messages".to_string(),
            condition: RouteCondition::PayloadSize {
                min: Some(100),
                max: None,
            },
            target_queue: "large_queue".to_string(),
            weight: 1.0,
        });

        let small = b"small";
        let large = vec![0u8; 200];

        assert_eq!(router.route(small, None), "default");
        assert_eq!(router.route(&large, None), "large_queue");
    }

    #[test]
    fn test_payload_contains_routing() {
        let mut router = MessageRouter::new("default");

        router.add_rule(RoutingRule {
            name: "error_messages".to_string(),
            condition: RouteCondition::PayloadContains("ERROR".to_string()),
            target_queue: "error_queue".to_string(),
            weight: 1.0,
        });

        assert_eq!(router.route(b"ERROR: test", None), "error_queue");
        assert_eq!(router.route(b"INFO: test", None), "default");
    }

    #[test]
    fn test_round_robin_strategy() {
        let mut router = MessageRouter::with_strategy("default", RoutingStrategy::RoundRobin);

        router.add_rule(RoutingRule {
            name: "queue1".to_string(),
            condition: RouteCondition::Always,
            target_queue: "q1".to_string(),
            weight: 1.0,
        });

        router.add_rule(RoutingRule {
            name: "queue2".to_string(),
            condition: RouteCondition::Always,
            target_queue: "q2".to_string(),
            weight: 1.0,
        });

        assert_eq!(router.route(b"test", None), "q1");
        assert_eq!(router.route(b"test", None), "q2");
        assert_eq!(router.route(b"test", None), "q1");
    }

    #[test]
    fn test_multicast_routing() {
        let mut router = MessageRouter::new("default");

        router.add_rule(RoutingRule {
            name: "rule1".to_string(),
            condition: RouteCondition::Always,
            target_queue: "q1".to_string(),
            weight: 1.0,
        });

        router.add_rule(RoutingRule {
            name: "rule2".to_string(),
            condition: RouteCondition::Always,
            target_queue: "q2".to_string(),
            weight: 1.0,
        });

        let queues = router.route_multicast(b"test", None);
        assert_eq!(queues.len(), 2);
        assert!(queues.contains(&"q1".to_string()));
        assert!(queues.contains(&"q2".to_string()));
    }

    #[test]
    fn test_route_statistics() {
        let mut router = MessageRouter::new("default");

        router.add_rule(RoutingRule {
            name: "test_rule".to_string(),
            condition: RouteCondition::Always,
            target_queue: "test_queue".to_string(),
            weight: 1.0,
        });

        router.route(b"test", None);
        router.route(b"test", None);

        let stats = router.stats();
        assert_eq!(stats.total_routed, 2);
        assert_eq!(stats.queue_counts.get("test_queue"), Some(&2));
        assert_eq!(stats.rule_matches.get("test_rule"), Some(&2));
    }

    #[test]
    fn test_route_builder() {
        let rule = RouteBuilder::new("test")
            .condition(RouteCondition::Priority(5))
            .to_queue("target")
            .with_weight(0.5)
            .build()
            .unwrap();

        assert_eq!(rule.name, "test");
        assert_eq!(rule.target_queue, "target");
        assert_eq!(rule.weight, 0.5);
    }

    #[test]
    fn test_fallback_routing() {
        let mut router = MessageRouter::new("fallback");

        router.add_rule(RoutingRule {
            name: "specific".to_string(),
            condition: RouteCondition::Priority(9),
            target_queue: "specific_queue".to_string(),
            weight: 1.0,
        });

        // Should use fallback for non-matching messages
        let queue = router.route(b"test", Some(5));
        assert_eq!(queue, "fallback");
        assert_eq!(router.stats().fallback_count, 1);
    }
}
