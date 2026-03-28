//! AMQP Topic Routing Integration
//!
//! Maps task name patterns to AMQP topic exchange routing keys.
//! Enables automatic routing of tasks to specific queues based on
//! task name patterns using glob-style matching.
//!
//! # Example
//! ```rust
//! use celers_broker_amqp::topic_routing::{AmqpRoutingConfig, TopicRoutingRule, TopicRouter};
//!
//! let config = AmqpRoutingConfig::new()
//!     .with_rule(TopicRoutingRule::new("email-tasks", "task.email.*", "tasks.email"))
//!     .with_rule(TopicRoutingRule::new("report-tasks", "task.report.*", "tasks.report"));
//!
//! let router = TopicRouter::new(config);
//! assert_eq!(router.resolve_routing_key("task.email.send"), "tasks.email");
//! assert_eq!(router.resolve_routing_key("task.unknown"), "task.default");
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// TopicRoutingRule
// ============================================================================

/// A single routing rule that maps a task name pattern to a routing key.
///
/// Rules are matched by priority (higher = checked first). When a task name
/// matches the glob pattern, the associated routing key is used for AMQP
/// topic exchange routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRoutingRule {
    /// Human-readable name for this rule
    pub name: String,
    /// Glob pattern to match task names against.
    /// Segments are separated by `.`:
    /// - `*` matches any single segment (between dots)
    /// - `**` or `#` matches any number of segments
    pub task_pattern: String,
    /// The AMQP routing key to use when the pattern matches
    pub routing_key: String,
    /// Priority for rule evaluation (higher = checked first)
    pub priority: u8,
    /// Whether this rule is active
    pub enabled: bool,
}

impl TopicRoutingRule {
    /// Create a new routing rule with default priority (0) and enabled.
    pub fn new(
        name: impl Into<String>,
        task_pattern: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            task_pattern: task_pattern.into(),
            routing_key: routing_key.into(),
            priority: 0,
            enabled: true,
        }
    }

    /// Set the priority for this rule (higher = checked first).
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set whether this rule is enabled.
    #[must_use]
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

// ============================================================================
// AmqpRoutingConfig
// ============================================================================

/// Configuration for AMQP topic routing.
///
/// Holds the exchange name, a set of routing rules, and a default routing key
/// used when no rules match a given task name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpRoutingConfig {
    /// Name of the AMQP topic exchange
    pub exchange_name: String,
    /// Ordered list of routing rules
    pub routing_rules: Vec<TopicRoutingRule>,
    /// Routing key used when no rules match
    pub default_routing_key: String,
}

impl Default for AmqpRoutingConfig {
    fn default() -> Self {
        Self {
            exchange_name: "celery.topic".to_string(),
            routing_rules: Vec::new(),
            default_routing_key: "task.default".to_string(),
        }
    }
}

impl AmqpRoutingConfig {
    /// Create a new routing config with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the exchange name.
    #[must_use]
    pub fn with_exchange(mut self, name: impl Into<String>) -> Self {
        self.exchange_name = name.into();
        self
    }

    /// Set the default routing key.
    #[must_use]
    pub fn with_default_routing_key(mut self, key: impl Into<String>) -> Self {
        self.default_routing_key = key.into();
        self
    }

    /// Add a routing rule.
    #[must_use]
    pub fn with_rule(mut self, rule: TopicRoutingRule) -> Self {
        self.routing_rules.push(rule);
        self
    }
}

// ============================================================================
// Pattern matching internals
// ============================================================================

/// Internal pattern segment for matching dot-separated task names.
#[derive(Debug, Clone)]
enum PatternSegment {
    /// Matches a specific literal segment
    Literal(String),
    /// `*` — matches any single segment (between dots)
    SingleWild,
    /// `**` or `#` — matches any number of segments (zero or more)
    DoubleWild,
}

/// A compiled routing rule for efficient matching.
#[derive(Debug)]
struct CompiledRule {
    rule: TopicRoutingRule,
    segments: Vec<PatternSegment>,
}

impl CompiledRule {
    /// Compile a routing rule's pattern into segments for efficient matching.
    fn compile(rule: TopicRoutingRule) -> Self {
        let segments = rule
            .task_pattern
            .split('.')
            .map(|part| match part {
                "*" => PatternSegment::SingleWild,
                "**" | "#" => PatternSegment::DoubleWild,
                other => PatternSegment::Literal(other.to_string()),
            })
            .collect();
        Self { rule, segments }
    }

    /// Check whether a dot-separated task name matches this compiled pattern.
    fn matches(&self, task_name: &str) -> bool {
        let parts: Vec<&str> = task_name.split('.').collect();
        Self::match_segments(&self.segments, &parts)
    }

    /// Recursive segment matching algorithm.
    ///
    /// - `Literal`: exact match on current part, then advance both
    /// - `SingleWild`: match any single part, then advance both
    /// - `DoubleWild`: try consuming 0, 1, 2, … parts from the input
    fn match_segments(segments: &[PatternSegment], parts: &[&str]) -> bool {
        if segments.is_empty() {
            return parts.is_empty();
        }

        match &segments[0] {
            PatternSegment::Literal(s) => {
                if parts.is_empty() || parts[0] != s.as_str() {
                    return false;
                }
                Self::match_segments(&segments[1..], &parts[1..])
            }
            PatternSegment::SingleWild => {
                if parts.is_empty() {
                    return false;
                }
                Self::match_segments(&segments[1..], &parts[1..])
            }
            PatternSegment::DoubleWild => {
                // Try consuming 0 through all remaining parts
                for i in 0..=parts.len() {
                    if Self::match_segments(&segments[1..], &parts[i..]) {
                        return true;
                    }
                }
                false
            }
        }
    }
}

// ============================================================================
// TopicRoutingStats
// ============================================================================

/// Statistics for topic routing decisions.
///
/// Thread-safe counters that track how many messages were routed by each rule
/// and how many fell through to the default routing key.
#[derive(Debug, Default)]
pub struct TopicRoutingStats {
    total_routed: AtomicU64,
    default_fallbacks: AtomicU64,
    per_rule_matches: std::sync::Mutex<HashMap<String, u64>>,
}

impl TopicRoutingStats {
    /// Create empty stats.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of routing decisions made.
    pub fn total_routed(&self) -> u64 {
        self.total_routed.load(Ordering::Relaxed)
    }

    /// Number of routing decisions that fell through to the default key.
    pub fn default_fallbacks(&self) -> u64 {
        self.default_fallbacks.load(Ordering::Relaxed)
    }

    /// Per-rule match counts (cloned snapshot).
    pub fn per_rule_matches(&self) -> HashMap<String, u64> {
        self.per_rule_matches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Record a match against a named rule.
    fn record_match(&self, rule_name: &str) {
        self.total_routed.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.per_rule_matches.lock() {
            *map.entry(rule_name.to_string()).or_insert(0) += 1;
        }
    }

    /// Record a fallback to the default routing key.
    fn record_default(&self) {
        self.total_routed.fetch_add(1, Ordering::Relaxed);
        self.default_fallbacks.fetch_add(1, Ordering::Relaxed);
    }
}

// ============================================================================
// TopicRouter
// ============================================================================

/// The main topic router that resolves task names to AMQP routing keys.
///
/// Rules are compiled once on construction and sorted by priority (descending).
/// The first matching rule wins; if none match, the default routing key is used.
pub struct TopicRouter {
    config: AmqpRoutingConfig,
    compiled_rules: Vec<CompiledRule>,
    stats: TopicRoutingStats,
}

impl TopicRouter {
    /// Create a new router from the given configuration.
    ///
    /// Only enabled rules are compiled; rules are sorted by priority (descending).
    #[must_use]
    pub fn new(config: AmqpRoutingConfig) -> Self {
        let mut compiled_rules: Vec<CompiledRule> = config
            .routing_rules
            .iter()
            .filter(|r| r.enabled)
            .cloned()
            .map(CompiledRule::compile)
            .collect();
        // Sort by priority descending so higher-priority rules are checked first
        compiled_rules.sort_by_key(|r| std::cmp::Reverse(r.rule.priority));

        Self {
            config,
            compiled_rules,
            stats: TopicRoutingStats::new(),
        }
    }

    /// Resolve a task name to a routing key.
    ///
    /// Returns the routing key from the first matching rule (by priority),
    /// or the default routing key if no rules match.
    pub fn resolve_routing_key(&self, task_name: &str) -> &str {
        for compiled in &self.compiled_rules {
            if compiled.matches(task_name) {
                self.stats.record_match(&compiled.rule.name);
                return &compiled.rule.routing_key;
            }
        }
        self.stats.record_default();
        &self.config.default_routing_key
    }

    /// Add a new routing rule dynamically.
    ///
    /// The rule is compiled and inserted, then all rules are re-sorted by priority.
    pub fn add_rule(&mut self, rule: TopicRoutingRule) {
        if rule.enabled {
            let compiled = CompiledRule::compile(rule.clone());
            self.compiled_rules.push(compiled);
            // Re-sort by priority descending
            self.compiled_rules
                .sort_by_key(|r| std::cmp::Reverse(r.rule.priority));
        }
        self.config.routing_rules.push(rule);
    }

    /// Remove a rule by name.
    ///
    /// Returns the removed rule if found, or `None` if no rule with that name exists.
    pub fn remove_rule(&mut self, name: &str) -> Option<TopicRoutingRule> {
        if let Some(pos) = self.compiled_rules.iter().position(|r| r.rule.name == name) {
            let removed = self.compiled_rules.remove(pos);
            self.config.routing_rules.retain(|r| r.name != name);
            Some(removed.rule)
        } else {
            // Also check config in case the rule was disabled (not in compiled_rules)
            let pos = self
                .config
                .routing_rules
                .iter()
                .position(|r| r.name == name);
            if let Some(idx) = pos {
                Some(self.config.routing_rules.remove(idx))
            } else {
                None
            }
        }
    }

    /// Get all configured rules (including disabled ones).
    #[must_use]
    pub fn rules(&self) -> &[TopicRoutingRule] {
        &self.config.routing_rules
    }

    /// Get the number of active (compiled) rules.
    #[must_use]
    pub fn rule_count(&self) -> usize {
        self.compiled_rules.len()
    }

    /// Get routing statistics.
    #[must_use]
    pub fn stats(&self) -> &TopicRoutingStats {
        &self.stats
    }

    /// Get the routing configuration.
    #[must_use]
    pub fn config(&self) -> &AmqpRoutingConfig {
        &self.config
    }

    /// Get the exchange name.
    #[must_use]
    pub fn exchange_name(&self) -> &str {
        &self.config.exchange_name
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_config_defaults() {
        let config = AmqpRoutingConfig::default();
        assert_eq!(config.exchange_name, "celery.topic");
        assert_eq!(config.default_routing_key, "task.default");
        assert!(config.routing_rules.is_empty());
    }

    #[test]
    fn test_exact_match() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "exact",
            "task.email.send",
            "queue.email",
        ));
        let router = TopicRouter::new(config);
        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.email");
    }

    #[test]
    fn test_single_wildcard() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "email",
            "task.email.*",
            "queue.email",
        ));
        let router = TopicRouter::new(config);
        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.email");
        assert_eq!(
            router.resolve_routing_key("task.email.receive"),
            "queue.email"
        );
        // Single wildcard should NOT match multiple segments
        assert_eq!(
            router.resolve_routing_key("task.email.send.retry"),
            "task.default"
        );
    }

    #[test]
    fn test_double_wildcard() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "email-all",
            "task.email.**",
            "queue.email.all",
        ));
        let router = TopicRouter::new(config);
        assert_eq!(
            router.resolve_routing_key("task.email.send"),
            "queue.email.all"
        );
        assert_eq!(
            router.resolve_routing_key("task.email.send.retry"),
            "queue.email.all"
        );
        // Also matches zero additional segments
        assert_eq!(router.resolve_routing_key("task.email"), "queue.email.all");
    }

    #[test]
    fn test_hash_wildcard() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "all-tasks",
            "task.#",
            "queue.all",
        ));
        let router = TopicRouter::new(config);
        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.all");
        assert_eq!(
            router.resolve_routing_key("task.report.generate.pdf"),
            "queue.all"
        );
        assert_eq!(router.resolve_routing_key("task"), "queue.all");
    }

    #[test]
    fn test_no_match_falls_to_default() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "email",
            "task.email.*",
            "queue.email",
        ));
        let router = TopicRouter::new(config);
        assert_eq!(
            router.resolve_routing_key("task.report.generate"),
            "task.default"
        );
        assert_eq!(router.resolve_routing_key("other.thing"), "task.default");
    }

    #[test]
    fn test_priority_ordering() {
        let config = AmqpRoutingConfig::new()
            .with_rule(
                TopicRoutingRule::new("general", "task.*.*", "queue.general").with_priority(1),
            )
            .with_rule(
                TopicRoutingRule::new("email-specific", "task.email.*", "queue.email")
                    .with_priority(10),
            );
        let router = TopicRouter::new(config);
        // Higher priority rule should win
        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.email");
        // Non-email tasks still match the general rule
        assert_eq!(
            router.resolve_routing_key("task.report.gen"),
            "queue.general"
        );
    }

    #[test]
    fn test_add_rule_dynamic() {
        let config = AmqpRoutingConfig::new();
        let mut router = TopicRouter::new(config);

        assert_eq!(
            router.resolve_routing_key("task.email.send"),
            "task.default"
        );

        router.add_rule(TopicRoutingRule::new(
            "email",
            "task.email.*",
            "queue.email",
        ));
        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.email");
        assert_eq!(router.rule_count(), 1);
    }

    #[test]
    fn test_remove_rule() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "email",
            "task.email.*",
            "queue.email",
        ));
        let mut router = TopicRouter::new(config);

        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.email");

        let removed = router.remove_rule("email");
        assert!(removed.is_some());
        assert_eq!(removed.as_ref().map(|r| r.name.as_str()), Some("email"));
        assert_eq!(
            router.resolve_routing_key("task.email.send"),
            "task.default"
        );
        assert_eq!(router.rule_count(), 0);
    }

    #[test]
    fn test_remove_nonexistent_rule() {
        let config = AmqpRoutingConfig::new();
        let mut router = TopicRouter::new(config);
        assert!(router.remove_rule("nonexistent").is_none());
    }

    #[test]
    fn test_disabled_rule_skipped() {
        let config = AmqpRoutingConfig::new().with_rule(
            TopicRoutingRule::new("email", "task.email.*", "queue.email").with_enabled(false),
        );
        let router = TopicRouter::new(config);
        // Disabled rule should not match
        assert_eq!(
            router.resolve_routing_key("task.email.send"),
            "task.default"
        );
        // But it should still be in the config
        assert_eq!(router.rules().len(), 1);
        assert_eq!(router.rule_count(), 0); // 0 compiled/active rules
    }

    #[test]
    fn test_stats_tracking() {
        let config = AmqpRoutingConfig::new().with_rule(TopicRoutingRule::new(
            "email",
            "task.email.*",
            "queue.email",
        ));
        let router = TopicRouter::new(config);

        let _ = router.resolve_routing_key("task.email.send");
        let _ = router.resolve_routing_key("task.email.receive");
        let _ = router.resolve_routing_key("task.unknown");

        assert_eq!(router.stats().total_routed(), 3);
        assert_eq!(router.stats().default_fallbacks(), 1);

        let per_rule = router.stats().per_rule_matches();
        assert_eq!(per_rule.get("email").copied(), Some(2));
    }

    #[test]
    fn test_empty_router() {
        let config = AmqpRoutingConfig::new();
        let router = TopicRouter::new(config);

        assert_eq!(router.resolve_routing_key("anything"), "task.default");
        assert_eq!(
            router.resolve_routing_key("task.email.send"),
            "task.default"
        );
        assert_eq!(router.rule_count(), 0);
    }

    #[test]
    fn test_complex_patterns() {
        let config = AmqpRoutingConfig::new()
            .with_rule(TopicRoutingRule::new(
                "middle-wild",
                "*.*.send",
                "queue.send",
            ))
            .with_rule(TopicRoutingRule::new(
                "prefix-match",
                "task.**.final",
                "queue.final",
            ));
        let router = TopicRouter::new(config);

        // *.*.send should match any two segments followed by "send"
        assert_eq!(router.resolve_routing_key("task.email.send"), "queue.send");
        assert_eq!(router.resolve_routing_key("job.report.send"), "queue.send");
        // Should not match wrong suffix
        assert_eq!(
            router.resolve_routing_key("task.email.receive"),
            "task.default"
        );

        // task.**.final matches task + any segments + final
        assert_eq!(router.resolve_routing_key("task.a.b.final"), "queue.final");
        assert_eq!(router.resolve_routing_key("task.final"), "queue.final");
    }

    #[test]
    fn test_config_builder() {
        let config = AmqpRoutingConfig::new()
            .with_exchange("my.exchange")
            .with_default_routing_key("my.default")
            .with_rule(TopicRoutingRule::new("r1", "task.*", "q1"));

        assert_eq!(config.exchange_name, "my.exchange");
        assert_eq!(config.default_routing_key, "my.default");
        assert_eq!(config.routing_rules.len(), 1);
    }

    #[test]
    fn test_rule_builder() {
        let rule = TopicRoutingRule::new("test", "task.*", "queue")
            .with_priority(5)
            .with_enabled(false);
        assert_eq!(rule.name, "test");
        assert_eq!(rule.priority, 5);
        assert!(!rule.enabled);
    }

    #[test]
    fn test_exchange_name() {
        let config = AmqpRoutingConfig::new().with_exchange("custom.exchange");
        let router = TopicRouter::new(config);
        assert_eq!(router.exchange_name(), "custom.exchange");
    }

    #[test]
    fn test_add_disabled_rule_dynamic() {
        let config = AmqpRoutingConfig::new();
        let mut router = TopicRouter::new(config);

        router.add_rule(TopicRoutingRule::new("disabled", "task.*", "queue").with_enabled(false));
        // Rule is in config but not compiled
        assert_eq!(router.rules().len(), 1);
        assert_eq!(router.rule_count(), 0);
        assert_eq!(router.resolve_routing_key("task.foo"), "task.default");
    }
}
