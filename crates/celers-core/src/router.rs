//! Task Routing
//!
//! This module provides task routing capabilities for directing tasks to appropriate
//! queues based on various criteria:
//!
//! - **Name Patterns**: Route tasks based on glob or regex patterns
//! - **Queue Routing**: Route tasks to specific queues based on task type
//! - **Priority Routing**: Route tasks based on priority levels
//! - **Custom Strategies**: Implement custom routing logic
//!
//! # Example
//!
//! ```rust
//! use celers_core::router::{Router, RouteRule, PatternMatcher};
//!
//! // Create a router with rules
//! let mut router = Router::new();
//!
//! // Route all tasks starting with "email." to the "email" queue
//! router.add_rule(RouteRule::new(
//!     PatternMatcher::glob("email.*"),
//!     "email"
//! ));
//!
//! // Route high priority tasks to "high_priority" queue
//! router.add_rule(RouteRule::new(
//!     PatternMatcher::glob("urgent.*"),
//!     "high_priority"
//! ).with_priority(10));
//!
//! // Get the queue for a task
//! assert_eq!(router.route("email.send_newsletter"), Some("email".to_string()));
//! ```

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pattern matching strategy for task routing
#[derive(Debug, Clone)]
pub enum PatternMatcher {
    /// Exact name match
    Exact(String),
    /// Glob pattern (supports * and ?)
    Glob(GlobPattern),
    /// Regular expression pattern
    Regex(RegexPattern),
    /// Match all tasks
    All,
}

impl PatternMatcher {
    /// Create an exact match pattern
    pub fn exact(name: impl Into<String>) -> Self {
        Self::Exact(name.into())
    }

    /// Create a glob pattern matcher
    ///
    /// Supports:
    /// - `*` matches any sequence of characters
    /// - `?` matches any single character
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_core::router::PatternMatcher;
    ///
    /// let matcher = PatternMatcher::glob("tasks.*");
    /// assert!(matcher.matches("tasks.add"));
    /// assert!(matcher.matches("tasks.multiply"));
    /// assert!(!matcher.matches("other.task"));
    /// ```
    pub fn glob(pattern: impl Into<String>) -> Self {
        Self::Glob(GlobPattern::new(pattern))
    }

    /// Create a regex pattern matcher
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_core::router::PatternMatcher;
    ///
    /// let matcher = PatternMatcher::regex(r"tasks\.[a-z]+").unwrap();
    /// assert!(matcher.matches("tasks.add"));
    /// assert!(!matcher.matches("tasks.Add"));
    /// ```
    pub fn regex(pattern: &str) -> Result<Self, regex::Error> {
        Ok(Self::Regex(RegexPattern::new(pattern)?))
    }

    /// Create a matcher that matches all tasks
    pub fn all() -> Self {
        Self::All
    }

    /// Check if a task name matches this pattern
    pub fn matches(&self, task_name: &str) -> bool {
        match self {
            Self::Exact(name) => task_name == name,
            Self::Glob(glob) => glob.matches(task_name),
            Self::Regex(regex) => regex.matches(task_name),
            Self::All => true,
        }
    }
}

/// Glob pattern for task name matching
#[derive(Debug, Clone)]
pub struct GlobPattern {
    pattern: String,
    regex: Regex,
}

impl GlobPattern {
    /// Create a new glob pattern
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let regex_str = glob_to_regex(&pattern);
        let regex = Regex::new(&regex_str).expect("Invalid glob pattern");
        Self { pattern, regex }
    }

    /// Check if a task name matches this glob pattern
    pub fn matches(&self, task_name: &str) -> bool {
        self.regex.is_match(task_name)
    }

    /// Get the original pattern string
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// Regular expression pattern for task name matching
#[derive(Debug, Clone)]
pub struct RegexPattern {
    pattern: String,
    regex: Regex,
}

impl RegexPattern {
    /// Create a new regex pattern
    pub fn new(pattern: &str) -> Result<Self, regex::Error> {
        let regex = Regex::new(pattern)?;
        Ok(Self {
            pattern: pattern.to_string(),
            regex,
        })
    }

    /// Check if a task name matches this regex pattern
    pub fn matches(&self, task_name: &str) -> bool {
        self.regex.is_match(task_name)
    }

    /// Get the original pattern string
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// Convert a glob pattern to a regex pattern
fn glob_to_regex(glob: &str) -> String {
    let mut regex = String::with_capacity(glob.len() * 2 + 2);
    regex.push('^');

    for c in glob.chars() {
        match c {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }

    regex.push('$');
    regex
}

/// A routing rule that maps task names to queues
#[derive(Debug, Clone)]
pub struct RouteRule {
    /// Pattern matcher for task names
    pub matcher: PatternMatcher,
    /// Target queue name
    pub queue: String,
    /// Rule priority (higher = evaluated first)
    pub priority: i32,
    /// Optional routing key (for AMQP exchanges)
    pub routing_key: Option<String>,
    /// Optional exchange name (for AMQP)
    pub exchange: Option<String>,
}

impl RouteRule {
    /// Create a new routing rule
    pub fn new(matcher: PatternMatcher, queue: impl Into<String>) -> Self {
        Self {
            matcher,
            queue: queue.into(),
            priority: 0,
            routing_key: None,
            exchange: None,
        }
    }

    /// Set the rule priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the routing key (for AMQP)
    pub fn with_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = Some(routing_key.into());
        self
    }

    /// Set the exchange name (for AMQP)
    pub fn with_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = Some(exchange.into());
        self
    }

    /// Check if this rule matches a task name
    pub fn matches(&self, task_name: &str) -> bool {
        self.matcher.matches(task_name)
    }
}

/// Routing result containing queue and optional AMQP settings
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteResult {
    /// Target queue name
    pub queue: String,
    /// Optional routing key (for AMQP)
    pub routing_key: Option<String>,
    /// Optional exchange name (for AMQP)
    pub exchange: Option<String>,
}

impl RouteResult {
    /// Create a new route result
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            routing_key: None,
            exchange: None,
        }
    }

    /// Create from a route rule
    pub fn from_rule(rule: &RouteRule) -> Self {
        Self {
            queue: rule.queue.clone(),
            routing_key: rule.routing_key.clone(),
            exchange: rule.exchange.clone(),
        }
    }
}

/// Task router for directing tasks to appropriate queues
#[derive(Debug, Default)]
pub struct Router {
    /// Routing rules (sorted by priority)
    rules: Vec<RouteRule>,
    /// Default queue for unmatched tasks
    default_queue: Option<String>,
    /// Direct task-to-queue mappings
    direct_routes: HashMap<String, RouteResult>,
}

impl Router {
    /// Create a new empty router
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a router with a default queue
    pub fn with_default_queue(queue: impl Into<String>) -> Self {
        Self {
            rules: Vec::new(),
            default_queue: Some(queue.into()),
            direct_routes: HashMap::new(),
        }
    }

    /// Add a routing rule
    ///
    /// Rules are sorted by priority (higher priority rules are evaluated first)
    pub fn add_rule(&mut self, rule: RouteRule) {
        self.rules.push(rule);
        // Sort by priority (descending)
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Add a direct route for a specific task
    ///
    /// Direct routes take precedence over pattern-based rules
    pub fn add_direct_route(&mut self, task_name: impl Into<String>, result: RouteResult) {
        self.direct_routes.insert(task_name.into(), result);
    }

    /// Set the default queue for unmatched tasks
    pub fn set_default_queue(&mut self, queue: impl Into<String>) {
        self.default_queue = Some(queue.into());
    }

    /// Route a task to a queue
    ///
    /// Returns `None` if no matching rule and no default queue
    pub fn route(&self, task_name: &str) -> Option<String> {
        self.route_full(task_name).map(|r| r.queue)
    }

    /// Route a task and get full routing information
    ///
    /// Returns `None` if no matching rule and no default queue
    pub fn route_full(&self, task_name: &str) -> Option<RouteResult> {
        // Check direct routes first
        if let Some(result) = self.direct_routes.get(task_name) {
            return Some(result.clone());
        }

        // Check pattern-based rules
        for rule in &self.rules {
            if rule.matches(task_name) {
                return Some(RouteResult::from_rule(rule));
            }
        }

        // Fall back to default queue
        self.default_queue.as_ref().map(RouteResult::new)
    }

    /// Check if a task has any matching route
    pub fn has_route(&self, task_name: &str) -> bool {
        self.direct_routes.contains_key(task_name)
            || self.rules.iter().any(|r| r.matches(task_name))
            || self.default_queue.is_some()
    }

    /// Get all registered rules
    pub fn rules(&self) -> &[RouteRule] {
        &self.rules
    }

    /// Remove all rules matching a pattern
    pub fn remove_rules_by_queue(&mut self, queue: &str) {
        self.rules.retain(|r| r.queue != queue);
    }

    /// Clear all rules
    pub fn clear(&mut self) {
        self.rules.clear();
        self.direct_routes.clear();
    }
}

/// Builder for creating routers with fluent API
#[derive(Debug, Default)]
pub struct RouterBuilder {
    router: Router,
}

impl RouterBuilder {
    /// Create a new router builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule that routes tasks matching a glob pattern to a queue
    pub fn route_glob(mut self, pattern: &str, queue: &str) -> Self {
        self.router
            .add_rule(RouteRule::new(PatternMatcher::glob(pattern), queue));
        self
    }

    /// Add a rule that routes tasks matching a regex pattern to a queue
    pub fn route_regex(mut self, pattern: &str, queue: &str) -> Result<Self, regex::Error> {
        self.router
            .add_rule(RouteRule::new(PatternMatcher::regex(pattern)?, queue));
        Ok(self)
    }

    /// Add a rule that routes a specific task to a queue
    pub fn route_exact(mut self, task_name: &str, queue: &str) -> Self {
        self.router
            .add_rule(RouteRule::new(PatternMatcher::exact(task_name), queue));
        self
    }

    /// Add a direct route for a specific task
    pub fn direct_route(mut self, task_name: &str, queue: &str) -> Self {
        self.router
            .add_direct_route(task_name, RouteResult::new(queue));
        self
    }

    /// Set the default queue for unmatched tasks
    pub fn default_queue(mut self, queue: &str) -> Self {
        self.router.set_default_queue(queue);
        self
    }

    /// Build the router
    pub fn build(self) -> Router {
        self.router
    }
}

/// Serializable routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Default queue for unmatched tasks
    #[serde(default)]
    pub default_queue: Option<String>,
    /// Routing rules (glob pattern -> queue)
    #[serde(default)]
    pub routes: HashMap<String, String>,
    /// Direct task-to-queue mappings
    #[serde(default)]
    pub task_routes: HashMap<String, String>,
}

impl RoutingConfig {
    /// Create a new empty routing configuration
    pub fn new() -> Self {
        Self {
            default_queue: None,
            routes: HashMap::new(),
            task_routes: HashMap::new(),
        }
    }

    /// Create a router from this configuration
    pub fn into_router(self) -> Router {
        let mut router = match self.default_queue {
            Some(queue) => Router::with_default_queue(queue),
            None => Router::new(),
        };

        // Add glob pattern routes
        for (pattern, queue) in self.routes {
            router.add_rule(RouteRule::new(PatternMatcher::glob(&pattern), queue));
        }

        // Add direct task routes
        for (task_name, queue) in self.task_routes {
            router.add_direct_route(task_name, RouteResult::new(queue));
        }

        router
    }
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_pattern() {
        let matcher = PatternMatcher::exact("tasks.add");
        assert!(matcher.matches("tasks.add"));
        assert!(!matcher.matches("tasks.multiply"));
        assert!(!matcher.matches("tasks"));
    }

    #[test]
    fn test_glob_pattern() {
        let matcher = PatternMatcher::glob("tasks.*");
        assert!(matcher.matches("tasks.add"));
        assert!(matcher.matches("tasks.multiply"));
        assert!(!matcher.matches("other.task"));
        assert!(!matcher.matches("tasks"));

        let matcher = PatternMatcher::glob("*.add");
        assert!(matcher.matches("tasks.add"));
        assert!(matcher.matches("math.add"));
        assert!(!matcher.matches("tasks.multiply"));

        let matcher = PatternMatcher::glob("task?");
        assert!(matcher.matches("task1"));
        assert!(matcher.matches("taskA"));
        assert!(matcher.matches("tasks")); // 's' is a single character, so it matches
        assert!(!matcher.matches("task")); // No character after "task"
        assert!(!matcher.matches("task12")); // Two characters after "task"
    }

    #[test]
    fn test_regex_pattern() {
        let matcher = PatternMatcher::regex(r"tasks\.[a-z]+").unwrap();
        assert!(matcher.matches("tasks.add"));
        assert!(matcher.matches("tasks.multiply"));
        assert!(!matcher.matches("tasks.Add"));
        assert!(!matcher.matches("tasks.123"));

        let matcher = PatternMatcher::regex(r"^(email|sms)\.").unwrap();
        assert!(matcher.matches("email.send"));
        assert!(matcher.matches("sms.send"));
        assert!(!matcher.matches("push.send"));
    }

    #[test]
    fn test_all_pattern() {
        let matcher = PatternMatcher::all();
        assert!(matcher.matches("anything"));
        assert!(matcher.matches(""));
        assert!(matcher.matches("complex.task.name"));
    }

    #[test]
    fn test_router_basic() {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(PatternMatcher::glob("email.*"), "email"));
        router.add_rule(RouteRule::new(PatternMatcher::glob("sms.*"), "sms"));

        assert_eq!(router.route("email.send"), Some("email".to_string()));
        assert_eq!(router.route("sms.notify"), Some("sms".to_string()));
        assert_eq!(router.route("push.notify"), None);
    }

    #[test]
    fn test_router_with_default() {
        let mut router = Router::with_default_queue("default");
        router.add_rule(RouteRule::new(PatternMatcher::glob("email.*"), "email"));

        assert_eq!(router.route("email.send"), Some("email".to_string()));
        assert_eq!(router.route("other.task"), Some("default".to_string()));
    }

    #[test]
    fn test_router_priority() {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(PatternMatcher::glob("*"), "default").with_priority(0));
        router
            .add_rule(RouteRule::new(PatternMatcher::glob("urgent.*"), "urgent").with_priority(10));

        // Urgent tasks should go to urgent queue
        assert_eq!(router.route("urgent.email"), Some("urgent".to_string()));
        // Other tasks should go to default queue
        assert_eq!(router.route("email.send"), Some("default".to_string()));
    }

    #[test]
    fn test_router_direct_route() {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(PatternMatcher::glob("tasks.*"), "tasks"));
        router.add_direct_route("tasks.special", RouteResult::new("special"));

        // Direct route takes precedence
        assert_eq!(router.route("tasks.special"), Some("special".to_string()));
        // Pattern-based route
        assert_eq!(router.route("tasks.normal"), Some("tasks".to_string()));
    }

    #[test]
    fn test_route_result() {
        let mut router = Router::new();
        router.add_rule(
            RouteRule::new(PatternMatcher::glob("amqp.*"), "amqp_queue")
                .with_routing_key("amqp.routing")
                .with_exchange("amqp_exchange"),
        );

        let result = router.route_full("amqp.task").unwrap();
        assert_eq!(result.queue, "amqp_queue");
        assert_eq!(result.routing_key, Some("amqp.routing".to_string()));
        assert_eq!(result.exchange, Some("amqp_exchange".to_string()));
    }

    #[test]
    fn test_router_builder() {
        let router = RouterBuilder::new()
            .route_glob("email.*", "email")
            .route_glob("sms.*", "sms")
            .direct_route("special.task", "special")
            .default_queue("default")
            .build();

        assert_eq!(router.route("email.send"), Some("email".to_string()));
        assert_eq!(router.route("sms.notify"), Some("sms".to_string()));
        assert_eq!(router.route("special.task"), Some("special".to_string()));
        assert_eq!(router.route("other.task"), Some("default".to_string()));
    }

    #[test]
    fn test_routing_config() {
        let mut config = RoutingConfig::new();
        config.default_queue = Some("default".to_string());
        config
            .routes
            .insert("email.*".to_string(), "email".to_string());
        config
            .task_routes
            .insert("special.task".to_string(), "special".to_string());

        let router = config.into_router();
        assert_eq!(router.route("email.send"), Some("email".to_string()));
        assert_eq!(router.route("special.task"), Some("special".to_string()));
        assert_eq!(router.route("other.task"), Some("default".to_string()));
    }

    #[test]
    fn test_routing_config_serialization() {
        let mut config = RoutingConfig::new();
        config.default_queue = Some("default".to_string());
        config
            .routes
            .insert("email.*".to_string(), "email".to_string());

        let json = serde_json::to_string(&config).unwrap();
        let parsed: RoutingConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.default_queue, Some("default".to_string()));
        assert_eq!(parsed.routes.get("email.*"), Some(&"email".to_string()));
    }

    #[test]
    fn test_glob_special_chars() {
        // Test escaping of regex special characters in glob patterns
        let matcher = PatternMatcher::glob("tasks.v1.0");
        assert!(matcher.matches("tasks.v1.0"));
        assert!(!matcher.matches("tasks.v1x0"));

        let matcher = PatternMatcher::glob("(test)");
        assert!(matcher.matches("(test)"));
        assert!(!matcher.matches("test"));
    }

    #[test]
    fn test_has_route() {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(PatternMatcher::glob("email.*"), "email"));

        assert!(router.has_route("email.send"));
        assert!(!router.has_route("sms.send"));

        router.set_default_queue("default");
        assert!(router.has_route("sms.send"));
    }

    #[test]
    fn test_remove_rules() {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(PatternMatcher::glob("email.*"), "email"));
        router.add_rule(RouteRule::new(PatternMatcher::glob("sms.*"), "sms"));

        router.remove_rules_by_queue("email");
        assert_eq!(router.route("email.send"), None);
        assert_eq!(router.route("sms.send"), Some("sms".to_string()));
    }

    #[test]
    fn test_clear() {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(PatternMatcher::glob("email.*"), "email"));
        router.add_direct_route("special", RouteResult::new("special"));

        router.clear();
        assert_eq!(router.route("email.send"), None);
        assert_eq!(router.route("special"), None);
    }
}
