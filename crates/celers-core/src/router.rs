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
    #[must_use]
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
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns an error if the regex pattern is invalid.
    pub fn regex(pattern: &str) -> Result<Self, regex::Error> {
        Ok(Self::Regex(RegexPattern::new(pattern)?))
    }

    /// Create a matcher that matches all tasks
    #[must_use]
    pub fn all() -> Self {
        Self::All
    }

    /// Check if a task name matches this pattern
    #[inline]
    #[must_use]
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
    ///
    /// # Panics
    ///
    /// Panics if the glob pattern cannot be converted to a valid regex.
    #[must_use]
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let regex_str = glob_to_regex(&pattern);
        let regex = Regex::new(&regex_str).expect("Invalid glob pattern");
        Self { pattern, regex }
    }

    /// Check if a task name matches this glob pattern
    #[inline]
    #[must_use]
    pub fn matches(&self, task_name: &str) -> bool {
        self.regex.is_match(task_name)
    }

    /// Get the original pattern string
    #[inline]
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns an error if the regex pattern is invalid.
    pub fn new(pattern: &str) -> Result<Self, regex::Error> {
        let regex = Regex::new(pattern)?;
        Ok(Self {
            pattern: pattern.to_string(),
            regex,
        })
    }

    /// Check if a task name matches this regex pattern
    #[inline]
    #[must_use]
    pub fn matches(&self, task_name: &str) -> bool {
        self.regex.is_match(task_name)
    }

    /// Get the original pattern string
    #[inline]
    #[must_use]
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
    /// Optional argument condition for argument-based routing
    pub argument_condition: Option<ArgumentCondition>,
}

impl RouteRule {
    /// Create a new routing rule
    #[must_use]
    pub fn new(matcher: PatternMatcher, queue: impl Into<String>) -> Self {
        Self {
            matcher,
            queue: queue.into(),
            priority: 0,
            routing_key: None,
            exchange: None,
            argument_condition: None,
        }
    }

    /// Set the rule priority
    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the routing key (for AMQP)
    #[must_use]
    pub fn with_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = Some(routing_key.into());
        self
    }

    /// Set the exchange name (for AMQP)
    #[must_use]
    pub fn with_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = Some(exchange.into());
        self
    }

    /// Set the argument condition for argument-based routing
    #[must_use]
    pub fn with_argument_condition(mut self, condition: ArgumentCondition) -> Self {
        self.argument_condition = Some(condition);
        self
    }

    /// Check if this rule matches a task name
    #[inline]
    #[must_use]
    pub fn matches(&self, task_name: &str) -> bool {
        self.matcher.matches(task_name)
    }

    /// Check if this rule matches a task name and arguments
    ///
    /// Returns true if:
    /// - The task name matches the pattern matcher
    /// - AND (if `argument_condition` is set) the arguments match the condition
    #[must_use]
    pub fn matches_with_args(
        &self,
        task_name: &str,
        args: &[serde_json::Value],
        kwargs: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        if !self.matcher.matches(task_name) {
            return false;
        }

        match &self.argument_condition {
            Some(condition) => condition.evaluate(args, kwargs),
            None => true,
        }
    }
}

// ============================================================================
// Argument-Based Routing
// ============================================================================

/// Condition for matching task arguments
///
/// Allows routing based on the content of task arguments or keyword arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ArgumentCondition {
    /// Check if a positional argument at index equals a value
    ArgEquals {
        /// Argument index (0-based)
        index: usize,
        /// Expected value
        value: serde_json::Value,
    },

    /// Check if a positional argument at index exists
    ArgExists {
        /// Argument index (0-based)
        index: usize,
    },

    /// Check if a keyword argument equals a value
    KwargEquals {
        /// Keyword argument name
        key: String,
        /// Expected value
        value: serde_json::Value,
    },

    /// Check if a keyword argument exists
    KwargExists {
        /// Keyword argument name
        key: String,
    },

    /// Check if a keyword argument matches a pattern
    KwargMatches {
        /// Keyword argument name
        key: String,
        /// Regex pattern
        pattern: String,
    },

    /// Check if a positional argument is greater than a threshold
    ArgGreaterThan {
        /// Argument index (0-based)
        index: usize,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a positional argument is less than a threshold
    ArgLessThan {
        /// Argument index (0-based)
        index: usize,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a keyword argument is greater than a threshold
    KwargGreaterThan {
        /// Keyword argument name
        key: String,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a keyword argument is less than a threshold
    KwargLessThan {
        /// Keyword argument name
        key: String,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a keyword argument contains a value (for strings/arrays)
    KwargContains {
        /// Keyword argument name
        key: String,
        /// Value to search for
        value: serde_json::Value,
    },

    /// Logical AND of multiple conditions
    And(Vec<ArgumentCondition>),

    /// Logical OR of multiple conditions
    Or(Vec<ArgumentCondition>),

    /// Logical NOT of a condition
    Not(Box<ArgumentCondition>),

    /// Always true (no argument condition)
    Always,
}

impl ArgumentCondition {
    /// Create a condition that checks if arg\[index\] == value
    #[must_use]
    pub fn arg_equals(index: usize, value: serde_json::Value) -> Self {
        Self::ArgEquals { index, value }
    }

    /// Create a condition that checks if arg\[index\] exists
    #[must_use]
    pub fn arg_exists(index: usize) -> Self {
        Self::ArgExists { index }
    }

    /// Create a condition that checks if kwargs\[key\] == value
    #[must_use]
    pub fn kwarg_equals(key: impl Into<String>, value: serde_json::Value) -> Self {
        Self::KwargEquals {
            key: key.into(),
            value,
        }
    }

    /// Create a condition that checks if kwargs\[key\] exists
    #[must_use]
    pub fn kwarg_exists(key: impl Into<String>) -> Self {
        Self::KwargExists { key: key.into() }
    }

    /// Create a condition that checks if kwargs\[key\] matches a regex pattern
    #[must_use]
    pub fn kwarg_matches(key: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::KwargMatches {
            key: key.into(),
            pattern: pattern.into(),
        }
    }

    /// Create a condition that checks if arg\[index\] > threshold
    #[must_use]
    pub fn arg_greater_than(index: usize, threshold: f64) -> Self {
        Self::ArgGreaterThan { index, threshold }
    }

    /// Create a condition that checks if arg\[index\] < threshold
    #[must_use]
    pub fn arg_less_than(index: usize, threshold: f64) -> Self {
        Self::ArgLessThan { index, threshold }
    }

    /// Create a condition that checks if kwargs\[key\] > threshold
    #[must_use]
    pub fn kwarg_greater_than(key: impl Into<String>, threshold: f64) -> Self {
        Self::KwargGreaterThan {
            key: key.into(),
            threshold,
        }
    }

    /// Create a condition that checks if kwargs\[key\] < threshold
    #[must_use]
    pub fn kwarg_less_than(key: impl Into<String>, threshold: f64) -> Self {
        Self::KwargLessThan {
            key: key.into(),
            threshold,
        }
    }

    /// Create a condition that checks if kwargs\[key\] contains value
    #[must_use]
    pub fn kwarg_contains(key: impl Into<String>, value: serde_json::Value) -> Self {
        Self::KwargContains {
            key: key.into(),
            value,
        }
    }

    /// Create an always-true condition
    #[must_use]
    pub fn always() -> Self {
        Self::Always
    }

    /// Combine with AND
    #[must_use]
    pub fn and(self, other: ArgumentCondition) -> Self {
        match self {
            Self::And(mut conditions) => {
                conditions.push(other);
                Self::And(conditions)
            }
            _ => Self::And(vec![self, other]),
        }
    }

    /// Combine with OR
    #[must_use]
    pub fn or(self, other: ArgumentCondition) -> Self {
        match self {
            Self::Or(mut conditions) => {
                conditions.push(other);
                Self::Or(conditions)
            }
            _ => Self::Or(vec![self, other]),
        }
    }

    /// Negate the condition
    #[must_use]
    pub fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }

    /// Evaluate the condition against task arguments
    ///
    /// # Arguments
    /// * `args` - Positional arguments as JSON values
    /// * `kwargs` - Keyword arguments as a JSON object
    #[must_use]
    pub fn evaluate(
        &self,
        args: &[serde_json::Value],
        kwargs: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        match self {
            Self::Always => true,

            Self::ArgEquals { index, value } => args.get(*index).is_some_and(|v| v == value),

            Self::ArgExists { index } => args.len() > *index,

            Self::KwargEquals { key, value } => kwargs.get(key).is_some_and(|v| v == value),

            Self::KwargExists { key } => kwargs.contains_key(key),

            Self::KwargMatches { key, pattern } => {
                if let Some(serde_json::Value::String(s)) = kwargs.get(key) {
                    Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            Self::ArgGreaterThan { index, threshold } => args
                .get(*index)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|v| v > *threshold),

            Self::ArgLessThan { index, threshold } => args
                .get(*index)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|v| v < *threshold),

            Self::KwargGreaterThan { key, threshold } => kwargs
                .get(key)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|v| v > *threshold),

            Self::KwargLessThan { key, threshold } => kwargs
                .get(key)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|v| v < *threshold),

            Self::KwargContains { key, value } => {
                if let Some(v) = kwargs.get(key) {
                    match v {
                        serde_json::Value::String(s) => {
                            if let Some(needle) = value.as_str() {
                                s.contains(needle)
                            } else {
                                false
                            }
                        }
                        serde_json::Value::Array(arr) => arr.contains(value),
                        _ => false,
                    }
                } else {
                    false
                }
            }

            Self::And(conditions) => conditions.iter().all(|c| c.evaluate(args, kwargs)),

            Self::Or(conditions) => conditions.iter().any(|c| c.evaluate(args, kwargs)),

            Self::Not(condition) => !condition.evaluate(args, kwargs),
        }
    }
}

impl std::fmt::Display for ArgumentCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "always"),
            Self::ArgEquals { index, value } => write!(f, "args[{index}] == {value}"),
            Self::ArgExists { index } => write!(f, "args[{index}] exists"),
            Self::KwargEquals { key, value } => write!(f, "kwargs[{key}] == {value}"),
            Self::KwargExists { key } => write!(f, "kwargs[{key}] exists"),
            Self::KwargMatches { key, pattern } => {
                write!(f, "kwargs[{key}] matches /{pattern}/")
            }
            Self::ArgGreaterThan { index, threshold } => {
                write!(f, "args[{index}] > {threshold}")
            }
            Self::ArgLessThan { index, threshold } => write!(f, "args[{index}] < {threshold}"),
            Self::KwargGreaterThan { key, threshold } => {
                write!(f, "kwargs[{key}] > {threshold}")
            }
            Self::KwargLessThan { key, threshold } => write!(f, "kwargs[{key}] < {threshold}"),
            Self::KwargContains { key, value } => {
                write!(f, "kwargs[{key}] contains {value}")
            }
            Self::And(conditions) => {
                let parts: Vec<String> = conditions.iter().map(|c| format!("{c}")).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            Self::Or(conditions) => {
                let parts: Vec<String> = conditions.iter().map(|c| format!("{c}")).collect();
                write!(f, "({})", parts.join(" OR "))
            }
            Self::Not(condition) => write!(f, "NOT ({condition})"),
        }
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
    #[must_use]
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            routing_key: None,
            exchange: None,
        }
    }

    /// Create from a route rule
    #[must_use]
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a router with a default queue
    #[must_use]
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
    #[must_use]
    pub fn route(&self, task_name: &str) -> Option<String> {
        self.route_full(task_name).map(|r| r.queue)
    }

    /// Route a task and get full routing information
    ///
    /// Returns `None` if no matching rule and no default queue
    #[must_use]
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

    /// Route a task with arguments to a queue
    ///
    /// This method considers both task name patterns and argument conditions.
    /// Returns `None` if no matching rule and no default queue.
    #[must_use]
    pub fn route_with_args(
        &self,
        task_name: &str,
        args: &[serde_json::Value],
        kwargs: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<String> {
        self.route_full_with_args(task_name, args, kwargs)
            .map(|r| r.queue)
    }

    /// Route a task with arguments and get full routing information
    ///
    /// This method considers both task name patterns and argument conditions.
    /// Returns `None` if no matching rule and no default queue.
    #[must_use]
    pub fn route_full_with_args(
        &self,
        task_name: &str,
        args: &[serde_json::Value],
        kwargs: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<RouteResult> {
        // Check direct routes first (direct routes don't have argument conditions)
        if let Some(result) = self.direct_routes.get(task_name) {
            return Some(result.clone());
        }

        // Check pattern-based rules with argument conditions
        for rule in &self.rules {
            if rule.matches_with_args(task_name, args, kwargs) {
                return Some(RouteResult::from_rule(rule));
            }
        }

        // Fall back to default queue
        self.default_queue.as_ref().map(RouteResult::new)
    }

    /// Check if a task has any matching route
    #[inline]
    #[must_use]
    pub fn has_route(&self, task_name: &str) -> bool {
        self.direct_routes.contains_key(task_name)
            || self.rules.iter().any(|r| r.matches(task_name))
            || self.default_queue.is_some()
    }

    /// Get all registered rules
    #[inline]
    #[must_use]
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule that routes tasks matching a glob pattern to a queue
    #[must_use]
    pub fn route_glob(mut self, pattern: &str, queue: &str) -> Self {
        self.router
            .add_rule(RouteRule::new(PatternMatcher::glob(pattern), queue));
        self
    }

    /// Add a rule that routes tasks matching a regex pattern to a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the regex pattern is invalid.
    pub fn route_regex(mut self, pattern: &str, queue: &str) -> Result<Self, regex::Error> {
        self.router
            .add_rule(RouteRule::new(PatternMatcher::regex(pattern)?, queue));
        Ok(self)
    }

    /// Add a rule that routes a specific task to a queue
    #[must_use]
    pub fn route_exact(mut self, task_name: &str, queue: &str) -> Self {
        self.router
            .add_rule(RouteRule::new(PatternMatcher::exact(task_name), queue));
        self
    }

    /// Add a direct route for a specific task
    #[must_use]
    pub fn direct_route(mut self, task_name: &str, queue: &str) -> Self {
        self.router
            .add_direct_route(task_name, RouteResult::new(queue));
        self
    }

    /// Add a rule that routes tasks based on argument conditions
    ///
    /// # Example
    /// ```
    /// use celers_core::router::{RouterBuilder, PatternMatcher, ArgumentCondition};
    ///
    /// let router = RouterBuilder::new()
    ///     .route_with_args(
    ///         PatternMatcher::glob("process.*"),
    ///         "high_priority",
    ///         ArgumentCondition::kwarg_equals("priority", serde_json::json!("high")),
    ///     )
    ///     .route_with_args(
    ///         PatternMatcher::glob("process.*"),
    ///         "low_priority",
    ///         ArgumentCondition::kwarg_equals("priority", serde_json::json!("low")),
    ///     )
    ///     .default_queue("default")
    ///     .build();
    /// ```
    #[must_use]
    pub fn route_with_args(
        mut self,
        matcher: PatternMatcher,
        queue: &str,
        condition: ArgumentCondition,
    ) -> Self {
        self.router
            .add_rule(RouteRule::new(matcher, queue).with_argument_condition(condition));
        self
    }

    /// Add a rule with both priority and argument condition
    #[must_use]
    pub fn route_with_args_priority(
        mut self,
        matcher: PatternMatcher,
        queue: &str,
        condition: ArgumentCondition,
        priority: i32,
    ) -> Self {
        self.router.add_rule(
            RouteRule::new(matcher, queue)
                .with_argument_condition(condition)
                .with_priority(priority),
        );
        self
    }

    /// Set the default queue for unmatched tasks
    #[must_use]
    pub fn default_queue(mut self, queue: &str) -> Self {
        self.router.set_default_queue(queue);
        self
    }

    /// Build the router
    #[must_use]
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_queue: None,
            routes: HashMap::new(),
            task_routes: HashMap::new(),
        }
    }

    /// Create a router from this configuration
    #[must_use]
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

// ============================================================================
// Topic-Based Routing (AMQP Topic Exchange Pattern)
// ============================================================================

/// Topic pattern matcher for AMQP-style topic routing
///
/// Supports wildcards:
/// - `*` (star) matches exactly one word
/// - `#` (hash) matches zero or more words
/// - Words are separated by dots (`.`)
///
/// # Examples
///
/// ```rust
/// use celers_core::router::TopicPattern;
///
/// let pattern = TopicPattern::new("user.*.created");
/// assert!(pattern.matches("user.email.created"));
/// assert!(pattern.matches("user.profile.created"));
/// assert!(!pattern.matches("user.created"));  // No middle word
/// assert!(!pattern.matches("user.email.verified.created"));  // Too many words
///
/// let pattern = TopicPattern::new("user.#");
/// assert!(pattern.matches("user.email"));
/// assert!(pattern.matches("user.email.created"));
/// assert!(pattern.matches("user.email.verified.sent"));
/// assert!(!pattern.matches("admin.email"));  // Doesn't start with "user"
/// ```
#[derive(Debug, Clone)]
pub struct TopicPattern {
    pattern: String,
    segments: Vec<TopicSegment>,
}

#[derive(Debug, Clone, PartialEq)]
enum TopicSegment {
    /// Exact word match
    Literal(String),
    /// Wildcard: matches exactly one word (*)
    Star,
    /// Wildcard: matches zero or more words (#)
    Hash,
}

impl TopicPattern {
    /// Create a new topic pattern
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let segments = Self::parse(&pattern);
        Self { pattern, segments }
    }

    fn parse(pattern: &str) -> Vec<TopicSegment> {
        pattern
            .split('.')
            .map(|s| match s {
                "*" => TopicSegment::Star,
                "#" => TopicSegment::Hash,
                literal => TopicSegment::Literal(literal.to_string()),
            })
            .collect()
    }

    /// Check if a routing key matches this topic pattern
    #[inline]
    #[must_use]
    pub fn matches(&self, routing_key: &str) -> bool {
        let key_parts: Vec<&str> = routing_key.split('.').collect();
        self.matches_parts(&key_parts, 0, 0)
    }

    fn matches_parts(&self, key_parts: &[&str], key_idx: usize, pattern_idx: usize) -> bool {
        // Base cases
        if pattern_idx >= self.segments.len() {
            return key_idx >= key_parts.len();
        }

        if key_idx >= key_parts.len() {
            // Check if remaining segments are all # (which match zero words)
            return self.segments[pattern_idx..]
                .iter()
                .all(|seg| matches!(seg, TopicSegment::Hash));
        }

        match &self.segments[pattern_idx] {
            TopicSegment::Literal(literal) => {
                if key_parts[key_idx] == literal {
                    self.matches_parts(key_parts, key_idx + 1, pattern_idx + 1)
                } else {
                    false
                }
            }
            TopicSegment::Star => {
                // * matches exactly one word
                self.matches_parts(key_parts, key_idx + 1, pattern_idx + 1)
            }
            TopicSegment::Hash => {
                // # matches zero or more words
                // Try matching zero words (skip this segment)
                if self.matches_parts(key_parts, key_idx, pattern_idx + 1) {
                    return true;
                }
                // Try matching one or more words
                for i in key_idx..key_parts.len() {
                    if self.matches_parts(key_parts, i + 1, pattern_idx + 1) {
                        return true;
                    }
                }
                // Also check if # can match all remaining words
                pattern_idx + 1 >= self.segments.len()
            }
        }
    }

    /// Get the original pattern string
    #[inline]
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Get pattern complexity (number of segments)
    #[inline]
    #[must_use]
    pub const fn complexity(&self) -> usize {
        self.segments.len()
    }

    /// Check if pattern contains wildcards
    #[inline]
    #[must_use]
    pub fn has_wildcards(&self) -> bool {
        self.segments
            .iter()
            .any(|s| matches!(s, TopicSegment::Star | TopicSegment::Hash))
    }

    /// Check if pattern is exact (no wildcards)
    #[inline]
    #[must_use]
    pub fn is_exact(&self) -> bool {
        !self.has_wildcards()
    }
}

/// Topic exchange router for AMQP-style topic routing
///
/// Routes messages based on topic patterns with wildcards.
#[derive(Debug, Clone)]
pub struct TopicRouter {
    /// Topic bindings: (pattern, queue)
    bindings: Vec<(TopicPattern, String)>,

    /// Default queue for unmatched topics
    default_queue: Option<String>,
}

impl TopicRouter {
    /// Create a new topic router
    #[must_use]
    pub fn new() -> Self {
        Self {
            bindings: Vec::new(),
            default_queue: None,
        }
    }

    /// Bind a topic pattern to a queue
    pub fn bind(&mut self, pattern: impl Into<String>, queue: impl Into<String>) {
        let pattern = TopicPattern::new(pattern);
        self.bindings.push((pattern, queue.into()));
    }

    /// Bind multiple patterns to a queue
    pub fn bind_many(&mut self, patterns: Vec<String>, queue: impl Into<String>) {
        let queue = queue.into();
        for pattern in patterns {
            self.bind(pattern, queue.clone());
        }
    }

    /// Set default queue for unmatched routing keys
    pub fn set_default_queue(&mut self, queue: impl Into<String>) {
        self.default_queue = Some(queue.into());
    }

    /// Route a message based on routing key
    ///
    /// Returns the first matching queue, or the default queue if no match.
    #[must_use]
    pub fn route(&self, routing_key: &str) -> Option<String> {
        for (pattern, queue) in &self.bindings {
            if pattern.matches(routing_key) {
                return Some(queue.clone());
            }
        }

        self.default_queue.clone()
    }

    /// Get all queues that match a routing key
    #[must_use]
    pub fn route_all(&self, routing_key: &str) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|(pattern, _)| pattern.matches(routing_key))
            .map(|(_, queue)| queue.clone())
            .collect()
    }

    /// Remove all bindings for a queue
    pub fn unbind_queue(&mut self, queue: &str) -> usize {
        let original_len = self.bindings.len();
        self.bindings.retain(|(_, q)| q != queue);
        original_len - self.bindings.len()
    }

    /// Remove a specific binding
    pub fn unbind_pattern(&mut self, pattern: &str) -> bool {
        let original_len = self.bindings.len();
        self.bindings.retain(|(p, _)| p.pattern() != pattern);
        self.bindings.len() < original_len
    }

    /// Get all bindings
    #[must_use]
    pub fn bindings(&self) -> Vec<(String, String)> {
        self.bindings
            .iter()
            .map(|(pattern, queue)| (pattern.pattern().to_string(), queue.clone()))
            .collect()
    }

    /// Clear all bindings
    pub fn clear(&mut self) {
        self.bindings.clear();
        self.default_queue = None;
    }

    /// Get number of bindings
    #[must_use]
    pub const fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    /// Check if a routing key has any matches
    #[inline]
    #[must_use]
    pub fn has_match(&self, routing_key: &str) -> bool {
        self.bindings.iter().any(|(p, _)| p.matches(routing_key)) || self.default_queue.is_some()
    }
}

impl Default for TopicRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Topic exchange configuration for declarative setup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicExchangeConfig {
    /// Exchange name
    pub name: String,

    /// Topic bindings: pattern -> queue
    pub bindings: HashMap<String, String>,

    /// Default queue for unmatched topics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_queue: Option<String>,

    /// Whether the exchange is durable
    #[serde(default = "default_true")]
    pub durable: bool,

    /// Whether to auto-delete when unused
    #[serde(default)]
    pub auto_delete: bool,
}

fn default_true() -> bool {
    true
}

impl TopicExchangeConfig {
    /// Create a new topic exchange configuration
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bindings: HashMap::new(),
            default_queue: None,
            durable: true,
            auto_delete: false,
        }
    }

    /// Add a topic binding
    #[must_use]
    pub fn with_binding(mut self, pattern: impl Into<String>, queue: impl Into<String>) -> Self {
        self.bindings.insert(pattern.into(), queue.into());
        self
    }

    /// Set default queue
    #[must_use]
    pub fn with_default_queue(mut self, queue: impl Into<String>) -> Self {
        self.default_queue = Some(queue.into());
        self
    }

    /// Set durable flag
    #[must_use]
    pub fn with_durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Build a topic router from this configuration
    #[must_use]
    pub fn build_router(&self) -> TopicRouter {
        let mut router = TopicRouter::new();

        for (pattern, queue) in &self.bindings {
            router.bind(pattern.clone(), queue.clone());
        }

        if let Some(ref default_queue) = self.default_queue {
            router.set_default_queue(default_queue.clone());
        }

        router
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
