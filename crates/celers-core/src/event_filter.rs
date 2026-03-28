//! Event filtering and routing for advanced event processing
//!
//! This module provides trait-based event filters and a priority-aware event router
//! that dispatches events to appropriate handlers based on filter criteria.
//!
//! # Overview
//!
//! - [`EventFilterTrait`] - Trait for custom event filters
//! - [`GlobEventFilter`] - Glob pattern matching on event types (e.g., "task-*")
//! - [`ExactEventFilter`] - Exact event type matching
//! - [`PrefixEventFilter`] - Prefix-based event type matching
//! - [`CompositeEventFilter`] - Combine multiple filters with AND/OR/NOT logic
//! - [`EventHandlerTrait`] - Async event handler trait
//! - [`EventRouter`] - Priority-aware event routing
//! - [`LoggingEventHandler`] - Debug logging handler
//! - [`CollectingEventHandler`] - In-memory event collector (useful for tests)
//!
//! # Example
//!
//! ```rust
//! use celers_core::event_filter::{
//!     EventRouter, GlobEventFilter, ExactEventFilter, LoggingEventHandler,
//! };
//!
//! # async fn example() -> celers_core::Result<()> {
//! let router = EventRouter::new()
//!     .route(
//!         GlobEventFilter::new("task-*"),
//!         LoggingEventHandler::new("task-events"),
//!     )
//!     .route_with_priority(
//!         ExactEventFilter::new("task-failed"),
//!         LoggingEventHandler::new("failures"),
//!         100,
//!     );
//! # Ok(())
//! # }
//! ```

use crate::event::Event;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

// ============================================================================
// EventFilterTrait
// ============================================================================

/// Trait for filtering events based on criteria
///
/// Implementors define custom matching logic to select which events
/// should be processed by a handler.
pub trait EventFilterTrait: Send + Sync + std::fmt::Debug {
    /// Returns true if the event matches this filter
    fn matches(&self, event: &Event) -> bool;

    /// Human-readable description of this filter
    fn description(&self) -> String;
}

// ============================================================================
// GlobPart (internal)
// ============================================================================

#[derive(Debug, Clone)]
enum GlobPart {
    /// Literal text segment
    Literal(String),
    /// Single wildcard `*`: matches anything except '-'
    Wildcard,
    /// Double wildcard `**`: matches everything including '-'
    DoubleWild,
}

// ============================================================================
// GlobEventFilter
// ============================================================================

/// Filter events by glob pattern on event type
///
/// Supports:
/// - `*` matches any characters except `-` (single segment)
/// - `**` matches any characters including `-` (multiple segments)
/// - Literal text for exact segment matching
///
/// # Examples
///
/// - `"task-*"` matches `"task-sent"`, `"task-failed"`, but not `"worker-online"`
/// - `"*"` matches everything (single segment, but event types have dashes)
/// - `"**"` matches everything
/// - `"task-sent"` matches only `"task-sent"`
#[derive(Debug, Clone)]
pub struct GlobEventFilter {
    pattern: String,
    parts: Vec<GlobPart>,
}

impl GlobEventFilter {
    /// Create a new glob event filter
    ///
    /// The pattern supports `*` (matches non-dash chars) and `**` (matches everything).
    #[must_use]
    pub fn new(pattern: &str) -> Self {
        let parts = Self::parse_pattern(pattern);
        Self {
            pattern: pattern.to_string(),
            parts,
        }
    }

    /// Parse a glob pattern into parts
    fn parse_pattern(pattern: &str) -> Vec<GlobPart> {
        let mut parts = Vec::new();
        let mut chars = pattern.chars().peekable();
        let mut literal = String::new();

        while let Some(ch) = chars.next() {
            if ch == '*' {
                // Flush any accumulated literal
                if !literal.is_empty() {
                    parts.push(GlobPart::Literal(std::mem::take(&mut literal)));
                }

                // Check for double wildcard
                if chars.peek() == Some(&'*') {
                    chars.next();
                    parts.push(GlobPart::DoubleWild);
                } else {
                    parts.push(GlobPart::Wildcard);
                }
            } else {
                literal.push(ch);
            }
        }

        // Flush remaining literal
        if !literal.is_empty() {
            parts.push(GlobPart::Literal(literal));
        }

        parts
    }

    /// Match an event type string against the compiled glob parts
    fn matches_str(&self, text: &str) -> bool {
        Self::match_parts(&self.parts, text)
    }

    /// Recursive glob matching
    fn match_parts(parts: &[GlobPart], text: &str) -> bool {
        if parts.is_empty() {
            return text.is_empty();
        }

        match &parts[0] {
            GlobPart::Literal(lit) => {
                if let Some(rest) = text.strip_prefix(lit.as_str()) {
                    Self::match_parts(&parts[1..], rest)
                } else {
                    false
                }
            }
            GlobPart::Wildcard => {
                // `*` matches any number of non-dash characters
                // Try matching 0..n characters that are not '-'
                let mut end = 0;
                loop {
                    if Self::match_parts(&parts[1..], &text[end..]) {
                        return true;
                    }
                    // Advance one character if it is not '-'
                    if end < text.len() {
                        let next_char = text[end..].chars().next();
                        match next_char {
                            Some(c) if c != '-' => {
                                end += c.len_utf8();
                            }
                            _ => break,
                        }
                    } else {
                        break;
                    }
                }
                false
            }
            GlobPart::DoubleWild => {
                // `**` matches everything (greedy with backtracking)
                for end in 0..=text.len() {
                    // Only try at valid char boundaries
                    if text.is_char_boundary(end) && Self::match_parts(&parts[1..], &text[end..]) {
                        return true;
                    }
                }
                false
            }
        }
    }
}

impl EventFilterTrait for GlobEventFilter {
    fn matches(&self, event: &Event) -> bool {
        self.matches_str(event.event_type())
    }

    fn description(&self) -> String {
        format!("glob({})", self.pattern)
    }
}

// ============================================================================
// ExactEventFilter
// ============================================================================

/// Filter by exact event type match
///
/// Only events whose `event_type()` exactly equals the configured string will match.
#[derive(Debug, Clone)]
pub struct ExactEventFilter {
    event_type: String,
}

impl ExactEventFilter {
    /// Create a new exact event filter
    #[must_use]
    pub fn new(event_type: &str) -> Self {
        Self {
            event_type: event_type.to_string(),
        }
    }
}

impl EventFilterTrait for ExactEventFilter {
    fn matches(&self, event: &Event) -> bool {
        event.event_type() == self.event_type
    }

    fn description(&self) -> String {
        format!("exact({})", self.event_type)
    }
}

// ============================================================================
// PrefixEventFilter
// ============================================================================

/// Filter by event type prefix
///
/// Events whose `event_type()` starts with the configured prefix will match.
///
/// # Example
///
/// `PrefixEventFilter::new("task")` matches `"task-sent"`, `"task-failed"`, etc.
#[derive(Debug, Clone)]
pub struct PrefixEventFilter {
    prefix: String,
}

impl PrefixEventFilter {
    /// Create a new prefix event filter
    #[must_use]
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }
}

impl EventFilterTrait for PrefixEventFilter {
    fn matches(&self, event: &Event) -> bool {
        event.event_type().starts_with(&self.prefix)
    }

    fn description(&self) -> String {
        format!("prefix({})", self.prefix)
    }
}

// ============================================================================
// CompositeEventFilter
// ============================================================================

/// How to combine multiple filters in a [`CompositeEventFilter`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterMode {
    /// All filters must match (AND)
    All,
    /// At least one filter must match (OR)
    Any,
    /// No filter should match (NOT / NOR)
    None,
}

/// Combine multiple filters with configurable logic
///
/// Supports AND, OR, and NOT (none) combination modes.
pub struct CompositeEventFilter {
    filters: Vec<Box<dyn EventFilterTrait>>,
    mode: FilterMode,
}

impl CompositeEventFilter {
    /// Create a new composite filter with the given mode
    #[must_use]
    pub fn new(mode: FilterMode) -> Self {
        Self {
            filters: Vec::new(),
            mode,
        }
    }

    /// Add a filter to this composite
    #[must_use]
    pub fn with_filter(mut self, filter: impl EventFilterTrait + 'static) -> Self {
        self.filters.push(Box::new(filter));
        self
    }

    /// Get the filter mode
    #[must_use]
    pub fn mode(&self) -> FilterMode {
        self.mode
    }

    /// Get the number of sub-filters
    #[must_use]
    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }
}

impl std::fmt::Debug for CompositeEventFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeEventFilter")
            .field("mode", &self.mode)
            .field("filter_count", &self.filters.len())
            .finish()
    }
}

impl EventFilterTrait for CompositeEventFilter {
    fn matches(&self, event: &Event) -> bool {
        match self.mode {
            FilterMode::All => self.filters.iter().all(|f| f.matches(event)),
            FilterMode::Any => self.filters.iter().any(|f| f.matches(event)),
            FilterMode::None => !self.filters.iter().any(|f| f.matches(event)),
        }
    }

    fn description(&self) -> String {
        let descriptions: Vec<String> = self.filters.iter().map(|f| f.description()).collect();
        let mode_str = match self.mode {
            FilterMode::All => "ALL",
            FilterMode::Any => "ANY",
            FilterMode::None => "NONE",
        };
        format!("{}({})", mode_str, descriptions.join(", "))
    }
}

// ============================================================================
// EventHandlerTrait
// ============================================================================

/// Async event handler trait
///
/// Handlers process events that pass through the router's filters.
#[async_trait]
pub trait EventHandlerTrait: Send + Sync {
    /// Handle an event
    async fn handle(&self, event: Event) -> crate::Result<()>;

    /// Human-readable handler name (for logging/debugging)
    fn name(&self) -> &str;
}

// ============================================================================
// EventRouter
// ============================================================================

/// A route entry binding a filter to a handler with a priority
struct EventRoute {
    filter: Box<dyn EventFilterTrait>,
    handler: Box<dyn EventHandlerTrait>,
    priority: i32,
}

/// Routes events to appropriate handlers based on filters
///
/// Events are dispatched to all handlers whose filter matches.
/// Routes are processed in priority order (higher priority first).
pub struct EventRouter {
    routes: Vec<EventRoute>,
}

impl EventRouter {
    /// Create a new empty event router
    #[must_use]
    pub fn new() -> Self {
        Self { routes: Vec::new() }
    }

    /// Add a route with default priority (0)
    #[must_use]
    pub fn route(
        self,
        filter: impl EventFilterTrait + 'static,
        handler: impl EventHandlerTrait + 'static,
    ) -> Self {
        self.route_with_priority(filter, handler, 0)
    }

    /// Add a route with explicit priority (higher = processed first)
    #[must_use]
    pub fn route_with_priority(
        mut self,
        filter: impl EventFilterTrait + 'static,
        handler: impl EventHandlerTrait + 'static,
        priority: i32,
    ) -> Self {
        self.routes.push(EventRoute {
            filter: Box::new(filter),
            handler: Box::new(handler),
            priority,
        });
        // Sort descending by priority so highest-priority routes come first
        self.routes.sort_by_key(|r| std::cmp::Reverse(r.priority));
        self
    }

    /// Get the number of registered routes
    #[must_use]
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    /// Dispatch an event to all matching handlers (in priority order)
    ///
    /// Returns the number of handlers that processed the event.
    ///
    /// # Errors
    ///
    /// Returns an error if any handler fails to process the event.
    pub async fn dispatch(&self, event: Event) -> crate::Result<usize> {
        let mut count = 0usize;

        for route in &self.routes {
            if route.filter.matches(&event) {
                route.handler.handle(event.clone()).await?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Dispatch a batch of events
    ///
    /// Returns the total number of handler invocations across all events.
    ///
    /// # Errors
    ///
    /// Returns an error if any handler fails to process any event.
    pub async fn dispatch_batch(&self, events: Vec<Event>) -> crate::Result<usize> {
        let mut total = 0usize;

        for event in events {
            total += self.dispatch(event).await?;
        }

        Ok(total)
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EventRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventRouter")
            .field("route_count", &self.routes.len())
            .finish()
    }
}

// ============================================================================
// LoggingEventHandler
// ============================================================================

/// A simple logging event handler for debugging
///
/// Logs each received event at the `debug` level using `tracing`.
#[derive(Debug)]
pub struct LoggingEventHandler {
    prefix: String,
}

impl LoggingEventHandler {
    /// Create a new logging handler with the given prefix
    #[must_use]
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }
}

#[async_trait]
impl EventHandlerTrait for LoggingEventHandler {
    async fn handle(&self, event: Event) -> crate::Result<()> {
        tracing::debug!(
            prefix = %self.prefix,
            event_type = %event.event_type(),
            task_id = ?event.task_id(),
            hostname = ?event.hostname(),
            "Event handled"
        );
        Ok(())
    }

    fn name(&self) -> &str {
        &self.prefix
    }
}

// ============================================================================
// CollectingEventHandler
// ============================================================================

/// A collecting handler that stores events in memory
///
/// Useful for tests and debugging. Events are stored in a shared, thread-safe
/// vector with an optional capacity limit (oldest events are evicted when full).
#[derive(Debug, Clone)]
pub struct CollectingEventHandler {
    name: String,
    events: Arc<RwLock<Vec<Event>>>,
    max_capacity: usize,
}

impl CollectingEventHandler {
    /// Create a new collecting handler
    ///
    /// `max_capacity` limits how many events are retained. When the limit is
    /// reached, the oldest event is removed to make room.
    #[must_use]
    pub fn new(name: &str, max_capacity: usize) -> Self {
        Self {
            name: name.to_string(),
            events: Arc::new(RwLock::new(Vec::new())),
            max_capacity,
        }
    }

    /// Get a snapshot of all collected events
    pub async fn events(&self) -> Vec<Event> {
        self.events.read().await.clone()
    }

    /// Get the number of collected events
    pub async fn len(&self) -> usize {
        self.events.read().await.len()
    }

    /// Check if no events have been collected
    pub async fn is_empty(&self) -> bool {
        self.events.read().await.is_empty()
    }

    /// Clear all collected events
    pub async fn clear(&self) {
        self.events.write().await.clear();
    }
}

#[async_trait]
impl EventHandlerTrait for CollectingEventHandler {
    async fn handle(&self, event: Event) -> crate::Result<()> {
        let mut events = self.events.write().await;
        events.push(event);

        // Evict oldest if over capacity
        if events.len() > self.max_capacity {
            let excess = events.len() - self.max_capacity;
            events.drain(0..excess);
        }

        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{TaskEvent, WorkerEvent};
    use chrono::Utc;
    use uuid::Uuid;

    // ---- helpers ----

    fn make_task_sent() -> Event {
        Event::Task(TaskEvent::Sent {
            task_id: Uuid::new_v4(),
            task_name: "add".to_string(),
            queue: "default".to_string(),
            timestamp: Utc::now(),
            args: None,
            kwargs: None,
            eta: None,
            expires: None,
            retries: None,
        })
    }

    fn make_task_failed() -> Event {
        Event::Task(TaskEvent::Failed {
            task_id: Uuid::new_v4(),
            task_name: "divide".to_string(),
            hostname: "worker-1".to_string(),
            timestamp: Utc::now(),
            exception: "ZeroDivisionError".to_string(),
            traceback: None,
        })
    }

    fn make_worker_online() -> Event {
        Event::Worker(WorkerEvent::Online {
            hostname: "worker-1".to_string(),
            timestamp: Utc::now(),
            sw_ident: "celers".to_string(),
            sw_ver: "0.2.0".to_string(),
            sw_sys: "linux".to_string(),
        })
    }

    fn make_worker_heartbeat() -> Event {
        Event::Worker(WorkerEvent::Heartbeat {
            hostname: "worker-1".to_string(),
            timestamp: Utc::now(),
            active: 3,
            processed: 42,
            loadavg: Some([1.0, 0.8, 0.5]),
            freq: 2.0,
        })
    }

    // ---- GlobEventFilter tests ----

    #[test]
    fn glob_task_star_matches_task_events() {
        let filter = GlobEventFilter::new("task-*");

        assert!(filter.matches(&make_task_sent()));
        assert!(filter.matches(&make_task_failed()));
        assert!(!filter.matches(&make_worker_online()));
        assert!(!filter.matches(&make_worker_heartbeat()));
    }

    #[test]
    fn glob_double_star_matches_everything() {
        let filter = GlobEventFilter::new("**");

        assert!(filter.matches(&make_task_sent()));
        assert!(filter.matches(&make_task_failed()));
        assert!(filter.matches(&make_worker_online()));
        assert!(filter.matches(&make_worker_heartbeat()));
    }

    #[test]
    fn glob_single_star_matches_single_segment() {
        // '*' does not match '-', so "worker-*" matches "worker-" followed by non-dash chars
        let filter = GlobEventFilter::new("worker-*");

        // "worker-online" => "worker-" literal + "online" (no dash => matches)
        assert!(filter.matches(&make_worker_online()));
        // "worker-heartbeat" => "worker-" literal + "heartbeat" (no dash => matches)
        assert!(filter.matches(&make_worker_heartbeat()));
        // task events don't start with "worker-"
        assert!(!filter.matches(&make_task_sent()));
    }

    #[test]
    fn glob_literal_only() {
        let filter = GlobEventFilter::new("task-sent");
        assert!(filter.matches(&make_task_sent()));
        assert!(!filter.matches(&make_task_failed()));
        assert!(!filter.matches(&make_worker_online()));
    }

    #[test]
    fn glob_star_alone() {
        // '*' alone matches anything without dashes.
        // All event types contain dashes, so '*' alone should NOT match any.
        let filter = GlobEventFilter::new("*");
        // "task-sent" has a dash, so single '*' won't cross it
        assert!(!filter.matches(&make_task_sent()));
        assert!(!filter.matches(&make_worker_online()));
    }

    #[test]
    fn glob_description() {
        let filter = GlobEventFilter::new("task-*");
        assert_eq!(filter.description(), "glob(task-*)");
    }

    // ---- ExactEventFilter tests ----

    #[test]
    fn exact_match_only() {
        let filter = ExactEventFilter::new("task-sent");
        assert!(filter.matches(&make_task_sent()));
        assert!(!filter.matches(&make_task_failed()));
        assert!(!filter.matches(&make_worker_online()));
    }

    #[test]
    fn exact_no_partial() {
        let filter = ExactEventFilter::new("task");
        assert!(!filter.matches(&make_task_sent()));
    }

    #[test]
    fn exact_description() {
        let filter = ExactEventFilter::new("task-sent");
        assert_eq!(filter.description(), "exact(task-sent)");
    }

    // ---- PrefixEventFilter tests ----

    #[test]
    fn prefix_matches_task_prefix() {
        let filter = PrefixEventFilter::new("task");
        assert!(filter.matches(&make_task_sent()));
        assert!(filter.matches(&make_task_failed()));
        assert!(!filter.matches(&make_worker_online()));
    }

    #[test]
    fn prefix_matches_worker_prefix() {
        let filter = PrefixEventFilter::new("worker");
        assert!(!filter.matches(&make_task_sent()));
        assert!(filter.matches(&make_worker_online()));
        assert!(filter.matches(&make_worker_heartbeat()));
    }

    #[test]
    fn prefix_empty_matches_all() {
        let filter = PrefixEventFilter::new("");
        assert!(filter.matches(&make_task_sent()));
        assert!(filter.matches(&make_worker_online()));
    }

    #[test]
    fn prefix_description() {
        let filter = PrefixEventFilter::new("task");
        assert_eq!(filter.description(), "prefix(task)");
    }

    // ---- CompositeEventFilter tests ----

    #[test]
    fn composite_all_mode() {
        // Both must match: prefix "task" AND exact "task-sent"
        let filter = CompositeEventFilter::new(FilterMode::All)
            .with_filter(PrefixEventFilter::new("task"))
            .with_filter(ExactEventFilter::new("task-sent"));

        assert!(filter.matches(&make_task_sent()));
        assert!(!filter.matches(&make_task_failed())); // prefix matches but exact doesn't
        assert!(!filter.matches(&make_worker_online()));
    }

    #[test]
    fn composite_any_mode() {
        // Either must match: exact "task-sent" OR exact "worker-online"
        let filter = CompositeEventFilter::new(FilterMode::Any)
            .with_filter(ExactEventFilter::new("task-sent"))
            .with_filter(ExactEventFilter::new("worker-online"));

        assert!(filter.matches(&make_task_sent()));
        assert!(!filter.matches(&make_task_failed()));
        assert!(filter.matches(&make_worker_online()));
    }

    #[test]
    fn composite_none_mode() {
        // None should match: exclude "task-sent" and "task-failed"
        let filter = CompositeEventFilter::new(FilterMode::None)
            .with_filter(ExactEventFilter::new("task-sent"))
            .with_filter(ExactEventFilter::new("task-failed"));

        assert!(!filter.matches(&make_task_sent()));
        assert!(!filter.matches(&make_task_failed()));
        assert!(filter.matches(&make_worker_online()));
        assert!(filter.matches(&make_worker_heartbeat()));
    }

    #[test]
    fn composite_empty_all_matches_everything() {
        let filter = CompositeEventFilter::new(FilterMode::All);
        // No sub-filters => all() on empty iterator => true
        assert!(filter.matches(&make_task_sent()));
    }

    #[test]
    fn composite_empty_any_matches_nothing() {
        let filter = CompositeEventFilter::new(FilterMode::Any);
        // No sub-filters => any() on empty iterator => false
        assert!(!filter.matches(&make_task_sent()));
    }

    #[test]
    fn composite_empty_none_matches_everything() {
        let filter = CompositeEventFilter::new(FilterMode::None);
        // No sub-filters => none match => true
        assert!(filter.matches(&make_task_sent()));
    }

    #[test]
    fn composite_description() {
        let filter = CompositeEventFilter::new(FilterMode::All)
            .with_filter(PrefixEventFilter::new("task"))
            .with_filter(ExactEventFilter::new("task-sent"));

        let desc = filter.description();
        assert!(desc.starts_with("ALL("));
        assert!(desc.contains("prefix(task)"));
        assert!(desc.contains("exact(task-sent)"));
    }

    #[test]
    fn composite_debug_and_accessors() {
        let filter =
            CompositeEventFilter::new(FilterMode::Any).with_filter(PrefixEventFilter::new("task"));

        assert_eq!(filter.mode(), FilterMode::Any);
        assert_eq!(filter.filter_count(), 1);
        let debug = format!("{:?}", filter);
        assert!(debug.contains("CompositeEventFilter"));
    }

    // ---- EventRouter tests ----

    #[tokio::test]
    async fn router_dispatch_to_correct_handlers() {
        let task_collector = CollectingEventHandler::new("tasks", 100);
        let worker_collector = CollectingEventHandler::new("workers", 100);

        // Clone so we can inspect after routing
        let task_collector_clone = task_collector.clone();
        let worker_collector_clone = worker_collector.clone();

        let router = EventRouter::new()
            .route(PrefixEventFilter::new("task"), task_collector)
            .route(PrefixEventFilter::new("worker"), worker_collector);

        let sent = make_task_sent();
        let online = make_worker_online();

        let count1 = router.dispatch(sent).await;
        assert!(count1.is_ok());
        assert_eq!(count1.ok(), Some(1));

        let count2 = router.dispatch(online).await;
        assert!(count2.is_ok());
        assert_eq!(count2.ok(), Some(1));

        assert_eq!(task_collector_clone.len().await, 1);
        assert_eq!(worker_collector_clone.len().await, 1);
    }

    #[tokio::test]
    async fn router_priority_ordering() {
        // Use two collectors; the high-priority one should be invoked first
        // (both match all events via "**" glob)
        let high_priority = CollectingEventHandler::new("high", 100);
        let low_priority = CollectingEventHandler::new("low", 100);

        let high_clone = high_priority.clone();
        let low_clone = low_priority.clone();

        let router = EventRouter::new()
            .route_with_priority(GlobEventFilter::new("**"), low_priority, 1)
            .route_with_priority(GlobEventFilter::new("**"), high_priority, 100);

        let count = router.dispatch(make_task_sent()).await;
        assert!(count.is_ok());
        assert_eq!(count.ok(), Some(2));

        // Both handlers received the event
        assert_eq!(high_clone.len().await, 1);
        assert_eq!(low_clone.len().await, 1);

        // Verify internal ordering: routes[0] should have priority 100
        assert_eq!(router.routes[0].priority, 100);
        assert_eq!(router.routes[1].priority, 1);
    }

    #[tokio::test]
    async fn router_dispatch_batch() {
        let collector = CollectingEventHandler::new("all", 100);
        let collector_clone = collector.clone();

        let router = EventRouter::new().route(GlobEventFilter::new("**"), collector);

        let events = vec![make_task_sent(), make_task_failed(), make_worker_online()];

        let total = router.dispatch_batch(events).await;
        assert!(total.is_ok());
        assert_eq!(total.ok(), Some(3));
        assert_eq!(collector_clone.len().await, 3);
    }

    #[tokio::test]
    async fn empty_router_returns_zero() {
        let router = EventRouter::new();
        let count = router.dispatch(make_task_sent()).await;
        assert!(count.is_ok());
        assert_eq!(count.ok(), Some(0));
    }

    #[tokio::test]
    async fn router_route_count() {
        let router = EventRouter::new()
            .route(
                ExactEventFilter::new("task-sent"),
                LoggingEventHandler::new("a"),
            )
            .route(
                ExactEventFilter::new("task-failed"),
                LoggingEventHandler::new("b"),
            );

        assert_eq!(router.route_count(), 2);
    }

    #[tokio::test]
    async fn router_no_match_returns_zero() {
        let collector = CollectingEventHandler::new("tasks", 100);
        let collector_clone = collector.clone();

        let router = EventRouter::new().route(ExactEventFilter::new("task-sent"), collector);

        // Dispatch a worker event which won't match
        let count = router.dispatch(make_worker_online()).await;
        assert!(count.is_ok());
        assert_eq!(count.ok(), Some(0));
        assert!(collector_clone.is_empty().await);
    }

    // ---- LoggingEventHandler tests ----

    #[tokio::test]
    async fn logging_handler_processes_without_error() {
        let handler = LoggingEventHandler::new("test");
        assert_eq!(handler.name(), "test");

        let result = handler.handle(make_task_sent()).await;
        assert!(result.is_ok());
    }

    // ---- CollectingEventHandler tests ----

    #[tokio::test]
    async fn collecting_handler_stores_events() {
        let handler = CollectingEventHandler::new("test", 100);
        assert!(handler.is_empty().await);

        handler.handle(make_task_sent()).await.ok();
        handler.handle(make_task_failed()).await.ok();

        assert_eq!(handler.len().await, 2);
        assert!(!handler.is_empty().await);

        let events = handler.events().await;
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn collecting_handler_respects_capacity() {
        let handler = CollectingEventHandler::new("test", 2);

        handler.handle(make_task_sent()).await.ok();
        handler.handle(make_task_failed()).await.ok();
        handler.handle(make_worker_online()).await.ok();

        // Should have evicted the oldest
        assert_eq!(handler.len().await, 2);
        let events = handler.events().await;
        // First event should be task-failed (oldest retained after eviction)
        assert_eq!(events[0].event_type(), "task-failed");
        assert_eq!(events[1].event_type(), "worker-online");
    }

    #[tokio::test]
    async fn collecting_handler_clear() {
        let handler = CollectingEventHandler::new("test", 100);
        handler.handle(make_task_sent()).await.ok();
        assert_eq!(handler.len().await, 1);

        handler.clear().await;
        assert!(handler.is_empty().await);
    }

    #[tokio::test]
    async fn collecting_handler_name() {
        let handler = CollectingEventHandler::new("my-collector", 10);
        assert_eq!(handler.name(), "my-collector");
    }

    // ---- Router debug ----

    #[test]
    fn router_debug_display() {
        let router = EventRouter::new().route(
            ExactEventFilter::new("task-sent"),
            LoggingEventHandler::new("test"),
        );
        let debug = format!("{:?}", router);
        assert!(debug.contains("EventRouter"));
        assert!(debug.contains("route_count"));
    }

    // ---- Multiple matching routes ----

    #[tokio::test]
    async fn multiple_routes_match_same_event() {
        let collector_a = CollectingEventHandler::new("a", 100);
        let collector_b = CollectingEventHandler::new("b", 100);

        let a_clone = collector_a.clone();
        let b_clone = collector_b.clone();

        let router = EventRouter::new()
            .route(PrefixEventFilter::new("task"), collector_a)
            .route(ExactEventFilter::new("task-sent"), collector_b);

        let count = router.dispatch(make_task_sent()).await;
        assert!(count.is_ok());
        assert_eq!(count.ok(), Some(2));
        assert_eq!(a_clone.len().await, 1);
        assert_eq!(b_clone.len().await, 1);
    }
}
