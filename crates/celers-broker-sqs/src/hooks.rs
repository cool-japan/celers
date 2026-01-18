// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Observability hooks for AWS SQS broker operations.
//!
//! This module provides extensible hooks for integrating custom metrics,
//! logging, and monitoring into SQS broker operations. Hooks can be used
//! to integrate with:
//! - Prometheus metrics
//! - Datadog monitoring
//! - Custom logging frameworks
//! - Distributed tracing systems
//! - Alert management systems
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::hooks::{ObservabilityHooks, OperationEvent, EventType};
//!
//! let mut hooks = ObservabilityHooks::new();
//!
//! // Add a custom hook for publish operations
//! hooks.on_publish(|event| {
//!     println!("Published message to {}", event.queue_name);
//! });
//!
//! // Trigger the hook
//! hooks.trigger_publish("my-queue", 1, true);
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Event type for broker operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Message publish event
    Publish,
    /// Message consume event
    Consume,
    /// Message acknowledgment event
    Ack,
    /// Message rejection event
    Reject,
    /// Connection event
    Connect,
    /// Disconnection event
    Disconnect,
    /// Error event
    Error,
}

/// Operation event details
#[derive(Debug, Clone)]
pub struct OperationEvent {
    /// Event type
    pub event_type: EventType,
    /// Queue name
    pub queue_name: String,
    /// Number of messages involved
    pub message_count: usize,
    /// Operation duration
    pub duration: Option<Duration>,
    /// Success status
    pub success: bool,
    /// Error message (if failed)
    pub error_message: Option<String>,
    /// Additional metadata
    pub metadata: std::collections::HashMap<String, String>,
}

impl OperationEvent {
    /// Create a new operation event
    pub fn new(event_type: EventType, queue_name: &str) -> Self {
        Self {
            event_type,
            queue_name: queue_name.to_string(),
            message_count: 0,
            duration: None,
            success: true,
            error_message: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Set message count
    pub fn with_message_count(mut self, count: usize) -> Self {
        self.message_count = count;
        self
    }

    /// Set duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = Some(duration);
        self
    }

    /// Set success status
    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }

    /// Set error message
    pub fn with_error(mut self, error: String) -> Self {
        self.success = false;
        self.error_message = Some(error);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

/// Hook function type
pub type HookFn = dyn Fn(&OperationEvent) + Send + Sync;

/// Observability hooks for broker operations
///
/// Provides extensible hooks for custom monitoring and logging.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::hooks::ObservabilityHooks;
///
/// let hooks = ObservabilityHooks::new();
/// assert_eq!(hooks.hook_count(), 0);
/// ```
#[derive(Clone)]
pub struct ObservabilityHooks {
    /// Registered hooks
    hooks: Arc<Mutex<Vec<Arc<HookFn>>>>,
}

impl ObservabilityHooks {
    /// Create new observability hooks
    pub fn new() -> Self {
        Self {
            hooks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a hook for all events
    ///
    /// # Arguments
    ///
    /// * `hook` - Function to call on each event
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::hooks::ObservabilityHooks;
    ///
    /// let mut hooks = ObservabilityHooks::new();
    /// hooks.register(|event| {
    ///     println!("Event: {:?}", event.event_type);
    /// });
    /// ```
    pub fn register<F>(&mut self, hook: F)
    where
        F: Fn(&OperationEvent) + Send + Sync + 'static,
    {
        self.hooks.lock().unwrap().push(Arc::new(hook));
    }

    /// Register a hook for publish events
    pub fn on_publish<F>(&mut self, hook: F)
    where
        F: Fn(&OperationEvent) + Send + Sync + 'static,
    {
        self.hooks.lock().unwrap().push(Arc::new(move |event| {
            if event.event_type == EventType::Publish {
                hook(event);
            }
        }));
    }

    /// Register a hook for consume events
    pub fn on_consume<F>(&mut self, hook: F)
    where
        F: Fn(&OperationEvent) + Send + Sync + 'static,
    {
        self.hooks.lock().unwrap().push(Arc::new(move |event| {
            if event.event_type == EventType::Consume {
                hook(event);
            }
        }));
    }

    /// Register a hook for acknowledgment events
    pub fn on_ack<F>(&mut self, hook: F)
    where
        F: Fn(&OperationEvent) + Send + Sync + 'static,
    {
        self.hooks.lock().unwrap().push(Arc::new(move |event| {
            if event.event_type == EventType::Ack {
                hook(event);
            }
        }));
    }

    /// Register a hook for error events
    pub fn on_error<F>(&mut self, hook: F)
    where
        F: Fn(&OperationEvent) + Send + Sync + 'static,
    {
        self.hooks.lock().unwrap().push(Arc::new(move |event| {
            if event.event_type == EventType::Error {
                hook(event);
            }
        }));
    }

    /// Trigger all registered hooks
    ///
    /// # Arguments
    ///
    /// * `event` - Event to trigger hooks for
    pub fn trigger(&self, event: &OperationEvent) {
        let hooks = self.hooks.lock().unwrap();
        for hook in hooks.iter() {
            hook(event);
        }
    }

    /// Trigger publish event hooks
    pub fn trigger_publish(&self, queue_name: &str, count: usize, success: bool) {
        let event = OperationEvent::new(EventType::Publish, queue_name)
            .with_message_count(count)
            .with_success(success);
        self.trigger(&event);
    }

    /// Trigger consume event hooks
    pub fn trigger_consume(&self, queue_name: &str, count: usize, duration: Duration) {
        let event = OperationEvent::new(EventType::Consume, queue_name)
            .with_message_count(count)
            .with_duration(duration);
        self.trigger(&event);
    }

    /// Trigger acknowledgment event hooks
    pub fn trigger_ack(&self, queue_name: &str, count: usize) {
        let event = OperationEvent::new(EventType::Ack, queue_name).with_message_count(count);
        self.trigger(&event);
    }

    /// Trigger error event hooks
    pub fn trigger_error(&self, queue_name: &str, error: &str) {
        let event = OperationEvent::new(EventType::Error, queue_name).with_error(error.to_string());
        self.trigger(&event);
    }

    /// Get number of registered hooks
    pub fn hook_count(&self) -> usize {
        self.hooks.lock().unwrap().len()
    }

    /// Clear all registered hooks
    pub fn clear(&mut self) {
        self.hooks.lock().unwrap().clear();
    }
}

impl Default for ObservabilityHooks {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ObservabilityHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservabilityHooks")
            .field("hook_count", &self.hook_count())
            .finish()
    }
}

/// Prometheus-style metrics collector
///
/// Collects metrics compatible with Prometheus exposition format.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::hooks::MetricsCollector;
///
/// let collector = MetricsCollector::new();
/// assert_eq!(collector.total_publishes(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    /// Total publish operations
    total_publishes: Arc<Mutex<u64>>,
    /// Total consume operations
    total_consumes: Arc<Mutex<u64>>,
    /// Total acknowledgments
    total_acks: Arc<Mutex<u64>>,
    /// Total errors
    total_errors: Arc<Mutex<u64>>,
    /// Total messages published
    total_messages_published: Arc<Mutex<u64>>,
    /// Total messages consumed
    total_messages_consumed: Arc<Mutex<u64>>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            total_publishes: Arc::new(Mutex::new(0)),
            total_consumes: Arc::new(Mutex::new(0)),
            total_acks: Arc::new(Mutex::new(0)),
            total_errors: Arc::new(Mutex::new(0)),
            total_messages_published: Arc::new(Mutex::new(0)),
            total_messages_consumed: Arc::new(Mutex::new(0)),
        }
    }

    /// Record a publish operation
    pub fn record_publish(&self, message_count: usize) {
        *self.total_publishes.lock().unwrap() += 1;
        *self.total_messages_published.lock().unwrap() += message_count as u64;
    }

    /// Record a consume operation
    pub fn record_consume(&self, message_count: usize) {
        *self.total_consumes.lock().unwrap() += 1;
        *self.total_messages_consumed.lock().unwrap() += message_count as u64;
    }

    /// Record an acknowledgment
    pub fn record_ack(&self, _message_count: usize) {
        *self.total_acks.lock().unwrap() += 1;
    }

    /// Record an error
    pub fn record_error(&self) {
        *self.total_errors.lock().unwrap() += 1;
    }

    /// Get total publish operations
    pub fn total_publishes(&self) -> u64 {
        *self.total_publishes.lock().unwrap()
    }

    /// Get total consume operations
    pub fn total_consumes(&self) -> u64 {
        *self.total_consumes.lock().unwrap()
    }

    /// Get total acknowledgments
    pub fn total_acks(&self) -> u64 {
        *self.total_acks.lock().unwrap()
    }

    /// Get total errors
    pub fn total_errors(&self) -> u64 {
        *self.total_errors.lock().unwrap()
    }

    /// Get total messages published
    pub fn total_messages_published(&self) -> u64 {
        *self.total_messages_published.lock().unwrap()
    }

    /// Get total messages consumed
    pub fn total_messages_consumed(&self) -> u64 {
        *self.total_messages_consumed.lock().unwrap()
    }

    /// Reset all metrics
    pub fn reset(&self) {
        *self.total_publishes.lock().unwrap() = 0;
        *self.total_consumes.lock().unwrap() = 0;
        *self.total_acks.lock().unwrap() = 0;
        *self.total_errors.lock().unwrap() = 0;
        *self.total_messages_published.lock().unwrap() = 0;
        *self.total_messages_consumed.lock().unwrap() = 0;
    }

    /// Create observability hooks that update this collector
    pub fn create_hooks(&self) -> ObservabilityHooks {
        let mut hooks = ObservabilityHooks::new();

        let collector = self.clone();
        hooks.on_publish(move |event| {
            collector.record_publish(event.message_count);
        });

        let collector = self.clone();
        hooks.on_consume(move |event| {
            collector.record_consume(event.message_count);
        });

        let collector = self.clone();
        hooks.on_ack(move |event| {
            collector.record_ack(event.message_count);
        });

        let collector = self.clone();
        hooks.on_error(move |_event| {
            collector.record_error();
        });

        hooks
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_hooks_new() {
        let hooks = ObservabilityHooks::new();
        assert_eq!(hooks.hook_count(), 0);
    }

    #[test]
    fn test_register_hook() {
        let mut hooks = ObservabilityHooks::new();
        hooks.register(|_event| {});
        assert_eq!(hooks.hook_count(), 1);
    }

    #[test]
    fn test_on_publish_hook() {
        let mut hooks = ObservabilityHooks::new();
        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();

        hooks.on_publish(move |_event| {
            *called_clone.lock().unwrap() = true;
        });

        hooks.trigger_publish("test-queue", 1, true);
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn test_on_consume_hook() {
        let mut hooks = ObservabilityHooks::new();
        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();

        hooks.on_consume(move |_event| {
            *called_clone.lock().unwrap() = true;
        });

        hooks.trigger_consume("test-queue", 5, Duration::from_millis(100));
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn test_on_error_hook() {
        let mut hooks = ObservabilityHooks::new();
        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();

        hooks.on_error(move |_event| {
            *called_clone.lock().unwrap() = true;
        });

        hooks.trigger_error("test-queue", "Test error");
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn test_clear_hooks() {
        let mut hooks = ObservabilityHooks::new();
        hooks.register(|_event| {});
        assert_eq!(hooks.hook_count(), 1);

        hooks.clear();
        assert_eq!(hooks.hook_count(), 0);
    }

    #[test]
    fn test_metrics_collector_new() {
        let collector = MetricsCollector::new();
        assert_eq!(collector.total_publishes(), 0);
        assert_eq!(collector.total_consumes(), 0);
    }

    #[test]
    fn test_metrics_collector_record_publish() {
        let collector = MetricsCollector::new();
        collector.record_publish(5);
        assert_eq!(collector.total_publishes(), 1);
        assert_eq!(collector.total_messages_published(), 5);
    }

    #[test]
    fn test_metrics_collector_record_consume() {
        let collector = MetricsCollector::new();
        collector.record_consume(3);
        assert_eq!(collector.total_consumes(), 1);
        assert_eq!(collector.total_messages_consumed(), 3);
    }

    #[test]
    fn test_metrics_collector_reset() {
        let collector = MetricsCollector::new();
        collector.record_publish(5);
        collector.record_consume(3);

        collector.reset();
        assert_eq!(collector.total_publishes(), 0);
        assert_eq!(collector.total_consumes(), 0);
        assert_eq!(collector.total_messages_published(), 0);
    }

    #[test]
    fn test_metrics_collector_create_hooks() {
        let collector = MetricsCollector::new();
        let hooks = collector.create_hooks();

        hooks.trigger_publish("test-queue", 5, true);
        hooks.trigger_consume("test-queue", 3, Duration::from_millis(100));

        assert_eq!(collector.total_publishes(), 1);
        assert_eq!(collector.total_consumes(), 1);
        assert_eq!(collector.total_messages_published(), 5);
        assert_eq!(collector.total_messages_consumed(), 3);
    }

    #[test]
    fn test_operation_event_builder() {
        let event = OperationEvent::new(EventType::Publish, "test-queue")
            .with_message_count(5)
            .with_duration(Duration::from_millis(100))
            .with_success(true)
            .with_metadata("key", "value");

        assert_eq!(event.event_type, EventType::Publish);
        assert_eq!(event.queue_name, "test-queue");
        assert_eq!(event.message_count, 5);
        assert!(event.success);
        assert_eq!(event.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_event_type_equality() {
        assert_eq!(EventType::Publish, EventType::Publish);
        assert_ne!(EventType::Publish, EventType::Consume);
    }
}
