//! Message Lifecycle Hooks for AMQP
//!
//! Provides extensible hooks for intercepting and augmenting AMQP message lifecycle events.
//!
//! # Features
//!
//! - **Pre/Post Publish Hooks**: Validate, enrich, or route messages before/after publishing
//! - **Pre/Post Consume Hooks**: Preprocess messages or perform side effects when consuming
//! - **Acknowledgment Hooks**: React to message acknowledgment/rejection events
//! - **Composable Hook Chains**: Multiple hooks can be combined
//! - **Async Support**: All hooks support async operations
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::hooks::{
//!     PublishHook, HookContext, HookResult, AmqpHookRegistry,
//! };
//! use celers_protocol::Message;
//!
//! // Create a custom validation hook
//! struct PayloadSizeValidator {
//!     max_size: usize,
//! }
//!
//! #[async_trait::async_trait]
//! impl PublishHook for PayloadSizeValidator {
//!     async fn before_publish(
//!         &self,
//!         message: &mut Message,
//!         _ctx: &HookContext,
//!     ) -> HookResult<()> {
//!         let payload_size = serde_json::to_vec(&message.body)
//!             .map(|v| v.len())
//!             .unwrap_or(0);
//!
//!         if payload_size > self.max_size {
//!             return Err(format!(
//!                 "Message payload size {} exceeds maximum {}",
//!                 payload_size, self.max_size
//!             ).into());
//!         }
//!         Ok(())
//!     }
//! }
//!
//! // Register the hook
//! let registry = AmqpHookRegistry::new();
//! registry.add_publish_hook(Box::new(PayloadSizeValidator {
//!     max_size: 1_000_000,
//! }));
//! ```

use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Result type for hook operations
pub type HookResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Context passed to hooks containing metadata about the operation
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Queue or exchange name
    pub target: String,
    /// Routing key (if applicable)
    pub routing_key: Option<String>,
    /// Additional context metadata
    pub metadata: HashMap<String, String>,
}

impl HookContext {
    /// Create a new hook context
    pub fn new(target: String) -> Self {
        Self {
            target,
            routing_key: None,
            metadata: HashMap::new(),
        }
    }

    /// Set routing key
    pub fn with_routing_key(mut self, routing_key: String) -> Self {
        self.routing_key = Some(routing_key);
        self
    }

    /// Add metadata to the context
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

/// Hook for message publish operations
#[async_trait]
pub trait PublishHook: Send + Sync {
    /// Called before a message is published
    ///
    /// Can modify the message or reject the publish operation.
    ///
    /// # Arguments
    ///
    /// * `message` - The message about to be published (can be modified)
    /// * `ctx` - Hook context with operation metadata
    ///
    /// # Returns
    ///
    /// `Ok(())` to allow publish, `Err(_)` to reject
    async fn before_publish(&self, message: &mut Message, ctx: &HookContext) -> HookResult<()> {
        let _ = (message, ctx);
        Ok(())
    }

    /// Called after a message is successfully published
    ///
    /// # Arguments
    ///
    /// * `message` - The published message
    /// * `ctx` - Hook context with operation metadata
    async fn after_publish(&self, message: &Message, ctx: &HookContext) -> HookResult<()> {
        let _ = (message, ctx);
        Ok(())
    }
}

/// Hook for message consume operations
#[async_trait]
pub trait ConsumeHook: Send + Sync {
    /// Called before a message is consumed
    ///
    /// Can perform preprocessing or reject the consume.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Hook context with operation metadata
    async fn before_consume(&self, ctx: &HookContext) -> HookResult<()> {
        let _ = ctx;
        Ok(())
    }

    /// Called after a message is successfully consumed
    ///
    /// Can modify the message before it's processed.
    ///
    /// # Arguments
    ///
    /// * `message` - The consumed message (can be modified)
    /// * `ctx` - Hook context with operation metadata
    async fn after_consume(&self, message: &mut Message, ctx: &HookContext) -> HookResult<()> {
        let _ = (message, ctx);
        Ok(())
    }
}

/// Message acknowledgment status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckStatus {
    /// Message acknowledged successfully
    Acknowledged { delivery_tag: u64 },
    /// Message rejected (nack)
    Rejected { delivery_tag: u64, requeue: bool },
    /// Multiple messages acknowledged
    AcknowledgedMultiple { delivery_tag: u64, count: usize },
    /// Multiple messages rejected
    RejectedMultiple {
        delivery_tag: u64,
        count: usize,
        requeue: bool,
    },
}

impl AckStatus {
    /// Get the delivery tag
    pub fn delivery_tag(&self) -> u64 {
        match self {
            AckStatus::Acknowledged { delivery_tag }
            | AckStatus::Rejected { delivery_tag, .. }
            | AckStatus::AcknowledgedMultiple { delivery_tag, .. }
            | AckStatus::RejectedMultiple { delivery_tag, .. } => *delivery_tag,
        }
    }

    /// Check if the operation was successful (acknowledged)
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            AckStatus::Acknowledged { .. } | AckStatus::AcknowledgedMultiple { .. }
        )
    }

    /// Check if the operation was a rejection
    pub fn is_rejection(&self) -> bool {
        matches!(
            self,
            AckStatus::Rejected { .. } | AckStatus::RejectedMultiple { .. }
        )
    }

    /// Check if messages should be requeued (for rejections)
    pub fn should_requeue(&self) -> bool {
        match self {
            AckStatus::Rejected { requeue, .. } | AckStatus::RejectedMultiple { requeue, .. } => {
                *requeue
            }
            _ => false,
        }
    }
}

/// Hook for message acknowledgment events
#[async_trait]
pub trait AcknowledgmentHook: Send + Sync {
    /// Called when a message is acknowledged or rejected
    ///
    /// # Arguments
    ///
    /// * `status` - Acknowledgment status
    /// * `ctx` - Hook context with operation metadata
    async fn on_acknowledgment(&self, status: &AckStatus, ctx: &HookContext) -> HookResult<()> {
        let _ = (status, ctx);
        Ok(())
    }
}

/// Hook for error events
#[async_trait]
pub trait ErrorHook: Send + Sync {
    /// Called when an error occurs during message processing
    ///
    /// # Arguments
    ///
    /// * `error` - The error that occurred
    /// * `ctx` - Hook context with operation metadata
    async fn on_error(&self, error: &str, ctx: &HookContext) -> HookResult<()> {
        let _ = (error, ctx);
        Ok(())
    }
}

/// Registry for managing AMQP message lifecycle hooks
#[derive(Clone, Default)]
pub struct AmqpHookRegistry {
    publish_hooks: Arc<RwLock<Vec<Box<dyn PublishHook>>>>,
    consume_hooks: Arc<RwLock<Vec<Box<dyn ConsumeHook>>>>,
    ack_hooks: Arc<RwLock<Vec<Box<dyn AcknowledgmentHook>>>>,
    error_hooks: Arc<RwLock<Vec<Box<dyn ErrorHook>>>>,
}

impl AmqpHookRegistry {
    /// Create a new empty hook registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a publish hook
    pub fn add_publish_hook(&self, hook: Box<dyn PublishHook>) {
        self.publish_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Add a consume hook
    pub fn add_consume_hook(&self, hook: Box<dyn ConsumeHook>) {
        self.consume_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Add an acknowledgment hook
    pub fn add_acknowledgment_hook(&self, hook: Box<dyn AcknowledgmentHook>) {
        self.ack_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Add an error hook
    pub fn add_error_hook(&self, hook: Box<dyn ErrorHook>) {
        self.error_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Execute all before_publish hooks
    ///
    /// Note: Acquires and releases read lock for each hook to avoid holding lock across await.
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_before_publish(
        &self,
        message: &mut Message,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .publish_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .publish_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].before_publish(message, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all after_publish hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_after_publish(
        &self,
        message: &Message,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .publish_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .publish_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].after_publish(message, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all before_consume hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_before_consume(&self, ctx: &HookContext) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .consume_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .consume_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].before_consume(ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all after_consume hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_after_consume(
        &self,
        message: &mut Message,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .consume_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .consume_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].after_consume(message, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all acknowledgment hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_on_acknowledgment(
        &self,
        status: &AckStatus,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self.ack_hooks.read().expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self.ack_hooks.read().expect("lock should not be poisoned");
                guard[i].on_acknowledgment(status, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all error hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_on_error(&self, error: &str, ctx: &HookContext) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .error_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .error_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].on_error(error, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Get the number of registered publish hooks
    pub fn publish_hook_count(&self) -> usize {
        self.publish_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Get the number of registered consume hooks
    pub fn consume_hook_count(&self) -> usize {
        self.consume_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Get the number of registered acknowledgment hooks
    pub fn acknowledgment_hook_count(&self) -> usize {
        self.ack_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Get the number of registered error hooks
    pub fn error_hook_count(&self) -> usize {
        self.error_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_protocol::builder::MessageBuilder;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingPublishHook {
        before_count: Arc<AtomicUsize>,
        after_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PublishHook for CountingPublishHook {
        async fn before_publish(
            &self,
            _message: &mut Message,
            _ctx: &HookContext,
        ) -> HookResult<()> {
            self.before_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn after_publish(&self, _message: &Message, _ctx: &HookContext) -> HookResult<()> {
            self.after_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct ValidationPublishHook;

    #[async_trait]
    impl PublishHook for ValidationPublishHook {
        async fn before_publish(
            &self,
            message: &mut Message,
            _ctx: &HookContext,
        ) -> HookResult<()> {
            if message.headers.task.is_empty() {
                return Err("Task name cannot be empty".into());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_publish_hooks() {
        let registry = AmqpHookRegistry::new();
        let before_count = Arc::new(AtomicUsize::new(0));
        let after_count = Arc::new(AtomicUsize::new(0));

        registry.add_publish_hook(Box::new(CountingPublishHook {
            before_count: before_count.clone(),
            after_count: after_count.clone(),
        }));

        let mut message = MessageBuilder::new("test.task").build().unwrap();
        let ctx = HookContext::new("test_queue".to_string());

        registry
            .execute_before_publish(&mut message, &ctx)
            .await
            .unwrap();
        assert_eq!(before_count.load(Ordering::SeqCst), 1);

        registry
            .execute_after_publish(&message, &ctx)
            .await
            .unwrap();
        assert_eq!(after_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_validation_hook() {
        let registry = AmqpHookRegistry::new();
        registry.add_publish_hook(Box::new(ValidationPublishHook));

        let mut message = MessageBuilder::new("").build().unwrap();
        let ctx = HookContext::new("test_queue".to_string());

        let result = registry.execute_before_publish(&mut message, &ctx).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Task name cannot be empty"));
    }

    #[tokio::test]
    async fn test_ack_status() {
        let status = AckStatus::Acknowledged { delivery_tag: 123 };
        assert_eq!(status.delivery_tag(), 123);
        assert!(status.is_success());
        assert!(!status.is_rejection());
        assert!(!status.should_requeue());

        let status = AckStatus::Rejected {
            delivery_tag: 456,
            requeue: true,
        };
        assert_eq!(status.delivery_tag(), 456);
        assert!(!status.is_success());
        assert!(status.is_rejection());
        assert!(status.should_requeue());
    }

    #[tokio::test]
    async fn test_hook_registry_counts() {
        let registry = AmqpHookRegistry::new();
        assert_eq!(registry.publish_hook_count(), 0);
        assert_eq!(registry.consume_hook_count(), 0);
        assert_eq!(registry.acknowledgment_hook_count(), 0);
        assert_eq!(registry.error_hook_count(), 0);

        registry.add_publish_hook(Box::new(ValidationPublishHook));
        assert_eq!(registry.publish_hook_count(), 1);
    }

    #[tokio::test]
    async fn test_hook_context() {
        let ctx = HookContext::new("test_queue".to_string())
            .with_routing_key("test.routing.key".to_string())
            .with_metadata("key1".to_string(), "value1".to_string());

        assert_eq!(ctx.target, "test_queue");
        assert_eq!(ctx.routing_key, Some("test.routing.key".to_string()));
        assert_eq!(ctx.get_metadata("key1"), Some(&"value1".to_string()));
        assert_eq!(ctx.get_metadata("key2"), None);
    }
}
