//! Middleware trait and chain definitions.

use async_trait::async_trait;
use celers_protocol::Message;
use std::time::Duration;

use crate::{Consumer, Envelope, Producer, Result};

// =============================================================================
// Middleware Support
// =============================================================================

/// Message middleware for pre/post processing
#[async_trait]
pub trait MessageMiddleware: Send + Sync {
    /// Process message before publishing
    async fn before_publish(&self, message: &mut Message) -> Result<()>;

    /// Process message after consuming
    async fn after_consume(&self, message: &mut Message) -> Result<()>;

    /// Get middleware name for logging
    fn name(&self) -> &str;
}

/// Middleware chain for processing messages
///
/// # Examples
///
/// ```
/// use celers_kombu::{MiddlewareChain, ValidationMiddleware, LoggingMiddleware};
///
/// let chain = MiddlewareChain::new()
///     .with_middleware(Box::new(ValidationMiddleware::new()))
///     .with_middleware(Box::new(LoggingMiddleware::new("MyApp")));
///
/// assert_eq!(chain.len(), 2);
/// assert!(!chain.is_empty());
/// ```
pub struct MiddlewareChain {
    middlewares: Vec<Box<dyn MessageMiddleware>>,
}

impl MiddlewareChain {
    /// Create a new empty middleware chain
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn MessageMiddleware>) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// Process message through all middlewares (before publish)
    pub async fn process_before_publish(&self, message: &mut Message) -> Result<()> {
        for middleware in &self.middlewares {
            middleware.before_publish(message).await?;
        }
        Ok(())
    }

    /// Process message through all middlewares (after consume)
    pub async fn process_after_consume(&self, message: &mut Message) -> Result<()> {
        for middleware in &self.middlewares {
            middleware.after_consume(message).await?;
        }
        Ok(())
    }

    /// Get number of middlewares in chain
    pub fn len(&self) -> usize {
        self.middlewares.len()
    }

    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }
}

impl Default for MiddlewareChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Producer with middleware support
#[async_trait]
pub trait MiddlewareProducer: Producer {
    /// Publish a message with middleware processing
    async fn publish_with_middleware(
        &mut self,
        queue: &str,
        mut message: Message,
        chain: &MiddlewareChain,
    ) -> Result<()> {
        // Process through middleware chain
        chain.process_before_publish(&mut message).await?;
        // Publish the processed message
        self.publish(queue, message).await
    }
}

/// Consumer with middleware support
#[async_trait]
pub trait MiddlewareConsumer: Consumer {
    /// Consume a message with middleware processing
    async fn consume_with_middleware(
        &mut self,
        queue: &str,
        timeout: Duration,
        chain: &MiddlewareChain,
    ) -> Result<Option<Envelope>> {
        // Consume the message
        if let Some(mut envelope) = self.consume(queue, timeout).await? {
            // Process through middleware chain
            chain.process_after_consume(&mut envelope.message).await?;
            Ok(Some(envelope))
        } else {
            Ok(None)
        }
    }
}
