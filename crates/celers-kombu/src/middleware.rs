//! Middleware trait and chain definitions.

use async_trait::async_trait;
use celers_protocol::Message;
use std::time::Duration;

use crate::{BrokerError, Consumer, Envelope, Producer, Result};

// =============================================================================
// Middleware Support
// =============================================================================

/// Outcome of running a message through a [`MiddlewareChain`] on the consume
/// side.
///
/// Some middlewares (deduplication, content filtering, statistical
/// sampling, retry-limit enforcement, ...) need to signal "this message
/// should not be delivered to the caller" as part of their normal,
/// designed operation rather than as a genuine processing failure. Before
/// this type existed the only way to express that was to return an `Err`
/// from [`MessageMiddleware::after_consume`], which [`MiddlewareChain`] and
/// [`MiddlewareConsumer::consume_with_middleware`] had no way to
/// distinguish from a real error - the envelope (and with it the broker
/// delivery tag) was simply dropped, leaving the message neither
/// acknowledged nor rejected.
///
/// `MiddlewareDecision` is produced by [`MiddlewareChain::process_after_consume`]
/// and lets callers settle the delivery tag correctly in both cases: ack
/// (or reject-without-requeue) a designed [`MiddlewareDecision::Drop`], and
/// reject-with-requeue a genuine `Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MiddlewareDecision {
    /// The message passed every middleware in the chain (unchanged or
    /// transformed) and should be handed to the caller for normal
    /// processing.
    Accept,
    /// A middleware intentionally decided this message must not be
    /// processed further. This is not an error: the message should be
    /// removed from the queue (acknowledged / rejected without requeue)
    /// rather than redelivered.
    Drop {
        /// Name of the middleware that requested the drop (see
        /// [`MessageMiddleware::name`]).
        middleware: String,
        /// Human-readable reason, taken from the underlying error.
        reason: String,
    },
}

impl MiddlewareDecision {
    /// True if this decision means the message should be delivered to the
    /// caller.
    pub fn is_accept(&self) -> bool {
        matches!(self, MiddlewareDecision::Accept)
    }

    /// True if this decision means the message was intentionally dropped.
    pub fn is_drop(&self) -> bool {
        matches!(self, MiddlewareDecision::Drop { .. })
    }
}

/// Resolve the effective priority (0-9) for a message.
///
/// Prefers the typed [`celers_protocol::MessageProperties::priority`]
/// field - the field actually populated by
/// [`celers_protocol::Message::with_priority`] and the message builder, and
/// the one real priority-queue implementations in this workspace read
/// (e.g. `celers_protocol::priority_queue`) - falling back to a legacy
/// `headers.extra["priority"]` marker for callers that only ever set it
/// there, and finally to `default` if neither is present.
pub(crate) fn effective_priority(message: &Message, default: u8) -> u8 {
    message.properties.priority.unwrap_or_else(|| {
        message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(default)
    })
}

/// Resolve the effective retry count for a message.
///
/// Prefers the typed [`celers_protocol::MessageHeaders::retries`] field
/// (the one field name actually reserved on the wire format for this
/// purpose - see the `retries` doc comment on `MessageHeaders`), falling
/// back to a legacy `headers.extra["retries"]` marker for callers that only
/// ever set it there.
pub(crate) fn effective_retries(message: &Message) -> u32 {
    message.headers.retries.unwrap_or_else(|| {
        message
            .headers
            .extra
            .get("retries")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32
    })
}

/// Message middleware for pre/post processing
#[async_trait]
pub trait MessageMiddleware: Send + Sync {
    /// Process message before publishing
    async fn before_publish(&self, message: &mut Message) -> Result<()>;

    /// Process message after consuming
    async fn after_consume(&self, message: &mut Message) -> Result<()>;

    /// Get middleware name for logging
    fn name(&self) -> &str;

    /// Classify whether an `Err` returned from [`Self::after_consume`]
    /// represents an intentional decision to drop the message (duplicate
    /// detected, filtered out, sampled out, retry limit exceeded, ...)
    /// rather than a genuine processing failure that should cause
    /// redelivery.
    ///
    /// The default is `false`: every `Err` is treated as a real failure.
    /// Middlewares that use `Err` as their "drop this message" signal
    /// should override this to return `true` (unconditionally, or after
    /// inspecting `err` if the middleware has more than one error path)
    /// so that [`MiddlewareChain::process_after_consume`] and
    /// [`MiddlewareConsumer::consume_with_middleware`] can settle the
    /// delivery tag as an intentional drop instead of leaving it
    /// unacknowledged or endlessly redelivering it.
    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        false
    }
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
    ///
    /// Middlewares are run in **reverse** installation order on the
    /// consume side. `before_publish` wraps a message outward (each
    /// middleware transforms the output of the previous one, e.g.
    /// compress-then-sign), so unwrapping it must peel those
    /// transformations off in the opposite order (verify-then-decompress);
    /// running `after_consume` forward would, for example, try to verify a
    /// signature computed over the *compressed* body against the
    /// already-decompressed one. See [`MessageMiddleware::before_publish`]
    /// vs [`MessageMiddleware::after_consume`].
    ///
    /// A middleware whose `after_consume` returns an `Err` it classifies
    /// (via [`MessageMiddleware::is_drop_signal`]) as an intentional drop
    /// short-circuits the chain with `Ok(`[`MiddlewareDecision::Drop`]`)`
    /// instead of propagating the error, so callers can settle the
    /// delivery without treating a designed filter as a failure. Any other
    /// `Err` still propagates.
    pub async fn process_after_consume(&self, message: &mut Message) -> Result<MiddlewareDecision> {
        for middleware in self.middlewares.iter().rev() {
            if let Err(err) = middleware.after_consume(message).await {
                if middleware.is_drop_signal(&err) {
                    return Ok(MiddlewareDecision::Drop {
                        middleware: middleware.name().to_string(),
                        reason: err.to_string(),
                    });
                }
                return Err(err);
            }
        }
        Ok(MiddlewareDecision::Accept)
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
    ///
    /// The envelope's delivery tag is always settled before this method
    /// returns:
    ///
    /// - If every middleware accepts the message, the envelope is handed
    ///   back as `Ok(Some(envelope))` (unsettled - the caller is expected
    ///   to `ack`/`reject` it once processing completes, same as
    ///   [`Consumer::consume`]).
    /// - If a middleware intentionally drops the message (see
    ///   [`MiddlewareDecision::Drop`]), the envelope is rejected without
    ///   requeue (removed from the queue / sent to a DLQ where the broker
    ///   supports it) and `Ok(None)` is returned, exactly as if no message
    ///   had been available.
    /// - If a middleware returns a genuine error, the envelope is rejected
    ///   *with* requeue (so it is not silently lost - another attempt, or
    ///   another consumer, gets a chance to process it) and the error is
    ///   propagated.
    ///
    /// Previously the chain error was propagated directly with `?`, which
    /// discarded the `Envelope` - and with it the delivery tag - without
    /// ever acking or rejecting it, leaving the message stuck for
    /// ack-based brokers.
    async fn consume_with_middleware(
        &mut self,
        queue: &str,
        timeout: Duration,
        chain: &MiddlewareChain,
    ) -> Result<Option<Envelope>> {
        // Consume the message
        let Some(mut envelope) = self.consume(queue, timeout).await? else {
            return Ok(None);
        };

        match chain.process_after_consume(&mut envelope.message).await {
            Ok(MiddlewareDecision::Accept) => Ok(Some(envelope)),
            Ok(MiddlewareDecision::Drop { middleware, reason }) => {
                tracing::warn!(
                    delivery_tag = %envelope.delivery_tag,
                    task = %envelope.task_name(),
                    middleware = %middleware,
                    reason = %reason,
                    "Dropping message"
                );
                // Best-effort: settling the tag matters more than the
                // settle call itself succeeding (the message is being
                // dropped either way, and a broker-level error here is
                // reported the same way a lost ack from any other consumer
                // would be - via the broker's own redelivery/visibility
                // timeout).
                let _ = self.reject(&envelope.delivery_tag, false).await;
                Ok(None)
            }
            Err(err) => {
                // A genuine processing failure: requeue rather than
                // silently leaking the delivery tag, so the message gets
                // another chance (possibly on a different consumer)
                // instead of being lost.
                let _ = self.reject(&envelope.delivery_tag, true).await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Transport;
    use async_trait::async_trait;
    use celers_protocol::Message;
    use uuid::Uuid;

    // -------------------------------------------------------------------
    // idx108: after_consume must unwind a transform pipeline in reverse.
    // -------------------------------------------------------------------

    /// Test-only stand-in for a body-compressing middleware: reverses the
    /// byte order of whatever body is currently present. Reversing twice
    /// is a no-op, but composed with `ChecksumMiddleware` below (which
    /// checksums whatever bytes are present *at the point it runs*) the
    /// pair becomes order-sensitive in exactly the way real
    /// compression+signing middleware pairs are (see idx108).
    struct ReverseMiddleware;

    #[async_trait]
    impl MessageMiddleware for ReverseMiddleware {
        async fn before_publish(&self, message: &mut Message) -> Result<()> {
            message.body.reverse();
            Ok(())
        }

        async fn after_consume(&self, message: &mut Message) -> Result<()> {
            message.body.reverse();
            Ok(())
        }

        fn name(&self) -> &str {
            "test-reverse"
        }
    }

    /// Test-only stand-in for a signing middleware: appends a checksum
    /// byte (wrapping sum) over the *current* body on publish, and on
    /// consume pops the last byte and verifies it against the checksum of
    /// the remaining bytes, failing loudly (mirroring HMAC verification
    /// failure) on mismatch rather than returning corrupted data.
    struct ChecksumMiddleware;

    impl ChecksumMiddleware {
        fn checksum(body: &[u8]) -> u8 {
            body.iter().fold(0u8, |acc, b| acc.wrapping_add(*b))
        }
    }

    #[async_trait]
    impl MessageMiddleware for ChecksumMiddleware {
        async fn before_publish(&self, message: &mut Message) -> Result<()> {
            let checksum = Self::checksum(&message.body);
            message.body.push(checksum);
            Ok(())
        }

        async fn after_consume(&self, message: &mut Message) -> Result<()> {
            let claimed = message
                .body
                .pop()
                .ok_or_else(|| BrokerError::OperationFailed("empty body".to_string()))?;
            let actual = Self::checksum(&message.body);
            if claimed != actual {
                return Err(BrokerError::OperationFailed(
                    "checksum verification failed".to_string(),
                ));
            }
            Ok(())
        }

        fn name(&self) -> &str {
            "test-checksum"
        }
    }

    #[tokio::test]
    async fn after_consume_unwinds_transform_chain_in_reverse_order() {
        let publish_chain = MiddlewareChain::new()
            .with_middleware(Box::new(ReverseMiddleware))
            .with_middleware(Box::new(ChecksumMiddleware));

        let original = b"hello celers".to_vec();
        let mut message = Message::new("test_task".to_string(), Uuid::new_v4(), original.clone());

        publish_chain
            .process_before_publish(&mut message)
            .await
            .unwrap();
        // Sanity check: the wire body really was transformed.
        assert_ne!(message.body, original);

        // A chain built with the *same* installation order must undo the
        // transforms correctly on the consume side, because
        // `process_after_consume` walks it in reverse internally.
        let consume_chain = MiddlewareChain::new()
            .with_middleware(Box::new(ReverseMiddleware))
            .with_middleware(Box::new(ChecksumMiddleware));

        let decision = consume_chain
            .process_after_consume(&mut message)
            .await
            .unwrap();
        assert_eq!(decision, MiddlewareDecision::Accept);
        assert_eq!(message.body, original);
    }

    #[tokio::test]
    async fn after_consume_forward_order_would_break_round_trip() {
        // Regression guard for the bug itself: manually driving the two
        // middlewares in *forward* (publish) order on the consume side -
        // the behaviour before this fix - must fail, demonstrating why the
        // reversal in `process_after_consume` is required rather than
        // incidental.
        let reverse = ReverseMiddleware;
        let checksum = ChecksumMiddleware;

        let original = b"hello celers".to_vec();
        let mut message = Message::new("test_task".to_string(), Uuid::new_v4(), original);

        reverse.before_publish(&mut message).await.unwrap();
        checksum.before_publish(&mut message).await.unwrap();

        // Buggy forward order: reverse first, checksum second.
        assert!(reverse.after_consume(&mut message).await.is_ok());
        assert!(checksum.after_consume(&mut message).await.is_err());
    }

    // -------------------------------------------------------------------
    // idx122: consume_with_middleware must settle the delivery tag rather
    // than silently discarding the envelope on a middleware error.
    // -------------------------------------------------------------------

    struct AlwaysDropMiddleware;

    #[async_trait]
    impl MessageMiddleware for AlwaysDropMiddleware {
        async fn before_publish(&self, _message: &mut Message) -> Result<()> {
            Ok(())
        }

        async fn after_consume(&self, _message: &mut Message) -> Result<()> {
            Err(BrokerError::OperationFailed("dropped by test".to_string()))
        }

        fn name(&self) -> &str {
            "test-always-drop"
        }

        fn is_drop_signal(&self, _err: &BrokerError) -> bool {
            true
        }
    }

    struct AlwaysFailMiddleware;

    #[async_trait]
    impl MessageMiddleware for AlwaysFailMiddleware {
        async fn before_publish(&self, _message: &mut Message) -> Result<()> {
            Ok(())
        }

        async fn after_consume(&self, _message: &mut Message) -> Result<()> {
            Err(BrokerError::OperationFailed("genuine failure".to_string()))
        }

        fn name(&self) -> &str {
            "test-always-fail"
        }
        // is_drop_signal defaults to false: this is a real failure.
    }

    #[tokio::test]
    async fn consume_with_middleware_settles_dropped_envelope() {
        use crate::MockBroker;

        let mut broker = MockBroker::new();
        broker.connect().await.unwrap();

        let message = Message::new("test_task".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
        broker.publish("queue", message).await.unwrap();

        let chain = MiddlewareChain::new().with_middleware(Box::new(AlwaysDropMiddleware));

        let result = broker
            .consume_with_middleware("queue", Duration::from_secs(1), &chain)
            .await
            .unwrap();

        // The message must not be handed back to the caller...
        assert!(result.is_none());
        // ...but it must have been settled (rejected), not leaked: nothing
        // is left pending acknowledgement, and re-consuming the (now
        // empty) queue yields nothing rather than the same envelope stuck
        // forever.
        assert_eq!(broker.queue_len("queue"), 0);
        let requeued = broker
            .consume_with_middleware("queue", Duration::from_secs(1), &chain)
            .await
            .unwrap();
        assert!(requeued.is_none());
    }

    #[tokio::test]
    async fn consume_with_middleware_requeues_on_genuine_failure() {
        use crate::MockBroker;

        let mut broker = MockBroker::new();
        broker.connect().await.unwrap();

        // MockBroker::reject(.., requeue=true) always redelivers onto a
        // fixed "celery" queue (it doesn't track which queue an envelope
        // originally came from), so publish there too.
        let message = Message::new("test_task".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
        broker.publish("celery", message).await.unwrap();

        let chain = MiddlewareChain::new().with_middleware(Box::new(AlwaysFailMiddleware));

        let result = broker
            .consume_with_middleware("celery", Duration::from_secs(1), &chain)
            .await;

        // A genuine failure propagates as an error...
        assert!(result.is_err());
        // ...but the delivery tag was still settled (rejected with
        // requeue=true), so the message is back on the queue instead of
        // being leaked with no delivery tag at all.
        assert_eq!(broker.queue_len("celery"), 1);
    }

    #[tokio::test]
    async fn consume_with_middleware_accepts_normally() {
        use crate::MockBroker;

        struct OkMiddleware;
        #[async_trait]
        impl MessageMiddleware for OkMiddleware {
            async fn before_publish(&self, _message: &mut Message) -> Result<()> {
                Ok(())
            }
            async fn after_consume(&self, _message: &mut Message) -> Result<()> {
                Ok(())
            }
            fn name(&self) -> &str {
                "test-ok"
            }
        }

        let mut broker = MockBroker::new();
        broker.connect().await.unwrap();

        let task_id = Uuid::new_v4();
        let message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
        broker.publish("queue", message).await.unwrap();

        let chain = MiddlewareChain::new().with_middleware(Box::new(OkMiddleware));
        let result = broker
            .consume_with_middleware("queue", Duration::from_secs(1), &chain)
            .await
            .unwrap();

        assert_eq!(result.unwrap().task_id(), task_id);
    }

    #[test]
    fn effective_priority_prefers_typed_field_over_legacy_header() {
        let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        // Neither set: falls back to the provided default.
        assert_eq!(effective_priority(&message, 5), 5);

        // Legacy header only: used as a fallback.
        message
            .headers
            .extra
            .insert("priority".to_string(), serde_json::json!(2));
        assert_eq!(effective_priority(&message, 5), 2);

        // Typed field set: takes precedence over the legacy header, even
        // though the (now stale) legacy header is still present.
        message.properties.priority = Some(9);
        assert_eq!(effective_priority(&message, 5), 9);
    }

    #[test]
    fn effective_retries_prefers_typed_field_over_legacy_header() {
        let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        assert_eq!(effective_retries(&message), 0);

        message
            .headers
            .extra
            .insert("retries".to_string(), serde_json::json!(4));
        assert_eq!(effective_retries(&message), 4);

        message.headers.retries = Some(7);
        assert_eq!(effective_retries(&message), 7);
    }
}
