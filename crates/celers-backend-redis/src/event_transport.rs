//! Redis event transport for real-time event publishing
//!
//! This module provides Redis pub/sub based event transport for CeleRS events.
//! It implements the `EventEmitter` trait from `celers-core` and publishes
//! events to Redis channels following Celery's event protocol.
//!
//! # Celery Event Channels
//!
//! Events are published to the following Redis channels:
//! - `celeryev` - Default channel for all events
//! - `celeryev.task` - Task events only
//! - `celeryev.worker` - Worker events only
//!
//! # Example
//!
//! ```no_run
//! use celers_backend_redis::event_transport::RedisEventEmitter;
//! use celers_core::event::{Event, EventEmitter, WorkerEventBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let emitter = RedisEventEmitter::new("redis://localhost")?;
//!
//! // Emit a worker online event
//! let event = WorkerEventBuilder::new("worker-1").online();
//! emitter.emit(event).await?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use celers_core::event::{Event, EventEmitter};
use celers_core::{CelersError, Result};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Default channel name for all events
const DEFAULT_CHANNEL: &str = "celeryev";

/// Channel name for task events
const TASK_CHANNEL: &str = "celeryev.task";

/// Channel name for worker events
const WORKER_CHANNEL: &str = "celeryev.worker";

/// Configuration for Redis event transport
#[derive(Debug, Clone)]
pub struct RedisEventConfig {
    /// Main event channel name
    pub channel: String,

    /// Task event channel name (set to None to disable)
    pub task_channel: Option<String>,

    /// Worker event channel name (set to None to disable)
    pub worker_channel: Option<String>,

    /// Whether to publish to type-specific channels
    pub publish_to_type_channels: bool,

    /// Whether the emitter is enabled
    pub enabled: bool,
}

impl Default for RedisEventConfig {
    fn default() -> Self {
        Self {
            channel: DEFAULT_CHANNEL.to_string(),
            task_channel: Some(TASK_CHANNEL.to_string()),
            worker_channel: Some(WORKER_CHANNEL.to_string()),
            publish_to_type_channels: true,
            enabled: true,
        }
    }
}

impl RedisEventConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the main channel name
    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = channel.into();
        self
    }

    /// Set the task channel name (or None to disable)
    pub fn task_channel(mut self, channel: Option<String>) -> Self {
        self.task_channel = channel;
        self
    }

    /// Set the worker channel name (or None to disable)
    pub fn worker_channel(mut self, channel: Option<String>) -> Self {
        self.worker_channel = channel;
        self
    }

    /// Enable or disable type-specific channel publishing
    pub fn publish_to_type_channels(mut self, enabled: bool) -> Self {
        self.publish_to_type_channels = enabled;
        self
    }

    /// Enable or disable the emitter
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Redis event emitter using pub/sub
///
/// Publishes CeleRS events to Redis channels for real-time monitoring
/// and event-driven architectures.
pub struct RedisEventEmitter {
    client: Client,
    config: RedisEventConfig,
    /// Reconnecting connection for publishing.
    ///
    /// A [`ConnectionManager`] re-dials with backoff on its own. The previous
    /// implementation cached a `MultiplexedConnection` that nothing ever
    /// replaced, so after a Redis restart, failover or idle disconnect every
    /// subsequent `emit` failed permanently for the lifetime of the process.
    conn: Arc<OnceCell<ConnectionManager>>,
}

impl RedisEventEmitter {
    /// Create a new Redis event emitter with default configuration
    ///
    /// # Arguments
    /// * `url` - Redis connection URL (e.g., "redis://localhost:6379")
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::event_transport::RedisEventEmitter;
    ///
    /// let emitter = RedisEventEmitter::new("redis://localhost").unwrap();
    /// ```
    pub fn new(url: &str) -> std::result::Result<Self, crate::BackendError> {
        let client = Client::open(url).map_err(|e| {
            crate::BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self {
            client,
            config: RedisEventConfig::default(),
            conn: Arc::new(OnceCell::new()),
        })
    }

    /// Create a new Redis event emitter with custom configuration
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `config` - Event transport configuration
    pub fn with_config(
        url: &str,
        config: RedisEventConfig,
    ) -> std::result::Result<Self, crate::BackendError> {
        let client = Client::open(url).map_err(|e| {
            crate::BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self {
            client,
            config,
            conn: Arc::new(OnceCell::new()),
        })
    }

    /// Get the shared reconnecting connection, establishing it on first use.
    ///
    /// If the initial connect fails the cell stays empty, so the next call
    /// re-dials rather than caching a permanent failure.
    async fn get_connection(&self) -> std::result::Result<ConnectionManager, crate::BackendError> {
        self.conn
            .get_or_try_init(|| async {
                ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(crate::BackendError::from)
            })
            .await
            .cloned()
    }

    /// Publish an event to a channel
    async fn publish(&self, channel: &str, event_json: &str) -> Result<()> {
        let mut conn = self
            .get_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        conn.publish::<_, _, ()>(channel, event_json)
            .await
            .map_err(|e| CelersError::Other(format!("Redis publish error: {}", e)))?;

        Ok(())
    }

    /// Get the configuration
    pub fn config(&self) -> &RedisEventConfig {
        &self.config
    }

    /// Check if the emitter is enabled
    pub fn is_active(&self) -> bool {
        self.config.enabled
    }

    /// Get the main channel name
    pub fn channel(&self) -> &str {
        &self.config.channel
    }
}

#[async_trait]
impl EventEmitter for RedisEventEmitter {
    async fn emit(&self, event: Event) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Serialize the event to JSON
        let event_json = serde_json::to_string(&event)
            .map_err(|e| CelersError::Other(format!("Event serialization error: {}", e)))?;

        // Publish to main channel
        self.publish(&self.config.channel, &event_json).await?;

        // Publish to type-specific channel if enabled
        if self.config.publish_to_type_channels {
            match &event {
                Event::Task(_) => {
                    if let Some(ref task_channel) = self.config.task_channel {
                        self.publish(task_channel, &event_json).await?;
                    }
                }
                Event::Worker(_) => {
                    if let Some(ref worker_channel) = self.config.worker_channel {
                        self.publish(worker_channel, &event_json).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn emit_batch(&self, events: Vec<Event>) -> Result<()> {
        if !self.config.enabled || events.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .get_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        // Use pipelining for efficient batch publishing
        let mut pipe = redis::pipe();

        for event in &events {
            let event_json = serde_json::to_string(event)
                .map_err(|e| CelersError::Other(format!("Event serialization error: {}", e)))?;

            // Add to main channel
            pipe.publish(&self.config.channel, &event_json);

            // Add to type-specific channel if enabled
            if self.config.publish_to_type_channels {
                match event {
                    Event::Task(_) => {
                        if let Some(ref task_channel) = self.config.task_channel {
                            pipe.publish(task_channel, &event_json);
                        }
                    }
                    Event::Worker(_) => {
                        if let Some(ref worker_channel) = self.config.worker_channel {
                            pipe.publish(worker_channel, &event_json);
                        }
                    }
                }
            }
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Redis event receiver for subscribing to events
///
/// Subscribes to Redis pub/sub channels and receives CeleRS events.
pub struct RedisEventReceiver {
    client: Client,
    channels: Vec<String>,
}

impl RedisEventReceiver {
    /// Create a new event receiver subscribing to default channels
    pub fn new(url: &str) -> std::result::Result<Self, crate::BackendError> {
        let client = Client::open(url).map_err(|e| {
            crate::BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self {
            client,
            channels: vec![DEFAULT_CHANNEL.to_string()],
        })
    }

    /// Create a new event receiver subscribing to specific channels
    pub fn with_channels(
        url: &str,
        channels: Vec<String>,
    ) -> std::result::Result<Self, crate::BackendError> {
        let client = Client::open(url).map_err(|e| {
            crate::BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self { client, channels })
    }

    /// Subscribe to all default event channels (main, task, worker)
    pub fn subscribe_all(url: &str) -> std::result::Result<Self, crate::BackendError> {
        let client = Client::open(url).map_err(|e| {
            crate::BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self {
            client,
            channels: vec![
                DEFAULT_CHANNEL.to_string(),
                TASK_CHANNEL.to_string(),
                WORKER_CHANNEL.to_string(),
            ],
        })
    }

    /// Get a pub/sub connection for receiving events
    ///
    /// Returns a Redis pub/sub connection that can be used to receive events.
    /// The caller should use the connection's `into_on_message()` or similar
    /// methods to process incoming messages.
    pub async fn subscribe(&self) -> std::result::Result<redis::aio::PubSub, crate::BackendError> {
        let conn = self.client.get_async_pubsub().await?;
        Ok(conn)
    }

    /// Get the channels this receiver is configured for
    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    /// Start receiving events and call the handler for each event
    ///
    /// This is a convenience method that subscribes to configured channels
    /// and processes incoming messages.
    ///
    /// # Errors
    /// Returns [`BackendError::Connection`](crate::BackendError::Connection)
    /// when the pub/sub stream terminates. Redis pub/sub has no orderly
    /// end-of-stream, so a stream that ends means the connection dropped —
    /// reporting `Ok(())` there made a permanently dead subscription look like
    /// a clean shutdown. Use [`Self::receive_with_reconnect`] to resubscribe
    /// automatically.
    ///
    /// # Arguments
    /// * `handler` - Async function to call for each received event
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::event_transport::RedisEventReceiver;
    /// use celers_core::event::Event;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let receiver = RedisEventReceiver::new("redis://localhost")?;
    ///
    /// receiver.receive(|event| async move {
    ///     println!("Received event: {:?}", event.event_type());
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn receive<F, Fut>(
        &self,
        mut handler: F,
    ) -> std::result::Result<(), crate::BackendError>
    where
        F: FnMut(Event) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<(), crate::BackendError>>,
    {
        use futures_util::StreamExt;

        let mut pubsub = self.subscribe().await?;

        // Subscribe to all configured channels
        for channel in &self.channels {
            pubsub.subscribe(channel).await?;
        }

        // Process messages
        let mut stream = pubsub.on_message();

        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload()?;

            // Deserialize the event
            match serde_json::from_str::<Event>(&payload) {
                Ok(event) => {
                    handler(event).await?;
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize event: {}", e);
                }
            }
        }

        Err(crate::BackendError::Connection(
            "Redis pub/sub stream ended unexpectedly".to_string(),
        ))
    }

    /// Receive events forever, resubscribing with backoff after a drop
    ///
    /// Redis pub/sub is fire-and-forget: events published while the connection
    /// is down are lost. This loop keeps the subscription alive so the outage
    /// is bounded by the reconnect delay instead of lasting for the rest of the
    /// process's life.
    ///
    /// Runs until `handler` returns an error, which is propagated to the
    /// caller. Connection failures are retried indefinitely using `strategy`'s
    /// backoff curve (its `max_attempts` bounds the *consecutive* failures
    /// tolerated before giving up).
    pub async fn receive_with_reconnect<F, Fut>(
        &self,
        strategy: crate::retry::RetryStrategy,
        mut handler: F,
    ) -> std::result::Result<(), crate::BackendError>
    where
        F: FnMut(Event) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<(), crate::BackendError>>,
    {
        let max_attempts = strategy.max_attempts.max(1);
        let mut consecutive_failures: u32 = 0;

        loop {
            let started = std::time::Instant::now();
            let outcome = self.receive(&mut handler).await;

            match outcome {
                // `receive` only ever returns on failure.
                Ok(()) => return Ok(()),
                Err(error) => {
                    if !strategy.is_retryable(&error) {
                        return Err(error);
                    }

                    // A subscription that survived longer than the backoff
                    // ceiling was healthy: do not count it against the budget.
                    if started.elapsed() > strategy.max_backoff {
                        consecutive_failures = 0;
                    }

                    consecutive_failures += 1;
                    if consecutive_failures >= max_attempts {
                        return Err(error);
                    }

                    let backoff = strategy.backoff_duration(consecutive_failures - 1);
                    tracing::warn!(
                        attempt = consecutive_failures,
                        backoff_ms = backoff.as_millis(),
                        error = %error,
                        "Redis event subscription dropped; resubscribing"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_event_config_default() {
        let config = RedisEventConfig::default();
        assert_eq!(config.channel, "celeryev");
        assert_eq!(config.task_channel, Some("celeryev.task".to_string()));
        assert_eq!(config.worker_channel, Some("celeryev.worker".to_string()));
        assert!(config.publish_to_type_channels);
        assert!(config.enabled);
    }

    #[test]
    fn test_redis_event_config_builder() {
        let config = RedisEventConfig::new()
            .channel("my-events")
            .task_channel(None)
            .worker_channel(Some("my-worker-events".to_string()))
            .publish_to_type_channels(false)
            .enabled(true);

        assert_eq!(config.channel, "my-events");
        assert!(config.task_channel.is_none());
        assert_eq!(config.worker_channel, Some("my-worker-events".to_string()));
        assert!(!config.publish_to_type_channels);
        assert!(config.enabled);
    }
}
