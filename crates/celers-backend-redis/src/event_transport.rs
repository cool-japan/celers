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
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use tokio::sync::RwLock;

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
    /// Connection for publishing (kept open for efficiency)
    conn: Arc<RwLock<Option<redis::aio::MultiplexedConnection>>>,
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
            conn: Arc::new(RwLock::new(None)),
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
            conn: Arc::new(RwLock::new(None)),
        })
    }

    /// Get or create a connection
    async fn get_connection(
        &self,
    ) -> std::result::Result<redis::aio::MultiplexedConnection, crate::BackendError> {
        // Check if we have a cached connection
        {
            let conn_guard = self.conn.read().await;
            if let Some(ref conn) = *conn_guard {
                return Ok(conn.clone());
            }
        }

        // Create a new connection
        let conn = self.client.get_multiplexed_async_connection().await?;

        // Cache it
        {
            let mut conn_guard = self.conn.write().await;
            *conn_guard = Some(conn.clone());
        }

        Ok(conn)
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

        Ok(())
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
