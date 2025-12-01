//! Queue control implementation
//!
//! Provides queue management capabilities including:
//! - Queue pausing and resuming
//! - Drain mode (process remaining but don't accept new)
//! - Emergency stop

use celers_core::{CelersError, Result};
use redis::AsyncCommands;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Queue pause state key suffix
const PAUSE_KEY_SUFFIX: &str = ":paused";

/// Queue drain mode key suffix
const DRAIN_KEY_SUFFIX: &str = ":drain";

/// Queue control state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueState {
    /// Queue is active (normal operation)
    Active,
    /// Queue is paused (no dequeue, no enqueue)
    Paused,
    /// Queue is draining (no enqueue, dequeue allowed)
    Draining,
}

impl std::fmt::Display for QueueState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueState::Active => write!(f, "active"),
            QueueState::Paused => write!(f, "paused"),
            QueueState::Draining => write!(f, "draining"),
        }
    }
}

/// Queue controller for managing queue state
pub struct QueueController {
    client: redis::Client,
    queue_name: String,
    /// Local emergency stop flag (doesn't require Redis)
    emergency_stop: Arc<AtomicBool>,
}

impl QueueController {
    /// Create a new queue controller
    pub fn new(client: redis::Client, queue_name: &str) -> Self {
        Self {
            client,
            queue_name: queue_name.to_string(),
            emergency_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the pause key for this queue
    fn pause_key(&self) -> String {
        format!("{}{}", self.queue_name, PAUSE_KEY_SUFFIX)
    }

    /// Get the drain key for this queue
    fn drain_key(&self) -> String {
        format!("{}{}", self.queue_name, DRAIN_KEY_SUFFIX)
    }

    /// Pause the queue (stops both enqueue and dequeue)
    pub async fn pause(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        // Set pause flag and remove drain flag
        let mut pipe = redis::pipe();
        pipe.set(self.pause_key(), "1");
        pipe.del(self.drain_key());

        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to pause queue: {}", e)))?;

        Ok(())
    }

    /// Resume the queue (normal operation)
    pub async fn resume(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        // Remove both pause and drain flags
        let mut pipe = redis::pipe();
        pipe.del(self.pause_key());
        pipe.del(self.drain_key());

        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to resume queue: {}", e)))?;

        // Also clear emergency stop
        self.emergency_stop.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Enable drain mode (dequeue allowed, enqueue blocked)
    pub async fn drain(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        // Set drain flag and remove pause flag
        let mut pipe = redis::pipe();
        pipe.set(self.drain_key(), "1");
        pipe.del(self.pause_key());

        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to set drain mode: {}", e)))?;

        Ok(())
    }

    /// Trigger emergency stop (local flag, instant effect)
    pub fn emergency_stop(&self) {
        self.emergency_stop.store(true, Ordering::SeqCst);
    }

    /// Check if emergency stop is active
    pub fn is_emergency_stopped(&self) -> bool {
        self.emergency_stop.load(Ordering::SeqCst)
    }

    /// Get current queue state
    pub async fn get_state(&self) -> Result<QueueState> {
        if self.is_emergency_stopped() {
            return Ok(QueueState::Paused);
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let paused: Option<String> = conn
            .get(self.pause_key())
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check pause state: {}", e)))?;

        if paused.is_some() {
            return Ok(QueueState::Paused);
        }

        let draining: Option<String> = conn
            .get(self.drain_key())
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check drain state: {}", e)))?;

        if draining.is_some() {
            return Ok(QueueState::Draining);
        }

        Ok(QueueState::Active)
    }

    /// Check if enqueue is allowed
    pub async fn can_enqueue(&self) -> Result<bool> {
        let state = self.get_state().await?;
        Ok(state == QueueState::Active)
    }

    /// Check if dequeue is allowed
    pub async fn can_dequeue(&self) -> Result<bool> {
        let state = self.get_state().await?;
        Ok(state == QueueState::Active || state == QueueState::Draining)
    }

    /// Get a clone of the emergency stop flag for sharing
    pub fn emergency_stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.emergency_stop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_state_display() {
        assert_eq!(QueueState::Active.to_string(), "active");
        assert_eq!(QueueState::Paused.to_string(), "paused");
        assert_eq!(QueueState::Draining.to_string(), "draining");
    }

    #[test]
    fn test_emergency_stop() {
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let controller = QueueController::new(client, "test_queue");

        assert!(!controller.is_emergency_stopped());

        controller.emergency_stop();
        assert!(controller.is_emergency_stopped());

        // Test flag cloning
        let flag = controller.emergency_stop_flag();
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_key_generation() {
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let controller = QueueController::new(client, "my_queue");

        assert_eq!(controller.pause_key(), "my_queue:paused");
        assert_eq!(controller.drain_key(), "my_queue:drain");
    }
}
