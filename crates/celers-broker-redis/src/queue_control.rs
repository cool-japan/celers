//! Queue control implementation
//!
//! Provides queue management capabilities including:
//! - Queue pausing and resuming
//! - Drain mode (process remaining but don't accept new)
//! - Emergency stop
//!
//! # Wiring this into a `Broker` implementation
//!
//! This module only manages the Redis-visible pause/drain flags (and a
//! local, per-instance emergency-stop flag) — it does not, by itself, stop
//! `RedisBroker::enqueue`/`dequeue` from proceeding, because that check has
//! to live in the `Broker` trait impl. Two cheap, connection-level helpers —
//! [`is_enqueue_allowed`] and [`is_dequeue_allowed`] — are exposed
//! specifically so a `Broker` impl can consult queue state with a single
//! extra round trip on a connection it already holds, without constructing a
//! full [`QueueController`] per call. `QueueController` also derives
//! [`Clone`] (cheaply — `redis::Client` is a lightweight handle and
//! `emergency_stop` is an `Arc`) so that a broker wanting emergency-stop
//! semantics can construct **one** controller up front and clone it out to
//! callers, rather than constructing a fresh one (and therefore a fresh,
//! immediately-orphaned emergency-stop flag) on every call.

use celers_core::{CelersError, Result};
use redis::AsyncCommands;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Queue pause state key suffix
const PAUSE_KEY_SUFFIX: &str = ":paused";

/// Queue drain mode key suffix
const DRAIN_KEY_SUFFIX: &str = ":drain";

/// The Redis key that holds a queue's pause flag.
///
/// Exposed as a free function so callers that already hold a connection
/// (such as a `Broker` implementation) can check queue state directly — see
/// [`is_enqueue_allowed`] / [`is_dequeue_allowed`] — without constructing a
/// [`QueueController`].
pub fn pause_key_for(queue_name: &str) -> String {
    format!("{}{}", queue_name, PAUSE_KEY_SUFFIX)
}

/// The Redis key that holds a queue's drain flag. See [`pause_key_for`].
pub fn drain_key_for(queue_name: &str) -> String {
    format!("{}{}", queue_name, DRAIN_KEY_SUFFIX)
}

/// Cheaply check whether `queue_name` currently accepts new enqueues (i.e.
/// it is neither paused nor draining), using a connection the caller already
/// holds. Issues a single `MGET` (one round trip) rather than two sequential
/// `GET`s.
pub async fn is_enqueue_allowed(
    conn: &mut redis::aio::MultiplexedConnection,
    queue_name: &str,
) -> Result<bool> {
    let keys = [pause_key_for(queue_name), drain_key_for(queue_name)];
    let vals: Vec<Option<String>> = conn
        .mget(&keys)
        .await
        .map_err(|e| CelersError::Broker(format!("Failed to check queue state: {}", e)))?;
    Ok(vals.iter().all(|v| v.is_none()))
}

/// Cheaply check whether `queue_name` currently allows dequeues (draining
/// still allows dequeue — only a full pause blocks it), using a connection
/// the caller already holds.
pub async fn is_dequeue_allowed(
    conn: &mut redis::aio::MultiplexedConnection,
    queue_name: &str,
) -> Result<bool> {
    let paused: Option<String> = conn
        .get(pause_key_for(queue_name))
        .await
        .map_err(|e| CelersError::Broker(format!("Failed to check pause state: {}", e)))?;
    Ok(paused.is_none())
}

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

/// Queue controller for managing queue state.
///
/// Cheap to [`Clone`]: construct one instance and share clones of it (all of
/// which observe the same `emergency_stop` flag via the shared `Arc`)
/// instead of constructing a fresh controller — and therefore a fresh,
/// disconnected emergency-stop flag — per call site.
#[derive(Clone)]
pub struct QueueController {
    client: redis::Client,
    queue_name: String,
    /// Local emergency stop flag (doesn't require Redis). Shared across
    /// clones of this controller via the `Arc`, so `emergency_stop()`
    /// observed through one clone is visible through every other clone (and
    /// the original) — but note it is still process-local: a *different*
    /// process's controller (even for the same `queue_name`) has its own
    /// independent flag, since nothing here is persisted to Redis.
    emergency_stop: Arc<AtomicBool>,
}

impl QueueController {
    /// Create a new queue controller.
    ///
    /// Prefer constructing this once and sharing [`Clone`]s of it rather
    /// than calling `new` again for every use — a fresh call here always
    /// starts `emergency_stop` at `false`, so if `new` runs on every
    /// call site, `emergency_stop()`/`is_emergency_stopped()` observed
    /// through different call sites will never agree with each other.
    pub fn new(client: redis::Client, queue_name: &str) -> Self {
        Self {
            client,
            queue_name: queue_name.to_string(),
            emergency_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the pause key for this queue
    fn pause_key(&self) -> String {
        pause_key_for(&self.queue_name)
    }

    /// Get the drain key for this queue
    fn drain_key(&self) -> String {
        drain_key_for(&self.queue_name)
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

        // Single round trip for both flags instead of two sequential GETs.
        let vals: Vec<Option<String>> = conn
            .mget(&[self.pause_key(), self.drain_key()])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check queue state: {}", e)))?;

        if vals.first().is_some_and(Option::is_some) {
            return Ok(QueueState::Paused);
        }

        if vals.get(1).is_some_and(Option::is_some) {
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
    fn test_clone_shares_emergency_stop_flag() {
        // The whole point of `QueueController` deriving `Clone`: a broker
        // that constructs one controller and hands out clones (instead of
        // calling `new` per call site) must have `emergency_stop()` on one
        // clone observed by every other clone.
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let original = QueueController::new(client, "test_queue_clone");
        let cloned = original.clone();

        assert!(!original.is_emergency_stopped());
        assert!(!cloned.is_emergency_stopped());

        cloned.emergency_stop();

        assert!(
            original.is_emergency_stopped(),
            "emergency stop set on a clone must be visible on the original"
        );
        assert!(cloned.is_emergency_stopped());
    }

    #[tokio::test]
    async fn test_is_enqueue_and_dequeue_allowed_reflect_pause_and_drain() {
        let queue_name = format!("test-qc-{}", uuid::Uuid::new_v4());
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let controller = QueueController::new(client.clone(), &queue_name);
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();

        // Active: both enqueue and dequeue allowed.
        assert!(is_enqueue_allowed(&mut conn, &queue_name).await.unwrap());
        assert!(is_dequeue_allowed(&mut conn, &queue_name).await.unwrap());

        // Draining: enqueue blocked, dequeue still allowed (drain the
        // backlog, accept nothing new).
        controller.drain().await.unwrap();
        assert!(!is_enqueue_allowed(&mut conn, &queue_name).await.unwrap());
        assert!(is_dequeue_allowed(&mut conn, &queue_name).await.unwrap());

        // Paused: both blocked.
        controller.pause().await.unwrap();
        assert!(!is_enqueue_allowed(&mut conn, &queue_name).await.unwrap());
        assert!(!is_dequeue_allowed(&mut conn, &queue_name).await.unwrap());

        // Resumed: both allowed again.
        controller.resume().await.unwrap();
        assert!(is_enqueue_allowed(&mut conn, &queue_name).await.unwrap());
        assert!(is_dequeue_allowed(&mut conn, &queue_name).await.unwrap());
    }

    #[test]
    fn test_key_generation() {
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let controller = QueueController::new(client, "my_queue");

        assert_eq!(controller.pause_key(), "my_queue:paused");
        assert_eq!(controller.drain_key(), "my_queue:drain");
    }
}
