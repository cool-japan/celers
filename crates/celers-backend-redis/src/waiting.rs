//! Waiting for task results
//!
//! Rather than busy-polling a fixed interval, [`RedisResultBackend::wait_for_result`]
//! subscribes to the result's notification channel first, then reads once to
//! close the race against a result that landed before the subscription was
//! established, and finally races incoming notifications against an exponential
//! backoff timer.
//!
//! The backoff is the safety net: a notification can be lost (Redis pub/sub is
//! fire-and-forget, and a result may have been written by a producer with
//! notifications disabled), so the waiter still re-reads periodically — just
//! with a rapidly growing interval instead of a fixed one.

use std::time::Duration;

use futures_util::StreamExt;
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::types::{Result, TaskMeta};

/// Shortest interval between reads while waiting.
const INITIAL_BACKOFF: Duration = Duration::from_millis(10);

impl RedisResultBackend {
    /// Wait for a task result with timeout
    ///
    /// Returns as soon as the task reaches a terminal state, or when `timeout`
    /// elapses.
    ///
    /// # Arguments
    /// * `task_id` - Task to wait for
    /// * `timeout` - Maximum time to wait
    /// * `poll_interval` - Upper bound for the backoff between reads. Reads
    ///   start 10 ms apart and double up to this ceiling, so a task that
    ///   finishes quickly is observed quickly even with a large interval.
    ///
    /// # Returns
    /// * `Ok(Some(meta))` - Task completed (terminal state)
    /// * `Ok(None)` - Timeout reached before completion
    /// * `Err(...)` - Backend error
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Wait up to 30 seconds for result
    /// match backend.wait_for_result(
    ///     task_id,
    ///     Duration::from_secs(30),
    ///     Duration::from_millis(500)
    /// ).await? {
    ///     Some(meta) => println!("Task completed: {:?}", meta.result),
    ///     None => println!("Task timed out"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_result(
        &mut self,
        task_id: Uuid,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<Option<TaskMeta>> {
        let start = std::time::Instant::now();

        // Subscribe *before* the first read: a result written in between would
        // otherwise be missed by both the read and the notification.
        let mut pubsub = self.subscribe_to_result(task_id).await;

        let ceiling = poll_interval.max(INITIAL_BACKOFF);
        let mut backoff = INITIAL_BACKOFF.min(ceiling);

        loop {
            // Always read through Redis: the in-memory cache holds terminal
            // results only, and a cached miss must not shortcut the read.
            if let Some(meta) = self.get_result_uncached(task_id).await? {
                if meta.is_terminal() {
                    return Ok(Some(meta));
                }
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Ok(None);
            }

            let wait = backoff.min(timeout - elapsed);

            let subscription_ended = match pubsub.as_mut() {
                Some(subscriber) => {
                    let mut stream = subscriber.on_message();
                    let mut ended = false;
                    tokio::select! {
                        biased;
                        message = stream.next() => {
                            // `None` means the pub/sub connection dropped;
                            // fall back to plain polling from here on.
                            ended = message.is_none();
                        }
                        _ = tokio::time::sleep(wait) => {}
                    }
                    ended
                }
                None => {
                    tokio::time::sleep(wait).await;
                    false
                }
            };

            if subscription_ended {
                tracing::debug!(
                    %task_id,
                    "Result notification subscription ended; falling back to polling"
                );
                pubsub = None;
            }

            backoff = backoff.saturating_mul(2).min(ceiling);
        }
    }

    /// Subscribe to a task's result-notification channel.
    ///
    /// Returns `None` when pub/sub is unavailable — the caller then relies on
    /// backoff polling alone, which is always correct, just less immediate.
    async fn subscribe_to_result(&self, task_id: Uuid) -> Option<redis::aio::PubSub> {
        let channel = self.notify_channel(task_id);

        match self.client.get_async_pubsub().await {
            Ok(mut pubsub) => match pubsub.subscribe(&channel).await {
                Ok(()) => Some(pubsub),
                Err(e) => {
                    tracing::debug!(%task_id, error = %e, "Result notification subscribe failed");
                    None
                }
            },
            Err(e) => {
                tracing::debug!(%task_id, error = %e, "Pub/sub connection unavailable");
                None
            }
        }
    }
}
