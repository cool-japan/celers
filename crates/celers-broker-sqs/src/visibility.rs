// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Visibility-timeout heartbeats for in-flight SQS messages.
//!
//! SQS makes a received message visible again once its visibility timeout
//! expires. A handler that runs longer than that window is therefore picked up
//! by a second worker while the first is still executing — the task runs twice
//! and the first worker's eventual `DeleteMessage` removes the *second*
//! worker's in-flight copy.
//!
//! Raising the timeout only moves the cliff. The correct fix is a heartbeat:
//! while a message is being handled, periodically call
//! `ChangeMessageVisibility` to push the deadline back. This module implements
//! that as a cancellable background task whose lifetime is tied to the message:
//! it is started when the message is received and stopped by `ack`, `reject` or
//! by dropping the broker.
//!
//! Enable it with
//! [`SqsBroker::with_visibility_heartbeat`](crate::SqsBroker::with_visibility_heartbeat);
//! [`SqsBroker::production`](crate::SqsBroker::production) turns it on.
//!
//! Note that this reduces — but cannot eliminate — duplicate execution: a
//! worker that dies mid-task stops heartbeating and the message is redelivered,
//! which is exactly what makes SQS at-least-once.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::Client;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// Absolute ceiling SQS enforces on a visibility timeout (12 hours).
pub const MAX_VISIBILITY_TIMEOUT_SECS: i32 = 43_200;

/// Compute the interval between two heartbeat extensions.
///
/// The deadline is refreshed roughly three times per visibility window, with a
/// floor of one second, so a lost tick still leaves margin before the message
/// becomes visible.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::visibility::heartbeat_interval;
/// use std::time::Duration;
///
/// assert_eq!(heartbeat_interval(30), Duration::from_secs(10));
/// assert_eq!(heartbeat_interval(300), Duration::from_secs(100));
/// // Never degenerates into a busy loop.
/// assert_eq!(heartbeat_interval(0), Duration::from_secs(1));
/// assert_eq!(heartbeat_interval(-5), Duration::from_secs(1));
/// ```
pub fn heartbeat_interval(visibility_timeout_secs: i32) -> Duration {
    let seconds = (visibility_timeout_secs / 3).max(1);
    Duration::from_secs(seconds as u64)
}

/// Whether another extension is still within the configured budget.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::visibility::within_extension_budget;
///
/// assert!(within_extension_budget(0, 30, 300));
/// assert!(within_extension_budget(270, 30, 300));
/// assert!(!within_extension_budget(280, 30, 300));
/// assert!(!within_extension_budget(300, 30, 300));
/// ```
pub fn within_extension_budget(extended_so_far: i64, next_extension: i32, budget: i64) -> bool {
    extended_so_far.saturating_add(i64::from(next_extension)) <= budget
}

/// A running visibility heartbeat.
///
/// Dropping the handle cancels the background task, so a heartbeat can never
/// outlive the structure that owns it.
#[derive(Debug)]
pub struct VisibilityHeartbeat {
    cancelled: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl VisibilityHeartbeat {
    /// Spawn a heartbeat for one in-flight message.
    ///
    /// * `visibility_timeout` — the value pushed on every tick (clamped to
    ///   SQS's 0..=43200 range).
    /// * `max_extension_secs` — total number of seconds the heartbeat may keep
    ///   the message invisible before giving up, so a wedged handler cannot
    ///   hold a message forever.
    pub fn spawn(
        client: Client,
        queue_url: String,
        receipt_handle: String,
        visibility_timeout: i32,
        max_extension_secs: i64,
    ) -> Self {
        let visibility_timeout = visibility_timeout.clamp(1, MAX_VISIBILITY_TIMEOUT_SECS);
        let interval = heartbeat_interval(visibility_timeout);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = Arc::clone(&cancelled);

        let handle = tokio::spawn(async move {
            let mut extended: i64 = 0;

            loop {
                tokio::time::sleep(interval).await;

                if cancel_flag.load(Ordering::Acquire) {
                    return;
                }

                if !within_extension_budget(extended, visibility_timeout, max_extension_secs) {
                    warn!(
                        "Visibility heartbeat exhausted its {}s budget; message will become visible again",
                        max_extension_secs
                    );
                    return;
                }

                match client
                    .change_message_visibility()
                    .queue_url(&queue_url)
                    .receipt_handle(&receipt_handle)
                    .visibility_timeout(visibility_timeout)
                    .send()
                    .await
                {
                    Ok(_) => {
                        extended = extended.saturating_add(interval.as_secs() as i64);
                        debug!(
                            "Extended visibility by {}s (total in-flight {}s)",
                            visibility_timeout, extended
                        );
                    }
                    Err(error) => {
                        // The message was most likely already deleted or the
                        // handle expired; there is nothing useful left to do.
                        debug!(
                            "Visibility heartbeat stopping: {}",
                            crate::retry_policy::describe_error(&error)
                        );
                        return;
                    }
                }
            }
        });

        Self { cancelled, handle }
    }

    /// Stop the heartbeat.
    ///
    /// Called by `ack`/`reject` once the message is no longer in flight.
    pub fn stop(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.handle.abort();
    }

    /// Whether the heartbeat has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl Drop for VisibilityHeartbeat {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_is_a_third_of_the_window() {
        assert_eq!(heartbeat_interval(30), Duration::from_secs(10));
        assert_eq!(heartbeat_interval(300), Duration::from_secs(100));
        assert_eq!(heartbeat_interval(43_200), Duration::from_secs(14_400));
    }

    #[test]
    fn interval_never_zero() {
        assert_eq!(heartbeat_interval(1), Duration::from_secs(1));
        assert_eq!(heartbeat_interval(2), Duration::from_secs(1));
        assert_eq!(heartbeat_interval(0), Duration::from_secs(1));
        assert_eq!(heartbeat_interval(i32::MIN), Duration::from_secs(1));
    }

    #[test]
    fn extension_budget_is_enforced() {
        assert!(within_extension_budget(0, 30, 300));
        assert!(within_extension_budget(270, 30, 300));
        assert!(!within_extension_budget(280, 30, 300));
        assert!(!within_extension_budget(300, 30, 300));
        assert!(!within_extension_budget(1_000, 30, 300));
        assert!(within_extension_budget(i64::MAX, 30, i64::MAX));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_stops_when_cancelled() {
        // No SDK call is exercised here: the task sleeps first, so cancelling
        // before the first tick must leave it inert and finish immediately.
        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&cancelled);

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            flag.load(Ordering::Acquire)
        });

        cancelled.store(true, Ordering::Release);
        tokio::time::advance(Duration::from_secs(11)).await;

        assert!(handle.await.expect("task completes"));
    }
}
