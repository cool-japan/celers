//! Message replay utilities for DLQ management and disaster recovery
//!
//! This module provides utilities for replaying messages from Dead Letter Queues (DLQ)
//! or for disaster recovery scenarios where messages need to be reprocessed.
//!
//! # Features
//!
//! - Batch message replay with rate limiting
//! - Selective replay based on filters (time range, task name, error pattern)
//! - Progress tracking and reporting
//! - Automatic retry with backoff for failed replays
//! - Message transformation during replay
//! - DLQ-to-main-queue migration
//!
//! # Example
//!
//! ```ignore
//! use celers_broker_sqs::replay::{ReplayManager, ReplayConfig, ReplayFilter};
//! use celers_broker_sqs::{SqsBroker, DlqConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut broker = SqsBroker::new("tasks")
//!     .await?
//!     .with_dlq(DlqConfig::new("arn:aws:sqs:us-east-1:123456789012:tasks-dlq", 5));
//!
//! // Replay payment tasks that failed in the last hour, at most 500 of them.
//! let config = ReplayConfig::new()
//!     .with_batch_size(10)
//!     .with_rate_limit(100)   // 100 messages per second max
//!     .with_retry_failed(true);
//!
//! let filter = ReplayFilter::new()
//!     .with_time_range_hours(1)
//!     .with_task_pattern("tasks.payment.*")
//!     .with_max_messages(500);
//!
//! let mut manager = ReplayManager::new(config);
//!
//! // Rehearse first: nothing is published and nothing leaves the DLQ.
//! let planned = manager.replay_from_dlq(&mut broker, &filter, true).await?;
//! println!("{} messages would be replayed", planned.successful);
//!
//! // Then do it for real.
//! let result = manager.replay_from_dlq(&mut broker, &filter, false).await?;
//! println!("replayed {}/{} in {:?}", result.successful, result.total, result.duration);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use celers_kombu::{BrokerError, Envelope, Producer, Result};
use tracing::{debug, info, warn};

use crate::broker_core::SqsBroker;

/// Configuration for message replay operations
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Number of messages to replay per batch
    pub batch_size: usize,
    /// Maximum replay rate (messages per second), 0 = unlimited
    pub rate_limit: usize,
    /// Whether to retry failed replays
    pub retry_failed: bool,
    /// Maximum retry attempts for failed replays
    pub max_retries: usize,
    /// Delay between retry attempts
    pub retry_delay: Duration,
    /// Whether to preserve message attributes
    pub preserve_attributes: bool,
    /// Whether to track replay progress
    pub track_progress: bool,
}

impl ReplayConfig {
    /// Create a new replay configuration with defaults
    pub fn new() -> Self {
        Self {
            batch_size: 10,
            rate_limit: 100,
            retry_failed: true,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            preserve_attributes: true,
            track_progress: true,
        }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.clamp(1, 10);
        self
    }

    /// Set rate limit (messages per second)
    pub fn with_rate_limit(mut self, limit: usize) -> Self {
        self.rate_limit = limit;
        self
    }

    /// Set whether to retry failed replays
    pub fn with_retry_failed(mut self, retry: bool) -> Self {
        self.retry_failed = retry;
        self
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set retry delay
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Set whether to preserve message attributes
    pub fn with_preserve_attributes(mut self, preserve: bool) -> Self {
        self.preserve_attributes = preserve;
        self
    }

    /// Set whether to track progress
    pub fn with_track_progress(mut self, track: bool) -> Self {
        self.track_progress = track;
        self
    }
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter for selective message replay
#[derive(Debug, Clone, Default)]
pub struct ReplayFilter {
    /// Minimum timestamp (Unix epoch seconds)
    pub min_timestamp: Option<u64>,
    /// Maximum timestamp (Unix epoch seconds)
    pub max_timestamp: Option<u64>,
    /// Task name pattern (glob-style)
    pub task_pattern: Option<String>,
    /// Error message pattern
    pub error_pattern: Option<String>,
    /// Minimum failure count
    pub min_failure_count: Option<u32>,
    /// Maximum messages to replay (0 = unlimited)
    pub max_messages: usize,
}

impl ReplayFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Set time range (last N hours)
    ///
    /// Saturating throughout: a clock before the Unix epoch, or a range longer
    /// than the epoch itself, yields `0` rather than panicking or wrapping.
    pub fn with_time_range_hours(mut self, hours: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        self.min_timestamp = Some(now.saturating_sub(hours.saturating_mul(3600)));
        self
    }

    /// Set time range (custom timestamps)
    pub fn with_time_range(mut self, min: u64, max: u64) -> Self {
        self.min_timestamp = Some(min);
        self.max_timestamp = Some(max);
        self
    }

    /// Set task name pattern
    pub fn with_task_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.task_pattern = Some(pattern.into());
        self
    }

    /// Set error message pattern
    pub fn with_error_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.error_pattern = Some(pattern.into());
        self
    }

    /// Set minimum failure count
    pub fn with_min_failure_count(mut self, count: u32) -> Self {
        self.min_failure_count = Some(count);
        self
    }

    /// Set maximum messages to replay
    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = max;
        self
    }

    /// Check if a message matches this filter
    pub fn matches(&self, message: &ReplayableMessage) -> bool {
        // Check timestamp range
        if let Some(min) = self.min_timestamp {
            if message.timestamp < min {
                return false;
            }
        }
        if let Some(max) = self.max_timestamp {
            if message.timestamp > max {
                return false;
            }
        }

        // Check task pattern
        if let Some(ref pattern) = self.task_pattern {
            if !glob_match(pattern, &message.task_name) {
                return false;
            }
        }

        // Check error pattern
        if let Some(ref pattern) = self.error_pattern {
            if let Some(ref error) = message.error_message {
                if !error.contains(pattern) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check failure count
        if let Some(min_count) = self.min_failure_count {
            if message.failure_count < min_count {
                return false;
            }
        }

        true
    }
}

/// A message that can be replayed
#[derive(Debug, Clone)]
pub struct ReplayableMessage {
    /// Message ID
    pub message_id: String,
    /// Message body (JSON)
    pub body: String,
    /// Task name
    pub task_name: String,
    /// Message attributes
    pub attributes: HashMap<String, String>,
    /// Timestamp (Unix epoch seconds)
    pub timestamp: u64,
    /// Error message (if from DLQ)
    pub error_message: Option<String>,
    /// Failure count
    pub failure_count: u32,
}

/// Result of a replay operation
#[derive(Debug, Clone)]
pub struct ReplayResult {
    /// Total messages attempted
    pub total: usize,
    /// Successfully replayed messages
    pub successful: usize,
    /// Failed replays
    pub failed: usize,
    /// Skipped messages (filtered out)
    pub skipped: usize,
    /// Duration of replay operation
    pub duration: Duration,
    /// Average throughput (messages/sec)
    pub throughput: f64,
    /// Failed message IDs
    pub failed_message_ids: Vec<String>,
}

/// Progress information during replay
#[derive(Debug, Clone)]
pub struct ReplayProgress {
    /// Messages replayed so far
    pub replayed: usize,
    /// Total messages to replay
    pub total: usize,
    /// Successful replays
    pub successful: usize,
    /// Failed replays
    pub failed: usize,
    /// Elapsed time
    pub elapsed: Duration,
    /// Estimated time remaining
    pub estimated_remaining: Option<Duration>,
}

impl ReplayProgress {
    /// Calculate progress percentage
    pub fn percentage(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.replayed as f64 / self.total as f64) * 100.0
        }
    }

    /// Calculate current throughput (messages/sec)
    pub fn throughput(&self) -> f64 {
        if self.elapsed.as_secs_f64() == 0.0 {
            0.0
        } else {
            self.replayed as f64 / self.elapsed.as_secs_f64()
        }
    }
}

/// Message replay manager
///
/// Drives a DLQ-to-main-queue redrive: receive from the DLQ, filter, publish to
/// the main queue, delete from the DLQ, respecting a rate limit and reporting
/// progress.
pub struct ReplayManager {
    /// Replay configuration
    pub config: ReplayConfig,
    progress: Option<ReplayProgress>,
    start_time: Option<Instant>,
}

/// Hard ceiling on receive rounds, so a filter that rejects everything cannot
/// spin forever against a queue whose messages keep becoming visible again.
const MAX_REPLAY_ROUNDS: usize = 10_000;

impl ReplayManager {
    /// Create a new replay manager
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            progress: None,
            start_time: None,
        }
    }

    /// Get current replay progress
    pub fn progress(&self) -> Option<&ReplayProgress> {
        self.progress.as_ref()
    }

    /// Reset progress tracking
    pub fn reset_progress(&mut self) {
        self.progress = None;
        self.start_time = None;
    }

    /// Replay messages from the broker's Dead Letter Queue to its main queue.
    ///
    /// For every batch received from the DLQ:
    ///
    /// 1. each message is converted to a [`ReplayableMessage`] (its age and
    ///    failure count come from the SQS system attributes the broker
    ///    records on receive);
    /// 2. messages rejected by `filter` are skipped and left in the DLQ — the
    ///    run resets their visibility timeout to zero on the way out, so they
    ///    are available again the moment it returns rather than after the
    ///    queue's visibility timeout elapses;
    /// 3. matching messages are published to the main queue and only then
    ///    deleted **from the DLQ** (never from the main queue: an SQS receipt
    ///    handle is valid only against the queue it came from);
    /// 4. failures are retried according to
    ///    [`ReplayConfig::retry_failed`] / `max_retries` / `retry_delay`;
    /// 5. the configured rate limit is applied with an async sleep.
    ///
    /// With `dry_run` nothing is published and nothing is deleted: the messages
    /// that *would* be replayed are counted and returned to the DLQ — really
    /// returned, by resetting their visibility timeout once the run is over, so
    /// that the real replay a dry run is usually followed by still finds them.
    /// (Reading a message from SQS makes it invisible even when nothing is
    /// deleted; without the reset, `dry_run = true` immediately followed by
    /// `dry_run = false` moved nothing and reported success.)
    ///
    /// The loop stops when the DLQ returns an empty batch, when
    /// [`ReplayFilter::max_messages`] is reached, or at a hard round ceiling.
    ///
    /// # Errors
    ///
    /// Returns an error when the broker has no DLQ configured, or when the DLQ
    /// cannot be read at all. Individual publish failures are counted in
    /// [`ReplayResult::failed`] rather than aborting the run.
    pub async fn replay_from_dlq(
        &mut self,
        broker: &mut SqsBroker,
        filter: &ReplayFilter,
        dry_run: bool,
    ) -> Result<ReplayResult> {
        let started = Instant::now();
        self.start_time = Some(started);
        self.progress = None;

        let dlq_name = broker.dlq_queue_name()?;
        let main_queue = broker.main_queue_name();
        let batch_size = self.config.batch_size.clamp(1, 10) as i32;
        let limit = filter.max_messages;

        let mut total = 0usize;
        let mut successful = 0usize;
        let mut failed = 0usize;
        let mut skipped = 0usize;
        let mut failed_message_ids: Vec<String> = Vec::new();
        let mut sent_since_start = 0usize;

        // Delivery tags of the messages this run received from the DLQ but
        // deliberately left there: everything a dry run inspected, and
        // everything `filter` rejected.
        //
        // Reading a message from SQS starts its visibility timeout even when
        // nothing is deleted, so "left in the DLQ" is only true again once
        // that timeout expires — up to 12 hours, and 30 seconds by default.
        // Until then the message is invisible, which made the obvious
        // operator workflow silently do nothing:
        //
        //   let planned = replay_from_dlq(.., dry_run = true).await?;  // 1
        //   let done    = replay_from_dlq(.., dry_run = false).await?; // 0 (!)
        //
        // The real run received an empty DLQ and reported success having moved
        // nothing. Resetting the visibility timeout to zero puts each message
        // back immediately instead — but only *after* the receive loop has
        // finished, never inside it: releasing during the loop would make the
        // very next round receive the same message again and count it twice.
        let mut to_release: Vec<String> = Vec::new();

        for round in 0..MAX_REPLAY_ROUNDS {
            if limit > 0 && total >= limit {
                break;
            }

            let remaining = if limit > 0 {
                (limit - total).min(batch_size as usize) as i32
            } else {
                batch_size
            };

            let envelopes = broker.get_dlq_messages(remaining).await?;
            if envelopes.is_empty() {
                debug!("Replay finished after {} round(s): DLQ is empty", round);
                break;
            }

            for envelope in envelopes {
                total += 1;

                let replayable = replayable_from_envelope(broker, &envelope);
                if !filter.matches(&replayable) {
                    skipped += 1;
                    to_release.push(envelope.delivery_tag.clone());
                    continue;
                }

                if dry_run {
                    successful += 1;
                    to_release.push(envelope.delivery_tag.clone());
                    continue;
                }

                match self
                    .replay_one(broker, &main_queue, &dlq_name, &envelope)
                    .await
                {
                    Ok(()) => {
                        successful += 1;
                        sent_since_start += 1;
                    }
                    Err(error) => {
                        warn!(
                            "Failed to replay message {}: {}",
                            replayable.message_id, error
                        );
                        failed += 1;
                        failed_message_ids.push(replayable.message_id.clone());
                    }
                }
            }

            self.update_progress(total, total.max(limit), successful, failed);
            self.apply_rate_limit(sent_since_start, started.elapsed())
                .await;
        }

        // Put back everything this run only looked at. A failure here is
        // logged, not returned: the messages still become visible again when
        // their visibility timeout expires, and a dry run that inspected the
        // DLQ correctly must not be reported as a failed run because the
        // release call did not land.
        for delivery_tag in &to_release {
            // `reject_on(.., requeue = true)` rather than a bare
            // `extend_visibility(tag, 0)`: it stops any visibility heartbeat
            // this receive started first (see
            // `SqsBroker::with_visibility_heartbeat`), which would otherwise
            // extend the timeout straight back out from under the reset. The
            // queue is taken from the delivery tag either way, so the message
            // is released against the DLQ it came from, never the main queue.
            if let Err(error) = broker.reject_on(&dlq_name, delivery_tag, true).await {
                warn!(
                    "Failed to return an inspected message to DLQ {}: {} \
                     (it becomes visible again when its visibility timeout expires)",
                    dlq_name, error
                );
            }
        }

        let duration = started.elapsed();
        let throughput = if duration.as_secs_f64() > 0.0 {
            successful as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        info!(
            "Replay from DLQ {} -> {} complete: {} replayed, {} failed, {} skipped{}",
            dlq_name,
            main_queue,
            successful,
            failed,
            skipped,
            if dry_run { " (dry run)" } else { "" }
        );

        Ok(ReplayResult {
            total,
            successful,
            failed,
            skipped,
            duration,
            throughput,
            failed_message_ids,
        })
    }

    /// Publish one DLQ message to the main queue and delete it from the DLQ.
    async fn replay_one(
        &self,
        broker: &mut SqsBroker,
        main_queue: &str,
        dlq_name: &str,
        envelope: &Envelope,
    ) -> Result<()> {
        let attempts = if self.config.retry_failed {
            self.config.max_retries.saturating_add(1).max(1)
        } else {
            1
        };

        let mut last_error = BrokerError::OperationFailed("replay never attempted".to_string());

        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_delay).await;
            }

            match broker.publish(main_queue, envelope.message.clone()).await {
                Ok(()) => {
                    // Delete from the DLQ, addressed explicitly to the DLQ.
                    return broker.ack_on(dlq_name, &envelope.delivery_tag).await;
                }
                Err(error) => last_error = error,
            }
        }

        Err(last_error)
    }

    /// Apply rate limiting
    ///
    /// Uses an async sleep: the previous `std::thread::sleep` would have
    /// blocked the whole tokio worker thread.
    async fn apply_rate_limit(&self, messages_sent: usize, elapsed: Duration) {
        if self.config.rate_limit == 0 || messages_sent == 0 {
            return;
        }

        let target = replay_target_duration(messages_sent, self.config.rate_limit);
        if elapsed < target {
            tokio::time::sleep(target - elapsed).await;
        }
    }

    /// Update progress tracking
    fn update_progress(&mut self, replayed: usize, total: usize, successful: usize, failed: usize) {
        if !self.config.track_progress {
            return;
        }

        let start = self.start_time.unwrap_or_else(Instant::now);
        let elapsed = start.elapsed();

        let estimated_remaining = estimate_remaining(replayed, total, elapsed);

        self.progress = Some(ReplayProgress {
            replayed,
            total,
            successful,
            failed,
            elapsed,
            estimated_remaining,
        });
    }
}

/// Time a given number of sends should have taken at a target rate.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::replay::replay_target_duration;
/// use std::time::Duration;
///
/// // 100 messages at 100/s should take one second.
/// assert_eq!(replay_target_duration(100, 100), Duration::from_secs(1));
/// // An unlimited rate never asks anyone to wait.
/// assert_eq!(replay_target_duration(100, 0), Duration::ZERO);
/// ```
pub fn replay_target_duration(messages_sent: usize, rate_limit: usize) -> Duration {
    if rate_limit == 0 {
        return Duration::ZERO;
    }

    Duration::from_secs_f64(messages_sent as f64 / rate_limit as f64)
}

/// Estimate the remaining replay time from the progress made so far.
///
/// Returns `None` when there is not enough information (nothing replayed yet,
/// no elapsed time, or the total is not larger than what is already done).
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::replay::estimate_remaining;
/// use std::time::Duration;
///
/// // 50 of 100 in 10s -> 10s left.
/// assert_eq!(
///     estimate_remaining(50, 100, Duration::from_secs(10)),
///     Some(Duration::from_secs(10))
/// );
/// // Nothing done yet: unknown.
/// assert_eq!(estimate_remaining(0, 100, Duration::from_secs(10)), None);
/// // More done than expected: nothing left, never a negative estimate.
/// assert_eq!(
///     estimate_remaining(120, 100, Duration::from_secs(10)),
///     Some(Duration::ZERO)
/// );
/// ```
pub fn estimate_remaining(replayed: usize, total: usize, elapsed: Duration) -> Option<Duration> {
    if replayed == 0 || elapsed.as_secs_f64() <= 0.0 {
        return None;
    }

    let remaining = total.saturating_sub(replayed);
    if remaining == 0 {
        return Some(Duration::ZERO);
    }

    let rate = replayed as f64 / elapsed.as_secs_f64();
    if rate <= 0.0 {
        return None;
    }

    Some(Duration::from_secs_f64(remaining as f64 / rate))
}

/// Build a [`ReplayableMessage`] from a DLQ envelope.
///
/// The age and failure count come from the SQS system attributes the broker
/// recorded when it received the message; without them every time-range filter
/// would silently match nothing.
fn replayable_from_envelope(broker: &SqsBroker, envelope: &Envelope) -> ReplayableMessage {
    let metadata = broker.receipt_metadata(&envelope.delivery_tag);

    let timestamp = metadata
        .and_then(|m| m.sent_timestamp_secs())
        .or_else(|| {
            envelope
                .message
                .headers
                .created_at
                .map(|created| created.timestamp().max(0) as u64)
        })
        .unwrap_or(0);

    let failure_count = metadata
        .map(|m| m.receive_count)
        .or(envelope.message.headers.retries)
        .unwrap_or(0);

    let message_id = metadata
        .and_then(|m| m.message_id.clone())
        .unwrap_or_else(|| envelope.message.headers.id.to_string());

    ReplayableMessage {
        message_id,
        body: String::from_utf8_lossy(&envelope.message.body).into_owned(),
        task_name: envelope.message.headers.task.clone(),
        attributes: HashMap::new(),
        timestamp,
        error_message: None,
        failure_count,
    }
}

/// Simple glob pattern matching (supports * wildcard)
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
    }

    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            // First part - must match at start
            if !text.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Last part - must match at end
            return text[pos..].ends_with(part);
        } else {
            // Middle part - must be found somewhere
            if let Some(found_pos) = text[pos..].find(part) {
                pos += found_pos + part.len();
            } else {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_config_default() {
        let config = ReplayConfig::new();
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.rate_limit, 100);
        assert!(config.retry_failed);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_replay_config_builder() {
        let config = ReplayConfig::new()
            .with_batch_size(5)
            .with_rate_limit(50)
            .with_retry_failed(false)
            .with_max_retries(2);

        assert_eq!(config.batch_size, 5);
        assert_eq!(config.rate_limit, 50);
        assert!(!config.retry_failed);
        assert_eq!(config.max_retries, 2);
    }

    #[test]
    fn test_replay_filter_matches_task_pattern() {
        let filter = ReplayFilter::new().with_task_pattern("tasks.payment.*");

        let message = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.payment.process".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: None,
            failure_count: 1,
        };

        assert!(filter.matches(&message));
    }

    #[test]
    fn test_replay_filter_matches_time_range() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();

        let filter = ReplayFilter::new().with_time_range(now - 3600, now);

        let recent_message = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 1800, // 30 minutes ago
            error_message: None,
            failure_count: 1,
        };

        let old_message = ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 7200, // 2 hours ago
            error_message: None,
            failure_count: 1,
        };

        assert!(filter.matches(&recent_message));
        assert!(!filter.matches(&old_message));
    }

    #[test]
    fn test_replay_filter_matches_failure_count() {
        let filter = ReplayFilter::new().with_min_failure_count(3);

        let high_failure = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: None,
            failure_count: 5,
        };

        let low_failure = ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: None,
            failure_count: 1,
        };

        assert!(filter.matches(&high_failure));
        assert!(!filter.matches(&low_failure));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("tasks.*", "tasks.process"));
        assert!(glob_match("tasks.payment.*", "tasks.payment.process"));
        assert!(!glob_match("tasks.payment.*", "tasks.email.send"));
        assert!(glob_match("tasks.*.process", "tasks.payment.process"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "not_exact"));
    }

    #[test]
    fn test_replay_progress_percentage() {
        let progress = ReplayProgress {
            replayed: 50,
            total: 100,
            successful: 45,
            failed: 5,
            elapsed: Duration::from_secs(10),
            estimated_remaining: None,
        };

        assert_eq!(progress.percentage(), 50.0);
        assert_eq!(progress.throughput(), 5.0);
    }

    #[test]
    fn test_replay_progress_throughput() {
        let progress = ReplayProgress {
            replayed: 100,
            total: 200,
            successful: 95,
            failed: 5,
            elapsed: Duration::from_secs(10),
            estimated_remaining: None,
        };

        assert_eq!(progress.throughput(), 10.0);
    }

    #[test]
    fn test_replay_manager_progress_tracking() {
        let config = ReplayConfig::new().with_track_progress(true);
        let mut manager = ReplayManager::new(config);

        manager.start_time = Some(Instant::now());
        manager.update_progress(50, 100, 45, 5);

        let progress = manager.progress().unwrap();
        assert_eq!(progress.replayed, 50);
        assert_eq!(progress.total, 100);
        assert_eq!(progress.successful, 45);
        assert_eq!(progress.failed, 5);
        assert!(progress.estimated_remaining.is_some());
    }

    #[test]
    fn test_batch_size_clamping() {
        let config1 = ReplayConfig::new().with_batch_size(0);
        assert_eq!(config1.batch_size, 1);

        let config2 = ReplayConfig::new().with_batch_size(20);
        assert_eq!(config2.batch_size, 10);

        let config3 = ReplayConfig::new().with_batch_size(5);
        assert_eq!(config3.batch_size, 5);
    }

    #[test]
    fn test_replay_filter_error_pattern() {
        let filter = ReplayFilter::new().with_error_pattern("timeout");

        let matching = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: Some("Connection timeout occurred".to_string()),
            failure_count: 1,
        };

        let non_matching = ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: Some("Validation failed".to_string()),
            failure_count: 1,
        };

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }
}
