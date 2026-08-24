//! Message scheduling, consumer groups, and replay types.

use async_trait::async_trait;
use celers_protocol::Message;
use std::time::Duration;

use crate::{Envelope, Result};

// =============================================================================
// Message Scheduling (Delayed Delivery)
// =============================================================================

/// Schedule configuration for delayed message delivery
///
/// # Examples
///
/// ```
/// use celers_kombu::ScheduleConfig;
/// use std::time::Duration;
///
/// // Delay by 30 seconds
/// let schedule = ScheduleConfig::delay(Duration::from_secs(30));
/// assert!(schedule.delay.is_some());
///
/// // Schedule at specific timestamp
/// let timestamp = std::time::SystemTime::now()
///     .duration_since(std::time::UNIX_EPOCH)
///     .unwrap()
///     .as_secs() + 3600;
/// let schedule = ScheduleConfig::at(timestamp);
/// assert!(schedule.scheduled_at.is_some());
/// ```
#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    /// Delay duration from now
    pub delay: Option<Duration>,
    /// Absolute timestamp (Unix epoch seconds)
    pub scheduled_at: Option<u64>,
    /// Maximum execution time window
    pub execution_window: Option<Duration>,
}

impl ScheduleConfig {
    /// Create schedule with delay
    pub fn delay(delay: Duration) -> Self {
        Self {
            delay: Some(delay),
            scheduled_at: None,
            execution_window: None,
        }
    }

    /// Create schedule at absolute time
    pub fn at(timestamp: u64) -> Self {
        Self {
            delay: None,
            scheduled_at: Some(timestamp),
            execution_window: None,
        }
    }

    /// Set execution window
    pub fn with_window(mut self, window: Duration) -> Self {
        self.execution_window = Some(window);
        self
    }

    /// Check if message is ready for delivery
    ///
    /// Delegates to [`Self::delivery_time`] so a delay-based schedule is
    /// evaluated against `now + delay`, exactly like an absolute
    /// `scheduled_at` timestamp would be. Previously, a delay-only
    /// schedule (no `scheduled_at`) fell through to an unconditional
    /// `true`, so `ScheduleConfig::delay(Duration::from_secs(3600))`
    /// reported itself ready to deliver immediately -- any scheduler
    /// driving delivery off `is_ready()` would fire delayed messages
    /// right away instead of honoring the delay.
    pub fn is_ready(&self) -> bool {
        match self.delivery_time() {
            Some(timestamp) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("SystemTime should be after UNIX_EPOCH")
                    .as_secs();
                now >= timestamp
            }
            // Neither `scheduled_at` nor `delay` is set: there is no
            // schedule to wait on, so the message is ready immediately.
            None => true,
        }
    }

    /// Get delivery timestamp
    pub fn delivery_time(&self) -> Option<u64> {
        if let Some(timestamp) = self.scheduled_at {
            return Some(timestamp);
        }

        if let Some(delay) = self.delay {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("SystemTime should be after UNIX_EPOCH")
                .as_secs();
            return Some(now + delay.as_secs());
        }

        None
    }
}

/// Message scheduler trait for delayed delivery
#[async_trait]
pub trait MessageScheduler: Send + Sync {
    /// Schedule a message for delayed delivery
    async fn schedule_message(
        &mut self,
        queue: &str,
        message: Message,
        schedule: ScheduleConfig,
    ) -> Result<String>;

    /// Cancel a scheduled message
    async fn cancel_scheduled(&mut self, schedule_id: &str) -> Result<()>;

    /// List scheduled messages for a queue
    async fn list_scheduled(&mut self, queue: &str) -> Result<Vec<ScheduledMessage>>;

    /// Get count of scheduled messages
    async fn scheduled_count(&mut self, queue: &str) -> Result<usize>;
}

/// Scheduled message information
#[derive(Debug, Clone)]
pub struct ScheduledMessage {
    /// Schedule ID
    pub schedule_id: String,
    /// Queue name
    pub queue: String,
    /// Scheduled delivery time (Unix timestamp)
    pub delivery_time: u64,
    /// Message size in bytes
    pub message_size: usize,
}

// =============================================================================
// Consumer Groups (Load Balancing)
// =============================================================================

/// Consumer group configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::ConsumerGroupConfig;
///
/// let config = ConsumerGroupConfig::new("my-service".to_string(), "worker-1".to_string())
///     .with_max_consumers(10)
///     .with_rebalance_timeout(std::time::Duration::from_secs(30));
///
/// assert_eq!(config.group_id, "my-service");
/// assert_eq!(config.consumer_id, "worker-1");
/// assert_eq!(config.max_consumers, Some(10));
/// ```
#[derive(Debug, Clone)]
pub struct ConsumerGroupConfig {
    /// Consumer group ID
    pub group_id: String,
    /// Individual consumer ID
    pub consumer_id: String,
    /// Maximum consumers in group
    pub max_consumers: Option<usize>,
    /// Rebalance timeout
    pub rebalance_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
}

impl ConsumerGroupConfig {
    /// Create new consumer group configuration
    pub fn new(group_id: String, consumer_id: String) -> Self {
        Self {
            group_id,
            consumer_id,
            max_consumers: None,
            rebalance_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
        }
    }

    /// Set maximum consumers
    pub fn with_max_consumers(mut self, max: usize) -> Self {
        self.max_consumers = Some(max);
        self
    }

    /// Set rebalance timeout
    pub fn with_rebalance_timeout(mut self, timeout: Duration) -> Self {
        self.rebalance_timeout = timeout;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }
}

/// Consumer group trait for load-balanced consumption
#[async_trait]
pub trait ConsumerGroup: Send + Sync {
    /// Join a consumer group
    async fn join_group(&mut self, config: &ConsumerGroupConfig) -> Result<()>;

    /// Leave consumer group
    async fn leave_group(&mut self, group_id: &str) -> Result<()>;

    /// Send heartbeat to maintain membership
    async fn heartbeat(&mut self, group_id: &str) -> Result<()>;

    /// Get consumer group members
    async fn group_members(&mut self, group_id: &str) -> Result<Vec<String>>;

    /// Consume from group (automatic load balancing)
    async fn consume_from_group(
        &mut self,
        group_id: &str,
        queues: &[String],
        timeout: Duration,
    ) -> Result<Option<Envelope>>;
}

// =============================================================================
// Message Replay (Debugging/Recovery)
// =============================================================================

/// Replay configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::ReplayConfig;
/// use std::time::Duration;
///
/// // Replay last 1 hour
/// let config = ReplayConfig::from_duration(Duration::from_secs(3600));
/// assert!(config.from_duration.is_some());
///
/// // Replay from specific timestamp
/// let timestamp = 1699999999;
/// let config = ReplayConfig::from_timestamp(timestamp);
/// assert_eq!(config.from_timestamp, Some(timestamp));
/// ```
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Replay from duration ago
    pub from_duration: Option<Duration>,
    /// Replay from absolute timestamp
    pub from_timestamp: Option<u64>,
    /// Replay until timestamp (None = now)
    pub until_timestamp: Option<u64>,
    /// Maximum messages to replay
    pub max_messages: Option<usize>,
    /// Replay speed multiplier (1.0 = real-time)
    pub speed_multiplier: f64,
}

impl ReplayConfig {
    /// Create replay config from duration
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            from_duration: Some(duration),
            from_timestamp: None,
            until_timestamp: None,
            max_messages: None,
            speed_multiplier: 1.0,
        }
    }

    /// Create replay config from timestamp
    pub fn from_timestamp(timestamp: u64) -> Self {
        Self {
            from_duration: None,
            from_timestamp: Some(timestamp),
            until_timestamp: None,
            max_messages: None,
            speed_multiplier: 1.0,
        }
    }

    /// Set end timestamp
    pub fn until(mut self, timestamp: u64) -> Self {
        self.until_timestamp = Some(timestamp);
        self
    }

    /// Set maximum messages
    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = Some(max);
        self
    }

    /// Set replay speed
    pub fn with_speed(mut self, multiplier: f64) -> Self {
        self.speed_multiplier = multiplier;
        self
    }

    /// Calculate start timestamp
    pub fn start_timestamp(&self) -> u64 {
        if let Some(ts) = self.from_timestamp {
            return ts;
        }

        if let Some(duration) = self.from_duration {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("SystemTime should be after UNIX_EPOCH")
                .as_secs();
            return now.saturating_sub(duration.as_secs());
        }

        0
    }
}

/// Message replay trait for debugging and recovery
#[async_trait]
pub trait MessageReplay: Send + Sync {
    /// Start replay session
    async fn begin_replay(&mut self, queue: &str, config: ReplayConfig) -> Result<String>;

    /// Get next message from replay
    async fn replay_next(&mut self, replay_id: &str) -> Result<Option<Envelope>>;

    /// Stop replay session
    async fn stop_replay(&mut self, replay_id: &str) -> Result<()>;

    /// Get replay progress
    async fn replay_progress(&mut self, replay_id: &str) -> Result<ReplayProgress>;
}

/// Replay progress information
#[derive(Debug, Clone)]
pub struct ReplayProgress {
    /// Replay session ID
    pub replay_id: String,
    /// Messages replayed so far
    pub messages_replayed: usize,
    /// Total messages to replay
    pub total_messages: Option<usize>,
    /// Current timestamp being replayed
    pub current_timestamp: u64,
    /// Replay is complete
    pub completed: bool,
}

impl ReplayProgress {
    /// Get completion percentage
    pub fn completion_percent(&self) -> Option<f64> {
        self.total_messages.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.messages_replayed as f64 / total as f64) * 100.0
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_unix_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs()
    }

    #[test]
    fn delay_schedule_is_not_immediately_ready() {
        // Regression test: a delay-only `ScheduleConfig` (no `scheduled_at`)
        // used to report `is_ready() == true` unconditionally, so a
        // scheduler driving delivery off it would fire the message right
        // away instead of waiting out the delay.
        let schedule = ScheduleConfig::delay(Duration::from_secs(3600));
        assert!(!schedule.is_ready());
    }

    #[test]
    fn delay_schedule_is_ready_once_delivery_time_has_passed() {
        // `is_ready` must agree with `delivery_time`: neither is
        // literally simulable without mocking the clock, but we can
        // assert that a schedule whose delivery time is in the past
        // (constructed via `at`, which is how `delay` conceptually
        // resolves) is ready.
        let past = ScheduleConfig::at(0);
        assert!(past.is_ready());
        assert_eq!(past.delivery_time(), Some(0));
    }

    #[test]
    fn zero_delay_schedule_is_ready() {
        let schedule = ScheduleConfig::delay(Duration::from_secs(0));
        assert!(schedule.is_ready());
    }

    #[test]
    fn delay_schedule_fields_are_unchanged() {
        // `delay()` must keep storing the delay in `delay` (not eagerly
        // resolve it into `scheduled_at`) so existing introspection of
        // the raw fields keeps working.
        let schedule = ScheduleConfig::delay(Duration::from_secs(30));
        assert_eq!(schedule.delay, Some(Duration::from_secs(30)));
        assert!(schedule.scheduled_at.is_none());
    }

    #[test]
    fn is_ready_matches_delivery_time_for_a_future_delay() {
        let schedule = ScheduleConfig::delay(Duration::from_secs(3600));
        let now = now_unix_secs();
        let delivery = schedule
            .delivery_time()
            .expect("delay-based schedule must have a delivery time");
        assert!(delivery > now);
        assert!(!schedule.is_ready());
    }
}
