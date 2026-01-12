//! Message scheduling for delayed delivery
//!
//! This module provides scheduling capabilities for delayed message delivery,
//! allowing messages to be published at a specific time in the future.
//!
//! # Use Cases
//!
//! - **Delayed Retries**: Schedule retries with exponential backoff
//! - **Scheduled Tasks**: Execute tasks at specific times
//! - **Rate Smoothing**: Spread message delivery over time
//! - **Time-based Workflows**: Implement time-dependent business logic
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::scheduler::{MessageScheduler, ScheduledMessage};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let scheduler = MessageScheduler::new();
//!
//! // Schedule a message for delivery in 5 seconds
//! let msg_id = scheduler.schedule_after(
//!     "task_queue",
//!     vec![1, 2, 3],
//!     Duration::from_secs(5),
//! ).await;
//!
//! // Schedule a message for a specific time
//! use std::time::{SystemTime, UNIX_EPOCH};
//! let delivery_time = SystemTime::now() + Duration::from_secs(60);
//! let msg_id = scheduler.schedule_at(
//!     "task_queue",
//!     vec![4, 5, 6],
//!     delivery_time,
//! ).await;
//!
//! println!("Scheduled message: {}", msg_id);
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Scheduled message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledMessage {
    /// Unique message ID
    pub id: String,
    /// Target queue name
    pub queue: String,
    /// Message payload
    pub payload: Vec<u8>,
    /// Scheduled delivery time (Unix timestamp in milliseconds)
    pub delivery_time: u64,
    /// Message priority (0-9)
    pub priority: Option<u8>,
    /// Number of retry attempts
    pub retry_count: usize,
}

impl ScheduledMessage {
    /// Create a new scheduled message
    pub fn new(queue: String, payload: Vec<u8>, delivery_time: SystemTime) -> Self {
        let timestamp = delivery_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: Uuid::new_v4().to_string(),
            queue,
            payload,
            delivery_time: timestamp,
            priority: None,
            retry_count: 0,
        }
    }

    /// Set message priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority.min(9));
        self
    }

    /// Check if message is ready for delivery
    pub fn is_ready(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now >= self.delivery_time
    }

    /// Get time until delivery
    pub fn time_until_delivery(&self) -> Option<Duration> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if self.delivery_time > now {
            Some(Duration::from_millis(self.delivery_time - now))
        } else {
            None
        }
    }

    /// Get delivery time as SystemTime
    pub fn delivery_time_as_systemtime(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.delivery_time)
    }
}

impl PartialEq for ScheduledMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ScheduledMessage {}

impl PartialOrd for ScheduledMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare delivery times (used with Reverse wrapper in BinaryHeap for min-heap)
        self.delivery_time.cmp(&other.delivery_time)
    }
}

/// Message scheduler statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStats {
    /// Total messages scheduled
    pub total_scheduled: u64,
    /// Messages currently pending
    pub pending_messages: usize,
    /// Messages delivered
    pub delivered_messages: u64,
    /// Messages cancelled
    pub cancelled_messages: u64,
    /// Average delay between schedule and delivery
    pub avg_delay_ms: f64,
}

impl SchedulerStats {
    /// Get completion rate
    pub fn completion_rate(&self) -> f64 {
        if self.total_scheduled == 0 {
            return 0.0;
        }
        self.delivered_messages as f64 / self.total_scheduled as f64
    }

    /// Get cancellation rate
    pub fn cancellation_rate(&self) -> f64 {
        if self.total_scheduled == 0 {
            return 0.0;
        }
        self.cancelled_messages as f64 / self.total_scheduled as f64
    }
}

#[derive(Debug)]
struct SchedulerState {
    /// Priority queue of scheduled messages (earliest first)
    queue: BinaryHeap<Reverse<ScheduledMessage>>,
    /// Total messages scheduled
    total_scheduled: u64,
    /// Messages delivered
    delivered_messages: u64,
    /// Messages cancelled
    cancelled_messages: u64,
    /// Total delivery delay in milliseconds
    total_delay_ms: u64,
}

/// Message scheduler for delayed delivery
///
/// Maintains a priority queue of scheduled messages ordered by delivery time.
/// Messages can be scheduled for delivery at a specific time or after a delay.
#[derive(Debug, Clone)]
pub struct MessageScheduler {
    state: Arc<Mutex<SchedulerState>>,
}

impl MessageScheduler {
    /// Create a new message scheduler
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_amqp::scheduler::MessageScheduler;
    ///
    /// let scheduler = MessageScheduler::new();
    /// ```
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SchedulerState {
                queue: BinaryHeap::new(),
                total_scheduled: 0,
                delivered_messages: 0,
                cancelled_messages: 0,
                total_delay_ms: 0,
            })),
        }
    }

    /// Schedule a message for delivery at a specific time
    ///
    /// # Arguments
    ///
    /// * `queue` - Target queue name
    /// * `payload` - Message payload
    /// * `delivery_time` - When to deliver the message
    ///
    /// Returns the message ID
    pub async fn schedule_at(
        &self,
        queue: impl Into<String>,
        payload: Vec<u8>,
        delivery_time: SystemTime,
    ) -> String {
        let message = ScheduledMessage::new(queue.into(), payload, delivery_time);
        let id = message.id.clone();

        let mut state = self.state.lock().await;
        state.queue.push(Reverse(message));
        state.total_scheduled += 1;

        id
    }

    /// Schedule a message for delivery after a delay
    ///
    /// # Arguments
    ///
    /// * `queue` - Target queue name
    /// * `payload` - Message payload
    /// * `delay` - How long to wait before delivery
    ///
    /// Returns the message ID
    pub async fn schedule_after(
        &self,
        queue: impl Into<String>,
        payload: Vec<u8>,
        delay: Duration,
    ) -> String {
        let delivery_time = SystemTime::now() + delay;
        self.schedule_at(queue, payload, delivery_time).await
    }

    /// Schedule a message with priority
    pub async fn schedule_with_priority(
        &self,
        queue: impl Into<String>,
        payload: Vec<u8>,
        delay: Duration,
        priority: u8,
    ) -> String {
        let delivery_time = SystemTime::now() + delay;
        let message =
            ScheduledMessage::new(queue.into(), payload, delivery_time).with_priority(priority);
        let id = message.id.clone();

        let mut state = self.state.lock().await;
        state.queue.push(Reverse(message));
        state.total_scheduled += 1;

        id
    }

    /// Get next ready message (non-blocking)
    ///
    /// Returns `Some(message)` if a message is ready for delivery, `None` otherwise
    pub async fn poll(&self) -> Option<ScheduledMessage> {
        let mut state = self.state.lock().await;

        // Peek at next message
        if let Some(Reverse(ref msg)) = state.queue.peek() {
            if msg.is_ready() {
                // Message is ready, remove and return it
                let Reverse(msg) = state.queue.pop().unwrap();

                // Update statistics
                state.delivered_messages += 1;
                if let Some(delay) = msg.time_until_delivery() {
                    state.total_delay_ms += delay.as_millis() as u64;
                }

                return Some(msg);
            }
        }

        None
    }

    /// Wait for next message to be ready
    ///
    /// Blocks until a message is ready for delivery
    pub async fn next(&self) -> Option<ScheduledMessage> {
        loop {
            // Check if message is ready
            if let Some(msg) = self.poll().await {
                return Some(msg);
            }

            // Calculate sleep time until next message
            let sleep_duration = {
                let state = self.state.lock().await;
                if let Some(Reverse(ref msg)) = state.queue.peek() {
                    msg.time_until_delivery()
                        .unwrap_or(Duration::from_millis(100))
                } else {
                    // No messages scheduled
                    return None;
                }
            };

            tokio::time::sleep(sleep_duration).await;
        }
    }

    /// Cancel a scheduled message by ID
    ///
    /// Returns `true` if message was found and cancelled
    pub async fn cancel(&self, message_id: &str) -> bool {
        let mut state = self.state.lock().await;

        // Remove message from queue
        let original_len = state.queue.len();
        state.queue.retain(|Reverse(msg)| msg.id != message_id);

        let cancelled = state.queue.len() < original_len;
        if cancelled {
            state.cancelled_messages += 1;
        }

        cancelled
    }

    /// Get number of pending messages
    pub async fn pending_count(&self) -> usize {
        let state = self.state.lock().await;
        state.queue.len()
    }

    /// Get time until next message is ready
    pub async fn time_until_next(&self) -> Option<Duration> {
        let state = self.state.lock().await;
        state
            .queue
            .peek()
            .and_then(|Reverse(msg)| msg.time_until_delivery())
    }

    /// Clear all scheduled messages
    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        let cancelled = state.queue.len();
        state.queue.clear();
        state.cancelled_messages += cancelled as u64;
    }

    /// Get scheduler statistics
    pub async fn statistics(&self) -> SchedulerStats {
        let state = self.state.lock().await;
        let avg_delay_ms = if state.delivered_messages > 0 {
            state.total_delay_ms as f64 / state.delivered_messages as f64
        } else {
            0.0
        };

        SchedulerStats {
            total_scheduled: state.total_scheduled,
            pending_messages: state.queue.len(),
            delivered_messages: state.delivered_messages,
            cancelled_messages: state.cancelled_messages,
            avg_delay_ms,
        }
    }
}

impl Default for MessageScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_schedule_at() {
        let scheduler = MessageScheduler::new();
        let delivery_time = SystemTime::now() + Duration::from_secs(10);

        let id = scheduler
            .schedule_at("test_queue", vec![1, 2, 3], delivery_time)
            .await;

        assert!(!id.is_empty());
        assert_eq!(scheduler.pending_count().await, 1);
    }

    #[tokio::test]
    async fn test_schedule_after() {
        let scheduler = MessageScheduler::new();

        let id = scheduler
            .schedule_after("test_queue", vec![1, 2, 3], Duration::from_secs(10))
            .await;

        assert!(!id.is_empty());
        assert_eq!(scheduler.pending_count().await, 1);
    }

    #[tokio::test]
    async fn test_poll_not_ready() {
        let scheduler = MessageScheduler::new();

        scheduler
            .schedule_after("test_queue", vec![1, 2, 3], Duration::from_secs(10))
            .await;

        // Message not ready yet
        assert!(scheduler.poll().await.is_none());
    }

    #[tokio::test]
    async fn test_poll_ready() {
        let scheduler = MessageScheduler::new();

        scheduler
            .schedule_after("test_queue", vec![1, 2, 3], Duration::from_millis(10))
            .await;

        // Wait for message to be ready
        tokio::time::sleep(Duration::from_millis(20)).await;

        let msg = scheduler.poll().await;
        assert!(msg.is_some());

        let msg = msg.unwrap();
        assert_eq!(msg.queue, "test_queue");
        assert_eq!(msg.payload, vec![1, 2, 3]);
        assert_eq!(scheduler.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_message_ordering() {
        let scheduler = MessageScheduler::new();

        // Schedule messages in reverse order
        let _id3 = scheduler
            .schedule_after("queue", vec![3], Duration::from_millis(300))
            .await;
        let _id2 = scheduler
            .schedule_after("queue", vec![2], Duration::from_millis(200))
            .await;
        let _id1 = scheduler
            .schedule_after("queue", vec![1], Duration::from_millis(100))
            .await;

        // Wait for all messages to be ready
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Should be delivered in order (earliest first)
        let msg1 = scheduler.poll().await.unwrap();
        assert_eq!(msg1.payload, vec![1]);

        let msg2 = scheduler.poll().await.unwrap();
        assert_eq!(msg2.payload, vec![2]);

        let msg3 = scheduler.poll().await.unwrap();
        assert_eq!(msg3.payload, vec![3]);
    }

    #[tokio::test]
    async fn test_cancel() {
        let scheduler = MessageScheduler::new();

        let id = scheduler
            .schedule_after("test_queue", vec![1, 2, 3], Duration::from_secs(10))
            .await;

        assert_eq!(scheduler.pending_count().await, 1);

        let cancelled = scheduler.cancel(&id).await;
        assert!(cancelled);
        assert_eq!(scheduler.pending_count().await, 0);

        // Cancelling again should return false
        let cancelled = scheduler.cancel(&id).await;
        assert!(!cancelled);
    }

    #[tokio::test]
    async fn test_clear() {
        let scheduler = MessageScheduler::new();

        scheduler
            .schedule_after("queue1", vec![1], Duration::from_secs(10))
            .await;
        scheduler
            .schedule_after("queue2", vec![2], Duration::from_secs(20))
            .await;

        assert_eq!(scheduler.pending_count().await, 2);

        scheduler.clear().await;
        assert_eq!(scheduler.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_statistics() {
        let scheduler = MessageScheduler::new();

        scheduler
            .schedule_after("queue", vec![1], Duration::from_millis(10))
            .await;
        scheduler
            .schedule_after("queue", vec![2], Duration::from_millis(20))
            .await;

        tokio::time::sleep(Duration::from_millis(30)).await;

        let _ = scheduler.poll().await;
        let _ = scheduler.poll().await;

        let stats = scheduler.statistics().await;
        assert_eq!(stats.total_scheduled, 2);
        assert_eq!(stats.delivered_messages, 2);
        assert_eq!(stats.pending_messages, 0);
        assert_eq!(stats.completion_rate(), 1.0);
    }

    #[tokio::test]
    async fn test_time_until_next() {
        let scheduler = MessageScheduler::new();

        scheduler
            .schedule_after("queue", vec![1], Duration::from_millis(100))
            .await;

        let time = scheduler.time_until_next().await;
        assert!(time.is_some());

        let duration = time.unwrap();
        assert!(duration.as_millis() <= 100);
        assert!(duration.as_millis() >= 50);
    }

    #[tokio::test]
    async fn test_scheduled_message_is_ready() {
        let past = SystemTime::now() - Duration::from_secs(1);
        let future = SystemTime::now() + Duration::from_secs(10);

        let msg_past = ScheduledMessage::new("queue".to_string(), vec![1], past);
        let msg_future = ScheduledMessage::new("queue".to_string(), vec![2], future);

        assert!(msg_past.is_ready());
        assert!(!msg_future.is_ready());
    }

    #[tokio::test]
    async fn test_scheduled_message_with_priority() {
        let delivery_time = SystemTime::now() + Duration::from_secs(10);
        let msg =
            ScheduledMessage::new("queue".to_string(), vec![1], delivery_time).with_priority(5);

        assert_eq!(msg.priority, Some(5));
    }

    #[tokio::test]
    async fn test_next_blocking() {
        let scheduler = MessageScheduler::new();

        scheduler
            .schedule_after("queue", vec![1], Duration::from_millis(100))
            .await;

        let start = std::time::Instant::now();
        let msg = scheduler.next().await;
        let elapsed = start.elapsed();

        assert!(msg.is_some());
        assert!(elapsed >= Duration::from_millis(90));
        assert!(elapsed <= Duration::from_millis(200));
    }
}
