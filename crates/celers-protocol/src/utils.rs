//! Message utility helpers
//!
//! This module provides convenient helper functions for common message operations.

use crate::{Message, ValidationError};
use chrono::{Duration, Utc};
use uuid::Uuid;

/// Check if a message is expired based on current time
///
/// # Arguments
///
/// * `message` - The message to check
///
/// # Returns
///
/// `true` if the message has an expiration time that has passed, `false` otherwise
pub fn is_message_expired(message: &Message) -> bool {
    message
        .headers
        .expires
        .map(|expires| expires < Utc::now())
        .unwrap_or(false)
}

/// Check if a message should be executed now (ETA has passed)
///
/// # Arguments
///
/// * `message` - The message to check
///
/// # Returns
///
/// `true` if the message has no ETA or the ETA has passed, `false` otherwise
pub fn is_ready_to_execute(message: &Message) -> bool {
    message
        .headers
        .eta
        .map(|eta| eta <= Utc::now())
        .unwrap_or(true)
}

/// Get the time remaining until a message expires
///
/// # Arguments
///
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(Duration)` if the message has an expiration time in the future, `None` otherwise
pub fn time_until_expiration(message: &Message) -> Option<Duration> {
    message.headers.expires.and_then(|expires| {
        let now = Utc::now();
        if expires > now {
            Some(expires - now)
        } else {
            None
        }
    })
}

/// Get the time remaining until a message should be executed
///
/// # Arguments
///
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(Duration)` if the message has an ETA in the future, `None` otherwise
pub fn time_until_eta(message: &Message) -> Option<Duration> {
    message.headers.eta.and_then(|eta| {
        let now = Utc::now();
        if eta > now {
            Some(eta - now)
        } else {
            None
        }
    })
}

/// Calculate the age of a message based on its ETA or expires timestamp
///
/// # Arguments
///
/// * `message` - The message to check
///
/// # Returns
///
/// `Duration` representing the estimated age of the message.
/// If the message has an ETA in the past, returns the duration since that ETA.
/// If the message has an expires timestamp, estimates age as 1/4 of time until expiration.
/// Otherwise returns zero.
///
/// Note: This is an estimation since messages don't carry creation timestamps.
/// For accurate message age tracking, add a custom header with creation timestamp.
pub fn message_age(message: &Message) -> Duration {
    let now = Utc::now();

    // If ETA is in the past, assume message was created around that time
    if let Some(eta) = message.headers.eta {
        if eta < now {
            return now - eta;
        }
    }

    // If expires is set, estimate age based on typical TTL patterns
    // This is a heuristic: assume message was created 1 hour before expiration
    // or 25% of the time to expiration, whichever is smaller
    if let Some(expires) = message.headers.expires {
        if expires > now {
            let time_to_expire = expires - now;
            let estimated_ttl = time_to_expire + Duration::hours(1);
            return Duration::hours(1).min(estimated_ttl / 4);
        } else {
            // Message is expired, estimate it was created 1 hour before expiration
            return now - (expires - Duration::hours(1));
        }
    }

    Duration::zero()
}

/// Check if a message should be retried based on retry count
///
/// # Arguments
///
/// * `message` - The message to check
/// * `max_retries` - Maximum number of retries allowed
///
/// # Returns
///
/// `true` if the message can be retried, `false` otherwise
pub fn can_retry(message: &Message, max_retries: u32) -> bool {
    message.headers.retries.unwrap_or(0) < max_retries
}

/// Create a retry message with incremented retry count
///
/// # Arguments
///
/// * `message` - The original message
/// * `delay` - Optional delay before retry
///
/// # Returns
///
/// A new message with incremented retry count and optional ETA
pub fn create_retry_message(message: &Message, delay: Option<Duration>) -> Message {
    let mut retry_msg = message.clone();
    let current_retries = retry_msg.headers.retries.unwrap_or(0);
    retry_msg.headers.retries = Some(current_retries + 1);

    if let Some(delay) = delay {
        retry_msg.headers.eta = Some(Utc::now() + delay);
    }

    retry_msg
}

/// Clone a message with a new task ID (useful for retries or re-queuing)
///
/// # Arguments
///
/// * `message` - The message to clone
///
/// # Returns
///
/// A new message with a new UUID
pub fn clone_with_new_id(message: &Message) -> Message {
    let mut new_msg = message.clone();
    new_msg.headers.id = Uuid::new_v4();
    new_msg
}

/// Calculate exponential backoff delay for retries
///
/// # Arguments
///
/// * `retry_count` - Current retry attempt number (0-indexed)
/// * `base_delay_secs` - Base delay in seconds
/// * `max_delay_secs` - Maximum delay in seconds
///
/// # Returns
///
/// Duration for the backoff delay
pub fn exponential_backoff(
    retry_count: u32,
    base_delay_secs: u32,
    max_delay_secs: u32,
) -> Duration {
    let delay_secs = (base_delay_secs * 2_u32.pow(retry_count)).min(max_delay_secs);
    Duration::seconds(delay_secs as i64)
}

/// Validate multiple messages in batch
///
/// # Arguments
///
/// * `messages` - Slice of messages to validate
///
/// # Returns
///
/// `Ok(())` if all messages are valid, `Err(ValidationError)` for the first invalid message
pub fn validate_batch(messages: &[Message]) -> Result<(), ValidationError> {
    for msg in messages {
        msg.validate()?;
    }
    Ok(())
}

/// Filter messages by task name pattern
///
/// # Arguments
///
/// * `messages` - Slice of messages to filter
/// * `pattern` - Task name pattern (supports prefix matching)
///
/// # Returns
///
/// Vector of references to messages matching the pattern
pub fn filter_by_task<'a>(messages: &'a [Message], pattern: &str) -> Vec<&'a Message> {
    messages
        .iter()
        .filter(|msg| msg.headers.task.starts_with(pattern))
        .collect()
}

/// Group messages by task name
///
/// # Arguments
///
/// * `messages` - Slice of messages to group
///
/// # Returns
///
/// HashMap mapping task names to vectors of messages
pub fn group_by_task(messages: Vec<Message>) -> std::collections::HashMap<String, Vec<Message>> {
    let mut groups = std::collections::HashMap::new();
    for msg in messages {
        groups
            .entry(msg.headers.task.clone())
            .or_insert_with(Vec::new)
            .push(msg);
    }
    groups
}

/// Sort messages by priority (highest first)
///
/// # Arguments
///
/// * `messages` - Mutable slice of messages to sort
pub fn sort_by_priority(messages: &mut [Message]) {
    messages.sort_by(|a, b| {
        let priority_a = a.properties.priority.unwrap_or(0);
        let priority_b = b.properties.priority.unwrap_or(0);
        priority_b.cmp(&priority_a) // Reverse order (highest first)
    });
}

/// Sort messages by ETA (earliest first)
///
/// # Arguments
///
/// * `messages` - Mutable slice of messages to sort
pub fn sort_by_eta(messages: &mut [Message]) {
    messages.sort_by(|a, b| match (a.headers.eta, b.headers.eta) {
        (Some(eta_a), Some(eta_b)) => eta_a.cmp(&eta_b),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MessageBuilder;

    fn create_test_message() -> Message {
        MessageBuilder::new("tasks.test")
            .args(vec![serde_json::json!(1)])
            .build()
            .unwrap()
    }

    #[test]
    fn test_is_message_expired() {
        let mut msg = create_test_message();

        // Message without expiration is not expired
        assert!(!is_message_expired(&msg));

        // Message with future expiration is not expired
        msg.headers.expires = Some(Utc::now() + Duration::hours(1));
        assert!(!is_message_expired(&msg));

        // Message with past expiration is expired
        msg.headers.expires = Some(Utc::now() - Duration::hours(1));
        assert!(is_message_expired(&msg));
    }

    #[test]
    fn test_is_ready_to_execute() {
        let mut msg = create_test_message();

        // Message without ETA is ready
        assert!(is_ready_to_execute(&msg));

        // Message with past ETA is ready
        msg.headers.eta = Some(Utc::now() - Duration::hours(1));
        assert!(is_ready_to_execute(&msg));

        // Message with future ETA is not ready
        msg.headers.eta = Some(Utc::now() + Duration::hours(1));
        assert!(!is_ready_to_execute(&msg));
    }

    #[test]
    fn test_time_until_expiration() {
        let mut msg = create_test_message();

        // No expiration
        assert!(time_until_expiration(&msg).is_none());

        // Future expiration
        msg.headers.expires = Some(Utc::now() + Duration::hours(1));
        let remaining = time_until_expiration(&msg);
        assert!(remaining.is_some());
        assert!(remaining.unwrap().num_minutes() > 50);

        // Past expiration
        msg.headers.expires = Some(Utc::now() - Duration::hours(1));
        assert!(time_until_expiration(&msg).is_none());
    }

    #[test]
    fn test_can_retry() {
        let mut msg = create_test_message();

        // No retries yet
        assert!(can_retry(&msg, 3));

        // Some retries
        msg.headers.retries = Some(2);
        assert!(can_retry(&msg, 3));

        // Max retries reached
        msg.headers.retries = Some(3);
        assert!(!can_retry(&msg, 3));
    }

    #[test]
    fn test_create_retry_message() {
        let msg = create_test_message();

        // Without delay
        let retry = create_retry_message(&msg, None);
        assert_eq!(retry.headers.retries, Some(1));
        assert_eq!(retry.headers.task, msg.headers.task);

        // With delay
        let delay = Duration::minutes(5);
        let retry_delayed = create_retry_message(&msg, Some(delay));
        assert_eq!(retry_delayed.headers.retries, Some(1));
        assert!(retry_delayed.headers.eta.is_some());
    }

    #[test]
    fn test_clone_with_new_id() {
        let msg = create_test_message();
        let original_id = msg.headers.id;

        let cloned = clone_with_new_id(&msg);
        assert_ne!(cloned.headers.id, original_id);
        assert_eq!(cloned.headers.task, msg.headers.task);
    }

    #[test]
    fn test_exponential_backoff() {
        assert_eq!(exponential_backoff(0, 1, 60), Duration::seconds(1));
        assert_eq!(exponential_backoff(1, 1, 60), Duration::seconds(2));
        assert_eq!(exponential_backoff(2, 1, 60), Duration::seconds(4));
        assert_eq!(exponential_backoff(3, 1, 60), Duration::seconds(8));

        // Test max cap
        assert_eq!(exponential_backoff(10, 1, 60), Duration::seconds(60));
    }

    #[test]
    fn test_validate_batch() {
        let msg1 = create_test_message();
        let msg2 = create_test_message();
        let messages = vec![msg1, msg2];

        assert!(validate_batch(&messages).is_ok());

        // Test with invalid message
        let mut invalid = create_test_message();
        invalid.headers.task = String::new();
        let messages_with_invalid = vec![create_test_message(), invalid];

        assert!(validate_batch(&messages_with_invalid).is_err());
    }

    #[test]
    fn test_filter_by_task() {
        let msg1 = MessageBuilder::new("tasks.add").build().unwrap();
        let msg2 = MessageBuilder::new("tasks.subtract").build().unwrap();
        let msg3 = MessageBuilder::new("email.send").build().unwrap();

        let messages = vec![msg1, msg2, msg3];
        let filtered = filter_by_task(&messages, "tasks.");

        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_sort_by_priority() {
        let mut msg1 = create_test_message();
        let mut msg2 = create_test_message();
        let mut msg3 = create_test_message();

        msg1.properties.priority = Some(5);
        msg2.properties.priority = Some(9);
        msg3.properties.priority = Some(1);

        let mut messages = vec![msg1, msg2, msg3];
        sort_by_priority(&mut messages);

        assert_eq!(messages[0].properties.priority, Some(9));
        assert_eq!(messages[1].properties.priority, Some(5));
        assert_eq!(messages[2].properties.priority, Some(1));
    }

    #[test]
    fn test_sort_by_eta() {
        let now = Utc::now();
        let mut msg1 = create_test_message();
        let mut msg2 = create_test_message();
        let mut msg3 = create_test_message();

        msg1.headers.eta = Some(now + Duration::hours(2));
        msg2.headers.eta = Some(now + Duration::hours(1));
        msg3.headers.eta = None;

        let mut messages = vec![msg1, msg2, msg3];
        sort_by_eta(&mut messages);

        assert!(messages[0].headers.eta.is_some());
        assert!(messages[1].headers.eta.is_some());
        assert!(messages[2].headers.eta.is_none());
    }
}
