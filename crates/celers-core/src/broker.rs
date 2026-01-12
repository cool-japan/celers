use crate::{Result, SerializedTask, TaskId};

/// Message envelope for broker operations
#[derive(Debug, Clone)]
pub struct BrokerMessage {
    /// The serialized task
    pub task: SerializedTask,

    /// Receipt handle for acknowledging/rejecting the message
    pub receipt_handle: Option<String>,
}

impl BrokerMessage {
    /// Create a new broker message
    #[must_use]
    pub const fn new(task: SerializedTask) -> Self {
        Self {
            task,
            receipt_handle: None,
        }
    }

    /// Create a new broker message with a receipt handle
    #[must_use]
    pub const fn with_receipt_handle(task: SerializedTask, receipt_handle: String) -> Self {
        Self {
            task,
            receipt_handle: Some(receipt_handle),
        }
    }

    /// Check if message has a receipt handle
    #[inline]
    #[must_use]
    pub const fn has_receipt_handle(&self) -> bool {
        self.receipt_handle.is_some()
    }

    /// Get task ID
    #[inline]
    #[must_use]
    pub const fn task_id(&self) -> crate::TaskId {
        self.task.metadata.id
    }

    /// Get task name
    #[inline]
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task.metadata.name
    }

    /// Get task priority
    #[inline]
    #[must_use]
    pub const fn priority(&self) -> i32 {
        self.task.metadata.priority
    }

    /// Check if task is expired
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.task.is_expired()
    }

    /// Get task age
    #[inline]
    #[must_use]
    pub fn age(&self) -> chrono::Duration {
        self.task.age()
    }
}

impl std::fmt::Display for BrokerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrokerMessage[task={}]", self.task)?;
        if let Some(ref handle) = self.receipt_handle {
            write!(f, " receipt={}", &handle[..handle.len().min(8)])?;
        }
        Ok(())
    }
}

/// Core trait for task queue brokers
#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    /// Enqueue a task to the broker
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId>;

    /// Dequeue a task from the broker (blocking/waiting)
    async fn dequeue(&self) -> Result<Option<BrokerMessage>>;

    /// Acknowledge successful processing of a task
    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()>;

    /// Reject a task and potentially requeue it
    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()>;

    /// Get the current queue size
    async fn queue_size(&self) -> Result<usize>;

    /// Cancel a pending task
    async fn cancel(&self, task_id: &TaskId) -> Result<bool>;

    // Batch Operations (optional, with default implementations)

    /// Enqueue multiple tasks in a single operation (batch)
    ///
    /// Default implementation calls `enqueue()` for each task.
    /// Brokers should override this for better performance.
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        let mut task_ids = Vec::with_capacity(tasks.len());
        for task in tasks {
            task_ids.push(self.enqueue(task).await?);
        }
        Ok(task_ids)
    }

    /// Dequeue multiple tasks in a single operation (batch)
    ///
    /// Returns up to `count` messages from the queue.
    /// Default implementation calls `dequeue()` multiple times.
    /// Brokers should override this for better performance.
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        let mut messages = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(msg) = self.dequeue().await? {
                messages.push(msg);
            } else {
                break;
            }
        }
        Ok(messages)
    }

    /// Acknowledge multiple tasks in a single operation (batch)
    ///
    /// Default implementation calls `ack()` for each task.
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        for (task_id, receipt_handle) in tasks {
            self.ack(task_id, receipt_handle.as_deref()).await?;
        }
        Ok(())
    }

    // Delayed Task Execution (optional, with default implementations)

    /// Schedule a task for execution at a specific Unix timestamp (seconds)
    ///
    /// Default implementation executes the task immediately via `enqueue()`.
    /// Brokers with scheduling support should override this.
    async fn enqueue_at(&self, task: SerializedTask, _execute_at: i64) -> Result<TaskId> {
        // Default: execute immediately
        self.enqueue(task).await
    }

    /// Schedule a task for execution after a delay (seconds)
    ///
    /// Default implementation executes the task immediately via `enqueue()`.
    /// Brokers with scheduling support should override this.
    async fn enqueue_after(&self, task: SerializedTask, _delay_secs: u64) -> Result<TaskId> {
        // Default: execute immediately
        self.enqueue(task).await
    }
}

/// Batch utilities for `BrokerMessage` collections
pub mod broker_batch {
    use super::{BrokerMessage, TaskId};
    use std::collections::HashMap;

    /// Sort messages by priority (highest first)
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let mut messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1]).with_priority(1)),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![2]).with_priority(10)),
    ///     BrokerMessage::new(SerializedTask::new("task3".to_string(), vec![3]).with_priority(5)),
    /// ];
    ///
    /// broker_batch::sort_by_priority(&mut messages);
    /// assert_eq!(messages[0].priority(), 10);
    /// assert_eq!(messages[2].priority(), 1);
    /// ```
    #[inline]
    pub fn sort_by_priority(messages: &mut [BrokerMessage]) {
        messages.sort_by_key(|b| std::cmp::Reverse(b.priority()));
    }

    /// Group messages by task name
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1])),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![2])),
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![3])),
    /// ];
    ///
    /// let grouped = broker_batch::group_by_task_name(&messages);
    /// assert_eq!(grouped.get("task1").unwrap().len(), 2);
    /// assert_eq!(grouped.get("task2").unwrap().len(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub fn group_by_task_name(messages: &[BrokerMessage]) -> HashMap<String, Vec<&BrokerMessage>> {
        let mut map: HashMap<String, Vec<&BrokerMessage>> = HashMap::new();
        for msg in messages {
            map.entry(msg.task_name().to_string())
                .or_default()
                .push(msg);
        }
        map
    }

    /// Filter messages by task name pattern
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("process_data".to_string(), vec![1])),
    ///     BrokerMessage::new(SerializedTask::new("send_email".to_string(), vec![2])),
    ///     BrokerMessage::new(SerializedTask::new("process_image".to_string(), vec![3])),
    /// ];
    ///
    /// let process_messages = broker_batch::filter_by_name_prefix(&messages, "process");
    /// assert_eq!(process_messages.len(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub fn filter_by_name_prefix<'a>(
        messages: &'a [BrokerMessage],
        prefix: &str,
    ) -> Vec<&'a BrokerMessage> {
        messages
            .iter()
            .filter(|msg| msg.task_name().starts_with(prefix))
            .collect()
    }

    /// Get total payload size of all messages
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1, 2, 3])),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![4, 5])),
    /// ];
    ///
    /// let total_size = broker_batch::total_payload_size(&messages);
    /// assert_eq!(total_size, 5);
    /// ```
    #[inline]
    #[must_use]
    pub fn total_payload_size(messages: &[BrokerMessage]) -> usize {
        messages.iter().map(|msg| msg.task.payload.len()).sum()
    }

    /// Filter expired messages
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1])),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![2])),
    /// ];
    ///
    /// let expired = broker_batch::filter_expired(&messages);
    /// // Newly created tasks should not be expired
    /// assert_eq!(expired.len(), 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn filter_expired(messages: &[BrokerMessage]) -> Vec<&BrokerMessage> {
        messages.iter().filter(|msg| msg.is_expired()).collect()
    }

    /// Extract task IDs and receipt handles for batch acknowledgement
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::with_receipt_handle(
    ///         SerializedTask::new("task1".to_string(), vec![1]),
    ///         "receipt1".to_string()
    ///     ),
    ///     BrokerMessage::with_receipt_handle(
    ///         SerializedTask::new("task2".to_string(), vec![2]),
    ///         "receipt2".to_string()
    ///     ),
    /// ];
    ///
    /// let ack_data = broker_batch::prepare_ack_batch(&messages);
    /// assert_eq!(ack_data.len(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub fn prepare_ack_batch(messages: &[BrokerMessage]) -> Vec<(TaskId, Option<String>)> {
        messages
            .iter()
            .map(|msg| (msg.task_id(), msg.receipt_handle.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::SerializedTask;

    fn create_test_task() -> SerializedTask {
        SerializedTask::new("test_task".to_string(), vec![1, 2, 3])
    }

    #[test]
    fn test_broker_message_new() {
        let task = create_test_task();
        let task_id = task.metadata.id;
        let msg = BrokerMessage::new(task);

        assert_eq!(msg.task_id(), task_id);
        assert_eq!(msg.task_name(), "test_task");
        assert!(!msg.has_receipt_handle());
        assert!(msg.receipt_handle.is_none());
    }

    #[test]
    fn test_broker_message_with_receipt_handle() {
        let task = create_test_task();
        let receipt = "receipt-123456".to_string();
        let msg = BrokerMessage::with_receipt_handle(task, receipt.clone());

        assert!(msg.has_receipt_handle());
        assert_eq!(msg.receipt_handle, Some(receipt));
    }

    #[test]
    fn test_broker_message_task_accessors() {
        let task = create_test_task().with_priority(5);
        let task_id = task.metadata.id;
        let msg = BrokerMessage::new(task);

        assert_eq!(msg.task_id(), task_id);
        assert_eq!(msg.task_name(), "test_task");
        assert_eq!(msg.priority(), 5);
    }

    #[test]
    fn test_broker_message_is_expired() {
        let task = create_test_task();
        let msg = BrokerMessage::new(task);

        // Newly created task should not be expired
        assert!(!msg.is_expired());
    }

    #[test]
    fn test_broker_message_age() {
        let task = create_test_task();
        let msg = BrokerMessage::new(task);

        // Age should be very small for newly created task
        let age = msg.age();
        assert!(age.num_seconds() < 1);
    }

    #[test]
    fn test_broker_message_display() {
        let task = create_test_task();
        let msg = BrokerMessage::new(task);

        let display = format!("{msg}");
        assert!(display.contains("BrokerMessage"));
        assert!(display.contains("task="));
    }

    #[test]
    fn test_broker_message_display_with_receipt() {
        let task = create_test_task();
        let receipt = "very-long-receipt-handle-12345678901234567890".to_string();
        let msg = BrokerMessage::with_receipt_handle(task, receipt);

        let display = format!("{msg}");
        assert!(display.contains("BrokerMessage"));
        assert!(display.contains("receipt="));
        // Should truncate to 8 characters
        assert!(display.contains("very-lon"));
    }
}
