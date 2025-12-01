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
    pub fn new(task: SerializedTask) -> Self {
        Self {
            task,
            receipt_handle: None,
        }
    }

    /// Create a new broker message with a receipt handle
    pub fn with_receipt_handle(task: SerializedTask, receipt_handle: String) -> Self {
        Self {
            task,
            receipt_handle: Some(receipt_handle),
        }
    }

    /// Check if message has a receipt handle
    pub fn has_receipt_handle(&self) -> bool {
        self.receipt_handle.is_some()
    }

    /// Get task ID
    pub fn task_id(&self) -> crate::TaskId {
        self.task.metadata.id
    }

    /// Get task name
    pub fn task_name(&self) -> &str {
        &self.task.metadata.name
    }

    /// Get task priority
    pub fn priority(&self) -> i32 {
        self.task.metadata.priority
    }

    /// Check if task is expired
    pub fn is_expired(&self) -> bool {
        self.task.is_expired()
    }

    /// Get task age
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
    /// Default implementation calls enqueue() for each task.
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
    /// Default implementation calls dequeue() multiple times.
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
    /// Default implementation calls ack() for each task.
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        for (task_id, receipt_handle) in tasks {
            self.ack(task_id, receipt_handle.as_deref()).await?;
        }
        Ok(())
    }

    // Delayed Task Execution (optional, with default implementations)

    /// Schedule a task for execution at a specific Unix timestamp (seconds)
    ///
    /// Default implementation executes the task immediately via enqueue().
    /// Brokers with scheduling support should override this.
    async fn enqueue_at(&self, task: SerializedTask, _execute_at: i64) -> Result<TaskId> {
        // Default: execute immediately
        self.enqueue(task).await
    }

    /// Schedule a task for execution after a delay (seconds)
    ///
    /// Default implementation executes the task immediately via enqueue().
    /// Brokers with scheduling support should override this.
    async fn enqueue_after(&self, task: SerializedTask, _delay_secs: u64) -> Result<TaskId> {
        // Default: execute immediately
        self.enqueue(task).await
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

        let display = format!("{}", msg);
        assert!(display.contains("BrokerMessage"));
        assert!(display.contains("task="));
    }

    #[test]
    fn test_broker_message_display_with_receipt() {
        let task = create_test_task();
        let receipt = "very-long-receipt-handle-12345678901234567890".to_string();
        let msg = BrokerMessage::with_receipt_handle(task, receipt);

        let display = format!("{}", msg);
        assert!(display.contains("BrokerMessage"));
        assert!(display.contains("receipt="));
        // Should truncate to 8 characters
        assert!(display.contains("very-lon"));
    }
}
