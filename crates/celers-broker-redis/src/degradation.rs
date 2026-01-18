//! Graceful degradation for Redis broker failures
//!
//! Provides fallback mechanisms when Redis is unavailable or experiencing issues:
//! - Local in-memory queue fallback
//! - Read-only mode
//! - Automatic recovery detection
//! - Queued operations replay

use celers_core::{CelersError, Result, SerializedTask, TaskId};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Degradation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationMode {
    /// Normal operation with Redis
    Normal,
    /// Read-only mode (only dequeue and ack operations allowed)
    ReadOnly,
    /// Local fallback mode (operations queued in memory)
    LocalFallback,
    /// Complete failure (all operations fail)
    Failed,
}

impl std::fmt::Display for DegradationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DegradationMode::Normal => write!(f, "Normal"),
            DegradationMode::ReadOnly => write!(f, "ReadOnly"),
            DegradationMode::LocalFallback => write!(f, "LocalFallback"),
            DegradationMode::Failed => write!(f, "Failed"),
        }
    }
}

/// Queued operation during degradation
#[derive(Debug, Clone)]
pub enum QueuedOperation {
    /// Enqueue task
    Enqueue { task: Box<SerializedTask> },
    /// Ack task
    Ack {
        task_id: TaskId,
        receipt_handle: Option<String>,
    },
    /// Reject task
    Reject {
        task_id: TaskId,
        receipt_handle: Option<String>,
        requeue: bool,
    },
    /// Cancel task
    Cancel { task_id: TaskId },
}

/// Graceful degradation manager
pub struct DegradationManager {
    mode: Arc<RwLock<DegradationMode>>,
    /// Local fallback queue for tasks
    local_queue: Arc<RwLock<VecDeque<SerializedTask>>>,
    /// Queued operations to replay when Redis recovers
    queued_operations: Arc<RwLock<VecDeque<QueuedOperation>>>,
    /// Maximum local queue size (prevent memory exhaustion)
    max_local_queue_size: usize,
    /// Maximum queued operations (prevent memory exhaustion)
    max_queued_operations: usize,
    /// Statistics
    fallback_count: Arc<RwLock<u64>>,
    recovered_count: Arc<RwLock<u64>>,
}

impl DegradationManager {
    /// Create a new degradation manager
    pub fn new() -> Self {
        Self {
            mode: Arc::new(RwLock::new(DegradationMode::Normal)),
            local_queue: Arc::new(RwLock::new(VecDeque::new())),
            queued_operations: Arc::new(RwLock::new(VecDeque::new())),
            max_local_queue_size: 10_000,
            max_queued_operations: 10_000,
            fallback_count: Arc::new(RwLock::new(0)),
            recovered_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Create with custom limits
    pub fn with_limits(max_local_queue_size: usize, max_queued_operations: usize) -> Self {
        Self {
            mode: Arc::new(RwLock::new(DegradationMode::Normal)),
            local_queue: Arc::new(RwLock::new(VecDeque::new())),
            queued_operations: Arc::new(RwLock::new(VecDeque::new())),
            max_local_queue_size,
            max_queued_operations,
            fallback_count: Arc::new(RwLock::new(0)),
            recovered_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Get current degradation mode
    pub async fn mode(&self) -> DegradationMode {
        *self.mode.read().await
    }

    /// Enter degradation mode
    pub async fn enter_mode(&self, mode: DegradationMode) -> Result<()> {
        let mut current_mode = self.mode.write().await;
        if *current_mode != mode {
            warn!("Entering degradation mode: {}", mode);
            *current_mode = mode;

            if mode != DegradationMode::Normal {
                let mut count = self.fallback_count.write().await;
                *count += 1;
            }
        }
        Ok(())
    }

    /// Return to normal mode (after Redis recovery)
    pub async fn recover(&self) -> Result<()> {
        info!("Recovering from degradation mode");

        let mut mode = self.mode.write().await;
        *mode = DegradationMode::Normal;

        let mut count = self.recovered_count.write().await;
        *count += 1;

        Ok(())
    }

    /// Check if operations are allowed in current mode
    pub async fn can_enqueue(&self) -> bool {
        let mode = self.mode.read().await;
        matches!(
            *mode,
            DegradationMode::Normal | DegradationMode::LocalFallback
        )
    }

    /// Check if dequeue operations are allowed
    pub async fn can_dequeue(&self) -> bool {
        let mode = self.mode.read().await;
        matches!(
            *mode,
            DegradationMode::Normal | DegradationMode::ReadOnly | DegradationMode::LocalFallback
        )
    }

    /// Enqueue task to local fallback queue
    pub async fn local_enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let mut queue = self.local_queue.write().await;

        if queue.len() >= self.max_local_queue_size {
            return Err(CelersError::Broker(
                "Local fallback queue is full".to_string(),
            ));
        }

        let task_id = task.metadata.id;
        queue.push_back(task);

        info!("Task {} enqueued to local fallback queue", task_id);
        Ok(task_id)
    }

    /// Dequeue task from local fallback queue
    pub async fn local_dequeue(&self) -> Result<Option<SerializedTask>> {
        let mut queue = self.local_queue.write().await;
        Ok(queue.pop_front())
    }

    /// Get local queue size
    pub async fn local_queue_size(&self) -> usize {
        self.local_queue.read().await.len()
    }

    /// Queue an operation for later replay
    pub async fn queue_operation(&self, operation: QueuedOperation) -> Result<()> {
        let mut ops = self.queued_operations.write().await;

        if ops.len() >= self.max_queued_operations {
            return Err(CelersError::Broker(
                "Queued operations limit reached".to_string(),
            ));
        }

        ops.push_back(operation);
        Ok(())
    }

    /// Get all queued operations (for replay)
    pub async fn get_queued_operations(&self) -> Vec<QueuedOperation> {
        let mut ops = self.queued_operations.write().await;
        ops.drain(..).collect()
    }

    /// Get number of queued operations
    pub async fn queued_operations_count(&self) -> usize {
        self.queued_operations.read().await.len()
    }

    /// Clear all local state (use with caution!)
    pub async fn clear(&self) {
        let mut queue = self.local_queue.write().await;
        let mut ops = self.queued_operations.write().await;

        queue.clear();
        ops.clear();

        warn!("Cleared all local fallback data");
    }

    /// Get degradation statistics
    pub async fn stats(&self) -> DegradationStats {
        DegradationStats {
            mode: *self.mode.read().await,
            local_queue_size: self.local_queue.read().await.len(),
            queued_operations: self.queued_operations.read().await.len(),
            fallback_count: *self.fallback_count.read().await,
            recovered_count: *self.recovered_count.read().await,
        }
    }
}

impl Default for DegradationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Degradation statistics
#[derive(Debug, Clone)]
pub struct DegradationStats {
    /// Current degradation mode
    pub mode: DegradationMode,
    /// Number of tasks in local queue
    pub local_queue_size: usize,
    /// Number of queued operations
    pub queued_operations: usize,
    /// Number of times entered fallback mode
    pub fallback_count: u64,
    /// Number of times recovered
    pub recovered_count: u64,
}

impl DegradationStats {
    /// Check if currently degraded
    pub fn is_degraded(&self) -> bool {
        self.mode != DegradationMode::Normal
    }

    /// Check if using local fallback
    pub fn is_local_fallback(&self) -> bool {
        self.mode == DegradationMode::LocalFallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_task() -> SerializedTask {
        let payload_json = serde_json::json!({"key": "value"});
        let payload_bytes = serde_json::to_vec(&payload_json).unwrap();

        let task_json = serde_json::json!({
            "metadata": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "test.task",
                "priority": 5,
                "state": "Pending",
                "retries": 0,
                "max_retries": 3,
                "eta": null,
                "expires": null,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            },
            "payload": payload_bytes
        });

        serde_json::from_value(task_json).unwrap()
    }

    #[test]
    fn test_degradation_mode_display() {
        assert_eq!(DegradationMode::Normal.to_string(), "Normal");
        assert_eq!(DegradationMode::ReadOnly.to_string(), "ReadOnly");
        assert_eq!(DegradationMode::LocalFallback.to_string(), "LocalFallback");
        assert_eq!(DegradationMode::Failed.to_string(), "Failed");
    }

    #[tokio::test]
    async fn test_default_mode() {
        let manager = DegradationManager::new();
        assert_eq!(manager.mode().await, DegradationMode::Normal);
    }

    #[tokio::test]
    async fn test_enter_mode() {
        let manager = DegradationManager::new();

        manager
            .enter_mode(DegradationMode::LocalFallback)
            .await
            .unwrap();
        assert_eq!(manager.mode().await, DegradationMode::LocalFallback);

        manager.enter_mode(DegradationMode::ReadOnly).await.unwrap();
        assert_eq!(manager.mode().await, DegradationMode::ReadOnly);
    }

    #[tokio::test]
    async fn test_recover() {
        let manager = DegradationManager::new();

        manager
            .enter_mode(DegradationMode::LocalFallback)
            .await
            .unwrap();
        manager.recover().await.unwrap();

        assert_eq!(manager.mode().await, DegradationMode::Normal);

        let stats = manager.stats().await;
        assert_eq!(stats.recovered_count, 1);
    }

    #[tokio::test]
    async fn test_can_enqueue() {
        let manager = DegradationManager::new();

        assert!(manager.can_enqueue().await);

        manager.enter_mode(DegradationMode::ReadOnly).await.unwrap();
        assert!(!manager.can_enqueue().await);

        manager
            .enter_mode(DegradationMode::LocalFallback)
            .await
            .unwrap();
        assert!(manager.can_enqueue().await);
    }

    #[tokio::test]
    async fn test_can_dequeue() {
        let manager = DegradationManager::new();

        assert!(manager.can_dequeue().await);

        manager.enter_mode(DegradationMode::Failed).await.unwrap();
        assert!(!manager.can_dequeue().await);

        manager.enter_mode(DegradationMode::ReadOnly).await.unwrap();
        assert!(manager.can_dequeue().await);
    }

    #[tokio::test]
    async fn test_local_enqueue_dequeue() {
        let manager = DegradationManager::new();
        let task = create_test_task();
        let task_id = task.metadata.id;

        manager.local_enqueue(task).await.unwrap();
        assert_eq!(manager.local_queue_size().await, 1);

        let dequeued = manager.local_dequeue().await.unwrap();
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().metadata.id, task_id);
        assert_eq!(manager.local_queue_size().await, 0);
    }

    #[tokio::test]
    async fn test_local_queue_limit() {
        let manager = DegradationManager::with_limits(2, 100);

        let task1 = create_test_task();
        let task2 = create_test_task();
        let task3 = create_test_task();

        manager.local_enqueue(task1).await.unwrap();
        manager.local_enqueue(task2).await.unwrap();

        let result = manager.local_enqueue(task3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_queue_operations() {
        let manager = DegradationManager::new();
        let task = create_test_task();

        let op = QueuedOperation::Enqueue {
            task: Box::new(task),
        };
        manager.queue_operation(op).await.unwrap();

        assert_eq!(manager.queued_operations_count().await, 1);

        let ops = manager.get_queued_operations().await;
        assert_eq!(ops.len(), 1);
        assert_eq!(manager.queued_operations_count().await, 0);
    }

    #[tokio::test]
    async fn test_queued_operations_limit() {
        let manager = DegradationManager::with_limits(100, 2);
        let task = create_test_task();

        let op1 = QueuedOperation::Enqueue {
            task: Box::new(task.clone()),
        };
        let op2 = QueuedOperation::Enqueue {
            task: Box::new(task.clone()),
        };
        let op3 = QueuedOperation::Enqueue {
            task: Box::new(task),
        };

        manager.queue_operation(op1).await.unwrap();
        manager.queue_operation(op2).await.unwrap();

        let result = manager.queue_operation(op3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_clear() {
        let manager = DegradationManager::new();
        let task = create_test_task();

        manager.local_enqueue(task.clone()).await.unwrap();
        manager
            .queue_operation(QueuedOperation::Enqueue {
                task: Box::new(task),
            })
            .await
            .unwrap();

        assert_eq!(manager.local_queue_size().await, 1);
        assert_eq!(manager.queued_operations_count().await, 1);

        manager.clear().await;

        assert_eq!(manager.local_queue_size().await, 0);
        assert_eq!(manager.queued_operations_count().await, 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = DegradationManager::new();

        manager
            .enter_mode(DegradationMode::LocalFallback)
            .await
            .unwrap();

        let task = create_test_task();
        manager.local_enqueue(task).await.unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.mode, DegradationMode::LocalFallback);
        assert_eq!(stats.local_queue_size, 1);
        assert_eq!(stats.fallback_count, 1);
        assert!(stats.is_degraded());
        assert!(stats.is_local_fallback());
    }
}
