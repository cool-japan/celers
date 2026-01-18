//! Task Lifecycle Hooks
//!
//! Provides extensible hooks for intercepting and augmenting task lifecycle events.
//!
//! # Features
//!
//! - **Pre/Post Enqueue Hooks**: Validate, enrich, or route tasks before/after enqueueing
//! - **Pre/Post Dequeue Hooks**: Preprocess tasks or perform side effects when dequeuing
//! - **Task Completion Hooks**: React to task success/failure events
//! - **Composable Hook Chains**: Multiple hooks can be combined
//! - **Async Support**: All hooks support async operations
//!
//! # Example
//!
//! ```rust
//! use celers_broker_redis::hooks::{
//!     EnqueueHook, HookContext, HookResult, TaskHookRegistry,
//! };
//! use celers_core::SerializedTask;
//!
//! // Create a custom validation hook
//! struct ValidationHook;
//!
//! #[async_trait::async_trait]
//! impl EnqueueHook for ValidationHook {
//!     async fn before_enqueue(
//!         &self,
//!         task: &mut SerializedTask,
//!         _ctx: &HookContext,
//!     ) -> HookResult<()> {
//!         // Validate task payload size
//!         if task.payload.len() > 1_000_000 {
//!             return Err("Task payload too large".into());
//!         }
//!         Ok(())
//!     }
//! }
//!
//! // Register the hook
//! let registry = TaskHookRegistry::new();
//! registry.add_enqueue_hook(Box::new(ValidationHook));
//! ```

use async_trait::async_trait;
use celers_core::{SerializedTask, TaskId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Result type for hook operations
pub type HookResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Context passed to hooks containing metadata about the operation
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Queue name
    pub queue_name: String,
    /// Additional context metadata
    pub metadata: HashMap<String, String>,
}

impl HookContext {
    /// Create a new hook context
    pub fn new(queue_name: String) -> Self {
        Self {
            queue_name,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the context
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

/// Hook for task enqueue operations
#[async_trait]
pub trait EnqueueHook: Send + Sync {
    /// Called before a task is enqueued
    ///
    /// Can modify the task or reject the enqueue operation.
    ///
    /// # Arguments
    ///
    /// * `task` - The task about to be enqueued (can be modified)
    /// * `ctx` - Hook context with operation metadata
    ///
    /// # Returns
    ///
    /// `Ok(())` to allow enqueue, `Err(_)` to reject
    async fn before_enqueue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        let _ = (task, ctx);
        Ok(())
    }

    /// Called after a task is successfully enqueued
    ///
    /// # Arguments
    ///
    /// * `task_id` - ID of the enqueued task
    /// * `task` - The enqueued task
    /// * `ctx` - Hook context with operation metadata
    async fn after_enqueue(
        &self,
        task_id: &TaskId,
        task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let _ = (task_id, task, ctx);
        Ok(())
    }
}

/// Hook for task dequeue operations
#[async_trait]
pub trait DequeueHook: Send + Sync {
    /// Called before a task is dequeued
    ///
    /// Can perform preprocessing or reject the dequeue.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Hook context with operation metadata
    async fn before_dequeue(&self, ctx: &HookContext) -> HookResult<()> {
        let _ = ctx;
        Ok(())
    }

    /// Called after a task is successfully dequeued
    ///
    /// Can modify the task before it's processed.
    ///
    /// # Arguments
    ///
    /// * `task` - The dequeued task (can be modified)
    /// * `ctx` - Hook context with operation metadata
    async fn after_dequeue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        let _ = (task, ctx);
        Ok(())
    }
}

/// Task completion status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionStatus {
    /// Task completed successfully
    Success,
    /// Task failed with error
    Failure { error: String },
    /// Task was retried
    Retried { attempt: u32 },
    /// Task was rejected and moved to DLQ
    Rejected,
}

/// Hook for task completion events
#[async_trait]
pub trait CompletionHook: Send + Sync {
    /// Called when a task completes (success or failure)
    ///
    /// # Arguments
    ///
    /// * `task_id` - ID of the completed task
    /// * `status` - Completion status
    /// * `ctx` - Hook context with operation metadata
    async fn on_completion(
        &self,
        task_id: &TaskId,
        status: &CompletionStatus,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let _ = (task_id, status, ctx);
        Ok(())
    }
}

/// Registry for managing task lifecycle hooks
#[derive(Clone)]
pub struct TaskHookRegistry {
    enqueue_hooks: Arc<RwLock<Vec<Box<dyn EnqueueHook>>>>,
    dequeue_hooks: Arc<RwLock<Vec<Box<dyn DequeueHook>>>>,
    completion_hooks: Arc<RwLock<Vec<Box<dyn CompletionHook>>>>,
}

impl TaskHookRegistry {
    /// Create a new empty hook registry
    pub fn new() -> Self {
        Self {
            enqueue_hooks: Arc::new(RwLock::new(Vec::new())),
            dequeue_hooks: Arc::new(RwLock::new(Vec::new())),
            completion_hooks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add an enqueue hook
    pub fn add_enqueue_hook(&self, hook: Box<dyn EnqueueHook>) {
        self.enqueue_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Add a dequeue hook
    pub fn add_dequeue_hook(&self, hook: Box<dyn DequeueHook>) {
        self.dequeue_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Add a completion hook
    pub fn add_completion_hook(&self, hook: Box<dyn CompletionHook>) {
        self.completion_hooks
            .write()
            .expect("lock should not be poisoned")
            .push(hook);
    }

    /// Execute all before_enqueue hooks
    ///
    /// Note: Holds read lock during hook execution. This is safe because:
    /// 1. It's a read lock, not a write lock
    /// 2. Hooks are only added during initialization, not during operation
    /// 3. The lock is released between iterations
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_before_enqueue(
        &self,
        task: &mut SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .enqueue_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .enqueue_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].before_enqueue(task, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all after_enqueue hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_after_enqueue(
        &self,
        task_id: &TaskId,
        task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .enqueue_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .enqueue_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].after_enqueue(task_id, task, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all before_dequeue hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_before_dequeue(&self, ctx: &HookContext) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .dequeue_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .dequeue_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].before_dequeue(ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all after_dequeue hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_after_dequeue(
        &self,
        task: &mut SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .dequeue_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .dequeue_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].after_dequeue(task, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Execute all completion hooks
    #[allow(clippy::await_holding_lock)]
    pub async fn execute_on_completion(
        &self,
        task_id: &TaskId,
        status: &CompletionStatus,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let hooks_count = {
            let guard = self
                .completion_hooks
                .read()
                .expect("lock should not be poisoned");
            guard.len()
        };

        for i in 0..hooks_count {
            let result = {
                let guard = self
                    .completion_hooks
                    .read()
                    .expect("lock should not be poisoned");
                guard[i].on_completion(task_id, status, ctx).await
            };
            result?;
        }
        Ok(())
    }

    /// Get the number of registered enqueue hooks
    pub fn enqueue_hooks_count(&self) -> usize {
        self.enqueue_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Get the number of registered dequeue hooks
    pub fn dequeue_hooks_count(&self) -> usize {
        self.dequeue_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Get the number of registered completion hooks
    pub fn completion_hooks_count(&self) -> usize {
        self.completion_hooks
            .read()
            .expect("lock should not be poisoned")
            .len()
    }
}

impl Default for TaskHookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Built-in Hooks
// ============================================================================

/// Hook that enriches tasks with timestamp metadata
pub struct TimestampEnrichmentHook;

#[async_trait]
impl EnqueueHook for TimestampEnrichmentHook {
    async fn before_enqueue(
        &self,
        _task: &mut SerializedTask,
        _ctx: &HookContext,
    ) -> HookResult<()> {
        // Timestamp is already added by TaskMetadata creation
        // This is a placeholder for custom enrichment
        Ok(())
    }
}

/// Hook that validates task payload size
pub struct PayloadSizeValidator {
    max_size: usize,
}

impl PayloadSizeValidator {
    /// Create a new payload size validator
    ///
    /// # Arguments
    ///
    /// * `max_size` - Maximum allowed payload size in bytes
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

#[async_trait]
impl EnqueueHook for PayloadSizeValidator {
    async fn before_enqueue(
        &self,
        task: &mut SerializedTask,
        _ctx: &HookContext,
    ) -> HookResult<()> {
        if task.payload.len() > self.max_size {
            return Err(format!(
                "Task payload size {} exceeds maximum allowed size {}",
                task.payload.len(),
                self.max_size
            )
            .into());
        }
        Ok(())
    }
}

/// Hook that logs task operations
pub struct LoggingHook {
    level: LogLevel,
}

/// Logging level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warn level
    Warn,
}

impl LoggingHook {
    /// Create a new logging hook
    pub fn new(level: LogLevel) -> Self {
        Self { level }
    }

    #[allow(dead_code)]
    fn log(&self, message: &str) {
        match self.level {
            LogLevel::Debug => tracing::debug!("{}", message),
            LogLevel::Info => tracing::info!("{}", message),
            LogLevel::Warn => tracing::warn!("{}", message),
        }
    }
}

#[async_trait]
impl EnqueueHook for LoggingHook {
    async fn before_enqueue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        self.log(&format!(
            "Enqueueing task {} to queue {}",
            task.metadata.id, ctx.queue_name
        ));
        Ok(())
    }

    async fn after_enqueue(
        &self,
        task_id: &TaskId,
        _task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        self.log(&format!(
            "Enqueued task {} to queue {}",
            task_id, ctx.queue_name
        ));
        Ok(())
    }
}

#[async_trait]
impl DequeueHook for LoggingHook {
    async fn after_dequeue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        self.log(&format!(
            "Dequeued task {} from queue {}",
            task.metadata.id, ctx.queue_name
        ));
        Ok(())
    }
}

#[async_trait]
impl CompletionHook for LoggingHook {
    async fn on_completion(
        &self,
        task_id: &TaskId,
        status: &CompletionStatus,
        ctx: &HookContext,
    ) -> HookResult<()> {
        self.log(&format!(
            "Task {} completed with status {:?} in queue {}",
            task_id, status, ctx.queue_name
        ));
        Ok(())
    }
}

/// Hook that tracks metrics for task operations
pub struct MetricsHook {
    enqueue_count: std::sync::atomic::AtomicU64,
    dequeue_count: std::sync::atomic::AtomicU64,
    success_count: std::sync::atomic::AtomicU64,
    failure_count: std::sync::atomic::AtomicU64,
}

impl MetricsHook {
    /// Create a new metrics hook
    pub fn new() -> Self {
        Self {
            enqueue_count: std::sync::atomic::AtomicU64::new(0),
            dequeue_count: std::sync::atomic::AtomicU64::new(0),
            success_count: std::sync::atomic::AtomicU64::new(0),
            failure_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get enqueue count
    pub fn enqueue_count(&self) -> u64 {
        self.enqueue_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get dequeue count
    pub fn dequeue_count(&self) -> u64 {
        self.dequeue_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get success count
    pub fn success_count(&self) -> u64 {
        self.success_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get failure count
    pub fn failure_count(&self) -> u64 {
        self.failure_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MetricsHook {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EnqueueHook for MetricsHook {
    async fn after_enqueue(
        &self,
        _task_id: &TaskId,
        _task: &SerializedTask,
        _ctx: &HookContext,
    ) -> HookResult<()> {
        self.enqueue_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl DequeueHook for MetricsHook {
    async fn after_dequeue(
        &self,
        _task: &mut SerializedTask,
        _ctx: &HookContext,
    ) -> HookResult<()> {
        self.dequeue_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl CompletionHook for MetricsHook {
    async fn on_completion(
        &self,
        _task_id: &TaskId,
        status: &CompletionStatus,
        _ctx: &HookContext,
    ) -> HookResult<()> {
        match status {
            CompletionStatus::Success => {
                self.success_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            CompletionStatus::Failure { .. } | CompletionStatus::Rejected => {
                self.failure_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            CompletionStatus::Retried { .. } => {
                // Don't count retries as success or failure
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_context_creation() {
        let ctx = HookContext::new("test_queue".to_string());
        assert_eq!(ctx.queue_name, "test_queue");
        assert!(ctx.metadata.is_empty());
    }

    #[test]
    fn test_hook_context_with_metadata() {
        let ctx = HookContext::new("test_queue".to_string())
            .with_metadata("key1".to_string(), "value1".to_string())
            .with_metadata("key2".to_string(), "value2".to_string());

        assert_eq!(ctx.get_metadata("key1"), Some(&"value1".to_string()));
        assert_eq!(ctx.get_metadata("key2"), Some(&"value2".to_string()));
        assert_eq!(ctx.get_metadata("nonexistent"), None);
    }

    #[test]
    fn test_completion_status_equality() {
        assert_eq!(CompletionStatus::Success, CompletionStatus::Success);
        assert_eq!(
            CompletionStatus::Failure {
                error: "test".to_string()
            },
            CompletionStatus::Failure {
                error: "test".to_string()
            }
        );
        assert_eq!(
            CompletionStatus::Retried { attempt: 1 },
            CompletionStatus::Retried { attempt: 1 }
        );
    }

    #[test]
    fn test_hook_registry_creation() {
        let registry = TaskHookRegistry::new();
        assert_eq!(registry.enqueue_hooks_count(), 0);
        assert_eq!(registry.dequeue_hooks_count(), 0);
        assert_eq!(registry.completion_hooks_count(), 0);
    }

    #[test]
    fn test_hook_registry_add_hooks() {
        let registry = TaskHookRegistry::new();

        registry.add_enqueue_hook(Box::new(TimestampEnrichmentHook));
        registry.add_dequeue_hook(Box::new(LoggingHook::new(LogLevel::Info)));
        registry.add_completion_hook(Box::new(MetricsHook::new()));

        assert_eq!(registry.enqueue_hooks_count(), 1);
        assert_eq!(registry.dequeue_hooks_count(), 1);
        assert_eq!(registry.completion_hooks_count(), 1);
    }

    #[tokio::test]
    async fn test_payload_size_validator() {
        let validator = PayloadSizeValidator::new(100);
        let ctx = HookContext::new("test_queue".to_string());

        // Small payload should pass
        let mut small_task = SerializedTask::new("test".to_string(), vec![0u8; 50]);
        assert!(validator
            .before_enqueue(&mut small_task, &ctx)
            .await
            .is_ok());

        // Large payload should fail
        let mut large_task = SerializedTask::new("test".to_string(), vec![0u8; 150]);
        assert!(validator
            .before_enqueue(&mut large_task, &ctx)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_metrics_hook() {
        let hook = MetricsHook::new();
        let ctx = HookContext::new("test_queue".to_string());
        let task_id = TaskId::new_v4();
        let task = SerializedTask::new("test".to_string(), vec![]);

        // Test enqueue
        hook.after_enqueue(&task_id, &task, &ctx).await.unwrap();
        assert_eq!(hook.enqueue_count(), 1);

        // Test dequeue
        let mut task_mut = task.clone();
        hook.after_dequeue(&mut task_mut, &ctx).await.unwrap();
        assert_eq!(hook.dequeue_count(), 1);

        // Test success completion
        hook.on_completion(&task_id, &CompletionStatus::Success, &ctx)
            .await
            .unwrap();
        assert_eq!(hook.success_count(), 1);

        // Test failure completion
        hook.on_completion(
            &task_id,
            &CompletionStatus::Failure {
                error: "test".to_string(),
            },
            &ctx,
        )
        .await
        .unwrap();
        assert_eq!(hook.failure_count(), 1);
    }

    #[tokio::test]
    async fn test_hook_registry_execute_before_enqueue() {
        let registry = TaskHookRegistry::new();
        registry.add_enqueue_hook(Box::new(PayloadSizeValidator::new(100)));

        let ctx = HookContext::new("test_queue".to_string());
        let mut task = SerializedTask::new("test".to_string(), vec![0u8; 50]);

        // Should succeed with small payload
        assert!(registry
            .execute_before_enqueue(&mut task, &ctx)
            .await
            .is_ok());

        let mut large_task = SerializedTask::new("test".to_string(), vec![0u8; 150]);

        // Should fail with large payload
        assert!(registry
            .execute_before_enqueue(&mut large_task, &ctx)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_hook_registry_execute_after_enqueue() {
        let registry = TaskHookRegistry::new();
        let metrics = MetricsHook::new();
        registry.add_enqueue_hook(Box::new(metrics));

        let ctx = HookContext::new("test_queue".to_string());
        let task_id = TaskId::new_v4();
        let task = SerializedTask::new("test".to_string(), vec![]);

        registry
            .execute_after_enqueue(&task_id, &task, &ctx)
            .await
            .unwrap();

        // Note: We can't access the metrics directly since it's boxed,
        // but we can verify the hook was called without errors
    }

    #[tokio::test]
    async fn test_logging_hook_creation() {
        let hook = LoggingHook::new(LogLevel::Info);
        let ctx = HookContext::new("test_queue".to_string());
        let mut task = SerializedTask::new("test".to_string(), vec![]);

        // Should not panic
        assert!(hook.before_enqueue(&mut task, &ctx).await.is_ok());
        assert!(hook.after_dequeue(&mut task, &ctx).await.is_ok());
    }

    #[test]
    fn test_log_level_equality() {
        assert_eq!(LogLevel::Info, LogLevel::Info);
        assert_ne!(LogLevel::Info, LogLevel::Debug);
    }
}
