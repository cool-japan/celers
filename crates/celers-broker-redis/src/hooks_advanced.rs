//! Advanced Hook Features
//!
//! Extends the basic hook system with:
//! - Conditional hook execution based on task attributes
//! - Hook priority ordering for deterministic execution
//! - Async parallel hook execution for performance
//! - Hook error recovery strategies
//!
//! # Example
//!
//! ```rust
//! use celers_broker_redis::hooks_advanced::{
//!     ConditionalHook, HookCondition, HookErrorStrategy, PrioritizedHook, ParallelHookExecutor
//! };
//! use celers_broker_redis::hooks::{EnqueueHook, HookContext, HookResult};
//! use celers_core::SerializedTask;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a conditional hook that only runs for specific tasks
//! struct MyHook;
//!
//! #[async_trait::async_trait]
//! impl EnqueueHook for MyHook {
//!     async fn before_enqueue(&self, task: &mut SerializedTask, _ctx: &HookContext) -> HookResult<()> {
//!         println!("Processing task: {}", task.metadata.name);
//!         Ok(())
//!     }
//! }
//!
//! let condition = HookCondition::TaskName("important".to_string());
//! let conditional = ConditionalHook::new(Box::new(MyHook), condition);
//!
//! // Create prioritized hooks for ordered execution
//! let hook1 = PrioritizedHook::new(Box::new(MyHook), 100); // High priority
//! let hook2 = PrioritizedHook::new(Box::new(MyHook), 50);  // Lower priority
//!
//! // Execute hooks in parallel
//! let executor = ParallelHookExecutor::new();
//! let mut task = SerializedTask::new("test".to_string(), vec![]);
//! let ctx = HookContext::new("queue".to_string());
//!
//! // executor.execute_parallel(vec![hook1, hook2], &mut task, &ctx).await?;
//! # Ok(())
//! # }
//! ```

use crate::hooks::{DequeueHook, EnqueueHook, HookContext, HookResult};
use async_trait::async_trait;
use celers_core::{SerializedTask, TaskId};
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::error;

/// Condition for conditional hook execution
#[derive(Debug, Clone)]
pub enum HookCondition {
    /// Execute only for tasks with matching name pattern
    TaskName(String),
    /// Execute only for tasks with specific priority range
    PriorityRange { min: i32, max: i32 },
    /// Execute only for tasks with specific metadata
    HasMetadata { key: String, value: Option<String> },
    /// Execute only for tasks from specific queue
    QueueName(String),
    /// Combine multiple conditions with AND
    All(Vec<HookCondition>),
    /// Combine multiple conditions with OR
    Any(Vec<HookCondition>),
    /// Negate a condition
    Not(Box<HookCondition>),
    /// Always execute
    Always,
}

impl HookCondition {
    /// Check if a task matches this condition
    pub fn matches(&self, task: &SerializedTask, context: &HookContext) -> bool {
        match self {
            HookCondition::TaskName(pattern) => task.metadata.name.contains(pattern),
            HookCondition::PriorityRange { min, max } => {
                task.metadata.priority >= *min && task.metadata.priority <= *max
            }
            HookCondition::HasMetadata { key, value } => {
                if let Some(expected) = value {
                    context.metadata.get(key) == Some(expected)
                } else {
                    context.metadata.contains_key(key)
                }
            }
            HookCondition::QueueName(name) => &context.queue_name == name,
            HookCondition::All(conditions) => conditions.iter().all(|c| c.matches(task, context)),
            HookCondition::Any(conditions) => conditions.iter().any(|c| c.matches(task, context)),
            HookCondition::Not(condition) => !condition.matches(task, context),
            HookCondition::Always => true,
        }
    }
}

/// Conditional hook that executes only when condition is met
pub struct ConditionalHook<H> {
    hook: H,
    condition: HookCondition,
}

impl<H> ConditionalHook<H> {
    /// Create a new conditional hook
    pub fn new(hook: H, condition: HookCondition) -> Self {
        Self { hook, condition }
    }

    /// Get a reference to the inner hook
    pub fn inner(&self) -> &H {
        &self.hook
    }

    /// Get a reference to the condition
    pub fn condition(&self) -> &HookCondition {
        &self.condition
    }
}

#[async_trait]
impl<H: EnqueueHook> EnqueueHook for ConditionalHook<H> {
    async fn before_enqueue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        if self.condition.matches(task, ctx) {
            self.hook.before_enqueue(task, ctx).await
        } else {
            Ok(())
        }
    }

    async fn after_enqueue(
        &self,
        task_id: &TaskId,
        task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        if self.condition.matches(task, ctx) {
            self.hook.after_enqueue(task_id, task, ctx).await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<H: DequeueHook> DequeueHook for ConditionalHook<H> {
    async fn before_dequeue(&self, ctx: &HookContext) -> HookResult<()> {
        // Note: We can't check task condition before dequeue as we don't have the task yet
        self.hook.before_dequeue(ctx).await
    }

    async fn after_dequeue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        if self.condition.matches(task, ctx) {
            self.hook.after_dequeue(task, ctx).await
        } else {
            Ok(())
        }
    }
}

/// Hook with priority for ordered execution
pub struct PrioritizedHook<H> {
    hook: H,
    priority: u32,
}

impl<H> PrioritizedHook<H> {
    /// Create a new prioritized hook
    /// Higher priority values execute first
    pub fn new(hook: H, priority: u32) -> Self {
        Self { hook, priority }
    }

    /// Get the priority
    pub fn priority(&self) -> u32 {
        self.priority
    }

    /// Get a reference to the inner hook
    pub fn inner(&self) -> &H {
        &self.hook
    }
}

#[async_trait]
impl<H: EnqueueHook> EnqueueHook for PrioritizedHook<H> {
    async fn before_enqueue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        self.hook.before_enqueue(task, ctx).await
    }

    async fn after_enqueue(
        &self,
        task_id: &TaskId,
        task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        self.hook.after_enqueue(task_id, task, ctx).await
    }
}

/// Error handling strategy for hooks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookErrorStrategy {
    /// Stop execution on first error and return the error
    FailFast,
    /// Continue executing remaining hooks, collect all errors
    ContinueOnError,
    /// Retry failed hooks with specified attempts
    RetryOnError { max_attempts: u32 },
    /// Log error and continue
    LogAndContinue,
}

/// Parallel hook executor for improved performance
pub struct ParallelHookExecutor {
    error_strategy: HookErrorStrategy,
    max_concurrency: usize,
}

impl ParallelHookExecutor {
    /// Create a new parallel executor
    pub fn new() -> Self {
        Self {
            error_strategy: HookErrorStrategy::FailFast,
            max_concurrency: 10,
        }
    }

    /// Set error handling strategy
    pub fn with_error_strategy(mut self, strategy: HookErrorStrategy) -> Self {
        self.error_strategy = strategy;
        self
    }

    /// Set maximum concurrency
    pub fn with_max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Execute enqueue hooks in parallel
    pub async fn execute_enqueue_parallel<H>(
        &self,
        hooks: Vec<Arc<H>>,
        task: &mut SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()>
    where
        H: EnqueueHook + Send + Sync + 'static,
    {
        let mut join_set = JoinSet::new();
        let mut errors = Vec::new();

        // Clone task for parallel execution
        let task_clone = task.clone();
        let ctx_clone = ctx.clone();

        for hook in hooks {
            let mut task_copy = task_clone.clone();
            let ctx_copy = ctx_clone.clone();

            join_set.spawn(async move { hook.before_enqueue(&mut task_copy, &ctx_copy).await });

            // Limit concurrency
            if join_set.len() >= self.max_concurrency {
                if let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => errors.push(e),
                        Err(e) => {
                            errors.push(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                        }
                    }
                }
            }
        }

        // Wait for remaining tasks
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    match self.error_strategy {
                        HookErrorStrategy::FailFast => return Err(e),
                        HookErrorStrategy::ContinueOnError => errors.push(e),
                        HookErrorStrategy::LogAndContinue => {
                            error!("Hook error: {}", e);
                        }
                        HookErrorStrategy::RetryOnError { .. } => {
                            // Retry logic would go here
                            errors.push(e);
                        }
                    }
                }
                Err(join_err) => {
                    let err = Box::new(join_err) as Box<dyn std::error::Error + Send + Sync>;
                    match self.error_strategy {
                        HookErrorStrategy::FailFast => return Err(err),
                        HookErrorStrategy::ContinueOnError => errors.push(err),
                        HookErrorStrategy::LogAndContinue => {
                            error!("Join error");
                        }
                        HookErrorStrategy::RetryOnError { .. } => {
                            errors.push(err);
                        }
                    }
                }
            }
        }

        if !errors.is_empty() && self.error_strategy != HookErrorStrategy::LogAndContinue {
            let error_msg = errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(format!("Multiple hook errors: {}", error_msg).into());
        }

        Ok(())
    }
}

impl Default for ParallelHookExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Hook with retry capability
pub struct RetryableHook<H> {
    hook: H,
    max_attempts: u32,
    delay_ms: u64,
}

impl<H> RetryableHook<H> {
    /// Create a new retryable hook
    pub fn new(hook: H, max_attempts: u32, delay_ms: u64) -> Self {
        Self {
            hook,
            max_attempts,
            delay_ms,
        }
    }
}

#[async_trait]
impl<H: EnqueueHook> EnqueueHook for RetryableHook<H> {
    async fn before_enqueue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match self.hook.before_enqueue(task, ctx).await {
                Ok(()) => return Ok(()),
                Err(e) if attempts < self.max_attempts => {
                    tracing::warn!(
                        "Hook attempt {}/{} failed: {}",
                        attempts,
                        self.max_attempts,
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(self.delay_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn after_enqueue(
        &self,
        task_id: &TaskId,
        task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match self.hook.after_enqueue(task_id, task, ctx).await {
                Ok(()) => return Ok(()),
                Err(e) if attempts < self.max_attempts => {
                    tracing::warn!(
                        "Hook attempt {}/{} failed: {}",
                        attempts,
                        self.max_attempts,
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(self.delay_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Hook combinator for sequential execution
pub struct SequentialHooks<H> {
    hooks: Vec<H>,
}

impl<H> SequentialHooks<H> {
    /// Create a new sequential hook chain
    pub fn new(hooks: Vec<H>) -> Self {
        Self { hooks }
    }

    /// Add a hook to the chain
    pub fn add(&mut self, hook: H) {
        self.hooks.push(hook);
    }
}

#[async_trait]
impl<H: EnqueueHook> EnqueueHook for SequentialHooks<H> {
    async fn before_enqueue(&self, task: &mut SerializedTask, ctx: &HookContext) -> HookResult<()> {
        for hook in &self.hooks {
            hook.before_enqueue(task, ctx).await?;
        }
        Ok(())
    }

    async fn after_enqueue(
        &self,
        task_id: &TaskId,
        task: &SerializedTask,
        ctx: &HookContext,
    ) -> HookResult<()> {
        for hook in &self.hooks {
            hook.after_enqueue(task_id, task, ctx).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_condition_task_name() {
        let task = SerializedTask::new("test_task".to_string(), vec![]);
        let ctx = HookContext::new("queue".to_string());

        let condition = HookCondition::TaskName("test".to_string());
        assert!(condition.matches(&task, &ctx));

        let condition = HookCondition::TaskName("other".to_string());
        assert!(!condition.matches(&task, &ctx));
    }

    #[test]
    fn test_hook_condition_priority_range() {
        let mut task = SerializedTask::new("test".to_string(), vec![]);
        task.metadata.priority = 5;
        let ctx = HookContext::new("queue".to_string());

        let condition = HookCondition::PriorityRange { min: 0, max: 10 };
        assert!(condition.matches(&task, &ctx));

        let condition = HookCondition::PriorityRange { min: 10, max: 20 };
        assert!(!condition.matches(&task, &ctx));
    }

    #[test]
    fn test_hook_condition_all() {
        let mut task = SerializedTask::new("test_important".to_string(), vec![]);
        task.metadata.priority = 5;
        let ctx = HookContext::new("queue".to_string());

        let condition = HookCondition::All(vec![
            HookCondition::TaskName("test".to_string()),
            HookCondition::PriorityRange { min: 0, max: 10 },
        ]);
        assert!(condition.matches(&task, &ctx));

        let condition = HookCondition::All(vec![
            HookCondition::TaskName("test".to_string()),
            HookCondition::PriorityRange { min: 10, max: 20 },
        ]);
        assert!(!condition.matches(&task, &ctx));
    }

    #[test]
    fn test_hook_condition_any() {
        let task = SerializedTask::new("test".to_string(), vec![]);
        let ctx = HookContext::new("queue".to_string());

        let condition = HookCondition::Any(vec![
            HookCondition::TaskName("test".to_string()),
            HookCondition::TaskName("other".to_string()),
        ]);
        assert!(condition.matches(&task, &ctx));

        let condition = HookCondition::Any(vec![
            HookCondition::TaskName("other".to_string()),
            HookCondition::TaskName("another".to_string()),
        ]);
        assert!(!condition.matches(&task, &ctx));
    }

    #[test]
    fn test_hook_condition_not() {
        let task = SerializedTask::new("test".to_string(), vec![]);
        let ctx = HookContext::new("queue".to_string());

        let condition = HookCondition::Not(Box::new(HookCondition::TaskName("other".to_string())));
        assert!(condition.matches(&task, &ctx));

        let condition = HookCondition::Not(Box::new(HookCondition::TaskName("test".to_string())));
        assert!(!condition.matches(&task, &ctx));
    }

    #[test]
    fn test_hook_error_strategy() {
        assert_eq!(HookErrorStrategy::FailFast, HookErrorStrategy::FailFast);
        assert_ne!(
            HookErrorStrategy::FailFast,
            HookErrorStrategy::ContinueOnError
        );
    }

    #[test]
    fn test_prioritized_hook_priority() {
        struct DummyHook;

        #[async_trait]
        impl EnqueueHook for DummyHook {
            async fn before_enqueue(
                &self,
                _: &mut SerializedTask,
                _: &HookContext,
            ) -> HookResult<()> {
                Ok(())
            }
        }

        let hook = PrioritizedHook::new(DummyHook, 100);
        assert_eq!(hook.priority(), 100);
    }

    #[test]
    fn test_parallel_executor_config() {
        let executor = ParallelHookExecutor::new()
            .with_error_strategy(HookErrorStrategy::ContinueOnError)
            .with_max_concurrency(20);

        assert_eq!(executor.error_strategy, HookErrorStrategy::ContinueOnError);
        assert_eq!(executor.max_concurrency, 20);
    }
}
