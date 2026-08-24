//! Task-level timeout management with cleanup hooks
//!
//! This module provides fine-grained timeout control for individual tasks
//! with support for cleanup hooks that execute when a timeout occurs.
//!
//! # Features
//!
//! - Per-task timeout configuration
//! - Cleanup hooks for resource cleanup on timeout
//! - Graceful vs forceful timeout strategies
//! - Timeout context with task metadata
//! - Multiple cleanup handlers per task
//!
//! # Timeout strategies
//!
//! Both strategies *stop* the task; they differ in how much warning it gets.
//!
//! - [`TimeoutStrategy::Graceful`] (default) trips the task's
//!   [`CancellationToken`] so cooperative code can unwind, waits up to
//!   [`TimeoutConfig::grace_period`], and only then aborts the task's
//!   [`JoinHandle`]. A task that ignores cancellation is still killed — a
//!   timeout that leaves the work running would defeat the whole point of
//!   bounding task runtime.
//! - [`TimeoutStrategy::Forceful`] aborts immediately, without the grace
//!   window.
//!
//! Aborting only works for tasks registered with their [`JoinHandle`] (see
//! [`TimeoutManager::register_task_with_handle`]); a task registered without
//! one can only be signalled through its cancellation token.
//!
//! # Example
//!
//! ```
//! use celers_worker::{TimeoutConfig, TimeoutManager, CleanupHook};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = TimeoutConfig::new(Duration::from_secs(30))
//!     .with_grace_period(Duration::from_secs(5));
//!
//! let mut manager = TimeoutManager::new(config);
//!
//! // Add cleanup hook
//! manager.add_cleanup_hook(CleanupHook::new(|ctx| {
//!     Box::pin(async move {
//!         println!("Cleaning up task: {}", ctx.task_id);
//!         Ok(())
//!     })
//! }));
//! # }
//! ```

use crate::cancellation::CancellationToken;
use celers_core::TaskId;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// Cleanup function type
pub type CleanupFn = Arc<
    dyn Fn(TimeoutContext) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Context passed to cleanup hooks when a timeout occurs
#[derive(Clone, Debug)]
pub struct TimeoutContext {
    /// Task ID
    pub task_id: String,
    /// Task name/type
    pub task_name: String,
    /// When the task started
    pub started_at: Instant,
    /// Configured timeout duration
    pub timeout_duration: Duration,
    /// Time elapsed when timeout occurred
    pub elapsed: Duration,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl TimeoutContext {
    /// Create a new timeout context
    pub fn new(task_id: String, task_name: String, timeout_duration: Duration) -> Self {
        Self {
            task_id,
            task_name,
            started_at: Instant::now(),
            timeout_duration,
            elapsed: Duration::ZERO,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the context
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Update elapsed time
    pub fn update_elapsed(&mut self) {
        self.elapsed = self.started_at.elapsed();
    }

    /// Check if the task has exceeded the timeout
    pub fn is_timed_out(&self) -> bool {
        self.started_at.elapsed() >= self.timeout_duration
    }

    /// Get remaining time before timeout
    pub fn remaining(&self) -> Duration {
        self.timeout_duration
            .saturating_sub(self.started_at.elapsed())
    }
}

/// Cleanup hook that executes when a task times out
pub struct CleanupHook {
    /// Hook name for logging/debugging
    pub name: String,
    /// Cleanup function
    pub func: CleanupFn,
}

impl CleanupHook {
    /// Create a new cleanup hook with a default name
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: Fn(TimeoutContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        Self::with_name("cleanup_hook", func)
    }

    /// Create a new cleanup hook with a specific name
    pub fn with_name<F, Fut>(name: impl Into<String>, func: F) -> Self
    where
        F: Fn(TimeoutContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let func = Arc::new(move |ctx: TimeoutContext| Box::pin(func(ctx)) as _);
        Self {
            name: name.into(),
            func,
        }
    }

    /// Execute the cleanup hook
    pub async fn execute(&self, ctx: TimeoutContext) -> Result<(), String> {
        debug!(
            "Executing cleanup hook '{}' for task {}",
            self.name, ctx.task_id
        );
        match (self.func)(ctx.clone()).await {
            Ok(()) => {
                info!("Cleanup hook '{}' completed successfully", self.name);
                Ok(())
            }
            Err(e) => {
                warn!("Cleanup hook '{}' failed: {}", self.name, e);
                Err(e)
            }
        }
    }
}

/// Timeout strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TimeoutStrategy {
    /// Graceful timeout - allow cleanup hooks to run
    #[default]
    Graceful,
    /// Forceful timeout - immediately cancel the task
    Forceful,
}

/// Timeout configuration
#[derive(Clone)]
pub struct TimeoutConfig {
    /// Default timeout duration
    pub default_timeout: Duration,
    /// Grace period for cleanup after timeout
    pub grace_period: Duration,
    /// Timeout strategy
    pub strategy: TimeoutStrategy,
    /// Enable timeout monitoring
    pub enabled: bool,
}

impl TimeoutConfig {
    /// Create a new timeout configuration
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            default_timeout,
            grace_period: Duration::from_secs(5),
            strategy: TimeoutStrategy::default(),
            enabled: true,
        }
    }

    /// Set the grace period for cleanup
    pub fn with_grace_period(mut self, grace_period: Duration) -> Self {
        self.grace_period = grace_period;
        self
    }

    /// Set the timeout strategy
    pub fn with_strategy(mut self, strategy: TimeoutStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Enable or disable timeout monitoring
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.default_timeout.is_zero() {
            return Err("Default timeout must be greater than zero".to_string());
        }
        Ok(())
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }
}

/// Task handle entry for tracking executing tasks
struct TaskHandleEntry {
    /// Task context
    context: TimeoutContext,
    /// Task join handle for forced termination
    handle: Option<JoinHandle<()>>,
    /// Cooperative cancellation signal handed to the running task.
    ///
    /// Tripped before any abort so that a task which polls
    /// [`CancellationToken::is_cancelled`] (or awaits
    /// [`CancellationToken::cancelled`]) can unwind and release resources
    /// during the grace period.
    cancel: CancellationToken,
}

/// Timeout manager that tracks task timeouts and executes cleanup hooks
pub struct TimeoutManager {
    /// Configuration
    config: TimeoutConfig,
    /// Cleanup hooks
    cleanup_hooks: Vec<CleanupHook>,
    /// Active task contexts and handles
    active_tasks: Arc<RwLock<HashMap<String, TaskHandleEntry>>>,
}

impl TimeoutManager {
    /// Create a new timeout manager
    pub fn new(config: TimeoutConfig) -> Self {
        Self {
            config,
            cleanup_hooks: Vec::new(),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a cleanup hook
    pub fn add_cleanup_hook(&mut self, hook: CleanupHook) {
        self.cleanup_hooks.push(hook);
    }

    /// Register a task for timeout monitoring
    ///
    /// Returns the task's [`TimeoutContext`]; use
    /// [`TimeoutManager::cancellation_token`] to obtain the cooperative
    /// cancellation signal that a timeout will trip.
    pub async fn register_task(
        &self,
        task_id: String,
        task_name: String,
        timeout: Option<Duration>,
    ) -> TimeoutContext {
        self.register_task_with_handle(task_id, task_name, timeout, None)
            .await
    }

    /// Register a task for timeout monitoring with an optional task handle
    ///
    /// The task handle is what makes a timeout enforceable: without one, a
    /// task that ignores its cancellation token cannot be stopped.
    pub async fn register_task_with_handle(
        &self,
        task_id: String,
        task_name: String,
        timeout: Option<Duration>,
        handle: Option<JoinHandle<()>>,
    ) -> TimeoutContext {
        let (ctx, _token) = self
            .register_task_with_cancellation(task_id, task_name, timeout, handle, None)
            .await;
        ctx
    }

    /// Register a task, supplying (or receiving) its cancellation token.
    ///
    /// Pass `token` when the task already owns one — typically the token from
    /// [`crate::cancellation::CancellationRegistry`] used for broker-driven
    /// revocation — so that a timeout and a revocation trip the *same*
    /// signal. Pass `None` to have the manager mint one; either way the token
    /// in use is returned so the caller can hand it to the running task.
    ///
    /// When [`TimeoutConfig::enabled`] is `false` the task is not tracked at
    /// all, and the returned token is never tripped by this manager.
    pub async fn register_task_with_cancellation(
        &self,
        task_id: String,
        task_name: String,
        timeout: Option<Duration>,
        handle: Option<JoinHandle<()>>,
        token: Option<CancellationToken>,
    ) -> (TimeoutContext, CancellationToken) {
        let timeout_duration = timeout.unwrap_or(self.config.default_timeout);
        let ctx = TimeoutContext::new(task_id.clone(), task_name, timeout_duration);
        let token = token.unwrap_or_else(|| CancellationToken::new(uuid_for(&task_id)));

        if !self.config.enabled {
            debug!(
                "Timeout monitoring is disabled; not tracking task {}",
                task_id
            );
            return (ctx, token);
        }

        let mut tasks = self.active_tasks.write().await;
        tasks.insert(
            task_id,
            TaskHandleEntry {
                context: ctx.clone(),
                handle,
                cancel: token.clone(),
            },
        );

        (ctx, token)
    }

    /// Get the cancellation token associated with a tracked task.
    pub async fn cancellation_token(&self, task_id: &str) -> Option<CancellationToken> {
        self.active_tasks
            .read()
            .await
            .get(task_id)
            .map(|entry| entry.cancel.clone())
    }

    /// Unregister a task (called when task completes successfully)
    pub async fn unregister_task(&self, task_id: &str) {
        let mut tasks = self.active_tasks.write().await;
        tasks.remove(task_id);
    }

    /// Handle a task timeout: signal, escalate, and run cleanup hooks.
    ///
    /// The escalation depends on [`TimeoutConfig::strategy`]:
    ///
    /// * [`TimeoutStrategy::Graceful`] trips the cancellation token, waits up
    ///   to [`TimeoutConfig::grace_period`] for the task to finish on its own
    ///   and aborts it if it does not.
    /// * [`TimeoutStrategy::Forceful`] trips the token and aborts
    ///   immediately.
    ///
    /// Cleanup hooks run afterwards in both cases. Returns `Ok(())` without
    /// doing anything when [`TimeoutConfig::enabled`] is `false`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the task is not tracked, or if any cleanup hook
    /// failed (the escalation still happened in that case).
    pub async fn handle_timeout(&self, task_id: &str) -> Result<(), String> {
        if !self.config.enabled {
            debug!(
                "Timeout monitoring is disabled; ignoring timeout for task {}",
                task_id
            );
            return Ok(());
        }

        // Get the task context and handle
        let mut tasks = self.active_tasks.write().await;
        let entry = match tasks.remove(task_id) {
            Some(entry) => entry,
            None => {
                warn!("Task {} not found in timeout manager", task_id);
                return Err(format!("Task {} not found", task_id));
            }
        };

        let mut ctx = entry.context;
        ctx.update_elapsed();
        let handle = entry.handle;
        let cancel = entry.cancel;

        // Drop the write lock before executing hooks
        drop(tasks);

        info!(
            "Task {} timed out after {:?} (strategy: {:?})",
            task_id, ctx.elapsed, self.config.strategy
        );

        // Always signal cooperative cancellation first: a task that polls its
        // token can unwind cleanly instead of being killed mid-write.
        cancel.cancel();

        match self.config.strategy {
            TimeoutStrategy::Forceful => match handle {
                Some(handle) => {
                    warn!("Forcefully terminating task {}", task_id);
                    handle.abort();
                    info!("Task {} forcefully terminated", task_id);
                }
                None => warn!(
                    "Forceful termination requested for task {} but no handle available; \
                     the task was signalled through its cancellation token only",
                    task_id
                ),
            },
            TimeoutStrategy::Graceful => match handle {
                Some(mut handle) => {
                    let grace = self.config.grace_period;
                    debug!(
                        "Waiting up to {:?} for task {} to stop cooperatively",
                        grace, task_id
                    );
                    // `JoinHandle` is a `Unpin` future, so awaiting `&mut
                    // handle` under a timeout gives the task its grace window
                    // *without* consuming the handle -- consuming it would
                    // drop it on expiry, and a dropped `JoinHandle` detaches
                    // the task instead of stopping it.
                    match tokio::time::timeout(grace, &mut handle).await {
                        Ok(Ok(())) => {
                            info!("Task {} stopped gracefully after cancellation", task_id);
                        }
                        Ok(Err(join_err)) => {
                            if join_err.is_cancelled() {
                                info!("Task {} was already cancelled", task_id);
                            } else {
                                warn!("Task {} panicked while stopping: {}", task_id, join_err);
                            }
                        }
                        Err(_) => {
                            warn!(
                                "Task {} ignored cancellation for {:?}; aborting",
                                task_id, grace
                            );
                            handle.abort();
                        }
                    }
                }
                None => debug!(
                    "No join handle for task {}; relying on cooperative cancellation",
                    task_id
                ),
            },
        }

        // Execute cleanup hooks (even for forceful termination)
        info!(
            "Executing {} cleanup hooks for task {}",
            self.cleanup_hooks.len(),
            task_id
        );
        let mut errors = Vec::new();
        for hook in &self.cleanup_hooks {
            if let Err(e) = hook.execute(ctx.clone()).await {
                errors.push(format!("{}: {}", hook.name, e));
            }
        }

        if !errors.is_empty() {
            Err(format!("Cleanup hooks failed: {}", errors.join(", ")))
        } else {
            Ok(())
        }
    }

    /// Get the number of active tasks
    pub async fn active_count(&self) -> usize {
        self.active_tasks.read().await.len()
    }

    /// Get all active task IDs
    pub async fn active_task_ids(&self) -> Vec<String> {
        self.active_tasks.read().await.keys().cloned().collect()
    }

    /// Check if a task is timed out
    pub async fn is_timed_out(&self, task_id: &str) -> bool {
        let tasks = self.active_tasks.read().await;
        tasks
            .get(task_id)
            .map(|entry| entry.context.is_timed_out())
            .unwrap_or(false)
    }

    /// Start a background monitoring task that checks for timeouts
    ///
    /// Returns a join handle for the monitoring task and a cancellation function
    pub fn start_monitoring(
        self: Arc<Self>,
        check_interval: Duration,
    ) -> (JoinHandle<()>, Box<dyn Fn() + Send + Sync>) {
        let cancel_flag = Arc::new(tokio::sync::Notify::new());
        let cancel_flag_clone = Arc::clone(&cancel_flag);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.check_and_handle_timeouts().await;
                    }
                    _ = cancel_flag_clone.notified() => {
                        info!("Timeout monitoring task cancelled");
                        break;
                    }
                }
            }
        });

        let cancel_fn = Box::new(move || {
            cancel_flag.notify_one();
        });

        (handle, cancel_fn)
    }

    /// Check all active tasks and handle any that have timed out
    async fn check_and_handle_timeouts(&self) {
        if !self.config.enabled {
            return;
        }

        let timed_out_tasks: Vec<String> = {
            let tasks = self.active_tasks.read().await;
            tasks
                .iter()
                .filter(|(_, entry)| entry.context.is_timed_out())
                .map(|(task_id, _)| task_id.clone())
                .collect()
        };

        for task_id in timed_out_tasks {
            debug!("Detected timeout for task {}", task_id);
            if let Err(e) = self.handle_timeout(&task_id).await {
                error!("Failed to handle timeout for task {}: {}", task_id, e);
            }
        }
    }

    /// Get timeout configuration
    pub fn config(&self) -> &TimeoutConfig {
        &self.config
    }
}

/// Derive a [`TaskId`] for a cancellation token from the manager's string
/// task id.
///
/// Task ids are Celery-style UUID strings in practice, so they parse
/// directly; anything else (a synthetic id from a test or an embedding
/// application) is hashed into a stable identifier so that two tokens minted
/// for the same task id still carry the same [`TaskId`].
fn uuid_for(task_id: &str) -> TaskId {
    if let Ok(parsed) = TaskId::parse_str(task_id) {
        return parsed;
    }

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut high = DefaultHasher::new();
    task_id.hash(&mut high);
    let mut low = DefaultHasher::new();
    (task_id, 0xA5_u8).hash(&mut low);

    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&high.finish().to_be_bytes());
    bytes[8..].copy_from_slice(&low.finish().to_be_bytes());
    TaskId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::time::sleep;

    #[test]
    fn test_timeout_context_creation() {
        let ctx = TimeoutContext::new(
            "task-123".to_string(),
            "test_task".to_string(),
            Duration::from_secs(30),
        );

        assert_eq!(ctx.task_id, "task-123");
        assert_eq!(ctx.task_name, "test_task");
        assert_eq!(ctx.timeout_duration, Duration::from_secs(30));
        assert_eq!(ctx.elapsed, Duration::ZERO);
    }

    #[test]
    fn test_timeout_context_with_metadata() {
        let ctx = TimeoutContext::new(
            "task-123".to_string(),
            "test_task".to_string(),
            Duration::from_secs(30),
        )
        .with_metadata("worker", "worker-1")
        .with_metadata("retry_count", "3");

        assert_eq!(ctx.metadata.get("worker"), Some(&"worker-1".to_string()));
        assert_eq!(ctx.metadata.get("retry_count"), Some(&"3".to_string()));
    }

    #[tokio::test]
    async fn test_timeout_context_is_timed_out() {
        let ctx = TimeoutContext::new(
            "task-123".to_string(),
            "test_task".to_string(),
            Duration::from_millis(100),
        );

        assert!(!ctx.is_timed_out());

        sleep(Duration::from_millis(150)).await;
        assert!(ctx.is_timed_out());
    }

    #[test]
    fn test_timeout_config_default() {
        let config = TimeoutConfig::default();
        assert_eq!(config.default_timeout, Duration::from_secs(300));
        assert_eq!(config.grace_period, Duration::from_secs(5));
        assert_eq!(config.strategy, TimeoutStrategy::Graceful);
        assert!(config.enabled);
    }

    #[test]
    fn test_timeout_config_builder() {
        let config = TimeoutConfig::new(Duration::from_secs(60))
            .with_grace_period(Duration::from_secs(10))
            .with_strategy(TimeoutStrategy::Forceful)
            .enabled(false);

        assert_eq!(config.default_timeout, Duration::from_secs(60));
        assert_eq!(config.grace_period, Duration::from_secs(10));
        assert_eq!(config.strategy, TimeoutStrategy::Forceful);
        assert!(!config.enabled);
    }

    #[test]
    fn test_timeout_config_validation() {
        let config = TimeoutConfig::new(Duration::ZERO);
        assert!(config.validate().is_err());

        let config = TimeoutConfig::new(Duration::from_secs(30));
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_cleanup_hook_execution() {
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = Arc::clone(&executed);

        let hook = CleanupHook::with_name("test_hook", move |ctx: TimeoutContext| {
            let executed = Arc::clone(&executed_clone);
            async move {
                assert_eq!(ctx.task_id, "task-123");
                executed.store(true, Ordering::SeqCst);
                Ok(())
            }
        });

        let ctx = TimeoutContext::new(
            "task-123".to_string(),
            "test_task".to_string(),
            Duration::from_secs(30),
        );

        assert!(!executed.load(Ordering::SeqCst));
        hook.execute(ctx).await.unwrap();
        assert!(executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_timeout_manager_register_unregister() {
        let config = TimeoutConfig::default();
        let manager = TimeoutManager::new(config);

        assert_eq!(manager.active_count().await, 0);

        let _ctx = manager
            .register_task(
                "task-123".to_string(),
                "test_task".to_string(),
                Some(Duration::from_secs(60)),
            )
            .await;

        assert_eq!(manager.active_count().await, 1);
        assert!(manager
            .active_task_ids()
            .await
            .contains(&"task-123".to_string()));

        manager.unregister_task("task-123").await;
        assert_eq!(manager.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_timeout_manager_handle_timeout() {
        let config = TimeoutConfig::default();
        let mut manager = TimeoutManager::new(config);

        let hook_executed = Arc::new(AtomicBool::new(false));
        let hook_executed_clone = Arc::clone(&hook_executed);

        manager.add_cleanup_hook(CleanupHook::with_name("cleanup", move |_ctx| {
            let executed = Arc::clone(&hook_executed_clone);
            async move {
                executed.store(true, Ordering::SeqCst);
                Ok(())
            }
        }));

        let _ctx = manager
            .register_task(
                "task-123".to_string(),
                "test_task".to_string(),
                Some(Duration::from_secs(1)),
            )
            .await;

        assert!(!hook_executed.load(Ordering::SeqCst));

        manager.handle_timeout("task-123").await.unwrap();

        assert!(hook_executed.load(Ordering::SeqCst));
        assert_eq!(manager.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_timeout_manager_is_timed_out() {
        let config = TimeoutConfig::default();
        let manager = TimeoutManager::new(config);

        let _ctx = manager
            .register_task(
                "task-123".to_string(),
                "test_task".to_string(),
                Some(Duration::from_millis(100)),
            )
            .await;

        assert!(!manager.is_timed_out("task-123").await);

        sleep(Duration::from_millis(150)).await;
        assert!(manager.is_timed_out("task-123").await);
    }

    #[tokio::test]
    async fn test_timeout_manager_multiple_hooks() {
        let config = TimeoutConfig::default();
        let mut manager = TimeoutManager::new(config);

        let hook1_executed = Arc::new(AtomicBool::new(false));
        let hook2_executed = Arc::new(AtomicBool::new(false));

        let hook1_clone = Arc::clone(&hook1_executed);
        let hook2_clone = Arc::clone(&hook2_executed);

        manager.add_cleanup_hook(CleanupHook::with_name("hook1", move |_ctx| {
            let executed = Arc::clone(&hook1_clone);
            async move {
                executed.store(true, Ordering::SeqCst);
                Ok(())
            }
        }));

        manager.add_cleanup_hook(CleanupHook::with_name("hook2", move |_ctx| {
            let executed = Arc::clone(&hook2_clone);
            async move {
                executed.store(true, Ordering::SeqCst);
                Ok(())
            }
        }));

        let _ctx = manager
            .register_task(
                "task-123".to_string(),
                "test_task".to_string(),
                Some(Duration::from_secs(1)),
            )
            .await;

        manager.handle_timeout("task-123").await.unwrap();

        assert!(hook1_executed.load(Ordering::SeqCst));
        assert!(hook2_executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_forceful_termination() {
        let config =
            TimeoutConfig::new(Duration::from_millis(100)).with_strategy(TimeoutStrategy::Forceful);
        let manager = TimeoutManager::new(config);

        let task_running = Arc::new(AtomicBool::new(true));
        let task_running_clone = Arc::clone(&task_running);

        // Spawn a long-running task
        let handle = tokio::spawn(async move {
            while task_running_clone.load(Ordering::SeqCst) {
                sleep(Duration::from_millis(10)).await;
            }
        });

        let _ctx = manager
            .register_task_with_handle(
                "task-123".to_string(),
                "long_task".to_string(),
                Some(Duration::from_millis(100)),
                Some(handle),
            )
            .await;

        sleep(Duration::from_millis(150)).await;

        // Handle timeout should abort the task
        manager.handle_timeout("task-123").await.unwrap();

        // The task should have been aborted
        // Note: We can't directly check if the handle was aborted,
        // but we can verify the task is no longer tracked
        assert_eq!(manager.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_graceful_vs_forceful_strategy() {
        // Test graceful strategy
        let graceful_config =
            TimeoutConfig::new(Duration::from_secs(30)).with_strategy(TimeoutStrategy::Graceful);
        assert_eq!(graceful_config.strategy, TimeoutStrategy::Graceful);

        // Test forceful strategy
        let forceful_config =
            TimeoutConfig::new(Duration::from_secs(30)).with_strategy(TimeoutStrategy::Forceful);
        assert_eq!(forceful_config.strategy, TimeoutStrategy::Forceful);
    }

    #[tokio::test]
    async fn test_monitoring_task() {
        let config =
            TimeoutConfig::new(Duration::from_millis(100)).with_strategy(TimeoutStrategy::Graceful);
        let manager = Arc::new(TimeoutManager::new(config));

        // Register a task that will timeout
        let _ctx = manager
            .register_task(
                "task-123".to_string(),
                "test_task".to_string(),
                Some(Duration::from_millis(100)),
            )
            .await;

        // Start monitoring
        let (monitor_handle, cancel_fn) =
            manager.clone().start_monitoring(Duration::from_millis(50));

        // Wait for timeout to be detected
        sleep(Duration::from_millis(200)).await;

        // Task should have been handled
        assert_eq!(manager.active_count().await, 0);

        // Cancel monitoring
        cancel_fn();
        monitor_handle.await.unwrap();
    }

    // --- Regression tests for the inert Graceful strategy (idx 171) ---

    #[tokio::test(start_paused = true)]
    async fn test_graceful_timeout_aborts_uncooperative_task() {
        let config = TimeoutConfig::new(Duration::from_millis(100))
            .with_strategy(TimeoutStrategy::Graceful)
            .with_grace_period(Duration::from_secs(5));
        let manager = TimeoutManager::new(config);

        // A task that never completes and never checks its token: the grace
        // period must expire and the manager must abort it. The drop guard
        // fires when the aborted future is dropped by the runtime, which is a
        // sturdier signal than polling `is_finished`.
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropFlag(Arc::clone(&dropped));
        let handle = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });

        manager
            .register_task_with_handle(
                "task-graceful".to_string(),
                "stubborn".to_string(),
                Some(Duration::from_millis(100)),
                Some(handle),
            )
            .await;

        manager
            .handle_timeout("task-graceful")
            .await
            .expect("timeout handling should succeed");

        // Yield so the runtime can process the abort (no sleeps: the loop
        // just gives the scheduler ticks).
        for _ in 0..16 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            dropped.load(Ordering::SeqCst),
            "a Graceful timeout must still stop a task that ignores cancellation"
        );
        assert_eq!(manager.active_count().await, 0);
    }

    /// Sets its flag when dropped; used to observe task aborts without sleeps.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_graceful_timeout_lets_cooperative_task_finish() {
        let config = TimeoutConfig::new(Duration::from_millis(100))
            .with_strategy(TimeoutStrategy::Graceful)
            .with_grace_period(Duration::from_secs(5));
        let manager = TimeoutManager::new(config);

        let unwound = Arc::new(AtomicBool::new(false));
        let unwound_clone = Arc::clone(&unwound);

        let token = CancellationToken::new(uuid::Uuid::new_v4());
        let token_id = token.task_id();
        let task_token = token.clone();
        let handle = tokio::spawn(async move {
            task_token.cancelled().await;
            unwound_clone.store(true, Ordering::SeqCst);
        });

        let (_ctx, returned) = manager
            .register_task_with_cancellation(
                "task-coop".to_string(),
                "cooperative".to_string(),
                Some(Duration::from_millis(100)),
                Some(handle),
                Some(token),
            )
            .await;
        assert_eq!(
            returned.task_id(),
            token_id,
            "a caller-supplied token must be adopted, not replaced"
        );

        manager
            .handle_timeout("task-coop")
            .await
            .expect("timeout handling should succeed");

        assert!(
            unwound.load(Ordering::SeqCst),
            "the cancellation token must be tripped so the task can unwind"
        );
    }

    #[tokio::test]
    async fn test_forceful_timeout_aborts_immediately() {
        let config = TimeoutConfig::new(Duration::from_millis(100))
            .with_strategy(TimeoutStrategy::Forceful)
            .with_grace_period(Duration::from_secs(3600));
        let manager = TimeoutManager::new(config);

        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let abort_probe = handle.abort_handle();

        manager
            .register_task_with_handle(
                "task-forceful".to_string(),
                "stubborn".to_string(),
                Some(Duration::from_millis(100)),
                Some(handle),
            )
            .await;

        // Must not wait out the (deliberately huge) grace period.
        manager
            .handle_timeout("task-forceful")
            .await
            .expect("timeout handling should succeed");

        tokio::task::yield_now().await;
        assert!(abort_probe.is_finished());
    }

    #[tokio::test]
    async fn test_disabled_config_disables_tracking_and_handling() {
        let config = TimeoutConfig::new(Duration::from_millis(1)).enabled(false);
        let manager = TimeoutManager::new(config);

        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let abort_probe = handle.abort_handle();

        manager
            .register_task_with_handle(
                "task-disabled".to_string(),
                "ignored".to_string(),
                Some(Duration::from_millis(1)),
                Some(handle),
            )
            .await;

        assert_eq!(
            manager.active_count().await,
            0,
            "a disabled manager must not track tasks"
        );
        // Handling a timeout is a no-op rather than an error.
        assert!(manager.handle_timeout("task-disabled").await.is_ok());
        tokio::task::yield_now().await;
        assert!(
            !abort_probe.is_finished(),
            "a disabled manager must not abort anything"
        );

        abort_probe.abort();
    }

    #[tokio::test]
    async fn test_cancellation_token_is_exposed() {
        let manager = TimeoutManager::new(TimeoutConfig::default());
        let (_ctx, token) = manager
            .register_task_with_cancellation(
                "task-token".to_string(),
                "test".to_string(),
                None,
                None,
                None,
            )
            .await;

        let looked_up = manager
            .cancellation_token("task-token")
            .await
            .expect("token should be tracked");
        assert_eq!(looked_up.task_id(), token.task_id());
        assert!(!looked_up.is_cancelled());

        manager
            .handle_timeout("task-token")
            .await
            .expect("timeout handling should succeed");
        assert!(
            token.is_cancelled(),
            "a timeout must trip the task's cancellation token"
        );
    }

    #[test]
    fn test_uuid_for_is_stable() {
        let a = uuid_for("not-a-uuid");
        let b = uuid_for("not-a-uuid");
        assert_eq!(a, b);
        assert_ne!(a, uuid_for("other-id"));

        let real = uuid::Uuid::new_v4();
        assert_eq!(uuid_for(&real.to_string()), real);
    }

    #[tokio::test]
    async fn test_register_task_with_handle() {
        let config = TimeoutConfig::default();
        let manager = TimeoutManager::new(config);

        let handle = tokio::spawn(async {
            sleep(Duration::from_secs(10)).await;
        });

        let ctx = manager
            .register_task_with_handle(
                "task-123".to_string(),
                "test_task".to_string(),
                Some(Duration::from_secs(30)),
                Some(handle),
            )
            .await;

        assert_eq!(ctx.task_id, "task-123");
        assert_eq!(manager.active_count().await, 1);
    }
}
