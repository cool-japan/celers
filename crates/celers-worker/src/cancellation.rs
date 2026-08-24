//! Task cancellation support
//!
//! Provides cooperative cancellation for long-running tasks using cancellation tokens.
//! Tasks can check for cancellation requests and gracefully terminate.
//!
//! # Cancellation Model
//!
//! - **Cooperative**: Tasks must explicitly check for cancellation
//! - **Graceful**: Tasks can perform cleanup before terminating
//! - **Hierarchical**: Parent cancellation propagates to children
//!
//! # Example
//!
//! ```rust
//! use celers_worker::cancellation::{CancellationToken, CancellationRegistry};
//!
//! # async fn example() {
//! let registry = CancellationRegistry::new();
//! let task_id = uuid::Uuid::new_v4();
//!
//! // Create a token for the task
//! let token = registry.create_token(task_id).await;
//!
//! // In the task execution
//! let token_clone = token.clone();
//! tokio::spawn(async move {
//!     loop {
//!         // Check for cancellation
//!         if token_clone.is_cancelled() {
//!             println!("Task cancelled, cleaning up...");
//!             break;
//!         }
//!
//!         // Do work...
//!         tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
//!     }
//! });
//!
//! // Cancel the task from elsewhere
//! registry.cancel(&task_id).await;
//! # }
//! ```

use celers_core::TaskId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, info};

/// Cancellation token for cooperative task cancellation
///
/// Cloning is cheap: clones share the same underlying cancellation flag and
/// notification primitive, so cancelling any clone is observed by all of them.
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
    task_id: TaskId,
}

impl CancellationToken {
    /// Create a new cancellation token
    pub fn new(task_id: TaskId) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            task_id,
        }
    }

    /// Check if cancellation has been requested
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Request cancellation
    ///
    /// Idempotent: calling this multiple times has the same effect as calling it
    /// once. All current and future waiters on [`cancelled`](Self::cancelled) are
    /// woken.
    pub fn cancel(&self) {
        // `swap` returns the previous value; only log/notify on the first trip so
        // repeated cancellations stay quiet but still wake any late waiters.
        let was_cancelled = self.cancelled.swap(true, Ordering::Release);
        if !was_cancelled {
            debug!("Cancellation requested for task {}", self.task_id);
        }
        // `notify_waiters` only wakes *currently registered* waiters, so combine
        // it with the flag check inside `cancelled()` to avoid lost wakeups.
        self.notify.notify_waiters();
    }

    /// Get the task ID
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Wait for cancellation signal
    ///
    /// This is an async method that completes when cancellation is requested.
    /// Useful for tasks that want to await cancellation rather than polling, e.g.
    /// inside a [`tokio::select!`] alongside the task's own work.
    ///
    /// The implementation is notification-based (no busy polling): it returns
    /// immediately if cancellation already happened, otherwise it parks until
    /// [`cancel`](Self::cancel) wakes it.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            // Register interest *before* re-checking the flag to close the
            // race where `cancel()` fires between the check and the await.
            //
            // `Notify::notified()` only *constructs* the `Notified` future;
            // tokio does not add it to the notify's waiter list until the
            // future is first polled. A plain `notified.await` here would
            // therefore leave a real window on a multi-threaded runtime:
            // another thread could run `cancel()` -- which does
            // `swap(true)` then `notify_waiters()` -- entirely between the
            // check below and the first poll of `notified`, and
            // `notify_waiters()` only wakes *already-registered* waiters,
            // so that cancellation would be silently lost and this task
            // would park forever.
            //
            // Pinning the future and calling `enable()` forces the waiter
            // registration to happen synchronously, right now, before the
            // flag is re-checked -- closing the window: either `cancel()`
            // already ran (and the check below observes it), or it runs
            // after this point (and we are already registered to be woken
            // by it).
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }

    /// Check cancellation and return an error if cancelled
    pub fn check_cancelled(&self) -> Result<(), CancellationError> {
        if self.is_cancelled() {
            Err(CancellationError::Cancelled(self.task_id))
        } else {
            Ok(())
        }
    }
}

/// Cancellation error
#[derive(Debug, thiserror::Error)]
pub enum CancellationError {
    #[error("Task {0} was cancelled")]
    Cancelled(TaskId),
}

/// Registry for managing cancellation tokens
pub struct CancellationRegistry {
    tokens: Arc<RwLock<HashMap<TaskId, CancellationToken>>>,
}

impl CancellationRegistry {
    /// Create a new cancellation registry
    pub fn new() -> Self {
        Self {
            tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a cancellation token for a task
    ///
    /// Returns a token that can be used to check for cancellation.
    /// The token is automatically registered in the registry.
    pub async fn create_token(&self, task_id: TaskId) -> CancellationToken {
        let token = CancellationToken::new(task_id);
        let mut tokens = self.tokens.write().await;
        tokens.insert(task_id, token.clone());
        debug!("Created cancellation token for task {}", task_id);
        token
    }

    /// Get an existing cancellation token
    pub async fn get_token(&self, task_id: &TaskId) -> Option<CancellationToken> {
        let tokens = self.tokens.read().await;
        tokens.get(task_id).cloned()
    }

    /// Cancel a task by ID
    ///
    /// Returns true if the task was found and cancelled, false otherwise.
    pub async fn cancel(&self, task_id: &TaskId) -> bool {
        let tokens = self.tokens.read().await;
        if let Some(token) = tokens.get(task_id) {
            token.cancel();
            info!("Cancelled task {}", task_id);
            true
        } else {
            debug!("Task {} not found in cancellation registry", task_id);
            false
        }
    }

    /// Cancel all tasks
    pub async fn cancel_all(&self) {
        let tokens = self.tokens.read().await;
        let count = tokens.len();
        for (task_id, token) in tokens.iter() {
            token.cancel();
            debug!("Cancelled task {}", task_id);
        }
        info!("Cancelled {} tasks", count);
    }

    /// Remove a token from the registry (cleanup)
    pub async fn remove_token(&self, task_id: &TaskId) {
        let mut tokens = self.tokens.write().await;
        tokens.remove(task_id);
        debug!("Removed cancellation token for task {}", task_id);
    }

    /// Get the number of registered tokens
    pub async fn token_count(&self) -> usize {
        let tokens = self.tokens.read().await;
        tokens.len()
    }

    /// Check if a task is registered
    pub async fn has_token(&self, task_id: &TaskId) -> bool {
        let tokens = self.tokens.read().await;
        tokens.contains_key(task_id)
    }

    /// Get all task IDs with active tokens
    pub async fn get_all_task_ids(&self) -> Vec<TaskId> {
        let tokens = self.tokens.read().await;
        tokens.keys().copied().collect()
    }

    /// Get all cancelled task IDs
    pub async fn get_cancelled_task_ids(&self) -> Vec<TaskId> {
        let tokens = self.tokens.read().await;
        tokens
            .iter()
            .filter(|(_, token)| token.is_cancelled())
            .map(|(id, _)| *id)
            .collect()
    }
}

impl Default for CancellationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CancellationRegistry {
    fn clone(&self) -> Self {
        Self {
            tokens: Arc::clone(&self.tokens),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancellation_token_new() {
        let task_id = uuid::Uuid::new_v4();
        let token = CancellationToken::new(task_id);
        assert!(!token.is_cancelled());
        assert_eq!(token.task_id(), task_id);
    }

    #[test]
    fn test_cancellation_token_cancel() {
        let task_id = uuid::Uuid::new_v4();
        let token = CancellationToken::new(task_id);

        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_cancellation_token_check_cancelled() {
        let task_id = uuid::Uuid::new_v4();
        let token = CancellationToken::new(task_id);

        assert!(token.check_cancelled().is_ok());
        token.cancel();
        assert!(token.check_cancelled().is_err());
    }

    #[tokio::test]
    async fn test_cancellation_registry_create_token() {
        let registry = CancellationRegistry::new();
        let task_id = uuid::Uuid::new_v4();

        let token = registry.create_token(task_id).await;
        assert_eq!(token.task_id(), task_id);
        assert!(!token.is_cancelled());
        assert_eq!(registry.token_count().await, 1);
    }

    #[tokio::test]
    async fn test_cancellation_registry_get_token() {
        let registry = CancellationRegistry::new();
        let task_id = uuid::Uuid::new_v4();

        registry.create_token(task_id).await;
        let token = registry.get_token(&task_id).await;
        assert!(token.is_some());
        assert_eq!(token.unwrap().task_id(), task_id);
    }

    #[tokio::test]
    async fn test_cancellation_registry_cancel() {
        let registry = CancellationRegistry::new();
        let task_id = uuid::Uuid::new_v4();

        let token = registry.create_token(task_id).await;
        assert!(!token.is_cancelled());

        assert!(registry.cancel(&task_id).await);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_registry_cancel_nonexistent() {
        let registry = CancellationRegistry::new();
        let task_id = uuid::Uuid::new_v4();

        assert!(!registry.cancel(&task_id).await);
    }

    #[tokio::test]
    async fn test_cancellation_registry_cancel_all() {
        let registry = CancellationRegistry::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        let token1 = registry.create_token(task1).await;
        let token2 = registry.create_token(task2).await;

        assert!(!token1.is_cancelled());
        assert!(!token2.is_cancelled());

        registry.cancel_all().await;

        assert!(token1.is_cancelled());
        assert!(token2.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_registry_remove_token() {
        let registry = CancellationRegistry::new();
        let task_id = uuid::Uuid::new_v4();

        registry.create_token(task_id).await;
        assert_eq!(registry.token_count().await, 1);

        registry.remove_token(&task_id).await;
        assert_eq!(registry.token_count().await, 0);
    }

    #[tokio::test]
    async fn test_cancellation_registry_has_token() {
        let registry = CancellationRegistry::new();
        let task_id = uuid::Uuid::new_v4();

        assert!(!registry.has_token(&task_id).await);
        registry.create_token(task_id).await;
        assert!(registry.has_token(&task_id).await);
    }

    #[tokio::test]
    async fn test_cancellation_registry_get_all_task_ids() {
        let registry = CancellationRegistry::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        registry.create_token(task1).await;
        registry.create_token(task2).await;

        let ids = registry.get_all_task_ids().await;
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&task1));
        assert!(ids.contains(&task2));
    }

    #[tokio::test]
    async fn test_cancellation_registry_get_cancelled_task_ids() {
        let registry = CancellationRegistry::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        registry.create_token(task1).await;
        registry.create_token(task2).await;

        registry.cancel(&task1).await;

        let cancelled = registry.get_cancelled_task_ids().await;
        assert_eq!(cancelled.len(), 1);
        assert!(cancelled.contains(&task1));
        assert!(!cancelled.contains(&task2));
    }

    #[tokio::test]
    async fn test_cancellation_token_clone() {
        let task_id = uuid::Uuid::new_v4();
        let token1 = CancellationToken::new(task_id);
        let token2 = token1.clone();

        token1.cancel();
        assert!(token2.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancelled_returns_immediately_when_already_cancelled() {
        let token = CancellationToken::new(uuid::Uuid::new_v4());
        token.cancel();
        // Must complete promptly (notification-based, not a 100ms poll).
        tokio::time::timeout(std::time::Duration::from_millis(50), token.cancelled())
            .await
            .expect("cancelled() should return immediately when already cancelled");
    }

    #[tokio::test]
    async fn test_cancelled_wakes_on_cancel() {
        let token = CancellationToken::new(uuid::Uuid::new_v4());
        let waiter = token.clone();
        let handle = tokio::spawn(async move {
            waiter.cancelled().await;
        });
        // Give the waiter a moment to park on `notified()`.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(200), handle)
            .await
            .expect("waiter should wake quickly after cancel")
            .expect("waiter task should not panic");
    }

    #[test]
    fn test_cancel_is_idempotent() {
        let token = CancellationToken::new(uuid::Uuid::new_v4());
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    /// Regression test for the lost-wakeup race: `Notify::notified()` only
    /// *constructs* the future -- the waiter is not registered until the
    /// future is first polled (or explicitly `enable()`d). Without pinning
    /// and enabling before the flag re-check, a `cancel()` racing in on
    /// another OS thread between the re-check and the first poll can be
    /// missed entirely, leaving the waiter parked forever.
    ///
    /// This is only reachable under genuine thread-level parallelism (a
    /// single-threaded/cooperative scheduler can never interleave between
    /// two back-to-back synchronous statements), so the test runs on a
    /// multi-thread runtime and races `cancel()` against a freshly spawned
    /// waiter with no synchronizing delay, many times over, so that
    /// natural OS scheduling jitter spreads across the tiny window. Each
    /// iteration is bounded by a timeout used only as a hang detector (not
    /// as a correctness signal): with the fix, every iteration must
    /// resolve promptly and deterministically; the pre-fix lost-wakeup bug
    /// would eventually strand at least one iteration and trip it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cancelled_no_lost_wakeup_under_tight_race() {
        for _ in 0..500 {
            let token = CancellationToken::new(uuid::Uuid::new_v4());
            let waiter = token.clone();
            let handle = tokio::spawn(async move {
                waiter.cancelled().await;
            });

            // No sleep: race `cancel()` against the freshly spawned waiter
            // as tightly as possible instead of giving it time to park
            // first (which is what the other, non-racing test above does).
            token.cancel();

            tokio::time::timeout(std::time::Duration::from_secs(2), handle)
                .await
                .expect("lost wakeup: cancelled() never woke up after cancel()")
                .expect("waiter task should not panic");
        }
    }
}
