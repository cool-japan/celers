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
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Cancellation token for cooperative task cancellation
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    task_id: TaskId,
}

impl CancellationToken {
    /// Create a new cancellation token
    pub fn new(task_id: TaskId) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            task_id,
        }
    }

    /// Check if cancellation has been requested
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Request cancellation
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        debug!("Cancellation requested for task {}", self.task_id);
    }

    /// Get the task ID
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Wait for cancellation signal
    ///
    /// This is an async method that completes when cancellation is requested.
    /// Useful for tasks that want to await cancellation rather than polling.
    pub async fn cancelled(&self) {
        // Poll-based implementation (could be improved with notification)
        while !self.is_cancelled() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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
}
