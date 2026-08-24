//! Message pooling for memory efficiency
//!
//! This module provides object pooling for messages to reduce allocation overhead
//! in high-throughput scenarios. By reusing message structures, we can significantly
//! reduce GC pressure and improve performance.
//!
//! # Examples
//!
//! ```
//! use celers_protocol::pool::{MessagePool, PooledMessage};
//! use uuid::Uuid;
//!
//! // Create a message pool
//! let pool = MessagePool::new();
//!
//! // Acquire a pooled message
//! let mut msg = pool.acquire();
//! msg.headers.task = "tasks.add".to_string();
//! msg.headers.id = Uuid::new_v4();
//! msg.body = vec![1, 2, 3];
//!
//! // When dropped, the message is returned to the pool
//! drop(msg);
//!
//! // The next acquire will reuse the same allocation
//! let msg2 = pool.acquire();
//! assert_eq!(pool.size(), 0); // Pool is now empty
//! ```

use chrono::Utc;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// A pool of reusable messages
///
/// Uses a simple stack-based pool with thread-safe access.
/// Messages are automatically returned to the pool when dropped.
#[derive(Debug, Clone)]
pub struct MessagePool {
    inner: Arc<Mutex<Vec<crate::Message>>>,
    max_size: usize,
}

impl MessagePool {
    /// Create a new message pool with default capacity (1000)
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    /// Create a new message pool with specified maximum capacity
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            max_size,
        }
    }

    /// Acquire a message from the pool
    ///
    /// If the pool is empty, a new message is allocated.
    /// Otherwise, a recycled message is returned (after clearing).
    pub fn acquire(&self) -> PooledMessage {
        let msg = {
            let mut pool = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            pool.pop()
        };

        let mut msg =
            msg.unwrap_or_else(|| crate::Message::new("".to_string(), Uuid::nil(), Vec::new()));

        // Clear the message for reuse
        msg.headers.task.clear();
        msg.headers.id = Uuid::nil();
        msg.headers.lang = crate::DEFAULT_LANG.to_string();
        msg.headers.root_id = None;
        msg.headers.parent_id = None;
        msg.headers.group = None;
        msg.headers.retries = None;
        msg.headers.eta = None;
        msg.headers.expires = None;
        // A freshly acquired message is, semantically, newly created: reset
        // the creation timestamp so it doesn't leak the previous occupant's
        // `created_at`. Without this, `Message::created_at()` /
        // `MessageExt::get_age_seconds()` / age-based routing would compute
        // an age that belongs to whatever message last occupied this slot.
        msg.headers.created_at = Some(Utc::now());
        msg.headers.extra.clear();
        msg.properties = crate::MessageProperties::default();
        msg.body.clear();
        msg.content_type = crate::CONTENT_TYPE_JSON.to_string();
        msg.content_encoding = crate::ENCODING_UTF8.to_string();

        PooledMessage {
            message: msg,
            pool: self.clone(),
            taken: false,
        }
    }

    /// Return a message to the pool
    fn release(&self, msg: crate::Message) {
        let mut pool = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < self.max_size {
            pool.push(msg);
        }
        // Otherwise, drop the message (pool is full)
    }

    /// Get the current number of messages in the pool
    #[inline]
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Get the maximum pool size
    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Clear all messages from the pool
    pub fn clear(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

impl Default for MessagePool {
    fn default() -> Self {
        Self::new()
    }
}

/// A pooled message that automatically returns to the pool when dropped
///
/// Provides deref access to the underlying message.
///
/// The message is stored directly (not as `Option<Message>`): `taken` alone
/// records whether [`PooledMessage::take`] already removed it from pool
/// management, so every accessor is a plain, infallible field access - none
/// of `get`/`get_mut`/`Deref`/`DerefMut`/`take` can ever panic.
pub struct PooledMessage {
    message: crate::Message,
    pool: MessagePool,
    taken: bool,
}

impl PooledMessage {
    /// Take ownership of the message, removing it from pool management
    pub fn take(mut self) -> crate::Message {
        self.taken = true;
        std::mem::replace(
            &mut self.message,
            crate::Message::new(String::new(), Uuid::nil(), Vec::new()),
        )
    }

    /// Get a reference to the message
    #[inline]
    pub fn get(&self) -> &crate::Message {
        &self.message
    }

    /// Get a mutable reference to the message
    #[inline]
    pub fn get_mut(&mut self) -> &mut crate::Message {
        &mut self.message
    }
}

impl std::ops::Deref for PooledMessage {
    type Target = crate::Message;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl std::ops::DerefMut for PooledMessage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}

impl Drop for PooledMessage {
    fn drop(&mut self) {
        if !self.taken {
            let msg = std::mem::replace(
                &mut self.message,
                crate::Message::new(String::new(), Uuid::nil(), Vec::new()),
            );
            self.pool.release(msg);
        }
    }
}

/// A pool of reusable task arguments
#[derive(Debug, Clone)]
pub struct TaskArgsPool {
    inner: Arc<Mutex<Vec<crate::TaskArgs>>>,
    max_size: usize,
}

impl TaskArgsPool {
    /// Create a new task args pool with default capacity (1000)
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    /// Create a new task args pool with specified maximum capacity
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            max_size,
        }
    }

    /// Acquire task arguments from the pool
    pub fn acquire(&self) -> PooledTaskArgs {
        let args = {
            let mut pool = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            pool.pop()
        };

        let mut args = args.unwrap_or_else(crate::TaskArgs::new);

        // Clear for reuse
        args.args.clear();
        args.kwargs.clear();

        PooledTaskArgs {
            args,
            pool: self.clone(),
            taken: false,
        }
    }

    /// Return task arguments to the pool
    fn release(&self, args: crate::TaskArgs) {
        let mut pool = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < self.max_size {
            pool.push(args);
        }
    }

    /// Get the current number of task args in the pool
    #[inline]
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Get the maximum pool size
    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Clear the pool
    pub fn clear(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

impl Default for TaskArgsPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Pooled task arguments
///
/// Like [`PooledMessage`], the arguments are stored directly (not as
/// `Option<TaskArgs>`); `taken` alone records whether
/// [`PooledTaskArgs::take`] already removed them from pool management, so no
/// accessor here can panic.
pub struct PooledTaskArgs {
    args: crate::TaskArgs,
    pool: TaskArgsPool,
    taken: bool,
}

impl PooledTaskArgs {
    /// Take ownership, removing from pool management
    pub fn take(mut self) -> crate::TaskArgs {
        self.taken = true;
        std::mem::take(&mut self.args)
    }

    /// Get a reference
    #[inline]
    pub fn get(&self) -> &crate::TaskArgs {
        &self.args
    }

    /// Get a mutable reference
    #[inline]
    pub fn get_mut(&mut self) -> &mut crate::TaskArgs {
        &mut self.args
    }
}

impl std::ops::Deref for PooledTaskArgs {
    type Target = crate::TaskArgs;

    fn deref(&self) -> &Self::Target {
        &self.args
    }
}

impl std::ops::DerefMut for PooledTaskArgs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.args
    }
}

impl Drop for PooledTaskArgs {
    fn drop(&mut self) {
        if !self.taken {
            self.pool.release(std::mem::take(&mut self.args));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_pool_basic() {
        let pool = MessagePool::new();
        assert_eq!(pool.size(), 0);

        let msg = pool.acquire();
        assert_eq!(pool.size(), 0); // Acquired, not in pool

        drop(msg);
        assert_eq!(pool.size(), 1); // Returned to pool
    }

    #[test]
    fn test_message_pool_reuse() {
        let pool = MessagePool::new();

        // First acquire creates new
        let mut msg1 = pool.acquire();
        msg1.headers.task = "test".to_string();
        drop(msg1);

        // Second acquire reuses
        let msg2 = pool.acquire();
        assert_eq!(msg2.headers.task, ""); // Cleared
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_message_pool_max_size() {
        let pool = MessagePool::with_capacity(2);

        let msg1 = pool.acquire();
        let msg2 = pool.acquire();
        let msg3 = pool.acquire();

        drop(msg1);
        drop(msg2);
        drop(msg3);

        // Only 2 should be retained (max_size = 2)
        assert_eq!(pool.size(), 2);
    }

    #[test]
    fn test_pooled_message_deref() {
        let pool = MessagePool::new();
        let mut msg = pool.acquire();

        msg.headers.task = "tasks.test".to_string();
        assert_eq!(msg.headers.task, "tasks.test");
    }

    #[test]
    fn test_pooled_message_take() {
        let pool = MessagePool::new();
        let mut msg = pool.acquire();
        msg.headers.task = "tasks.test".to_string();

        let owned = msg.take();
        assert_eq!(owned.headers.task, "tasks.test");

        // Message was taken, not returned to pool
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_task_args_pool_basic() {
        let pool = TaskArgsPool::new();
        assert_eq!(pool.size(), 0);

        let args = pool.acquire();
        assert_eq!(pool.size(), 0);

        drop(args);
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_task_args_pool_reuse() {
        let pool = TaskArgsPool::new();

        {
            let mut args1 = pool.acquire();
            args1.get_mut().args.push(serde_json::json!(42));
        }

        let args2 = pool.acquire();
        assert_eq!(args2.get().args.len(), 0); // Cleared
    }

    #[test]
    fn test_task_args_pool_deref() {
        let pool = TaskArgsPool::new();
        let mut args = pool.acquire();

        args.get_mut().args.push(serde_json::json!(1));
        assert_eq!(args.get().args.len(), 1);
    }

    #[test]
    fn test_task_args_pool_take() {
        let pool = TaskArgsPool::new();
        let mut args = pool.acquire();
        args.get_mut().args.push(serde_json::json!(1));

        let owned = args.take();
        assert_eq!(owned.args.len(), 1);

        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_message_pool_acquire_refreshes_created_at() {
        // Regression: a recycled message must not carry the previous
        // occupant's `created_at` timestamp.
        let pool = MessagePool::new();

        let mut msg1 = pool.acquire();
        let stale_created_at = Utc::now() - chrono::Duration::hours(2);
        msg1.headers.created_at = Some(stale_created_at);
        drop(msg1);

        let msg2 = pool.acquire();
        let created_at = msg2
            .headers
            .created_at
            .expect("acquire always sets created_at");
        assert!(created_at > stale_created_at);
        assert!(Utc::now() - created_at < chrono::Duration::seconds(5));
    }

    #[test]
    fn test_message_pool_survives_poisoned_lock() {
        // Regression: a panic while holding the pool's internal mutex used
        // to poison it, after which every subsequent acquire()/size()/
        // clear() call would itself panic via `.expect("lock should not be
        // poisoned")`. The pool must recover the guard instead.
        let pool = MessagePool::new();
        let inner = pool.inner.clone();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = inner.lock().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(inner.is_poisoned());

        // None of these may panic even though the lock is poisoned.
        let msg = pool.acquire();
        drop(msg);
        assert_eq!(pool.size(), 1);
        pool.clear();
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_task_args_pool_survives_poisoned_lock() {
        let pool = TaskArgsPool::new();
        let inner = pool.inner.clone();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = inner.lock().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(inner.is_poisoned());

        let args = pool.acquire();
        drop(args);
        assert_eq!(pool.size(), 1);
        pool.clear();
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_pool_clear() {
        let pool = MessagePool::new();

        let msg1 = pool.acquire();
        let msg2 = pool.acquire();
        drop(msg1);
        drop(msg2);

        assert_eq!(pool.size(), 2);

        pool.clear();
        assert_eq!(pool.size(), 0);
    }
}
