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
            let mut pool = self.inner.lock().unwrap();
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
        msg.headers.extra.clear();
        msg.properties = crate::MessageProperties::default();
        msg.body.clear();
        msg.content_type = crate::CONTENT_TYPE_JSON.to_string();
        msg.content_encoding = crate::ENCODING_UTF8.to_string();

        PooledMessage {
            message: Some(msg),
            pool: self.clone(),
        }
    }

    /// Return a message to the pool
    fn release(&self, msg: crate::Message) {
        let mut pool = self.inner.lock().unwrap();
        if pool.len() < self.max_size {
            pool.push(msg);
        }
        // Otherwise, drop the message (pool is full)
    }

    /// Get the current number of messages in the pool
    #[inline]
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Get the maximum pool size
    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Clear all messages from the pool
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
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
pub struct PooledMessage {
    message: Option<crate::Message>,
    pool: MessagePool,
}

impl PooledMessage {
    /// Take ownership of the message, removing it from pool management
    pub fn take(mut self) -> crate::Message {
        self.message.take().unwrap()
    }

    /// Get a reference to the message
    #[inline]
    pub fn get(&self) -> &crate::Message {
        self.message.as_ref().unwrap()
    }

    /// Get a mutable reference to the message
    #[inline]
    pub fn get_mut(&mut self) -> &mut crate::Message {
        self.message.as_mut().unwrap()
    }
}

impl std::ops::Deref for PooledMessage {
    type Target = crate::Message;

    fn deref(&self) -> &Self::Target {
        self.message.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledMessage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.message.as_mut().unwrap()
    }
}

impl Drop for PooledMessage {
    fn drop(&mut self) {
        if let Some(msg) = self.message.take() {
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
            let mut pool = self.inner.lock().unwrap();
            pool.pop()
        };

        let mut args = args.unwrap_or_else(crate::TaskArgs::new);

        // Clear for reuse
        args.args.clear();
        args.kwargs.clear();

        PooledTaskArgs {
            args: Some(args),
            pool: self.clone(),
        }
    }

    /// Return task arguments to the pool
    fn release(&self, args: crate::TaskArgs) {
        let mut pool = self.inner.lock().unwrap();
        if pool.len() < self.max_size {
            pool.push(args);
        }
    }

    /// Get the current number of task args in the pool
    #[inline]
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Get the maximum pool size
    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Clear the pool
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

impl Default for TaskArgsPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Pooled task arguments
pub struct PooledTaskArgs {
    args: Option<crate::TaskArgs>,
    pool: TaskArgsPool,
}

impl PooledTaskArgs {
    /// Take ownership, removing from pool management
    pub fn take(mut self) -> crate::TaskArgs {
        self.args.take().unwrap()
    }

    /// Get a reference
    #[inline]
    pub fn get(&self) -> &crate::TaskArgs {
        self.args.as_ref().unwrap()
    }

    /// Get a mutable reference
    #[inline]
    pub fn get_mut(&mut self) -> &mut crate::TaskArgs {
        self.args.as_mut().unwrap()
    }
}

impl std::ops::Deref for PooledTaskArgs {
    type Target = crate::TaskArgs;

    fn deref(&self) -> &Self::Target {
        self.args.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledTaskArgs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.args.as_mut().unwrap()
    }
}

impl Drop for PooledTaskArgs {
    fn drop(&mut self) {
        if let Some(args) = self.args.take() {
            self.pool.release(args);
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
