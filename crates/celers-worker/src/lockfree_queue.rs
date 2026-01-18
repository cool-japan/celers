//! Lock-free task queue implementation
//!
//! This module provides a high-performance lock-free queue for task scheduling
//! using work-stealing techniques to maximize throughput and reduce contention.
//!
//! # Features
//!
//! - Lock-free operations for better performance under contention
//! - Work-stealing support for load balancing
//! - Thread-safe producer and consumer handles
//! - Batch dequeue operations
//! - Zero-copy task passing when possible
//!
//! # Example
//!
//! ```
//! use celers_worker::LockFreeQueue;
//!
//! let queue = LockFreeQueue::new();
//!
//! // Push tasks
//! queue.push("task-1".to_string());
//! queue.push("task-2".to_string());
//!
//! // Pop tasks
//! assert_eq!(queue.pop(), Some("task-1".to_string()));
//! assert_eq!(queue.pop(), Some("task-2".to_string()));
//! assert_eq!(queue.pop(), None);
//! ```

use crossbeam_deque::{Injector, Steal};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Lock-free queue for tasks
///
/// Uses crossbeam's injector for high-performance concurrent access.
/// This queue is thread-safe and can be shared across threads.
pub struct LockFreeQueue<T> {
    /// Global injector for tasks
    injector: Arc<Injector<T>>,
    /// Total task count (approximate)
    count: Arc<AtomicUsize>,
}

impl<T> LockFreeQueue<T> {
    /// Create a new lock-free queue
    pub fn new() -> Self {
        Self {
            injector: Arc::new(Injector::new()),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Push a task to the queue
    pub fn push(&self, task: T) {
        self.injector.push(task);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Pop a task from the queue (FIFO order)
    pub fn pop(&self) -> Option<T> {
        loop {
            match self.injector.steal() {
                Steal::Success(task) => {
                    self.count.fetch_sub(1, Ordering::Relaxed);
                    return Some(task);
                }
                Steal::Empty => return None,
                Steal::Retry => continue, // Retry on contention
            }
        }
    }

    /// Get approximate queue size
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.injector.is_empty()
    }

    /// Batch pop multiple tasks
    pub fn pop_batch(&self, max_count: usize) -> Vec<T> {
        let mut tasks = Vec::with_capacity(max_count);
        for _ in 0..max_count {
            match self.pop() {
                Some(task) => tasks.push(task),
                None => break,
            }
        }
        tasks
    }

    /// Get the underlying injector for advanced usage
    pub fn injector(&self) -> &Arc<Injector<T>> {
        &self.injector
    }

    /// Try to pop without retrying on contention
    pub fn try_pop(&self) -> Option<T> {
        match self.injector.steal() {
            Steal::Success(task) => {
                self.count.fetch_sub(1, Ordering::Relaxed);
                Some(task)
            }
            Steal::Empty | Steal::Retry => None,
        }
    }
}

impl<T> Clone for LockFreeQueue<T> {
    fn clone(&self) -> Self {
        Self {
            injector: Arc::clone(&self.injector),
            count: Arc::clone(&self.count),
        }
    }
}

impl<T> Default for LockFreeQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

// Implement Send and Sync explicitly to ensure thread-safety
unsafe impl<T: Send> Send for LockFreeQueue<T> {}
unsafe impl<T: Send> Sync for LockFreeQueue<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lockfree_queue_basic() {
        let queue = LockFreeQueue::new();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());

        queue.push("task-1".to_string());
        queue.push("task-2".to_string());

        assert_eq!(queue.len(), 2);
        assert!(!queue.is_empty());

        assert_eq!(queue.pop(), Some("task-1".to_string()));
        assert_eq!(queue.pop(), Some("task-2".to_string()));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_lockfree_queue_fifo() {
        let queue = LockFreeQueue::new();

        for i in 0..10 {
            queue.push(i);
        }

        for i in 0..10 {
            assert_eq!(queue.pop(), Some(i));
        }
    }

    #[test]
    fn test_lockfree_queue_batch_pop() {
        let queue = LockFreeQueue::new();

        for i in 0..10 {
            queue.push(i);
        }

        let batch = queue.pop_batch(5);
        assert_eq!(batch.len(), 5);
        assert_eq!(batch, vec![0, 1, 2, 3, 4]);

        let remaining = queue.pop_batch(10);
        assert_eq!(remaining.len(), 5);
        assert_eq!(remaining, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_lockfree_queue_try_pop() {
        let queue = LockFreeQueue::new();

        queue.push(42);
        assert_eq!(queue.try_pop(), Some(42));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_lockfree_queue_clone() {
        let queue1 = LockFreeQueue::new();
        queue1.push(1);
        queue1.push(2);

        let queue2 = queue1.clone();

        // Both queues share the same underlying data
        assert_eq!(queue2.pop(), Some(1));
        assert_eq!(queue1.pop(), Some(2));
        assert!(queue1.is_empty());
        assert!(queue2.is_empty());
    }

    #[test]
    fn test_lockfree_queue_concurrent() {
        use std::thread;

        let queue = Arc::new(LockFreeQueue::new());
        let mut handles = vec![];

        // Spawn producers
        for i in 0..4 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                for j in 0..25 {
                    q.push(i * 100 + j);
                }
            }));
        }

        // Wait for producers
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 100 tasks
        assert_eq!(queue.len(), 100);

        // Consume all tasks
        let mut consumed = Vec::new();
        while let Some(task) = queue.pop() {
            consumed.push(task);
        }

        assert_eq!(consumed.len(), 100);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_lockfree_queue_concurrent_producers_consumers() {
        use std::sync::Barrier;
        use std::thread;

        let queue = Arc::new(LockFreeQueue::new());
        let barrier = Arc::new(Barrier::new(8));
        let mut producer_handles = vec![];
        let mut consumer_handles = vec![];

        // Spawn 4 producers
        for i in 0..4 {
            let q = Arc::clone(&queue);
            let b = Arc::clone(&barrier);
            producer_handles.push(thread::spawn(move || {
                b.wait(); // Synchronize start
                for j in 0..50 {
                    q.push(i * 1000 + j);
                }
            }));
        }

        // Spawn 4 consumers
        for _ in 0..4 {
            let q = Arc::clone(&queue);
            let b = Arc::clone(&barrier);
            consumer_handles.push(thread::spawn(move || {
                b.wait(); // Synchronize start
                let mut consumed = 0;
                while consumed < 50 {
                    if q.pop().is_some() {
                        consumed += 1;
                    }
                }
                consumed
            }));
        }

        // Wait for producers
        for handle in producer_handles {
            handle.join().unwrap();
        }

        // Wait for consumers and count
        let mut total_consumed = 0;
        for handle in consumer_handles {
            total_consumed += handle.join().unwrap();
        }

        assert_eq!(total_consumed, 200); // 4 consumers * 50 each
        assert!(queue.is_empty());
    }
}
