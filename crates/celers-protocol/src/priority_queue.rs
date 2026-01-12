//! Priority queue for message processing
//!
//! This module provides priority-based message queues for efficient
//! task scheduling and execution ordering.

use crate::Message;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Wrapper for messages with priority ordering
#[derive(Debug, Clone)]
struct PriorityMessage {
    message: Message,
    priority: u8,
    sequence: u64, // For FIFO ordering within same priority
}

impl PartialEq for PriorityMessage {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for PriorityMessage {}

impl PartialOrd for PriorityMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first, then earlier sequence
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => other.sequence.cmp(&self.sequence), // FIFO for same priority
            ordering => ordering,
        }
    }
}

/// Priority queue for messages
///
/// Messages are ordered by priority (0-9, higher = more important),
/// with FIFO ordering within the same priority level.
#[derive(Debug, Clone)]
pub struct MessagePriorityQueue {
    heap: BinaryHeap<PriorityMessage>,
    sequence_counter: u64,
    max_size: Option<usize>,
}

impl MessagePriorityQueue {
    /// Create a new priority queue
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            sequence_counter: 0,
            max_size: None,
        }
    }

    /// Create a priority queue with a maximum size
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(max_size),
            sequence_counter: 0,
            max_size: Some(max_size),
        }
    }

    /// Push a message onto the queue
    ///
    /// Returns `true` if the message was added, `false` if the queue is full
    pub fn push(&mut self, message: Message) -> bool {
        if let Some(max) = self.max_size {
            if self.heap.len() >= max {
                return false;
            }
        }

        let priority = message.properties.priority.unwrap_or(5);
        let priority_msg = PriorityMessage {
            message,
            priority,
            sequence: self.sequence_counter,
        };

        self.sequence_counter = self.sequence_counter.wrapping_add(1);
        self.heap.push(priority_msg);
        true
    }

    /// Pop the highest priority message from the queue
    pub fn pop(&mut self) -> Option<Message> {
        self.heap.pop().map(|pm| pm.message)
    }

    /// Peek at the highest priority message without removing it
    pub fn peek(&self) -> Option<&Message> {
        self.heap.peek().map(|pm| &pm.message)
    }

    /// Get the number of messages in the queue
    #[inline]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Check if the queue is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Check if the queue is full (if max_size is set)
    #[inline]
    pub fn is_full(&self) -> bool {
        if let Some(max) = self.max_size {
            self.heap.len() >= max
        } else {
            false
        }
    }

    /// Clear all messages from the queue
    pub fn clear(&mut self) {
        self.heap.clear();
        self.sequence_counter = 0;
    }

    /// Drain all messages from the queue in priority order
    pub fn drain(&mut self) -> Vec<Message> {
        let mut messages = Vec::with_capacity(self.heap.len());
        while let Some(msg) = self.pop() {
            messages.push(msg);
        }
        messages
    }

    /// Get messages with a specific priority
    pub fn filter_by_priority(&self, priority: u8) -> Vec<&Message> {
        self.heap
            .iter()
            .filter(|pm| pm.priority == priority)
            .map(|pm| &pm.message)
            .collect()
    }

    /// Count messages by priority level
    pub fn count_by_priority(&self) -> [usize; 10] {
        let mut counts = [0; 10];
        for pm in &self.heap {
            if (pm.priority as usize) < 10 {
                counts[pm.priority as usize] += 1;
            }
        }
        counts
    }
}

impl Default for MessagePriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<Message> for MessagePriorityQueue {
    fn from_iter<T: IntoIterator<Item = Message>>(iter: T) -> Self {
        let mut queue = Self::new();
        for message in iter {
            queue.push(message);
        }
        queue
    }
}

impl Extend<Message> for MessagePriorityQueue {
    fn extend<T: IntoIterator<Item = Message>>(&mut self, iter: T) {
        for message in iter {
            if !self.push(message) {
                break; // Stop if queue is full
            }
        }
    }
}

impl IntoIterator for MessagePriorityQueue {
    type Item = Message;
    type IntoIter = PriorityQueueIter;

    fn into_iter(self) -> Self::IntoIter {
        PriorityQueueIter { queue: self }
    }
}

/// Iterator that drains messages from a priority queue in priority order
pub struct PriorityQueueIter {
    queue: MessagePriorityQueue,
}

impl Iterator for PriorityQueueIter {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.pop()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.queue.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for PriorityQueueIter {
    fn len(&self) -> usize {
        self.queue.len()
    }
}

/// Multi-level priority queues with separate queues for each priority
#[derive(Debug, Clone)]
pub struct MultiLevelQueue {
    queues: [Vec<Message>; 10], // One queue per priority level (0-9)
    total_size: usize,
    max_size: Option<usize>,
}

impl MultiLevelQueue {
    /// Create a new multi-level queue
    pub fn new() -> Self {
        Self {
            queues: Default::default(),
            total_size: 0,
            max_size: None,
        }
    }

    /// Create a multi-level queue with maximum size
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            queues: Default::default(),
            total_size: 0,
            max_size: Some(max_size),
        }
    }

    /// Push a message to the appropriate priority queue
    pub fn push(&mut self, message: Message) -> bool {
        if let Some(max) = self.max_size {
            if self.total_size >= max {
                return false;
            }
        }

        let priority = message.properties.priority.unwrap_or(5) as usize;
        if priority < 10 {
            self.queues[priority].push(message);
            self.total_size += 1;
            true
        } else {
            false
        }
    }

    /// Pop the highest priority message
    pub fn pop(&mut self) -> Option<Message> {
        // Iterate from highest to lowest priority
        for queue in self.queues.iter_mut().rev() {
            if let Some(msg) = queue.pop() {
                self.total_size -= 1;
                return Some(msg);
            }
        }
        None
    }

    /// Peek at the highest priority message
    pub fn peek(&self) -> Option<&Message> {
        for queue in self.queues.iter().rev() {
            if let Some(msg) = queue.last() {
                return Some(msg);
            }
        }
        None
    }

    /// Get the total number of messages across all queues
    #[inline]
    pub fn len(&self) -> usize {
        self.total_size
    }

    /// Check if all queues are empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    /// Get the number of messages at a specific priority level
    #[inline]
    pub fn len_at_priority(&self, priority: u8) -> usize {
        if (priority as usize) < 10 {
            self.queues[priority as usize].len()
        } else {
            0
        }
    }

    /// Clear all queues
    pub fn clear(&mut self) {
        for queue in &mut self.queues {
            queue.clear();
        }
        self.total_size = 0;
    }

    /// Drain all messages in priority order
    pub fn drain(&mut self) -> Vec<Message> {
        let mut messages = Vec::with_capacity(self.total_size);
        while let Some(msg) = self.pop() {
            messages.push(msg);
        }
        messages
    }
}

impl Default for MultiLevelQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<Message> for MultiLevelQueue {
    fn from_iter<T: IntoIterator<Item = Message>>(iter: T) -> Self {
        let mut queue = Self::new();
        for message in iter {
            queue.push(message);
        }
        queue
    }
}

impl Extend<Message> for MultiLevelQueue {
    fn extend<T: IntoIterator<Item = Message>>(&mut self, iter: T) {
        for message in iter {
            if !self.push(message) {
                break; // Stop if queue is full
            }
        }
    }
}

impl IntoIterator for MultiLevelQueue {
    type Item = Message;
    type IntoIter = MultiLevelQueueIter;

    fn into_iter(self) -> Self::IntoIter {
        MultiLevelQueueIter { queue: self }
    }
}

/// Iterator that drains messages from a multi-level queue in priority order
pub struct MultiLevelQueueIter {
    queue: MultiLevelQueue,
}

impl Iterator for MultiLevelQueueIter {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.pop()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.queue.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for MultiLevelQueueIter {
    fn len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MessageBuilder;

    fn create_message_with_priority(task: &str, priority: u8) -> Message {
        MessageBuilder::new(task)
            .priority(priority)
            .build()
            .unwrap()
    }

    #[test]
    fn test_priority_queue_push_pop() {
        let mut queue = MessagePriorityQueue::new();

        let msg1 = create_message_with_priority("task1", 5);
        let msg2 = create_message_with_priority("task2", 9);
        let msg3 = create_message_with_priority("task3", 1);

        queue.push(msg1);
        queue.push(msg2);
        queue.push(msg3);

        assert_eq!(queue.len(), 3);

        // Should pop in priority order: 9, 5, 1
        let popped = queue.pop().unwrap();
        assert_eq!(popped.properties.priority, Some(9));

        let popped = queue.pop().unwrap();
        assert_eq!(popped.properties.priority, Some(5));

        let popped = queue.pop().unwrap();
        assert_eq!(popped.properties.priority, Some(1));

        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_fifo_same_priority() {
        let mut queue = MessagePriorityQueue::new();

        let msg1 = create_message_with_priority("task1", 5);
        let msg2 = create_message_with_priority("task2", 5);
        let msg3 = create_message_with_priority("task3", 5);

        queue.push(msg1.clone());
        queue.push(msg2.clone());
        queue.push(msg3.clone());

        // Should pop in FIFO order for same priority
        let popped1 = queue.pop().unwrap();
        assert_eq!(popped1.headers.task, "task1");

        let popped2 = queue.pop().unwrap();
        assert_eq!(popped2.headers.task, "task2");

        let popped3 = queue.pop().unwrap();
        assert_eq!(popped3.headers.task, "task3");
    }

    #[test]
    fn test_priority_queue_peek() {
        let mut queue = MessagePriorityQueue::new();

        let msg1 = create_message_with_priority("task1", 5);
        let msg2 = create_message_with_priority("task2", 9);

        queue.push(msg1);
        queue.push(msg2);

        let peeked = queue.peek().unwrap();
        assert_eq!(peeked.properties.priority, Some(9));
        assert_eq!(queue.len(), 2); // Peek doesn't remove
    }

    #[test]
    fn test_priority_queue_with_capacity() {
        let mut queue = MessagePriorityQueue::with_capacity(2);

        assert!(queue.push(create_message_with_priority("task1", 5)));
        assert!(queue.push(create_message_with_priority("task2", 5)));
        assert!(!queue.push(create_message_with_priority("task3", 5))); // Full

        assert!(queue.is_full());
    }

    #[test]
    fn test_priority_queue_clear() {
        let mut queue = MessagePriorityQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 5));

        assert_eq!(queue.len(), 2);

        queue.clear();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_drain() {
        let mut queue = MessagePriorityQueue::new();

        queue.push(create_message_with_priority("task1", 1));
        queue.push(create_message_with_priority("task2", 9));
        queue.push(create_message_with_priority("task3", 5));

        let messages = queue.drain();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].properties.priority, Some(9));
        assert_eq!(messages[1].properties.priority, Some(5));
        assert_eq!(messages[2].properties.priority, Some(1));
        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_filter_by_priority() {
        let mut queue = MessagePriorityQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 9));
        queue.push(create_message_with_priority("task3", 5));

        let filtered = queue.filter_by_priority(5);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_priority_queue_count_by_priority() {
        let mut queue = MessagePriorityQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 9));
        queue.push(create_message_with_priority("task3", 5));
        queue.push(create_message_with_priority("task4", 1));

        let counts = queue.count_by_priority();
        assert_eq!(counts[1], 1);
        assert_eq!(counts[5], 2);
        assert_eq!(counts[9], 1);
    }

    #[test]
    fn test_multi_level_queue_push_pop() {
        let mut queue = MultiLevelQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 9));
        queue.push(create_message_with_priority("task3", 1));

        assert_eq!(queue.len(), 3);

        // Should pop in priority order
        let popped = queue.pop().unwrap();
        assert_eq!(popped.properties.priority, Some(9));

        let popped = queue.pop().unwrap();
        assert_eq!(popped.properties.priority, Some(5));

        let popped = queue.pop().unwrap();
        assert_eq!(popped.properties.priority, Some(1));
    }

    #[test]
    fn test_multi_level_queue_len_at_priority() {
        let mut queue = MultiLevelQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 5));
        queue.push(create_message_with_priority("task3", 9));

        assert_eq!(queue.len_at_priority(5), 2);
        assert_eq!(queue.len_at_priority(9), 1);
        assert_eq!(queue.len_at_priority(0), 0);
    }

    #[test]
    fn test_multi_level_queue_peek() {
        let mut queue = MultiLevelQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 9));

        let peeked = queue.peek().unwrap();
        assert_eq!(peeked.properties.priority, Some(9));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn test_multi_level_queue_clear() {
        let mut queue = MultiLevelQueue::new();

        queue.push(create_message_with_priority("task1", 5));
        queue.push(create_message_with_priority("task2", 9));

        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_from_iterator() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
        ];

        let queue: MessagePriorityQueue = messages.into_iter().collect();
        assert_eq!(queue.len(), 3);
    }

    #[test]
    fn test_priority_queue_extend() {
        let mut queue = MessagePriorityQueue::new();
        queue.push(create_message_with_priority("task1", 5));

        let new_messages = vec![
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
        ];

        queue.extend(new_messages);
        assert_eq!(queue.len(), 3);

        // Check priority order
        assert_eq!(queue.pop().unwrap().properties.priority, Some(9));
        assert_eq!(queue.pop().unwrap().properties.priority, Some(5));
        assert_eq!(queue.pop().unwrap().properties.priority, Some(1));
    }

    #[test]
    fn test_priority_queue_extend_with_capacity() {
        let mut queue = MessagePriorityQueue::with_capacity(3);
        queue.push(create_message_with_priority("task1", 5));

        let new_messages = vec![
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
            create_message_with_priority("task4", 7), // Should not be added (over capacity)
        ];

        queue.extend(new_messages);
        assert_eq!(queue.len(), 3);
        assert!(queue.is_full());
    }

    #[test]
    fn test_priority_queue_into_iterator() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
        ];

        let queue: MessagePriorityQueue = messages.into_iter().collect();
        let mut count = 0;
        let mut priorities = Vec::new();

        for msg in queue {
            priorities.push(msg.properties.priority.unwrap());
            count += 1;
        }

        assert_eq!(count, 3);
        assert_eq!(priorities, vec![9, 5, 1]); // Should be in priority order
    }

    #[test]
    fn test_priority_queue_iter_exact_size() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
        ];

        let queue: MessagePriorityQueue = messages.into_iter().collect();
        let iter = queue.into_iter();

        assert_eq!(iter.len(), 3);

        let collected: Vec<_> = iter.collect();
        assert_eq!(collected.len(), 3);
    }

    #[test]
    fn test_priority_queue_iterator_chain() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
            create_message_with_priority("task4", 7),
        ];

        let queue: MessagePriorityQueue = messages.into_iter().collect();

        let task_names: Vec<String> = queue
            .into_iter()
            .map(|msg| msg.headers.task.clone())
            .collect();

        assert_eq!(task_names, vec!["task2", "task4", "task1", "task3"]);
    }

    #[test]
    fn test_multi_level_queue_from_iterator() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
        ];

        let queue: MultiLevelQueue = messages.into_iter().collect();
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.len_at_priority(5), 1);
        assert_eq!(queue.len_at_priority(9), 1);
        assert_eq!(queue.len_at_priority(1), 1);
    }

    #[test]
    fn test_multi_level_queue_extend() {
        let mut queue = MultiLevelQueue::new();
        queue.push(create_message_with_priority("task1", 5));

        let new_messages = vec![
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 5),
        ];

        queue.extend(new_messages);
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.len_at_priority(5), 2);
        assert_eq!(queue.len_at_priority(9), 1);
    }

    #[test]
    fn test_multi_level_queue_into_iterator() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
        ];

        let queue: MultiLevelQueue = messages.into_iter().collect();
        let mut count = 0;
        let mut priorities = Vec::new();

        for msg in queue {
            priorities.push(msg.properties.priority.unwrap());
            count += 1;
        }

        assert_eq!(count, 3);
        assert_eq!(priorities, vec![9, 5, 1]); // Should be in priority order
    }

    #[test]
    fn test_multi_level_queue_iter_exact_size() {
        let messages = vec![
            create_message_with_priority("task1", 5),
            create_message_with_priority("task2", 9),
        ];

        let queue: MultiLevelQueue = messages.into_iter().collect();
        let iter = queue.into_iter();

        assert_eq!(iter.len(), 2);

        let collected: Vec<_> = iter.collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_multi_level_queue_extend_with_capacity() {
        let mut queue = MultiLevelQueue::with_capacity(3);
        queue.push(create_message_with_priority("task1", 5));

        let new_messages = vec![
            create_message_with_priority("task2", 9),
            create_message_with_priority("task3", 1),
            create_message_with_priority("task4", 7), // Should not be added
        ];

        queue.extend(new_messages);
        assert_eq!(queue.len(), 3);
    }
}
