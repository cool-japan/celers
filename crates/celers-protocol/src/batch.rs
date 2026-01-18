//! Batch message processing utilities
//!
//! This module provides utilities for efficient batch processing of messages.

use crate::{Message, ValidationError};
use std::collections::HashMap;

/// Batch of messages for efficient processing
#[derive(Debug, Clone)]
pub struct MessageBatch {
    messages: Vec<Message>,
    max_size: usize,
}

impl MessageBatch {
    /// Create a new message batch with default max size (100)
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            max_size: 100,
        }
    }

    /// Create a new message batch with specified max size
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            messages: Vec::with_capacity(max_size),
            max_size,
        }
    }

    /// Add a message to the batch
    ///
    /// Returns `true` if the message was added, `false` if the batch is full
    pub fn push(&mut self, message: Message) -> bool {
        if self.messages.len() < self.max_size {
            self.messages.push(message);
            true
        } else {
            false
        }
    }

    /// Get the number of messages in the batch
    #[inline]
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Check if the batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Check if the batch is full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.messages.len() >= self.max_size
    }

    /// Get the messages in the batch
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Take all messages from the batch, leaving it empty
    pub fn drain(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }

    /// Validate all messages in the batch
    pub fn validate(&self) -> Result<(), ValidationError> {
        for msg in &self.messages {
            msg.validate()?;
        }
        Ok(())
    }

    /// Split the batch into smaller batches of the specified size
    pub fn split(self, chunk_size: usize) -> Vec<MessageBatch> {
        self.messages
            .chunks(chunk_size)
            .map(|chunk| {
                let mut batch = MessageBatch::with_capacity(chunk_size);
                for msg in chunk {
                    batch.push(msg.clone());
                }
                batch
            })
            .collect()
    }

    /// Merge another batch into this one
    ///
    /// Returns the messages that didn't fit if the combined size exceeds max_size
    pub fn merge(&mut self, other: MessageBatch) -> Vec<Message> {
        let mut overflow = Vec::new();
        for msg in other.messages {
            if !self.push(msg.clone()) {
                overflow.push(msg);
            }
        }
        overflow
    }
}

impl Default for MessageBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<Message> for MessageBatch {
    fn from_iter<T: IntoIterator<Item = Message>>(iter: T) -> Self {
        let messages: Vec<_> = iter.into_iter().collect();
        let max_size = messages.len().max(100);
        Self { messages, max_size }
    }
}

impl IntoIterator for MessageBatch {
    type Item = Message;
    type IntoIter = std::vec::IntoIter<Message>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

impl<'a> IntoIterator for &'a MessageBatch {
    type Item = &'a Message;
    type IntoIter = std::slice::Iter<'a, Message>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

impl<'a> IntoIterator for &'a mut MessageBatch {
    type Item = &'a mut Message;
    type IntoIter = std::slice::IterMut<'a, Message>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter_mut()
    }
}

impl std::ops::Index<usize> for MessageBatch {
    type Output = Message;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.messages[index]
    }
}

impl std::ops::IndexMut<usize> for MessageBatch {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.messages[index]
    }
}

impl Extend<Message> for MessageBatch {
    fn extend<T: IntoIterator<Item = Message>>(&mut self, iter: T) {
        for msg in iter {
            if !self.push(msg) {
                break; // Stop if batch is full
            }
        }
    }
}

impl AsRef<[Message]> for MessageBatch {
    #[inline]
    fn as_ref(&self) -> &[Message] {
        &self.messages
    }
}

impl AsMut<[Message]> for MessageBatch {
    #[inline]
    fn as_mut(&mut self) -> &mut [Message] {
        &mut self.messages
    }
}

/// Batch processor for processing messages in groups
pub struct BatchProcessor {
    batch_size: usize,
    timeout_ms: u64,
}

impl BatchProcessor {
    /// Create a new batch processor with default settings
    pub fn new() -> Self {
        Self {
            batch_size: 100,
            timeout_ms: 1000,
        }
    }

    /// Set the batch size
    #[must_use]
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the timeout in milliseconds
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Create batches from a vector of messages
    pub fn create_batches(&self, messages: Vec<Message>) -> Vec<MessageBatch> {
        messages
            .chunks(self.batch_size)
            .map(|chunk| {
                let mut batch = MessageBatch::with_capacity(self.batch_size);
                for msg in chunk {
                    batch.push(msg.clone());
                }
                batch
            })
            .collect()
    }

    /// Process messages in batches with a callback function
    pub fn process<F>(&self, messages: Vec<Message>, mut callback: F) -> Result<(), String>
    where
        F: FnMut(&[Message]) -> Result<(), String>,
    {
        for batch in self.create_batches(messages) {
            callback(batch.messages())?;
        }
        Ok(())
    }
}

impl Default for BatchProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for batch processing
#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    /// Total number of messages processed
    pub total_messages: usize,
    /// Number of batches processed
    pub total_batches: usize,
    /// Number of successful messages
    pub successful: usize,
    /// Number of failed messages
    pub failed: usize,
}

impl BatchStats {
    /// Create new batch statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a batch result
    pub fn record_batch(&mut self, batch_size: usize, successes: usize, failures: usize) {
        self.total_batches += 1;
        self.total_messages += batch_size;
        self.successful += successes;
        self.failed += failures;
    }

    /// Get the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_messages == 0 {
            0.0
        } else {
            (self.successful as f64 / self.total_messages as f64) * 100.0
        }
    }

    /// Get the average batch size
    pub fn average_batch_size(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.total_messages as f64 / self.total_batches as f64
        }
    }
}

/// Group messages by a key function
pub fn group_by<F, K>(messages: Vec<Message>, key_fn: F) -> HashMap<K, Vec<Message>>
where
    F: Fn(&Message) -> K,
    K: Eq + std::hash::Hash,
{
    let mut groups = HashMap::new();
    for msg in messages {
        let key = key_fn(&msg);
        groups.entry(key).or_insert_with(Vec::new).push(msg);
    }
    groups
}

/// Partition messages into two groups based on a predicate
pub fn partition<F>(messages: Vec<Message>, predicate: F) -> (Vec<Message>, Vec<Message>)
where
    F: Fn(&Message) -> bool,
{
    let mut true_group = Vec::new();
    let mut false_group = Vec::new();

    for msg in messages {
        if predicate(&msg) {
            true_group.push(msg);
        } else {
            false_group.push(msg);
        }
    }

    (true_group, false_group)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MessageBuilder;

    fn create_test_message(task: &str) -> Message {
        MessageBuilder::new(task).build().unwrap()
    }

    #[test]
    fn test_message_batch_new() {
        let batch = MessageBatch::new();
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
        assert!(!batch.is_full());
    }

    #[test]
    fn test_message_batch_push() {
        let mut batch = MessageBatch::with_capacity(2);
        assert!(batch.push(create_test_message("task1")));
        assert!(batch.push(create_test_message("task2")));
        assert!(!batch.push(create_test_message("task3"))); // Full

        assert_eq!(batch.len(), 2);
        assert!(batch.is_full());
    }

    #[test]
    fn test_message_batch_drain() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        let messages = batch.drain();
        assert_eq!(messages.len(), 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_message_batch_validate() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        assert!(batch.validate().is_ok());
    }

    #[test]
    fn test_message_batch_split() {
        let mut batch = MessageBatch::new();
        for i in 0..10 {
            batch.push(create_test_message(&format!("task{}", i)));
        }

        let batches = batch.split(3);
        assert_eq!(batches.len(), 4); // 10 messages / 3 = 4 batches (3, 3, 3, 1)
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 3);
        assert_eq!(batches[2].len(), 3);
        assert_eq!(batches[3].len(), 1);
    }

    #[test]
    fn test_message_batch_merge() {
        let mut batch1 = MessageBatch::with_capacity(5);
        batch1.push(create_test_message("task1"));
        batch1.push(create_test_message("task2"));

        let mut batch2 = MessageBatch::new();
        batch2.push(create_test_message("task3"));
        batch2.push(create_test_message("task4"));

        let overflow = batch1.merge(batch2);
        assert_eq!(batch1.len(), 4);
        assert!(overflow.is_empty());
    }

    #[test]
    fn test_batch_processor_create_batches() {
        let processor = BatchProcessor::new().with_batch_size(3);
        let messages = vec![
            create_test_message("task1"),
            create_test_message("task2"),
            create_test_message("task3"),
            create_test_message("task4"),
            create_test_message("task5"),
        ];

        let batches = processor.create_batches(messages);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 2);
    }

    #[test]
    fn test_batch_processor_process() {
        let processor = BatchProcessor::new().with_batch_size(2);
        let messages = vec![
            create_test_message("task1"),
            create_test_message("task2"),
            create_test_message("task3"),
        ];

        let mut count = 0;
        let result = processor.process(messages, |batch| {
            count += batch.len();
            Ok(())
        });

        assert!(result.is_ok());
        assert_eq!(count, 3);
    }

    #[test]
    fn test_batch_stats() {
        let mut stats = BatchStats::new();
        stats.record_batch(10, 8, 2);
        stats.record_batch(10, 9, 1);

        assert_eq!(stats.total_batches, 2);
        assert_eq!(stats.total_messages, 20);
        assert_eq!(stats.successful, 17);
        assert_eq!(stats.failed, 3);
        assert_eq!(stats.success_rate(), 85.0);
        assert_eq!(stats.average_batch_size(), 10.0);
    }

    #[test]
    fn test_group_by() {
        let messages = vec![
            create_test_message("tasks.add"),
            create_test_message("tasks.subtract"),
            create_test_message("tasks.add"),
            create_test_message("email.send"),
        ];

        let groups = group_by(messages, |msg| msg.headers.task.clone());
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.get("tasks.add").unwrap().len(), 2);
        assert_eq!(groups.get("tasks.subtract").unwrap().len(), 1);
        assert_eq!(groups.get("email.send").unwrap().len(), 1);
    }

    #[test]
    fn test_partition() {
        let messages = vec![
            create_test_message("tasks.add"),
            create_test_message("email.send"),
            create_test_message("tasks.subtract"),
        ];

        let (task_messages, other_messages) =
            partition(messages, |msg| msg.headers.task.starts_with("tasks."));

        assert_eq!(task_messages.len(), 2);
        assert_eq!(other_messages.len(), 1);
    }

    #[test]
    fn test_message_batch_into_iterator() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));
        batch.push(create_test_message("task3"));

        let mut count = 0;
        for msg in batch {
            assert!(!msg.headers.task.is_empty());
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_message_batch_into_iterator_ref() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        let mut count = 0;
        for msg in &batch {
            assert!(!msg.headers.task.is_empty());
            count += 1;
        }
        assert_eq!(count, 2);
        assert_eq!(batch.len(), 2); // Batch still exists
    }

    #[test]
    fn test_message_batch_into_iterator_mut() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        for msg in &mut batch {
            msg.headers.retries = Some(1);
        }

        for msg in &batch {
            assert_eq!(msg.headers.retries, Some(1));
        }
    }

    #[test]
    fn test_message_batch_index() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));
        batch.push(create_test_message("task3"));

        assert_eq!(batch[0].headers.task, "task1");
        assert_eq!(batch[1].headers.task, "task2");
        assert_eq!(batch[2].headers.task, "task3");
    }

    #[test]
    fn test_message_batch_index_mut() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        batch[0].headers.retries = Some(5);
        batch[1].headers.retries = Some(10);

        assert_eq!(batch[0].headers.retries, Some(5));
        assert_eq!(batch[1].headers.retries, Some(10));
    }

    #[test]
    fn test_message_batch_extend() {
        let mut batch = MessageBatch::with_capacity(5);
        batch.push(create_test_message("task1"));

        let new_messages = vec![create_test_message("task2"), create_test_message("task3")];

        batch.extend(new_messages);
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_message_batch_extend_with_capacity_limit() {
        let mut batch = MessageBatch::with_capacity(3);
        batch.push(create_test_message("task1"));

        let new_messages = vec![
            create_test_message("task2"),
            create_test_message("task3"),
            create_test_message("task4"), // This should not be added (over capacity)
        ];

        batch.extend(new_messages);
        assert_eq!(batch.len(), 3);
        assert!(batch.is_full());
    }

    #[test]
    fn test_message_batch_as_ref() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        let slice: &[Message] = batch.as_ref();
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0].headers.task, "task1");
    }

    #[test]
    fn test_message_batch_as_mut() {
        let mut batch = MessageBatch::new();
        batch.push(create_test_message("task1"));
        batch.push(create_test_message("task2"));

        let slice: &mut [Message] = batch.as_mut();
        slice[0].headers.retries = Some(99);

        assert_eq!(batch[0].headers.retries, Some(99));
    }

    #[test]
    fn test_message_batch_iterator_chain() {
        let messages = vec![
            create_test_message("task1"),
            create_test_message("task2"),
            create_test_message("task3"),
            create_test_message("task4"),
        ];

        let batch: MessageBatch = messages.into_iter().collect();
        assert_eq!(batch.len(), 4);

        let task_names: Vec<String> = batch
            .into_iter()
            .map(|msg| msg.headers.task.clone())
            .collect();

        assert_eq!(task_names, vec!["task1", "task2", "task3", "task4"]);
    }
}
