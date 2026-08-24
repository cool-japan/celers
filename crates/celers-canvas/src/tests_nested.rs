//! Integration tests for the nested-workflow canvas elements.
//!
//! Split out of `tests_advanced.rs` to keep every source file under the
//! 2000-line ceiling. These exercise [`NestedChain`](crate::NestedChain) and
//! [`NestedGroup`](crate::NestedGroup) against a recording broker: what reaches
//! the broker, and which compositions are refused because they cannot be
//! sequenced.

#![cfg(test)]

use crate::*;
use std::sync::{Arc, Mutex};

/// Broker recording the name of every enqueued task, in order.
#[derive(Clone, Default)]
struct MockBroker {
    tasks: Arc<Mutex<Vec<String>>>,
}

impl MockBroker {
    fn new() -> Self {
        Self::default()
    }

    fn task_count(&self) -> usize {
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

#[async_trait::async_trait]
impl celers_core::Broker for MockBroker {
    async fn enqueue(
        &self,
        task: celers_core::SerializedTask,
    ) -> celers_core::Result<celers_core::TaskId> {
        let task_id = task.metadata.id;
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(task.metadata.name.clone());
        Ok(task_id)
    }

    async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
        Ok(None)
    }

    async fn ack(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> celers_core::Result<usize> {
        Ok(self.task_count())
    }

    async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
        Ok(true)
    }
}

// ===== NestedChain Tests =====

#[tokio::test]
async fn test_nested_chain_apply() {
    let broker = MockBroker::new();

    // A NestedChain of linear elements is dispatched as one real chain:
    // only the head reaches the broker, the rest ride in its tail.
    let nested_chain = NestedChain::new()
        .then("task1", vec![serde_json::json!(1)])
        .then("task2", vec![serde_json::json!(2)])
        .then("task3", vec![serde_json::json!(3)]);

    let result = nested_chain.apply(&broker).await;
    assert!(result.is_ok(), "NestedChain apply should succeed");

    assert_eq!(
        broker.task_count(),
        1,
        "Only the head task is enqueued; steps 2..N travel in its chain tail"
    );
}

#[tokio::test]
async fn test_nested_chain_with_midchain_group_is_rejected() {
    let broker = MockBroker::new();

    // A group in the middle of a chain cannot be sequenced by the link
    // mechanism: `task3` would start immediately, in parallel with the
    // group, rather than after it. That must fail closed.
    let nested_chain = NestedChain::new()
        .then("task1", vec![serde_json::json!(1)])
        .then_group(
            Group::new()
                .add("task2a", vec![serde_json::json!(2)])
                .add("task2b", vec![serde_json::json!(3)]),
        )
        .then("task3", vec![serde_json::json!(4)]);

    assert!(
        nested_chain.validate().is_err(),
        "the composition is rejected at build time"
    );

    let result = nested_chain.apply(&broker).await;
    assert!(result.is_err(), "and again at dispatch time");
    assert_eq!(broker.task_count(), 0, "nothing may be enqueued");
}

#[tokio::test]
async fn test_nested_chain_with_chains() {
    let broker = MockBroker::new();

    let nested_chain = NestedChain::new()
        .then_chain(Chain::new().then("step1", vec![]).then("step2", vec![]))
        .then_chain(Chain::new().then("step3", vec![]).then("step4", vec![]));

    let result = nested_chain.apply(&broker).await;
    assert!(result.is_ok(), "NestedChain with chains should succeed");

    // Both nested chains are flattened into a single four-step chain, so
    // only its head is enqueued now; step2..step4 travel in the tail.
    assert_eq!(
        broker.task_count(),
        1,
        "Should publish only the head of the flattened chain"
    );
}

#[tokio::test]
async fn test_nested_chain_empty_error() {
    let broker = MockBroker::new();
    let nested_chain = NestedChain::new();

    let result = nested_chain.apply(&broker).await;
    assert!(result.is_err(), "Empty NestedChain should return error");
    match result {
        Err(CanvasError::Invalid(msg)) => {
            assert!(msg.contains("empty"));
        }
        _ => panic!("Expected Invalid error for empty NestedChain"),
    }
}

#[test]
fn test_nested_chain_builder_methods() {
    let chain = NestedChain::new()
        .then("task1", vec![])
        .then_signature(Signature::new("task2".to_string()))
        .then_group(Group::new().add("task3", vec![]));

    assert_eq!(chain.len(), 3);
    assert!(!chain.is_empty());
}

#[test]
fn test_nested_chain_display() {
    let chain = NestedChain::new()
        .then("task1", vec![])
        .then("task2", vec![]);

    let display = format!("{}", chain);
    assert!(display.contains("NestedChain"));
    assert!(display.contains("->"));
}

// ===== NestedGroup Tests =====

#[tokio::test]
async fn test_nested_group_apply() {
    let broker = MockBroker::new();

    let nested_group = NestedGroup::new()
        .add("task1", vec![serde_json::json!(1)])
        .add_chain(Chain::new().then("task2a", vec![]).then("task2b", vec![]))
        .add("task3", vec![serde_json::json!(3)]);

    let result = nested_group.apply(&broker).await;
    assert!(result.is_ok(), "NestedGroup apply should succeed");

    // Verify tasks were published
    // Chain only enqueues first task, so: task1 + task2a (from chain) + task3 = 3
    assert_eq!(
        broker.task_count(),
        3,
        "Should publish 3 tasks (task1, task2a from chain, task3)"
    );
}

#[tokio::test]
async fn test_nested_group_with_multiple_chains() {
    let broker = MockBroker::new();

    let nested_group = NestedGroup::new()
        .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
        .add_chain(Chain::new().then("b1", vec![]).then("b2", vec![]))
        .add_chain(Chain::new().then("c1", vec![]).then("c2", vec![]));

    let result = nested_group.apply(&broker).await;
    assert!(
        result.is_ok(),
        "NestedGroup with multiple chains should succeed"
    );

    // Each chain only enqueues the first task (with links)
    // So we expect 3 tasks (one per chain), not 6
    assert_eq!(
        broker.task_count(),
        3,
        "Should publish 3 first tasks from three chains"
    );
}

#[tokio::test]
async fn test_nested_group_empty_error() {
    let broker = MockBroker::new();
    let nested_group = NestedGroup::new();

    let result = nested_group.apply(&broker).await;
    assert!(result.is_err(), "Empty NestedGroup should return error");
    match result {
        Err(CanvasError::Invalid(msg)) => {
            assert!(msg.contains("empty"));
        }
        _ => panic!("Expected Invalid error for empty NestedGroup"),
    }
}

#[test]
fn test_nested_group_builder_methods() {
    let group = NestedGroup::new()
        .add("task1", vec![])
        .add_signature(Signature::new("task2".to_string()))
        .add_chain(Chain::new().then("task3", vec![]));

    assert_eq!(group.len(), 3);
    assert!(!group.is_empty());
}

#[test]
fn test_nested_group_display() {
    let group = NestedGroup::new().add("task1", vec![]).add("task2", vec![]);

    let display = format!("{}", group);
    assert!(display.contains("NestedGroup"));
    assert!(display.contains("|"));
}

#[tokio::test]
async fn test_nested_workflows_complex_composition() {
    let broker = MockBroker::new();

    // A chain of [ Group -> Chain -> Group ] promises that `seq1` runs only
    // after all three `parallel*` tasks are done, and that the final group
    // runs only after `seq2`. The link mechanism cannot express either
    // barrier, so the composition is refused instead of being dispatched
    // all at once with no ordering at all.
    let nested = NestedChain::new()
        .then_group(
            Group::new()
                .add("parallel1", vec![])
                .add("parallel2", vec![])
                .add("parallel3", vec![]),
        )
        .then_chain(Chain::new().then("seq1", vec![]).then("seq2", vec![]))
        .then_element(CanvasElement::Group(
            Group::new().add("final1", vec![]).add("final2", vec![]),
        ));

    assert!(nested.validate().is_err(), "rejected while being built");

    let result = nested.apply(&broker).await;
    assert!(result.is_err(), "and refused at dispatch");
    assert_eq!(
        broker.task_count(),
        0,
        "an unsequenceable workflow must not be partially dispatched"
    );
}

#[tokio::test]
async fn test_nested_workflows_linear_composition_is_one_chain() {
    let broker = MockBroker::new();

    // The sequenceable form of the same intent: everything linear, so the
    // whole thing collapses into one linked chain.
    let nested = NestedChain::new()
        .then("prepare", vec![])
        .then_chain(Chain::new().then("seq1", vec![]).then("seq2", vec![]))
        .then("finish", vec![]);

    let result = nested.apply(&broker).await;
    assert!(result.is_ok(), "a linear composition dispatches");
    assert_eq!(
        broker.task_count(),
        1,
        "only the head is enqueued; the other three steps ride in its tail"
    );
}
