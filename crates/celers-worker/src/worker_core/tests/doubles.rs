//! Shared test doubles and helpers for the [`super::super`] regression suite.
//!
//! Every submodule under `worker_core::tests` pulls its mocks and fixtures
//! from here via `use super::doubles::*;` so each one stays focused on the
//! behaviour it actually pins.

use celers_core::{
    Broker, BrokerMessage, Event, EventEmitter, Result, SerializedTask, Task, TaskEvent, TaskId,
};

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

// --------------------------------------------------------------------------
// Test doubles
// --------------------------------------------------------------------------

/// A broker that records every interaction and (optionally) redelivers
/// whatever is enqueued, so a worker-driven retry actually comes back around.
pub(super) struct RecordingBroker {
    pending: Mutex<VecDeque<BrokerMessage>>,
    enqueued: Mutex<Vec<(SerializedTask, u64)>>,
    acked: Mutex<Vec<TaskId>>,
    requeued: Mutex<Vec<TaskId>>,
    rejected: Mutex<Vec<TaskId>>,
    redeliver: bool,
}

impl RecordingBroker {
    pub(super) fn new(messages: Vec<BrokerMessage>, redeliver: bool) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(messages.into()),
            enqueued: Mutex::new(Vec::new()),
            acked: Mutex::new(Vec::new()),
            requeued: Mutex::new(Vec::new()),
            rejected: Mutex::new(Vec::new()),
            redeliver,
        })
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.lock().expect("lock").len()
    }

    pub(super) fn enqueued(&self) -> Vec<(SerializedTask, u64)> {
        self.enqueued.lock().expect("lock").clone()
    }

    pub(super) fn acked(&self) -> Vec<TaskId> {
        self.acked.lock().expect("lock").clone()
    }

    pub(super) fn requeued(&self) -> Vec<TaskId> {
        self.requeued.lock().expect("lock").clone()
    }

    pub(super) fn rejected(&self) -> Vec<TaskId> {
        self.rejected.lock().expect("lock").clone()
    }
}

#[async_trait::async_trait]
impl Broker for RecordingBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        self.enqueued.lock().expect("lock").push((task.clone(), 0));
        if self.redeliver {
            self.pending
                .lock()
                .expect("lock")
                .push_back(BrokerMessage::new(task));
        }
        Ok(task_id)
    }

    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        // Record the requested delay, then make the task immediately available
        // again: tests assert on the *requested* backoff instead of sleeping
        // through it.
        let task_id = task.metadata.id;
        self.enqueued
            .lock()
            .expect("lock")
            .push((task.clone(), delay_secs));
        if self.redeliver {
            self.pending
                .lock()
                .expect("lock")
                .push_back(BrokerMessage::new(task));
        }
        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        Ok(self.pending.lock().expect("lock").pop_front())
    }

    async fn ack(&self, task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        self.acked.lock().expect("lock").push(*task_id);
        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        _receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        if requeue {
            self.requeued.lock().expect("lock").push(*task_id);
        } else {
            self.rejected.lock().expect("lock").push(*task_id);
        }
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(self.pending_len())
    }

    async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
        Ok(false)
    }
}

/// A broker that redelivers the same message forever, so a deferral loop is
/// observable.
pub(super) struct AlwaysRedeliverBroker {
    message: BrokerMessage,
    dequeues: AtomicUsize,
}

impl AlwaysRedeliverBroker {
    pub(super) fn new(message: BrokerMessage) -> Arc<Self> {
        Arc::new(Self {
            message,
            dequeues: AtomicUsize::new(0),
        })
    }

    pub(super) fn dequeues(&self) -> usize {
        self.dequeues.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Broker for AlwaysRedeliverBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        Ok(task.metadata.id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        self.dequeues.fetch_add(1, Ordering::Relaxed);
        Ok(Some(self.message.clone()))
    }

    async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(1)
    }

    async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
        Ok(false)
    }
}

/// Event emitter that records what it received and how it was delivered.
#[derive(Clone, Default)]
pub(super) struct CapturingEmitter {
    events: Arc<Mutex<Vec<Event>>>,
    batch_calls: Arc<AtomicUsize>,
}

impl CapturingEmitter {
    pub(super) fn events(&self) -> Vec<Event> {
        self.events.lock().expect("lock").clone()
    }

    pub(super) fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::Relaxed)
    }

    pub(super) fn task_event_names(&self) -> Vec<&'static str> {
        self.events()
            .iter()
            .filter_map(|event| match event {
                Event::Task(TaskEvent::Received { .. }) => Some("received"),
                Event::Task(TaskEvent::Started { .. }) => Some("started"),
                Event::Task(TaskEvent::Succeeded { .. }) => Some("succeeded"),
                Event::Task(TaskEvent::Failed { .. }) => Some("failed"),
                Event::Task(TaskEvent::Retried { .. }) => Some("retried"),
                Event::Task(TaskEvent::Rejected { .. }) => Some("rejected"),
                Event::Task(TaskEvent::Revoked { .. }) => Some("revoked"),
                Event::Task(TaskEvent::SoftTimeLimitExceeded { .. }) => {
                    Some("soft-time-limit-exceeded")
                }
                Event::Task(TaskEvent::Sent { .. }) | Event::Worker(_) => None,
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl EventEmitter for CapturingEmitter {
    async fn emit(&self, event: Event) -> Result<()> {
        self.events.lock().expect("lock").push(event);
        Ok(())
    }

    async fn emit_batch(&self, events: Vec<Event>) -> Result<()> {
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        self.events.lock().expect("lock").extend(events);
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

// --------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub(super) struct Empty {}

/// Completes immediately, counting executions.
pub(super) struct CountingTask {
    pub(super) runs: Arc<AtomicUsize>,
    pub(super) name: &'static str,
}

#[async_trait::async_trait]
impl Task for CountingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// Always fails, counting attempts.
pub(super) struct AlwaysFailingTask {
    pub(super) runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for AlwaysFailingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        Err(celers_core::CelersError::TaskExecution(
            "deliberate failure".to_string(),
        ))
    }

    fn name(&self) -> &'static str {
        "failing_task"
    }
}

/// Panics inside the handler.
pub(super) struct PanickingTask {
    pub(super) runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for PanickingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        panic!("handler exploded");
    }

    fn name(&self) -> &'static str {
        "panicking_task"
    }
}

/// Blocks until released, so in-flight state is observable.
pub(super) struct BlockingTask {
    pub(super) started: Arc<AtomicUsize>,
    pub(super) finished: Arc<AtomicUsize>,
    pub(super) release: Arc<Notify>,
}

#[async_trait::async_trait]
impl Task for BlockingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.started.fetch_add(1, Ordering::Relaxed);
        self.release.notified().await;
        self.finished.fetch_add(1, Ordering::Relaxed);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "blocking_task"
    }
}

/// Returns a result far larger than a small configured limit.
pub(super) struct BigResultTask;

#[async_trait::async_trait]
impl Task for BigResultTask {
    type Input = Empty;
    type Output = String;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        Ok("x".repeat(4096))
    }

    fn name(&self) -> &'static str {
        "big_result_task"
    }
}

// --------------------------------------------------------------------------

pub(super) fn serialized(name: &str) -> SerializedTask {
    SerializedTask::new(
        name.to_string(),
        serde_json::to_vec(&Empty {}).expect("serialize empty"),
    )
}

/// Poll `cond` until it holds, panicking after ~5 seconds.
pub(super) async fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for: {label}");
}
