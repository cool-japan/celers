//! Regression tests for the worker execution loop.
//!
//! Every test here pins behaviour that was previously broken: unbounded
//! concurrency, panics skipping cleanup, shutdown abandoning in-flight work,
//! retries that never advanced, admission deferrals spinning the loop, and the
//! configuration options the runtime silently ignored.

use super::support::{
    clamp_defer_delay, effective_max_retries, panic_message, schedulable_delay_secs,
    InFlightRegistry,
};
use super::Worker;

use crate::affinity::{AffinityRegistry, TaskAffinity};
use crate::coordinated_rate_limit::WorkerRateLimitCoordinator;
use crate::feature_flags::{FeatureFlags, TaskFeatureRequirements};
use crate::routing::{RoutingStrategy, WorkerTags};
use crate::types::{WorkerConfig, WorkerStats};
use crate::WorkerLabels;

use celers_core::rate_limit::RateLimitConfig;
use celers_core::rate_limit_distributed::InMemoryDistributedBackend;
use celers_core::{
    Broker, BrokerMessage, Event, EventEmitter, NoOpEventEmitter, Result, SerializedTask, Task,
    TaskEvent, TaskId, TaskRegistry, TaskState,
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
struct RecordingBroker {
    pending: Mutex<VecDeque<BrokerMessage>>,
    enqueued: Mutex<Vec<(SerializedTask, u64)>>,
    acked: Mutex<Vec<TaskId>>,
    requeued: Mutex<Vec<TaskId>>,
    rejected: Mutex<Vec<TaskId>>,
    redeliver: bool,
}

impl RecordingBroker {
    fn new(messages: Vec<BrokerMessage>, redeliver: bool) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(messages.into()),
            enqueued: Mutex::new(Vec::new()),
            acked: Mutex::new(Vec::new()),
            requeued: Mutex::new(Vec::new()),
            rejected: Mutex::new(Vec::new()),
            redeliver,
        })
    }

    fn pending_len(&self) -> usize {
        self.pending.lock().expect("lock").len()
    }

    fn enqueued(&self) -> Vec<(SerializedTask, u64)> {
        self.enqueued.lock().expect("lock").clone()
    }

    fn acked(&self) -> Vec<TaskId> {
        self.acked.lock().expect("lock").clone()
    }

    fn requeued(&self) -> Vec<TaskId> {
        self.requeued.lock().expect("lock").clone()
    }

    fn rejected(&self) -> Vec<TaskId> {
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
struct AlwaysRedeliverBroker {
    message: BrokerMessage,
    dequeues: AtomicUsize,
}

impl AlwaysRedeliverBroker {
    fn new(message: BrokerMessage) -> Arc<Self> {
        Arc::new(Self {
            message,
            dequeues: AtomicUsize::new(0),
        })
    }

    fn dequeues(&self) -> usize {
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
struct CapturingEmitter {
    events: Arc<Mutex<Vec<Event>>>,
    batch_calls: Arc<AtomicUsize>,
}

impl CapturingEmitter {
    fn events(&self) -> Vec<Event> {
        self.events.lock().expect("lock").clone()
    }

    fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::Relaxed)
    }

    fn task_event_names(&self) -> Vec<&'static str> {
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
                _ => None,
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
// Test tasks
// --------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct Empty {}

/// Completes immediately, counting executions.
struct CountingTask {
    runs: Arc<AtomicUsize>,
    name: &'static str,
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
struct AlwaysFailingTask {
    runs: Arc<AtomicUsize>,
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
struct PanickingTask {
    runs: Arc<AtomicUsize>,
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
struct BlockingTask {
    started: Arc<AtomicUsize>,
    finished: Arc<AtomicUsize>,
    release: Arc<Notify>,
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
struct BigResultTask;

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
// Helpers
// --------------------------------------------------------------------------

fn serialized(name: &str) -> SerializedTask {
    SerializedTask::new(
        name.to_string(),
        serde_json::to_vec(&Empty {}).expect("serialize empty"),
    )
}

/// Poll `cond` until it holds, panicking after ~5 seconds.
async fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for: {label}");
}

// --------------------------------------------------------------------------
// Unit tests: decision helpers
// --------------------------------------------------------------------------

#[test]
fn test_effective_max_retries_is_capped_by_the_worker_budget() {
    // idx 182: `DynamicConfig::max_retries` (what `set_max_retries` writes) was
    // never consulted -- only the task's own value.
    assert_eq!(effective_max_retries(10, 3), 3);
    assert_eq!(effective_max_retries(1, 3), 1);
    assert_eq!(effective_max_retries(0, 3), 0);
}

#[test]
fn test_schedulable_delay_rounds_up_and_rejects_subsecond() {
    // Truncating 1.9s to 1s would shorten every backoff; truncating 900ms to 0
    // would remove it entirely.
    assert_eq!(schedulable_delay_secs(Duration::from_millis(900)), None);
    assert_eq!(schedulable_delay_secs(Duration::from_secs(1)), Some(1));
    assert_eq!(schedulable_delay_secs(Duration::from_millis(1900)), Some(2));
    assert_eq!(schedulable_delay_secs(Duration::from_secs(60)), Some(60));
}

#[test]
fn test_clamp_defer_delay_bounds_an_unbounded_retry_hint() {
    // A zero-rate limiter reports an effectively infinite `retry_after`;
    // sleeping on it verbatim would park the dequeue loop for hours.
    let clamped = clamp_defer_delay(Duration::from_secs(86_400), 100, 2_000);
    assert!(
        clamped <= Duration::from_millis(2_000),
        "clamped delay {clamped:?} must respect the configured maximum"
    );
    assert!(clamped >= Duration::from_millis(100));

    let floored = clamp_defer_delay(Duration::from_millis(1), 100, 2_000);
    assert!(floored >= Duration::from_millis(100));

    assert_eq!(clamp_defer_delay(Duration::ZERO, 0, 0), Duration::ZERO);
}

#[test]
fn test_panic_message_extracts_both_payload_shapes() {
    assert_eq!(panic_message(&"boom"), "boom");
    assert_eq!(panic_message(&"boom".to_string()), "boom");
    assert_eq!(panic_message(&42_u8), "task panicked");
}

#[test]
fn test_in_flight_registry_claims_exactly_once() {
    // The registry entry is the disposition token: the shutdown requeue and the
    // task's own ack must not both act on the same message.
    let registry = InFlightRegistry::new();
    let task_id = TaskId::new_v4();
    registry.register(task_id, Some("receipt".to_string()));

    assert_eq!(registry.len(), 1);
    assert!(registry.claim(&task_id));
    assert!(!registry.claim(&task_id));
    assert!(registry.is_empty());
    assert!(registry.take_all().is_empty());
}

#[test]
fn test_worker_stats_active_count_saturates_at_zero() {
    // An unbalanced decrement used to wrap to u64::MAX, hanging every drain.
    let stats = WorkerStats::new();
    stats.task_completed();
    assert_eq!(stats.active(), 0);
    assert_eq!(stats.processed(), 1);

    stats.task_started();
    stats.task_completed();
    stats.task_completed();
    assert_eq!(stats.active(), 0);
}

#[tokio::test]
async fn test_backoff_delay_does_not_overflow_for_large_retry_counts() {
    // `base * 2u64.pow(retry_count)` panicked on overflow for retry_count >= 64.
    let broker = RecordingBroker::new(Vec::new(), false);
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(broker, TaskRegistry::new(), WorkerConfig::default());

    assert_eq!(worker.calculate_backoff_delay(0).as_millis(), 1000);
    assert_eq!(worker.calculate_backoff_delay(1).as_millis(), 2000);
    assert_eq!(worker.calculate_backoff_delay(64).as_millis(), 60_000);
    assert_eq!(worker.calculate_backoff_delay(u32::MAX).as_millis(), 60_000);
}

// --------------------------------------------------------------------------
// Loop-level regression tests
// --------------------------------------------------------------------------

/// idx 158: a panicking handler must not skip cleanup. The task is failed like
/// any other error (so it is acked/rejected rather than stranded), the active
/// count returns to zero and the panic is counted.
#[tokio::test]
async fn test_panicking_task_is_failed_and_cleans_up() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(PanickingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    let task = serialized("panicking_task").with_max_retries(0);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("panicking task to be rejected", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(broker.rejected(), vec![task_id]);
    assert!(
        broker.acked().is_empty(),
        "a panicked task must not be acked"
    );
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(stats.panicked(), 1, "the panic must be counted");

    // Cleanup ran: the active counter is back to zero, so drain can complete.
    wait_until("active count to return to zero", || stats.active() == 0).await;
    assert_eq!(stats.processed(), 1);

    handle.shutdown().await.expect("shutdown");
}

/// idx 166 + 167: the worker itself advances the retry state and applies the
/// configured backoff, so a permanently failing task terminates after exactly
/// `max_retries` attempts even on a broker whose `reject(requeue = true)`
/// returns the task unchanged.
#[tokio::test]
async fn test_retry_advances_state_and_terminates_after_max_retries() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // One retry allowed: attempt 0 re-enqueues with Retrying(1), attempt 1 is
    // terminal.
    let task = serialized("failing_task").with_max_retries(1);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("failing task to dead-letter", || {
        !broker.rejected().is_empty()
    })
    .await;

    // Exactly two executions: the original and one retry.
    assert_eq!(
        runs.load(Ordering::Relaxed),
        2,
        "task must stop after its retry budget is spent"
    );

    let enqueued = broker.enqueued();
    assert_eq!(enqueued.len(), 1, "exactly one retry copy is re-enqueued");
    let (retry_task, delay_secs) = &enqueued[0];
    assert_eq!(retry_task.metadata.id, task_id, "retry keeps the task id");
    assert_eq!(
        retry_task.metadata.state,
        TaskState::Retrying(1),
        "the worker must advance the retry state itself"
    );
    assert_eq!(
        *delay_secs, 1,
        "the retry must be scheduled with the configured backoff"
    );

    // The original delivery was acked (the retry copy replaced it) and the
    // final attempt was dead-lettered.
    assert_eq!(broker.acked(), vec![task_id]);
    assert_eq!(broker.rejected(), vec![task_id]);
    assert_eq!(dlq.size().await, 1, "the terminal failure lands in the DLQ");

    handle.shutdown().await.expect("shutdown");
}

/// idx 159 + 295: `concurrency` is enforced, and the permit is taken *before*
/// dequeuing, so a saturated worker leaves messages in the broker instead of
/// draining the queue into RAM.
#[tokio::test]
async fn test_concurrency_limit_applies_backpressure_to_the_broker() {
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let registry = TaskRegistry::new();
    registry
        .register(BlockingTask {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
            release: Arc::clone(&release),
        })
        .await;

    let messages: Vec<BrokerMessage> = (0..5)
        .map(|_| BrokerMessage::new(serialized("blocking_task")))
        .collect();
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("two tasks to be in flight", || {
        started.load(Ordering::Relaxed) == 2
    })
    .await;

    // While both permits are held nothing else may start, and the remaining
    // messages must still be in the broker.
    for _ in 0..20 {
        assert!(
            started.load(Ordering::Relaxed) <= 2,
            "concurrency limit exceeded: {} tasks started",
            started.load(Ordering::Relaxed)
        );
        assert_eq!(stats.active(), 2);
        assert_eq!(
            broker.pending_len(),
            3,
            "a saturated worker must not keep dequeuing"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Release everything: the rest of the queue drains.
    release.notify_waiters();
    wait_until("all tasks to complete", || {
        release.notify_waiters();
        finished.load(Ordering::Relaxed) == 5
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 160: shutdown waits for in-flight work and, once the deadline expires,
/// hands the still-undisposed message back to the broker instead of stranding
/// it. The late-finishing task must not then ack it.
#[tokio::test]
async fn test_shutdown_requeues_tasks_that_outlive_the_drain_deadline() {
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let registry = TaskRegistry::new();
    registry
        .register(BlockingTask {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
            release: Arc::clone(&release),
        })
        .await;

    let task = serialized("blocking_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        concurrency: 1,
        poll_interval_ms: 10,
        shutdown_timeout_secs: 1,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to start", || started.load(Ordering::Relaxed) == 1).await;

    handle.shutdown().await.expect("shutdown");

    wait_until("the in-flight message to be requeued", || {
        !broker.requeued().is_empty()
    })
    .await;
    assert_eq!(broker.requeued(), vec![task_id]);

    // The task finishes afterwards: it lost the disposition race and must not
    // ack a message the broker has already redelivered.
    release.notify_waiters();
    wait_until("the straggler to finish", || {
        release.notify_waiters();
        finished.load(Ordering::Relaxed) == 1
    })
    .await;
    assert!(
        broker.acked().is_empty(),
        "a requeued message must not also be acked"
    );
}

/// idx 162: an admission deferral must not spin the dequeue loop. With a long
/// deferral delay configured, a broker that always redelivers is polled once,
/// not thousands of times.
#[tokio::test]
async fn test_admission_deferral_backs_off_instead_of_spinning() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "gpu_task",
        })
        .await;

    let broker = AlwaysRedeliverBroker::new(BrokerMessage::new(serialized("gpu_task")));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        defer_delay_ms: 3_000,
        defer_max_delay_ms: 3_000,
        worker_labels: WorkerLabels::from_iter(["cpu"]),
        ..Default::default()
    };
    let affinity =
        AffinityRegistry::new().with_task("gpu_task", TaskAffinity::new().require("gpu"));
    let worker: Worker<AlwaysRedeliverBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config).with_affinity(affinity);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the first deferral", || stats.deferred() >= 1).await;

    // The configured back-off is 3s, so over the next 200ms the loop must stay
    // parked rather than re-popping the same message.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        stats.deferred(),
        1,
        "deferral must back off, not spin (dequeues: {})",
        broker.dequeues()
    );
    assert_eq!(runs.load(Ordering::Relaxed), 0, "task must not execute");

    handle.shutdown().await.expect("shutdown");
}

/// idx 182: `max_result_size_bytes` is enforced. An oversized result fails the
/// task terminally (event + DLQ + reject) instead of being stored unchecked.
#[tokio::test]
async fn test_oversized_result_fails_the_task() {
    let registry = TaskRegistry::new();
    registry.register(BigResultTask).await;

    let task = serialized("big_result_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        max_result_size_bytes: 64,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the oversized result to be rejected", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(broker.rejected(), vec![task_id]);
    assert!(
        broker.acked().is_empty(),
        "an oversized result must not be acked as success"
    );
    assert_eq!(dlq.size().await, 1);

    handle.shutdown().await.expect("shutdown");
}

/// idx 172: the circuit breaker sees *every* failed execution, not just the
/// final retries-exhausted one. With a threshold of 2, two failed attempts of
/// the same task must trip the circuit — previously it took
/// `threshold * (max_retries + 1)` real failures.
#[tokio::test]
async fn test_circuit_breaker_counts_every_failed_attempt() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // One retry: two executions in total, both failures.
    let task = serialized("failing_task").with_max_retries(1);
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_circuit_breaker: true,
        circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let breaker = worker.circuit_breaker.clone().expect("breaker enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let mut opened = false;
    for _ in 0..400 {
        if breaker.get_state("failing_task").await.is_open() {
            opened = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        opened,
        "two failed executions must trip a threshold-2 breaker (runs: {})",
        runs.load(Ordering::Relaxed)
    );
    assert_eq!(
        runs.load(Ordering::Relaxed),
        2,
        "the retry budget allows exactly two attempts"
    );

    handle.shutdown().await.expect("shutdown");
}

/// idx 173: an open circuit is a terminal, observable outcome — `task-failed`
/// plus a DLQ entry — not a silent disappearance.
#[tokio::test]
async fn test_open_circuit_emits_failure_and_dead_letters() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let task = serialized("quick_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_circuit_breaker: true,
        circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            ..Default::default()
        },
        enable_dlq: true,
        enable_events: true,
        ..Default::default()
    };
    let emitter = CapturingEmitter::default();
    let worker =
        Worker::with_event_emitter_from_arc(Arc::clone(&broker), registry, config, emitter.clone());
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let breaker = worker.circuit_breaker.clone().expect("breaker enabled");

    // Trip the breaker before the worker sees the message.
    breaker.record_failure("quick_task").await;

    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to be dead-lettered", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(broker.rejected(), vec![task_id]);
    assert_eq!(runs.load(Ordering::Relaxed), 0, "the task must not run");
    assert_eq!(dlq.size().await, 1, "an open circuit records a DLQ entry");

    wait_until("a task-failed event", || {
        emitter.task_event_names().contains(&"failed")
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 176: the cluster-wide rate limiter is keyed by the worker's configured
/// queue, not by a hard-coded `"default"` literal.
#[tokio::test]
async fn test_rate_limit_uses_the_configured_queue_name() {
    let backend: Arc<dyn celers_core::rate_limit_distributed::DistributedRateLimitBackend> =
        Arc::new(InMemoryDistributedBackend::new());
    let coordinator = WorkerRateLimitCoordinator::new(
        Arc::clone(&backend),
        RateLimitConfig::new(0.0).with_burst(1),
    )
    .with_key_strategy(crate::coordinated_rate_limit::RateLimitKeyStrategy::Queue);

    // Exhaust the budget of the "images" queue only.
    assert!(coordinator
        .acquire("quick_task", "images")
        .await
        .expect("acquire")
        .is_allowed());

    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let task = serialized("quick_task");
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        queue_name: "images".to_string(),
        defer_delay_ms: 10,
        defer_max_delay_ms: 20,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_rate_limit_coordinator(coordinator);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to be rate limited", || stats.rate_limited() >= 1).await;
    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "the queue's budget was exhausted, so nothing may run"
    );

    handle.shutdown().await.expect("shutdown");
}

/// idx 182: feature-flag admission. A worker missing a required feature defers
/// the task instead of executing it.
#[tokio::test]
async fn test_feature_requirements_gate_execution() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let broker = AlwaysRedeliverBroker::new(BrokerMessage::new(serialized("quick_task")));

    let mut requirements = std::collections::HashMap::new();
    requirements.insert(
        "quick_task".to_string(),
        TaskFeatureRequirements::new().require("gpu"),
    );

    let config = WorkerConfig {
        poll_interval_ms: 10,
        defer_delay_ms: 3_000,
        defer_max_delay_ms: 3_000,
        feature_flags: FeatureFlags::from_features(["cpu"]),
        task_feature_requirements: requirements,
        ..Default::default()
    };
    let worker: Worker<AlwaysRedeliverBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the feature-gated deferral", || stats.deferred() >= 1).await;
    assert_eq!(runs.load(Ordering::Relaxed), 0);

    handle.shutdown().await.expect("shutdown");
}

/// idx 182: `routing_strategy` is honoured. Under `Strict` a task type that is
/// not on the worker's allow-list is deferred, where `Lenient` admits it.
#[tokio::test]
async fn test_strict_routing_requires_an_explicit_allow_list_entry() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let broker = AlwaysRedeliverBroker::new(BrokerMessage::new(serialized("quick_task")));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        defer_delay_ms: 3_000,
        defer_max_delay_ms: 3_000,
        enable_routing: true,
        routing_strategy: RoutingStrategy::Strict,
        worker_tags: WorkerTags::new().with_task_type("other_task"),
        ..Default::default()
    };
    let worker: Worker<AlwaysRedeliverBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the routing deferral", || stats.deferred() >= 1).await;
    assert_eq!(runs.load(Ordering::Relaxed), 0);
    handle.shutdown().await.expect("shutdown");

    // The same worker under the default (lenient) strategy runs the task.
    let runs_lenient = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs_lenient),
            name: "quick_task",
        })
        .await;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(serialized("quick_task"))], false);
    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_routing: true,
        routing_strategy: RoutingStrategy::Lenient,
        worker_tags: WorkerTags::new(),
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the lenient worker to run the task", || {
        runs_lenient.load(Ordering::Relaxed) == 1
    })
    .await;
    handle.shutdown().await.expect("shutdown");
}

/// idx 161: with `coalesce_require_same_task_id`, distinct submissions that
/// happen to share arguments all run — only true redelivery duplicates are
/// collapsed, so no caller is left waiting on a result that never comes.
#[tokio::test]
async fn test_id_scoped_coalescing_keeps_distinct_submissions() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    // Three distinct ids with identical payloads, plus one true duplicate of
    // the first delivery.
    let first = serialized("quick_task");
    let duplicate = first.clone();
    let messages = vec![
        BrokerMessage::new(first),
        BrokerMessage::new(duplicate),
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("quick_task")),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_batch_dequeue: true,
        batch_size: 10,
        enable_coalescing: true,
        coalesce_require_same_task_id: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    // Three distinct tasks execute; only the repeated delivery is coalesced.
    wait_until("all distinct submissions to run", || {
        runs.load(Ordering::Relaxed) == 3
    })
    .await;
    wait_until("every message to be acked", || broker.acked().len() == 4).await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 301: lifecycle events leave the critical path through the buffered
/// emitter (`emit_batch`) while keeping their per-task order.
#[tokio::test]
async fn test_lifecycle_events_are_emitted_in_order_via_batches() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let broker = RecordingBroker::new(vec![BrokerMessage::new(serialized("quick_task"))], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_events: true,
        ..Default::default()
    };
    let emitter = CapturingEmitter::default();
    let worker =
        Worker::with_event_emitter_from_arc(Arc::clone(&broker), registry, config, emitter.clone());
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task lifecycle events", || {
        let names = emitter.task_event_names();
        names.contains(&"received") && names.contains(&"started") && names.contains(&"succeeded")
    })
    .await;

    let names = emitter.task_event_names();
    let received = names.iter().position(|n| *n == "received");
    let started = names.iter().position(|n| *n == "started");
    let succeeded = names.iter().position(|n| *n == "succeeded");
    assert!(
        received < started && started < succeeded,
        "per-task event order must be preserved, got {names:?}"
    );
    assert!(
        emitter.batch_calls() >= 1,
        "task events must be flushed through emit_batch, not one round trip each"
    );

    handle.shutdown().await.expect("shutdown");
}
