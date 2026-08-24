//! Regression tests for the worker execution loop.
//!
//! Every test here pins behaviour that was previously broken: unbounded
//! concurrency, panics skipping cleanup, shutdown abandoning in-flight work,
//! retries that never advanced, admission deferrals spinning the loop, and the
//! configuration options the runtime silently ignored.

use super::execution::ExecutionLimits;
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
use celers_core::task_security::PayloadHygiene;
use celers_core::time_limit::{TimeLimitConfig, WorkerTimeLimits};
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
    let task = SerializedTask::new("dispose_once".to_string(), vec![1, 2, 3]);
    let task_id = task.metadata.id;
    registry.register(task_id, Some("receipt".to_string()), &task);

    assert_eq!(registry.len(), 1);
    assert!(registry.claim(&task_id));
    assert!(!registry.claim(&task_id));
    assert!(registry.is_empty());
    assert!(registry.take_all().is_empty());
}

#[test]
fn test_in_flight_registry_snapshot_reports_task_identity() {
    // `inspect active` reads this snapshot: the counters know how many tasks
    // are running, only the registry knows which.
    // No payload hygiene configured: the preview is the payload verbatim, which
    // is the baseline the opt-in redaction has to be measured against.
    let registry = InFlightRegistry::with_args_capture(None);
    let task = SerializedTask::new("send_email".to_string(), br#"{"to":"a@b.c"}"#.to_vec());
    let task_id = task.metadata.id;
    registry.register(task_id, None, &task);

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 1);
    let (seen_id, entry) = &snapshot[0];
    assert_eq!(*seen_id, task_id);
    assert_eq!(entry.name, "send_email");
    assert_eq!(entry.args_preview.as_deref(), Some(r#"{"to":"a@b.c"}"#));
    assert!(entry.started > 0.0);
}

#[test]
fn test_in_flight_registry_redacts_the_preview_when_hygiene_is_configured() {
    // Same payload as the baseline above; the only difference is the opt-in.
    let registry = InFlightRegistry::with_args_capture(Some(PayloadHygiene::recommended()));
    let raw = br#"{"to":"alice@example.com","api_token":"sk-live-9"}"#.to_vec();
    let task = SerializedTask::new("send_email".to_string(), raw.clone());
    let task_id = task.metadata.id;
    registry.register(task_id, None, &task);

    let snapshot = registry.snapshot();
    let preview = snapshot[0]
        .1
        .args_preview
        .clone()
        .expect("args capture is on");

    assert!(
        !preview.contains("alice@example.com"),
        "PII survived: {preview}"
    );
    assert!(!preview.contains("sk-live-9"), "secret survived: {preview}");
    assert!(preview.contains("\"to\""), "keys are kept: {preview}");

    // The executing payload is untouched — this is the whole point.
    assert_eq!(task.payload, raw);
}

#[test]
fn test_in_flight_registry_skips_args_capture_by_default() {
    // A worker without remote control must not retain payload copies.
    let registry = InFlightRegistry::new();
    let task = SerializedTask::new("bulky".to_string(), vec![7; 4096]);
    registry.register(task.metadata.id, None, &task);

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot[0].1.args_preview.is_none());
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

// --------------------------------------------------------------------------
// Time limits (idx 42)
// --------------------------------------------------------------------------

/// A task that runs until its soft time limit fires, then wraps up cleanly.
///
/// This is the cooperative shape Celery's `SoftTimeLimitExceeded` exists for:
/// the task is *told* it is out of time and returns partial work rather than
/// being killed.
struct SoftLimitAwareTask {
    /// Set once the task observed its soft-limit signal.
    observed_soft_limit: Arc<AtomicUsize>,
    /// Incremented when the task returns normally.
    finished: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for SoftLimitAwareTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        // Safety valve so a broken signal fails the test instead of hanging it.
        for _ in 0..2_000 {
            if crate::execution_context::check_soft_time_limit().is_err() {
                self.observed_soft_limit.fetch_add(1, Ordering::Relaxed);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        self.finished.fetch_add(1, Ordering::Relaxed);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "soft_limit_aware_task"
    }
}

/// A task that ignores every signal and runs effectively forever.
struct NeverEndingTask {
    started: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for NeverEndingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.started.fetch_add(1, Ordering::Relaxed);
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn name(&self) -> &'static str {
        "never_ending_task"
    }
}

#[test]
fn test_execution_limits_deadline_is_the_earlier_bound() {
    // No time limits: the plain execution timeout is the deadline.
    let plain = ExecutionLimits::from_timeout(30);
    assert_eq!(plain.deadline(), Duration::from_secs(30));
    assert!(!plain.hard_limit_is_binding());

    // A shorter hard limit wins.
    let short_hard = ExecutionLimits::from_timeout(30).with_time_limits(
        &TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(1))
            .with_hard_limit(Duration::from_secs(5)),
    );
    assert_eq!(short_hard.deadline(), Duration::from_secs(5));
    assert!(short_hard.hard_limit_is_binding());
    assert_eq!(short_hard.soft_limit, Some(Duration::from_secs(1)));

    // A longer hard limit does not extend the task's own timeout.
    let long_hard = ExecutionLimits::from_timeout(30)
        .with_time_limits(&TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)));
    assert_eq!(long_hard.deadline(), Duration::from_secs(30));
    assert!(!long_hard.hard_limit_is_binding());

    // Sub-second hard limits survive (they are stored in milliseconds).
    let sub_second = ExecutionLimits::from_timeout(30)
        .with_time_limits(&TimeLimitConfig::new().with_hard_limit(Duration::from_millis(250)));
    assert_eq!(sub_second.deadline(), Duration::from_millis(250));
}

#[test]
fn test_execution_limits_report_the_limit_that_actually_fired() {
    let task_id = TaskId::new_v4();

    let plain = ExecutionLimits::from_timeout(30);
    let failure = plain.timeout_failure(task_id, Duration::from_secs(30));
    assert_eq!(failure.failure_type, "timeout");
    assert!(failure.message.contains("timed out after 30s"));
    assert_eq!(failure.metadata, vec![("timeout_secs", "30".to_string())]);

    let hard = ExecutionLimits::from_timeout(30)
        .with_time_limits(&TimeLimitConfig::new().with_hard_limit(Duration::from_secs(5)));
    let failure = hard.timeout_failure(task_id, Duration::from_secs(5));
    assert_eq!(failure.failure_type, "hard_time_limit");
    assert!(
        failure.message.contains("Hard time limit exceeded"),
        "got {}",
        failure.message
    );
    assert_eq!(
        failure.metadata,
        vec![("hard_limit_millis", "5000".to_string())]
    );
}

/// idx 42: a per-task override merges onto the manager default, and both halves
/// reach the execution loop.
#[tokio::test]
async fn test_worker_resolves_merged_time_limits_per_task_name() {
    let limits = WorkerTimeLimits::with_default(
        TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(30))
            .with_hard_limit(Duration::from_secs(60)),
    );
    limits.set_task_limit(
        "slow_task",
        TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)),
    );

    let broker = RecordingBroker::new(Vec::new(), false);
    let worker: Worker<RecordingBroker, NoOpEventEmitter> = Worker::new_from_arc(
        Arc::clone(&broker),
        TaskRegistry::new(),
        WorkerConfig::default(),
    )
    .with_time_limits(limits);

    let task_id = TaskId::new_v4();
    let slow = worker
        .resolve_time_limits(task_id, "slow_task")
        .expect("the override applies");
    assert_eq!(
        slow.soft_limit(),
        Some(Duration::from_secs(30)),
        "a hard-limit-only override must not drop the default soft limit"
    );
    assert_eq!(slow.hard_limit(), Some(Duration::from_secs(600)));

    let other = worker
        .resolve_time_limits(task_id, "other_task")
        .expect("the default applies");
    assert_eq!(other.hard_limit(), Some(Duration::from_secs(60)));

    // Without a manager configured, nothing is resolved.
    let bare: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(broker, TaskRegistry::new(), WorkerConfig::default());
    assert!(bare.resolve_time_limits(task_id, "slow_task").is_none());
}

/// idx 42: the soft limit is a *warning*, not a disposition. It trips the
/// cooperative signal the task observes, is counted in `WorkerStats`, and the
/// task still finishes successfully (acked, not revoked, not dead-lettered).
#[tokio::test]
async fn test_soft_time_limit_warns_without_killing_the_task() {
    let observed = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(SoftLimitAwareTask {
            observed_soft_limit: Arc::clone(&observed),
            finished: Arc::clone(&finished),
        })
        .await;

    let task = serialized("soft_limit_aware_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config).with_time_limits(
            WorkerTimeLimits::with_default(
                // Soft only: nothing may kill this task.
                TimeLimitConfig::new().with_soft_limit(Duration::from_millis(30)),
            ),
        );
    let stats = worker.stats_arc();
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to finish after its soft limit", || {
        finished.load(Ordering::Relaxed) == 1
    })
    .await;

    assert_eq!(
        observed.load(Ordering::Relaxed),
        1,
        "the running task must observe its own soft time limit"
    );
    assert_eq!(
        stats.soft_timeouts(),
        1,
        "the soft-limit expiry must be counted"
    );
    assert_eq!(stats.revoked(), 0, "a soft limit must never revoke a task");
    wait_until("the successful task to be acked", || {
        broker.acked() == vec![task_id]
    })
    .await;
    assert!(broker.rejected().is_empty());
    assert_eq!(dlq.size().await, 0, "a soft limit is not a failure");

    handle.shutdown().await.expect("shutdown");
}

/// idx 7: a soft-limit expiry is published as a `task-soft-time-limit-exceeded`
/// event. It used to surface only as a `WorkerStats` counter and a `warn!`, so
/// a monitor watching the event stream could not tell a slow task from a
/// healthy one until the hard limit turned it into a failure.
#[tokio::test]
async fn test_soft_time_limit_emits_a_lifecycle_event() {
    let observed = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(SoftLimitAwareTask {
            observed_soft_limit: Arc::clone(&observed),
            finished: Arc::clone(&finished),
        })
        .await;

    let task = serialized("soft_limit_aware_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_events: true,
        hostname: "celery@test-host".to_string(),
        ..Default::default()
    };
    let emitter = CapturingEmitter::default();
    let worker =
        Worker::with_event_emitter_from_arc(Arc::clone(&broker), registry, config, emitter.clone())
            .with_time_limits(WorkerTimeLimits::with_default(
                TimeLimitConfig::new().with_soft_limit(Duration::from_millis(30)),
            ));
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("a task-soft-time-limit-exceeded event", || {
        emitter
            .task_event_names()
            .contains(&"soft-time-limit-exceeded")
    })
    .await;

    let breaches: Vec<Event> = emitter
        .events()
        .into_iter()
        .filter(|event| matches!(event, Event::Task(TaskEvent::SoftTimeLimitExceeded { .. })))
        .collect();
    assert_eq!(
        breaches.len(),
        1,
        "the expiry must be reported exactly once, not once per poll"
    );

    let Event::Task(TaskEvent::SoftTimeLimitExceeded {
        task_id: event_task_id,
        ref task_name,
        ref hostname,
        elapsed_secs,
        limit_secs,
        ..
    }) = breaches[0]
    else {
        panic!("filtered to soft-limit events");
    };
    assert_eq!(event_task_id, task_id);
    assert_eq!(task_name, "soft_limit_aware_task");
    assert_eq!(hostname, "celery@test-host");
    assert!(
        (limit_secs - 0.030).abs() < 1e-9,
        "the event must report the configured limit, got {limit_secs}"
    );
    assert!(
        elapsed_secs >= limit_secs,
        "the task ran at least as long as the limit ({elapsed_secs} < {limit_secs})"
    );

    // The event must also be publishable in the Celery wire shape.
    let wire = breaches[0].to_wire_json().expect("renders to the wire");
    assert!(wire.contains(r#""type":"task-soft-time-limit-exceeded""#));
    assert!(wire.contains(&format!(r#""uuid":"{task_id}""#)));

    wait_until("the task to finish after its soft limit", || {
        finished.load(Ordering::Relaxed) == 1
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 42: the hard limit *is* terminal. It aborts the task and maps onto the
/// worker's existing timeout failure path — dead-lettered (retries exhausted)
/// with a failure class naming the limit that fired.
#[tokio::test]
async fn test_hard_time_limit_aborts_the_task_with_a_timeout_failure() {
    let started = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(NeverEndingTask {
            started: Arc::clone(&started),
        })
        .await;

    // No retry budget: the first hard-limit expiry is terminal.
    let task = serialized("never_ending_task").with_max_retries(0);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        // Far longer than the hard limit, so the hard limit is what fires.
        default_timeout_secs: 300,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config).with_time_limits(
            WorkerTimeLimits::with_default(
                TimeLimitConfig::new().with_hard_limit(Duration::from_millis(50)),
            ),
        );
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the hard limit to kill the task", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(started.load(Ordering::Relaxed), 1);
    assert_eq!(broker.rejected(), vec![task_id]);
    assert!(
        broker.acked().is_empty(),
        "a task killed by its hard limit must not be acked as success"
    );

    let entries = dlq.get_entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].metadata.get("failure_type").map(String::as_str),
        Some("hard_time_limit"),
        "the DLQ entry must name the limit that fired: {:?}",
        entries[0].metadata
    );
    assert_eq!(
        entries[0]
            .metadata
            .get("hard_limit_millis")
            .map(String::as_str),
        Some("50")
    );
    assert!(
        entries[0]
            .error_message
            .contains("Hard time limit exceeded"),
        "got {}",
        entries[0].error_message
    );

    handle.shutdown().await.expect("shutdown");
}

// --------------------------------------------------------------------------
// Workflow continuation
// --------------------------------------------------------------------------

/// The success path must advance the task's workflow. Before this,
/// `workflows::handle_workflow_completion` had zero production callers: chain
/// continuation, branch/switch evaluation and chord barriers were implemented
/// and unit-proven but never invoked by a running worker, so every chain
/// stopped after its first step.
#[cfg(feature = "canvas")]
#[tokio::test]
async fn test_worker_runs_every_step_of_a_canvas_chain() {
    let step_a = Arc::new(AtomicUsize::new(0));
    let step_b = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&step_a),
            name: "chain_step_a",
        })
        .await;
    registry
        .register(CountingTask {
            runs: Arc::clone(&step_b),
            name: "chain_step_b",
        })
        .await;

    // `redeliver: true` makes everything the worker enqueues available for the
    // next dequeue, so the chain's own continuation comes back around.
    let broker = RecordingBroker::new(Vec::new(), true);
    celers_canvas::Chain::new()
        .then("chain_step_a", Vec::new())
        .then("chain_step_b", Vec::new())
        .apply(&*broker)
        .await
        .expect("chain dispatches");

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the chain's second step to run", || {
        step_b.load(Ordering::Relaxed) == 1
    })
    .await;

    assert_eq!(step_a.load(Ordering::Relaxed), 1);
    assert_eq!(
        step_b.load(Ordering::Relaxed),
        1,
        "the worker must enqueue the chain tail after the head succeeds"
    );

    handle.shutdown().await.expect("shutdown");
}

/// The legacy `on_success_link` path (a bare successor *name*, no canvas tail)
/// is driven by the same call site.
#[cfg(feature = "canvas")]
#[tokio::test]
async fn test_worker_enqueues_a_bare_on_success_link() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let task = serialized("quick_task").with_on_success_link("follow_up".to_string());
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the link to be enqueued", || {
        broker
            .enqueued()
            .iter()
            .any(|(task, _)| task.metadata.name == "follow_up")
    })
    .await;

    assert_eq!(runs.load(Ordering::Relaxed), 1);

    handle.shutdown().await.expect("shutdown");
}

// --------------------------------------------------------------------------
// DLQ lifecycle
// --------------------------------------------------------------------------

#[test]
fn test_dlq_cleanup_interval_is_bounded() {
    // Sweeping once per TTL keeps a short TTL honest...
    assert_eq!(
        super::support::dlq_cleanup_interval(30),
        Duration::from_secs(30)
    );
    // ...without letting a multi-day TTL mean "never swept in practice".
    assert_eq!(
        super::support::dlq_cleanup_interval(7 * 24 * 3600),
        Duration::from_secs(3600)
    );
    // `tokio::time::interval` panics on a zero period.
    assert_eq!(
        super::support::dlq_cleanup_interval(0),
        Duration::from_secs(1)
    );
}

/// A configured `ttl_seconds` used to be decoration: nothing ran the sweep, so
/// expired dead-letter entries accumulated for the life of the process.
#[tokio::test]
async fn test_worker_runs_the_dlq_ttl_sweep() {
    let broker = RecordingBroker::new(Vec::new(), false);
    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        dlq_config: crate::dlq::DlqConfig::new(true).with_ttl(1),
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), TaskRegistry::new(), config);
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    // Back-date the entry so the very first sweep reclaims it.
    let mut entry = crate::dlq::DlqEntry::new(
        serialized("stale_task"),
        TaskId::new_v4(),
        0,
        "boom".to_string(),
        "test-host".to_string(),
    );
    entry.dlq_timestamp = entry.dlq_timestamp.saturating_sub(3_600);
    dlq.add_entry(entry).await.expect("entry is recorded");
    assert_eq!(dlq.size().await, 1);

    let mut swept = false;
    for _ in 0..300 {
        if dlq.size().await == 0 {
            swept = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        swept,
        "a configured DLQ TTL must actually reclaim expired entries"
    );

    handle.shutdown().await.expect("shutdown");
}

/// `Worker::connect` opens the configured DLQ backend instead of silently
/// downgrading a persistent dead-letter queue to a volatile in-memory one.
#[tokio::test]
async fn test_connect_opens_the_configured_dlq_backend() {
    let config = WorkerConfig {
        enable_dlq: true,
        dlq_config: crate::dlq::DlqConfig::new(true),
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> = Worker::connect(
        Arc::try_unwrap(RecordingBroker::new(Vec::new(), false))
            .ok()
            .expect("sole owner"),
        TaskRegistry::new(),
        config,
    )
    .await
    .expect("the in-memory backend always connects");

    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    assert!(dlq.is_enabled());
    assert_eq!(dlq.size().await, 0);

    // A backend whose feature is not compiled in is a hard error, not a silent
    // downgrade to memory.
    #[cfg(not(feature = "redis"))]
    {
        let config = WorkerConfig {
            enable_dlq: true,
            dlq_config: crate::dlq::DlqConfig::new(true).with_storage(
                crate::dlq::DlqStorageBackend::Redis {
                    url: "redis://127.0.0.1:6379".to_string(),
                    key_prefix: Some("celers:test".to_string()),
                },
            ),
            ..Default::default()
        };
        let failed: std::result::Result<Worker<RecordingBroker, NoOpEventEmitter>, _> =
            Worker::connect(
                Arc::try_unwrap(RecordingBroker::new(Vec::new(), false))
                    .ok()
                    .expect("sole owner"),
                TaskRegistry::new(),
                config,
            )
            .await;
        assert!(
            failed.is_err(),
            "an unavailable DLQ backend must surface, not degrade to memory"
        );
    }
}

/// idx 161: the *default* coalescing key is id-scoped, so enabling coalescing
/// can no longer silently destroy independent submissions that happen to share
/// their arguments.
#[tokio::test]
async fn test_coalescing_defaults_to_the_lossless_id_scoped_key() {
    assert!(
        WorkerConfig::default().coalesce_require_same_task_id,
        "the default must be the lossless key"
    );

    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    // Three independent submissions with byte-identical payloads.
    let messages = vec![
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("quick_task")),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_batch_dequeue: true,
        batch_size: 10,
        enable_coalescing: true,
        // Deliberately *not* setting `coalesce_require_same_task_id`.
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("every distinct submission to run", || {
        runs.load(Ordering::Relaxed) == 3
    })
    .await;
    wait_until("every message to be acked", || broker.acked().len() == 3).await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 172, worker-loop half: a task rejected because the *half-open* probe
/// budget is in use is deferred (requeued), not dead-lettered. Only a genuinely
/// OPEN circuit is terminal — a recovering circuit must not permanently fail
/// the traffic it is about to start serving again.
#[tokio::test]
async fn test_half_open_probe_budget_defers_instead_of_dead_lettering() {
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

    let messages = vec![
        BrokerMessage::new(serialized("blocking_task")),
        BrokerMessage::new(serialized("blocking_task")),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        concurrency: 4,
        enable_dlq: true,
        enable_circuit_breaker: true,
        circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 5,
            // Already past the recovery window, so the first `should_allow`
            // flips the circuit straight to half-open.
            timeout_secs: 0,
            window_secs: 60,
            half_open_max_concurrent: 1,
        },
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let breaker = worker.circuit_breaker.clone().expect("breaker enabled");
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");

    // Open the circuit before the worker ever polls.
    breaker.record_failure("blocking_task").await;
    assert!(breaker.get_state("blocking_task").await.is_open());

    let handle = worker.run_with_shutdown().await.expect("worker starts");

    // The first task takes the single probe slot and blocks; the second finds
    // the budget spent.
    wait_until("the probe task to start", || {
        started.load(Ordering::Relaxed) == 1
    })
    .await;
    wait_until("the over-budget task to be requeued", || {
        !broker.requeued().is_empty()
    })
    .await;

    assert!(
        broker.rejected().is_empty(),
        "a half-open probe-budget miss must never be dead-lettered"
    );
    assert_eq!(
        dlq.size().await,
        0,
        "a deferred task is not a failed task: {:?}",
        dlq.get_entries().await
    );
    assert!(
        breaker.get_state("blocking_task").await.is_half_open(),
        "the circuit is still probing"
    );

    release.notify_waiters();
    handle.shutdown().await.expect("shutdown");
}

/// The disposition token also owns advancing the workflow: a task whose
/// delivery was already requeued by the shutdown drain must not enqueue its
/// chain successor, or the redelivery enqueues it a second time.
#[cfg(feature = "canvas")]
#[tokio::test]
async fn test_unclaimed_delivery_does_not_double_enqueue_the_chain() {
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

    let task = serialized("blocking_task").with_on_success_link("follow_up".to_string());
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        graceful_shutdown: true,
        // Deadline expires while the task is still blocked, so the drain
        // requeues the delivery and the task loses its claim.
        shutdown_timeout_secs: 1,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to start", || started.load(Ordering::Relaxed) == 1).await;
    handle.shutdown().await.expect("shutdown");

    wait_until("the drain deadline to requeue the delivery", || {
        broker.requeued() == vec![task_id]
    })
    .await;

    // Now let the task finish: it no longer owns the disposition.
    release.notify_waiters();
    wait_until("the task to finish", || {
        finished.load(Ordering::Relaxed) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        broker
            .enqueued()
            .iter()
            .all(|(task, _)| task.metadata.name != "follow_up"),
        "an unclaimed delivery must leave workflow continuation to its redelivery"
    );
    assert!(
        broker.acked().is_empty(),
        "and must not ack a delivery it no longer owns"
    );
}

// --------------------------------------------------------------------------
// Poison-pill quarantine and health accounting
// --------------------------------------------------------------------------

/// A task that keeps failing accumulates strikes and, once quarantined, is
/// dead-lettered without ever executing again — instead of cycling through the
/// broker forever.
#[tokio::test]
async fn test_poison_pill_quarantine_stops_a_repeatedly_failing_task() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // Budget for 5 attempts; quarantine trips after 2 failed executions.
    let task = serialized("failing_task").with_max_retries(5);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let detector = Arc::new(crate::poison_pill::PoisonPillDetector::new(
        crate::poison_pill::PoisonPillConfig::new().with_threshold(2),
    ));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_poison_pill(Arc::clone(&detector));
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let mut quarantined = false;
    for _ in 0..400 {
        if detector.is_poison(&task_id).await {
            quarantined = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        quarantined,
        "two failed executions must trip a threshold-2 detector (runs: {})",
        runs.load(Ordering::Relaxed)
    );

    // Once quarantined the task is dead-lettered on its next delivery instead
    // of being executed again.
    wait_until("the quarantined task to be dead-lettered", || {
        !broker.rejected().is_empty()
    })
    .await;
    let runs_at_quarantine = runs.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        runs.load(Ordering::Relaxed),
        runs_at_quarantine,
        "a quarantined task must never execute again"
    );

    let entries = dlq.get_entries().await;
    assert!(
        entries.iter().any(
            |entry| entry.metadata.get("failure_type").map(String::as_str) == Some("poison_pill")
        ),
        "the quarantine must be observable in the DLQ: {:?}",
        entries
            .iter()
            .map(|entry| entry.metadata.clone())
            .collect::<Vec<_>>()
    );

    handle.shutdown().await.expect("shutdown");
}

/// A re-attempt whose previous failure this worker never saw (another worker's,
/// or one that died mid-task) is the only evidence a poison pill leaves, so it
/// gets its own strike. A retry this worker *did* fail must not be
/// double-counted.
#[tokio::test]
async fn test_redelivery_strike_only_counts_unseen_failures() {
    let broker = RecordingBroker::new(Vec::new(), false);
    let detector = Arc::new(crate::poison_pill::PoisonPillDetector::new(
        crate::poison_pill::PoisonPillConfig::new().with_threshold(10),
    ));
    let worker: Worker<RecordingBroker, NoOpEventEmitter> = Worker::new_from_arc(
        Arc::clone(&broker),
        TaskRegistry::new(),
        WorkerConfig::default(),
    )
    .with_poison_pill(Arc::clone(&detector));

    // A first delivery is never a redelivery.
    let fresh = TaskId::new_v4();
    assert!(!worker.is_quarantined(fresh, 0).await);
    assert_eq!(detector.strike_count(&fresh).await, 0);

    // A re-attempt this detector has no record of: strike.
    let foreign = TaskId::new_v4();
    assert!(!worker.is_quarantined(foreign, 1).await);
    assert_eq!(detector.strike_count(&foreign).await, 1);

    // A re-attempt whose failure this worker already recorded: no second strike.
    let ours = TaskId::new_v4();
    detector.record_failure(ours, "boom").await;
    assert_eq!(detector.strike_count(&ours).await, 1);
    assert!(!worker.is_quarantined(ours, 2).await);
    assert_eq!(
        detector.strike_count(&ours).await,
        1,
        "a failure this worker recorded must not be counted twice on redelivery"
    );

    // Without a detector the gate is a no-op.
    let bare: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(broker, TaskRegistry::new(), WorkerConfig::default());
    assert!(!bare.is_quarantined(TaskId::new_v4(), 9).await);
}

/// Health accounting is fed by real executions and readable through the handle
/// the worker leaves behind, so an embedder can serve liveness/readiness.
#[tokio::test]
async fn test_health_tracks_real_task_outcomes() {
    let ok_runs = Arc::new(AtomicUsize::new(0));
    let bad_runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&ok_runs),
            name: "quick_task",
        })
        .await;
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&bad_runs),
        })
        .await;

    let messages = vec![
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("failing_task").with_max_retries(0)),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let health = worker.health();
    assert_eq!(health.get_health().tasks_processed, 0);

    let handle = worker.run_with_shutdown().await.expect("worker starts");
    // The handle exposes the same shared accounting.
    let from_handle = handle.health();

    wait_until("both tasks to be accounted for", || {
        let info = from_handle.get_health();
        info.tasks_processed == 1 && info.tasks_failed == 1
    })
    .await;

    let info = health.get_health();
    assert_eq!(info.tasks_processed, 1, "one success");
    assert_eq!(info.tasks_failed, 1, "one failure");
    assert_eq!(info.consecutive_failures, 1);
    wait_until("the worker to report itself idle again", || {
        !health.get_health().is_processing
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}

// --------------------------------------------------------------------------
// Task checkpoints
// --------------------------------------------------------------------------

/// Fails on its first attempt, having checkpointed its progress; on the retry
/// it resumes from the checkpoint and succeeds.
struct ResumableTask {
    /// Progress observed at the start of each attempt.
    resumed_from: Arc<Mutex<Vec<u64>>>,
    attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for ResumableTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        let resume_from = crate::execution_context::load_checkpoint()
            .await
            .and_then(|checkpoint| String::from_utf8(checkpoint.data).ok())
            .and_then(|text| text.parse::<u64>().ok())
            .unwrap_or(0);
        self.resumed_from.lock().expect("lock").push(resume_from);

        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        if attempt == 0 {
            // Record progress, then fail so the worker retries us.
            crate::execution_context::save_checkpoint(b"42".to_vec())
                .await
                .expect("checkpoint saves");
            return Err(celers_core::CelersError::TaskExecution(
                "interrupted".to_string(),
            ));
        }
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "resumable_task"
    }
}

/// A checkpoint written by one attempt is visible to the next, and the store is
/// emptied once the task finally succeeds.
#[tokio::test]
async fn test_checkpoints_resume_a_retried_task_and_are_cleared_on_success() {
    let resumed_from = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(ResumableTask {
            resumed_from: Arc::clone(&resumed_from),
            attempts: Arc::clone(&attempts),
        })
        .await;

    let task = serialized("resumable_task").with_max_retries(2);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let checkpoints = Arc::new(crate::checkpoint::CheckpointManager::new(
        crate::checkpoint::CheckpointConfig::new(),
    ));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_checkpoints(Arc::clone(&checkpoints));
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the retry to run", || attempts.load(Ordering::Relaxed) == 2).await;

    // Note the retry carries a *new* delivery of the same task id, so the
    // checkpoint key is stable across attempts.
    wait_until("the successful attempt to be acked", || {
        !broker.acked().is_empty()
    })
    .await;

    let observed = resumed_from.lock().expect("lock").clone();
    assert_eq!(
        observed,
        vec![0, 42],
        "the retry must resume from the checkpoint the first attempt wrote"
    );

    let mut cleared = false;
    for _ in 0..200 {
        if !checkpoints.has_checkpoint(&task_id.to_string()).await {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        cleared,
        "a completed task's checkpoints must not be left behind"
    );

    handle.shutdown().await.expect("shutdown");
}

/// Without a manager installed the ambient helpers are no-ops rather than
/// errors, so task code can call them unconditionally.
#[tokio::test]
async fn test_checkpoint_helpers_are_noops_without_a_manager() {
    assert!(crate::execution_context::current_checkpoints().is_none());
    assert!(crate::execution_context::load_checkpoint().await.is_none());
    assert!(
        !crate::execution_context::save_checkpoint(b"ignored".to_vec())
            .await
            .expect("a no-op cannot fail"),
        "reports that nothing was stored"
    );
}
