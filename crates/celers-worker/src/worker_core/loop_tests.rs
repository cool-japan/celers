//! Regression tests for the dequeue loop's shutdown responsiveness and the
//! worker's platform readings.
//!
//! Kept beside [`tests`](super::tests) rather than inside it: that module is
//! already at the workspace's file-size cap.

use super::{broker_dequeue_is_cancel_safe, Worker};

use crate::types::{WorkerConfig, WorkerStats};

use celers_core::{
    Broker, BrokerMessage, Event, EventEmitter, InMemoryBroker, NoOpEventEmitter, Result,
    SerializedTask, Task, TaskId, TaskRegistry, WorkerEvent,
};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// --------------------------------------------------------------------------
// Test doubles
// --------------------------------------------------------------------------

/// Records the worker lifecycle events, so "the run loop exited" is
/// observable from outside the spawned worker task.
///
/// Clones share one log: the worker takes the emitter by value, so the test
/// keeps a clone to read.
#[derive(Clone, Default)]
struct LifecycleEmitter {
    events: Arc<Mutex<Vec<Event>>>,
}

impl LifecycleEmitter {
    fn saw_offline(&self) -> bool {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .any(|event| matches!(event, Event::Worker(WorkerEvent::Offline { .. })))
    }

    fn saw_online(&self) -> bool {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .any(|event| matches!(event, Event::Worker(WorkerEvent::Online { .. })))
    }
}

#[async_trait::async_trait]
impl EventEmitter for LifecycleEmitter {
    async fn emit(&self, event: Event) -> Result<()> {
        self.events.lock().expect("lock").push(event);
        Ok(())
    }

    async fn emit_batch(&self, events: Vec<Event>) -> Result<()> {
        self.events.lock().expect("lock").extend(events);
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

/// A broker whose `dequeue` blocks forever: it stands in for every broker the
/// worker must *not* race, and its `dequeue` is deliberately not cancel-safe
/// in spirit (nothing here may be dropped mid-flight by the loop).
#[derive(Default)]
struct NeverReturnsBroker;

#[async_trait::async_trait]
impl Broker for NeverReturnsBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        Ok(task.metadata.id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        std::future::pending::<()>().await;
        unreachable!("pending never resolves")
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
        Ok(0)
    }

    async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
        Ok(false)
    }
}

#[derive(Serialize, Deserialize)]
struct Empty {}

/// Completes immediately, counting executions.
struct CountingTask {
    runs: Arc<AtomicUsize>,
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
        "counting_task"
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

/// A worker configuration whose poll interval is long enough that any prompt
/// reaction has to come from the shutdown path rather than from a poll tick.
fn event_config(hostname: &str) -> WorkerConfig {
    WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 60_000,
        shutdown_timeout_secs: 1,
        enable_events: true,
        // No heartbeat: it would emit worker events of its own on a timer.
        heartbeat_interval_secs: 0,
        hostname: hostname.to_string(),
        ..Default::default()
    }
}

/// Poll `cond` until it holds or `budget` elapses; reports whether it held.
async fn holds_within(budget: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// --------------------------------------------------------------------------
// Cancel-safety allowlist
// --------------------------------------------------------------------------

/// The allowlist is exactly one broker wide, and every other broker inherits
/// the conservative default. A broker that starts racing its dequeue by
/// accident would lose messages, so this is pinned rather than inferred.
#[test]
fn test_only_the_in_memory_broker_is_treated_as_cancel_safe_by_default() {
    assert!(broker_dequeue_is_cancel_safe::<InMemoryBroker>());
    assert!(!broker_dequeue_is_cancel_safe::<NeverReturnsBroker>());
}

/// The auto-detection reaches the built worker, and whoever built it can
/// override the answer in either direction.
#[tokio::test]
async fn test_cancel_safe_dequeue_is_auto_detected_and_overridable() {
    let in_memory: Worker<InMemoryBroker, NoOpEventEmitter> = Worker::new(
        InMemoryBroker::new(),
        TaskRegistry::new(),
        WorkerConfig::default(),
    );
    assert!(in_memory.cancel_safe_dequeue());
    assert!(!in_memory
        .with_cancel_safe_dequeue(false)
        .cancel_safe_dequeue());

    let other: Worker<NeverReturnsBroker, NoOpEventEmitter> = Worker::new(
        NeverReturnsBroker,
        TaskRegistry::new(),
        WorkerConfig::default(),
    );
    assert!(!other.cancel_safe_dequeue());
    assert!(other.with_cancel_safe_dequeue(true).cancel_safe_dequeue());
}

// --------------------------------------------------------------------------
// The dequeue-versus-shutdown race
// --------------------------------------------------------------------------

/// An idle worker on the in-memory broker used to observe `control shutdown`
/// only when the next message arrived: `InMemoryBroker::dequeue` has no block
/// timeout, so the loop parked in it indefinitely. Its dequeue is cancel-safe,
/// so the loop races it against the shutdown signal and the worker exits with
/// nothing in the queue at all.
#[tokio::test]
async fn test_idle_in_memory_worker_shuts_down_without_waiting_for_a_message() {
    let emitter = LifecycleEmitter::default();
    let broker = Arc::new(InMemoryBroker::new());
    let registry = TaskRegistry::new();

    let worker: Worker<InMemoryBroker, LifecycleEmitter> = Worker::with_event_emitter_from_arc(
        Arc::clone(&broker),
        registry,
        event_config("idle-worker"),
        emitter.clone(),
    );
    assert!(worker.cancel_safe_dequeue());

    let handle = worker.run_with_shutdown().await.expect("worker starts");
    assert!(
        holds_within(Duration::from_secs(5), || emitter.saw_online()).await,
        "the worker should have come online"
    );

    handle.shutdown().await.expect("shutdown");

    assert!(
        holds_within(Duration::from_secs(5), || emitter.saw_offline()).await,
        "an idle worker must exit on shutdown without a message arriving first"
    );
    assert_eq!(
        broker.queue_size().await.expect("queue size"),
        0,
        "nothing was enqueued, so nothing may have been lost"
    );
}

/// The negative control for the test above, and the reason the race is gated:
/// with the race turned off the worker keeps the documented behaviour and
/// stays parked in `dequeue` until a message arrives -- which it then runs,
/// exiting on the following pass. A broker whose dequeue is not cancel-safe
/// gets exactly this, so no message can be dropped with the future.
#[tokio::test]
async fn test_without_the_race_shutdown_waits_for_the_next_message() {
    let runs = Arc::new(AtomicUsize::new(0));
    let emitter = LifecycleEmitter::default();
    let broker = Arc::new(InMemoryBroker::new());
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    let worker: Worker<InMemoryBroker, LifecycleEmitter> = Worker::with_event_emitter_from_arc(
        Arc::clone(&broker),
        registry,
        event_config("parked-worker"),
        emitter.clone(),
    )
    .with_cancel_safe_dequeue(false);

    let handle = worker.run_with_shutdown().await.expect("worker starts");
    assert!(
        holds_within(Duration::from_secs(5), || emitter.saw_online()).await,
        "the worker should have come online"
    );

    handle.shutdown().await.expect("shutdown");

    // Parked in `dequeue`, so the shutdown is acknowledged but not yet acted on.
    assert!(
        !holds_within(Duration::from_millis(300), || emitter.saw_offline()).await,
        "without the race the worker must stay parked in dequeue"
    );

    // The message that finally releases it is still executed, not discarded.
    broker
        .enqueue(serialized("counting_task"))
        .await
        .expect("enqueue");

    assert!(
        holds_within(Duration::from_secs(5), || emitter.saw_offline()).await,
        "the worker should exit once its dequeue returns"
    );
    assert_eq!(
        runs.load(Ordering::Relaxed),
        1,
        "the message that unparked the worker must still run"
    );
}

/// The race must not swallow work: when a message is available the dequeue
/// arm wins and the message is executed exactly as before.
#[tokio::test]
async fn test_the_race_still_delivers_every_queued_message() {
    const MESSAGES: usize = 8;

    let runs = Arc::new(AtomicUsize::new(0));
    let emitter = LifecycleEmitter::default();
    let broker = Arc::new(InMemoryBroker::new());
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    for _ in 0..MESSAGES {
        broker
            .enqueue(serialized("counting_task"))
            .await
            .expect("enqueue");
    }

    let worker: Worker<InMemoryBroker, LifecycleEmitter> = Worker::with_event_emitter_from_arc(
        Arc::clone(&broker),
        registry,
        event_config("busy-worker"),
        emitter.clone(),
    );
    assert!(worker.cancel_safe_dequeue());

    let handle = worker.run_with_shutdown().await.expect("worker starts");
    assert!(
        holds_within(Duration::from_secs(10), || {
            runs.load(Ordering::Relaxed) == MESSAGES
        })
        .await,
        "every queued message must run: saw {} of {MESSAGES}",
        runs.load(Ordering::Relaxed)
    );

    handle.shutdown().await.expect("shutdown");
    assert!(
        holds_within(Duration::from_secs(5), || emitter.saw_offline()).await,
        "the worker should exit after the queue drains"
    );
    assert_eq!(broker.queue_size().await.expect("queue size"), 0);
}

// --------------------------------------------------------------------------
// Platform readings
// --------------------------------------------------------------------------

/// The heartbeat's load average used to be read straight from `/proc/loadavg`
/// under a bare `#[cfg(unix)]`, so every macOS/BSD worker reported a flat
/// `[0.0, 0.0, 0.0]` forever. It now goes through
/// [`crate::sysinfo::read_load_average`], which uses `getloadavg(3)` where
/// there is no `/proc`.
#[test]
fn test_get_load_average_uses_the_platform_reader() {
    type AnyWorker = Worker<InMemoryBroker, NoOpEventEmitter>;

    let platform = crate::sysinfo::read_load_average();
    let reported = AnyWorker::get_load_average();

    match platform {
        // Where a real reading exists the heartbeat must carry *that*, not a
        // fabricated zero -- this is the half that fails under a
        // `/proc`-only implementation on a platform without `/proc`.
        Some(loads) => {
            assert_eq!(reported, loads);
            for load in reported {
                assert!(
                    (0.0..1_000_000.0).contains(&load),
                    "implausible load average: {load}"
                );
            }
        }
        // Where none exists, the documented zero sentinel -- not a panic, and
        // not a stale value from another platform's reader.
        None => assert_eq!(reported, [0.0, 0.0, 0.0]),
    }
}

/// `WorkerStats` is what the heartbeat reports alongside the load average; a
/// fresh worker has to start from zero on both.
#[test]
fn test_a_fresh_worker_reports_no_activity() {
    let stats = WorkerStats::new();
    assert_eq!(stats.active(), 0);
    assert_eq!(stats.processed(), 0);
}
