//! Hermetic end-to-end tests for broker-fed revocation.
//!
//! Every test here runs a real [`Worker`] against an [`InMemoryBroker`] and
//! revokes through the broker's own API — [`Broker::revoke`] — exactly as
//! another process would. Nothing is stubbed: the revocation travels the
//! broker's persisted revoked-id set and its revocation channel, through the
//! bridge installed by [`Worker::with_broker_revocation`], into the worker's
//! [`WorkerRevocationManager`] and [`RevocationWatcher`].
//!
//! The Redis half of the same loop lives in
//! `celers-cli/tests/revocation_redis.rs`, which is the only crate that can
//! depend on both a worker and the Redis broker; it is gated on a live server.

use crate::execution_context::is_cancelled;
use crate::types::{WorkerConfig, WorkerStats};
use crate::{Worker, WorkerHandle};

use celers_core::{
    Broker, Event, EventEmitter, InMemoryBroker, Result, SerializedTask, Task, TaskEvent, TaskId,
    TaskRegistry,
};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Deadline for the "wait until" helpers: generous enough for a loaded CI box,
/// short enough that a genuine hang fails the test rather than the suite.
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Serialize, Deserialize)]
struct Empty {}

/// Records every event the worker emits.
#[derive(Clone, Default)]
struct CapturingEmitter {
    events: Arc<Mutex<Vec<Event>>>,
}

impl CapturingEmitter {
    /// Whether a `TaskEvent::Revoked` was emitted for `task_id`.
    fn revoked(&self, task_id: TaskId) -> bool {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .any(|event| matches!(event, Event::Task(TaskEvent::Revoked { task_id: id, .. }) if *id == task_id))
    }
}

#[async_trait::async_trait]
impl EventEmitter for CapturingEmitter {
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
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// A cooperative long-running task: it polls the ambient cancellation token and
/// returns early when it is tripped, which is what a revocation must cause.
struct CooperativeTask {
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    /// How long to keep working when nothing cancels it.
    work_for: Duration,
}

#[async_trait::async_trait]
impl Task for CooperativeTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let deadline = Instant::now() + self.work_for;
        while Instant::now() < deadline {
            if is_cancelled() {
                // Park until the worker drops this future: returning `Ok` here
                // would look like a task that succeeded despite the revocation.
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "cooperative_task"
    }
}

/// A worker configuration that reacts quickly enough for a test.
fn fast_config(hostname: &str) -> WorkerConfig {
    WorkerConfig {
        concurrency: 4,
        poll_interval_ms: 10,
        defer_delay_ms: 20,
        defer_max_delay_ms: 60,
        shutdown_timeout_secs: 5,
        enable_events: true,
        hostname: hostname.to_string(),
        ..Default::default()
    }
}

/// Poll `predicate` until it holds or [`WAIT_DEADLINE`] expires.
async fn wait_until<F>(what: &str, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + WAIT_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// A running worker plus the handles a test needs afterwards.
struct Harness {
    broker: Arc<InMemoryBroker>,
    handle: WorkerHandle,
    stats: Arc<WorkerStats>,
    events: CapturingEmitter,
}

impl Harness {
    /// Start a worker with broker-fed revocation enabled, waiting until its
    /// bridge has actually subscribed so a test never races the subscription.
    async fn start(hostname: &str, registry: TaskRegistry) -> Self {
        Self::start_inner(hostname, registry, true).await
    }

    /// Start a worker *without* broker-fed revocation, to pin what the opt-in
    /// switch actually buys.
    async fn start_without_broker_revocation(hostname: &str, registry: TaskRegistry) -> Self {
        Self::start_inner(hostname, registry, false).await
    }

    async fn start_inner(hostname: &str, registry: TaskRegistry, broker_revocation: bool) -> Self {
        let broker = Arc::new(InMemoryBroker::new());
        let events = CapturingEmitter::default();

        let mut worker = Worker::with_event_emitter_from_arc(
            Arc::clone(&broker),
            registry,
            fast_config(hostname),
            events.clone(),
        );
        if broker_revocation {
            worker = worker.with_broker_revocation();
            assert!(worker.broker_revocation_enabled());
            assert!(
                worker.revocation_watcher().is_some(),
                "the builder must supply a watcher, or `terminate` could do nothing"
            );
        }

        let stats = worker.stats_arc();
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        if broker_revocation {
            let subscribed = Arc::clone(&broker);
            wait_until("the revocation bridge to subscribe", || {
                subscribed.revocation_subscriber_count() > 0
            })
            .await;
        }

        Self {
            broker,
            handle,
            stats,
            events,
        }
    }

    /// Enqueue one task of `name`, returning its id.
    async fn enqueue(&self, name: &str) -> TaskId {
        let payload = serde_json::to_vec(&Empty {}).expect("payload");
        let task = SerializedTask::new(name.to_string(), payload);
        self.broker.enqueue(task).await.expect("enqueue")
    }

    async fn shutdown(self) {
        let _ = self.handle.shutdown().await;
    }
}

#[tokio::test]
async fn a_task_revoked_before_it_is_dequeued_is_acked_skipped_and_reported() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "cancellable",
        })
        .await;

    let harness = Harness::start("revoke-queued", registry).await;

    // Revoke an id the broker has never seen: the durable revoked set is what
    // makes this stick, and it is the case a Pub/Sub-only implementation loses.
    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    let task = SerializedTask::new("cancellable".to_string(), payload);
    let task_id = task.metadata.id;
    assert!(
        !harness
            .broker
            .revoke(&task_id, false)
            .await
            .expect("revoke"),
        "nothing is queued yet, so no pending copy is found"
    );
    assert!(harness.broker.is_revoked(&task_id).await.expect("lookup"));

    // Now the message arrives. The worker dequeues it, sees the revocation and
    // must dispose of it instead of executing it.
    harness.broker.enqueue(task).await.expect("enqueue");

    let revoked = Arc::clone(&harness.stats);
    wait_until("the task to be counted as revoked", || {
        revoked.revoked() == 1
    })
    .await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "a revoked task must never execute"
    );
    assert!(
        harness.events.revoked(task_id),
        "the worker must report the revocation as a task event"
    );

    // Acknowledged, not requeued: a revoked message that came back would be
    // dequeued and dropped again forever.
    wait_until("the message to be acknowledged", || true).await;
    assert_eq!(
        harness.broker.queue_size().await.expect("queue size"),
        0,
        "a revoked task is acknowledged, not left to be redelivered"
    );
    assert_eq!(
        harness.broker.in_flight_len().await,
        0,
        "and it is not left in flight either"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn a_terminating_broker_revocation_aborts_a_running_task() {
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CooperativeTask {
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
            // Far longer than the test's deadline: only a revocation can end it.
            work_for: Duration::from_secs(60),
        })
        .await;

    let harness = Harness::start("revoke-in-flight", registry).await;
    let task_id = harness.enqueue("cooperative_task").await;

    let running = Arc::clone(&started);
    wait_until("the task to start", || running.load(Ordering::SeqCst) == 1).await;

    // Revoke through the broker, exactly as another process would.
    assert!(harness.broker.revoke(&task_id, true).await.expect("revoke"));

    let revoked = Arc::clone(&harness.stats);
    wait_until("the running task to be revoked", || revoked.revoked() == 1).await;

    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "a terminated task must not reach its end"
    );
    assert!(
        harness.events.revoked(task_id),
        "the worker must report the revocation as a task event"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn a_non_terminating_broker_revocation_leaves_a_running_task_alone() {
    // Celery's `revoke(id)` without `terminate=True` must not kill work that is
    // already under way; only `revoke(id, terminate=True)` does.
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CooperativeTask {
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
            work_for: Duration::from_millis(200),
        })
        .await;

    let harness = Harness::start("revoke-ignore", registry).await;
    let task_id = harness.enqueue("cooperative_task").await;

    let running = Arc::clone(&started);
    wait_until("the task to start", || running.load(Ordering::SeqCst) == 1).await;

    harness
        .broker
        .revoke(&task_id, false)
        .await
        .expect("revoke");

    let finished = Arc::clone(&completed);
    wait_until("the task to finish normally", || {
        finished.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        harness.stats.revoked(),
        0,
        "a non-terminating revocation must not abort a running task"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn broker_revocation_is_opt_in() {
    // Pins what the switch buys, so nobody "simplifies" the builder away: with
    // no bridge and no dequeue-time lookup, a worker cannot know about a
    // revocation recorded by another process.
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "cancellable",
        })
        .await;

    let harness = Harness::start_without_broker_revocation("no-bridge", registry).await;

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    let task = SerializedTask::new("cancellable".to_string(), payload);
    let task_id = task.metadata.id;
    harness
        .broker
        .revoke(&task_id, false)
        .await
        .expect("revoke");
    harness.broker.enqueue(task).await.expect("enqueue");

    let executed = Arc::clone(&runs);
    wait_until("the task to run", || executed.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        harness.stats.revoked(),
        0,
        "without the switch the worker never asks the broker"
    );
    assert_eq!(
        harness.broker.revoked_len().await,
        1,
        "the broker still recorded the revocation; nothing consulted it"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn an_expired_revocation_stops_refusing_the_task() {
    // The revoked set is TTL-bounded, so it cannot grow forever. A task whose
    // revocation has expired must run again rather than being dropped in
    // silence for the rest of the deployment's life.
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "cancellable",
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new().with_revocation_ttl(Duration::from_millis(50)));
    let events = CapturingEmitter::default();
    let worker = Worker::with_event_emitter_from_arc(
        Arc::clone(&broker),
        registry,
        fast_config("expiring-revocation"),
        events.clone(),
    )
    .with_broker_revocation();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    let task = SerializedTask::new("cancellable".to_string(), payload);
    let task_id = task.metadata.id;
    broker.revoke(&task_id, false).await.expect("revoke");

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        !broker.is_revoked(&task_id).await.expect("lookup"),
        "the revocation must have expired"
    );

    broker.enqueue(task).await.expect("enqueue");
    let executed = Arc::clone(&runs);
    wait_until("the task to run", || executed.load(Ordering::SeqCst) == 1).await;

    let _ = handle.shutdown().await;
}
