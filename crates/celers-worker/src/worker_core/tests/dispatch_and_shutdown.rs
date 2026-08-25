//! Regression tests for the run loop's admission and disposal of one
//! dispatched attempt: concurrency backpressure, the shutdown drain (both
//! that it happens at all and that it spends no retry doing so),
//! admission-deferral backoff and the oversized-result terminal failure.

use super::doubles::{
    serialized, wait_until, AlwaysRedeliverBroker, BigResultTask, BlockingTask, CountingTask,
    RecordingBroker,
};

use crate::affinity::{AffinityRegistry, TaskAffinity};
use crate::types::WorkerConfig;
use crate::worker_core::Worker;
use crate::WorkerLabels;

use celers_core::{
    Broker, BrokerMessage, InMemoryBroker, NoOpEventEmitter, TaskRegistry, TaskState,
};

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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

/// A broker that separately records `Broker::defer` and
/// `Broker::reject(requeue = true)` calls.
///
/// [`RecordingBroker`] cannot tell these two apart: it never overrides
/// `defer`, so the trait's default implementation silently forwards it to
/// `reject(requeue = true)`, and the test above stays green whichever one
/// `drain_in_flight` actually calls. This double exists so a regression back
/// to calling `reject` directly at drain -- which a broker that records
/// retry state (the Redis one) would read as a failed attempt, burning a
/// retry on a task that never got to run -- is caught here.
struct DisposalTrackingBroker {
    pending: Mutex<VecDeque<BrokerMessage>>,
    deferred: Mutex<Vec<(celers_core::TaskId, Duration)>>,
    requeued: Mutex<Vec<celers_core::TaskId>>,
}

impl DisposalTrackingBroker {
    fn new(messages: Vec<BrokerMessage>) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(messages.into()),
            deferred: Mutex::new(Vec::new()),
            requeued: Mutex::new(Vec::new()),
        })
    }

    fn deferred(&self) -> Vec<(celers_core::TaskId, Duration)> {
        self.deferred.lock().expect("lock").clone()
    }

    fn requeued(&self) -> Vec<celers_core::TaskId> {
        self.requeued.lock().expect("lock").clone()
    }
}

#[async_trait::async_trait]
impl Broker for DisposalTrackingBroker {
    async fn enqueue(
        &self,
        task: celers_core::SerializedTask,
    ) -> celers_core::Result<celers_core::TaskId> {
        Ok(task.metadata.id)
    }

    async fn dequeue(&self) -> celers_core::Result<Option<BrokerMessage>> {
        Ok(self.pending.lock().expect("lock").pop_front())
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
        task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        requeue: bool,
    ) -> celers_core::Result<()> {
        if requeue {
            self.requeued.lock().expect("lock").push(*task_id);
        }
        Ok(())
    }

    async fn defer(
        &self,
        task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        delay: Duration,
    ) -> celers_core::Result<()> {
        self.deferred.lock().expect("lock").push((*task_id, delay));
        Ok(())
    }

    async fn queue_size(&self) -> celers_core::Result<usize> {
        Ok(self.pending.lock().expect("lock").len())
    }

    async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }
}

/// The drain deadline expiring must hand the message back through
/// [`Broker::defer`], never [`Broker::reject`] -- see
/// [`DisposalTrackingBroker`]'s doc comment for why the test above cannot
/// see this distinction on its own.
#[tokio::test]
async fn test_shutdown_drain_calls_defer_not_reject() {
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
    let broker = DisposalTrackingBroker::new(vec![BrokerMessage::new(task)]);

    let config = WorkerConfig {
        concurrency: 1,
        poll_interval_ms: 10,
        shutdown_timeout_secs: 1,
        ..Default::default()
    };
    let worker: Worker<DisposalTrackingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to start", || started.load(Ordering::Relaxed) == 1).await;

    handle.shutdown().await.expect("shutdown");

    wait_until("the in-flight message to be deferred", || {
        !broker.deferred().is_empty()
    })
    .await;

    assert_eq!(
        broker.deferred(),
        vec![(task_id, Duration::ZERO)],
        "drain must return the message via Broker::defer, immediately visible again"
    );
    assert!(
        broker.requeued().is_empty(),
        "drain must not fall back to reject(requeue = true), which a \
         retry-recording broker would read as a failed attempt"
    );

    release.notify_waiters();
    wait_until("the straggler to finish", || {
        release.notify_waiters();
        finished.load(Ordering::Relaxed) == 1
    })
    .await;
}

/// Round-trip proof on the real in-memory broker: a message the drain
/// deferred keeps its payload and retry state untouched and is immediately
/// redeliverable, not merely accepted by a double that records the call.
#[tokio::test]
async fn test_shutdown_drain_redelivers_via_the_real_in_memory_broker() {
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

    let broker = Arc::new(InMemoryBroker::new());
    broker
        .enqueue(serialized("blocking_task"))
        .await
        .expect("enqueue");

    let config = WorkerConfig {
        concurrency: 1,
        poll_interval_ms: 10,
        shutdown_timeout_secs: 1,
        ..Default::default()
    };
    let worker: Worker<InMemoryBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to start", || started.load(Ordering::Relaxed) == 1).await;
    handle.shutdown().await.expect("shutdown");

    let redelivered = poll_for_redelivery(&*broker).await;
    assert_eq!(redelivered.task.metadata.name, "blocking_task");
    assert_eq!(
        redelivered.task.metadata.state,
        TaskState::Pending,
        "a message that was never executed keeps its original state"
    );

    release.notify_waiters();
    wait_until("the straggler to finish", || {
        release.notify_waiters();
        finished.load(Ordering::Relaxed) == 1
    })
    .await;
}

/// Wait for the drained message to come back through `Broker::dequeue`,
/// bounded to ~10 seconds.
///
/// `dequeue` rather than `try_dequeue`: the latter is an optional capability
/// ([`RedisBroker`](celers_broker_redis::RedisBroker) refuses it outright),
/// while every [`Broker`] must support the former. `InMemoryBroker::dequeue`
/// parks until a message is ready; `RedisBroker::dequeue` blocks up to its
/// own timeout and returns `None` on expiry, so the outer loop just retries.
async fn poll_for_redelivery<B: Broker>(broker: &B) -> BrokerMessage {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(msg) = broker.dequeue().await.expect("dequeue") {
                return msg;
            }
        }
    })
    .await
    .expect("timed out waiting for the drained message to be redelivered")
}

/// The regression this whole file exists to pin, on the broker that actually
/// has the bug: on Redis, `reject(requeue = true)` always rewrites the
/// payload to `Retrying(n + 1)`, even when the task never ran. A task
/// deferred at shutdown must come back with its retry count untouched.
/// Seeded at `Retrying(1)` (not the default `Pending`) so the assertion tells
/// "unchanged" apart from "never set". Gated on a live server via
/// `CELERS_TEST_REDIS_URL`; skipped otherwise so the suite stays hermetic.
#[tokio::test]
async fn test_shutdown_drain_does_not_spend_a_retry_on_redis() {
    let Ok(url) = std::env::var("CELERS_TEST_REDIS_URL") else {
        eprintln!(
            "SKIPPED: test_shutdown_drain_does_not_spend_a_retry_on_redis \
             (set CELERS_TEST_REDIS_URL to run)"
        );
        return;
    };

    let queue = format!("celers:test:drain:{}", uuid::Uuid::new_v4());
    let broker = Arc::new(
        celers_broker_redis::RedisBroker::new(&url, &queue)
            .expect("CELERS_TEST_REDIS_URL is set, so constructing the broker must succeed"),
    );

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

    // Seed the task as if this were already its second delivery.
    let mut task = serialized("blocking_task");
    task.metadata.state = TaskState::Retrying(1);
    let task_id = task.metadata.id;
    broker
        .enqueue(task)
        .await
        .expect("a live redis server accepts the enqueue");

    let config = WorkerConfig {
        concurrency: 1,
        poll_interval_ms: 10,
        shutdown_timeout_secs: 1,
        ..Default::default()
    };
    let worker: Worker<celers_broker_redis::RedisBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to start", || started.load(Ordering::Relaxed) == 1).await;
    handle.shutdown().await.expect("shutdown");

    let redelivered = poll_for_redelivery(&*broker).await;
    assert_eq!(redelivered.task.metadata.id, task_id);
    assert_eq!(
        redelivered.task.metadata.state,
        TaskState::Retrying(1),
        "a drained-but-never-run task must not have its retry count bumped"
    );

    release.notify_waiters();
    wait_until("the straggler to finish", || {
        release.notify_waiters();
        finished.load(Ordering::Relaxed) == 1
    })
    .await;
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
