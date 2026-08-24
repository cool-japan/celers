//! End-to-end tests of broker-fed revocation over real Redis.
//!
//! These are the multi-process shape of the hermetic suite in
//! `celers_worker::broker_revocation_tests`: a real [`Worker`] subscribes to a
//! Redis queue's revocation channel and consults its durable revoked-id set,
//! while a separate `celers control revoke` — the actual CLI entry point,
//! [`revoke_tasks`] — records and publishes the revocation. Nothing is mocked:
//! the revocation crosses the Redis wire, and the worker under test is given no
//! control channel at all, so the *only* path from the CLI to the running task
//! is the broker.
//!
//! This crate is the only one that depends on both `celers-worker` and
//! `celers-broker-redis`, which is why the cross-crate test lives here.
//!
//! # Running
//!
//! ```sh
//! export CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379
//! cargo nextest run -p celers-cli --test revocation_redis
//! ```
//!
//! Without `CELERS_TEST_REDIS_URL` every test returns immediately, so the
//! default suite stays hermetic. RESP3 (Redis 6.0+) is required: the revocation
//! subscription is delivered through server pushes.

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_cli::commands::{revoke_tasks, ControlOptions};
use celers_core::{Broker, Result, SerializedTask, Task, TaskRegistry};
use celers_worker::execution_context::is_cancelled;
use celers_worker::{Worker, WorkerConfig, WorkerHandle, WorkerStats};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Deadline for the "wait until" helpers.
const WAIT_DEADLINE: Duration = Duration::from_secs(10);

/// The live-server URL, or `None` when the suite is not enabled.
fn redis_url() -> Option<String> {
    std::env::var("CELERS_TEST_REDIS_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

#[derive(Serialize, Deserialize)]
struct Empty {}

// The task impls below are written in the desugared `#[async_trait]` form on
// purpose: `celers-cli` does not depend on the `async-trait` crate, and this
// test may not add one. The shape is exactly what the attribute would expand to
// on `async fn execute(&self, input) -> Result<Output>`.

/// Completes immediately, counting executions.
struct CountingTask {
    runs: Arc<AtomicUsize>,
    name: &'static str,
}

impl Task for CountingTask {
    type Input = Empty;
    type Output = Empty;

    fn execute<'life0, 'async_trait>(
        &'life0 self,
        _input: Self::Input,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Output>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let runs = Arc::clone(&self.runs);
        Box::pin(async move {
            runs.fetch_add(1, Ordering::SeqCst);
            Ok(Empty {})
        })
    }

    fn name(&self) -> &str {
        self.name
    }
}

/// A cooperative long-running task: it polls the ambient cancellation token and
/// stops making progress once it is tripped, which is what a terminating
/// revocation must cause.
struct CooperativeTask {
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    work_for: Duration,
}

impl Task for CooperativeTask {
    type Input = Empty;
    type Output = Empty;

    fn execute<'life0, 'async_trait>(
        &'life0 self,
        _input: Self::Input,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Output>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let started = Arc::clone(&self.started);
        let completed = Arc::clone(&self.completed);
        let work_for = self.work_for;
        Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + work_for;
            while Instant::now() < deadline {
                if is_cancelled() {
                    // Park until the worker drops this future: returning `Ok`
                    // would look like a task that finished despite the
                    // revocation.
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(Empty {})
        })
    }

    fn name(&self) -> &str {
        "cooperative_task"
    }
}

/// A worker consuming a run-unique Redis queue, with broker-fed revocation on
/// and **no** control channel — so every revocation these tests observe
/// travelled through the broker.
struct LiveWorker {
    broker: Arc<RedisBroker>,
    handle: WorkerHandle,
    stats: Arc<WorkerStats>,
    queue: String,
    url: String,
}

impl LiveWorker {
    async fn start(url: &str, label: &str, registry: TaskRegistry) -> Self {
        let queue = live_queue(label);
        let broker =
            Arc::new(RedisBroker::with_mode(url, &queue, QueueMode::Fifo).expect("redis broker"));

        let config = WorkerConfig {
            concurrency: 2,
            poll_interval_ms: 20,
            defer_delay_ms: 20,
            defer_max_delay_ms: 60,
            shutdown_timeout_secs: 5,
            queue_name: queue.clone(),
            hostname: format!("{label}-{}", Uuid::new_v4()),
            ..Default::default()
        };

        let worker = Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_broker_revocation()
            .with_broker_url(url);
        let stats = worker.stats_arc();
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        Self {
            broker,
            handle,
            stats,
            queue,
            url: url.to_string(),
        }
    }

    /// Enqueue one task of `name`, returning its id.
    async fn enqueue(&self, name: &str) -> Uuid {
        let payload = serde_json::to_vec(&Empty {}).expect("payload");
        self.broker
            .enqueue(SerializedTask::new(name.to_string(), payload))
            .await
            .expect("enqueue")
    }

    /// Options pointing the CLI's revoke at this run's queue and a control
    /// channel nothing else is on.
    fn control_options(&self) -> ControlOptions {
        let mut options = ControlOptions::new(&self.url);
        options.channel = Some(format!("celers.test.control.{}", Uuid::new_v4()));
        // No worker is on that channel, so do not spend the default two
        // seconds waiting for replies that cannot come.
        options.timeout_secs = 0.3;
        options
    }

    async fn cleanup(self) {
        let _ = self.handle.shutdown().await;
        delete_keys(&self.url, &self.queue, self.broker.queue_names()).await;
    }
}

/// A queue name unique to this run, so concurrent runs cannot collide.
fn live_queue(label: &str) -> String {
    format!("celers-test-revoke-{label}-{}", Uuid::new_v4())
}

/// Best-effort removal of the keys a test created.
async fn delete_keys(url: &str, queue: &str, mut keys: Vec<String>) {
    // `queue_names()` does not list the revoked set (purging messages must not
    // un-revoke anything), so name it explicitly here.
    keys.push(format!("{queue}:revoked"));
    if let Ok(client) = redis::Client::open(url) {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
            let _: std::result::Result<i64, _> = redis::AsyncCommands::del(&mut conn, &keys).await;
        }
    }
}

/// Poll an async predicate until it holds or the deadline passes.
async fn wait_until<F>(what: &str, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + WAIT_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn live_revocation_channel_delivers_a_notice_across_the_wire() {
    // The failure this guards against is silent: the broker's own client speaks
    // RESP2, on which a subscription connects happily and then never delivers a
    // message. Asserting delivery — not that `subscribe` returned `Ok` — is the
    // only thing that catches it.
    let Some(url) = redis_url() else {
        return;
    };
    let queue = live_queue("channel");
    let broker = RedisBroker::with_mode(&url, &queue, QueueMode::Fifo).expect("redis broker");

    let mut stream = broker
        .subscribe_revocations()
        .await
        .expect("subscribe")
        .expect("the Redis broker has a revocation channel");

    // A separate handle on the same queue, as another process would be.
    let revoker = RedisBroker::with_mode(&url, &queue, QueueMode::Fifo).expect("redis broker");
    let task_id = Uuid::new_v4();
    revoker.revoke(&task_id, true).await.expect("revoke");

    let notice = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("a notice arrives before the deadline")
        .expect("the stream is healthy")
        .expect("the stream is not closed");
    assert_eq!(notice.task_id, task_id);
    assert!(
        notice.terminate,
        "the terminate flag must survive the wire, or a fleet-wide revoke \
         cannot abort a running task"
    );

    delete_keys(&url, &queue, broker.queue_names()).await;
}

#[tokio::test]
async fn live_revoke_records_a_durable_set_with_no_worker_running() {
    // A control broadcast into an empty cluster revokes nothing. The CLI's
    // revoke has to leave something behind that a worker started *later* will
    // still honour.
    let Some(url) = redis_url() else {
        return;
    };
    let queue = live_queue("durable");
    let broker = RedisBroker::with_mode(&url, &queue, QueueMode::Fifo).expect("redis broker");

    let task_id = Uuid::new_v4();
    let mut options = ControlOptions::new(&url);
    options.channel = Some(format!("celers.test.control.{}", Uuid::new_v4()));
    options.timeout_secs = 0.3;

    revoke_tasks(&options, &queue, "fifo", &[task_id], false)
        .await
        .expect("revoking with no worker running is not an error");

    assert!(
        broker.is_revoked(&task_id).await.expect("lookup"),
        "the revocation must outlive the command that issued it"
    );
    assert!(
        !broker.is_revoked(&Uuid::new_v4()).await.expect("lookup"),
        "and it must not revoke anything else"
    );

    delete_keys(&url, &queue, broker.queue_names()).await;
}

#[tokio::test]
async fn live_revoking_a_queued_task_stops_it_from_running() {
    let Some(url) = redis_url() else {
        return;
    };
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "live.cancellable",
        })
        .await;

    let worker = LiveWorker::start(&url, "queued", registry).await;

    // Revoke before the message exists: the durable set is what makes this
    // stick, and it is exactly the case Pub/Sub alone loses.
    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    let task = SerializedTask::new("live.cancellable".to_string(), payload);
    let task_id = task.metadata.id;
    revoke_tasks(
        &worker.control_options(),
        &worker.queue,
        "fifo",
        &[task_id],
        false,
    )
    .await
    .expect("revoke");

    worker.broker.enqueue(task).await.expect("enqueue");

    // A control task that must run, so the test can tell "the revoked task has
    // not run *yet*" from "the worker is not consuming at all".
    let canary = worker.enqueue("live.cancellable").await;
    assert_ne!(canary, task_id);
    let executed = Arc::clone(&runs);
    wait_until("the unrevoked task to run", || {
        executed.load(Ordering::SeqCst) == 1
    })
    .await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the revoked task must never execute"
    );
    wait_until("the queue to drain", || true).await;
    assert_eq!(
        worker.broker.queue_size().await.expect("queue size"),
        0,
        "a revoked task is disposed of, not left to be redelivered"
    );

    worker.cleanup().await;
}

#[tokio::test]
async fn live_revoke_with_terminate_aborts_a_running_task() {
    let Some(url) = redis_url() else {
        return;
    };
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CooperativeTask {
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
            // Far longer than the deadline: only a revocation can end it.
            work_for: Duration::from_secs(60),
        })
        .await;

    let worker = LiveWorker::start(&url, "inflight", registry).await;
    let task_id = worker.enqueue("cooperative_task").await;

    let running = Arc::clone(&started);
    wait_until("the task to start", || running.load(Ordering::SeqCst) == 1).await;

    // The worker has no control channel: this can only reach it through the
    // queue's revocation channel.
    revoke_tasks(
        &worker.control_options(),
        &worker.queue,
        "fifo",
        &[task_id],
        true,
    )
    .await
    .expect("revoke");

    let revoked = Arc::clone(&worker.stats);
    wait_until("the running task to be revoked", || revoked.revoked() == 1).await;
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "a terminated task must not reach its end"
    );

    worker.cleanup().await;
}

#[tokio::test]
async fn live_revoke_without_terminate_leaves_a_running_task_alone() {
    // Celery's `revoke(id)` stops a task from starting; only
    // `revoke(id, terminate=True)` kills work already under way.
    let Some(url) = redis_url() else {
        return;
    };
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CooperativeTask {
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
            work_for: Duration::from_millis(500),
        })
        .await;

    let worker = LiveWorker::start(&url, "ignore", registry).await;
    let task_id = worker.enqueue("cooperative_task").await;

    let running = Arc::clone(&started);
    wait_until("the task to start", || running.load(Ordering::SeqCst) == 1).await;

    revoke_tasks(
        &worker.control_options(),
        &worker.queue,
        "fifo",
        &[task_id],
        false,
    )
    .await
    .expect("revoke");

    let finished = Arc::clone(&completed);
    wait_until("the task to finish normally", || {
        finished.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        worker.stats.revoked(),
        0,
        "a non-terminating revocation must not abort a running task"
    );

    worker.cleanup().await;
}
