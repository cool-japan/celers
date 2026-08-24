//! Hermetic end-to-end tests for the remote worker control protocol.
//!
//! Every test here drives a *running* worker over a real transport (the
//! in-process [`InMemoryControlTransport`]) through the public
//! [`ControlClient`], and asserts on the worker's observable behaviour rather
//! than on the shape of the reply. Nothing external is required: the broker is
//! [`InMemoryBroker`] and the control channel is in-process, so the whole loop
//! — client → transport → worker subscriber → dispatch → reply — is exercised
//! without a network.
//!
//! The Redis half of the same loop is covered by
//! `celers_broker_redis::control` (gated on a live server).

use crate::execution_context::RevocationWatcher;
use crate::types::{WorkerConfig, WorkerMode};
use crate::Worker;

use celers_core::control::{ControlCommand, ControlResponse, InspectCommand, InspectResponse};
use celers_core::control_transport::{
    ControlClient, ControlReply, ControlTransport, InMemoryControlTransport,
};
use celers_core::time_limit::WorkerTimeLimits;
use celers_core::{
    Broker, InMemoryBroker, Result, SerializedTask, Task, TaskRegistry, WorkerRevocationManager,
};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Deadline for every "wait until" helper. Generous enough for a loaded CI box,
/// short enough that a genuine hang fails the test instead of the suite.
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

/// Gather timeout for control commands in these tests.
const GATHER: Duration = Duration::from_secs(3);

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
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// Runs until the shared notifier fires, so a test can observe it as "active".
struct BlockingTask {
    started: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Task for BlockingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "blocking_task"
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

/// The single reply a one-worker cluster is expected to produce.
fn single(replies: Vec<ControlReply>) -> ControlReply {
    assert_eq!(replies.len(), 1, "expected exactly one worker to answer");
    replies.into_iter().next().expect("one reply")
}

/// A client bound to `transport`, expecting `workers` replies.
fn client(transport: &Arc<InMemoryControlTransport>, workers: usize) -> ControlClient {
    ControlClient::new(Arc::clone(transport) as Arc<dyn ControlTransport>)
        .with_timeout(GATHER)
        .with_expected_replies(workers)
}

/// Everything a test needs to keep hold of after the worker is moved into its
/// run loop.
struct Harness {
    broker: Arc<InMemoryBroker>,
    transport: Arc<InMemoryControlTransport>,
    handle: crate::WorkerHandle,
    stats: Arc<crate::WorkerStats>,
    revocations: WorkerRevocationManager,
    time_limits: WorkerTimeLimits,
}

impl Harness {
    /// Start a worker with remote control enabled and wait until it is
    /// subscribed to the control channel.
    async fn start(hostname: &str, registry: TaskRegistry) -> Self {
        let broker = Arc::new(InMemoryBroker::new());
        let transport = Arc::new(InMemoryControlTransport::new());
        let time_limits = WorkerTimeLimits::new();

        let worker = Worker::new_from_arc(Arc::clone(&broker), registry, fast_config(hostname))
            .with_control_transport(Arc::clone(&transport) as Arc<dyn ControlTransport>)
            .with_revocation_watcher(RevocationWatcher::new())
            .with_time_limits(time_limits.clone())
            .with_broker_url("memory://celers-test");

        let stats = worker.stats_arc();
        let revocations = worker.revocations().clone();
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // The subscriber is spawned by the run loop; wait for it so the first
        // broadcast is not published into an empty channel.
        let subscribed = Arc::clone(&transport);
        wait_until("the worker to subscribe to the control channel", || {
            subscribed.subscriber_count() > 0
        })
        .await;

        Self {
            broker,
            transport,
            handle,
            stats,
            revocations,
            time_limits,
        }
    }

    fn client(&self) -> ControlClient {
        client(&self.transport, 1)
    }

    /// Send one command and return the single reply.
    async fn send(&self, command: ControlCommand) -> ControlReply {
        single(self.client().broadcast(command).await.expect("broadcast"))
    }

    /// Send one inspect command and return the payload.
    async fn inspect(&self, command: InspectCommand) -> InspectResponse {
        match self.send(ControlCommand::Inspect(command)).await.response {
            ControlResponse::Inspect(payload) => *payload,
            other => panic!("expected an inspect response, got {other:?}"),
        }
    }

    async fn shutdown(&self) {
        let _ = self.handle.shutdown().await;
    }
}

#[tokio::test]
async fn ping_reaches_the_running_worker() {
    let harness = Harness::start("ping-worker", TaskRegistry::new()).await;

    let replies = harness.client().ping().await.expect("ping");
    let reply = single(replies);

    assert_eq!(reply.hostname, "ping-worker");
    match reply.response {
        ControlResponse::Pong {
            ref hostname,
            timestamp,
        } => {
            assert_eq!(hostname, "ping-worker");
            assert!(timestamp > 0.0);
        }
        other => panic!("expected a pong, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_registered_lists_the_worker_task_registry() {
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::new(AtomicUsize::new(0)),
            name: "beta",
        })
        .await;
    registry
        .register(CountingTask {
            runs: Arc::new(AtomicUsize::new(0)),
            name: "alpha",
        })
        .await;

    let harness = Harness::start("registry-worker", registry).await;

    match harness.inspect(InspectCommand::Registered).await {
        InspectResponse::Registered(names) => assert_eq!(names, vec!["alpha", "beta"]),
        other => panic!("expected registered names, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_active_reports_the_task_the_worker_is_running() {
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let registry = TaskRegistry::new();
    registry
        .register(BlockingTask {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .await;

    let harness = Harness::start("active-worker", registry).await;

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    let task = SerializedTask::new("blocking_task".to_string(), payload);
    let task_id = task.metadata.id;
    harness.broker.enqueue(task).await.expect("enqueue");

    let running = Arc::clone(&started);
    wait_until("the blocking task to start", || {
        running.load(Ordering::SeqCst) == 1
    })
    .await;

    match harness.inspect(InspectCommand::Active).await {
        InspectResponse::Active(active) => {
            assert_eq!(active.len(), 1, "exactly one task is running");
            let info = &active[0];
            assert_eq!(info.id, task_id);
            assert_eq!(info.name, "blocking_task");
            assert_eq!(info.hostname, "active-worker");
            assert_eq!(info.args, "{}", "the payload preview is reported");
            assert!(info.started > 0.0);
            assert_eq!(
                info.delivery_info.as_ref().map(|d| d.queue.as_str()),
                Some("celery")
            );
        }
        other => panic!("expected active tasks, got {other:?}"),
    }

    // Scheduled and reserved are always empty for a CeleRS worker; assert that
    // rather than leaving it as an undocumented surprise.
    match harness.inspect(InspectCommand::Scheduled).await {
        InspectResponse::Scheduled(tasks) => assert!(tasks.is_empty()),
        other => panic!("expected scheduled tasks, got {other:?}"),
    }
    match harness.inspect(InspectCommand::Reserved).await {
        InspectResponse::Reserved(tasks) => assert!(tasks.is_empty()),
        other => panic!("expected reserved tasks, got {other:?}"),
    }

    release.notify_waiters();
    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_stats_and_conf_report_live_worker_state() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "counted",
        })
        .await;

    let harness = Harness::start("stats-worker", registry).await;

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    harness
        .broker
        .enqueue(SerializedTask::new("counted".to_string(), payload))
        .await
        .expect("enqueue");

    let done = Arc::clone(&runs);
    wait_until("the counted task to run", || {
        done.load(Ordering::SeqCst) == 1
    })
    .await;
    let processed = Arc::clone(&harness.stats);
    wait_until("the worker to record the completion", || {
        processed.processed() >= 1
    })
    .await;

    match harness.inspect(InspectCommand::Stats).await {
        InspectResponse::Stats(stats) => {
            assert_eq!(stats.total_tasks, 1);
            assert_eq!(stats.active_tasks, 0);
            assert_eq!(stats.succeeded, 1);
            assert_eq!(stats.failed, 0);
            let pool = stats.pool.expect("pool stats");
            assert_eq!(pool.max_concurrency, 4);
            assert_eq!(pool.available, 4);
            let broker = stats.broker.expect("broker stats reported when known");
            assert_eq!(broker.url, "memory://celers-test");
            assert_eq!(broker.transport, "memory");
        }
        other => panic!("expected stats, got {other:?}"),
    }

    match harness.inspect(InspectCommand::Conf).await {
        InspectResponse::Conf(conf) => {
            assert_eq!(conf.hostname, "stats-worker");
            assert_eq!(conf.default_queue, "celery");
            assert_eq!(conf.concurrency, 4);
            assert!(conf.task_acks_late);
            assert_eq!(conf.broker_url, "memory://celers-test");
        }
        other => panic!("expected conf, got {other:?}"),
    }

    match harness.inspect(InspectCommand::QueueInfo).await {
        InspectResponse::QueueInfo(queues) => {
            let stats = queues.get("celery").expect("the worker's own queue");
            assert_eq!(stats.messages, 0);
            assert_eq!(stats.consumers, 1);
        }
        other => panic!("expected queue info, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn rate_limit_command_actually_throttles_the_running_worker() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "throttled",
        })
        .await;

    let harness = Harness::start("rate-worker", registry).await;

    // One task per second, so the single burst token is spent immediately and
    // everything after it must be deferred rather than executed.
    let reply = harness
        .send(ControlCommand::rate_limit("throttled", Some(1.0)))
        .await;
    assert!(
        matches!(reply.response, ControlResponse::Ack { ok: true, .. }),
        "rate limit was not accepted: {:?}",
        reply.response
    );

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    for _ in 0..5 {
        harness
            .broker
            .enqueue(SerializedTask::new(
                "throttled".to_string(),
                payload.clone(),
            ))
            .await
            .expect("enqueue");
    }

    // Give the worker ample time to burn through all five if it were not
    // throttled (five immediate tasks take milliseconds).
    let rate_limited = Arc::clone(&harness.stats);
    wait_until("the worker to defer a rate-limited task", || {
        rate_limited.rate_limited() > 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let executed = runs.load(Ordering::SeqCst);
    assert!(
        executed <= 2,
        "a 1/s limit must not let five tasks through: {executed} ran"
    );
    assert!(
        harness.broker.queue_size().await.expect("queue size") > 0,
        "throttled tasks must stay in the queue, not be dropped"
    );

    // Removing the limit lets the backlog drain.
    let reply = harness
        .send(ControlCommand::rate_limit("throttled", None))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    let drained = Arc::clone(&runs);
    wait_until("the backlog to drain once the limit is removed", || {
        drained.load(Ordering::SeqCst) == 5
    })
    .await;

    harness.shutdown().await;
}

#[tokio::test]
async fn rate_limit_command_rejects_a_nonsense_rate() {
    let harness = Harness::start("bad-rate-worker", TaskRegistry::new()).await;

    let reply = harness
        .send(ControlCommand::rate_limit("whatever", Some(0.0)))
        .await;
    assert!(
        matches!(reply.response, ControlResponse::Error { .. }),
        "a zero rate must be refused, not silently installed: {:?}",
        reply.response
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn time_limit_command_reaches_the_limits_the_worker_resolves() {
    let harness = Harness::start("limit-worker", TaskRegistry::new()).await;

    assert!(
        harness
            .time_limits
            .create_tracker("probe", "slow")
            .is_none(),
        "no limit is configured before the command"
    );

    let reply = harness
        .send(ControlCommand::time_limit("slow", Some(10), Some(30)))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    // The very same `WorkerTimeLimits` the run loop reads per task.
    let tracker = harness
        .time_limits
        .create_tracker("probe", "slow")
        .expect("the command installed a limit the worker can resolve");
    assert_eq!(tracker.config().soft_millis, Some(10_000));
    assert_eq!(tracker.config().hard_millis, Some(30_000));

    let reply = harness
        .send(ControlCommand::time_limit("slow", None, None))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));
    assert!(
        harness
            .time_limits
            .create_tracker("probe", "slow")
            .is_none(),
        "clearing both halves removes the limit"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn revoke_by_pattern_stops_matching_tasks_from_running() {
    let doomed = Arc::new(AtomicUsize::new(0));
    let spared = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&doomed),
            name: "report.nightly",
        })
        .await;
    registry
        .register(CountingTask {
            runs: Arc::clone(&spared),
            name: "email.welcome",
        })
        .await;

    let harness = Harness::start("revoke-worker", registry).await;

    let reply = harness
        .send(ControlCommand::revoke_by_pattern("report.*", true))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    harness
        .broker
        .enqueue(SerializedTask::new(
            "report.nightly".to_string(),
            payload.clone(),
        ))
        .await
        .expect("enqueue");
    harness
        .broker
        .enqueue(SerializedTask::new("email.welcome".to_string(), payload))
        .await
        .expect("enqueue");

    let survivor = Arc::clone(&spared);
    wait_until("the unmatched task to run", || {
        survivor.load(Ordering::SeqCst) == 1
    })
    .await;
    let revoked = Arc::clone(&harness.stats);
    wait_until("the matched task to be revoked", || revoked.revoked() == 1).await;

    assert_eq!(
        doomed.load(Ordering::SeqCst),
        0,
        "a revoked task must never execute"
    );
    assert_eq!(
        harness.broker.queue_size().await.expect("queue size"),
        0,
        "a revoked task is acknowledged, not left to be redelivered"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn revoke_command_records_the_id_and_reports_it_back() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "cancellable",
        })
        .await;

    let harness = Harness::start("revoke-id-worker", registry).await;

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    let task = SerializedTask::new("cancellable".to_string(), payload);
    let task_id = task.metadata.id;

    let reply = harness.send(ControlCommand::revoke(task_id, true)).await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));
    assert!(
        harness.revocations.is_revoked(task_id),
        "the worker must remember the revocation"
    );

    match harness.inspect(InspectCommand::Revoked).await {
        InspectResponse::Revoked(ids) => assert_eq!(ids, vec![task_id]),
        other => panic!("expected revoked ids, got {other:?}"),
    }

    // Enqueued after the revocation: the broker drops it (the control handler
    // asked it to) or the worker drops it; either way it must not execute.
    harness.broker.enqueue(task).await.expect("enqueue");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "a revoked task must never execute"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn cancel_consumer_suspends_and_add_consumer_resumes_the_worker() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "paused",
        })
        .await;

    let harness = Harness::start("consumer-worker", registry).await;

    let reply = harness
        .send(ControlCommand::cancel_consumer("celery"))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));
    assert_eq!(harness.handle.mode(), WorkerMode::Maintenance);

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    harness
        .broker
        .enqueue(SerializedTask::new("paused".to_string(), payload))
        .await
        .expect("enqueue");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "a suspended consumer must not run the task"
    );
    assert_eq!(
        harness.broker.queue_size().await.expect("queue size"),
        1,
        "a message dequeued while suspended is returned to the queue, not held"
    );

    let reply = harness.send(ControlCommand::add_consumer("celery")).await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));
    assert_eq!(harness.handle.mode(), WorkerMode::Normal);

    let resumed = Arc::clone(&runs);
    wait_until("the resumed worker to run the task", || {
        resumed.load(Ordering::SeqCst) == 1
    })
    .await;

    // A queue this worker does not serve is refused with a reason.
    let reply = harness.send(ControlCommand::add_consumer("other")).await;
    assert!(matches!(reply.response, ControlResponse::Error { .. }));

    harness.shutdown().await;
}

#[tokio::test]
async fn queue_length_is_answered_and_purge_is_honestly_refused() {
    let harness = Harness::start("queue-worker", TaskRegistry::new()).await;

    // Suspend consumption first, so the message stays queued for the length
    // probe instead of racing the dequeue loop.
    harness
        .send(ControlCommand::cancel_consumer("celery"))
        .await;

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    harness
        .broker
        .enqueue(SerializedTask::new("unregistered".to_string(), payload))
        .await
        .expect("enqueue");

    let queued = Arc::clone(&harness.broker);
    let deadline = Instant::now() + WAIT_DEADLINE;
    while queued.queue_size().await.expect("queue size") != 1 {
        assert!(
            Instant::now() < deadline,
            "the message never settled queued"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let reply = harness.send(ControlCommand::queue_length("celery")).await;
    match reply.response {
        ControlResponse::Queue(celers_core::QueueResponse::Length {
            ref queue,
            message_count,
        }) => {
            assert_eq!(queue, "celery");
            assert_eq!(message_count, 1);
        }
        other => panic!("expected a queue length, got {other:?}"),
    }

    let reply = harness.send(ControlCommand::queue_purge("celery")).await;
    match reply.response {
        ControlResponse::Error { ref error } => {
            assert!(
                error.contains("queue purge") && error.contains("Broker"),
                "the refusal must name the operation and the reason: {error}"
            );
        }
        other => panic!("purge must be refused, not faked: {other:?}"),
    }

    // Another worker's queue is not something this worker can measure.
    let reply = harness.send(ControlCommand::queue_length("other")).await;
    assert!(matches!(reply.response, ControlResponse::Error { .. }));

    harness.shutdown().await;
}

#[tokio::test]
async fn reset_circuit_breaker_reports_when_no_breaker_is_running() {
    let harness = Harness::start("no-breaker-worker", TaskRegistry::new()).await;

    match harness.inspect(InspectCommand::CircuitBreakers).await {
        InspectResponse::CircuitBreakers(states) => assert!(states.is_empty()),
        other => panic!("expected breaker states, got {other:?}"),
    }

    let reply = harness
        .send(ControlCommand::reset_circuit_breaker(None))
        .await;
    match reply.response {
        ControlResponse::Error { ref error } => {
            assert!(
                error.contains("circuit breaker"),
                "unhelpful error: {error}"
            );
        }
        other => panic!("expected an explanatory error, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn reset_circuit_breaker_closes_a_tripped_breaker() {
    let registry = TaskRegistry::new();
    let broker = Arc::new(InMemoryBroker::new());
    let transport = Arc::new(InMemoryControlTransport::new());

    let mut config = fast_config("breaker-worker");
    config.enable_circuit_breaker = true;
    config.circuit_breaker_config.failure_threshold = 1;
    config.circuit_breaker_config.timeout_secs = 3600;

    let worker = Worker::new_from_arc(Arc::clone(&broker), registry, config)
        .with_control_transport(Arc::clone(&transport) as Arc<dyn ControlTransport>);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let subscribed = Arc::clone(&transport);
    wait_until("the worker to subscribe", || {
        subscribed.subscriber_count() > 0
    })
    .await;

    let control = client(&transport, 1);

    // An unregistered task fails, tripping the breaker at the first failure.
    broker
        .enqueue(SerializedTask::new("nope".to_string(), vec![]))
        .await
        .expect("enqueue");

    let probe = client(&transport, 1);
    let tripped = || async {
        match single(
            probe
                .broadcast(ControlCommand::inspect_circuit_breakers())
                .await
                .expect("broadcast"),
        )
        .response
        {
            ControlResponse::Inspect(payload) => match *payload {
                InspectResponse::CircuitBreakers(states) => states.get("nope").cloned(),
                other => panic!("expected breaker states, got {other:?}"),
            },
            other => panic!("expected an inspect response, got {other:?}"),
        }
    };

    let deadline = Instant::now() + WAIT_DEADLINE;
    loop {
        if tripped().await.as_deref() == Some("Open") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the breaker never opened for the failing task"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let reply = single(
        control
            .broadcast(ControlCommand::reset_circuit_breaker(Some(
                "nope".to_string(),
            )))
            .await
            .expect("broadcast"),
    );
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    assert_ne!(
        tripped().await.as_deref(),
        Some("Open"),
        "the reset must actually close the breaker"
    );

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn shutdown_command_stops_the_running_worker() {
    let harness = Harness::start("shutdown-worker", TaskRegistry::new()).await;

    let reply = harness.send(ControlCommand::shutdown(Some(2))).await;
    match reply.response {
        ControlResponse::Ack { ok: true, .. } => {}
        other => panic!("expected an ack, got {other:?}"),
    }

    // Draining mode is what the run loop actually reads to stop dequeuing.
    assert_eq!(harness.handle.mode(), WorkerMode::Draining);

    let runs = Arc::new(AtomicUsize::new(0));
    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    harness
        .broker
        .enqueue(SerializedTask::new("anything".to_string(), payload))
        .await
        .expect("enqueue");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        harness.broker.queue_size().await.expect("queue size"),
        1,
        "a shut-down worker stops dequeuing"
    );
}

/// A transport whose reply publish yields, the way a real network publish does.
///
/// [`InMemoryControlTransport`] delivers a reply through a `broadcast::Sender`
/// without ever yielding, which hides every teardown race in the reply path.
/// A Redis `PUBLISH` awaits a socket. This wrapper reproduces that timing
/// hermetically so the race is testable without a server.
struct SlowReplyTransport {
    inner: InMemoryControlTransport,
    delay: Duration,
}

#[async_trait::async_trait]
impl ControlTransport for SlowReplyTransport {
    async fn broadcast(
        &self,
        envelope: &celers_core::control_transport::ControlEnvelope,
    ) -> celers_core::Result<usize> {
        self.inner.broadcast(envelope).await
    }

    async fn subscribe_commands(
        &self,
    ) -> celers_core::Result<Box<dyn celers_core::control_transport::ControlCommandStream>> {
        self.inner.subscribe_commands().await
    }

    async fn send_reply(&self, reply_to: &str, reply: &ControlReply) -> celers_core::Result<()> {
        tokio::time::sleep(self.delay).await;
        self.inner.send_reply(reply_to, reply).await
    }

    async fn subscribe_replies(
        &self,
        reply_to: &str,
    ) -> celers_core::Result<Box<dyn celers_core::control_transport::ControlReplyStream>> {
        self.inner.subscribe_replies(reply_to).await
    }
}

#[tokio::test]
async fn add_consumer_cannot_cancel_a_shutdown_in_progress() {
    // A shutdown is one-way. Writing `Normal` over `Draining` would, in the
    // window before the run loop notices, resurrect a worker the operator
    // believes has stopped — and afterwards report success while doing nothing.
    let harness = Harness::start("one-way-worker", TaskRegistry::new()).await;

    let reply = harness.send(ControlCommand::shutdown(None)).await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));
    assert_eq!(harness.handle.mode(), WorkerMode::Draining);

    for command in [
        ControlCommand::add_consumer("celery"),
        ControlCommand::cancel_consumer("celery"),
    ] {
        let reply = harness.send(command).await;
        match reply.response {
            ControlResponse::Error { ref error } => {
                assert!(
                    error.contains("shutting down"),
                    "the refusal must say why: {error}"
                );
            }
            other => panic!("a mode change during shutdown must be refused, got {other:?}"),
        }
        assert_eq!(
            harness.handle.mode(),
            WorkerMode::Draining,
            "a refused command must not have changed the mode"
        );
    }
}

/// A broker that is always empty and never blocks.
///
/// [`InMemoryBroker::dequeue`] parks indefinitely on an empty queue, so a
/// worker using it stays inside that call and never reaches the top of its run
/// loop — which hides every shutdown-path race behind "the loop never exited".
/// A real broker's dequeue has a block timeout; this double has none at all,
/// which is the timing the shutdown path has to be correct under.
struct IdleBroker;

#[async_trait::async_trait]
impl Broker for IdleBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<celers_core::TaskId> {
        Ok(task.metadata.id)
    }

    async fn dequeue(&self) -> Result<Option<celers_core::BrokerMessage>> {
        Ok(None)
    }

    async fn ack(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(0)
    }

    async fn cancel(&self, _task_id: &celers_core::TaskId) -> Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn shutdown_is_acknowledged_even_though_it_stops_the_worker() {
    // Regression: the run loop used to abort the control listener the instant
    // it exited, cancelling the in-flight reply to the very command that made
    // it exit. Over Redis (where publishing yields) `celers control shutdown`
    // then reported "no worker answered" while the worker had in fact shut
    // down — indistinguishable from a command that never arrived.
    let inner = InMemoryControlTransport::new();
    let transport = Arc::new(SlowReplyTransport {
        inner: inner.clone(),
        delay: Duration::from_millis(250),
    });

    let worker = Worker::new_from_arc(
        Arc::new(IdleBroker),
        TaskRegistry::new(),
        fast_config("ack-worker"),
    )
    .with_control_transport(Arc::clone(&transport) as Arc<dyn ControlTransport>);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the worker to subscribe", || inner.subscriber_count() > 0).await;

    let replies = ControlClient::new(Arc::clone(&transport) as Arc<dyn ControlTransport>)
        .with_timeout(Duration::from_secs(3))
        .with_expected_replies(1)
        .broadcast(ControlCommand::shutdown(Some(1)))
        .await
        .expect("broadcast");

    assert_eq!(
        replies.len(),
        1,
        "the shutdown command must be acknowledged before the listener is torn down"
    );
    assert!(matches!(
        replies[0].response,
        ControlResponse::Ack { ok: true, .. }
    ));
    assert_eq!(handle.mode(), WorkerMode::Draining);
}

#[tokio::test]
async fn a_destination_filter_targets_exactly_one_worker() {
    // Two workers on one channel: the classic reason an envelope carries a
    // destination list at all.
    let transport = Arc::new(InMemoryControlTransport::new());
    let mut handles = Vec::new();

    for hostname in ["fleet-a", "fleet-b"] {
        let worker = Worker::new_from_arc(
            Arc::new(InMemoryBroker::new()),
            TaskRegistry::new(),
            fast_config(hostname),
        )
        .with_control_transport(Arc::clone(&transport) as Arc<dyn ControlTransport>);
        handles.push(worker.run_with_shutdown().await.expect("worker starts"));
    }

    let subscribed = Arc::clone(&transport);
    wait_until("both workers to subscribe", || {
        subscribed.subscriber_count() == 2
    })
    .await;

    let broadcast = client(&transport, 2).ping().await.expect("ping");
    assert_eq!(broadcast.len(), 2, "an unaddressed ping reaches everyone");

    let targeted = ControlClient::new(Arc::clone(&transport) as Arc<dyn ControlTransport>)
        .with_timeout(Duration::from_millis(400))
        .with_destination(vec!["fleet-b".to_string()])
        .ping()
        .await
        .expect("ping");
    assert_eq!(targeted.len(), 1);
    assert_eq!(targeted[0].hostname, "fleet-b");

    for handle in handles {
        let _ = handle.shutdown().await;
    }
}
