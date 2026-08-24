//! End-to-end tests of the remote worker control protocol over real Redis.
//!
//! These are the multi-process shape of the hermetic suite in
//! `celers_worker::control_tests`: a real [`Worker`] subscribes to a Redis
//! Pub/Sub control channel through
//! [`RedisControlTransport`](celers_broker_redis::RedisControlTransport), and a
//! separate [`ControlClient`] — the one `celers inspect` / `celers control`
//! use — broadcasts commands and gathers replies. Nothing is mocked: the
//! command crosses the Redis wire in both directions.
//!
//! This crate is the only one that depends on both `celers-worker` and
//! `celers-broker-redis`, which is why the cross-crate test lives here.
//!
//! # Running
//!
//! ```sh
//! export CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379
//! cargo nextest run -p celers-cli --test control_redis
//! ```
//!
//! Without `CELERS_TEST_REDIS_URL` every test returns immediately, so the
//! default suite stays hermetic. RESP3 (Redis 6.0+) is required: the transport
//! delivers subscriptions through server pushes.

use celers_broker_redis::{QueueMode, RedisBroker, RedisControlTransport};
use celers_cli::commands::{run_inspect, ControlOptions};
use celers_core::control::{ControlCommand, ControlResponse, InspectCommand, InspectResponse};
use celers_core::control_transport::{ControlClient, ControlReply, ControlTransport};
use celers_core::{Broker, Result, SerializedTask, Task, TaskRegistry};
use celers_worker::{Worker, WorkerConfig, WorkerHandle};

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

/// Completes immediately, counting executions.
struct CountingTask {
    runs: Arc<AtomicUsize>,
    name: &'static str,
}

// Written in the desugared `#[async_trait]` form on purpose: `celers-cli` does
// not depend on the `async-trait` crate, and this test may not add one (see the
// crate's manifest follow-ups). The shape is exactly what the attribute would
// expand to on `async fn execute(&self, input) -> Result<Output>`.
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

/// A worker plus the handles a test needs after it is moved into its run loop.
struct LiveWorker {
    broker: Arc<RedisBroker>,
    transport: Arc<RedisControlTransport>,
    handle: WorkerHandle,
    hostname: String,
    queue: String,
}

impl LiveWorker {
    /// Start a worker bound to a run-unique queue and control channel.
    async fn start(url: &str, label: &str, registry: TaskRegistry) -> Self {
        let run_id = Uuid::new_v4();
        let queue = format!("celers-test-{label}-{run_id}");
        let hostname = format!("{label}-{run_id}");
        // A channel per run: several test binaries (and a developer's own
        // worker) can share one Redis without answering each other's commands.
        let transport = Arc::new(
            RedisControlTransport::with_channel(url, format!("celers.test.control.{run_id}"))
                .expect("control transport"),
        );

        let broker =
            Arc::new(RedisBroker::with_mode(url, &queue, QueueMode::Fifo).expect("redis broker"));

        let config = WorkerConfig {
            concurrency: 2,
            poll_interval_ms: 20,
            defer_delay_ms: 20,
            defer_max_delay_ms: 60,
            shutdown_timeout_secs: 5,
            queue_name: queue.clone(),
            hostname: hostname.clone(),
            ..Default::default()
        };

        let worker = Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_control_transport(Arc::clone(&transport) as Arc<dyn ControlTransport>)
            .with_broker_url(url);
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        let live = Self {
            broker,
            transport,
            handle,
            hostname,
            queue,
        };
        live.wait_until_reachable().await;
        live
    }

    /// A client addressed at this worker's control channel.
    fn client(&self) -> ControlClient {
        ControlClient::new(Arc::clone(&self.transport) as Arc<dyn ControlTransport>)
            .with_timeout(Duration::from_secs(5))
            .with_expected_replies(1)
    }

    /// Block until the worker answers a ping, so a test never races the
    /// subscription setup.
    async fn wait_until_reachable(&self) {
        let deadline = Instant::now() + WAIT_DEADLINE;
        loop {
            let replies =
                ControlClient::new(Arc::clone(&self.transport) as Arc<dyn ControlTransport>)
                    .with_timeout(Duration::from_millis(300))
                    .with_expected_replies(1)
                    .ping()
                    .await
                    .expect("ping");
            if !replies.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "worker {} never joined the control channel",
                self.hostname
            );
        }
    }

    /// Send one command and return the single reply.
    async fn send(&self, command: ControlCommand) -> ControlReply {
        let replies = self.client().broadcast(command).await.expect("broadcast");
        assert_eq!(replies.len(), 1, "expected exactly one worker to answer");
        replies.into_iter().next().expect("one reply")
    }

    /// Send one inspect command and return the payload.
    async fn inspect(&self, command: InspectCommand) -> InspectResponse {
        match self.send(ControlCommand::Inspect(command)).await.response {
            ControlResponse::Inspect(payload) => *payload,
            other => panic!("expected an inspect response, got {other:?}"),
        }
    }

    /// Stop the worker and delete the Redis keys it used.
    async fn cleanup(self) {
        let _ = self.handle.shutdown().await;
        // Best-effort: a leftover key would only waste space in a test server.
        if let Ok(client) = redis::Client::open(
            std::env::var("CELERS_TEST_REDIS_URL").unwrap_or_else(|_| String::new()),
        ) {
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                let keys = self.broker.queue_names();
                let _: std::result::Result<i64, _> =
                    redis::AsyncCommands::del(&mut conn, &keys).await;
            }
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
async fn live_ping_and_inspect_cross_the_redis_wire() {
    let Some(url) = redis_url() else {
        return;
    };

    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::new(AtomicUsize::new(0)),
            name: "live.echo",
        })
        .await;

    let worker = LiveWorker::start(&url, "inspect", registry).await;

    let replies = worker.client().ping().await.expect("ping");
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].hostname, worker.hostname);

    match worker.inspect(InspectCommand::Registered).await {
        InspectResponse::Registered(names) => assert_eq!(names, vec!["live.echo"]),
        other => panic!("expected registered names, got {other:?}"),
    }

    match worker.inspect(InspectCommand::Conf).await {
        InspectResponse::Conf(conf) => {
            assert_eq!(conf.hostname, worker.hostname);
            assert_eq!(conf.default_queue, worker.queue);
            // Credentials are stripped before a URL is reported; this one has
            // none, so it comes back verbatim.
            assert_eq!(conf.broker_url, url);
        }
        other => panic!("expected conf, got {other:?}"),
    }

    match worker.inspect(InspectCommand::QueueInfo).await {
        InspectResponse::QueueInfo(queues) => {
            assert!(
                queues.contains_key(&worker.queue),
                "the worker must report its own queue, got {queues:?}"
            );
        }
        other => panic!("expected queue info, got {other:?}"),
    }

    worker.cleanup().await;
}

#[tokio::test]
async fn live_rate_limit_command_throttles_a_worker_over_redis() {
    let Some(url) = redis_url() else {
        return;
    };

    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "live.throttled",
        })
        .await;

    let worker = LiveWorker::start(&url, "ratelimit", registry).await;

    let reply = worker
        .send(ControlCommand::rate_limit("live.throttled", Some(1.0)))
        .await;
    assert!(
        matches!(reply.response, ControlResponse::Ack { ok: true, .. }),
        "rate limit refused: {:?}",
        reply.response
    );

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    for _ in 0..4 {
        worker
            .broker
            .enqueue(SerializedTask::new(
                "live.throttled".to_string(),
                payload.clone(),
            ))
            .await
            .expect("enqueue");
    }

    // Well beyond the time four unthrottled tasks would take.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let executed = runs.load(Ordering::SeqCst);
    assert!(
        executed <= 2,
        "a 1/s limit sent over Redis must throttle the worker: {executed} of 4 ran"
    );

    // Lifting the limit lets the backlog drain, proving the throttle was the
    // cause rather than a stalled worker.
    let reply = worker
        .send(ControlCommand::rate_limit("live.throttled", None))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    let drained = Arc::clone(&runs);
    wait_until("the backlog to drain after the limit is lifted", || {
        drained.load(Ordering::SeqCst) == 4
    })
    .await;

    worker.cleanup().await;
}

#[tokio::test]
async fn live_revoke_by_pattern_stops_matching_tasks() {
    let Some(url) = redis_url() else {
        return;
    };

    let doomed = Arc::new(AtomicUsize::new(0));
    let spared = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&doomed),
            name: "live.report.nightly",
        })
        .await;
    registry
        .register(CountingTask {
            runs: Arc::clone(&spared),
            name: "live.email.welcome",
        })
        .await;

    let worker = LiveWorker::start(&url, "revoke", registry).await;

    let reply = worker
        .send(ControlCommand::revoke_by_pattern("live.report.*", true))
        .await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    worker
        .broker
        .enqueue(SerializedTask::new(
            "live.report.nightly".to_string(),
            payload.clone(),
        ))
        .await
        .expect("enqueue");
    worker
        .broker
        .enqueue(SerializedTask::new(
            "live.email.welcome".to_string(),
            payload,
        ))
        .await
        .expect("enqueue");

    let survivor = Arc::clone(&spared);
    wait_until("the unmatched task to run", || {
        survivor.load(Ordering::SeqCst) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        doomed.load(Ordering::SeqCst),
        0,
        "a task revoked over Redis must never execute"
    );

    worker.cleanup().await;
}

#[tokio::test]
async fn live_shutdown_command_stops_the_worker() {
    let Some(url) = redis_url() else {
        return;
    };

    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "live.after_shutdown",
        })
        .await;

    let worker = LiveWorker::start(&url, "shutdown", registry).await;

    let reply = worker.send(ControlCommand::shutdown(Some(2))).await;
    assert!(matches!(
        reply.response,
        ControlResponse::Ack { ok: true, .. }
    ));

    let payload = serde_json::to_vec(&Empty {}).expect("payload");
    worker
        .broker
        .enqueue(SerializedTask::new(
            "live.after_shutdown".to_string(),
            payload,
        ))
        .await
        .expect("enqueue");

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "a worker shut down over Redis must stop consuming"
    );

    worker.cleanup().await;
}

#[tokio::test]
async fn live_cli_inspect_helper_runs_against_a_real_worker() {
    let Some(url) = redis_url() else {
        return;
    };

    let worker = LiveWorker::start(&url, "cli", TaskRegistry::new()).await;

    // The exact path `celers inspect stats` takes, including its rendering.
    let mut options = ControlOptions::new(&url);
    options.channel = Some(worker.transport.channel().to_string());
    options.timeout_secs = 1.0;

    run_inspect(&options, InspectCommand::Stats)
        .await
        .expect("the CLI inspect path completes against a live worker");

    // And the JSON rendering, which is what a script would consume.
    options.json = true;
    run_inspect(&options, InspectCommand::Active)
        .await
        .expect("the CLI JSON path completes");

    // A destination naming nobody must still succeed, reporting zero replies
    // rather than erroring.
    options.destination = vec!["no-such-worker".to_string()];
    options.timeout_secs = 0.3;
    run_inspect(&options, InspectCommand::Stats)
        .await
        .expect("an unmatched destination is not an error");

    worker.cleanup().await;
}
