//! End-to-end tests for the worker's opt-in security controls.
//!
//! Everything here drives a *running* worker over the in-process
//! [`InMemoryBroker`] and, where `inspect active` is involved, the in-process
//! [`InMemoryControlTransport`]. Nothing external is required.
//!
//! What is pinned:
//!
//! * a message that fails signature verification never reaches a task handler,
//!   is counted, and is dead-lettered rather than requeued;
//! * a message that verifies runs normally;
//! * both controls are **off** by default;
//! * with hygiene configured, the arguments an operator sees through
//!   `inspect active` are redacted while the executing payload is not.

use celers_core::control::{ControlCommand, ControlResponse, InspectCommand, InspectResponse};
use celers_core::control_transport::{ControlClient, ControlTransport, InMemoryControlTransport};
use celers_core::task_security::{sign_task, PayloadHygiene, SigningOptions};
use celers_core::task_signature::{FreshnessWindow, ReplayGuard, TaskSigner};
use celers_core::{Broker, InMemoryBroker, SerializedTask, Task, TaskRegistry};
use celers_worker::{DlqConfig, SignatureVerification, Worker, WorkerConfig, WorkerHandle};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Deadline for every "wait until" helper: generous for a loaded box, short
/// enough that a genuine hang fails instead of hanging the suite.
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

/// The signing key both sides of these tests share.
const KEY: &[u8] = b"integration-test-key-integration!";

/// Records every payload it was handed, so a test can prove what did — and did
/// not — reach a task handler.
struct RecordingTask {
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[async_trait::async_trait]
impl Task for RecordingTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(input.clone());
        Ok(input)
    }

    fn name(&self) -> &str {
        "recording_task"
    }
}

/// Blocks until released, so `inspect active` has something to report.
struct BlockingTask {
    started: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Task for BlockingTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(input)
    }

    fn name(&self) -> &str {
        "blocking_task"
    }
}

/// A configuration that reacts fast enough for a test.
fn fast_config(hostname: &str) -> WorkerConfig {
    WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 10,
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

/// Build a task whose payload is `payload`, signed with [`KEY`].
fn signed_task(payload: serde_json::Value) -> SerializedTask {
    let bytes = serde_json::to_vec(&payload).expect("payload serializes");
    let mut task = SerializedTask::new("recording_task".to_string(), bytes);
    sign_task(&TaskSigner::new(KEY), &mut task, SigningOptions::default());
    task
}

/// What a test keeps hold of after the worker is moved into its run loop.
struct Running {
    handle: WorkerHandle,
    stats: Arc<celers_worker::WorkerStats>,
    dlq: Option<Arc<celers_worker::DlqHandler>>,
}

impl Running {
    async fn shutdown(self) {
        let _ = self.handle.shutdown().await;
    }
}

/// Start a worker over `broker` with `config`, returning its observable handles.
async fn start(
    broker: &Arc<InMemoryBroker>,
    registry: TaskRegistry,
    config: WorkerConfig,
) -> Running {
    let worker = Worker::new_from_arc(Arc::clone(broker), registry, config);
    let stats = worker.stats_arc();
    let dlq = worker.dlq_handler().cloned();
    let handle = worker.run_with_shutdown().await.expect("worker starts");
    Running { handle, stats, dlq }
}

#[tokio::test]
async fn a_tampered_message_is_rejected_before_it_reaches_a_handler() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = TaskRegistry::new();
    registry
        .register(RecordingTask {
            seen: Arc::clone(&seen),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let config = WorkerConfig {
        enable_dlq: true,
        dlq_config: DlqConfig::new(true),
        signature_verification: Some(SignatureVerification::new(TaskSigner::new(KEY))),
        ..fast_config("signature-worker")
    };
    let running = start(&broker, registry, config).await;

    // One authentic message, one whose payload was rewritten after signing.
    let good = signed_task(serde_json::json!({"amount": 10}));
    let mut tampered = signed_task(serde_json::json!({"amount": 10}));
    let tampered_id = tampered.metadata.id;
    tampered.payload =
        serde_json::to_vec(&serde_json::json!({"amount": 1_000_000})).expect("payload serializes");

    broker.enqueue(good).await.expect("enqueue");
    broker.enqueue(tampered).await.expect("enqueue");

    let rejected = Arc::clone(&running.stats);
    wait_until("the tampered message to be rejected", || {
        rejected.signature_rejected() == 1
    })
    .await;

    let executed = Arc::clone(&seen);
    wait_until("the authentic message to run", || {
        executed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == 1
    })
    .await;

    // The handler saw the authentic call and nothing else.
    let calls = seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(calls, vec![serde_json::json!({"amount": 10})]);

    // The forged message is recorded, not requeued.
    let dlq = running.dlq.clone().expect("dlq enabled");
    let entry = dlq.get_entry(&tampered_id).await.expect("dlq entry");
    assert_eq!(
        entry.metadata.get("failure_type").map(String::as_str),
        Some("signature_verification")
    );
    assert!(entry
        .metadata
        .get("signature_error")
        .is_some_and(|e| e.contains("MAC mismatch")));

    running.shutdown().await;

    // Nothing was left behind for a redelivery loop to pick up.
    assert!(
        broker.is_empty().await,
        "the forged message must not requeue"
    );
}

#[tokio::test]
async fn an_unsigned_message_is_rejected_when_a_signature_is_required() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = TaskRegistry::new();
    registry
        .register(RecordingTask {
            seen: Arc::clone(&seen),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let config = WorkerConfig {
        signature_verification: Some(SignatureVerification::new(TaskSigner::new(KEY))),
        ..fast_config("required-signature-worker")
    };
    let running = start(&broker, registry, config).await;

    broker
        .enqueue(SerializedTask::new(
            "recording_task".to_string(),
            br#"{"amount":1}"#.to_vec(),
        ))
        .await
        .expect("enqueue");

    let rejected = Arc::clone(&running.stats);
    wait_until("the unsigned message to be rejected", || {
        rejected.signature_rejected() == 1
    })
    .await;

    assert!(seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty());

    running.shutdown().await;
}

#[tokio::test]
async fn migration_mode_admits_unsigned_but_still_rejects_forged() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = TaskRegistry::new();
    registry
        .register(RecordingTask {
            seen: Arc::clone(&seen),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let config = WorkerConfig {
        signature_verification: Some(
            SignatureVerification::new(TaskSigner::new(KEY)).allow_unsigned(),
        ),
        ..fast_config("migration-worker")
    };
    let running = start(&broker, registry, config).await;

    let mut forged = signed_task(serde_json::json!({"amount": 1}));
    forged.payload = br#"{"amount":2}"#.to_vec();

    broker
        .enqueue(SerializedTask::new(
            "recording_task".to_string(),
            br#"{"amount":3}"#.to_vec(),
        ))
        .await
        .expect("enqueue");
    broker.enqueue(forged).await.expect("enqueue");

    let rejected = Arc::clone(&running.stats);
    wait_until("the forged message to be rejected", || {
        rejected.signature_rejected() == 1
    })
    .await;

    let executed = Arc::clone(&seen);
    wait_until("the unsigned message to run", || {
        executed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == 1
    })
    .await;
    assert_eq!(
        seen.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![serde_json::json!({"amount": 3})]
    );

    running.shutdown().await;
}

#[tokio::test]
async fn verification_is_off_by_default() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = TaskRegistry::new();
    registry
        .register(RecordingTask {
            seen: Arc::clone(&seen),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    // The claim the whole feature rests on: a *default* worker behaves exactly
    // as it did before either control existed. Asserted against
    // `WorkerConfig::default()` itself, not the test helper, so flipping either
    // default to `Some(..)` fails here.
    let defaults = WorkerConfig::default();
    assert!(defaults.signature_verification.is_none());
    assert!(defaults.payload_hygiene.is_none());

    let running = start(&broker, registry, fast_config("default-worker")).await;

    broker
        .enqueue(SerializedTask::new(
            "recording_task".to_string(),
            br#"{"amount":5}"#.to_vec(),
        ))
        .await
        .expect("enqueue");

    let executed = Arc::clone(&seen);
    wait_until("the unsigned message to run", || {
        executed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == 1
    })
    .await;
    assert_eq!(running.stats.signature_rejected(), 0);

    running.shutdown().await;
}

#[tokio::test]
async fn inspect_active_reports_a_redacted_payload() {
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let registry = TaskRegistry::new();
    registry
        .register(BlockingTask {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let transport = Arc::new(InMemoryControlTransport::new());
    let config = WorkerConfig {
        payload_hygiene: Some(PayloadHygiene::recommended()),
        ..fast_config("hygiene-worker")
    };

    let worker = Worker::new_from_arc(Arc::clone(&broker), registry, config)
        .with_control_transport(Arc::clone(&transport) as Arc<dyn ControlTransport>);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let subscribed = Arc::clone(&transport);
    wait_until("the worker to subscribe to the control channel", || {
        subscribed.subscriber_count() > 0
    })
    .await;

    let raw = br#"{"to":"alice@example.com","api_token":"sk-live-9"}"#.to_vec();
    let mut task = SerializedTask::new("blocking_task".to_string(), raw.clone());
    task.metadata.timeout_secs = Some(30);
    broker.enqueue(task).await.expect("enqueue");

    let running = Arc::clone(&started);
    wait_until("the blocking task to start", || {
        running.load(Ordering::SeqCst) == 1
    })
    .await;

    let replies = ControlClient::new(Arc::clone(&transport) as Arc<dyn ControlTransport>)
        .with_timeout(Duration::from_secs(3))
        .with_expected_replies(1)
        .broadcast(ControlCommand::Inspect(InspectCommand::Active))
        .await
        .expect("broadcast");
    assert_eq!(replies.len(), 1);

    match replies.into_iter().next().expect("one reply").response {
        ControlResponse::Inspect(payload) => match *payload {
            InspectResponse::Active(active) => {
                assert_eq!(active.len(), 1);
                let args = &active[0].args;
                assert!(
                    !args.contains("alice@example.com"),
                    "PII reached an operator view: {args}"
                );
                assert!(
                    !args.contains("sk-live-9"),
                    "a secret reached an operator view: {args}"
                );
                // Keys survive: the point is a usable, redacted view.
                assert!(args.contains("\"to\""), "unexpected rendering: {args}");
            }
            other => panic!("expected active tasks, got {other:?}"),
        },
        other => panic!("expected an inspect response, got {other:?}"),
    }

    release.notify_waiters();
    let _ = handle.shutdown().await;
}

/// A task that fails its first attempt and succeeds on the retry, so the retry
/// path is exercised end to end.
struct FlakyTask {
    attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for FlakyTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(celers_core::CelersError::TaskExecution(
                "first attempt always fails".to_string(),
            ));
        }
        Ok(input)
    }

    fn name(&self) -> &str {
        "flaky_task"
    }
}

#[tokio::test]
async fn the_worker_re_signs_its_own_retry_attempts() {
    // A retry is a message the worker constructs and enqueues. It inherits the
    // original MAC (neither `state` nor `updated_at` is signed), but it also
    // inherits the original nonce — so under a replay guard the worker would
    // reject its own retry as a replay unless it re-signs it with a fresh one.
    let attempts = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(FlakyTask {
            attempts: Arc::clone(&attempts),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let config = WorkerConfig {
        max_retries: 3,
        retry_base_delay_ms: 1,
        retry_max_delay_ms: 5,
        signature_verification: Some(
            SignatureVerification::new(TaskSigner::new(KEY)).with_replay_guard(Arc::new(
                ReplayGuard::new(FreshnessWindow::new(Duration::from_secs(300))),
            )),
        ),
        ..fast_config("retry-signing-worker")
    };
    let running = start(&broker, registry, config).await;

    let mut task = SerializedTask::new("flaky_task".to_string(), br#"{"n":1}"#.to_vec());
    sign_task(&TaskSigner::new(KEY), &mut task, SigningOptions::default());
    broker.enqueue(task).await.expect("enqueue");

    let ran = Arc::clone(&attempts);
    wait_until("the retry to run", || ran.load(Ordering::SeqCst) == 2).await;
    assert_eq!(
        running.stats.signature_rejected(),
        0,
        "the worker rejected its own retry"
    );

    running.shutdown().await;
}

/// A task whose completion carries an `on_success_link`, so the worker enqueues
/// a continuation of its own.
struct LinkedTask {
    name: &'static str,
    ran: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for LinkedTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        self.ran.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    }

    fn name(&self) -> &str {
        self.name
    }
}

#[tokio::test]
async fn a_workflow_continuation_is_signed_by_the_worker() {
    // The continuation is built by the worker, not by the producer: without
    // signing it, enabling verification would end every chain at its first hop.
    let first_ran = Arc::new(AtomicUsize::new(0));
    let second_ran = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(LinkedTask {
            name: "chain_head",
            ran: Arc::clone(&first_ran),
        })
        .await;
    registry
        .register(LinkedTask {
            name: "chain_tail",
            ran: Arc::clone(&second_ran),
        })
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let config = WorkerConfig {
        signature_verification: Some(SignatureVerification::new(TaskSigner::new(KEY))),
        ..fast_config("workflow-signing-worker")
    };
    let running = start(&broker, registry, config).await;

    let mut head = SerializedTask::new("chain_head".to_string(), br#"{"n":1}"#.to_vec());
    head.metadata.on_success_link = Some("chain_tail".to_string());
    sign_task(&TaskSigner::new(KEY), &mut head, SigningOptions::default());
    broker.enqueue(head).await.expect("enqueue");

    let head_ran = Arc::clone(&first_ran);
    wait_until("the chain head to run", || {
        head_ran.load(Ordering::SeqCst) == 1
    })
    .await;

    let tail_ran = Arc::clone(&second_ran);
    wait_until("the chain tail to run", || {
        tail_ran.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        running.stats.signature_rejected(),
        0,
        "the worker rejected its own continuation"
    );

    running.shutdown().await;
}
