// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! End-to-end proof that a real [`celers_worker::Worker`] can run entirely
//! over `celers-kombu`'s [`KombuBrokerAdapter`] bridge — the seam that makes
//! `celers_broker_amqp::AmqpBroker` and `celers_broker_sqs::SqsBroker`
//! worker-usable at all (see `celers_kombu::core_adapter` and
//! `celers::broker_helper::create_broker`, which now builds exactly this
//! kind of broker for `"amqp"`/`"sqs"` requests).
//!
//! [`MockBroker`] is the hermetic transport underneath: nothing here talks to
//! a network socket, a filesystem, or any live broker. That is deliberate —
//! this test is the facade-level guarantee that the bridge really is a
//! [`celers_core::Broker`] a worker can be built on and can drive a task to
//! completion, not just a set of types that happen to compile against each
//! other. It is what makes an end-to-end worker test possible without a
//! broker on the machine (see the doc comment on `celers_kombu::MockBroker`'s
//! `CoreBrokerTransport` impl).
//!
//! # Why this compiles under default features
//!
//! `celers_kombu::core_adapter` lives behind celers-kombu's `core-adapter`
//! feature, off by default. This crate's `Cargo.toml` requests it explicitly
//! in `[dev-dependencies]` (see the comment there) so this test runs in the
//! default suite instead of only under `--features amqp`/`sqs`.

use celers_core::{Broker, Result as CoreResult, SerializedTask, Task, TaskId, TaskRegistry};
use celers_kombu::core_adapter::KombuBrokerAdapter;
use celers_kombu::MockBroker;
use celers_worker::{Worker, WorkerConfig};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Deadline for the "wait until" helper: generous for a loaded CI box, short
/// enough that a genuine hang fails this test rather than the suite.
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

const TASK_NAME: &str = "kombu_bridge_e2e.add";

#[derive(Serialize, Deserialize)]
struct Numbers {
    a: i64,
    b: i64,
}

#[derive(Serialize, Deserialize)]
struct Sum {
    value: i64,
}

/// Records its own execution count and produces a real, checkable output —
/// proving the worker actually decoded the payload the mock transport
/// carried, not just that *something* dequeued.
struct AddTask {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for AddTask {
    type Input = Numbers;
    type Output = Sum;

    async fn execute(&self, input: Self::Input) -> CoreResult<Self::Output> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(Sum {
            value: input.a + input.b,
        })
    }

    fn name(&self) -> &'static str {
        TASK_NAME
    }
}

/// Poll `predicate` until it holds or [`WAIT_DEADLINE`] expires.
async fn wait_until<F: FnMut() -> bool>(what: &str, mut predicate: F) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn worker_executes_a_task_end_to_end_over_the_kombu_mock_transport() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AddTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // The adapter over the mock transport: exactly the shape
    // `AmqpBroker::into_core_broker`/`SqsBroker::into_core_broker` produce,
    // minus the real broker underneath.
    let broker = Arc::new(KombuBrokerAdapter::new(
        MockBroker::new(),
        "kombu-bridge-e2e-queue",
    ));

    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 10,
        ..Default::default()
    };

    let worker = Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let payload = serde_json::to_vec(&Numbers { a: 2, b: 40 }).expect("serialize payload");
    let task_id = broker
        .enqueue(SerializedTask::new(TASK_NAME.to_string(), payload))
        .await
        .expect("enqueue");

    wait_until("the task to run", || runs.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        stats.processed(),
        1,
        "the worker's own bookkeeping must count the run, not just the task body"
    );

    // A real, non-nil id round-tripped through the adapter, not an opaque
    // placeholder standing in for "some task ran".
    assert_ne!(task_id, TaskId::nil());

    let _ = handle.shutdown().await;

    // The worker is gone; the count from before shutdown must hold steady.
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
