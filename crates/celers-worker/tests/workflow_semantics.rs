//! End-to-end workflow semantics that need worker features the `celers` facade
//! cannot turn on for itself.
//!
//! The facade's own `workflow_semantics` integration test covers error links,
//! saga rollback, failure suppression and retry backoff, all of which work
//! without any optional worker feature. Two things do not, and live here:
//!
//! * **Branch/switch evaluation** needs `canvas`: without it the worker cannot
//!   decode a condition and deliberately ends the chain rather than guessing an
//!   arm. This is what makes `create_conditional_workflow` a real conditional
//!   — the success arm must not run when the condition is false.
//! * **Chord barriers** need `workflows` (the result-backend handle): the
//!   callback fires once, after every member has finished, with their results.
//!   The interesting case is a chord whose members are whole *chains*, which is
//!   what `create_parallel_chains(.., Some(aggregate))` builds: the barrier has
//!   to count each chain once, when its **last** step completes.
//!
//! Everything runs against the in-process [`InMemoryBroker`] and a real worker.

#![cfg(feature = "canvas")]

use celers_canvas::dispatch::{self, ChainStep};
use celers_canvas::{Branch, CanvasElement, Chain, Condition, NestedChain, Signature, Switch};
use celers_core::{InMemoryBroker, Result as CoreResult, Task, TaskRegistry};
use celers_worker::{Worker, WorkerConfig, WorkerHandle};

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Deadline for every "wait until" helper.
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

/// How long to let the runtime keep going before asserting something did not
/// happen.
const SETTLE: Duration = Duration::from_millis(300);

/// Shared, ordered log of which task ran and what it was handed.
#[derive(Clone, Default)]
struct RunLog {
    entries: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
}

impl RunLog {
    fn names(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn ran(&self, name: &str) -> bool {
        self.names().iter().any(|seen| seen == name)
    }

    fn input_of(&self, name: &str) -> Option<serde_json::Value> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|(seen, _)| seen == name)
            .map(|(_, input)| input.clone())
    }
}

/// A task that records its invocation and returns a fixed value.
struct Recorded {
    name: &'static str,
    log: RunLog,
    returns: serde_json::Value,
}

impl Recorded {
    fn new(name: &'static str, log: &RunLog, returns: serde_json::Value) -> Self {
        Self {
            name,
            log: log.clone(),
            returns,
        }
    }
}

#[async_trait::async_trait]
impl Task for Recorded {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> CoreResult<Self::Output> {
        self.log
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((self.name.to_string(), input));
        Ok(self.returns.clone())
    }

    fn name(&self) -> &str {
        self.name
    }
}

fn config(hostname: &str) -> WorkerConfig {
    WorkerConfig {
        concurrency: 4,
        poll_interval_ms: 5,
        shutdown_timeout_secs: 5,
        max_retries: 0,
        hostname: hostname.to_string(),
        ..Default::default()
    }
}

async fn wait_until<F>(what: &str, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + WAIT_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn settle() {
    tokio::time::sleep(SETTLE).await;
}

/// The conditional workflow `create_conditional_workflow` builds, with the
/// condition task returning `verdict`.
async fn run_conditional(verdict: serde_json::Value, hostname: &str) -> (RunLog, WorkerHandle) {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry
        .register(Recorded::new("check_balance", &log, verdict))
        .await;
    registry
        .register(Recorded::new(
            "process_payment",
            &log,
            serde_json::json!("charged"),
        ))
        .await;
    registry
        .register(Recorded::new(
            "send_insufficient_funds_notice",
            &log,
            serde_json::json!("notified"),
        ))
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let handle = Worker::new_from_arc(Arc::clone(&broker), registry, config(hostname))
        .run_with_shutdown()
        .await
        .expect("worker starts");

    let workflow = NestedChain::new()
        .then_signature(Signature::new("check_balance".to_string()))
        .then_branch(
            Branch::new(
                Condition::truthy(),
                Signature::new("process_payment".to_string()),
            )
            .otherwise(Signature::new("send_insufficient_funds_notice".to_string())),
        );
    workflow.apply(broker.as_ref()).await.expect("dispatch");

    (log, handle)
}

#[tokio::test]
async fn a_true_condition_takes_the_success_arm_only() {
    let (log, handle) = run_conditional(serde_json::json!(true), "branch-true").await;

    wait_until("the success arm to run", || log.ran("process_payment")).await;
    settle().await;

    assert_eq!(log.names(), vec!["check_balance", "process_payment"]);
    // The arm receives the condition's own result.
    let input = log.input_of("process_payment").expect("success arm input");
    assert_eq!(input["args"][0], serde_json::json!(true));

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn a_false_condition_takes_the_failure_arm_only() {
    let (log, handle) = run_conditional(serde_json::json!(false), "branch-false").await;

    wait_until("the failure arm to run", || {
        log.ran("send_insufficient_funds_notice")
    })
    .await;
    settle().await;

    assert_eq!(
        log.names(),
        vec!["check_balance", "send_insufficient_funds_notice"],
        "the card must not be charged when the balance check said no — this is \
         the defect a chain that merely appended the success task produced"
    );

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn a_switch_picks_exactly_one_case() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry
        .register(Recorded::new(
            "classify",
            &log,
            serde_json::json!({"status": "retry"}),
        ))
        .await;
    for arm in ["on_ok", "on_retry", "on_default"] {
        registry
            .register(Recorded::new(arm, &log, serde_json::json!(null)))
            .await;
    }

    let broker = Arc::new(InMemoryBroker::new());
    let handle = Worker::new_from_arc(Arc::clone(&broker), registry, config("switch"))
        .run_with_shutdown()
        .await
        .expect("worker starts");

    let workflow = NestedChain::new()
        .then_signature(Signature::new("classify".to_string()))
        .then_element(CanvasElement::Switch(
            Switch::new()
                .case(
                    Condition::field_equals("status", serde_json::json!("ok")),
                    Signature::new("on_ok".to_string()),
                )
                .case(
                    Condition::field_equals("status", serde_json::json!("retry")),
                    Signature::new("on_retry".to_string()),
                )
                .default(Signature::new("on_default".to_string())),
        ));
    workflow.apply(broker.as_ref()).await.expect("dispatch");

    wait_until("the matching case to run", || log.ran("on_retry")).await;
    settle().await;

    assert_eq!(log.names(), vec!["classify", "on_retry"]);

    let _ = handle.shutdown().await;
}

/// A chord whose members are whole chains — the shape
/// `create_parallel_chains(.., Some(aggregate))` produces.
#[cfg(feature = "workflows")]
mod chord_over_chains {
    use super::*;

    use celers_backend_redis::{ChordState, Result as BackendResult, ResultBackend, TaskMeta};
    use std::collections::HashMap;
    use uuid::Uuid;

    /// An in-process barrier store: enough of [`ResultBackend`] for a chord.
    #[derive(Default)]
    struct MemoryBarrier {
        state: Option<ChordState>,
        results: HashMap<Uuid, TaskMeta>,
        completed: usize,
    }

    #[async_trait::async_trait]
    impl ResultBackend for MemoryBarrier {
        async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> BackendResult<()> {
            self.results.insert(task_id, meta.clone());
            Ok(())
        }

        async fn get_result(&mut self, task_id: Uuid) -> BackendResult<Option<TaskMeta>> {
            Ok(self.results.get(&task_id).cloned())
        }

        async fn delete_result(&mut self, task_id: Uuid) -> BackendResult<()> {
            self.results.remove(&task_id);
            Ok(())
        }

        async fn set_expiration(
            &mut self,
            _task_id: Uuid,
            _ttl: std::time::Duration,
        ) -> BackendResult<()> {
            Ok(())
        }

        async fn chord_init(&mut self, state: ChordState) -> BackendResult<()> {
            self.state = Some(state);
            self.completed = 0;
            Ok(())
        }

        async fn chord_complete_task(&mut self, _chord_id: Uuid) -> BackendResult<usize> {
            self.completed += 1;
            Ok(self.completed)
        }

        async fn chord_get_state(&mut self, _chord_id: Uuid) -> BackendResult<Option<ChordState>> {
            Ok(self.state.clone())
        }
    }

    /// The barrier must count **chains**, not tasks: a two-step member counts
    /// once, when its second step finishes, and the aggregate then receives one
    /// result per chain — the value that chain's final step returned.
    #[tokio::test]
    async fn the_aggregate_runs_once_after_every_chain_finishes() {
        let log = RunLog::default();
        let registry = TaskRegistry::new();
        registry
            .register(Recorded::new("resize", &log, serde_json::json!("resized")))
            .await;
        registry
            .register(Recorded::new(
                "optimize",
                &log,
                serde_json::json!("images-done"),
            ))
            .await;
        registry
            .register(Recorded::new(
                "transcode",
                &log,
                serde_json::json!("transcoded"),
            ))
            .await;
        registry
            .register(Recorded::new(
                "thumbnail",
                &log,
                serde_json::json!("videos-done"),
            ))
            .await;
        registry
            .register(Recorded::new("finalize", &log, serde_json::json!("final")))
            .await;

        let barrier = Arc::new(tokio::sync::Mutex::new(MemoryBarrier::default()));
        let broker = Arc::new(InMemoryBroker::new());
        let handle = Worker::new_from_arc(Arc::clone(&broker), registry, config("chord-chains"))
            .with_chord_backend(Arc::clone(&barrier) as Arc<tokio::sync::Mutex<dyn ResultBackend>>)
            .run_with_shutdown()
            .await
            .expect("worker starts");

        // Two chains of two tasks each; only the last step of each carries the
        // pre-assigned id and the chord id.
        let chord_id = Uuid::new_v4();
        let chains = [("resize", "optimize"), ("transcode", "thumbnail")];

        let mut member_ids = Vec::new();
        let mut dispatched = Vec::new();
        for (head_name, tail_name) in chains {
            let member_id = Uuid::new_v4();
            member_ids.push(member_id);

            let head = Signature::new(head_name.to_string());
            let tail = Signature::new(tail_name.to_string())
                .with_task_id(member_id)
                .with_chord_id(chord_id);
            dispatched.push((head, vec![ChainStep::Task(tail)]));
        }

        {
            let mut guard = barrier.lock().await;
            guard
                .chord_init(
                    ChordState::new(chord_id, member_ids.len(), member_ids.clone())
                        .with_callback("finalize".to_string()),
                )
                .await
                .expect("register the barrier before anything can complete");
        }

        for (head, tail) in &dispatched {
            dispatch::dispatch_signature(broker.as_ref(), head, tail)
                .await
                .expect("dispatch chain head");
        }

        wait_until("the aggregate to run", || log.ran("finalize")).await;
        settle().await;

        let names = log.names();
        assert_eq!(
            names.iter().filter(|name| *name == "finalize").count(),
            1,
            "the aggregate must run exactly once, got {names:?}"
        );
        assert_eq!(
            names.len(),
            5,
            "every step of every chain plus the aggregate, got {names:?}"
        );

        // The aggregate receives one result per chain, in the order the barrier
        // recorded its members — the chains' *final* results, not their heads'.
        let input = log.input_of("finalize").expect("aggregate input");
        let results = input["args"][0]
            .as_array()
            .expect("the aggregate's first argument is the list of results");
        assert_eq!(
            results,
            &vec![
                serde_json::json!("images-done"),
                serde_json::json!("videos-done"),
            ]
        );

        let _ = handle.shutdown().await;
    }

    /// Without the barrier handle the header still runs, but the callback
    /// cannot fire — the failure mode this wiring exists to remove. Pinning it
    /// keeps "the worker was given a backend" from silently regressing to
    /// "chords quietly do nothing".
    #[tokio::test]
    async fn without_a_barrier_handle_the_aggregate_never_runs() {
        let log = RunLog::default();
        let registry = TaskRegistry::new();
        registry
            .register(Recorded::new("resize", &log, serde_json::json!("resized")))
            .await;
        registry
            .register(Recorded::new("finalize", &log, serde_json::json!("final")))
            .await;

        let broker = Arc::new(InMemoryBroker::new());
        let handle = Worker::new_from_arc(Arc::clone(&broker), registry, config("chord-nobarrier"))
            .run_with_shutdown()
            .await
            .expect("worker starts");

        let chord_id = Uuid::new_v4();
        let member = Signature::new("resize".to_string()).with_chord_id(chord_id);
        dispatch::dispatch_signature(broker.as_ref(), &member, &[])
            .await
            .expect("dispatch");

        wait_until("the member to run", || log.ran("resize")).await;
        settle().await;

        assert_eq!(log.names(), vec!["resize"]);

        let _ = handle.shutdown().await;
    }
}

/// A plain chain still works with `canvas` on: the branch machinery must not
/// have displaced the ordinary path.
#[tokio::test]
async fn a_plain_chain_still_runs_every_step_in_order() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    for name in ["one", "two", "three"] {
        registry
            .register(Recorded::new(name, &log, serde_json::json!(name)))
            .await;
    }

    let broker = Arc::new(InMemoryBroker::new());
    let handle = Worker::new_from_arc(Arc::clone(&broker), registry, config("plain-chain"))
        .run_with_shutdown()
        .await
        .expect("worker starts");

    Chain::new()
        .then("one", vec![])
        .then("two", vec![])
        .then("three", vec![])
        .apply(broker.as_ref())
        .await
        .expect("dispatch");

    wait_until("the chain to finish", || log.ran("three")).await;
    assert_eq!(log.names(), vec!["one", "two", "three"]);

    let _ = handle.shutdown().await;
}
