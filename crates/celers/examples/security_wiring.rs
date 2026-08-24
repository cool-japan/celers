//! Wiring CeleRS's security controls into a running pipeline.
//!
//! CeleRS ships four security/hygiene modules — message signing, argument
//! sanitizing, PII masking and result tombstones — and **none of them is on by
//! default**. This example is the wiring: producer, worker and result handle,
//! all in one process against the in-memory broker, so it runs with no external
//! service:
//!
//! ```text
//! cargo run -p celers --example security_wiring
//! ```
//!
//! What it demonstrates, in order:
//!
//! 1. **Signing** — the producer stamps every message with an HMAC
//!    (`sign_task`); the worker verifies it before dispatch
//!    (`SignatureVerification`). A message whose payload was rewritten in
//!    flight never reaches a task handler.
//! 2. **Hygiene** — `PayloadHygiene` redacts secret-looking keys and masks PII
//!    in the *copies* the worker shows operators (`inspect active`, debug
//!    logs). The payload the task executes is untouched, which the example
//!    asserts.
//! 3. **Tombstones** — `TombstoneRegistry` makes a forgotten result
//!    distinguishable from one that never existed.
//!
//! The signing key is read from `CELERS_TASK_SIGNING_KEY` when set, so the
//! example also shows where a real deployment's key comes from; it falls back
//! to a fixed demo key so the example runs unattended.

use celers::prelude::*;
use celers::{AsyncResult, Broker, InMemoryBroker, SerializedTask, TaskRegistry, TaskResultValue};

use celers_core::{Task, TaskId};

use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// Demo key. A real deployment supplies at least 32 bytes of entropy through a
/// secret manager and shares it with every producer.
const FALLBACK_KEY: &str = "celers-demo-signing-key-32-bytes!";

/// The task both halves of the example exchange.
struct ChargeCard {
    executed: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for ChargeCard {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        self.executed.fetch_add(1, Ordering::SeqCst);
        // The handler sees the ORIGINAL arguments: hygiene only ever touched
        // the copies the worker showed to operators.
        println!("  handler ran with: {input}");
        Ok(serde_json::json!({"charged": true}))
    }

    fn name(&self) -> &str {
        "billing.charge_card"
    }
}

/// A minimal, cloneable result store, so the example stays self-contained.
///
/// It deliberately implements none of the optional tombstone hooks — which is
/// the situation the [`TombstoneRegistry`] exists for: a backend that cannot
/// tell "forgotten" from "never existed" on its own.
#[derive(Clone, Default)]
struct DemoStore {
    results: Arc<Mutex<HashMap<TaskId, TaskResultValue>>>,
}

impl DemoStore {
    fn results(&self) -> std::sync::MutexGuard<'_, HashMap<TaskId, TaskResultValue>> {
        self.results.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl ResultStore for DemoStore {
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        self.results().insert(task_id, result);
        Ok(())
    }

    async fn get_result(&self, task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        Ok(self.results().get(&task_id).cloned())
    }

    async fn get_state(&self, task_id: TaskId) -> celers_core::Result<celers::TaskState> {
        Ok(match self.results().get(&task_id) {
            Some(TaskResultValue::Success(_)) => celers::TaskState::Succeeded(Vec::new()),
            Some(_) => celers::TaskState::Failed("failed".to_string()),
            None => celers::TaskState::Pending,
        })
    }

    async fn forget(&self, task_id: TaskId) -> celers_core::Result<()> {
        self.results().remove(&task_id);
        Ok(())
    }

    async fn has_result(&self, task_id: TaskId) -> celers_core::Result<bool> {
        Ok(self.results().contains_key(&task_id))
    }
}

/// Poll `predicate` until it holds, or give up after ~5 seconds.
async fn wait_until(what: &str, mut predicate: impl FnMut() -> bool) -> Result<(), String> {
    for _ in 0..500 {
        if predicate() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("timed out waiting for {what}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let key = std::env::var("CELERS_TASK_SIGNING_KEY").unwrap_or_else(|_| FALLBACK_KEY.to_string());
    let signer = TaskSigner::new(&key);

    // ------------------------------------------------------------------
    // 1. The worker. Both controls are opt-in, so both are named explicitly:
    //    leave either builder call out and the worker behaves exactly as a
    //    default CeleRS worker does — verifying nothing, redacting nothing.
    // ------------------------------------------------------------------
    let executed = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(ChargeCard {
            executed: Arc::clone(&executed),
        })
        .await;

    let config = WorkerConfig::builder()
        .concurrency(2)
        .poll_interval_ms(10)
        // Reject anything not signed with our key, *before* dispatch.
        .signature_verification(SignatureVerification::new(signer.clone()))
        // Redact the payload copies the worker logs and reports.
        .payload_hygiene(
            PayloadHygiene::new()
                .with_sanitizer(Sanitizer::new(SanitizerConfig::default()))
                .with_pii_detector(PiiDetector::new()),
        )
        .build()?;

    let broker = Arc::new(InMemoryBroker::new());
    let worker = Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await?;

    // ------------------------------------------------------------------
    // 2. The producer: sign immediately before enqueueing, because the MAC
    //    covers the payload bytes as they are at that moment.
    // ------------------------------------------------------------------
    let arguments = serde_json::json!({
        "customer_email": "alice@example.com",
        "api_token": "sk-live-9f2c",
        "amount_cents": 4200
    });
    let payload = serde_json::to_vec(&arguments)?;

    let mut authentic = SerializedTask::new("billing.charge_card".to_string(), payload.clone());
    // `SigningOptions::default()` also stamps `signed_at` and a nonce, which is
    // what lets a consumer add freshness and replay checks later.
    sign_task(&signer, &mut authentic, SigningOptions::default());
    broker.enqueue(authentic).await?;

    // A message an attacker rewrote after it was signed.
    let mut forged = SerializedTask::new("billing.charge_card".to_string(), payload.clone());
    sign_task(&signer, &mut forged, SigningOptions::default());
    forged.payload = serde_json::to_vec(&serde_json::json!({
        "customer_email": "alice@example.com",
        "api_token": "sk-live-9f2c",
        "amount_cents": 9_999_999
    }))?;
    broker.enqueue(forged).await?;

    println!("1. signature verification");
    wait_until("the authentic message to run", || {
        executed.load(Ordering::SeqCst) == 1
    })
    .await?;
    wait_until("the forged message to be rejected", || {
        stats.signature_rejected() == 1
    })
    .await?;
    println!(
        "  executed: {}, rejected by signature: {}",
        executed.load(Ordering::SeqCst),
        stats.signature_rejected()
    );

    // ------------------------------------------------------------------
    // 3. Hygiene: what an operator sees vs. what the task received.
    // ------------------------------------------------------------------
    println!("2. payload hygiene");
    let hygiene = PayloadHygiene::recommended();
    let redacted = hygiene.redact_payload(&payload);
    println!("  operator view: {}", redacted.text);
    println!(
        "  redacted {} secret key(s), masked {} PII match(es)",
        redacted
            .report
            .sanitize
            .as_ref()
            .map_or(0, |r| r.redacted_keys),
        redacted.report.pii_masked()
    );
    assert!(!redacted.text.contains("alice@example.com"));
    assert!(!redacted.text.contains("sk-live-9f2c"));
    // The bytes the worker dispatched are unchanged: hygiene worked on a copy.
    assert_eq!(payload, serde_json::to_vec(&arguments)?);

    handle.shutdown().await?;

    // ------------------------------------------------------------------
    // 4. Tombstones: forgotten vs. never existed.
    // ------------------------------------------------------------------
    println!("3. result tombstones");
    let store = DemoStore::default();
    let tombstones = Arc::new(TombstoneRegistry::new());

    let forgotten_id = TaskId::new_v4();
    let never_existed_id = TaskId::new_v4();
    store
        .store_result(
            forgotten_id,
            TaskResultValue::Success(serde_json::json!({"charged": true})),
        )
        .await?;

    let forgotten = AsyncResult::new(forgotten_id, store.clone())
        .with_tombstone_registry(Arc::clone(&tombstones));
    let never = AsyncResult::new(never_existed_id, store.clone())
        .with_tombstone_registry(Arc::clone(&tombstones));

    forgotten
        .forget_with_reason("GDPR erasure request #4711")
        .await?;

    println!("  forgotten result: {:?}", forgotten.existence().await?);
    println!("  never existed:    {:?}", never.existence().await?);
    assert!(forgotten.existence().await?.was_deleted());
    assert!(never.existence().await?.is_absent());

    // A waiter is told *why* the result is gone instead of polling forever.
    match forgotten.get(Some(Duration::from_secs(1))).await {
        Err(e) => println!("  a waiter is told: {e}"),
        Ok(_) => return Err("a forgotten result must not resolve".into()),
    }

    println!("\nAll three controls are opt-in: drop the builder calls and the");
    println!("registry above and CeleRS verifies, redacts and records nothing.");
    Ok(())
}
