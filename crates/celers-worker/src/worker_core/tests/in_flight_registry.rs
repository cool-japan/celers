//! Unit tests for [`InFlightRegistry`] (the shutdown/disposal claim ledger)
//! and the [`WorkerStats`] active-count saturation guard.

use crate::types::WorkerStats;
use crate::worker_core::support::InFlightRegistry;

use celers_core::task_security::PayloadHygiene;
use celers_core::SerializedTask;

#[test]
fn test_in_flight_registry_claims_exactly_once() {
    // The registry entry is the disposition token: the shutdown requeue and the
    // task's own ack must not both act on the same message.
    let registry = InFlightRegistry::new();
    let task = SerializedTask::new("dispose_once".to_string(), vec![1, 2, 3]);
    let task_id = task.metadata.id;
    registry.register(task_id, Some("receipt".to_string()), &task);

    assert_eq!(registry.len(), 1);
    assert!(registry.claim(&task_id));
    assert!(!registry.claim(&task_id));
    assert!(registry.is_empty());
    assert!(registry.take_all().is_empty());
}

#[test]
fn test_in_flight_registry_snapshot_reports_task_identity() {
    // `inspect active` reads this snapshot: the counters know how many tasks
    // are running, only the registry knows which.
    // No payload hygiene configured: the preview is the payload verbatim, which
    // is the baseline the opt-in redaction has to be measured against.
    let registry = InFlightRegistry::with_args_capture(None);
    let task = SerializedTask::new("send_email".to_string(), br#"{"to":"a@b.c"}"#.to_vec());
    let task_id = task.metadata.id;
    registry.register(task_id, None, &task);

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 1);
    let (seen_id, entry) = &snapshot[0];
    assert_eq!(*seen_id, task_id);
    assert_eq!(entry.name, "send_email");
    assert_eq!(entry.args_preview.as_deref(), Some(r#"{"to":"a@b.c"}"#));
    assert!(entry.started > 0.0);
}

#[test]
fn test_in_flight_registry_redacts_the_preview_when_hygiene_is_configured() {
    // Same payload as the baseline above; the only difference is the opt-in.
    let registry = InFlightRegistry::with_args_capture(Some(PayloadHygiene::recommended()));
    let raw = br#"{"to":"alice@example.com","api_token":"sk-live-9"}"#.to_vec();
    let task = SerializedTask::new("send_email".to_string(), raw.clone());
    let task_id = task.metadata.id;
    registry.register(task_id, None, &task);

    let snapshot = registry.snapshot();
    let preview = snapshot[0]
        .1
        .args_preview
        .clone()
        .expect("args capture is on");

    assert!(
        !preview.contains("alice@example.com"),
        "PII survived: {preview}"
    );
    assert!(!preview.contains("sk-live-9"), "secret survived: {preview}");
    assert!(preview.contains("\"to\""), "keys are kept: {preview}");

    // The executing payload is untouched — this is the whole point.
    assert_eq!(task.payload, raw);
}

#[test]
fn test_in_flight_registry_skips_args_capture_by_default() {
    // A worker without remote control must not retain payload copies.
    let registry = InFlightRegistry::new();
    let task = SerializedTask::new("bulky".to_string(), vec![7; 4096]);
    registry.register(task.metadata.id, None, &task);

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot[0].1.args_preview.is_none());
}

#[test]
fn test_worker_stats_active_count_saturates_at_zero() {
    // An unbalanced decrement used to wrap to u64::MAX, hanging every drain.
    let stats = WorkerStats::new();
    stats.task_completed();
    assert_eq!(stats.active(), 0);
    assert_eq!(stats.processed(), 1);

    stats.task_started();
    stats.task_completed();
    stats.task_completed();
    assert_eq!(stats.active(), 0);
}
