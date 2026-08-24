//! Regression tests for `celers_core::task` metadata semantics.
//!
//! These live in an integration test so `task.rs` stays comfortably under the
//! 2000-line file cap; everything exercised here is public API.

use celers_core::task::ValidationLimits;
use celers_core::{SerializedTask, TaskMetadata};
use chrono::Utc;

/// Regression: `is_expired()` used to measure `timeout_secs` from `created_at`
/// while `celers-worker` measures the same field from the start of *execution*,
/// so a task that merely waited in a queue was reported as an expired message.
///
/// The two are now distinct fields: `expires_at` (Celery's `expires`, an
/// absolute message deadline) drives `is_expired()`, and `timeout_secs`
/// (Celery's `time_limit`) is read only by `execution_time_elapsed()`.
#[test]
fn message_expiry_and_execution_limit_are_separate() {
    let mut metadata = TaskMetadata::new("batch.job".to_string());
    metadata.timeout_secs = Some(1);
    assert!(!metadata.is_expired());
    assert!(!metadata.execution_time_elapsed());

    // An hour in the queue: the execution budget has notionally elapsed ...
    metadata.created_at = Utc::now() - chrono::Duration::hours(1);
    assert!(metadata.execution_time_elapsed());
    // ... but the *message* is not expired, because no deadline was set.
    assert!(
        !metadata.is_expired(),
        "an execution time limit must never be read as a message expiry"
    );
    assert_eq!(metadata.expires_at, None);

    // A message deadline in the past does expire it, with no timeout involved.
    let expired = TaskMetadata::new("t".to_string())
        .with_expires_at(Utc::now() - chrono::Duration::seconds(1));
    assert!(expired.is_expired());
    assert!(!expired.execution_time_elapsed());
    assert!(expired
        .time_until_expiry()
        .is_some_and(|left| left.num_milliseconds() <= 0));

    // A deadline in the future does not.
    let live = TaskMetadata::new("t".to_string()).with_expires_in(chrono::Duration::hours(1));
    assert!(!live.is_expired());
    assert!(live
        .time_until_expiry()
        .is_some_and(|left| left.num_seconds() > 0));

    // Nothing configured: never expires, however old.
    let mut forever = TaskMetadata::new("t".to_string());
    forever.created_at = Utc::now() - chrono::Duration::days(365);
    assert!(!forever.is_expired());
    assert!(!forever.execution_time_elapsed());
    assert_eq!(forever.time_until_expiry(), None);
}

/// `expires_at` is optional on the wire, so a message serialized before the
/// field existed still decodes (and decodes as "never expires").
#[test]
fn expires_at_is_optional_on_the_wire() {
    let legacy = serde_json::json!({
        "id": uuid::Uuid::nil(),
        "name": "legacy.task",
        "state": "Pending",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "max_retries": 3,
        "timeout_secs": 60,
        "priority": 0
    });

    let metadata: TaskMetadata =
        serde_json::from_value(legacy).expect("pre-expires_at metadata must still decode");
    assert_eq!(metadata.expires_at, None);
    assert!(!metadata.is_expired());
    assert!(metadata.execution_time_elapsed());

    // And a set deadline round-trips.
    let deadline = Utc::now() + chrono::Duration::hours(2);
    let encoded =
        serde_json::to_value(metadata.with_expires_at(deadline)).expect("metadata must serialize");
    let decoded: TaskMetadata = serde_json::from_value(encoded).expect("metadata must round-trip");
    assert_eq!(
        decoded.expires_at.map(|at| at.timestamp_millis()),
        Some(deadline.timestamp_millis())
    );
}

/// A retry clone keeps the producer's absolute deadline: it must not be able to
/// outlive the expiry the caller set.
#[test]
fn retry_clone_keeps_the_absolute_expiry_deadline() {
    let deadline = Utc::now() + chrono::Duration::minutes(5);
    let original = TaskMetadata::new("t".to_string()).with_expires_at(deadline);

    let retry = original.with_new_id();
    assert_ne!(retry.id, original.id);
    assert_eq!(retry.expires_at, original.expires_at);
    assert!(retry.created_at >= original.created_at);
}

/// An absurd TTL saturates instead of wrapping into the past.
#[test]
fn huge_expiry_windows_saturate_instead_of_wrapping() {
    let metadata = TaskMetadata::new("t".to_string()).with_expires_in(chrono::Duration::MAX);
    assert!(!metadata.is_expired());
}

/// The full lifecycle of a message deadline, end to end on `SerializedTask`.
#[test]
fn expiration_lifecycle_reads_only_the_message_deadline() {
    // Fresh task with a 1s message expiry is not yet expired.
    let mut task = SerializedTask::new("expiring_task".to_string(), vec![1, 2, 3])
        .with_expires_in(chrono::Duration::seconds(1));
    assert!(!task.is_expired());

    // Back-date the deadline instead of sleeping: deterministic and instant.
    task.metadata.expires_at = Some(Utc::now() - chrono::Duration::seconds(5));
    assert!(task.is_expired());

    // A task with no expiry deadline never expires, however old.
    let mut forever = SerializedTask::new("forever".to_string(), vec![1]);
    forever.metadata.created_at = Utc::now() - chrono::Duration::days(365);
    assert!(!forever.is_expired());

    // An elapsed execution budget is not a message expiry.
    let mut queued = SerializedTask::new("slow_batch".to_string(), vec![1]).with_timeout(1);
    queued.metadata.created_at = Utc::now() - chrono::Duration::hours(1);
    assert!(queued.execution_time_elapsed());
    assert!(!queued.is_expired());
    assert_eq!(queued.metadata.expires_at, None);
}

/// A `timeout_secs` beyond `i64::MAX` seconds must not wrap or panic.
#[test]
fn huge_timeouts_do_not_overflow() {
    let mut metadata = TaskMetadata::new("t".to_string());
    metadata.timeout_secs = Some(u64::MAX);
    metadata.created_at = Utc::now() - chrono::Duration::days(365);
    assert!(!metadata.execution_time_elapsed());
}

#[test]
fn batch_expiry_helpers_agree_with_the_metadata_reading() {
    let stale = SerializedTask::new("t".to_string(), vec![1])
        .with_expires_at(Utc::now() - chrono::Duration::hours(1));
    let fresh = SerializedTask::new("t".to_string(), vec![1])
        .with_expires_in(chrono::Duration::seconds(3600));

    assert!(stale.is_expired());
    assert!(!fresh.is_expired());
    assert!(celers_core::task::batch::has_expired_tasks(
        std::slice::from_ref(&stale)
    ));
    assert_eq!(
        celers_core::task::batch::get_expired_tasks(&[stale, fresh]).len(),
        1
    );
}

/// The batch helpers must not report a merely long-queued task as expired.
#[test]
fn batch_expiry_helpers_ignore_the_execution_time_limit() {
    let mut queued = SerializedTask::new("t".to_string(), vec![1]).with_timeout(1);
    queued.metadata.created_at = Utc::now() - chrono::Duration::hours(1);

    assert!(queued.execution_time_elapsed());
    assert!(!celers_core::task::batch::has_expired_tasks(
        std::slice::from_ref(&queued)
    ));
    assert!(celers_core::task::batch::get_expired_tasks(std::slice::from_ref(&queued)).is_empty());
}

/// Regression: `validate()` imposed undocumented, unconfigurable hard caps, so a
/// legitimate multi-day batch job failed validation with no way to override it.
#[test]
fn validation_limits_are_configurable() {
    let mut metadata = TaskMetadata::new("batch.job".to_string());
    metadata.timeout_secs = Some(48 * 60 * 60); // two days
    metadata.max_retries = 5_000;

    // The defaults reproduce the historical caps.
    let err = metadata
        .validate()
        .expect_err("default limits still reject this task");
    assert!(err.contains("Max retries"), "unexpected: {err}");

    let limits = ValidationLimits::default()
        .with_max_retries(10_000)
        .with_max_timeout_secs(7 * 24 * 60 * 60);
    metadata
        .validate_with_limits(&limits)
        .expect("raised limits must accept a long-running batch job");

    // The individual bounds are still enforced against the raised limits.
    metadata.max_retries = 10_001;
    assert!(metadata.validate_with_limits(&limits).is_err());

    metadata.max_retries = 10_000;
    metadata.timeout_secs = Some(8 * 24 * 60 * 60);
    let err = metadata
        .validate_with_limits(&limits)
        .expect_err("timeout above the raised cap is still rejected");
    assert!(err.contains("Timeout"), "unexpected: {err}");
}

#[test]
fn default_validation_limits_match_the_historical_caps() {
    let limits = ValidationLimits::default();
    assert_eq!(limits.max_retries, 1000);
    assert_eq!(limits.max_timeout_secs, 86_400);

    let mut metadata = TaskMetadata::new("t".to_string());
    metadata.max_retries = 1000;
    metadata.timeout_secs = Some(86_400);
    assert!(metadata.validate().is_ok());

    metadata.max_retries = 1001;
    assert!(metadata.validate().is_err());

    metadata.max_retries = 1000;
    metadata.timeout_secs = Some(86_401);
    assert!(metadata.validate().is_err());

    // A zero timeout is still rejected outright.
    metadata.timeout_secs = Some(0);
    assert!(metadata.validate().is_err());

    // An empty name is still rejected.
    let mut empty = TaskMetadata::new(String::new());
    empty.timeout_secs = None;
    assert!(empty.validate().is_err());
}
