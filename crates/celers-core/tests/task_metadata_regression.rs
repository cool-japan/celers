//! Regression tests for `celers_core::task` metadata semantics.
//!
//! These live in an integration test so `task.rs` stays comfortably under the
//! 2000-line file cap; everything exercised here is public API.

use celers_core::task::ValidationLimits;
use celers_core::{SerializedTask, TaskMetadata};
use chrono::Utc;

/// `is_expired()` measures `timeout_secs` from creation while the worker
/// measures the same field from the start of execution. The two readings are
/// now at least *named* distinctly, and both are deterministic to test.
///
/// Separating them properly needs a distinct `expires_at` field on
/// `TaskMetadata`; that is source-breaking for exhaustive struct literals
/// outside this crate and is tracked as follow-up work.
#[test]
fn expiry_reading_is_creation_relative_and_named_as_such() {
    let mut metadata = TaskMetadata::new("batch.job".to_string());
    metadata.timeout_secs = Some(1);
    assert!(!metadata.is_expired());
    assert!(!metadata.execution_time_elapsed());

    metadata.created_at = Utc::now() - chrono::Duration::hours(1);
    assert!(metadata.execution_time_elapsed());
    assert_eq!(metadata.is_expired(), metadata.execution_time_elapsed());

    // No timeout configured: never expires, however old.
    let mut forever = TaskMetadata::new("t".to_string());
    forever.created_at = Utc::now() - chrono::Duration::days(365);
    assert!(!forever.is_expired());
    assert!(!forever.execution_time_elapsed());
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
    let mut stale = SerializedTask::new("t".to_string(), vec![1]).with_timeout(1);
    stale.metadata.created_at = Utc::now() - chrono::Duration::hours(1);
    let fresh = SerializedTask::new("t".to_string(), vec![1]).with_timeout(3600);

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
