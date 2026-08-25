//! Unit tests for the small pure-function helpers in
//! [`crate::worker_core::support`]: retry-budget clamping, delay rounding
//! and clamping, and panic-payload extraction.

use crate::worker_core::support::{
    clamp_defer_delay, effective_max_retries, panic_message, schedulable_delay_secs,
};

use std::time::Duration;

#[test]
fn test_effective_max_retries_is_capped_by_the_worker_budget() {
    // idx 182: `DynamicConfig::max_retries` (what `set_max_retries` writes) was
    // never consulted -- only the task's own value.
    assert_eq!(effective_max_retries(10, 3), 3);
    assert_eq!(effective_max_retries(1, 3), 1);
    assert_eq!(effective_max_retries(0, 3), 0);
}

#[test]
fn test_schedulable_delay_rounds_up_and_rejects_subsecond() {
    // Truncating 1.9s to 1s would shorten every backoff; truncating 900ms to 0
    // would remove it entirely.
    assert_eq!(schedulable_delay_secs(Duration::from_millis(900)), None);
    assert_eq!(schedulable_delay_secs(Duration::from_secs(1)), Some(1));
    assert_eq!(schedulable_delay_secs(Duration::from_millis(1900)), Some(2));
    assert_eq!(schedulable_delay_secs(Duration::from_secs(60)), Some(60));
}

#[test]
fn test_clamp_defer_delay_bounds_an_unbounded_retry_hint() {
    // A zero-rate limiter reports an effectively infinite `retry_after`;
    // sleeping on it verbatim would park the dequeue loop for hours.
    let clamped = clamp_defer_delay(Duration::from_secs(86_400), 100, 2_000);
    assert!(
        clamped <= Duration::from_millis(2_000),
        "clamped delay {clamped:?} must respect the configured maximum"
    );
    assert!(clamped >= Duration::from_millis(100));

    let floored = clamp_defer_delay(Duration::from_millis(1), 100, 2_000);
    assert!(floored >= Duration::from_millis(100));

    assert_eq!(clamp_defer_delay(Duration::ZERO, 0, 0), Duration::ZERO);
}

#[test]
fn test_panic_message_extracts_both_payload_shapes() {
    assert_eq!(panic_message(&"boom"), "boom");
    assert_eq!(panic_message(&"boom".to_string()), "boom");
    assert_eq!(panic_message(&42_u8), "task panicked");
}
