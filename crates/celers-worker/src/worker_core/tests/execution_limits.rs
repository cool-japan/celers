//! Unit tests for [`ExecutionLimits`]: which bound (the plain execution
//! timeout or a configured hard time limit) is the earlier deadline, and
//! which one is reported when it actually fires.

use crate::worker_core::execution::ExecutionLimits;

use celers_core::time_limit::TimeLimitConfig;
use celers_core::TaskId;

use std::time::Duration;

#[test]
fn test_execution_limits_deadline_is_the_earlier_bound() {
    // No time limits: the plain execution timeout is the deadline.
    let plain = ExecutionLimits::from_timeout(30);
    assert_eq!(plain.deadline(), Duration::from_secs(30));
    assert!(!plain.hard_limit_is_binding());

    // A shorter hard limit wins.
    let short_hard = ExecutionLimits::from_timeout(30).with_time_limits(
        &TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(1))
            .with_hard_limit(Duration::from_secs(5)),
    );
    assert_eq!(short_hard.deadline(), Duration::from_secs(5));
    assert!(short_hard.hard_limit_is_binding());
    assert_eq!(short_hard.soft_limit, Some(Duration::from_secs(1)));

    // A longer hard limit does not extend the task's own timeout.
    let long_hard = ExecutionLimits::from_timeout(30)
        .with_time_limits(&TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)));
    assert_eq!(long_hard.deadline(), Duration::from_secs(30));
    assert!(!long_hard.hard_limit_is_binding());

    // Sub-second hard limits survive (they are stored in milliseconds).
    let sub_second = ExecutionLimits::from_timeout(30)
        .with_time_limits(&TimeLimitConfig::new().with_hard_limit(Duration::from_millis(250)));
    assert_eq!(sub_second.deadline(), Duration::from_millis(250));
}

#[test]
fn test_execution_limits_report_the_limit_that_actually_fired() {
    let task_id = TaskId::new_v4();

    let plain = ExecutionLimits::from_timeout(30);
    let failure = plain.timeout_failure(task_id, Duration::from_secs(30));
    assert_eq!(failure.failure_type, "timeout");
    assert!(failure.message.contains("timed out after 30s"));
    assert_eq!(failure.metadata, vec![("timeout_secs", "30".to_string())]);

    let hard = ExecutionLimits::from_timeout(30)
        .with_time_limits(&TimeLimitConfig::new().with_hard_limit(Duration::from_secs(5)));
    let failure = hard.timeout_failure(task_id, Duration::from_secs(5));
    assert_eq!(failure.failure_type, "hard_time_limit");
    assert!(
        failure.message.contains("Hard time limit exceeded"),
        "got {}",
        failure.message
    );
    assert_eq!(
        failure.metadata,
        vec![("hard_limit_millis", "5000".to_string())]
    );
}
