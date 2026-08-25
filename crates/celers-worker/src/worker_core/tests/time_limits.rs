//! Regression tests for per-task soft/hard time limits: merged resolution
//! (manager default plus a per-task override), a soft limit that only warns,
//! its lifecycle event, and a hard limit that is terminal.

use super::doubles::{serialized, wait_until, CapturingEmitter, Empty, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::time_limit::{TimeLimitConfig, WorkerTimeLimits};
use celers_core::{
    BrokerMessage, Event, NoOpEventEmitter, Result, Task, TaskEvent, TaskId, TaskRegistry,
};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// --------------------------------------------------------------------------

/// A task that runs until its soft time limit fires, then wraps up cleanly.
///
/// This is the cooperative shape Celery's `SoftTimeLimitExceeded` exists for:
/// the task is *told* it is out of time and returns partial work rather than
/// being killed.
struct SoftLimitAwareTask {
    /// Set once the task observed its soft-limit signal.
    observed_soft_limit: Arc<AtomicUsize>,
    /// Incremented when the task returns normally.
    finished: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for SoftLimitAwareTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        // Safety valve so a broken signal fails the test instead of hanging it.
        for _ in 0..2_000 {
            if crate::execution_context::check_soft_time_limit().is_err() {
                self.observed_soft_limit.fetch_add(1, Ordering::Relaxed);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        self.finished.fetch_add(1, Ordering::Relaxed);
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "soft_limit_aware_task"
    }
}

/// A task that ignores every signal and runs effectively forever.
struct NeverEndingTask {
    started: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for NeverEndingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.started.fetch_add(1, Ordering::Relaxed);
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn name(&self) -> &'static str {
        "never_ending_task"
    }
}

// --------------------------------------------------------------------------

/// idx 42: a per-task override merges onto the manager default, and both halves
/// reach the execution loop.
#[tokio::test]
async fn test_worker_resolves_merged_time_limits_per_task_name() {
    let limits = WorkerTimeLimits::with_default(
        TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(30))
            .with_hard_limit(Duration::from_secs(60)),
    );
    limits.set_task_limit(
        "slow_task",
        TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)),
    );

    let broker = RecordingBroker::new(Vec::new(), false);
    let worker: Worker<RecordingBroker, NoOpEventEmitter> = Worker::new_from_arc(
        Arc::clone(&broker),
        TaskRegistry::new(),
        WorkerConfig::default(),
    )
    .with_time_limits(limits);

    let task_id = TaskId::new_v4();
    let slow = worker
        .resolve_time_limits(task_id, "slow_task")
        .expect("the override applies");
    assert_eq!(
        slow.soft_limit(),
        Some(Duration::from_secs(30)),
        "a hard-limit-only override must not drop the default soft limit"
    );
    assert_eq!(slow.hard_limit(), Some(Duration::from_secs(600)));

    let other = worker
        .resolve_time_limits(task_id, "other_task")
        .expect("the default applies");
    assert_eq!(other.hard_limit(), Some(Duration::from_secs(60)));

    // Without a manager configured, nothing is resolved.
    let bare: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(broker, TaskRegistry::new(), WorkerConfig::default());
    assert!(bare.resolve_time_limits(task_id, "slow_task").is_none());
}

/// idx 42: the soft limit is a *warning*, not a disposition. It trips the
/// cooperative signal the task observes, is counted in `WorkerStats`, and the
/// task still finishes successfully (acked, not revoked, not dead-lettered).
#[tokio::test]
async fn test_soft_time_limit_warns_without_killing_the_task() {
    let observed = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(SoftLimitAwareTask {
            observed_soft_limit: Arc::clone(&observed),
            finished: Arc::clone(&finished),
        })
        .await;

    let task = serialized("soft_limit_aware_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config).with_time_limits(
            WorkerTimeLimits::with_default(
                // Soft only: nothing may kill this task.
                TimeLimitConfig::new().with_soft_limit(Duration::from_millis(30)),
            ),
        );
    let stats = worker.stats_arc();
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to finish after its soft limit", || {
        finished.load(Ordering::Relaxed) == 1
    })
    .await;

    assert_eq!(
        observed.load(Ordering::Relaxed),
        1,
        "the running task must observe its own soft time limit"
    );
    assert_eq!(
        stats.soft_timeouts(),
        1,
        "the soft-limit expiry must be counted"
    );
    assert_eq!(stats.revoked(), 0, "a soft limit must never revoke a task");
    wait_until("the successful task to be acked", || {
        broker.acked() == vec![task_id]
    })
    .await;
    assert!(broker.rejected().is_empty());
    assert_eq!(dlq.size().await, 0, "a soft limit is not a failure");

    handle.shutdown().await.expect("shutdown");
}

/// idx 7: a soft-limit expiry is published as a `task-soft-time-limit-exceeded`
/// event. It used to surface only as a `WorkerStats` counter and a `warn!`, so
/// a monitor watching the event stream could not tell a slow task from a
/// healthy one until the hard limit turned it into a failure.
#[tokio::test]
async fn test_soft_time_limit_emits_a_lifecycle_event() {
    let observed = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(SoftLimitAwareTask {
            observed_soft_limit: Arc::clone(&observed),
            finished: Arc::clone(&finished),
        })
        .await;

    let task = serialized("soft_limit_aware_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_events: true,
        hostname: "celery@test-host".to_string(),
        ..Default::default()
    };
    let emitter = CapturingEmitter::default();
    let worker =
        Worker::with_event_emitter_from_arc(Arc::clone(&broker), registry, config, emitter.clone())
            .with_time_limits(WorkerTimeLimits::with_default(
                TimeLimitConfig::new().with_soft_limit(Duration::from_millis(30)),
            ));
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("a task-soft-time-limit-exceeded event", || {
        emitter
            .task_event_names()
            .contains(&"soft-time-limit-exceeded")
    })
    .await;

    let breaches: Vec<Event> = emitter
        .events()
        .into_iter()
        .filter(|event| matches!(event, Event::Task(TaskEvent::SoftTimeLimitExceeded { .. })))
        .collect();
    assert_eq!(
        breaches.len(),
        1,
        "the expiry must be reported exactly once, not once per poll"
    );

    let Event::Task(TaskEvent::SoftTimeLimitExceeded {
        task_id: event_task_id,
        ref task_name,
        ref hostname,
        elapsed_secs,
        limit_secs,
        ..
    }) = breaches[0]
    else {
        panic!("filtered to soft-limit events");
    };
    assert_eq!(event_task_id, task_id);
    assert_eq!(task_name, "soft_limit_aware_task");
    assert_eq!(hostname, "celery@test-host");
    assert!(
        (limit_secs - 0.030).abs() < 1e-9,
        "the event must report the configured limit, got {limit_secs}"
    );
    assert!(
        elapsed_secs >= limit_secs,
        "the task ran at least as long as the limit ({elapsed_secs} < {limit_secs})"
    );

    // The event must also be publishable in the Celery wire shape.
    let wire = breaches[0].to_wire_json().expect("renders to the wire");
    assert!(wire.contains(r#""type":"task-soft-time-limit-exceeded""#));
    assert!(wire.contains(&format!(r#""uuid":"{task_id}""#)));

    wait_until("the task to finish after its soft limit", || {
        finished.load(Ordering::Relaxed) == 1
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 42: the hard limit *is* terminal. It aborts the task and maps onto the
/// worker's existing timeout failure path — dead-lettered (retries exhausted)
/// with a failure class naming the limit that fired.
#[tokio::test]
async fn test_hard_time_limit_aborts_the_task_with_a_timeout_failure() {
    let started = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(NeverEndingTask {
            started: Arc::clone(&started),
        })
        .await;

    // No retry budget: the first hard-limit expiry is terminal.
    let task = serialized("never_ending_task").with_max_retries(0);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        // Far longer than the hard limit, so the hard limit is what fires.
        default_timeout_secs: 300,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config).with_time_limits(
            WorkerTimeLimits::with_default(
                TimeLimitConfig::new().with_hard_limit(Duration::from_millis(50)),
            ),
        );
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the hard limit to kill the task", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(started.load(Ordering::Relaxed), 1);
    assert_eq!(broker.rejected(), vec![task_id]);
    assert!(
        broker.acked().is_empty(),
        "a task killed by its hard limit must not be acked as success"
    );

    let entries = dlq.get_entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].metadata.get("failure_type").map(String::as_str),
        Some("hard_time_limit"),
        "the DLQ entry must name the limit that fired: {:?}",
        entries[0].metadata
    );
    assert_eq!(
        entries[0]
            .metadata
            .get("hard_limit_millis")
            .map(String::as_str),
        Some("50")
    );
    assert!(
        entries[0]
            .error_message
            .contains("Hard time limit exceeded"),
        "got {}",
        entries[0].error_message
    );

    handle.shutdown().await.expect("shutdown");
}
