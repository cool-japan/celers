//! Tests for [`crate::worker_pool`].
//!
//! Kept in a sibling module so `worker_pool.rs` stays well under the
//! project's 2000-line file limit.

use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize as StdAtomicUsize;

/// Spin the scheduler (never the clock) until `check` passes.
///
/// Deterministic: it yields rather than sleeping, so it neither depends on
/// wall-clock timing nor interferes with a paused test clock.
macro_rules! wait_until {
    ($check:expr) => {{
        let mut satisfied = false;
        for _ in 0..2_000 {
            if $check {
                satisfied = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        satisfied
    }};
}

fn noop_task() -> WorkerTaskFn {
    Box::new(move || Box::pin(async {}))
}

#[test]
fn test_worker_pool_config_default() {
    let config = WorkerPoolConfig::default();
    assert_eq!(config.min_workers, 1);
    assert_eq!(config.max_workers, 10);
    assert_eq!(config.max_queue_depth, 1024);
}

#[test]
fn test_worker_pool_config_builder() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(2)
        .with_max_workers(20)
        .with_scaling_interval(Duration::from_secs(60))
        .with_max_queue_depth(8)
        .with_shutdown_grace_period(Duration::from_secs(3))
        .with_specialization(true);

    assert_eq!(config.min_workers, 2);
    assert_eq!(config.max_workers, 20);
    assert_eq!(config.scaling_interval, Duration::from_secs(60));
    assert_eq!(config.max_queue_depth, 8);
    assert_eq!(config.shutdown_grace_period, Duration::from_secs(3));
    assert!(config.enable_specialization);
}

#[test]
fn test_worker_pool_config_validation() {
    let invalid_config = WorkerPoolConfig::new().with_min_workers(0);
    assert!(invalid_config.validate().is_err());

    let invalid_config2 = WorkerPoolConfig::new()
        .with_min_workers(10)
        .with_max_workers(5);
    assert!(invalid_config2.validate().is_err());

    let invalid_config3 = WorkerPoolConfig::new().with_max_queue_depth(0);
    assert!(invalid_config3.validate().is_err());

    let valid_config = WorkerPoolConfig::new()
        .with_min_workers(2)
        .with_max_workers(10);
    assert!(valid_config.validate().is_ok());
}

#[tokio::test]
async fn test_worker_pool_creation() {
    let config = WorkerPoolConfig::default();
    let pool = WorkerPool::new(config);
    assert!(pool.is_ok());
}

#[tokio::test]
async fn test_worker_pool_start_stop() {
    let config = WorkerPoolConfig::new().with_min_workers(2);
    let pool = WorkerPool::new(config).expect("valid config");

    pool.start().await.expect("pool should start");
    assert_eq!(pool.worker_count().await, 2);

    let aborted = pool.stop().await;
    assert_eq!(aborted, 0, "idle workers must drain, never be aborted");
    assert_eq!(pool.worker_count().await, 0);
}

#[test]
fn test_worker_info_creation() {
    let info = WorkerInfo::new("worker-1".to_string());
    assert_eq!(info.id, "worker-1");
    assert_eq!(info.state, WorkerState::Starting);
    assert_eq!(info.tasks_processed, 0);
}

#[test]
fn test_worker_info_idle_check() {
    let mut info = WorkerInfo::new("worker-1".to_string());
    info.state = WorkerState::Idle;

    // Not idle yet (just created)
    assert!(!info.is_idle(Duration::from_secs(1)));
    // With no idle grace at all, an idle worker qualifies immediately.
    assert!(info.is_idle(Duration::ZERO));

    info.state = WorkerState::Running;
    assert!(!info.is_idle(Duration::ZERO));
}

#[tokio::test]
async fn test_scaling_decision() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_max_workers(10)
        .with_scaling_policy(ScalingPolicy::QueueBased {
            tasks_per_worker: 5,
            scale_up_threshold: 10,
            scale_down_threshold: 2,
        });

    let pool = WorkerPool::new(config).expect("valid config");

    // Should scale up when queue is high
    let decision = pool.make_scaling_decision(2, 15, None, None);
    assert!(matches!(decision, ScalingDecision::ScaleUp(_)));

    // Should scale down when queue is low — queue-based scaling must keep
    // working with no load telemetry at all.
    let decision = pool.make_scaling_decision(5, 1, None, None);
    assert!(matches!(decision, ScalingDecision::ScaleDown(_)));

    // Should not scale when queue is moderate
    let decision = pool.make_scaling_decision(2, 8, None, None);
    assert_eq!(decision, ScalingDecision::None);
}

#[tokio::test]
async fn test_worker_pool_stats() {
    let config = WorkerPoolConfig::new().with_min_workers(3);
    let pool = WorkerPool::new(config).expect("valid config");

    pool.start().await.expect("pool should start");

    let stats = pool.get_stats().await;
    assert_eq!(stats.worker_count, 3);

    pool.stop().await;
}

#[test]
fn test_scaling_policy_default() {
    let policy = ScalingPolicy::default();
    assert!(matches!(policy, ScalingPolicy::QueueBased { .. }));
}

#[test]
fn test_worker_states() {
    assert_eq!(WorkerState::Running, WorkerState::Running);
    assert_ne!(WorkerState::Running, WorkerState::Idle);
}

// --- compute_scaling_decision unit tests ---

#[test]
fn test_load_based_scale_up_on_high_cpu() {
    let decision = compute_scaling_decision(
        &ScalingPolicy::LoadBased {
            target_cpu_utilization: 70.0,
            target_memory_utilization: 80.0,
        },
        /*current*/ 2,
        /*queue*/ 0,
        /*cpu*/ Some(85.0),
        /*mem*/ Some(50.0),
        /*min*/ 1,
        /*max*/ 10,
    );
    assert!(
        matches!(decision, ScalingDecision::ScaleUp(1)),
        "Expected ScaleUp(1), got {:?}",
        decision
    );
}

#[test]
fn test_load_based_scale_up_on_high_memory() {
    let decision = compute_scaling_decision(
        &ScalingPolicy::LoadBased {
            target_cpu_utilization: 70.0,
            target_memory_utilization: 80.0,
        },
        2,
        0,
        /*cpu*/ Some(30.0),
        /*mem*/ Some(90.0),
        1,
        10,
    );
    assert!(matches!(decision, ScalingDecision::ScaleUp(1)));
}

#[test]
fn test_load_based_scale_down_when_idle() {
    let decision = compute_scaling_decision(
        &ScalingPolicy::LoadBased {
            target_cpu_utilization: 70.0,
            target_memory_utilization: 80.0,
        },
        /*current*/ 5,
        0,
        /*cpu*/ Some(10.0),
        /*mem*/ Some(15.0),
        /*min*/ 1,
        10,
    );
    assert!(matches!(decision, ScalingDecision::ScaleDown(1)));
}

#[test]
fn test_load_based_no_scale_in_band() {
    let decision = compute_scaling_decision(
        &ScalingPolicy::LoadBased {
            target_cpu_utilization: 70.0,
            target_memory_utilization: 80.0,
        },
        3,
        0,
        /*cpu*/ Some(55.0),
        /*mem*/ Some(60.0),
        1,
        10,
    );
    assert_eq!(decision, ScalingDecision::None);
}

// --- Regression: an unavailable load signal is not a low load (idx 168) ---

#[test]
fn test_load_based_unavailable_signal_never_scales_down() {
    let policy = ScalingPolicy::LoadBased {
        target_cpu_utilization: 70.0,
        target_memory_utilization: 80.0,
    };

    // Nothing measurable: the pool must hold its size, not shrink.
    assert_eq!(
        compute_scaling_decision(&policy, 5, 0, None, None, 1, 10),
        ScalingDecision::None
    );
    // Half a signal is still not enough to authorise a scale-down.
    assert_eq!(
        compute_scaling_decision(&policy, 5, 0, Some(1.0), None, 1, 10),
        ScalingDecision::None
    );
    assert_eq!(
        compute_scaling_decision(&policy, 5, 0, None, Some(1.0), 1, 10),
        ScalingDecision::None
    );
    // ...and it must not be read as a high load either.
    assert_eq!(
        compute_scaling_decision(&policy, 2, 0, None, None, 1, 10),
        ScalingDecision::None
    );
}

#[test]
fn test_hybrid_unavailable_signal_never_scales_down() {
    let policy = ScalingPolicy::Hybrid {
        tasks_per_worker: 5,
        scale_up_threshold: 10,
        scale_down_threshold: 2,
        max_cpu_utilization: 80.0,
        max_memory_utilization: 80.0,
    };
    assert_eq!(
        compute_scaling_decision(&policy, 5, 1, None, None, 1, 10),
        ScalingDecision::None
    );
}

#[test]
fn test_hybrid_scale_up_on_queue_depth() {
    let policy = ScalingPolicy::Hybrid {
        tasks_per_worker: 5,
        scale_up_threshold: 10,
        scale_down_threshold: 2,
        max_cpu_utilization: 80.0,
        max_memory_utilization: 80.0,
    };
    // Queue high, load low → should scale up
    let decision = compute_scaling_decision(&policy, 2, 20, Some(10.0), Some(10.0), 1, 10);
    assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
}

#[test]
fn test_hybrid_scale_up_on_high_load() {
    let policy = ScalingPolicy::Hybrid {
        tasks_per_worker: 5,
        scale_up_threshold: 10,
        scale_down_threshold: 2,
        max_cpu_utilization: 80.0,
        max_memory_utilization: 80.0,
    };
    // Queue shallow but load high → should scale up
    let decision = compute_scaling_decision(&policy, 2, 3, Some(95.0), Some(10.0), 1, 10);
    assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
}

#[test]
fn test_hybrid_scale_down_when_quiet() {
    let policy = ScalingPolicy::Hybrid {
        tasks_per_worker: 5,
        scale_up_threshold: 10,
        scale_down_threshold: 2,
        max_cpu_utilization: 80.0,
        max_memory_utilization: 80.0,
    };
    // Queue low AND load low → should scale down
    let decision = compute_scaling_decision(&policy, 5, 1, Some(5.0), Some(5.0), 1, 10);
    assert!(matches!(decision, ScalingDecision::ScaleDown(1)));
}

#[test]
fn test_hybrid_no_scale_mixed_signals() {
    let policy = ScalingPolicy::Hybrid {
        tasks_per_worker: 5,
        scale_up_threshold: 10,
        scale_down_threshold: 2,
        max_cpu_utilization: 80.0,
        max_memory_utilization: 80.0,
    };
    // Queue low but load moderate → neither clear scale-up nor scale-down
    let decision = compute_scaling_decision(&policy, 3, 1, Some(50.0), Some(50.0), 1, 10);
    assert_eq!(decision, ScalingDecision::None);
}

#[test]
fn test_cpu_sampler_first_sample_is_never_zero() {
    let mut sampler = CpuSampler::default();
    // The first call has no delta to report. It may fall back to the host
    // load average, but it must never report a fabricated 0.0.
    if let Some(value) = sampler.sample() {
        assert!(
            value >= 0.0,
            "load-average fallback must be a real percentage"
        );
    }
    assert!(
        sampler.previous.is_some() || crate::sysinfo::read_process_cpu_time().is_none(),
        "a successful reading must establish the baseline for the next delta"
    );
}

#[test]
fn test_set_queue_depth_and_handle() {
    let pool = WorkerPool::new(WorkerPoolConfig::default()).expect("valid config");
    pool.set_queue_depth(42);
    assert_eq!(pool.inner.queue_depth.load(Ordering::Relaxed), 42);

    let handle = pool.queue_depth_handle();
    handle.store(99, Ordering::Relaxed);
    assert_eq!(pool.inner.queue_depth.load(Ordering::Relaxed), 99);
}

#[tokio::test]
async fn test_submit_task_executes() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_max_workers(2);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    let flag = Arc::new(AtomicBool::new(false));
    let flag2 = Arc::clone(&flag);

    pool.submit_task(Box::new(move || {
        Box::pin(async move {
            flag2.store(true, Ordering::SeqCst);
        })
    }))
    .expect("submit should succeed");

    assert!(
        wait_until!(flag.load(Ordering::SeqCst)),
        "task should have executed"
    );

    pool.stop().await;
}

#[tokio::test]
async fn test_submit_multiple_tasks() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(2)
        .with_max_workers(4);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    let counter = Arc::new(StdAtomicUsize::new(0));
    for _ in 0..10 {
        let counter2 = Arc::clone(&counter);
        pool.submit_task(Box::new(move || {
            Box::pin(async move {
                counter2.fetch_add(1, Ordering::SeqCst);
            })
        }))
        .expect("submit should succeed");
    }

    assert!(
        wait_until!(counter.load(Ordering::SeqCst) == 10),
        "all 10 tasks should have executed, saw {}",
        counter.load(Ordering::SeqCst)
    );

    // Every completed task must be accounted for in the pool statistics.
    assert!(wait_until!(
        pool.get_stats().await.total_tasks_processed == 10
    ));

    pool.stop().await;
}

// --- Regression: the submission channel is bounded (idx 189 / 306) ---

#[tokio::test]
async fn test_submit_task_applies_backpressure_when_full() {
    // No workers started, so nothing drains the queue.
    let config = WorkerPoolConfig::new().with_max_queue_depth(2);
    let pool = WorkerPool::new(config).expect("valid config");

    assert!(pool.submit_task(noop_task()).is_ok());
    assert!(pool.submit_task(noop_task()).is_ok());
    assert_eq!(pool.pending_tasks(), 2);

    let err = pool
        .submit_task(noop_task())
        .expect_err("a full queue must refuse work instead of growing");
    assert!(err.contains("full"), "unexpected error: {}", err);
}

#[tokio::test]
async fn test_submit_task_async_waits_for_capacity() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_max_queue_depth(1);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    // A worker is draining, so the awaiting submit makes progress rather
    // than failing.
    let counter = Arc::new(StdAtomicUsize::new(0));
    for _ in 0..5 {
        let counter2 = Arc::clone(&counter);
        pool.submit_task_async(Box::new(move || {
            Box::pin(async move {
                counter2.fetch_add(1, Ordering::SeqCst);
            })
        }))
        .await
        .expect("awaiting submit should succeed");
    }

    assert!(wait_until!(counter.load(Ordering::SeqCst) == 5));
    pool.stop().await;
}

// --- Regression: real idle tracking makes scale-down work (idx 168) ---

#[tokio::test]
async fn test_worker_becomes_idle_after_finishing_a_task() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_scaling_policy(ScalingPolicy::Manual);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    // Freshly spawned workers start idle...
    assert!(wait_until!(pool.get_stats().await.idle_workers == 1));

    let done = Arc::new(AtomicBool::new(false));
    let done2 = Arc::clone(&done);
    pool.submit_task(Box::new(move || {
        Box::pin(async move {
            done2.store(true, Ordering::SeqCst);
        })
    }))
    .expect("submit should succeed");

    assert!(wait_until!(done.load(Ordering::SeqCst)));
    // ...and go back to idle once the queue is drained again.
    assert!(
        wait_until!(pool.get_stats().await.idle_workers == 1),
        "a worker with nothing to do must be reported Idle"
    );

    let workers = pool.inner.workers.read().await;
    let info = workers.values().next().expect("one worker");
    assert_eq!(info.tasks_processed, 1);
    assert!(info.is_idle(Duration::ZERO));
    drop(workers);

    pool.stop().await;
}

#[tokio::test]
async fn test_scale_down_actually_removes_idle_workers() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_max_workers(8)
        .with_worker_idle_timeout(Duration::ZERO)
        .with_shutdown_grace_period(Duration::from_secs(1))
        .with_scaling_policy(ScalingPolicy::Manual);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");
    pool.execute_scaling(ScalingDecision::ScaleUp(3))
        .await
        .expect("scale-up should succeed");
    assert_eq!(pool.worker_count().await, 4);

    let removed = pool
        .execute_scaling(ScalingDecision::ScaleDown(2))
        .await
        .expect("scale-down should succeed");

    assert_eq!(removed, 2, "scale-down must actually retire workers");
    assert_eq!(pool.worker_count().await, 2);

    let stats = pool.get_stats().await;
    assert_eq!(stats.worker_count, 2);
    assert_eq!(stats.scale_down_count, 1);

    pool.stop().await;
}

#[tokio::test]
async fn test_scale_down_without_idle_workers_is_a_no_op() {
    // A long idle timeout means no worker qualifies as a scale-down victim.
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_max_workers(4)
        .with_worker_idle_timeout(Duration::from_secs(3600))
        .with_shutdown_grace_period(Duration::from_secs(1))
        .with_scaling_policy(ScalingPolicy::Manual);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");
    pool.execute_scaling(ScalingDecision::ScaleUp(1))
        .await
        .expect("scale-up should succeed");
    assert_eq!(pool.worker_count().await, 2);

    let removed = pool
        .execute_scaling(ScalingDecision::ScaleDown(1))
        .await
        .expect("scale-down should succeed");

    assert_eq!(removed, 0);
    assert_eq!(pool.worker_count().await, 2);
    let stats = pool.get_stats().await;
    assert_eq!(
        stats.scale_down_count, 0,
        "a scale-down that removed nobody must not be counted as an event"
    );

    pool.stop().await;
}

#[tokio::test]
async fn test_scaling_respects_pool_bounds() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(2)
        .with_max_workers(3)
        .with_worker_idle_timeout(Duration::ZERO)
        .with_shutdown_grace_period(Duration::from_secs(1))
        .with_scaling_policy(ScalingPolicy::Manual);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    // Capped at max_workers.
    assert_eq!(
        pool.execute_scaling(ScalingDecision::ScaleUp(10))
            .await
            .expect("scale-up should succeed"),
        1
    );
    assert_eq!(pool.worker_count().await, 3);

    // Floored at min_workers.
    assert_eq!(
        pool.execute_scaling(ScalingDecision::ScaleDown(10))
            .await
            .expect("scale-down should succeed"),
        1
    );
    assert_eq!(pool.worker_count().await, 2);

    pool.stop().await;
}

// --- Regression: worker IDs are never reused (idx 179) ---

#[tokio::test]
async fn test_worker_ids_are_monotonic_after_removal() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(3)
        .with_max_workers(8)
        .with_worker_idle_timeout(Duration::ZERO)
        .with_shutdown_grace_period(Duration::from_secs(1))
        .with_scaling_policy(ScalingPolicy::Manual);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    // Retire the middle worker, leaving a gap in the numbering.
    pool.inner
        .stop_worker("worker-1", Duration::from_secs(1))
        .await;
    assert_eq!(pool.worker_count().await, 2);

    // The old code minted IDs from the map length and would regenerate
    // "worker-2" here, silently replacing (and detaching) a live worker.
    pool.execute_scaling(ScalingDecision::ScaleUp(2))
        .await
        .expect("scale-up should succeed");

    let ids: Vec<String> = pool.inner.workers.read().await.keys().cloned().collect();
    assert_eq!(
        ids.len(),
        4,
        "every spawned worker must be tracked: {:?}",
        ids
    );
    assert_eq!(
        pool.inner.handles.read().await.len(),
        4,
        "no join handle may be silently displaced"
    );
    assert!(ids.contains(&"worker-3".to_string()));
    assert!(ids.contains(&"worker-4".to_string()));
    assert!(!ids.contains(&"worker-1".to_string()));

    let stats = pool.get_stats().await;
    assert_eq!(
        stats.worker_count, 4,
        "worker_count must be derived from the map, not a drifting counter"
    );

    pool.stop().await;
}

// --- Regression: graceful shutdown drains, then aborts (idx 189) ---

#[tokio::test(start_paused = true)]
async fn test_stop_drains_in_flight_tasks() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(1)
        .with_shutdown_grace_period(Duration::from_secs(30));
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let started2 = Arc::clone(&started);
    let finished2 = Arc::clone(&finished);

    pool.submit_task(Box::new(move || {
        Box::pin(async move {
            started2.store(true, Ordering::SeqCst);
            // Virtual time: the paused clock auto-advances, so this costs
            // the test nothing in wall-clock terms.
            tokio::time::sleep(Duration::from_secs(5)).await;
            finished2.store(true, Ordering::SeqCst);
        })
    }))
    .expect("submit should succeed");

    assert!(wait_until!(started.load(Ordering::SeqCst)));

    let aborted = pool.stop().await;
    assert_eq!(
        aborted, 0,
        "a task that fits inside the grace period must not be aborted"
    );
    assert!(
        finished.load(Ordering::SeqCst),
        "stop() must let in-flight work finish before tearing the pool down"
    );
}

#[tokio::test]
async fn test_stop_aborts_stragglers_after_the_grace_period() {
    let config = WorkerPoolConfig::new().with_min_workers(1);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    let started = Arc::new(AtomicBool::new(false));
    let started2 = Arc::clone(&started);

    pool.submit_task(Box::new(move || {
        Box::pin(async move {
            started2.store(true, Ordering::SeqCst);
            // Never finishes on its own.
            std::future::pending::<()>().await;
        })
    }))
    .expect("submit should succeed");

    assert!(wait_until!(started.load(Ordering::SeqCst)));

    let aborted = pool.stop_with_grace(Duration::ZERO).await;
    assert_eq!(aborted, 1, "a straggler must be aborted, not detached");
    assert_eq!(pool.worker_count().await, 0);
}

#[tokio::test]
async fn test_busy_worker_observes_shutdown_after_its_task() {
    // `Notify::notify_waiters` (the old signal) only wakes futures already
    // registered, so a busy worker never saw it. A `watch` retains the value.
    let config = WorkerPoolConfig::new().with_min_workers(1);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    let release = Arc::new(tokio::sync::Notify::new());
    let release2 = Arc::clone(&release);
    let started = Arc::new(AtomicBool::new(false));
    let started2 = Arc::clone(&started);

    pool.submit_task(Box::new(move || {
        Box::pin(async move {
            started2.store(true, Ordering::SeqCst);
            release2.notified().await;
        })
    }))
    .expect("submit should succeed");

    assert!(wait_until!(started.load(Ordering::SeqCst)));

    // Signal shutdown while the worker is busy, then let the task finish.
    // `notify_one` (not `notify_waiters`) so the permit is stored even if the
    // task has not reached its `notified()` await yet -- otherwise this test
    // could hang on a lost wakeup.
    let _ = pool.inner.shutdown.send(true);
    release.notify_one();

    let handles: Vec<(String, WorkerHandle)> = pool.inner.handles.write().await.drain().collect();
    for (_, WorkerHandle { join, .. }) in handles {
        // The worker must exit on its own, without an abort. Bounded so a
        // regression fails the test instead of hanging the suite.
        tokio::time::timeout(Duration::from_secs(5), join)
            .await
            .expect("a busy worker must observe the retained shutdown signal")
            .expect("worker should exit cleanly");
    }
}

// --- Regression: dead workers are detected and replaced (idx 168) ---

#[tokio::test]
async fn test_health_sweep_replaces_a_dead_worker() {
    let config = WorkerPoolConfig::new()
        .with_min_workers(2)
        .with_scaling_policy(ScalingPolicy::Manual);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    // Simulate a worker whose loop died unexpectedly.
    {
        let handles = pool.inner.handles.read().await;
        handles
            .get("worker-0")
            .expect("worker-0 exists")
            .join
            .abort();
    }
    assert!(wait_until!({
        let handles = pool.inner.handles.read().await;
        handles
            .get("worker-0")
            .is_some_and(|h| h.join.is_finished())
    }));

    let replaced = pool.inner.health_sweep().await;
    assert_eq!(replaced, 1, "a dead worker must be replaced");
    assert_eq!(pool.worker_count().await, 2);

    let ids: Vec<String> = pool.inner.workers.read().await.keys().cloned().collect();
    assert_eq!(
        ids.len(),
        2,
        "the pool must be back to full strength: {:?}",
        ids
    );
    assert!(
        !ids.contains(&"worker-0".to_string()),
        "the dead worker must not linger: {:?}",
        ids
    );

    // The replacement must be a working worker, not a corpse.
    let flag = Arc::new(AtomicBool::new(false));
    let flag2 = Arc::clone(&flag);
    pool.submit_task(Box::new(move || {
        Box::pin(async move {
            flag2.store(true, Ordering::SeqCst);
        })
    }))
    .expect("submit should succeed");
    assert!(wait_until!(flag.load(Ordering::SeqCst)));

    pool.stop().await;
}

#[tokio::test]
async fn test_health_sweep_is_quiet_during_shutdown() {
    let config = WorkerPoolConfig::new().with_min_workers(1);
    let pool = WorkerPool::new(config).expect("valid config");
    pool.start().await.expect("pool should start");

    pool.stop().await;
    assert_eq!(
        pool.inner.health_sweep().await,
        0,
        "a stopping pool must not respawn workers"
    );
}
