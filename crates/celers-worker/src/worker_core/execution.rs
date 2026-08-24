//! Per-task execution: drives one dequeued message to a terminal disposition.
//!
//! Split out of [`worker_core`](crate::worker_core) so the dequeue loop stays
//! readable. Everything here runs inside the task's own spawned future.

use crate::checkpoint::CheckpointManager;
use crate::circuit_breaker::CircuitBreaker;
use crate::dlq::{self, DlqHandler};
use crate::execution_context::{RevocationWatcher, TaskExecutionContext};
use crate::health::HealthChecker;
use crate::memory::{self, MemoryTracker};
use crate::middleware;
use crate::poison_pill::PoisonPillDetector;
use crate::retry::RetryConfig;
use crate::types::WorkerStats;

use super::support::{
    backoff_delay, panic_message, schedulable_delay_secs, ActiveTaskGuard, EventSink,
    InFlightRegistry,
};

use celers_core::time_limit::{TimeLimitConfig, TimeLimitExceeded};
use celers_core::{
    Broker, CelersError, Event, Result, SerializedTask, TaskEvent, TaskEventBuilder, TaskId,
    TaskRegistry, TaskState,
};

use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

#[cfg(feature = "metrics")]
use celers_metrics::{
    TASKS_COMPLETED_BY_TYPE, TASKS_COMPLETED_TOTAL, TASKS_FAILED_BY_TYPE, TASKS_FAILED_TOTAL,
    TASKS_RETRIED_BY_TYPE, TASKS_RETRIED_TOTAL, TASK_EXECUTION_TIME, TASK_EXECUTION_TIME_BY_TYPE,
};

/// The time bounds applied to one task's execution.
///
/// CeleRS (like Celery) has two independent notions of "running too long":
///
/// * the task's own **execution timeout**
///   ([`TaskMetadata::timeout_secs`](celers_core::TaskMetadata::timeout_secs),
///   falling back to the worker's `default_timeout_secs`), and
/// * the configured **time limits**
///   ([`WorkerTimeLimits`](celers_core::WorkerTimeLimits)): a *soft* limit that
///   only warns the running task, and a *hard* limit that kills it.
///
/// The task future is raced against whichever of the execution timeout and the
/// hard limit comes first. The soft limit is armed separately and never ends the
/// task: it trips the [`SoftTimeout`](crate::execution_context::SoftTimeout)
/// signal so cooperative task code can wrap up before the hard limit lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionLimits {
    /// The configured execution timeout, in seconds.
    pub(crate) timeout_secs: u64,
    /// Soft time limit, when configured.
    pub(crate) soft_limit: Option<Duration>,
    /// Hard time limit, when configured.
    pub(crate) hard_limit: Option<Duration>,
}

/// The terminal failure produced by an expired execution deadline.
///
/// Both the plain execution timeout and the hard time limit funnel through the
/// worker's single timeout path (retry while budget remains, dead-letter after);
/// only the wording, failure class and DLQ metadata differ, so callers can tell
/// a Celery `TimeLimitExceeded` apart from an ordinary timeout.
pub(crate) struct TimeoutFailure {
    /// Human-readable reason (the `task-failed` event's exception text).
    pub(crate) message: String,
    /// Machine-readable failure class recorded in the DLQ.
    pub(crate) failure_type: &'static str,
    /// Extra DLQ metadata describing the limit that fired.
    pub(crate) metadata: Vec<(&'static str, String)>,
}

impl ExecutionLimits {
    /// Limits consisting of just the execution timeout (no time limits).
    pub(crate) fn from_timeout(timeout_secs: u64) -> Self {
        Self {
            timeout_secs,
            soft_limit: None,
            hard_limit: None,
        }
    }

    /// Overlay a resolved [`TimeLimitConfig`] onto these limits.
    pub(crate) fn with_time_limits(mut self, config: &TimeLimitConfig) -> Self {
        self.soft_limit = config.soft_limit();
        self.hard_limit = config.hard_limit();
        self
    }

    /// The deadline the task future is actually raced against: the earlier of
    /// the execution timeout and the hard time limit.
    pub(crate) fn deadline(&self) -> Duration {
        let timeout = Duration::from_secs(self.timeout_secs);
        match self.hard_limit {
            Some(hard) => hard.min(timeout),
            None => timeout,
        }
    }

    /// Whether the hard time limit — rather than the plain execution timeout —
    /// is the binding deadline.
    ///
    /// Ties go to the hard limit: it is the more specific configuration, and its
    /// failure carries the limit that operators actually set.
    pub(crate) fn hard_limit_is_binding(&self) -> bool {
        self.hard_limit
            .is_some_and(|hard| hard <= Duration::from_secs(self.timeout_secs))
    }

    /// Describe the failure for a task that hit its deadline after `elapsed`.
    pub(crate) fn timeout_failure(&self, task_id: TaskId, elapsed: Duration) -> TimeoutFailure {
        match self.hard_limit.filter(|_| self.hard_limit_is_binding()) {
            Some(hard) => {
                let exceeded = TimeLimitExceeded::HardLimitExceeded {
                    task_id: task_id.to_string(),
                    elapsed_millis: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                    limit_millis: u64::try_from(hard.as_millis()).unwrap_or(u64::MAX),
                };
                TimeoutFailure {
                    message: exceeded.to_string(),
                    failure_type: "hard_time_limit",
                    metadata: vec![("hard_limit_millis", hard.as_millis().to_string())],
                }
            }
            None => TimeoutFailure {
                message: format!("Task timed out after {}s", self.timeout_secs),
                failure_type: "timeout",
                metadata: vec![("timeout_secs", self.timeout_secs.to_string())],
            },
        }
    }
}

/// Everything one dispatched message needs to run to a terminal disposition.
pub(crate) struct TaskDispatch<B: Broker> {
    /// Broker the message came from (used for ack/reject/re-enqueue).
    pub(crate) broker: Arc<B>,
    /// Handler registry.
    pub(crate) registry: Arc<TaskRegistry>,
    /// The task itself, shared with the inner execution task (no second copy).
    pub(crate) task: Arc<SerializedTask>,
    /// Id of the task (cached: also valid after `task` is consumed).
    pub(crate) task_id: TaskId,
    /// Broker receipt handle for this delivery, if any.
    pub(crate) receipt_handle: Option<String>,
    /// Non-blocking lifecycle event sink.
    pub(crate) events: EventSink,
    /// Worker hostname for event identification.
    pub(crate) hostname: String,
    /// Worker process id for event identification.
    pub(crate) pid: u32,
    /// Shared worker statistics.
    pub(crate) stats: Arc<WorkerStats>,
    /// Optional middleware stack.
    pub(crate) middleware: Option<Arc<middleware::MiddlewareStack>>,
    /// Optional dead-letter handler.
    pub(crate) dlq_handler: Option<Arc<DlqHandler>>,
    /// Optional circuit breaker.
    pub(crate) circuit_breaker: Option<Arc<CircuitBreaker>>,
    /// Revocation watcher (for unregistering the in-flight token).
    pub(crate) revocation_watcher: Option<RevocationWatcher>,
    /// Cooperative-cancellation context, when revocation is enabled.
    pub(crate) exec_context: Option<TaskExecutionContext>,
    /// Undisposed-message registry (also the disposition token).
    pub(crate) in_flight: InFlightRegistry,
    /// Optional result-memory tracker.
    pub(crate) memory_tracker: Option<Arc<MemoryTracker>>,
    /// Optional poison-pill detector (strike accounting).
    pub(crate) poison_pill: Option<Arc<PoisonPillDetector>>,
    /// Optional task-checkpoint store (cleared on successful completion).
    pub(crate) checkpoints: Option<Arc<CheckpointManager>>,
    /// Liveness/readiness accounting.
    pub(crate) health: HealthChecker,
    /// Execution timeout and (optional) soft/hard time limits for this task.
    pub(crate) limits: ExecutionLimits,
    /// Effective retry budget (task request capped by the worker's setting).
    pub(crate) max_retries: u32,
    /// Effective retry strategy (delays, jitter).
    pub(crate) retry_config: RetryConfig,
    /// Maximum accepted result size in bytes (0 = unlimited).
    pub(crate) max_result_size_bytes: usize,
}

/// Outcome of driving the task future, accounting for timeout *and*
/// cooperative cancellation *and* handler panics.
enum ExecOutcome {
    /// Task ran to completion (success or task error, panics included).
    Completed(Result<Vec<u8>>),
    /// Task exceeded its timeout.
    TimedOut,
    /// Task was cancelled/revoked while running.
    Cancelled,
}

/// A terminal failure that must be observable by the caller: emits
/// `task-failed`, records a DLQ entry and (optionally) removes the message
/// from the broker.
pub(crate) struct DeadLetterRequest<'a> {
    /// The task being failed.
    pub(crate) task: &'a SerializedTask,
    /// Its id.
    pub(crate) task_id: TaskId,
    /// The delivery's receipt handle, if any.
    pub(crate) receipt_handle: Option<&'a str>,
    /// Retry attempts already spent.
    pub(crate) retry_count: u32,
    /// Human-readable failure reason.
    pub(crate) error_msg: &'a str,
    /// Machine-readable failure class (`execution_error`, `timeout`, ...).
    pub(crate) failure_type: &'a str,
    /// Extra DLQ metadata.
    pub(crate) extra_metadata: Vec<(&'a str, String)>,
    /// Whether this worker still owns the broker-side disposition.
    pub(crate) dispose: bool,
}

/// A dequeued message that did not pass signature verification.
pub(crate) struct UnverifiedMessage<'a> {
    /// The message, exactly as the broker delivered it.
    pub(crate) task: &'a SerializedTask,
    /// The delivery's receipt handle, if any.
    pub(crate) receipt_handle: Option<&'a str>,
    /// Why verification failed.
    pub(crate) error: &'a celers_core::task_signature::SignatureError,
}

/// Refuse a message that failed signature verification, before it is dispatched.
///
/// Emits a `task-rejected` event naming the cause, then disposes of the message
/// through [`dead_letter`]: recorded in the dead-letter queue when one is
/// configured, and in either case rejected **without requeue** so a forged or
/// replayed message cannot cycle straight back into the queue.
///
/// The payload is never logged or echoed into the event: an unauthenticated one
/// is attacker-controlled, and a log line is the last place it should land.
pub(crate) async fn reject_unverified<B: Broker>(
    broker: &Arc<B>,
    dlq_handler: Option<&Arc<DlqHandler>>,
    events: &EventSink,
    hostname: &str,
    pid: u32,
    req: UnverifiedMessage<'_>,
) {
    let UnverifiedMessage {
        task,
        receipt_handle,
        error,
    } = req;
    let task_id = task.metadata.id;
    let task_name = task.metadata.name.clone();

    warn!(
        "Rejecting task {} ('{}'): signature verification failed: {}",
        task_id, task_name, error
    );

    events.emit(Event::Task(TaskEvent::Rejected {
        task_id,
        task_name: Some(task_name.clone()),
        hostname: hostname.to_string(),
        timestamp: chrono::Utc::now(),
        reason: format!("Signature verification failed: {error}"),
    }));

    dead_letter(
        broker,
        dlq_handler,
        events,
        hostname,
        pid,
        DeadLetterRequest {
            task,
            task_id,
            receipt_handle,
            retry_count: spent_retries(task),
            error_msg: "Task signature verification failed",
            failure_type: "signature_verification",
            extra_metadata: vec![
                ("task_name", task_name),
                ("signature_error", error.to_string()),
            ],
            dispose: true,
        },
    )
    .await;
}

/// Emit the terminal failure signals for a task and remove it from the broker.
///
/// Shared by the execution-error, timeout, oversized-result and open-circuit
/// paths so every one of them produces the same observable terminal state
/// (event + DLQ entry + metrics), instead of silently dropping the task.
pub(crate) async fn dead_letter<B: Broker>(
    broker: &Arc<B>,
    dlq_handler: Option<&Arc<DlqHandler>>,
    events: &EventSink,
    hostname: &str,
    pid: u32,
    req: DeadLetterRequest<'_>,
) {
    let task_name = req.task.metadata.name.clone();

    events.emit(
        TaskEventBuilder::new(req.task_id, &task_name)
            .hostname(hostname)
            .pid(pid)
            .failed(req.error_msg),
    );

    if let Some(dlq) = dlq_handler {
        let mut entry = dlq::DlqEntry::new(
            req.task.clone(),
            req.task_id,
            req.retry_count,
            req.error_msg.to_string(),
            hostname.to_string(),
        )
        .with_metadata("failure_type", req.failure_type);
        for (key, value) in &req.extra_metadata {
            entry = entry.with_metadata(*key, value.clone());
        }

        if let Err(e) = dlq.add_entry(entry).await {
            warn!("Failed to add task {} to DLQ: {}", req.task_id, e);
        }
    }

    #[cfg(feature = "metrics")]
    {
        TASKS_FAILED_TOTAL.inc();
        TASKS_FAILED_BY_TYPE.with_label_values(&[&task_name]).inc();
    }

    if req.dispose {
        if let Err(e) = broker.reject(&req.task_id, req.receipt_handle, false).await {
            error!("Failed to reject task {}: {}", req.task_id, e);
        }
    }
}

/// Re-enqueue a task for another attempt with its retry state advanced.
///
/// Retry accounting is the worker's job, not the broker's: the [`Broker`]
/// contract only says `reject(requeue = true)` returns a task to the queue, and
/// the in-memory and SQS brokers do exactly that, leaving `Retrying(n)`
/// untouched — which makes a permanently failing task retry forever. Writing
/// `Retrying(n + 1)` here (and re-enqueuing explicitly) terminates the retry
/// loop on *every* broker and lets the backoff delay be honoured through the
/// broker's delayed queue instead of a hot requeue.
async fn requeue_for_retry<B: Broker>(
    broker: &Arc<B>,
    task: &SerializedTask,
    task_id: TaskId,
    receipt_handle: Option<&str>,
    next_retry: u32,
    delay: Duration,
) {
    let mut retry_task = task.clone();
    retry_task.metadata.state = TaskState::Retrying(next_retry);
    retry_task.metadata.updated_at = chrono::Utc::now();

    // Prefer broker-side scheduling (ETA / delayed queue) so the worker does
    // not hold the task — and its concurrency permit — while it waits.
    let enqueued = match schedulable_delay_secs(delay) {
        Some(secs) => {
            debug!(
                "Scheduling retry {} of task {} in {}s",
                next_retry, task_id, secs
            );
            broker.enqueue_after(retry_task, secs).await
        }
        None => {
            if !delay.is_zero() {
                debug!(
                    "Delaying retry {} of task {} by {:?}",
                    next_retry, task_id, delay
                );
                sleep(delay).await;
            }
            broker.enqueue(retry_task).await
        }
    };

    match enqueued {
        Ok(_) => {
            // The retry copy is queued; drop the original delivery so the
            // broker does not keep it in its processing list.
            if let Err(e) = broker.ack(&task_id, receipt_handle).await {
                error!(
                    "Failed to acknowledge original delivery of retried task {}: {}",
                    task_id, e
                );
            }
        }
        Err(e) => {
            warn!(
                "Failed to schedule retry for task {}: {}; falling back to broker requeue",
                task_id, e
            );
            if let Err(e) = broker.reject(&task_id, receipt_handle, true).await {
                error!("Failed to requeue task {}: {}", task_id, e);
            }
        }
    }
}

/// Retry attempts already spent by a task, as recorded in its state.
pub(crate) fn spent_retries(task: &SerializedTask) -> u32 {
    match task.metadata.state {
        TaskState::Retrying(count) => count,
        _ => 0,
    }
}

/// Run one dispatched task to a terminal disposition.
///
/// `guard` is created by the dequeue loop before spawning (paired with
/// [`WorkerStats::task_started`]) and released here, so the active-task counter
/// and the concurrency permit are returned on every exit path.
pub(crate) async fn run_dispatched_task<B: Broker + 'static>(
    dispatch: TaskDispatch<B>,
    _guard: ActiveTaskGuard,
) {
    let TaskDispatch {
        broker,
        registry,
        task,
        task_id,
        receipt_handle,
        events,
        hostname,
        pid,
        stats,
        middleware,
        dlq_handler,
        circuit_breaker,
        revocation_watcher,
        exec_context,
        in_flight,
        memory_tracker,
        poison_pill,
        checkpoints,
        health,
        limits,
        max_retries,
        retry_config,
        max_result_size_bytes,
    } = dispatch;

    let start_time = Instant::now();
    let task_name = task.metadata.name.clone();
    let current_retry = spent_retries(&task);

    // Liveness: a worker with a task in hand is processing, whatever the
    // outcome. Cleared on every exit path below.
    health.set_processing(true);

    let mut ctx = middleware::TaskContext {
        task_id: task_id.to_string(),
        task_name: task_name.clone(),
        retry_count: current_retry,
        worker_name: hostname.clone(),
        metadata: std::collections::HashMap::new(),
    };

    events.emit(
        TaskEventBuilder::new(task_id, &task_name)
            .hostname(&hostname)
            .pid(pid)
            .started(),
    );

    if let Some(ref mw) = middleware {
        if let Err(e) = mw.before_task(&mut ctx).await {
            warn!("Middleware before_task error: {}", e);
        }
    }

    // Arm the soft time limit, if one is configured. It only *warns* the running
    // task (Celery semantics): the timer trips the cooperative signal the task
    // observes through `check_soft_time_limit()`, and the task keeps running
    // until it finishes or the hard limit ends it.
    let soft_timer = arm_soft_time_limit(
        exec_context.as_ref(),
        &limits,
        start_time,
        SoftLimitReporter {
            task_id,
            task_name: &task_name,
            hostname: &hostname,
            stats: &stats,
            events: &events,
        },
    );

    let exec_outcome = drive_task(
        Arc::clone(&registry),
        Arc::clone(&task),
        task_id,
        exec_context,
        limits.deadline(),
        &stats,
    )
    .await;

    // The task is done one way or another: stop the soft-limit timer so it can
    // never fire for a task that already finished.
    if let Some(timer) = soft_timer {
        timer.abort();
    }

    // Claim the right to dispose of this delivery. A graceful-shutdown deadline
    // may already have requeued it, in which case the broker owns the message
    // now and this worker must not ack/reject it a second time.
    let claimed = in_flight.claim(&task_id);
    if !claimed {
        warn!(
            "Task {} was already requeued by shutdown; skipping broker disposition",
            task_id
        );
    }

    match exec_outcome {
        ExecOutcome::Completed(Ok(result)) => {
            let duration = start_time.elapsed();

            // Enforce the configured result-size limit before anything stores
            // or forwards the result.
            if let Err(size_error) = memory::check_result_size(&result, max_result_size_bytes) {
                error!(
                    "Task {} produced an oversized result: {}",
                    task_id, size_error
                );

                if let Some(ref mw) = middleware {
                    if let Err(e) = mw.on_error(&ctx, &size_error).await {
                        warn!("Middleware on_error error: {}", e);
                    }
                }
                if let Some(ref cb) = circuit_breaker {
                    cb.record_failure(&task_name).await;
                }
                health.record_failure();
                if let Some(ref detector) = poison_pill {
                    detector.record_failure(task_id, size_error.clone()).await;
                }

                // Retrying cannot shrink a deterministic result, so this is
                // terminal regardless of the remaining retry budget.
                dead_letter(
                    &broker,
                    dlq_handler.as_ref(),
                    &events,
                    &hostname,
                    pid,
                    DeadLetterRequest {
                        task: &task,
                        task_id,
                        receipt_handle: receipt_handle.as_deref(),
                        retry_count: current_retry,
                        error_msg: &size_error,
                        failure_type: "result_too_large",
                        extra_metadata: vec![
                            ("result_bytes", result.len().to_string()),
                            ("max_result_bytes", max_result_size_bytes.to_string()),
                        ],
                        dispose: claimed,
                    },
                )
                .await;
            } else {
                info!("Task {} completed successfully in {:?}", task_id, duration);
                debug!("Result size: {} bytes", result.len());

                if let Some(ref tracker) = memory_tracker {
                    tracker.record_task_result(result.len());
                    #[cfg(feature = "metrics")]
                    tracker.update_metrics();
                    debug!(
                        "Result memory in flight after task {}: {} bytes",
                        task_id,
                        tracker.current_usage_bytes()
                    );
                }

                // Parse result as JSON for middleware (best effort)
                let result_json = serde_json::from_slice(&result)
                    .unwrap_or(serde_json::json!({"result": "binary"}));

                if let Some(ref mw) = middleware {
                    if let Err(e) = mw.after_task(&ctx, &result_json).await {
                        warn!("Middleware after_task error: {}", e);
                    }
                }

                events.emit(
                    TaskEventBuilder::new(task_id, &task_name)
                        .hostname(&hostname)
                        .pid(pid)
                        .succeeded(duration.as_secs_f64()),
                );

                if let Some(ref cb) = circuit_breaker {
                    cb.record_success(&task_name).await;
                }
                health.record_success();
                // A task that eventually succeeds is forgiven: its accumulated
                // poison-pill strikes are cleared.
                if let Some(ref detector) = poison_pill {
                    detector.record_success(&task_id).await;
                }
                // Its resume points are dead weight now: a completed task will
                // never restart from them, and leaving them behind is how a
                // checkpoint store grows without bound.
                if let Some(ref manager) = checkpoints {
                    let removed = manager.delete_checkpoints(&task_id.to_string()).await;
                    if removed > 0 {
                        debug!(
                            "Discarded {} checkpoint(s) for completed task {}",
                            removed, task_id
                        );
                    }
                }

                #[cfg(feature = "metrics")]
                {
                    TASKS_COMPLETED_TOTAL.inc();
                    TASK_EXECUTION_TIME.observe(duration.as_secs_f64());
                    TASKS_COMPLETED_BY_TYPE
                        .with_label_values(&[&task_name])
                        .inc();
                    TASK_EXECUTION_TIME_BY_TYPE
                        .with_label_values(&[&task_name])
                        .observe(duration.as_secs_f64());
                }

                // Workflow continuation and the ack are both owned by whoever
                // holds the disposition token. `claimed == false` means the
                // shutdown drain already requeued this delivery: the task will
                // run again on redelivery and advance the workflow then, so
                // advancing it here too would enqueue every chain successor
                // twice.
                if claimed {
                    // Chain tails, branch/switch steps and chord barriers all
                    // live in the finished task's payload and metadata;
                    // advancing them is what makes an N-step chain run all N
                    // steps instead of stopping after the first. Done *before*
                    // the ack so a crash in between redelivers the task rather
                    // than silently ending the workflow.
                    #[cfg(feature = "canvas")]
                    {
                        let continued = crate::workflows::handle_workflow_completion(
                            &task,
                            &result,
                            broker.as_ref(),
                            // The worker holds no result-backend handle yet, so
                            // chord barrier counting is skipped (chain
                            // continuation and branch/switch evaluation are not).
                            #[cfg(feature = "workflows")]
                            None,
                        )
                        .await;
                        if let Err(e) = continued {
                            error!(
                                "Failed to continue the workflow after task {} succeeded: {}",
                                task_id, e
                            );
                        }
                    }

                    if let Err(e) = broker.ack(&task_id, receipt_handle.as_deref()).await {
                        error!("Failed to acknowledge task {}: {}", task_id, e);
                    }
                }

                if let Some(ref tracker) = memory_tracker {
                    tracker.release_task_result(result.len());
                    #[cfg(feature = "metrics")]
                    tracker.update_metrics();
                }
            }
        }
        ExecOutcome::Completed(Err(e)) => {
            let error_msg = e.to_string();
            error!("Task {} failed: {}", task_id, error_msg);

            // The breaker must see *every* failed execution. Recording only the
            // final, retries-exhausted failure made it trip `max_retries + 1`
            // times slower than the configured threshold.
            if let Some(ref cb) = circuit_breaker {
                cb.record_failure(&task_name).await;
            }
            health.record_failure();
            // Every failed *execution* is a poison-pill strike, whether or not
            // the retry budget will send it round again: "this task id keeps
            // failing" is exactly the signal quarantine exists for.
            if let Some(ref detector) = poison_pill {
                detector.record_failure(task_id, error_msg.clone()).await;
            }

            if current_retry < max_retries {
                warn!(
                    "Requeuing task {} for retry {}/{}",
                    task_id,
                    current_retry + 1,
                    max_retries
                );

                if let Some(ref mw) = middleware {
                    if let Err(e) = mw.on_retry(&ctx, current_retry + 1).await {
                        warn!("Middleware on_retry error: {}", e);
                    }
                }

                events.emit(
                    TaskEventBuilder::new(task_id, &task_name)
                        .hostname(&hostname)
                        .pid(pid)
                        .retried(&error_msg, current_retry + 1),
                );

                #[cfg(feature = "metrics")]
                {
                    TASKS_RETRIED_TOTAL.inc();
                    TASKS_RETRIED_BY_TYPE.with_label_values(&[&task_name]).inc();
                }

                if claimed {
                    stats.task_retried();
                    requeue_for_retry(
                        &broker,
                        &task,
                        task_id,
                        receipt_handle.as_deref(),
                        current_retry + 1,
                        backoff_delay(&retry_config, current_retry),
                    )
                    .await;
                }
            } else {
                error!(
                    "Task {} failed permanently after {} retries",
                    task_id, current_retry
                );

                if let Some(ref mw) = middleware {
                    if let Err(e) = mw.on_error(&ctx, &error_msg).await {
                        warn!("Middleware on_error error: {}", e);
                    }
                }

                dead_letter(
                    &broker,
                    dlq_handler.as_ref(),
                    &events,
                    &hostname,
                    pid,
                    DeadLetterRequest {
                        task: &task,
                        task_id,
                        receipt_handle: receipt_handle.as_deref(),
                        retry_count: current_retry,
                        error_msg: &error_msg,
                        failure_type: "execution_error",
                        extra_metadata: Vec::new(),
                        dispose: claimed,
                    },
                )
                .await;
            }
        }
        ExecOutcome::TimedOut => {
            // A hard time limit and a plain execution timeout share this path;
            // only the wording and the recorded failure class differ.
            let failure = limits.timeout_failure(task_id, start_time.elapsed());
            let error_msg = failure.message;
            error!("Task {} exceeded its deadline: {}", task_id, error_msg);

            if let Some(ref cb) = circuit_breaker {
                cb.record_failure(&task_name).await;
            }
            health.record_failure();
            if let Some(ref detector) = poison_pill {
                detector.record_failure(task_id, error_msg.clone()).await;
            }

            if current_retry < max_retries {
                if let Some(ref mw) = middleware {
                    if let Err(e) = mw.on_retry(&ctx, current_retry + 1).await {
                        warn!("Middleware on_retry error: {}", e);
                    }
                }

                events.emit(
                    TaskEventBuilder::new(task_id, &task_name)
                        .hostname(&hostname)
                        .pid(pid)
                        .retried(&error_msg, current_retry + 1),
                );

                #[cfg(feature = "metrics")]
                {
                    TASKS_RETRIED_TOTAL.inc();
                    TASKS_RETRIED_BY_TYPE.with_label_values(&[&task_name]).inc();
                }

                if claimed {
                    stats.task_retried();
                    requeue_for_retry(
                        &broker,
                        &task,
                        task_id,
                        receipt_handle.as_deref(),
                        current_retry + 1,
                        backoff_delay(&retry_config, current_retry),
                    )
                    .await;
                }
            } else {
                if let Some(ref mw) = middleware {
                    if let Err(e) = mw.on_error(&ctx, &error_msg).await {
                        warn!("Middleware on_error error: {}", e);
                    }
                }

                dead_letter(
                    &broker,
                    dlq_handler.as_ref(),
                    &events,
                    &hostname,
                    pid,
                    DeadLetterRequest {
                        task: &task,
                        task_id,
                        receipt_handle: receipt_handle.as_deref(),
                        retry_count: current_retry,
                        error_msg: &error_msg,
                        failure_type: failure.failure_type,
                        extra_metadata: failure.metadata,
                        dispose: claimed,
                    },
                )
                .await;
            }
        }
        ExecOutcome::Cancelled => {
            // Task was revoked while running: transition to Revoked and stop.
            // The work is abandoned (the inner execution task was aborted); a
            // deliberately revoked task is never retried.
            let duration = start_time.elapsed();
            info!(
                "Task {} revoked after {:?}, transitioning to Revoked",
                task_id, duration
            );

            if let Some(ref mw) = middleware {
                if let Err(e) = mw.on_error(&ctx, "Task revoked during execution").await {
                    warn!("Middleware on_error error: {}", e);
                }
            }

            // A revoked task produced neither a success nor a failure, so the
            // breaker learns nothing from it — but it may have been admitted on
            // a half-open trial slot, which has to go back.
            if let Some(ref cb) = circuit_breaker {
                cb.release_probe(&task_name).await;
            }

            events.emit(Event::Task(TaskEvent::Revoked {
                task_id,
                task_name: Some(task_name.clone()),
                timestamp: chrono::Utc::now(),
                terminated: true,
                signum: None,
                expired: false,
            }));

            stats.task_revoked();

            // Acknowledge so the broker removes the task (it must not be
            // redelivered to run again).
            if claimed {
                if let Err(e) = broker.ack(&task_id, receipt_handle.as_deref()).await {
                    error!("Failed to acknowledge revoked task {}: {}", task_id, e);
                }
            }
        }
    }

    // Clean up the in-flight cancellation token (if any). Panics inside the
    // handler are converted to errors above, so this always runs.
    if let Some(ref watcher) = revocation_watcher {
        watcher.unregister(&task_id).await;
    }

    // `stats.active()` still counts *this* task (its guard drops below), so
    // "someone else is still working" is `> 1`. Without the comparison, one
    // task finishing would report a busy worker as idle.
    health.set_processing(stats.active() > 1);

    // `_guard` drops here: active-count decremented, concurrency permit
    // released — including on an unwind past this point.
}

/// Who a soft-limit expiry is reported to.
///
/// Bundled so [`arm_soft_time_limit`] keeps a readable signature: the limit and
/// the reporting targets are two separate concerns.
pub(crate) struct SoftLimitReporter<'a> {
    /// The task whose limit is being armed.
    pub(crate) task_id: TaskId,
    /// The task's registered name.
    pub(crate) task_name: &'a str,
    /// The worker publishing the event.
    pub(crate) hostname: &'a str,
    /// Counters the expiry is recorded in.
    pub(crate) stats: &'a Arc<WorkerStats>,
    /// Lifecycle event sink the `task-soft-time-limit-exceeded` event goes to.
    pub(crate) events: &'a EventSink,
}

/// Arm the task's **soft** time limit, if one is configured.
///
/// Returns the timer's [`JoinHandle`](tokio::task::JoinHandle) so the caller can
/// abort it the moment the task finishes. When it fires it trips the task's
/// [`SoftTimeout`](crate::execution_context::SoftTimeout) — which a cooperative
/// handler observes through
/// [`check_soft_time_limit`](crate::execution_context::check_soft_time_limit) —
/// counts the expiry in [`WorkerStats`], logs a warning and publishes a
/// `task-soft-time-limit-exceeded` event so a monitor sees the breach instead of
/// having to infer it from a later hard-limit failure. It deliberately does
/// **not** touch the cancellation token: tripping that would make the worker
/// treat the task as revoked (acked, never retried), whereas a soft-limit expiry
/// leaves the task running until it finishes or hits its hard limit.
fn arm_soft_time_limit(
    exec_context: Option<&TaskExecutionContext>,
    limits: &ExecutionLimits,
    start_time: Instant,
    reporter: SoftLimitReporter<'_>,
) -> Option<tokio::task::JoinHandle<()>> {
    let soft_limit = limits.soft_limit?;
    let signal = exec_context?.soft_timeout().clone();
    if !signal.is_configured() {
        // The context was built without the soft limit (nothing to trip).
        return None;
    }

    let SoftLimitReporter {
        task_id,
        task_name,
        hostname,
        stats,
        events,
    } = reporter;
    let stats = Arc::clone(stats);
    let task_name = task_name.to_string();
    let events = events.clone();
    let hostname = hostname.to_string();
    Some(tokio::spawn(async move {
        sleep(soft_limit).await;
        let elapsed = start_time.elapsed();
        // `expire` returns false when the signal had already tripped, so the
        // event is published exactly once per task.
        if signal.expire(elapsed) {
            stats.task_soft_timeout();
            warn!(
                "Soft time limit of {:?} exceeded for task {} ('{}'); the task may wrap up \
                 cooperatively until its hard limit",
                soft_limit, task_id, task_name
            );
            events.emit(
                TaskEventBuilder::new(task_id, &task_name)
                    .hostname(&hostname)
                    .soft_time_limit_exceeded(elapsed, soft_limit),
            );
        }
    }))
}

/// Drive the handler future, bounded by the timeout and racing the task's
/// cancellation token.
///
/// The handler runs in its own `tokio::spawn`ed task so a panic inside it is
/// captured as a [`JoinError`](tokio::task::JoinError) instead of unwinding the
/// worker's own future (which would skip every ack/reject, leak the active
/// count and strand the message in the broker's processing list forever).
/// Timeout and cancellation both `abort()` that task so the handler cannot keep
/// running detached.
async fn drive_task(
    registry: Arc<TaskRegistry>,
    task: Arc<SerializedTask>,
    task_id: TaskId,
    exec_context: Option<TaskExecutionContext>,
    deadline: Duration,
    stats: &Arc<WorkerStats>,
) -> ExecOutcome {
    let scoped_context = exec_context.clone();
    let mut handle = tokio::spawn(async move {
        match scoped_context {
            Some(context) => {
                let registry = Arc::clone(&registry);
                let task = Arc::clone(&task);
                context
                    .scope(async move { registry.execute(&task).await })
                    .await
            }
            None => registry.execute(&task).await,
        }
    });

    let joined = match exec_context.as_ref().map(|c| c.token().clone()) {
        Some(token) => {
            timeout(deadline, async {
                tokio::select! {
                    biased;
                    // Already cancelled (or cancelled mid-flight): stop now.
                    () = token.cancelled() => None,
                    joined = &mut handle => Some(joined),
                }
            })
            .await
        }
        None => timeout(deadline, &mut handle).await.map(Some),
    };

    match joined {
        Ok(Some(Ok(result))) => ExecOutcome::Completed(result),
        Ok(Some(Err(join_error))) => {
            if join_error.is_panic() {
                let payload = join_error.into_panic();
                let message = panic_message(payload.as_ref());
                stats.task_panicked();
                error!("Task {} panicked: {}", task_id, message);
                ExecOutcome::Completed(Err(CelersError::TaskExecution(format!(
                    "task panicked: {message}"
                ))))
            } else {
                ExecOutcome::Completed(Err(CelersError::TaskExecution(
                    "task was aborted before completion".to_string(),
                )))
            }
        }
        Ok(None) => {
            handle.abort();
            ExecOutcome::Cancelled
        }
        Err(_elapsed) => {
            handle.abort();
            ExecOutcome::TimedOut
        }
    }
}
