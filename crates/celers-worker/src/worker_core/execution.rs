//! Per-task execution: drives one dequeued message to a terminal disposition.
//!
//! Split out of [`worker_core`](crate::worker_core) so the dequeue loop stays
//! readable. Everything here runs inside the task's own spawned future.

use crate::circuit_breaker::CircuitBreaker;
use crate::dlq::{self, DlqHandler};
use crate::execution_context::{RevocationWatcher, TaskExecutionContext};
use crate::memory::{self, MemoryTracker};
use crate::middleware;
use crate::retry::RetryConfig;
use crate::types::WorkerStats;

use super::support::{
    backoff_delay, panic_message, schedulable_delay_secs, ActiveTaskGuard, EventSink,
    InFlightRegistry,
};

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
    /// Execution timeout for this task.
    pub(crate) timeout_secs: u64,
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
        timeout_secs,
        max_retries,
        retry_config,
        max_result_size_bytes,
    } = dispatch;

    let start_time = Instant::now();
    let task_name = task.metadata.name.clone();
    let current_retry = spent_retries(&task);

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

    let exec_outcome = drive_task(
        Arc::clone(&registry),
        Arc::clone(&task),
        task_id,
        exec_context,
        timeout_secs,
        &stats,
    )
    .await;

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

                if claimed {
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
            let error_msg = format!("Task timed out after {}s", timeout_secs);
            error!("Task {} timed out after {}s", task_id, timeout_secs);

            if let Some(ref cb) = circuit_breaker {
                cb.record_failure(&task_name).await;
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
                        failure_type: "timeout",
                        extra_metadata: vec![("timeout_secs", timeout_secs.to_string())],
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

    // `_guard` drops here: active-count decremented, concurrency permit
    // released — including on an unwind past this point.
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
    timeout_secs: u64,
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

    let deadline = Duration::from_secs(timeout_secs);
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
