//! Failure-handling helpers: fallbacks, dead-letter routing, retry backoff and
//! error suppression.
//!
//! All four are expressed with **error links** rather than by appending the
//! handler to a chain. That distinction is the whole point: a handler attached
//! with `.then()` runs on every success, which is the opposite of what a
//! fallback or a dead-letter handler is for. An error link runs only when the
//! task fails for good — retries exhausted, or no retry budget at all — and the
//! handler receives a JSON descriptor of the failure as its first argument:
//!
//! ```json
//! {"task_id": "…", "task": "fetch_from_primary_api",
//!  "error": "connection refused", "failure_type": "execution_error"}
//! ```
//!
//! The worker side lives in [`celers_worker::error_links`].

use crate::Signature;
use serde_json::Value;

/// Default multiplier for [`with_exponential_backoff`]: each retry waits twice
/// as long as the one before it.
const EXPONENTIAL_BACKOFF_FACTOR: f64 = 2.0;

/// Default ceiling for a single [`with_exponential_backoff`] delay: one hour.
///
/// Without a cap, `base * 2^n` reaches days within a dozen retries and then
/// overflows. Matching `TaskOptions::calculate_retry_delay`'s own default keeps
/// producer and worker in agreement about the schedule.
const EXPONENTIAL_BACKOFF_MAX_DELAY_SECS: u64 = 3_600;

/// Creates a task that falls back to another task **only if it fails**.
///
/// The fallback is attached as an error link, so on success it does not run at
/// all. On failure it is enqueued with the failure descriptor as its first
/// positional argument.
///
/// # Arguments
///
/// * `primary_task` - Name of the primary task to execute
/// * `primary_args` - Arguments for the primary task
/// * `fallback_task` - Name of the fallback task to execute on failure
/// * `fallback_args` - Arguments for the fallback task
///
/// # Example
///
/// ```
/// use celers::error_recovery::with_fallback;
/// use serde_json::json;
///
/// let task = with_fallback(
///     "fetch_from_primary_api",
///     vec![json!({"url": "https://api.example.com"})],
///     "fetch_from_backup_api",
///     vec![json!({"url": "https://backup.example.com"})],
/// );
///
/// assert_eq!(task.task, "fetch_from_primary_api");
/// // The backup is a *failure* callback, not a following step.
/// let handlers = task.options.all_link_errors();
/// assert_eq!(handlers.len(), 1);
/// assert_eq!(handlers[0].task, "fetch_from_backup_api");
/// ```
pub fn with_fallback(
    primary_task: &str,
    primary_args: Vec<Value>,
    fallback_task: &str,
    fallback_args: Vec<Value>,
) -> Signature {
    Signature::new(primary_task.to_string())
        .with_args(primary_args)
        .with_link_error(Signature::new(fallback_task.to_string()).with_args(fallback_args))
}

/// Creates a task whose failures are suppressed.
///
/// A task built this way is dispatched with canvas's
/// [`TaskOptions::ignore_errors`](celers_canvas::TaskOptions::ignore_errors)
/// flag, which the worker honours by **not** retrying it, **not**
/// dead-lettering it, recording an
/// [`Ignored`](celers_core::TaskResultValue::Ignored) result carrying the
/// suppressed error, and letting the surrounding workflow continue with a
/// `null` result in its place.
///
/// That last part is what makes it useful: a failed analytics ping must not
/// strand the chain behind it. (Setting `max_retries = 0`, which is all this
/// used to do, suppresses nothing — the task still fails, still dead-letters,
/// and still ends its chain.)
///
/// `max_retries` is pinned to zero as well, so the intent is legible on the
/// wire even to a consumer that does not implement suppression.
///
/// # What is suppressed, and what is not
///
/// Suppression covers everything that is an outcome of *running* the task: the
/// handler returning an error, the handler panicking, the attempt exceeding its
/// time limit, and the attempt producing an oversized result. It does not cover
/// the worker declining to run the task at all — an open circuit breaker, a
/// poison-pill quarantine, a failed signature check or a revocation still take
/// the ordinary terminal path and record a
/// [`Failure`](celers_core::TaskResultValue::Failure). Those are decisions
/// about the fleet or about the message, not about this task's outcome, so a
/// producer flag must not be able to wave them away.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments
///
/// # Example
///
/// ```
/// use celers::error_recovery::ignore_errors;
/// use serde_json::json;
///
/// let sig = ignore_errors("log_analytics", vec![json!({"event": "user_action"})]);
///
/// assert!(sig.options.ignore_errors);
/// assert_eq!(sig.options.max_retries, Some(0));
/// ```
pub fn ignore_errors(task_name: &str, args: Vec<Value>) -> Signature {
    let mut sig = Signature::new(task_name.to_string())
        .with_args(args)
        .ignoring_errors();
    // Retrying a task whose failures are ignored buys nothing; the worker skips
    // retries for it regardless, and stating it here keeps the two consistent.
    sig.options.max_retries = Some(0);
    sig
}

/// Creates a task with a real, growing exponential retry backoff.
///
/// The n-th retry waits `base_delay * 2^n` seconds — 2, 4, 8, 16, 32 for a
/// `base_delay` of 2 — capped at one hour. The worker reads this schedule off
/// the task's payload and uses it in place of its own configured retry strategy
/// (see [`celers_worker::error_links::TaskRetryPolicy`]), so it is the delay
/// actually applied rather than a hint.
///
/// A single `countdown`, which is what this used to set, is not a backoff at
/// all: it delays the *first* dispatch once and leaves every retry immediate.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments
/// * `max_retries` - Maximum number of retry attempts
/// * `base_delay` - Delay before the first retry, in seconds (doubles each retry)
///
/// # Example
///
/// ```
/// use celers::error_recovery::with_exponential_backoff;
/// use serde_json::json;
///
/// let sig = with_exponential_backoff(
///     "call_flaky_api",
///     vec![json!({"endpoint": "/data"})],
///     5, // Retry up to 5 times
///     2, // 2 seconds, then 4, 8, 16, 32
/// );
///
/// assert_eq!(sig.options.max_retries, Some(5));
/// assert_eq!(sig.options.retry_delay, Some(2));
/// assert_eq!(sig.options.retry_backoff, Some(2.0));
/// // The schedule the worker will apply.
/// assert_eq!(sig.options.calculate_retry_delay(0), 2);
/// assert_eq!(sig.options.calculate_retry_delay(1), 4);
/// assert_eq!(sig.options.calculate_retry_delay(4), 32);
/// ```
pub fn with_exponential_backoff(
    task_name: &str,
    args: Vec<Value>,
    max_retries: u32,
    base_delay: u64,
) -> Signature {
    Signature::new(task_name.to_string())
        .with_args(args)
        .with_retries(max_retries)
        .with_retry_delay(base_delay)
        .with_retry_backoff(EXPONENTIAL_BACKOFF_FACTOR)
        .with_retry_backoff_max(EXPONENTIAL_BACKOFF_MAX_DELAY_SECS)
}

/// Creates a task that routes to a dead-letter handler **only on failure**.
///
/// The handler is attached as an error link, so a successful task never
/// triggers it. On terminal failure it is enqueued with the failure descriptor
/// as its first positional argument, which is what a DLQ handler needs in order
/// to record or replay the failure.
///
/// This is a task-level route and is independent of the worker's own
/// [`DlqConfig`](celers_worker::DlqConfig): both can be in use at once, and a
/// failed task then produces a dead-letter *entry* and runs this handler.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments
/// * `dlq_task` - Name of the dead letter queue handler task
///
/// # Example
///
/// ```
/// use celers::error_recovery::with_dlq;
/// use serde_json::json;
///
/// let task = with_dlq("process_payment", vec![json!({"amount": 100})], "handle_failed_payment");
///
/// assert_eq!(task.task, "process_payment");
/// let handlers = task.options.all_link_errors();
/// assert_eq!(handlers.len(), 1);
/// assert_eq!(handlers[0].task, "handle_failed_payment");
/// ```
pub fn with_dlq(task_name: &str, args: Vec<Value>, dlq_task: &str) -> Signature {
    Signature::new(task_name.to_string())
        .with_args(args)
        .with_link_error(Signature::new(dlq_task.to_string()))
}
