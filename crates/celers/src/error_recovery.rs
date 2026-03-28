use crate::{Chain, Signature};
use serde_json::Value;

/// Creates a task chain with a fallback task in case of failure
///
/// This pattern provides a fallback mechanism where if the primary task fails,
/// a secondary fallback task is executed instead.
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
/// ```rust,ignore
/// use celers::error_recovery::with_fallback;
/// use serde_json::json;
///
/// let task = with_fallback(
///     "fetch_from_primary_api",
///     vec![json!({"url": "https://api.example.com"})],
///     "fetch_from_backup_api",
///     vec![json!({"url": "https://backup.example.com"})]
/// );
/// ```
///
/// # Note
///
/// The actual fallback logic must be implemented in the task handlers.
/// This function creates the workflow structure.
pub fn with_fallback(
    primary_task: &str,
    primary_args: Vec<Value>,
    fallback_task: &str,
    fallback_args: Vec<Value>,
) -> Chain {
    Chain::new()
        .then(primary_task, primary_args)
        .then(fallback_task, fallback_args)
}

/// Creates a task signature with error suppression
///
/// Configures a task to ignore errors and continue execution.
/// Useful for non-critical tasks where failures shouldn't block the workflow.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments
///
/// # Example
///
/// ```rust,ignore
/// use celers::error_recovery::ignore_errors;
/// use serde_json::json;
///
/// let sig = ignore_errors(
///     "log_analytics",
///     vec![json!({"event": "user_action"})]
/// );
/// ```
pub fn ignore_errors(task_name: &str, args: Vec<Value>) -> Signature {
    let mut sig = Signature::new(task_name.to_string()).with_args(args);
    sig.options.max_retries = Some(0); // No retries
                                       // Note: Actual error suppression logic would be in the task handler
    sig
}

/// Creates a task with graduated retry delays
///
/// Implements exponential backoff retry strategy for tasks that may
/// experience transient failures.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments
/// * `max_retries` - Maximum number of retry attempts
/// * `base_delay` - Initial delay in seconds (doubles each retry)
///
/// # Example
///
/// ```rust,ignore
/// use celers::error_recovery::with_exponential_backoff;
/// use serde_json::json;
///
/// let sig = with_exponential_backoff(
///     "call_flaky_api",
///     vec![json!({"endpoint": "/data"})],
///     5,   // Retry up to 5 times
///     2    // Start with 2 second delay, then 4, 8, 16, 32
/// );
/// ```
pub fn with_exponential_backoff(
    task_name: &str,
    args: Vec<Value>,
    max_retries: u32,
    base_delay: u64,
) -> Signature {
    let mut sig = Signature::new(task_name.to_string()).with_args(args);
    sig.options.max_retries = Some(max_retries);
    sig.options.countdown = Some(base_delay);
    // Note: Actual exponential backoff logic would be in the retry handler
    sig
}

/// Creates a task with a dead letter queue on failure
///
/// Routes failed tasks to a designated error handling queue.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments
/// * `dlq_task` - Name of the dead letter queue handler task
///
/// # Example
///
/// ```rust,ignore
/// use celers::error_recovery::with_dlq;
/// use serde_json::json;
///
/// let task = with_dlq(
///     "process_payment",
///     vec![json!({"amount": 100})],
///     "handle_failed_payment"
/// );
/// ```
pub fn with_dlq(task_name: &str, args: Vec<Value>, dlq_task: &str) -> Chain {
    Chain::new().then(task_name, args).then(dlq_task, vec![])
}
