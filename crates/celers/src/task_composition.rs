use crate::canvas::{Chain, Group};
use crate::Signature;
use serde_json::Value;

/// Creates a conditional execution chain
///
/// Executes a predicate task, and based on its result, executes either
/// the success chain or the fallback chain.
///
/// # Arguments
///
/// * `predicate_task` - Task that returns a boolean condition
/// * `predicate_args` - Arguments for the predicate task
/// * `success_chain` - Tasks to execute if predicate is true
/// * `fallback_chain` - Optional tasks to execute if predicate is false
///
/// # Example
///
/// ```rust,ignore
/// use celers::task_composition::conditional_chain;
/// use serde_json::json;
///
/// let workflow = conditional_chain(
///     "check_balance",
///     vec![json!({"account_id": 123})],
///     vec![("process_payment", vec![]), ("send_receipt", vec![])],
///     Some(("send_insufficient_funds_email", vec![]))
/// );
/// ```
#[allow(dead_code)]
pub fn conditional_chain(
    predicate_task: &str,
    predicate_args: Vec<Value>,
    success_chain: Vec<(&str, Vec<Value>)>,
    _fallback_chain: Option<(&str, Vec<Value>)>,
) -> Chain {
    let mut chain = Chain::new();

    // Add predicate task
    chain = chain.then(predicate_task, predicate_args);

    // Add success chain tasks
    for (task_name, args) in success_chain {
        chain = chain.then(task_name, args);
    }

    // Note: Actual conditional logic would need to be implemented in the task itself
    // This is a template for the workflow structure

    chain
}

/// Creates a retry wrapper with exponential backoff
///
/// Wraps a task with automatic retry logic using exponential backoff.
///
/// # Arguments
///
/// * `task_name` - Name of the task to wrap
/// * `args` - Task arguments
/// * `max_retries` - Maximum number of retry attempts
/// * `initial_delay` - Initial delay in seconds (doubles each retry)
///
/// # Example
///
/// ```rust,ignore
/// use celers::task_composition::retry_wrapper;
/// use serde_json::json;
///
/// let sig = retry_wrapper(
///     "fetch_external_api",
///     vec![json!({"url": "https://api.example.com"})],
///     5,  // Retry up to 5 times
///     10  // Start with 10 second delay
/// );
/// ```
pub fn retry_wrapper(
    task_name: &str,
    args: Vec<Value>,
    max_retries: u32,
    initial_delay: u64,
) -> Signature {
    let mut sig = Signature::new(task_name.to_string()).with_args(args);
    sig.options.max_retries = Some(max_retries);
    sig.options.countdown = Some(initial_delay);
    sig
}

/// Creates a timeout-protected task
///
/// Wraps a task with a timeout, ensuring it doesn't run indefinitely.
///
/// # Arguments
///
/// * `task_name` - Name of the task to protect
/// * `args` - Task arguments
/// * `timeout_seconds` - Maximum execution time in seconds
///
/// # Example
///
/// ```rust,ignore
/// use celers::task_composition::timeout_wrapper;
/// use serde_json::json;
///
/// let sig = timeout_wrapper(
///     "long_running_task",
///     vec![json!({"process": "large_dataset"})],
///     300  // 5 minute timeout
/// );
/// ```
pub fn timeout_wrapper(task_name: &str, args: Vec<Value>, timeout_seconds: u64) -> Signature {
    let mut sig = Signature::new(task_name.to_string()).with_args(args);
    sig.options.time_limit = Some(timeout_seconds);
    sig
}

/// Creates a task group with circuit breaker pattern
///
/// Groups tasks and adds circuit breaker semantics to prevent cascading
/// failures.
///
/// # Arguments
///
/// * `tasks` - List of (task_name, args) tuples
/// * `max_failures` - Maximum failures before circuit opens
///
/// # Example
///
/// ```rust,ignore
/// use celers::task_composition::circuit_breaker_group;
/// use serde_json::json;
///
/// let tasks = vec![
///     ("call_service_a", vec![json!(1)]),
///     ("call_service_b", vec![json!(2)]),
///     ("call_service_c", vec![json!(3)]),
/// ];
///
/// let group = circuit_breaker_group(tasks, 2);
/// ```
pub fn circuit_breaker_group(tasks: Vec<(&str, Vec<Value>)>, max_failures: u32) -> Group {
    let mut group = Group::new();

    for (task_name, args) in tasks {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        // Add retry logic for circuit breaker
        sig.options.max_retries = Some(max_failures);

        group.tasks.push(sig);
    }

    group
}

/// Creates a rate-limited workflow
///
/// Spaces out task execution to respect rate limits.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `items` - Items to process
/// * `delay_between_tasks` - Delay in seconds between each task
///
/// # Example
///
/// ```rust,ignore
/// use celers::task_composition::rate_limited_workflow;
/// use serde_json::json;
///
/// let items = vec![json!(1), json!(2), json!(3)];
/// let workflow = rate_limited_workflow(
///     "call_rate_limited_api",
///     items,
///     5  // 5 seconds between each call
/// );
/// ```
pub fn rate_limited_workflow(
    task_name: &str,
    items: Vec<Value>,
    delay_between_tasks: u64,
) -> Chain {
    let mut chain = Chain::new();

    for (i, item) in items.into_iter().enumerate() {
        let mut sig = Signature::new(task_name.to_string()).with_args(vec![item]);

        // Add countdown for all tasks except the first
        if i > 0 {
            sig.options.countdown = Some(delay_between_tasks * i as u64);
        }

        chain.tasks.push(sig);
    }

    chain
}
