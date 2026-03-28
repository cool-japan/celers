/// Create a task signature with fluent API
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::task;
///
/// let sig = task("my_task")
///     .with_args(vec![json!(1), json!(2)])
///     .with_priority(5)
///     .with_max_retries(3);
/// ```
pub fn task(name: impl Into<String>) -> crate::Signature {
    crate::Signature::new(name.into())
}

/// Create a chain workflow
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::chain;
///
/// let workflow = chain()
///     .add("task1", vec![json!(1)])
///     .add("task2", vec![json!(2)]);
/// ```
pub fn chain() -> crate::Chain {
    crate::Chain::new()
}

/// Create a group workflow
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::group;
///
/// let workflow = group()
///     .add("task1", vec![json!(1)])
///     .add("task2", vec![json!(2)]);
/// ```
pub fn group() -> crate::Group {
    crate::Group::new()
}

/// Create a chord workflow with header and callback
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::{group, task, chord};
///
/// let header = group()
///     .add("task1", vec![json!(1)])
///     .add("task2", vec![json!(2)]);
///
/// let callback = task("aggregate_results");
///
/// let workflow = chord(header, callback);
/// ```
pub fn chord(header: crate::Group, callback: crate::Signature) -> crate::Chord {
    crate::Chord::new(header, callback)
}

/// Create a chunks workflow for processing items in batches
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::chunks;
/// use serde_json::json;
///
/// // Process items in chunks of 10
/// let items = vec![json!(1), json!(2), json!(3)];
/// let workflow = chunks("process_item", items, 10);
/// ```
pub fn chunks<T: serde::Serialize>(
    task_name: impl Into<String>,
    items: Vec<T>,
    chunk_size: usize,
) -> crate::Chunks {
    let sig = crate::Signature::new(task_name.into());
    let serialized_items: Vec<serde_json::Value> = items
        .into_iter()
        .filter_map(|item| serde_json::to_value(item).ok())
        .collect();
    crate::Chunks::new(sig, serialized_items, chunk_size)
}

/// Create a map workflow for applying a task to each item
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::map;
/// use serde_json::json;
///
/// let items = vec![json!(1), json!(2), json!(3)];
/// let workflow = map("square", items);
/// ```
pub fn map<T: serde::Serialize>(task_name: impl Into<String>, items: Vec<T>) -> crate::Map {
    let sig = crate::Signature::new(task_name.into());
    let serialized_items: Vec<Vec<serde_json::Value>> = items
        .into_iter()
        .filter_map(|item| serde_json::to_value(item).ok().map(|v| vec![v]))
        .collect();
    crate::Map::new(sig, serialized_items)
}

/// Create a starmap workflow for applying a task with multiple arguments
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::starmap;
/// use serde_json::json;
///
/// let args = vec![
///     vec![json!(1), json!(2)],
///     vec![json!(3), json!(4)],
/// ];
/// let workflow = starmap("add", args);
/// ```
pub fn starmap<T: serde::Serialize>(
    task_name: impl Into<String>,
    args: Vec<Vec<T>>,
) -> crate::Starmap {
    let sig = crate::Signature::new(task_name.into());
    let serialized_args: Vec<Vec<serde_json::Value>> = args
        .into_iter()
        .map(|arg_list| {
            arg_list
                .into_iter()
                .filter_map(|item| serde_json::to_value(item).ok())
                .collect()
        })
        .collect();
    crate::Starmap::new(sig, serialized_args)
}

/// Create task options for configuring task execution
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::options;
///
/// let opts = options()
///     .max_retries(3)
///     .countdown(60)
///     .priority(5);
/// ```
pub fn options() -> crate::TaskOptions {
    crate::TaskOptions::default()
}

/// Create task options with retry configuration
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::with_retry;
///
/// let opts = with_retry(5, 60);  // 5 retries with 60s delay
/// ```
pub fn with_retry(max_retries: u32, retry_delay_secs: u64) -> crate::TaskOptions {
    crate::TaskOptions {
        max_retries: Some(max_retries),
        countdown: Some(retry_delay_secs),
        ..Default::default()
    }
}

/// Create task options with timeout configuration
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::with_timeout;
///
/// let opts = with_timeout(300);  // 5 minute timeout
/// ```
pub fn with_timeout(timeout_secs: u64) -> crate::TaskOptions {
    crate::TaskOptions {
        time_limit: Some(timeout_secs),
        ..Default::default()
    }
}

/// Create task options with priority
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::with_priority;
///
/// let opts = with_priority(9);  // High priority task
/// ```
pub fn with_priority(priority: u8) -> crate::TaskOptions {
    crate::TaskOptions {
        priority: Some(priority),
        ..Default::default()
    }
}

/// Create task options with countdown (delay in seconds)
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::with_countdown;
///
/// let opts = with_countdown(60);  // Run after 60 seconds
/// ```
pub fn with_countdown(countdown_secs: u64) -> crate::TaskOptions {
    crate::TaskOptions {
        countdown: Some(countdown_secs),
        ..Default::default()
    }
}

/// Create task options with expires (expiration time in seconds)
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::with_expires;
///
/// let opts = with_expires(7200);  // Expire after 2 hours
/// ```
pub fn with_expires(expires_secs: u64) -> crate::TaskOptions {
    crate::TaskOptions {
        expires: Some(expires_secs),
        ..Default::default()
    }
}

/// Create a batch of task signatures from a collection
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::batch;
/// use serde_json::json;
///
/// let items = vec![
///     vec![json!(1), json!(2)],
///     vec![json!(3), json!(4)],
/// ];
/// let tasks = batch("add", items);
/// ```
pub fn batch<T: serde::Serialize>(
    task_name: impl Into<String>,
    args_list: Vec<Vec<T>>,
) -> Vec<crate::Signature> {
    let task_name = task_name.into();
    args_list
        .into_iter()
        .map(|args| {
            let sig = crate::Signature::new(task_name.clone());
            let serialized_args: Vec<serde_json::Value> = args
                .into_iter()
                .filter_map(|arg| serde_json::to_value(arg).ok())
                .collect();
            sig.with_args(serialized_args)
        })
        .collect()
}

/// Create a chain from a list of task names and their arguments
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::chain_from;
/// use serde_json::json;
///
/// let tasks = vec![
///     ("task1", vec![json!(1), json!(2)]),
///     ("task2", vec![json!(3), json!(4)]),
/// ];
/// let workflow = chain_from(tasks);
/// ```
pub fn chain_from<T: serde::Serialize>(tasks: Vec<(&str, Vec<T>)>) -> crate::Chain {
    let mut chain = crate::Chain::new();
    for (task_name, args) in tasks {
        let serialized_args: Vec<serde_json::Value> = args
            .into_iter()
            .filter_map(|arg| serde_json::to_value(arg).ok())
            .collect();
        chain = chain.then(task_name, serialized_args);
    }
    chain
}

/// Create a group from a list of task names and their arguments
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::group_from;
/// use serde_json::json;
///
/// let tasks = vec![
///     ("task1", vec![json!(1)]),
///     ("task2", vec![json!(2)]),
///     ("task3", vec![json!(3)]),
/// ];
/// let workflow = group_from(tasks);
/// ```
pub fn group_from<T: serde::Serialize>(tasks: Vec<(&str, Vec<T>)>) -> crate::Group {
    let mut group = crate::Group::new();
    for (task_name, args) in tasks {
        let serialized_args: Vec<serde_json::Value> = args
            .into_iter()
            .filter_map(|arg| serde_json::to_value(arg).ok())
            .collect();
        group = group.add(task_name, serialized_args);
    }
    group
}

/// Create a signature with common task options applied
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::task_with_options;
/// use serde_json::json;
///
/// let sig = task_with_options(
///     "my_task",
///     vec![json!(1), json!(2)],
///     3,  // max_retries
///     5   // priority
/// );
/// ```
pub fn task_with_options(
    name: impl Into<String>,
    args: Vec<serde_json::Value>,
    max_retries: u32,
    priority: u8,
) -> crate::Signature {
    let mut sig = crate::Signature::new(name.into()).with_args(args);
    sig.options.max_retries = Some(max_retries);
    sig.options.priority = Some(priority);
    sig
}

/// Create a recurring task using crontab components (requires beat and beat-cron features)
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::recurring;
///
/// // Run every day at midnight
/// let schedule = recurring("cleanup_task", "0", "0", "*", "*", "*");
/// ```
#[cfg(all(feature = "beat", feature = "beat-cron"))]
pub fn recurring(
    task_name: impl Into<String>,
    minute: &str,
    hour: &str,
    day_of_week: &str,
    day_of_month: &str,
    month_of_year: &str,
) -> crate::ScheduledTask {
    crate::ScheduledTask::new(
        task_name.into(),
        crate::Schedule::crontab(minute, hour, day_of_week, day_of_month, month_of_year),
    )
}

/// Create a recurring task with an interval schedule
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::recurring_interval;
///
/// // Execute every 60 seconds
/// let task = recurring_interval("process_data", 60);
/// ```
#[cfg(feature = "beat")]
pub fn recurring_interval(task_name: impl Into<String>, seconds: u64) -> crate::ScheduledTask {
    crate::ScheduledTask::new(task_name.into(), crate::Schedule::interval(seconds))
}

/// Create a delayed task (delayed execution by specified seconds)
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::delay;
/// use serde_json::json;
///
/// // Execute after 60 seconds
/// let sig = delay("send_email", vec![json!("user@example.com")], 60);
/// ```
pub fn delay(
    task_name: impl Into<String>,
    args: Vec<serde_json::Value>,
    delay_secs: u64,
) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.countdown = Some(delay_secs);
    sig
}

/// Create a task with expiration time
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::expire_in;
/// use serde_json::json;
///
/// // Expire after 2 hours
/// let sig = expire_in("process_data", vec![json!({"id": 123})], 7200);
/// ```
pub fn expire_in(
    task_name: impl Into<String>,
    args: Vec<serde_json::Value>,
    expires_secs: u64,
) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.expires = Some(expires_secs);
    sig
}

/// Create a high priority task (priority level 9)
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::high_priority;
/// use serde_json::json;
///
/// let sig = high_priority("urgent_task", vec![json!({"alert": true})]);
/// ```
pub fn high_priority(
    task_name: impl Into<String>,
    args: Vec<serde_json::Value>,
) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.priority = Some(9);
    sig
}

/// Create a low priority task (priority level 1)
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::low_priority;
/// use serde_json::json;
///
/// let sig = low_priority("background_cleanup", vec![json!({})]);
/// ```
pub fn low_priority(
    task_name: impl Into<String>,
    args: Vec<serde_json::Value>,
) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.priority = Some(1);
    sig
}

/// Create a parallel workflow (alias for group)
///
/// This is a more intuitive name for the group workflow pattern.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::parallel;
///
/// let workflow = parallel()
///     .add("task1", vec![json!(1)])
///     .add("task2", vec![json!(2)])
///     .add("task3", vec![json!(3)]);
/// ```
pub fn parallel() -> crate::Group {
    crate::Group::new()
}

/// Create a critical task (high priority with maximum retries)
///
/// Critical tasks are executed with priority 9 and retry up to 5 times.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::critical;
/// use serde_json::json;
///
/// let sig = critical("process_payment", vec![json!({"amount": 100})]);
/// ```
pub fn critical(task_name: impl Into<String>, args: Vec<serde_json::Value>) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.priority = Some(9);
    sig.options.max_retries = Some(5);
    sig
}

/// Create a best-effort task (low priority with no retries)
///
/// Best-effort tasks run at low priority and don't retry on failure.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::best_effort;
/// use serde_json::json;
///
/// let sig = best_effort("update_cache", vec![json!({"key": "value"})]);
/// ```
pub fn best_effort(task_name: impl Into<String>, args: Vec<serde_json::Value>) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.priority = Some(1);
    sig.options.max_retries = Some(0);
    sig
}

/// Create a transient task (with expiration time)
///
/// Transient tasks expire if not executed within the specified TTL.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::transient;
/// use serde_json::json;
///
/// // Task expires after 5 minutes
/// let sig = transient("temp_notification", vec![json!({"msg": "hi"})], 300);
/// ```
pub fn transient(
    task_name: impl Into<String>,
    args: Vec<serde_json::Value>,
    ttl_secs: u64,
) -> crate::Signature {
    let mut sig = crate::Signature::new(task_name.into()).with_args(args);
    sig.options.expires = Some(ttl_secs);
    sig
}

/// Create task options with retry and exponential backoff
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::retry_with_backoff;
///
/// // Retry up to 5 times, starting with 60s delay
/// let opts = retry_with_backoff(5, 60);
/// ```
pub fn retry_with_backoff(max_retries: u32, initial_delay_secs: u64) -> crate::TaskOptions {
    crate::TaskOptions {
        max_retries: Some(max_retries),
        countdown: Some(initial_delay_secs),
        // Note: Exponential backoff is handled by the retry mechanism
        ..Default::default()
    }
}

/// Create a pipeline workflow (modern alias for chain)
///
/// Pipeline is a more modern/intuitive name for sequential task execution.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::pipeline;
///
/// let workflow = pipeline()
///     .then("fetch_data", vec![])
///     .then("process_data", vec![])
///     .then("save_results", vec![]);
/// ```
pub fn pipeline() -> crate::Chain {
    crate::Chain::new()
}

/// Create a fan-out workflow (parallel execution with map pattern)
///
/// More intuitive name for the map pattern - fan out to process multiple items.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::fan_out;
/// use serde_json::json;
///
/// let items = vec![json!(1), json!(2), json!(3)];
/// let workflow = fan_out("process_item", items);
/// ```
pub fn fan_out<T: serde::Serialize>(task_name: impl Into<String>, items: Vec<T>) -> crate::Map {
    map(task_name, items)
}

/// Create a fan-in workflow (gather results with chord pattern)
///
/// More intuitive name for chord - fan out tasks then fan in to callback.
///
/// # Example
/// ```rust,ignore
/// use celers::convenience::{parallel, task, fan_in};
///
/// let tasks = parallel()
///     .add("task1", vec![])
///     .add("task2", vec![]);
///
/// let callback = task("aggregate_results");
/// let workflow = fan_in(tasks, callback);
/// ```
pub fn fan_in(tasks: crate::Group, callback: crate::Signature) -> crate::Chord {
    crate::Chord::new(tasks, callback)
}
