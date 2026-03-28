use crate::canvas::{Chain, Chord, Group};
use crate::Signature;
use serde_json::Value;

/// Creates an ETL (Extract, Transform, Load) pipeline workflow
///
/// This pattern is commonly used for data processing pipelines where data
/// flows through multiple transformation stages.
///
/// # Arguments
///
/// * `extract_task` - Name of the task that extracts data
/// * `extract_args` - Arguments for the extract task
/// * `transform_task` - Name of the task that transforms data
/// * `load_task` - Name of the task that loads processed data
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::etl_pipeline;
/// use serde_json::json;
///
/// let pipeline = etl_pipeline(
///     "extract_from_api",
///     vec![json!({"url": "https://api.example.com"})],
///     "transform_data",
///     "load_to_database"
/// );
///
/// pipeline.apply_async(&broker).await?;
/// ```
pub fn etl_pipeline(
    extract_task: &str,
    extract_args: Vec<Value>,
    transform_task: &str,
    load_task: &str,
) -> Chain {
    Chain::new()
        .then(extract_task, extract_args)
        .then(transform_task, vec![])
        .then(load_task, vec![])
}

/// Creates a Map-Reduce workflow for parallel processing with aggregation
///
/// This pattern processes items in parallel (map phase) and then aggregates
/// the results (reduce phase).
///
/// # Arguments
///
/// * `map_task` - Name of the task to apply to each item
/// * `items` - Collection of items to process
/// * `reduce_task` - Name of the task that aggregates results
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::map_reduce_workflow;
/// use serde_json::json;
///
/// let workflow = map_reduce_workflow(
///     "process_number",
///     vec![json!(1), json!(2), json!(3), json!(4)],
///     "sum_results"
/// );
/// ```
pub fn map_reduce_workflow(map_task: &str, items: Vec<Value>, reduce_task: &str) -> Chord {
    let mut group = Group::new();
    for item in items {
        group = group.add(map_task, vec![item]);
    }

    let reduce_sig = Signature::new(reduce_task.to_string());

    Chord {
        header: group,
        body: reduce_sig,
    }
}

/// Creates a scatter-gather workflow for distributing work
///
/// This pattern distributes different tasks to be executed in parallel
/// and then gathers all results.
///
/// # Arguments
///
/// * `tasks` - List of (task_name, args) tuples to execute in parallel
/// * `gather_task` - Name of the task that gathers all results
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::scatter_gather;
/// use serde_json::json;
///
/// let tasks = vec![
///     ("fetch_user_data", vec![json!({"user_id": 1})]),
///     ("fetch_order_data", vec![json!({"user_id": 1})]),
///     ("fetch_preferences", vec![json!({"user_id": 1})]),
/// ];
///
/// let workflow = scatter_gather(tasks, "combine_user_profile");
/// ```
pub fn scatter_gather(tasks: Vec<(&str, Vec<Value>)>, gather_task: &str) -> Chord {
    let mut group = Group::new();
    for (task_name, args) in tasks {
        group = group.add(task_name, args);
    }

    let gather_sig = Signature::new(gather_task.to_string());

    Chord {
        header: group,
        body: gather_sig,
    }
}

/// Creates a batch processing workflow with automatic chunking
///
/// This pattern processes large datasets by dividing them into batches,
/// processing each batch in parallel, and then aggregating results.
///
/// # Arguments
///
/// * `process_task` - Name of the task that processes a batch
/// * `items` - All items to process
/// * `batch_size` - Number of items per batch
/// * `aggregate_task` - Optional task to aggregate all batch results
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::batch_processing;
/// use serde_json::json;
///
/// let items = (1..=100).map(|i| json!(i)).collect();
/// let workflow = batch_processing(
///     "process_batch",
///     items,
///     10, // Process 10 items per batch
///     Some("combine_batch_results")
/// );
/// ```
pub fn batch_processing(
    process_task: &str,
    items: Vec<Value>,
    batch_size: usize,
    aggregate_task: Option<&str>,
) -> Chord {
    let mut group = Group::new();

    // Split items into batches
    for chunk in items.chunks(batch_size) {
        let batch = Value::Array(chunk.to_vec());
        group = group.add(process_task, vec![batch]);
    }

    let body_sig = if let Some(agg_task) = aggregate_task {
        Signature::new(agg_task.to_string())
    } else {
        // Default no-op aggregation
        Signature::new("celers.noop".to_string())
    };

    Chord {
        header: group,
        body: body_sig,
    }
}

/// Creates a sequential pipeline workflow with error handling
///
/// This pattern chains tasks sequentially with automatic retry and
/// error recovery built in.
///
/// # Arguments
///
/// * `stages` - List of (task_name, args, max_retries) tuples
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::sequential_pipeline;
/// use serde_json::json;
///
/// let stages = vec![
///     ("validate_input", vec![json!({"data": "test"})], 3),
///     ("process_data", vec![], 5),
///     ("save_results", vec![], 3),
/// ];
///
/// let pipeline = sequential_pipeline(stages);
/// ```
pub fn sequential_pipeline(stages: Vec<(&str, Vec<Value>, u32)>) -> Chain {
    let mut chain = Chain::new();

    for (task_name, args, max_retries) in stages {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        sig.options.max_retries = Some(max_retries);

        chain.tasks.push(sig);
    }

    chain
}

/// Creates a priority-based workflow for handling urgent tasks first
///
/// This pattern creates parallel tasks with different priorities,
/// ensuring high-priority tasks are processed first.
///
/// # Arguments
///
/// * `tasks` - List of (task_name, args, priority) tuples
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::priority_workflow;
/// use serde_json::json;
///
/// let tasks = vec![
///     ("critical_task", vec![json!(1)], 9),
///     ("normal_task", vec![json!(2)], 5),
///     ("low_priority_task", vec![json!(3)], 1),
/// ];
///
/// let workflow = priority_workflow(tasks);
/// ```
pub fn priority_workflow(tasks: Vec<(&str, Vec<Value>, u8)>) -> Group {
    let mut group = Group::new();

    for (task_name, args, priority) in tasks {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        sig.options.priority = Some(priority);

        group.tasks.push(sig);
    }

    group
}
