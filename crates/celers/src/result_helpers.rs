use crate::Signature;
use serde_json::Value;

/// Creates a signature for collecting results from multiple tasks
///
/// Useful for aggregating results in map-reduce patterns.
///
/// # Arguments
///
/// * `collector_task` - Name of the task that collects results
/// * `expected_count` - Expected number of results to collect
///
/// # Example
///
/// ```rust,ignore
/// use celers::result_helpers::create_result_collector;
///
/// let collector = create_result_collector("aggregate_results", 100);
/// ```
pub fn create_result_collector(collector_task: &str, expected_count: usize) -> Signature {
    let mut sig = Signature::new(collector_task.to_string()).with_args(vec![serde_json::json!({
        "expected_count": expected_count
    })]);
    sig.options.time_limit = Some(300); // 5 minute timeout for collection
    sig
}

/// Creates a task that filters results based on criteria
///
/// # Arguments
///
/// * `filter_task` - Name of the filtering task
/// * `criteria` - Filtering criteria as JSON value
///
/// # Example
///
/// ```rust,ignore
/// use celers::result_helpers::create_result_filter;
/// use serde_json::json;
///
/// let filter = create_result_filter(
///     "filter_successful",
///     json!({"status": "success"})
/// );
/// ```
pub fn create_result_filter(filter_task: &str, criteria: Value) -> Signature {
    Signature::new(filter_task.to_string()).with_args(vec![criteria])
}

/// Creates a task for transforming result data
///
/// # Arguments
///
/// * `transform_task` - Name of the transformation task
/// * `transformation_config` - Configuration for the transformation
///
/// # Example
///
/// ```rust,ignore
/// use celers::result_helpers::create_result_transformer;
/// use serde_json::json;
///
/// let transformer = create_result_transformer(
///     "format_results",
///     json!({"format": "csv", "include_headers": true})
/// );
/// ```
pub fn create_result_transformer(transform_task: &str, transformation_config: Value) -> Signature {
    Signature::new(transform_task.to_string()).with_args(vec![transformation_config])
}

/// Creates a task for reducing/aggregating multiple results
///
/// # Arguments
///
/// * `reduce_task` - Name of the reduce task
/// * `operation` - Type of reduction operation (e.g., "sum", "average", "concat")
///
/// # Example
///
/// ```rust,ignore
/// use celers::result_helpers::create_result_reducer;
///
/// let reducer = create_result_reducer("sum_results", "sum");
/// ```
pub fn create_result_reducer(reduce_task: &str, operation: &str) -> Signature {
    Signature::new(reduce_task.to_string()).with_args(vec![serde_json::json!({
        "operation": operation
    })])
}
