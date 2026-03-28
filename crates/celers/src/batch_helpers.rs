use crate::{Chord, Group, Signature};
use serde_json::Value;

/// Creates batches with dynamic sizing based on item count
///
/// Automatically determines optimal batch size for efficient processing.
///
/// # Arguments
///
/// * `task_name` - Name of the task to process each batch
/// * `items` - Items to process
/// * `target_batch_size` - Target size for each batch
///
/// # Example
///
/// ```rust,ignore
/// use celers::batch_helpers::create_dynamic_batches;
/// use serde_json::json;
///
/// let items = (1..=100).map(|i| json!(i)).collect();
/// let workflow = create_dynamic_batches("process_batch", items, 10);
/// ```
pub fn create_dynamic_batches(
    task_name: &str,
    items: Vec<Value>,
    target_batch_size: usize,
) -> Chord {
    let mut group = Group::new();
    let batch_size = if target_batch_size == 0 {
        1
    } else {
        target_batch_size
    };

    for chunk in items.chunks(batch_size) {
        let batch = Value::Array(chunk.to_vec());
        group = group.add(task_name, vec![batch]);
    }

    Chord {
        header: group,
        body: Signature::new("batch_complete".to_string()),
    }
}

/// Creates adaptive batches that adjust size based on processing characteristics
///
/// # Arguments
///
/// * `task_name` - Name of the task to process each batch
/// * `items` - Items to process
/// * `min_batch_size` - Minimum batch size
/// * `max_batch_size` - Maximum batch size
///
/// # Example
///
/// ```rust,ignore
/// use celers::batch_helpers::create_adaptive_batches;
/// use serde_json::json;
///
/// let items = (1..=1000).map(|i| json!(i)).collect();
/// let workflow = create_adaptive_batches("process", items, 10, 100);
/// ```
pub fn create_adaptive_batches(
    task_name: &str,
    items: Vec<Value>,
    min_batch_size: usize,
    max_batch_size: usize,
) -> Chord {
    let mut group = Group::new();

    // Calculate adaptive batch size based on total items
    let batch_size = if items.len() < min_batch_size {
        items.len()
    } else if items.len() > max_batch_size * 10 {
        max_batch_size
    } else {
        // Use a size between min and max based on item count
        let calculated = (items.len() as f64).sqrt() as usize;
        calculated.clamp(min_batch_size, max_batch_size)
    };

    for chunk in items.chunks(batch_size.max(1)) {
        let batch = Value::Array(chunk.to_vec());
        group = group.add(task_name, vec![batch]);
    }

    Chord {
        header: group,
        body: Signature::new("batch_complete".to_string()),
    }
}

/// Creates prioritized batches with different priority levels
///
/// # Arguments
///
/// * `task_name` - Name of the task to process each batch
/// * `priority_items` - Items grouped by priority (high, medium, low)
/// * `batch_size` - Size of each batch
///
/// # Example
///
/// ```rust,ignore
/// use celers::batch_helpers::create_prioritized_batches;
/// use serde_json::json;
///
/// let high = vec![json!(1), json!(2)];
/// let medium = vec![json!(3), json!(4)];
/// let low = vec![json!(5), json!(6)];
///
/// let workflow = create_prioritized_batches("process", (high, medium, low), 2);
/// ```
pub fn create_prioritized_batches(
    task_name: &str,
    priority_items: (Vec<Value>, Vec<Value>, Vec<Value>),
    batch_size: usize,
) -> Group {
    let (high_priority, medium_priority, low_priority) = priority_items;
    let mut group = Group::new();

    // Process high priority items first
    for chunk in high_priority.chunks(batch_size.max(1)) {
        let batch = Value::Array(chunk.to_vec());
        let mut sig = Signature::new(task_name.to_string()).with_args(vec![batch]);
        sig.options.priority = Some(9);
        group.tasks.push(sig);
    }

    // Then medium priority
    for chunk in medium_priority.chunks(batch_size.max(1)) {
        let batch = Value::Array(chunk.to_vec());
        let mut sig = Signature::new(task_name.to_string()).with_args(vec![batch]);
        sig.options.priority = Some(5);
        group.tasks.push(sig);
    }

    // Finally low priority
    for chunk in low_priority.chunks(batch_size.max(1)) {
        let batch = Value::Array(chunk.to_vec());
        let mut sig = Signature::new(task_name.to_string()).with_args(vec![batch]);
        sig.options.priority = Some(1);
        group.tasks.push(sig);
    }

    group
}
