//! Canvas Workflows Example
//!
//! This example demonstrates CeleRS Canvas workflow primitives:
//! - Chain: Sequential task execution
//! - Group: Parallel task execution
//! - Chord: Map-Reduce (parallel tasks + callback)
//! - Map: Apply same task to multiple arguments
//!
//! # Running this example
//!
//! ```bash
//! # Start Redis (required for broker)
//! docker run -d -p 6379:6379 redis
//!
//! # Run the example
//! cargo run --example canvas_workflows
//! ```

use celers_broker_redis::RedisBroker;
use celers_canvas::{Chain, Group, Map, Signature};

#[cfg(feature = "workflows")]
use celers_canvas::Chord;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("CeleRS Canvas Workflows Example");
    println!("================================\n");

    // Connect to Redis broker
    let broker =
        RedisBroker::new("redis://127.0.0.1:6379", "celery").expect("Failed to connect to Redis");

    // Example 1: Chain - Sequential Execution
    println!("1. Chain Workflow (Sequential)");
    println!("   task1 -> task2 -> task3");
    println!("   -----------------------------");

    let chain = Chain::new()
        .then("process_data", vec![serde_json::json!("input1")])
        .then("transform_data", vec![serde_json::json!("input2")])
        .then("save_result", vec![serde_json::json!("input3")]);

    match chain.apply(&broker).await {
        Ok(task_id) => {
            println!("   ✓ Chain workflow started");
            println!("   First task ID: {}", task_id);
            println!("   Note: Subsequent tasks will be triggered by callbacks\n");
        }
        Err(e) => {
            eprintln!("   ✗ Failed to start chain: {}\n", e);
        }
    }

    // Example 2: Group - Parallel Execution
    println!("2. Group Workflow (Parallel)");
    println!("   task1 | task2 | task3");
    println!("   -----------------------------");

    let group = Group::new()
        .add("process_item", vec![serde_json::json!(1)])
        .add("process_item", vec![serde_json::json!(2)])
        .add("process_item", vec![serde_json::json!(3)]);

    match group.apply(&broker).await {
        Ok(group_id) => {
            println!("   ✓ Group workflow started");
            println!("   Group ID: {}", group_id);
            println!("   All 3 tasks enqueued in parallel\n");
        }
        Err(e) => {
            eprintln!("   ✗ Failed to start group: {}\n", e);
        }
    }

    // Example 3: Map - Apply task to multiple arguments
    println!("3. Map Workflow (Apply to Multiple Args)");
    println!("   process(1) | process(2) | process(3) | process(4)");
    println!("   -----------------------------");

    let task = Signature::new("process_number".to_string());
    let argsets = vec![
        vec![serde_json::json!(1)],
        vec![serde_json::json!(2)],
        vec![serde_json::json!(3)],
        vec![serde_json::json!(4)],
    ];

    let map = Map::new(task, argsets);

    match map.apply(&broker).await {
        Ok(group_id) => {
            println!("   ✓ Map workflow started");
            println!("   Group ID: {}", group_id);
            println!("   Applied 'process_number' to 4 different arguments\n");
        }
        Err(e) => {
            eprintln!("   ✗ Failed to start map: {}\n", e);
        }
    }

    // Example 4: Chord - Map-Reduce (requires result backend)
    println!("4. Chord Workflow (Map-Reduce)");
    println!("   (task1 | task2 | task3) -> callback");
    println!("   -----------------------------");

    #[cfg(feature = "workflows")]
    {
        use celers_backend_redis::RedisResultBackend;

        let header = Group::new()
            .add("compute_partial", vec![serde_json::json!(1)])
            .add("compute_partial", vec![serde_json::json!(2)])
            .add("compute_partial", vec![serde_json::json!(3)]);

        let callback = Signature::new("aggregate_results".to_string());

        let chord = Chord::new(header, callback);

        // Create result backend for chord coordination
        let mut backend = RedisResultBackend::new("redis://127.0.0.1:6379")
            .expect("Failed to connect to Redis backend");

        match chord.apply(&broker, &mut backend).await {
            Ok(chord_id) => {
                println!("   ✓ Chord workflow started");
                println!("   Chord ID: {}", chord_id);
                println!("   When all 3 tasks complete, 'aggregate_results' will be triggered\n");
            }
            Err(e) => {
                eprintln!("   ✗ Failed to start chord: {}\n", e);
            }
        }
    }

    #[cfg(not(feature = "workflows"))]
    {
        println!("   ⚠ Chord requires 'workflows' feature (includes backend-redis)");
        println!("   Note: Chord workflow coordination needs a result backend\n");
    }

    // Example 5: Priority Queues with Workflows
    println!("5. Priority Workflows");
    println!("   High priority tasks execute first");
    println!("   -----------------------------");

    let high_priority_sig = Signature::new("urgent_task".to_string())
        .with_args(vec![serde_json::json!("URGENT")])
        .with_priority(9);

    let normal_priority_sig = Signature::new("normal_task".to_string())
        .with_args(vec![serde_json::json!("normal")])
        .with_priority(5);

    let group = Group::new()
        .add_signature(high_priority_sig)
        .add_signature(normal_priority_sig);

    match group.apply(&broker).await {
        Ok(group_id) => {
            println!("   ✓ Priority group started");
            println!("   Group ID: {}", group_id);
            println!("   'urgent_task' will execute before 'normal_task'\n");
        }
        Err(e) => {
            eprintln!("   ✗ Failed to start priority group: {}\n", e);
        }
    }

    // Example 6: Complex Nested Workflow
    println!("6. Complex Nested Workflow");
    println!("   Chaining groups and other primitives");
    println!("   -----------------------------");

    // In a real implementation, you would:
    // 1. Create a signature for the initial step
    // 2. Create a group for parallel processing
    // 3. Use task callbacks (link) to connect workflow stages
    //
    // Example pattern:
    //   prepare_data -> (process_chunk | process_chunk | process_chunk) -> finalize

    println!("   ✓ Complex workflows can be built by chaining primitives");
    println!("   Use task callbacks (link) to connect different workflow stages\n");

    println!("Summary");
    println!("=======");
    println!("✓ All workflow primitives demonstrated");
    println!("✓ Chain: Sequential execution with callbacks");
    println!("✓ Group: Parallel execution with tracking");
    println!("✓ Chord: Map-Reduce with barrier synchronization");
    println!("✓ Map: Apply task to multiple argument sets");
    println!("\nNext Steps:");
    println!("- Start workers to process these tasks");
    println!("- Monitor task execution with metrics");
    println!("- Build complex workflows by combining primitives");

    Ok(())
}
