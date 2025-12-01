//! Comprehensive example showing CeleRS facade usage
//!
//! This example demonstrates:
//! - Using the facade crate for easy imports
//! - Defining tasks with the macro
//! - Setting up workers
//! - Enqueuing tasks with priorities
//! - Monitoring with metrics

use celers::prelude::*;
use std::time::Duration;

// Define task argument types
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
struct AddArgs {
    x: i32,
    y: i32,
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug)]
struct ProcessDataArgs {
    data: Vec<String>,
    filter: Option<String>,
}

// Simple math task
#[celers::task]
async fn add(args: AddArgs) -> Result<i32, Box<dyn std::error::Error>> {
    println!("Adding {} + {}", args.x, args.y);
    Ok(args.x + args.y)
}

// More complex task with error handling
#[celers::task]
async fn process_data(args: ProcessDataArgs) -> Result<usize, Box<dyn std::error::Error>> {
    println!("Processing {} items", args.data.len());

    let filtered: Vec<_> = match args.filter {
        Some(filter) => args
            .data
            .into_iter()
            .filter(|s| s.contains(&filter))
            .collect(),
        None => args.data,
    };

    println!("Filtered to {} items", filtered.len());
    Ok(filtered.len())
}

// Task that simulates long-running work
#[celers::task]
async fn long_task(_args: serde_json::Value) -> Result<String, Box<dyn std::error::Error>> {
    println!("Starting long task...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("Long task complete!");
    Ok("Done after 5 seconds".to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== CeleRS Facade Usage Example ===\n");

    // Example 1: Create a broker
    println!("1. Creating Redis broker...");
    let _broker = RedisBroker::new("redis://127.0.0.1:6379", "celers_example")?;
    println!("   ✓ Broker created\n");

    // Example 2: Enqueue tasks
    println!("2. Enqueuing tasks...");

    let add_task = SerializedTask::new(
        "add".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "x": 10,
            "y": 32
        }))?,
    );

    let process_task = SerializedTask::new(
        "process_data".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "data": ["apple", "banana", "apricot", "cherry"],
            "filter": "ap"
        }))?,
    )
    .with_priority(9); // High priority

    println!("   ✓ Tasks created\n");

    // Example 3: Show task configuration
    println!("3. Task Configuration:");
    println!("   - add task: priority={}", add_task.metadata.priority);
    println!(
        "   - process task: priority={}",
        process_task.metadata.priority
    );
    println!("   - max_retries: {}\n", process_task.metadata.max_retries);

    // Example 4: Setup worker (commented out to avoid requiring Redis)
    println!("4. Worker Setup (example code):");
    println!(
        r#"
    let config = WorkerConfig {{
        concurrency: 4,
        max_retries: 3,
        default_timeout: Duration::from_secs(300),
        poll_interval: Duration::from_millis(1000),
    }};

    let mut worker = Worker::new(broker, config);

    // Register tasks
    worker.register_task("add", add);
    worker.register_task("process_data", process_data);
    worker.register_task("long_task", long_task);

    // Start worker
    worker.run().await?;
    "#
    );

    // Example 5: Canvas workflow (demonstration)
    println!("\n5. Workflow Example (Canvas):");
    println!("   Chain:");
    let _chain = Chain::new()
        .then("add", vec![json!({"x": 1, "y": 2})])
        .then("process_data", vec![json!({"data": ["result"]})]);
    println!("     task1 -> task2 -> task3");

    println!("\n   Group (parallel):");
    let _group = Group::new()
        .add("add", vec![json!({"x": 1, "y": 1})])
        .add("add", vec![json!({"x": 2, "y": 2})])
        .add("add", vec![json!({"x": 3, "y": 3})]);
    println!("     task1 | task2 | task3");

    println!("\n   Chord (map-reduce):");
    println!("     (task1 | task2 | task3) -> callback");

    // Example 6: Monitoring
    println!("\n6. Metrics:");
    println!("   - Prometheus metrics available with 'metrics' feature");
    println!("   - Health check endpoints available");
    println!("   - See examples/prometheus_metrics.rs for full metrics example");

    println!("\n=== Example Complete ===");
    println!("\nTo run a real worker:");
    println!("  1. Start Redis: docker run -d -p 6379:6379 redis");
    println!("  2. Run: cargo run --example phase1_complete");
    println!("\nFor more examples, see the examples/ directory");

    Ok(())
}
