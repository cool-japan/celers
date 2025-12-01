//! Macro-based task definition example for CeleRS
//!
//! This example demonstrates how to use the #[task] macro to simplify
//! task definition by automatically generating the Task implementation.
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Run this example:
//! ```bash
//! # Terminal 1: Start the worker
//! cargo run --example macro_tasks -- worker
//!
//! # Terminal 2: Enqueue tasks
//! cargo run --example macro_tasks -- enqueue
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, TaskRegistry};
use celers_macros::task;
use celers_worker::{Worker, WorkerConfig};
use std::env;
use tokio::time::{sleep, Duration};

// ===== Task Definitions Using Macros =====

/// Simple addition task defined with #[task] macro
#[task]
async fn add_numbers(a: i32, b: i32) -> celers_core::Result<i32> {
    println!("AddNumbers: Computing {} + {}", a, b);
    sleep(Duration::from_millis(500)).await;
    Ok(a + b)
}

/// Multiplication task
#[task]
async fn multiply_numbers(x: i32, y: i32) -> celers_core::Result<i32> {
    println!("MultiplyNumbers: Computing {} * {}", x, y);
    sleep(Duration::from_millis(500)).await;
    Ok(x * y)
}

/// String processing task
#[task]
async fn process_text(text: String, uppercase: bool) -> celers_core::Result<String> {
    println!("ProcessText: Processing '{}'", text);
    sleep(Duration::from_millis(300)).await;

    let result = if uppercase {
        text.to_uppercase()
    } else {
        text.to_lowercase()
    };

    Ok(result)
}

/// Complex calculation task
#[task]
async fn calculate_factorial(n: u64) -> celers_core::Result<u64> {
    println!("CalculateFactorial: Computing {}!", n);

    if n > 20 {
        return Err(celers_core::CelersError::TaskExecution(
            "Number too large for factorial".to_string(),
        ));
    }

    sleep(Duration::from_secs(1)).await;

    let mut result = 1u64;
    for i in 2..=n {
        result *= i;
    }

    println!("CalculateFactorial: {}! = {}", n, result);
    Ok(result)
}

/// Task with validation
#[task]
async fn validate_and_double(value: i32) -> celers_core::Result<i32> {
    if value < 0 {
        return Err(celers_core::CelersError::TaskExecution(
            "Value must be non-negative".to_string(),
        ));
    }

    println!("ValidateAndDouble: Processing {}", value);
    sleep(Duration::from_millis(200)).await;
    Ok(value * 2)
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== CeleRS Macro Tasks Worker ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "macro_demo_queue")?;
    println!("✓ Connected to Redis broker");

    // Create task registry and register all macro-generated tasks
    let registry = TaskRegistry::new();

    // Register tasks - notice how clean this is with macros!
    registry.register(AddNumbersTask).await;
    registry.register(MultiplyNumbersTask).await;
    registry.register(ProcessTextTask).await;
    registry.register(CalculateFactorialTask).await;
    registry.register(ValidateAndDoubleTask).await;

    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    let config = WorkerConfig {
        concurrency: 3,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 30,
        ..Default::default()
    };

    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started with macro-generated tasks\n");

    worker.run().await?;

    Ok(())
}

async fn enqueue_tasks() -> anyhow::Result<()> {
    println!("=== Enqueuing Macro-Generated Tasks ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "macro_demo_queue")?;

    println!("Enqueueing tasks:");

    // Addition tasks
    for i in 1..=3 {
        let task = SerializedTask::new(
            "add_numbers".to_string(),
            serde_json::to_vec(&AddNumbersTaskInput {
                a: i * 10,
                b: i * 5,
            })?,
        )
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ AddNumbers: {}", task_id);
    }

    // Multiplication tasks
    for i in 1..=2 {
        let task = SerializedTask::new(
            "multiply_numbers".to_string(),
            serde_json::to_vec(&MultiplyNumbersTaskInput { x: i * 3, y: i * 4 })?,
        )
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ MultiplyNumbers: {}", task_id);
    }

    // Text processing tasks
    let texts = ["hello world", "Rust is awesome", "CeleRS Macros"];
    for (i, text) in texts.iter().enumerate() {
        let task = SerializedTask::new(
            "process_text".to_string(),
            serde_json::to_vec(&ProcessTextTaskInput {
                text: text.to_string(),
                uppercase: i % 2 == 0,
            })?,
        )
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ ProcessText: {}", task_id);
    }

    // Factorial tasks
    for n in [5, 10, 15] {
        let task = SerializedTask::new(
            "calculate_factorial".to_string(),
            serde_json::to_vec(&CalculateFactorialTaskInput { n })?,
        )
        .with_timeout(15);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ CalculateFactorial: {}", task_id);
    }

    // Validation tasks (some will fail)
    for value in [10, -5, 20, -1] {
        let task = SerializedTask::new(
            "validate_and_double".to_string(),
            serde_json::to_vec(&ValidateAndDoubleTaskInput { value })?,
        )
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        let status = if value < 0 { "(will fail)" } else { "(ok)" };
        println!("  ✓ ValidateAndDouble {}: {}", status, task_id);
    }

    println!("\n✓ Total tasks enqueued: {}", broker.queue_size().await?);
    println!("\nAll tasks enqueued! Watch them process in the worker.");

    Ok(())
}

async fn show_info() -> anyhow::Result<()> {
    println!("=== CeleRS Macro Tasks Demo ===\n");

    println!("The #[task] macro simplifies task definition!");
    println!("\nBefore (manual implementation):");
    println!("  - Define Input struct");
    println!("  - Define Output struct");
    println!("  - Define Task struct");
    println!("  - Implement Task trait");
    println!("  - Write execute() method");
    println!("  - Write name() method");
    println!("\nAfter (with #[task] macro):");
    println!("  #[task]");
    println!("  async fn my_task(param: Type) -> Result<Output> {{");
    println!("      // Your logic here");
    println!("  }}");
    println!("\nThe macro generates everything automatically!");

    println!("\n✨ Benefits:");
    println!("  • Less boilerplate code");
    println!("  • Type-safe input/output");
    println!("  • Automatic serialization");
    println!("  • Cleaner, more readable code");

    println!("\n🚀 Try it:");
    println!("  Terminal 1: cargo run --example macro_tasks -- worker");
    println!("  Terminal 2: cargo run --example macro_tasks -- enqueue\n");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        return show_info().await;
    }

    match args[1].as_str() {
        "worker" => run_worker().await,
        "enqueue" => enqueue_tasks().await,
        "info" => show_info().await,
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Use: worker, enqueue, or info");
            std::process::exit(1);
        }
    }
}
