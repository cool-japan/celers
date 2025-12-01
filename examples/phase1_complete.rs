//! Complete Phase 1 example: Enqueue tasks and process them with a worker
//!
//! This example demonstrates:
//! - Defining custom tasks
//! - Registering tasks in a registry
//! - Enqueuing tasks to Redis
//! - Running a worker to process tasks
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Run this example:
//! ```bash
//! # Terminal 1: Start the worker
//! cargo run --example phase1_complete -- worker
//!
//! # Terminal 2: Enqueue some tasks
//! cargo run --example phase1_complete -- enqueue
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, Task, TaskMetadata, TaskRegistry};
use celers_worker::{Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::env;

// ===== Task Definitions =====

/// Simple math task that adds two numbers
struct AddTask;

#[derive(Serialize, Deserialize, Debug)]
struct AddInput {
    a: i32,
    b: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct AddOutput {
    result: i32,
}

#[async_trait::async_trait]
impl Task for AddTask {
    type Input = AddInput;
    type Output = AddOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!("AddTask: Computing {} + {}", input.a, input.b);
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Simulate work
        Ok(AddOutput {
            result: input.a + input.b,
        })
    }

    fn name(&self) -> &str {
        "add"
    }
}

/// Task that multiplies two numbers
struct MultiplyTask;

#[derive(Serialize, Deserialize, Debug)]
struct MultiplyInput {
    a: i32,
    b: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct MultiplyOutput {
    result: i32,
}

#[async_trait::async_trait]
impl Task for MultiplyTask {
    type Input = MultiplyInput;
    type Output = MultiplyOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!("MultiplyTask: Computing {} * {}", input.a, input.b);
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // Simulate work
        Ok(MultiplyOutput {
            result: input.a * input.b,
        })
    }

    fn name(&self) -> &str {
        "multiply"
    }
}

/// Task that always fails (for testing retry logic)
struct FailingTask;

#[derive(Serialize, Deserialize, Debug)]
struct FailingInput {
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct FailingOutput {
    _never_returned: String,
}

#[async_trait::async_trait]
impl Task for FailingTask {
    type Input = FailingInput;
    type Output = FailingOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!("FailingTask: About to fail with message: {}", input.message);
        Err(celers_core::CelersError::TaskExecution(
            input.message.clone(),
        ))
    }

    fn name(&self) -> &str {
        "failing"
    }
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== Starting CeleRS Worker ===\n");

    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "phase1_queue")?;
    println!("✓ Connected to Redis broker");

    // Create task registry
    let registry = TaskRegistry::new();

    // Register tasks
    registry.register(AddTask).await;
    registry.register(MultiplyTask).await;
    registry.register(FailingTask).await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    // Configure worker
    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 30,
        ..Default::default()
    };

    // Create and run worker
    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started, waiting for tasks...\n");

    worker.run().await?;

    Ok(())
}

async fn enqueue_tasks() -> anyhow::Result<()> {
    println!("=== Enqueuing Tasks to CeleRS ===\n");

    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "phase1_queue")?;
    println!("✓ Connected to Redis broker");

    // Enqueue add tasks
    for i in 1..=3 {
        let task = SerializedTask {
            metadata: TaskMetadata::new("add".to_string())
                .with_max_retries(2)
                .with_timeout(10),
            payload: serde_json::to_vec(&AddInput {
                a: i * 10,
                b: i * 5,
            })?,
        };

        let task_id = broker.enqueue(task).await?;
        println!("✓ Enqueued add task: {}", task_id);
    }

    // Enqueue multiply tasks
    for i in 1..=2 {
        let task = SerializedTask {
            metadata: TaskMetadata::new("multiply".to_string())
                .with_max_retries(2)
                .with_timeout(15),
            payload: serde_json::to_vec(&MultiplyInput { a: i * 3, b: i * 4 })?,
        };

        let task_id = broker.enqueue(task).await?;
        println!("✓ Enqueued multiply task: {}", task_id);
    }

    // Enqueue a failing task (will be retried and then fail permanently)
    let task = SerializedTask {
        metadata: TaskMetadata::new("failing".to_string())
            .with_max_retries(2)
            .with_timeout(5),
        payload: serde_json::to_vec(&FailingInput {
            message: "This task is designed to fail!".to_string(),
        })?,
    };

    let task_id = broker.enqueue(task).await?;
    println!("✓ Enqueued failing task: {} (will retry and fail)", task_id);

    // Show queue status
    let queue_size = broker.queue_size().await?;
    println!("\n✓ Current queue size: {}", queue_size);

    println!("\nAll tasks enqueued! The worker should process them now.");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!("  {} worker    - Start the worker", args[0]);
        eprintln!("  {} enqueue   - Enqueue test tasks", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "worker" => run_worker().await,
        "enqueue" => enqueue_tasks().await,
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Use 'worker' or 'enqueue'");
            std::process::exit(1);
        }
    }
}
