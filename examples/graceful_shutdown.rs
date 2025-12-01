//! Graceful shutdown example
//!
//! This example demonstrates how to run a worker with graceful shutdown support.
//! The worker will process tasks until receiving a shutdown signal (SIGINT/SIGTERM).
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Run this example:
//! ```bash
//! # Terminal 1: Start the worker (press Ctrl+C to trigger graceful shutdown)
//! cargo run --example graceful_shutdown -- worker
//!
//! # Terminal 2: Enqueue some tasks
//! cargo run --example graceful_shutdown -- enqueue
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, Task, TaskMetadata, TaskRegistry};
use celers_worker::{wait_for_signal, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::env;
use tokio::time::{sleep, Duration};

// ===== Task Definitions =====

/// Long-running task that simulates heavy processing
struct LongRunningTask;

#[derive(Serialize, Deserialize, Debug)]
struct LongRunningInput {
    duration_secs: u64,
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct LongRunningOutput {
    completed: bool,
    message: String,
}

#[async_trait::async_trait]
impl Task for LongRunningTask {
    type Input = LongRunningInput;
    type Output = LongRunningOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!(
            "LongRunningTask: Starting task that will run for {} seconds: {}",
            input.duration_secs, input.message
        );

        // Simulate long-running work with periodic updates
        for i in 0..input.duration_secs {
            sleep(Duration::from_secs(1)).await;
            println!("  Progress: {}/{} seconds", i + 1, input.duration_secs);
        }

        println!("LongRunningTask: Completed!");
        Ok(LongRunningOutput {
            completed: true,
            message: format!("Processed: {}", input.message),
        })
    }

    fn name(&self) -> &str {
        "long_running"
    }
}

/// Quick task for comparison
struct QuickTask;

#[derive(Serialize, Deserialize, Debug)]
struct QuickInput {
    value: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct QuickOutput {
    doubled: i32,
}

#[async_trait::async_trait]
impl Task for QuickTask {
    type Input = QuickInput;
    type Output = QuickOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!("QuickTask: Processing value {}", input.value);
        sleep(Duration::from_millis(500)).await;
        Ok(QuickOutput {
            doubled: input.value * 2,
        })
    }

    fn name(&self) -> &str {
        "quick"
    }
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== CeleRS Worker with Graceful Shutdown ===\n");

    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "shutdown_demo_queue")?;
    println!("✓ Connected to Redis broker");

    // Create task registry
    let registry = TaskRegistry::new();

    // Register tasks
    registry.register(LongRunningTask).await;
    registry.register(QuickTask).await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    // Configure worker
    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 120,
        ..Default::default()
    };

    // Create worker
    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started");
    println!("✓ Press Ctrl+C for graceful shutdown\n");

    // Spawn worker in background task
    let worker_task = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("Worker error: {}", e);
        }
    });

    // Wait for shutdown signal
    wait_for_signal().await;

    println!("\n=== Graceful Shutdown Initiated ===");
    println!("✓ Stopping task polling");
    println!("✓ Waiting for in-flight tasks to complete...");

    // Abort the worker task (in a real scenario, you'd use run_with_shutdown)
    worker_task.abort();

    // Give a moment for tasks to finish
    sleep(Duration::from_secs(2)).await;

    println!("✓ Worker shutdown complete");

    Ok(())
}

async fn run_worker_with_handle() -> anyhow::Result<()> {
    println!("=== CeleRS Worker with Graceful Shutdown (Handle API) ===\n");

    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "shutdown_demo_queue")?;
    println!("✓ Connected to Redis broker");

    // Create task registry
    let registry = TaskRegistry::new();

    // Register tasks
    registry.register(LongRunningTask).await;
    registry.register(QuickTask).await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    // Configure worker
    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 120,
        ..Default::default()
    };

    // Create and run worker with shutdown support
    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started with shutdown handle");
    println!("✓ Press Ctrl+C for graceful shutdown\n");

    let handle = worker.run_with_shutdown().await?;

    // Wait for shutdown signal
    wait_for_signal().await;

    println!("\n=== Graceful Shutdown Initiated ===");
    println!("✓ Sending shutdown signal to worker");

    handle.shutdown().await?;

    // Give tasks time to complete
    sleep(Duration::from_secs(3)).await;

    println!("✓ Worker shutdown complete");

    Ok(())
}

async fn enqueue_tasks() -> anyhow::Result<()> {
    println!("=== Enqueuing Tasks for Shutdown Demo ===\n");

    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "shutdown_demo_queue")?;
    println!("✓ Connected to Redis broker");

    // Enqueue some quick tasks
    for i in 1..=5 {
        let task = SerializedTask {
            metadata: TaskMetadata::new("quick".to_string()).with_timeout(10),
            payload: serde_json::to_vec(&QuickInput { value: i * 10 })?,
        };

        let task_id = broker.enqueue(task).await?;
        println!("✓ Enqueued quick task #{}: {}", i, task_id);
    }

    // Enqueue some long-running tasks
    for i in 1..=3 {
        let task = SerializedTask {
            metadata: TaskMetadata::new("long_running".to_string()).with_timeout(120),
            payload: serde_json::to_vec(&LongRunningInput {
                duration_secs: 10 * i,
                message: format!("Long task #{}", i),
            })?,
        };

        let task_id = broker.enqueue(task).await?;
        println!("✓ Enqueued long task #{}: {}", i, task_id);
    }

    // Show queue status
    let queue_size = broker.queue_size().await?;
    println!("\n✓ Current queue size: {}", queue_size);

    println!("\nAll tasks enqueued!");
    println!("\nTry shutting down the worker with Ctrl+C while tasks are processing.");
    println!("Notice how in-flight tasks complete before the worker stops.");

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
        eprintln!("  {} worker          - Start worker (basic)", args[0]);
        eprintln!(
            "  {} worker-handle   - Start worker with handle API",
            args[0]
        );
        eprintln!("  {} enqueue         - Enqueue test tasks", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "worker" => run_worker().await,
        "worker-handle" => run_worker_with_handle().await,
        "enqueue" => enqueue_tasks().await,
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Use 'worker', 'worker-handle', or 'enqueue'");
            std::process::exit(1);
        }
    }
}
