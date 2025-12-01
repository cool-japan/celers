//! Dead Letter Queue (DLQ) example for CeleRS
//!
//! This example demonstrates how failed tasks are moved to the Dead Letter Queue
//! after exhausting all retry attempts, and how to inspect and replay them.
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Run this example:
//! ```bash
//! # Terminal 1: Start the worker
//! cargo run --example dead_letter_queue -- worker
//!
//! # Terminal 2: Enqueue tasks (some will fail)
//! cargo run --example dead_letter_queue -- enqueue
//!
//! # Terminal 3: Inspect the DLQ
//! cargo run --example dead_letter_queue -- inspect
//!
//! # Terminal 4: Replay a task from DLQ
//! cargo run --example dead_letter_queue -- replay <task-id>
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, Task, TaskRegistry};
use celers_worker::{Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::env;
use tokio::time::{sleep, Duration};

// ===== Task Definitions =====

/// Task that succeeds
struct SuccessTask;

#[derive(Serialize, Deserialize, Debug)]
struct SuccessInput {
    value: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct SuccessOutput {
    doubled: i32,
}

#[async_trait::async_trait]
impl Task for SuccessTask {
    type Input = SuccessInput;
    type Output = SuccessOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!("✓ SuccessTask: Processing value {}", input.value);
        sleep(Duration::from_secs(1)).await;
        Ok(SuccessOutput {
            doubled: input.value * 2,
        })
    }

    fn name(&self) -> &str {
        "success"
    }
}

/// Task that always fails (goes to DLQ after retries)
struct AlwaysFailTask;

#[derive(Serialize, Deserialize, Debug)]
struct FailInput {
    id: u32,
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct FailOutput {
    _never: String,
}

#[async_trait::async_trait]
impl Task for AlwaysFailTask {
    type Input = FailInput;
    type Output = FailOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!("✗ AlwaysFailTask #{}: {}", input.id, input.message);
        sleep(Duration::from_millis(500)).await;
        Err(celers_core::CelersError::TaskExecution(format!(
            "Task #{} is designed to fail!",
            input.id
        )))
    }

    fn name(&self) -> &str {
        "always_fail"
    }
}

/// Task that fails sometimes based on a condition
struct ConditionalFailTask;

#[derive(Serialize, Deserialize, Debug)]
struct ConditionalInput {
    id: u32,
    should_fail: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct ConditionalOutput {
    processed: bool,
}

#[async_trait::async_trait]
impl Task for ConditionalFailTask {
    type Input = ConditionalInput;
    type Output = ConditionalOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        if input.should_fail {
            println!("✗ ConditionalFailTask #{}: Will fail", input.id);
            Err(celers_core::CelersError::TaskExecution(
                "Conditional failure".to_string(),
            ))
        } else {
            println!("✓ ConditionalFailTask #{}: Success", input.id);
            sleep(Duration::from_millis(500)).await;
            Ok(ConditionalOutput { processed: true })
        }
    }

    fn name(&self) -> &str {
        "conditional"
    }
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== CeleRS Dead Letter Queue Worker ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "dlq_demo_queue")?;
    println!("✓ Connected to Redis broker");

    let registry = TaskRegistry::new();
    registry.register(SuccessTask).await;
    registry.register(AlwaysFailTask).await;
    registry.register(ConditionalFailTask).await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 500,
        max_retries: 2, // Tasks will retry twice, then go to DLQ
        default_timeout_secs: 30,
        ..Default::default()
    };

    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started (max 2 retries before DLQ)");
    println!("✓ Failed tasks will be moved to DLQ after exhausting retries\n");

    worker.run().await?;

    Ok(())
}

async fn enqueue_tasks() -> anyhow::Result<()> {
    println!("=== Enqueuing Tasks (Some Will Fail) ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "dlq_demo_queue")?;

    // Enqueue success tasks
    println!("Enqueueing success tasks:");
    for i in 1..=3 {
        let task = SerializedTask::new(
            "success".to_string(),
            serde_json::to_vec(&SuccessInput { value: i * 10 })?,
        )
        .with_max_retries(2)
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ Success task #{}: {}", i, task_id);
    }

    // Enqueue always-fail tasks (will go to DLQ)
    println!("\nEnqueueing always-fail tasks (will retry 2x then go to DLQ):");
    for i in 1..=3 {
        let task = SerializedTask::new(
            "always_fail".to_string(),
            serde_json::to_vec(&FailInput {
                id: i,
                message: format!("This task will end up in DLQ #{}", i),
            })?,
        )
        .with_max_retries(2)
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        println!("  ✗ Fail task #{}: {}", i, task_id);
    }

    // Enqueue conditional tasks
    println!("\nEnqueueing conditional tasks:");
    for i in 1..=4 {
        let task = SerializedTask::new(
            "conditional".to_string(),
            serde_json::to_vec(&ConditionalInput {
                id: i,
                should_fail: i % 2 == 0, // Even IDs fail
            })?,
        )
        .with_max_retries(2)
        .with_timeout(10);

        let task_id = broker.enqueue(task).await?;
        let status = if i % 2 == 0 {
            "✗ (will fail)"
        } else {
            "✓ (will succeed)"
        };
        println!("  {} Conditional task #{}: {}", status, i, task_id);
    }

    println!("\n✓ Total tasks enqueued: {}", broker.queue_size().await?);
    println!("\nWatch the worker process these tasks.");
    println!("Failed tasks will appear in the DLQ after retries.");

    Ok(())
}

async fn inspect_dlq() -> anyhow::Result<()> {
    println!("=== Inspecting Dead Letter Queue ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "dlq_demo_queue")?;

    let dlq_size = broker.dlq_size().await?;
    println!("DLQ contains {} task(s)", dlq_size);

    if dlq_size > 0 {
        println!("\nFailed tasks:");
        let tasks = broker.inspect_dlq(10).await?;

        for (idx, task) in tasks.iter().enumerate() {
            println!("\n{}. Task ID: {}", idx + 1, task.metadata.id);
            println!("   Name: {}", task.metadata.name);
            println!("   State: {:?}", task.metadata.state);
            println!("   Max Retries: {}", task.metadata.max_retries);
            println!("   Created: {}", task.metadata.created_at);

            // Try to deserialize and show payload
            if task.metadata.name == "always_fail" {
                if let Ok(input) = serde_json::from_slice::<FailInput>(&task.payload) {
                    println!("   Message: {}", input.message);
                }
            }
        }

        println!("\n--- DLQ Management Options ---");
        println!("• Replay a task:  cargo run --example dead_letter_queue -- replay <task-id>");
        println!("• Clear DLQ:      cargo run --example dead_letter_queue -- clear");
    } else {
        println!("\n✓ DLQ is empty - no failed tasks");
    }

    Ok(())
}

async fn replay_task(task_id_str: &str) -> anyhow::Result<()> {
    println!("=== Replaying Task from DLQ ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "dlq_demo_queue")?;

    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    let replayed = broker.replay_from_dlq(&task_id).await?;

    if replayed {
        println!("✓ Task {} replayed from DLQ to main queue", task_id);
        println!("  The task will be processed again by the worker");
        println!("  Note: If it fails again, it will return to DLQ");
    } else {
        println!("✗ Task {} not found in DLQ", task_id);
        println!("  Run 'inspect' to see available tasks");
    }

    Ok(())
}

async fn clear_dlq() -> anyhow::Result<()> {
    println!("=== Clearing Dead Letter Queue ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "dlq_demo_queue")?;

    let count = broker.clear_dlq().await?;

    println!("✓ Cleared {} task(s) from DLQ", count);
    println!("  DLQ is now empty");

    Ok(())
}

async fn show_stats() -> anyhow::Result<()> {
    println!("=== Queue Statistics ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "dlq_demo_queue")?;

    let queue_size = broker.queue_size().await?;
    let dlq_size = broker.dlq_size().await?;

    println!("Main Queue: {} tasks", queue_size);
    println!("Dead Letter Queue: {} tasks", dlq_size);
    println!("Total: {} tasks", queue_size + dlq_size);

    if dlq_size > 0 {
        println!("\n💡 Run 'inspect' to see DLQ details");
    }

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
        eprintln!("CeleRS Dead Letter Queue Demo\n");
        eprintln!("Usage:");
        eprintln!("  {} worker           - Start the worker", args[0]);
        eprintln!("  {} enqueue          - Enqueue test tasks", args[0]);
        eprintln!("  {} inspect          - Inspect DLQ", args[0]);
        eprintln!("  {} replay <task-id> - Replay a task from DLQ", args[0]);
        eprintln!("  {} clear            - Clear all DLQ tasks", args[0]);
        eprintln!("  {} stats            - Show queue statistics", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "worker" => run_worker().await,
        "enqueue" => enqueue_tasks().await,
        "inspect" => inspect_dlq().await,
        "replay" => {
            if args.len() < 3 {
                eprintln!("Error: task-id required");
                eprintln!("Usage: {} replay <task-id>", args[0]);
                std::process::exit(1);
            }
            replay_task(&args[2]).await
        }
        "clear" => clear_dlq().await,
        "stats" => show_stats().await,
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            std::process::exit(1);
        }
    }
}
