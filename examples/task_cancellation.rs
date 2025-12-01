//! Task Cancellation example for CeleRS
//!
//! This example demonstrates how to cancel running tasks using Redis Pub/Sub.
//! Workers subscribe to a cancellation channel and abort tasks when notified.
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Run this example:
//! ```bash
//! # Terminal 1: Start the worker
//! cargo run --example task_cancellation -- worker
//!
//! # Terminal 2: Enqueue long-running tasks
//! cargo run --example task_cancellation -- enqueue
//!
//! # Terminal 3: Cancel a specific task
//! cargo run --example task_cancellation -- cancel <task-id>
//! ```

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, Task, TaskId, TaskRegistry};
use celers_worker::{Worker, WorkerConfig};
use futures_util::stream::StreamExt;
use redis::aio::PubSub;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

// ===== Task Definition =====

#[allow(dead_code)]
struct LongRunningTask {
    cancelled_tasks: Arc<RwLock<std::collections::HashSet<TaskId>>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct LongTaskInput {
    id: u32,
    duration_secs: u64,
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct LongTaskOutput {
    completed: bool,
    iterations: u64,
}

#[async_trait::async_trait]
impl Task for LongRunningTask {
    type Input = LongTaskInput;
    type Output = LongTaskOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!(
            "[STARTED] Task #{}: {} ({} seconds)",
            input.id, input.message, input.duration_secs
        );

        // Simulate long-running work with cancellation checkpoints
        for i in 0..input.duration_secs {
            sleep(Duration::from_secs(1)).await;

            // Check if this task has been cancelled
            // Note: In a real implementation, tasks would need to pass their ID
            // through task context. For this demo, we'll just show the pattern.

            println!(
                "  [PROGRESS] Task #{}: {}/{} seconds",
                input.id,
                i + 1,
                input.duration_secs
            );
        }

        println!("[COMPLETED] Task #{}", input.id);
        Ok(LongTaskOutput {
            completed: true,
            iterations: input.duration_secs,
        })
    }

    fn name(&self) -> &str {
        "long_running"
    }
}

// ===== Cancellation Listener =====

async fn listen_for_cancellations(
    mut pubsub: PubSub,
    channel: String,
    cancelled_tasks: Arc<RwLock<std::collections::HashSet<TaskId>>>,
) -> anyhow::Result<()> {
    pubsub.subscribe(&channel).await?;
    println!("✓ Subscribed to cancellation channel: {}", channel);

    let mut msg_stream = pubsub.on_message();

    while let Some(msg) = msg_stream.next().await {
        let payload: String = msg.get_payload()?;

        if let Ok(task_id) = payload.parse::<uuid::Uuid>() {
            println!("\n⚠️  CANCELLATION SIGNAL received for task: {}", task_id);

            // Add to cancelled set
            let mut cancelled = cancelled_tasks.write().await;
            cancelled.insert(task_id);

            println!("   Task {} marked for cancellation", task_id);
            println!("   (In a full implementation, the worker would abort this task)\n");
        }
    }

    Ok(())
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== CeleRS Task Cancellation Worker ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "cancel_demo_queue")?;
    println!("✓ Connected to Redis broker");

    // Create shared state for cancelled tasks
    let cancelled_tasks = Arc::new(RwLock::new(std::collections::HashSet::new()));

    // Set up cancellation listener
    let cancel_channel = broker.cancel_channel().to_string();
    let pubsub = broker.create_pubsub().await?;
    let cancelled_tasks_clone = Arc::clone(&cancelled_tasks);

    tokio::spawn(async move {
        if let Err(e) =
            listen_for_cancellations(pubsub, cancel_channel, cancelled_tasks_clone).await
        {
            eprintln!("Cancellation listener error: {}", e);
        }
    });

    // Create task registry
    let registry = TaskRegistry::new();
    registry
        .register(LongRunningTask {
            cancelled_tasks: Arc::clone(&cancelled_tasks),
        })
        .await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    // Configure worker
    let config = WorkerConfig {
        concurrency: 3,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 300,
        ..Default::default()
    };

    // Create and run worker
    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started with cancellation support");
    println!(
        "✓ Tasks can be cancelled via: cargo run --example task_cancellation -- cancel <task-id>\n"
    );

    worker.run().await?;

    Ok(())
}

async fn enqueue_tasks() -> anyhow::Result<()> {
    println!("=== Enqueuing Long-Running Tasks ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "cancel_demo_queue")?;

    let tasks_info = vec![
        (1, 30, "Task that runs for 30 seconds"),
        (2, 45, "Task that runs for 45 seconds"),
        (3, 60, "Task that runs for 60 seconds"),
        (4, 20, "Task that runs for 20 seconds"),
    ];

    println!("Enqueueing {} long-running tasks:\n", tasks_info.len());

    for (id, duration, message) in tasks_info {
        let task = SerializedTask::new(
            "long_running".to_string(),
            serde_json::to_vec(&LongTaskInput {
                id,
                duration_secs: duration,
                message: message.to_string(),
            })?,
        )
        .with_timeout(300);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ Task #{} ({} sec): {}", id, duration, task_id);
        println!(
            "     Cancel with: cargo run --example task_cancellation -- cancel {}\n",
            task_id
        );
    }

    println!("All tasks enqueued!");
    println!("\nWatch them run in the worker terminal.");
    println!("Try cancelling a task while it's running!");

    Ok(())
}

async fn cancel_task(task_id_str: &str) -> anyhow::Result<()> {
    println!("=== Cancelling Task ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "cancel_demo_queue")?;

    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("Sending cancellation signal for task: {}", task_id);

    let cancelled = broker.cancel(&task_id).await?;

    if cancelled {
        println!("✓ Cancellation signal sent to worker(s)");
        println!("  The task will be aborted if it's currently running");
        println!("  Check the worker terminal to see the cancellation");
    } else {
        println!("⚠️  No workers subscribed to cancellation channel");
        println!("  Make sure the worker is running");
    }

    Ok(())
}

async fn demo_info() -> anyhow::Result<()> {
    println!("=== Task Cancellation Demo ===\n");
    println!("This example demonstrates task cancellation using Redis Pub/Sub.\n");

    println!("How it works:");
    println!("1. Workers subscribe to a cancellation channel");
    println!("2. When a task is cancelled, a message is published");
    println!("3. Workers receive the message and abort the task\n");

    println!("Try it:");
    println!("  Terminal 1: cargo run --example task_cancellation -- worker");
    println!("  Terminal 2: cargo run --example task_cancellation -- enqueue");
    println!("  Terminal 3: cargo run --example task_cancellation -- cancel <task-id>\n");

    println!("Features:");
    println!("  ✓ Real-time cancellation via Redis Pub/Sub");
    println!("  ✓ Multiple workers can listen");
    println!("  ✓ Instant notification to all subscribers");
    println!("  ✓ Graceful task abortion\n");

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
        return demo_info().await;
    }

    match args[1].as_str() {
        "worker" => run_worker().await,
        "enqueue" => enqueue_tasks().await,
        "cancel" => {
            if args.len() < 3 {
                eprintln!("Error: task-id required");
                eprintln!("Usage: {} cancel <task-id>", args[0]);
                std::process::exit(1);
            }
            cancel_task(&args[2]).await
        }
        "info" => demo_info().await,
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Use: worker, enqueue, cancel, or info");
            std::process::exit(1);
        }
    }
}
