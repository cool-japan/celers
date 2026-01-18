//! Priority queue example for CeleRS
//!
//! This example demonstrates how to use priority-based task processing with Redis.
//! Tasks with higher priority values are processed before lower priority tasks.
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Run this example:
//! ```bash
//! # Terminal 1: Start the worker
//! cargo run --example priority_queue -- worker
//!
//! # Terminal 2: Enqueue tasks with different priorities
//! cargo run --example priority_queue -- enqueue
//! ```

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_core::{Broker, SerializedTask, Task, TaskRegistry};
use celers_worker::{Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::env;
use tokio::time::{sleep, Duration};

// ===== Task Definition =====

struct ProcessTask;

#[derive(Serialize, Deserialize, Debug)]
struct ProcessInput {
    id: u32,
    priority: i32,
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ProcessOutput {
    processed: bool,
}

#[async_trait::async_trait]
impl Task for ProcessTask {
    type Input = ProcessInput;
    type Output = ProcessOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        println!(
            "[PROCESSING] Task #{} (Priority: {}) - {}",
            input.id, input.priority, input.message
        );

        // Simulate work
        sleep(Duration::from_secs(2)).await;

        println!("[COMPLETED] Task #{}", input.id);

        Ok(ProcessOutput { processed: true })
    }

    fn name(&self) -> &str {
        "process"
    }
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== CeleRS Priority Queue Worker ===\n");

    // Create broker with Priority mode
    let broker = RedisBroker::with_mode(
        "redis://localhost:6379",
        "priority_demo_queue",
        QueueMode::Priority,
    )?;
    println!("✓ Connected to Redis broker (Priority mode)");

    // Create task registry
    let registry = TaskRegistry::new();
    registry.register(ProcessTask).await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    // Configure worker
    let config = WorkerConfig {
        concurrency: 1, // Use 1 to see priority ordering clearly
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 30,
        ..Default::default()
    };

    // Create and run worker
    let worker = Worker::new(broker, registry, config);
    println!("\n✓ Worker started");
    println!("✓ Tasks will be processed in priority order (highest first)\n");

    worker.run().await?;

    Ok(())
}

async fn enqueue_tasks() -> anyhow::Result<()> {
    println!("=== Enqueuing Tasks with Different Priorities ===\n");

    // Create broker with Priority mode
    let broker = RedisBroker::with_mode(
        "redis://localhost:6379",
        "priority_demo_queue",
        QueueMode::Priority,
    )?;
    println!("✓ Connected to Redis broker (Priority mode)\n");

    // Enqueue tasks with varying priorities
    let tasks = vec![
        (1, 5, "Low priority task"),
        (2, 100, "HIGH PRIORITY TASK"),
        (3, 10, "Medium priority task"),
        (4, 50, "High priority task"),
        (5, 1, "Very low priority task"),
        (6, 200, "CRITICAL PRIORITY TASK"),
        (7, 25, "Medium-high priority task"),
    ];

    println!("Enqueueing tasks in random priority order:");
    for (id, priority, message) in tasks {
        let task = SerializedTask::new(
            "process".to_string(),
            serde_json::to_vec(&ProcessInput {
                id,
                priority,
                message: message.to_string(),
            })?,
        )
        .with_priority(priority)
        .with_timeout(30);

        let task_id = broker.enqueue(task).await?;
        println!("  ✓ Task #{}: {} (Priority: {})", id, task_id, priority);
    }

    let queue_size = broker.queue_size().await?;
    println!("\n✓ Total tasks in queue: {}", queue_size);

    println!("\n=== Expected Processing Order ===");
    println!("The worker should process tasks in this order:");
    println!("  1. Task #6: CRITICAL PRIORITY TASK (Priority: 200)");
    println!("  2. Task #2: HIGH PRIORITY TASK (Priority: 100)");
    println!("  3. Task #4: High priority task (Priority: 50)");
    println!("  4. Task #7: Medium-high priority task (Priority: 25)");
    println!("  5. Task #3: Medium priority task (Priority: 10)");
    println!("  6. Task #1: Low priority task (Priority: 5)");
    println!("  7. Task #5: Very low priority task (Priority: 1)");

    println!("\nStart the worker to see priority-based processing!");

    Ok(())
}

async fn compare_modes() -> anyhow::Result<()> {
    println!("=== Comparing FIFO vs Priority Queue ===\n");

    // Test FIFO mode
    println!("--- FIFO Mode ---");
    let fifo_broker = RedisBroker::new("redis://localhost:6379", "test_fifo")?;

    for i in 1..=5 {
        let task = SerializedTask::new(
            "test".to_string(),
            serde_json::to_vec(&ProcessInput {
                id: i,
                priority: (6 - i) as i32 * 10, // Reverse priority
                message: format!("FIFO task {}", i),
            })?,
        )
        .with_priority((6 - i) as i32 * 10);

        fifo_broker.enqueue(task).await?;
    }
    println!("✓ Enqueued 5 tasks to FIFO queue");
    println!("  Processing order: 1, 2, 3, 4, 5 (insertion order)");

    // Test Priority mode
    println!("\n--- Priority Mode ---");
    let priority_broker = RedisBroker::with_mode(
        "redis://localhost:6379",
        "test_priority",
        QueueMode::Priority,
    )?;

    for i in 1..=5 {
        let task = SerializedTask::new(
            "test".to_string(),
            serde_json::to_vec(&ProcessInput {
                id: i,
                priority: (6 - i) as i32 * 10, // Reverse priority
                message: format!("Priority task {}", i),
            })?,
        )
        .with_priority((6 - i) as i32 * 10);

        priority_broker.enqueue(task).await?;
    }
    println!("✓ Enqueued 5 tasks to Priority queue");
    println!("  Processing order: 1, 2, 3, 4, 5 (priority order: 50, 40, 30, 20, 10)");

    println!("\nNote: The queues are left for inspection.");
    println!("Use 'redis-cli' to examine:");
    println!("  LRANGE test_fifo 0 -1");
    println!("  ZRANGE test_priority 0 -1 WITHSCORES");

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
        eprintln!("  {} worker    - Start the priority worker", args[0]);
        eprintln!("  {} enqueue   - Enqueue tasks with priorities", args[0]);
        eprintln!("  {} compare   - Compare FIFO vs Priority modes", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "worker" => run_worker().await,
        "enqueue" => enqueue_tasks().await,
        "compare" => compare_modes().await,
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Use 'worker', 'enqueue', or 'compare'");
            std::process::exit(1);
        }
    }
}
