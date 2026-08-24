//! Task Cancellation example for CeleRS
//!
//! Cancelling a task has two halves, and this example shows both:
//!
//! - [`Broker::revoke`] records the revocation in the queue's durable
//!   revoked-id set (so a task still sitting in the queue is refused when it is
//!   finally dequeued, even by a worker that starts later) and publishes it on
//!   the queue's revocation channel.
//! - `Worker::with_broker_revocation` subscribes the worker to that channel and
//!   consults that set, so a task that is *already running* has its
//!   cancellation token tripped. The task body observes it through
//!   [`is_cancelled`] and returns early — this is Celery's
//!   `task.is_aborted()`.
//!
//! Cancellation is cooperative: a task that never checks its token keeps
//! running until its next `.await`, at which point the worker drops it.
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
use celers_core::revocation_channel::RevocationStream;
use celers_core::{Broker, SerializedTask, Task, TaskRegistry};
use celers_worker::execution_context::is_cancelled;
use celers_worker::{Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use std::env;
use tokio::time::{sleep, Duration};

// ===== Task Definition =====

struct LongRunningTask;

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

        // Simulate long-running work with cancellation checkpoints.
        for i in 0..input.duration_secs {
            sleep(Duration::from_secs(1)).await;

            // The cancellation token of the task being executed is ambient: the
            // worker installs it for the duration of this future, so nothing
            // has to be threaded through. This check is the whole cooperative
            // contract.
            if is_cancelled() {
                println!(
                    "[REVOKED] Task #{} stopping after {}/{} seconds",
                    input.id,
                    i + 1,
                    input.duration_secs
                );
                // Stop working and hand back what was finished. The worker
                // races this future against the cancellation token, so it
                // records the task as Revoked — never Succeeded — whichever of
                // the two wins, and never retries it.
                return Ok(LongTaskOutput {
                    completed: false,
                    iterations: i + 1,
                });
            }

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

/// Print every revocation as it crosses the queue's revocation channel.
///
/// Purely an observer, for the demo's benefit: the worker acts on these by
/// itself, through the same subscription, because it was built with
/// `with_broker_revocation`. Nothing here is required to make cancellation
/// work.
async fn watch_revocations(mut stream: Box<dyn RevocationStream>) {
    loop {
        match stream.recv().await {
            Ok(Some(notice)) => {
                println!(
                    "\n⚠️  REVOCATION received for task {} (terminate={})",
                    notice.task_id, notice.terminate
                );
                if notice.terminate {
                    println!("   A worker running it will trip its cancellation token\n");
                } else {
                    println!("   It will be refused if it has not started yet\n");
                }
            }
            // The connection closed; the worker's own bridge resubscribes, but
            // this observer is a demo and simply stops.
            Ok(None) => return,
            Err(e) => eprintln!("Revocation stream error: {e}"),
        }
    }
}

// ===== Main Functions =====

async fn run_worker() -> anyhow::Result<()> {
    println!("=== CeleRS Task Cancellation Worker ===\n");

    let broker = RedisBroker::new("redis://localhost:6379", "cancel_demo_queue")?;
    println!("✓ Connected to Redis broker");

    // An observer on the same channel the worker's bridge reads, so the demo
    // can show the revocation arriving.
    if let Some(stream) = broker.subscribe_revocations().await? {
        println!("✓ Watching revocation channel: {}", broker.cancel_channel());
        tokio::spawn(watch_revocations(stream));
    }

    // Create task registry
    let registry = TaskRegistry::new();
    registry.register(LongRunningTask).await;
    println!("✓ Registered tasks: {:?}", registry.list_tasks().await);

    // Configure worker
    let config = WorkerConfig {
        concurrency: 3,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 300,
        ..Default::default()
    };

    // Create and run worker. `with_broker_revocation` is what connects the
    // queue's revocation channel and revoked-id set to this worker; without it
    // the worker would run happily and ignore every `cancel` command.
    let worker = Worker::new(broker, registry, config).with_broker_revocation();
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

    println!("Revoking task: {}", task_id);

    // `terminate = true` is Celery's `revoke(id, terminate=True)`: it also asks
    // a worker that is already running the task to abort it. Plain `cancel`
    // (or `revoke(id, false)`) only stops it from starting.
    broker.revoke(&task_id, true).await?;

    println!("✓ Revocation recorded in the queue's revoked set");
    println!("  A task still in the queue will be refused when it is dequeued,");
    println!("  even if no worker is running right now.");
    println!("✓ Revocation published on the queue's revocation channel");
    println!("  A worker already running the task will abort it at its next");
    println!("  cancellation checkpoint — watch the worker terminal.");

    Ok(())
}

async fn demo_info() -> anyhow::Result<()> {
    println!("=== Task Cancellation Demo ===\n");
    println!("This example demonstrates task cancellation using Redis Pub/Sub.\n");

    println!("How it works:");
    println!("1. `revoke` records the task id in the queue's durable revoked set");
    println!("   and publishes a notice on the queue's revocation channel");
    println!("2. A worker built with `with_broker_revocation` reads both: the set");
    println!("   before running anything it dequeues, the channel continuously");
    println!("3. For a running task it trips the cancellation token; the task");
    println!("   observes it at its next `check_cancelled()` and returns early\n");

    println!("Try it:");
    println!("  Terminal 1: cargo run --example task_cancellation -- worker");
    println!("  Terminal 2: cargo run --example task_cancellation -- enqueue");
    println!("  Terminal 3: cargo run --example task_cancellation -- cancel <task-id>\n");

    println!("Features:");
    println!("  ✓ Real-time cancellation via Redis Pub/Sub");
    println!("  ✓ Durable revoked-id set, so a queued task is refused too");
    println!("  ✓ Multiple workers can listen; every one of them acts");
    println!("  ✓ Cooperative abortion: the task decides where it is safe to stop\n");

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
