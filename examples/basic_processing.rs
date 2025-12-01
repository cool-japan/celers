//! Basic example of using CeleRS with Redis broker

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, TaskMetadata};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("CeleRS Basic Processing Example");
    println!("================================\n");

    // Create a Redis broker
    let broker = RedisBroker::new("redis://localhost:6379", "example_queue")?;

    // Create a simple task
    let task = SerializedTask {
        metadata: TaskMetadata::new("example_task".to_string())
            .with_max_retries(3)
            .with_timeout(30)
            .with_priority(1),
        payload: serde_json::to_vec(&serde_json::json!({
            "message": "Hello from CeleRS!"
        }))?,
    };

    // Enqueue the task
    let task_id = broker.enqueue(task).await?;
    println!("✓ Enqueued task: {}", task_id);

    // Check queue size
    let size = broker.queue_size().await?;
    println!("✓ Current queue size: {}", size);

    println!("\nTo process this task, run:");
    println!(
        "  cargo run --bin celers -- worker --broker redis://localhost:6379 --queue example_queue"
    );

    Ok(())
}
