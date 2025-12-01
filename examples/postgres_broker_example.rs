//! Example demonstrating the PostgreSQL broker with priorities and DLQ
//!
//! This example shows:
//! - Task enqueuing and dequeueing with PostgreSQL
//! - Priority-based task execution
//! - Automatic Dead Letter Queue (DLQ) for failed tasks
//! - Distributed worker support with SKIP LOCKED

use async_trait::async_trait;
use celers_broker_postgres::PostgresBroker;
use celers_core::{Broker, Result, SerializedTask, Task, TaskRegistry};
use serde::{Deserialize, Serialize};
use std::env;

// Example task: Send email
struct SendEmailTask;

#[derive(Debug, Serialize, Deserialize)]
struct EmailInput {
    to: String,
    subject: String,
    body: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmailOutput {
    message_id: String,
}

#[async_trait]
impl Task for SendEmailTask {
    type Input = EmailInput;
    type Output = EmailOutput;

    async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        println!("📧 Sending email to {}: {}", input.to, input.subject);

        // Simulate email sending
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(EmailOutput {
            message_id: format!("msg-{}", uuid::Uuid::new_v4()),
        })
    }

    fn name(&self) -> &str {
        "send_email"
    }
}

// Example task: Process image
struct ProcessImageTask;

#[derive(Debug, Serialize, Deserialize)]
struct ImageInput {
    url: String,
    operation: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ImageOutput {
    processed_url: String,
}

#[async_trait]
impl Task for ProcessImageTask {
    type Input = ImageInput;
    type Output = ImageOutput;

    async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        println!("🖼️  Processing image: {} ({})", input.url, input.operation);

        // Simulate image processing
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        Ok(ImageOutput {
            processed_url: format!("{}/processed", input.url),
        })
    }

    fn name(&self) -> &str {
        "process_image"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mode = env::args().nth(1).unwrap_or_else(|| "worker".to_string());

    // Get database URL from environment or use default
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers".to_string());

    // Create PostgreSQL broker
    let broker = PostgresBroker::new(&database_url).await?;

    // Run migrations
    println!("🔧 Running database migrations...");
    broker.migrate().await?;

    match mode.as_str() {
        "worker" => run_worker(broker).await,
        "enqueue" => enqueue_tasks(broker).await,
        "priority" => enqueue_priority_tasks(broker).await,
        _ => {
            println!(
                "Usage: cargo run --example postgres_broker_example [worker|enqueue|priority]"
            );
            println!("\nModes:");
            println!("  worker   - Run a worker that processes tasks");
            println!("  enqueue  - Enqueue some example tasks");
            println!("  priority - Enqueue tasks with different priorities");
            Ok(())
        }
    }
}

async fn run_worker(broker: PostgresBroker) -> Result<()> {
    println!("🚀 Starting worker...");
    println!("📊 Current queue size: {}", broker.queue_size().await?);

    // Create task registry
    let registry = TaskRegistry::new();

    // Register tasks
    registry.register(SendEmailTask).await;
    registry.register(ProcessImageTask).await;

    println!("✅ Registered tasks: send_email, process_image");
    println!("\n⏳ Polling for tasks (Ctrl+C to stop)...\n");

    // Simple polling loop
    loop {
        match broker.dequeue().await? {
            Some(msg) => {
                println!("📦 Received task: {}", msg.task.metadata.name);

                // Execute task
                match registry.execute(&msg.task).await {
                    Ok(result) => {
                        println!("✅ Task completed successfully");
                        if let Ok(output) = String::from_utf8(result) {
                            println!("   Output: {}", output);
                        }
                        broker
                            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                            .await?;
                    }
                    Err(e) => {
                        println!("❌ Task failed: {}", e);
                        broker
                            .reject(&msg.task.metadata.id, msg.receipt_handle.as_deref(), true)
                            .await?;
                    }
                }
            }
            None => {
                // No tasks available, wait a bit
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn enqueue_tasks(broker: PostgresBroker) -> Result<()> {
    println!("📝 Enqueuing example tasks...\n");

    // Enqueue some email tasks
    for i in 1..=3 {
        let input = EmailInput {
            to: format!("user{}@example.com", i),
            subject: format!("Hello {}", i),
            body: format!("This is test email {}", i),
        };

        let payload = serde_json::to_vec(&input).unwrap();
        let task = SerializedTask::new("send_email".to_string(), payload);

        let task_id = broker.enqueue(task).await?;
        println!("✅ Enqueued email task {}: {}", i, task_id);
    }

    // Enqueue some image processing tasks
    for i in 1..=2 {
        let input = ImageInput {
            url: format!("https://example.com/image{}.jpg", i),
            operation: "resize".to_string(),
        };

        let payload = serde_json::to_vec(&input).unwrap();
        let task = SerializedTask::new("process_image".to_string(), payload);

        let task_id = broker.enqueue(task).await?;
        println!("✅ Enqueued image task {}: {}", i, task_id);
    }

    println!("\n📊 Queue size: {}", broker.queue_size().await?);
    println!("\n💡 Run worker mode to process these tasks:");
    println!("   cargo run --example postgres_broker_example worker");

    Ok(())
}

async fn enqueue_priority_tasks(broker: PostgresBroker) -> Result<()> {
    println!("📝 Enqueuing tasks with different priorities...\n");
    println!("   Priority: higher number = higher priority\n");

    // High priority email
    let input = EmailInput {
        to: "admin@example.com".to_string(),
        subject: "URGENT: System Alert".to_string(),
        body: "Critical issue detected".to_string(),
    };
    let payload = serde_json::to_vec(&input).unwrap();
    let task = SerializedTask::new("send_email".to_string(), payload).with_priority(100); // High priority

    let task_id = broker.enqueue(task).await?;
    println!(
        "✅ Enqueued HIGH priority task: {} (priority: 100)",
        task_id
    );

    // Normal priority tasks
    for i in 1..=3 {
        let input = EmailInput {
            to: format!("user{}@example.com", i),
            subject: format!("Normal email {}", i),
            body: format!("This is test email {}", i),
        };
        let payload = serde_json::to_vec(&input).unwrap();
        let task = SerializedTask::new("send_email".to_string(), payload);

        let task_id = broker.enqueue(task).await?;
        println!(
            "✅ Enqueued NORMAL priority task: {} (priority: 0)",
            task_id
        );
    }

    // Low priority task
    let input = ImageInput {
        url: "https://example.com/background.jpg".to_string(),
        operation: "optimize".to_string(),
    };
    let payload = serde_json::to_vec(&input).unwrap();
    let task = SerializedTask::new("process_image".to_string(), payload).with_priority(-10); // Low priority

    let task_id = broker.enqueue(task).await?;
    println!("✅ Enqueued LOW priority task: {} (priority: -10)", task_id);

    println!("\n📊 Queue size: {}", broker.queue_size().await?);
    println!("\n💡 Worker will process HIGH priority task first!");
    println!("   cargo run --example postgres_broker_example worker");

    Ok(())
}
