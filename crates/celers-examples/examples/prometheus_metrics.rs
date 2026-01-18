//! Example demonstrating Prometheus metrics integration
//!
//! This example shows how to:
//! 1. Enable metrics feature
//! 2. Run workers with metrics collection
//! 3. Expose metrics via HTTP endpoint
//! 4. Monitor task queue performance
//!
//! Run with: cargo run --example prometheus_metrics --features metrics

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_core::{Broker, SerializedTask, TaskRegistry};
use celers_macros::task;
use celers_metrics::gather_metrics;
use celers_worker::{Worker, WorkerConfig};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Example task that simulates work
#[task]
async fn process_item(item_id: u64) -> celers_core::Result<String> {
    // Simulate varying workloads
    let delay_ms = (item_id % 5) * 100;
    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;

    if item_id % 10 == 0 {
        // Simulate occasional failures
        Err(celers_core::CelersError::TaskExecution(
            "Simulated failure".to_string(),
        ))
    } else {
        Ok(format!("Processed item {}", item_id))
    }
}

/// Start HTTP server to expose Prometheus metrics
async fn start_metrics_server(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!(
        "📊 Metrics server listening on http://127.0.0.1:{}/metrics",
        port
    );

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let (reader, mut writer) = socket.split();
            let mut reader = BufReader::new(reader);
            let mut request_line = String::new();

            if reader.read_line(&mut request_line).await.is_ok() {
                if request_line.starts_with("GET /metrics") {
                    // Gather Prometheus metrics
                    let metrics = gather_metrics();

                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
                        metrics.len(),
                        metrics
                    );

                    let _ = writer.write_all(response.as_bytes()).await;
                } else {
                    // Return 404 for other paths
                    let response = "HTTP/1.1 404 Not Found\r\n\r\n";
                    let _ = writer.write_all(response.as_bytes()).await;
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    println!("=== CeleRS Prometheus Metrics Example ===\n");

    // Create broker and registry
    let broker = RedisBroker::with_mode("redis://localhost:6379", "celers", QueueMode::Fifo)?;
    let registry = TaskRegistry::new();
    registry.register(ProcessItemTask).await;

    // Start metrics HTTP server
    let metrics_port = 9090;
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_port).await {
            eprintln!("Metrics server error: {}", e);
        }
    });

    println!("✅ Metrics server started on port {}", metrics_port);
    println!(
        "📈 View metrics at: http://127.0.0.1:{}/metrics\n",
        metrics_port
    );

    // Enqueue some tasks
    println!("📤 Enqueueing tasks...");
    for i in 1..=20 {
        let task = SerializedTask::new(
            "process_item".to_string(),
            serde_json::to_vec(&ProcessItemTaskInput { item_id: i })?,
        );
        broker.enqueue(task).await?;
    }
    println!("✅ Enqueued 20 tasks\n");

    // Start worker
    println!("🚀 Starting worker...\n");
    let config = WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 500,
        max_retries: 2,
        default_timeout_secs: 10,
        ..Default::default()
    };

    let worker = Worker::new(broker, registry, config);

    println!("📊 Metrics being collected:");
    println!("  - celers_tasks_enqueued_total");
    println!("  - celers_tasks_completed_total");
    println!("  - celers_tasks_failed_total");
    println!("  - celers_tasks_retried_total");
    println!("  - celers_task_execution_seconds (histogram)");
    println!("  - celers_queue_size");
    println!("  - celers_processing_queue_size");
    println!("  - celers_dlq_size\n");

    println!("💡 Try these commands:");
    println!("  curl http://127.0.0.1:{}/metrics", metrics_port);
    println!(
        "  curl http://127.0.0.1:{}/metrics | grep celers_tasks",
        metrics_port
    );
    println!("\n⏹  Press Ctrl+C to stop\n");

    // Run worker
    worker.run().await?;

    Ok(())
}
