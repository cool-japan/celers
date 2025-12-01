//! Example demonstrating health check endpoints
//!
//! This example shows how to expose health check endpoints for:
//! - Kubernetes liveness probes
//! - Kubernetes readiness probes
//! - Load balancer health checks
//! - Monitoring systems
//!
//! Run with: cargo run --example health_checks
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//!
//! Test endpoints:
//! - curl http://localhost:8080/health - Overall health status
//! - curl http://localhost:8080/health/live - Liveness probe (is worker running?)
//! - curl http://localhost:8080/health/ready - Readiness probe (can accept work?)

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, TaskRegistry};
use celers_macros::task;
use celers_worker::health::{HealthChecker, HealthStatus};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Example task that sometimes fails
#[task]
async fn flaky_task(should_fail: bool) -> celers_core::Result<String> {
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    if should_fail {
        Err(celers_core::CelersError::TaskExecution(
            "Intentional failure for demo".to_string(),
        ))
    } else {
        Ok("Success".to_string())
    }
}

/// Start HTTP server for health checks
async fn start_health_server(port: u16, health_checker: Arc<HealthChecker>) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!(
        "🏥 Health check server listening on http://127.0.0.1:{}",
        port
    );
    println!("   GET /health - Full health status (JSON)");
    println!("   GET /health/live - Liveness probe (200 if running)");
    println!("   GET /health/ready - Readiness probe (200 if ready)\n");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let health_checker = Arc::clone(&health_checker);

        tokio::spawn(async move {
            let (reader, mut writer) = socket.split();
            let mut reader = BufReader::new(reader);
            let mut request_line = String::new();

            if reader.read_line(&mut request_line).await.is_ok() {
                let response = if request_line.starts_with("GET /health/live") {
                    // Liveness probe: Is the process running?
                    // Always return 200 if we can respond
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"alive\"}\r\n".to_string()
                } else if request_line.starts_with("GET /health/ready") {
                    // Readiness probe: Can we accept work?
                    let health = health_checker.get_health();
                    if health_checker.is_ready() {
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}\r\n",
                            serde_json::to_string(&serde_json::json!({
                                "status": "ready",
                                "tasks_processed": health.tasks_processed,
                                "is_processing": health.is_processing
                            }))
                            .unwrap()
                        )
                    } else {
                        format!(
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n\r\n{}\r\n",
                            serde_json::to_string(&serde_json::json!({
                                "status": "not_ready",
                                "reason": health.message.unwrap_or_default()
                            }))
                            .unwrap()
                        )
                    }
                } else if request_line.starts_with("GET /health") {
                    // Full health status
                    let health = health_checker.get_health();
                    let status_code = match health.status {
                        HealthStatus::Healthy => 200,
                        HealthStatus::Degraded => 200, // Still operational
                        HealthStatus::Unhealthy => 503,
                    };

                    let json = serde_json::to_string_pretty(&health).unwrap();
                    format!(
                        "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\n\r\n{}\r\n",
                        status_code, json
                    )
                } else {
                    "HTTP/1.1 404 Not Found\r\n\r\n".to_string()
                };

                let _ = writer.write_all(response.as_bytes()).await;
            }
        });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    println!("=== CeleRS Health Check Example ===\n");

    // Create broker and registry
    let broker = RedisBroker::new("redis://localhost:6379", "health_demo")?;
    let registry = TaskRegistry::new();
    registry.register(FlakyTaskTask).await;

    // Create health checker
    let health_checker = Arc::new(HealthChecker::new());

    // Start health check HTTP server
    let health_port = 8080;
    let health_checker_clone = Arc::clone(&health_checker);
    tokio::spawn(async move {
        if let Err(e) = start_health_server(health_port, health_checker_clone).await {
            eprintln!("Health server error: {}", e);
        }
    });

    // Enqueue some tasks (mix of success and failure)
    println!("📤 Enqueueing tasks...\n");
    for i in 1..=10 {
        let task = SerializedTask::new(
            "flaky_task".to_string(),
            serde_json::to_vec(&FlakyTaskTaskInput {
                should_fail: i % 3 == 0, // Every 3rd task fails
            })?,
        );
        broker.enqueue(task).await?;
    }

    println!("✅ Enqueued 10 tasks (some will fail intentionally)\n");

    // Simulate task processing with health tracking
    println!("🚀 Processing tasks with health tracking...\n");
    println!("💡 Try these commands in another terminal:");
    println!("   curl http://localhost:8080/health");
    println!("   curl http://localhost:8080/health/live");
    println!("   curl http://localhost:8080/health/ready");
    println!("   watch -n 1 'curl -s http://localhost:8080/health | jq .'");
    println!("\n⏹  Press Ctrl+C to stop\n");

    // For demonstration, manually process tasks and update health
    // In real usage, integrate HealthChecker into Worker
    loop {
        match broker.dequeue().await? {
            Some(msg) => {
                health_checker.set_processing(true);

                let task_name = msg.task.metadata.name.clone();
                let input: FlakyTaskTaskInput = serde_json::from_slice(&msg.task.payload).unwrap();

                println!(
                    "Processing task: {} (will_fail: {})",
                    task_name, input.should_fail
                );

                // Simulate task execution
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                if input.should_fail {
                    health_checker.record_failure();
                    println!("  ❌ Task failed");
                    let _ = broker
                        .reject(&msg.task.metadata.id, msg.receipt_handle.as_deref(), true)
                        .await;
                } else {
                    health_checker.record_success();
                    println!("  ✅ Task succeeded");
                    let _ = broker
                        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                        .await;
                }

                health_checker.set_processing(false);

                // Show current health
                let health = health_checker.get_health();
                println!(
                    "  📊 Health: {:?} (processed: {}, failures: {})\n",
                    health.status, health.tasks_processed, health.consecutive_failures
                );
            }
            None => {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
        }
    }
}
