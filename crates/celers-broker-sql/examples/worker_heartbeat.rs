//! Worker Heartbeat System Example
//!
//! This example demonstrates how to use the worker heartbeat system for
//! health monitoring and failure detection in distributed worker deployments.
//!
//! Features demonstrated:
//! - Worker registration with capabilities
//! - Heartbeat updates with status changes
//! - Monitoring all workers
//! - Detecting stale/offline workers
//! - Worker health dashboards
//!
//! Run with:
//! ```bash
//! cargo run --example worker_heartbeat
//! ```

use celers_broker_sql::{MysqlBroker, WorkerStatus};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Worker Heartbeat System Example ===\n");

    // Database connection
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers".to_string());

    println!("Connecting to database...");
    let broker = MysqlBroker::new(&database_url).await?;
    println!("Connected successfully!\n");

    // Example 1: Worker registration with capabilities
    println!("--- Example 1: Worker Registration ---");

    println!("Registering workers with different capabilities...\n");

    // Register worker 1: CPU-intensive tasks
    broker
        .register_worker(
            "worker-cpu-001",
            WorkerStatus::Active,
            Some(json!({
                "cpu_cores": 8,
                "memory_gb": 16,
                "capabilities": ["cpu_intensive", "data_processing"],
                "region": "us-east-1",
                "version": "2.0.0"
            })),
        )
        .await?;
    println!("✓ Registered worker-cpu-001 (CPU-intensive)");

    // Register worker 2: IO-intensive tasks
    broker
        .register_worker(
            "worker-io-001",
            WorkerStatus::Active,
            Some(json!({
                "cpu_cores": 4,
                "memory_gb": 32,
                "capabilities": ["io_intensive", "database_operations"],
                "region": "us-east-1",
                "version": "2.0.0"
            })),
        )
        .await?;
    println!("✓ Registered worker-io-001 (IO-intensive)");

    // Register worker 3: GPU tasks
    broker
        .register_worker(
            "worker-gpu-001",
            WorkerStatus::Active,
            Some(json!({
                "cpu_cores": 16,
                "memory_gb": 64,
                "gpu": "NVIDIA A100",
                "capabilities": ["ml_training", "inference"],
                "region": "us-west-2",
                "version": "2.0.0"
            })),
        )
        .await?;
    println!("✓ Registered worker-gpu-001 (GPU-accelerated)");

    // Example 2: Heartbeat updates with status changes
    println!("\n--- Example 2: Heartbeat Updates ---");

    println!("Simulating worker lifecycle...\n");

    // Worker becomes idle
    println!("worker-cpu-001: Active -> Idle");
    broker
        .update_worker_heartbeat("worker-cpu-001", WorkerStatus::Idle)
        .await?;
    sleep(Duration::from_millis(500)).await;

    // Worker becomes busy
    println!("worker-cpu-001: Idle -> Busy");
    broker
        .update_worker_heartbeat("worker-cpu-001", WorkerStatus::Busy)
        .await?;
    sleep(Duration::from_millis(500)).await;

    // Worker completes task and becomes active
    println!("worker-cpu-001: Busy -> Active");
    broker
        .update_worker_heartbeat("worker-cpu-001", WorkerStatus::Active)
        .await?;

    // Example 3: Monitoring all workers
    println!("\n--- Example 3: Worker Monitoring ---");

    let workers = broker.get_all_worker_heartbeats(60).await?;
    println!("Active workers: {}\n", workers.len());

    for worker in &workers {
        println!("Worker: {}", worker.worker_id);
        println!("  Status: {}", worker.status);
        println!(
            "  Last heartbeat: {}",
            worker.last_heartbeat.format("%H:%M:%S")
        );
        println!("  Task count: {}", worker.task_count);
        if let Some(caps) = &worker.capabilities {
            println!("  Capabilities: {}", caps);
        }
        println!();
    }

    // Example 4: Stale worker detection
    println!("--- Example 4: Stale Worker Detection ---");

    // Register a worker and don't update its heartbeat
    broker
        .register_worker(
            "worker-stale-001",
            WorkerStatus::Active,
            Some(json!({"test": true})),
        )
        .await?;
    println!("Registered worker-stale-001\n");

    // Wait for it to become stale (using a very short threshold for demo)
    println!("Waiting 3 seconds for worker to become stale...");
    sleep(Duration::from_secs(3)).await;

    // Check workers with a 2-second staleness threshold
    let workers = broker.get_all_worker_heartbeats(2).await?;

    println!("\nWorker status (2-second staleness threshold):");
    for worker in &workers {
        let status_indicator = match worker.status {
            WorkerStatus::Offline => "🔴",
            WorkerStatus::Busy => "🟡",
            WorkerStatus::Active => "🟢",
            WorkerStatus::Idle => "⚪",
        };
        println!(
            "{} {} - {}",
            status_indicator, worker.worker_id, worker.status
        );
    }

    // Example 5: Worker health dashboard
    println!("\n--- Example 5: Worker Health Dashboard ---");

    // Register more workers with various states
    for i in 1..=5 {
        let status = match i % 3 {
            0 => WorkerStatus::Busy,
            1 => WorkerStatus::Active,
            _ => WorkerStatus::Idle,
        };

        broker
            .register_worker(
                &format!("worker-pool-{:03}", i),
                status,
                Some(json!({
                    "pool": "general",
                    "instance_id": format!("i-{:08x}", i)
                })),
            )
            .await?;
    }

    let workers = broker.get_all_worker_heartbeats(60).await?;

    // Generate dashboard
    println!("\n╔══════════════════════════════════════════════╗");
    println!("║         WORKER HEALTH DASHBOARD           ║");
    println!("╠══════════════════════════════════════════════╣");

    let total = workers.len();
    let active = workers
        .iter()
        .filter(|w| w.status == WorkerStatus::Active)
        .count();
    let busy = workers
        .iter()
        .filter(|w| w.status == WorkerStatus::Busy)
        .count();
    let idle = workers
        .iter()
        .filter(|w| w.status == WorkerStatus::Idle)
        .count();
    let offline = workers
        .iter()
        .filter(|w| w.status == WorkerStatus::Offline)
        .count();

    println!("║ Total Workers:    {:>4}                     ║", total);
    println!("║ 🟢 Active:         {:>4}                     ║", active);
    println!("║ 🟡 Busy:           {:>4}                     ║", busy);
    println!("║ ⚪ Idle:           {:>4}                     ║", idle);
    println!("║ 🔴 Offline:        {:>4}                     ║", offline);
    println!("╚══════════════════════════════════════════════╝\n");

    // Worker details table
    println!("Worker Details:");
    println!("┌─────────────────────┬──────────┬─────────────┬────────────┐");
    println!("│ Worker ID           │ Status   │ Last Update │ Task Count │");
    println!("├─────────────────────┼──────────┼─────────────┼────────────┤");

    for worker in workers.iter().take(10) {
        let time_ago = chrono::Utc::now()
            .signed_duration_since(worker.last_heartbeat)
            .num_seconds();
        println!(
            "│ {:<19} │ {:>8} │ {:>8}s   │ {:>10} │",
            worker.worker_id,
            worker.status.to_string(),
            time_ago,
            worker.task_count
        );
    }
    println!("└─────────────────────┴──────────┴─────────────┴────────────┘");

    // Example 6: Worker lifecycle simulation
    println!("\n--- Example 6: Worker Lifecycle Simulation ---");

    let worker_id = "worker-lifecycle-demo";
    println!("Simulating complete worker lifecycle for {}:\n", worker_id);

    // 1. Registration
    println!("1. Worker starts up and registers");
    broker
        .register_worker(
            worker_id,
            WorkerStatus::Active,
            Some(json!({"startup_time": chrono::Utc::now()})),
        )
        .await?;
    sleep(Duration::from_secs(1)).await;

    // 2. Idle state
    println!("2. Worker is idle, waiting for tasks");
    broker
        .update_worker_heartbeat(worker_id, WorkerStatus::Idle)
        .await?;
    sleep(Duration::from_secs(1)).await;

    // 3. Processing tasks
    println!("3. Worker receives task and becomes busy");
    broker
        .update_worker_heartbeat(worker_id, WorkerStatus::Busy)
        .await?;
    sleep(Duration::from_secs(2)).await;

    // 4. Back to active
    println!("4. Worker completes task, back to active");
    broker
        .update_worker_heartbeat(worker_id, WorkerStatus::Active)
        .await?;
    sleep(Duration::from_secs(1)).await;

    // 5. Multiple task cycles
    println!("5. Worker processes multiple tasks");
    for i in 1..=3 {
        println!("   - Processing task {}", i);
        broker
            .update_worker_heartbeat(worker_id, WorkerStatus::Busy)
            .await?;
        sleep(Duration::from_millis(500)).await;
        broker
            .update_worker_heartbeat(worker_id, WorkerStatus::Active)
            .await?;
        sleep(Duration::from_millis(300)).await;
    }

    // 6. Idle before shutdown
    println!("6. Worker idle before graceful shutdown");
    broker
        .update_worker_heartbeat(worker_id, WorkerStatus::Idle)
        .await?;
    sleep(Duration::from_secs(1)).await;

    println!("7. Worker shuts down gracefully");
    println!("   (Worker stops sending heartbeats)");
    sleep(Duration::from_secs(3)).await;

    // Check final status
    let workers = broker.get_all_worker_heartbeats(2).await?;
    if let Some(worker) = workers.iter().find(|w| w.worker_id == worker_id) {
        println!(
            "   Final status: {} (automatically detected)",
            worker.status
        );
    }

    println!("\n=== Example Complete ===");
    println!("\nBest Practices:");
    println!("  - Send heartbeats every 30-60 seconds");
    println!("  - Set staleness threshold to 2-3x heartbeat interval");
    println!("  - Include meaningful capabilities in worker registration");
    println!("  - Monitor offline workers and trigger alerts");
    println!("  - Use worker status for load balancing decisions");
    println!("  - Track worker task counts for capacity planning");

    Ok(())
}
