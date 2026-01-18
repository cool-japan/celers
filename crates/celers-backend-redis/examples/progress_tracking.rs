/// Progress Tracking Example
///
/// Demonstrates:
/// - Setting and retrieving task progress
/// - Progress updates for long-running tasks
/// - Progress with messages
use celers_backend_redis::{ProgressInfo, RedisResultBackend, ResultBackend, TaskMeta, TaskResult};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Result Backend: Progress Tracking ===\n");

    let mut backend = RedisResultBackend::new("redis://127.0.0.1:6379")?;
    println!("✓ Connected to Redis\n");

    // Example 1: Basic progress tracking
    println!("=== Example 1: Basic Progress Tracking ===");
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "long_running.process".to_string());
    meta.result = TaskResult::Started;
    meta.started_at = Some(chrono::Utc::now());
    meta.worker = Some("worker-1".to_string());

    backend.store_result(task_id, &meta).await?;
    println!("✓ Task started: {}", task_id);

    // Simulate progress updates
    let total_steps = 100;
    for step in (0..=total_steps).step_by(20) {
        let progress = ProgressInfo::new(step, total_steps)
            .with_message(format!("Processing step {}/{}", step, total_steps));

        meta.progress = Some(progress);
        backend.store_result(task_id, &meta).await?;

        println!("✓ Progress: {}%", step);
        sleep(Duration::from_millis(200)).await;
    }

    // Mark complete
    meta.result = TaskResult::Success(serde_json::json!({"processed": total_steps}));
    meta.completed_at = Some(chrono::Utc::now());
    backend.store_result(task_id, &meta).await?;
    println!("✓ Task completed\n");

    // Example 2: Progress with detailed messages
    println!("=== Example 2: Progress with Messages ===");
    let task_id2 = Uuid::new_v4();
    let mut meta2 = TaskMeta::new(task_id2, "batch.processor".to_string());
    meta2.result = TaskResult::Started;
    meta2.started_at = Some(chrono::Utc::now());

    backend.store_result(task_id2, &meta2).await?;

    let stages = vec![
        (0, "Initializing..."),
        (10, "Loading configuration..."),
        (25, "Connecting to database..."),
        (40, "Fetching data..."),
        (60, "Processing records..."),
        (80, "Validating results..."),
        (95, "Finalizing..."),
        (100, "Complete!"),
    ];

    let stages_len = stages.len();
    for (current, message) in stages {
        let progress = ProgressInfo::new(current, 100).with_message(message.to_string());

        meta2.progress = Some(progress);
        backend.store_result(task_id2, &meta2).await?;

        println!("✓ [{}%] {}", current, message);
        sleep(Duration::from_millis(300)).await;
    }

    meta2.result = TaskResult::Success(serde_json::json!({"stages_completed": stages_len}));
    meta2.completed_at = Some(chrono::Utc::now());
    backend.store_result(task_id2, &meta2).await?;
    println!("\n=== Example 3: Retrieve Progress ===");

    // Create a task with progress
    let task_id3 = Uuid::new_v4();
    let mut meta3 = TaskMeta::new(task_id3, "data.analysis".to_string());
    meta3.result = TaskResult::Started;
    meta3.progress =
        Some(ProgressInfo::new(45, 150).with_message("Analyzing batch 3 of 10".to_string()));

    backend.store_result(task_id3, &meta3).await?;

    // Retrieve and display progress
    if let Some(progress) = backend.get_progress(task_id3).await? {
        println!("✓ Retrieved progress:");
        println!("  Current: {}/{}", progress.current, progress.total);
        println!("  Percent: {:.1}%", progress.percent);
        if let Some(msg) = &progress.message {
            println!("  Message: {}", msg);
        }
        println!("  Updated: {}", progress.updated_at);
    }

    println!("\n✅ All progress tracking examples completed!");
    Ok(())
}
