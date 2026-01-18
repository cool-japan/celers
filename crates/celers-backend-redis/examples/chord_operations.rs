/// Chord Operations Example
///
/// Demonstrates:
/// - Creating and initializing chord states
/// - Tracking chord completion (barrier synchronization)
/// - Chord timeouts
/// - Chord cancellation
/// - Partial results retrieval
use celers_backend_redis::{ChordState, RedisResultBackend, ResultBackend, TaskMeta, TaskResult};
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Result Backend: Chord Operations ===\n");

    let mut backend = RedisResultBackend::new("redis://127.0.0.1:6379")?;
    println!("✓ Connected to Redis\n");

    // Example 1: Basic chord barrier
    println!("=== Example 1: Basic Chord Barrier ===");
    let chord_id = Uuid::new_v4();
    let task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    // Initialize chord state
    let chord_state = ChordState::new(chord_id, task_ids.len(), task_ids.clone());
    backend.chord_init(chord_state).await?;
    println!("✓ Initialized chord with {} tasks", task_ids.len());

    // Simulate tasks completing one by one
    for (i, &task_id) in task_ids.iter().enumerate() {
        // Store task result
        let mut meta = TaskMeta::new(task_id, format!("chord.task_{}", i));
        meta.result = TaskResult::Success(serde_json::json!({"index": i}));
        meta.completed_at = Some(chrono::Utc::now());
        meta.worker = Some(format!("worker-{}", i));

        backend.store_result(task_id, &meta).await?;

        // Increment chord counter
        let completed = backend.chord_complete_task(chord_id).await?;
        println!("✓ Task {} completed ({}/{})", i, completed, task_ids.len());

        // Check if chord is complete
        if let Some(state) = backend.chord_get_state(chord_id).await? {
            if state.is_complete() {
                println!("🎉 Chord completed! All {} tasks finished", task_ids.len());
                break;
            }
        }
    }

    // Example 2: Chord with timeout
    println!("\n=== Example 2: Chord with Timeout ===");
    let timeout_chord_id = Uuid::new_v4();
    let timeout_task_ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();

    let timeout_state = ChordState::new(
        timeout_chord_id,
        timeout_task_ids.len(),
        timeout_task_ids.clone(),
    )
    .with_timeout(Duration::from_secs(60));

    backend.chord_init(timeout_state).await?;
    println!("✓ Initialized chord with 60 second timeout");

    // Complete some tasks
    for _ in 0..7 {
        backend.chord_complete_task(timeout_chord_id).await?;
    }

    if let Some(state) = backend.chord_get_state(timeout_chord_id).await? {
        println!("✓ Chord progress: {}/{}", state.completed, state.total);
        println!("  Percentage: {:.1}%", state.percent_complete());
        println!("  Remaining: {}", state.remaining());
        if let Some(remaining_time) = state.remaining_timeout() {
            println!("  Time remaining: {:?}", remaining_time);
        }
    }

    // Example 3: Chord cancellation
    println!("\n=== Example 3: Chord Cancellation ===");
    let cancel_chord_id = Uuid::new_v4();
    let cancel_task_ids: Vec<Uuid> = (0..20).map(|_| Uuid::new_v4()).collect();

    let cancel_state = ChordState::new(cancel_chord_id, cancel_task_ids.len(), cancel_task_ids);
    backend.chord_init(cancel_state).await?;
    println!("✓ Initialized chord with 20 tasks");

    // Complete some tasks
    for _ in 0..5 {
        backend.chord_complete_task(cancel_chord_id).await?;
    }

    // Cancel the chord
    backend
        .chord_cancel(
            cancel_chord_id,
            Some("User cancelled operation".to_string()),
        )
        .await?;
    println!("✓ Cancelled chord");

    if let Some(state) = backend.chord_get_state(cancel_chord_id).await? {
        println!("  Cancelled: {}", state.cancelled);
        if let Some(reason) = &state.cancellation_reason {
            println!("  Reason: {}", reason);
        }
        println!(
            "  Progress at cancellation: {}/{}",
            state.completed, state.total
        );
    }

    // Example 4: Partial chord results
    println!("\n=== Example 4: Partial Chord Results ===");
    let partial_chord_id = Uuid::new_v4();
    let partial_task_ids: Vec<Uuid> = (0..8).map(|_| Uuid::new_v4()).collect();

    let partial_state = ChordState::new(
        partial_chord_id,
        partial_task_ids.len(),
        partial_task_ids.clone(),
    );
    backend.chord_init(partial_state).await?;
    println!("✓ Initialized chord with {} tasks", partial_task_ids.len());

    // Store results for some tasks only
    for (i, &task_id) in partial_task_ids.iter().enumerate().take(5) {
        let mut meta = TaskMeta::new(task_id, format!("partial.task_{}", i));
        meta.result = TaskResult::Success(serde_json::json!({"value": i * 10}));
        meta.completed_at = Some(chrono::Utc::now());

        backend.store_result(task_id, &meta).await?;
        backend.chord_complete_task(partial_chord_id).await?;
    }

    // Get partial results
    let partial_results = backend.chord_get_partial_results(partial_chord_id).await?;
    println!("✓ Retrieved partial results:");
    for (task_id, meta_opt) in partial_results {
        match meta_opt {
            Some(meta) => println!("  {} - {:?}", task_id, meta.result),
            None => println!("  {} - Not yet completed", task_id),
        }
    }

    // Example 5: Chord retry logic
    println!("\n=== Example 5: Chord Retry Logic ===");
    let retry_chord_id = Uuid::new_v4();
    let retry_task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    let mut retry_state = ChordState::new(retry_chord_id, retry_task_ids.len(), retry_task_ids);
    retry_state.max_retries = Some(3);
    retry_state.retry_count = 1;

    backend.chord_init(retry_state).await?;
    println!("✓ Initialized chord with retry capability");
    println!("  Max retries: 3");
    println!("  Current retry: 1");

    // Attempt retry
    if backend.chord_retry(retry_chord_id).await? {
        println!("✓ Chord retry initiated");
        if let Some(state) = backend.chord_get_state(retry_chord_id).await? {
            println!("  Retry count: {}", state.retry_count);
            println!("  Can retry more: {}", state.can_retry());
            if let Some(remaining) = state.remaining_retries() {
                println!("  Remaining retries: {}", remaining);
            }
        }
    } else {
        println!("✗ Chord cannot be retried (max retries reached)");
    }

    // Example 6: Chord state display
    println!("\n=== Example 6: Chord State Display ===");
    let display_chord_id = Uuid::new_v4();
    let display_task_ids: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();

    let mut display_state = ChordState::new(
        display_chord_id,
        display_task_ids.len(),
        display_task_ids.clone(),
    )
    .with_timeout(Duration::from_secs(300));

    display_state.completed = 75;
    display_state.retry_count = 1;
    display_state.max_retries = Some(3);

    backend.chord_init(display_state).await?;

    if let Some(state) = backend.chord_get_state(display_chord_id).await? {
        println!("✓ Chord state:");
        println!("{}", state);
        println!("\n  Statistics:");
        println!("    Completion: {:.1}%", state.percent_complete());
        println!("    Remaining tasks: {}", state.remaining());
        println!("    Is complete: {}", state.is_complete());
        println!("    Can retry: {}", state.can_retry());
    }

    println!("\n✅ All chord operations demonstrated successfully!");
    Ok(())
}
