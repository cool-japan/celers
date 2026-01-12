/// Performance benchmarks for celers-beat scheduler
///
/// These benchmarks validate the performance claims in the README:
/// - Schedule evaluation: <10ms latency
/// - Scalability: 10,000+ scheduled tasks
/// - Memory: ~100-300B per task
///
/// Run with: cargo bench --bench scheduler_benchmarks
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
use chrono::Utc;
use std::time::Instant;

fn main() {
    println!("=== Scheduler Performance Benchmarks ===\n");

    // Benchmark 1: Task Addition Performance
    benchmark_task_addition();

    // Benchmark 2: Due Task Lookup Performance
    benchmark_due_task_lookup();

    // Benchmark 3: Schedule Evaluation Performance
    benchmark_schedule_evaluation();

    // Benchmark 4: Memory Usage
    benchmark_memory_usage();

    // Benchmark 5: Priority-Based Retrieval
    benchmark_priority_retrieval();

    // Benchmark 6: Batch Operations
    benchmark_batch_operations();

    // Benchmark 7: State Persistence
    benchmark_state_persistence();

    println!("\n=== Benchmarks Complete ===");
}

/// Benchmark task addition performance
fn benchmark_task_addition() {
    println!("1. Task Addition Performance");
    println!("----------------------------------------");

    let mut scheduler = BeatScheduler::new();

    // Add 10,000 tasks
    let start = Instant::now();
    for i in 0..10_000 {
        let task = ScheduledTask::new(
            format!("task_{}", i),
            Schedule::interval(60 + (i % 100) as u64),
        );
        scheduler.add_task(task).ok();
    }
    let duration = start.elapsed();

    println!("  Added 10,000 tasks in {:?}", duration);
    println!(
        "  Average: {:.2}µs per task",
        duration.as_micros() as f64 / 10_000.0
    );
    println!(
        "  Throughput: {:.0} tasks/sec\n",
        10_000.0 / duration.as_secs_f64()
    );
}

/// Benchmark due task lookup performance
fn benchmark_due_task_lookup() {
    println!("2. Due Task Lookup Performance");
    println!("----------------------------------------");

    let mut scheduler = BeatScheduler::new();

    // Add 10,000 tasks with varying schedules
    for i in 0..10_000 {
        let task = ScheduledTask::new(
            format!("task_{}", i),
            Schedule::interval(if i % 2 == 0 { 1 } else { 3600 }),
        );
        scheduler.add_task(task).ok();
    }

    // Mark all tasks as having run (to make them not due)
    for i in 0..10_000 {
        scheduler.mark_task_run(&format!("task_{}", i)).ok();
    }

    // Benchmark due task lookup
    let iterations = 1000;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = scheduler.get_due_tasks();
    }
    let duration = start.elapsed();

    println!(
        "  Checked 10,000 tasks {} times in {:?}",
        iterations, duration
    );
    println!(
        "  Average: {:.2}µs per check",
        duration.as_micros() as f64 / iterations as f64
    );
    println!(
        "  Latency: {:.2}ms\n",
        duration.as_millis() as f64 / iterations as f64
    );

    // Verify latency claim (<10ms)
    let avg_latency_ms = duration.as_millis() as f64 / iterations as f64;
    if avg_latency_ms < 10.0 {
        println!("  ✓ PASS: Latency < 10ms ({:.2}ms)\n", avg_latency_ms);
    } else {
        println!("  ✗ FAIL: Latency >= 10ms ({:.2}ms)\n", avg_latency_ms);
    }
}

/// Benchmark schedule evaluation performance
fn benchmark_schedule_evaluation() {
    println!("3. Schedule Evaluation Performance");
    println!("----------------------------------------");

    // Benchmark interval schedule
    let interval_schedule = Schedule::interval(60);
    let iterations = 100_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = interval_schedule.next_run(Some(Utc::now()));
    }
    let duration = start.elapsed();

    println!("  Interval schedule ({} iterations):", iterations);
    println!(
        "    Average: {:.2}µs per evaluation",
        duration.as_micros() as f64 / iterations as f64
    );

    // Benchmark cron schedule (if available)
    #[cfg(feature = "cron")]
    {
        let cron_schedule = Schedule::crontab("0", "9", "*", "*", "*");
        let start = Instant::now();
        for _ in 0..10_000 {
            let _ = cron_schedule.next_run(Some(Utc::now()));
        }
        let duration = start.elapsed();

        println!("  Crontab schedule (10,000 iterations):");
        println!(
            "    Average: {:.2}µs per evaluation",
            duration.as_micros() as f64 / 10_000.0
        );
    }

    println!();
}

/// Benchmark memory usage
fn benchmark_memory_usage() {
    println!("4. Memory Usage Estimation");
    println!("----------------------------------------");

    // Create a scheduler with 1,000 tasks
    let mut scheduler = BeatScheduler::new();

    for i in 0..1_000 {
        let task = ScheduledTask::new(format!("task_{}", i), Schedule::interval(60));
        scheduler.add_task(task).ok();
    }

    // Serialize to estimate memory usage
    let serialized = serde_json::to_string(&scheduler).unwrap();
    let bytes_total = serialized.len();
    let bytes_per_task = bytes_total / 1_000;

    println!("  1,000 tasks serialized size: {} bytes", bytes_total);
    println!("  Average per task: {} bytes", bytes_per_task);

    // Check against claimed memory usage (~100-300B per task)
    if (100..=300).contains(&bytes_per_task) {
        println!("  ✓ PASS: Within claimed range (100-300B)\n");
    } else if bytes_per_task < 100 {
        println!(
            "  ✓ EXCELLENT: Better than claimed ({} < 100B)\n",
            bytes_per_task
        );
    } else {
        println!(
            "  ⚠ WARNING: Higher than claimed ({} > 300B)\n",
            bytes_per_task
        );
    }
}

/// Benchmark priority-based task retrieval
fn benchmark_priority_retrieval() {
    println!("5. Priority-Based Task Retrieval");
    println!("----------------------------------------");

    let mut scheduler = BeatScheduler::new();

    // Add 10,000 tasks with random priorities
    for i in 0..10_000 {
        let mut task = ScheduledTask::new(
            format!("task_{}", i),
            Schedule::interval(1), // All due immediately
        );
        task.options.priority = Some((i % 9) as u8 + 1); // Priorities 1-9
        scheduler.add_task(task).ok();
    }

    // Benchmark priority sorting
    let iterations = 100;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = scheduler.get_due_tasks_by_priority();
    }
    let duration = start.elapsed();

    println!(
        "  Retrieved 10,000 tasks by priority {} times in {:?}",
        iterations, duration
    );
    println!(
        "  Average: {:.2}ms per retrieval\n",
        duration.as_millis() as f64 / iterations as f64
    );
}

/// Benchmark batch operations
fn benchmark_batch_operations() {
    println!("6. Batch Operations Performance");
    println!("----------------------------------------");

    let mut scheduler = BeatScheduler::new();

    // Create 1,000 tasks for batch addition
    let tasks: Vec<ScheduledTask> = (0..1_000)
        .map(|i| ScheduledTask::new(format!("task_{}", i), Schedule::interval(60)))
        .collect();

    // Benchmark batch add
    let start = Instant::now();
    scheduler.add_tasks_batch(tasks).ok();
    let add_duration = start.elapsed();

    println!("  Batch add 1,000 tasks: {:?}", add_duration);
    println!(
        "    {:.2}µs per task",
        add_duration.as_micros() as f64 / 1_000.0
    );

    // Benchmark batch remove
    let task_names: Vec<&str> = (0..1_000)
        .map(|i| {
            // Leak the string to get a static reference
            Box::leak(format!("task_{}", i).into_boxed_str()) as &str
        })
        .collect();

    let start = Instant::now();
    scheduler.remove_tasks_batch(&task_names).ok();
    let remove_duration = start.elapsed();

    println!("  Batch remove 1,000 tasks: {:?}", remove_duration);
    println!(
        "    {:.2}µs per task\n",
        remove_duration.as_micros() as f64 / 1_000.0
    );
}

/// Benchmark state persistence
fn benchmark_state_persistence() {
    println!("7. State Persistence Performance");
    println!("----------------------------------------");

    let state_file = "/tmp/bench_scheduler_state.json";

    // Create scheduler with 1,000 tasks
    let mut scheduler = BeatScheduler::with_persistence(state_file.to_string());

    for i in 0..1_000 {
        let task = ScheduledTask::new(format!("task_{}", i), Schedule::interval(60));
        scheduler.add_task(task).ok();
    }

    // Benchmark save
    let start = Instant::now();
    scheduler.save_state().ok();
    let save_duration = start.elapsed();

    println!("  Save 1,000 tasks: {:?}", save_duration);

    // Benchmark load
    let start = Instant::now();
    BeatScheduler::load_from_file(state_file).ok();
    let load_duration = start.elapsed();

    println!("  Load 1,000 tasks: {:?}", load_duration);

    // Cleanup
    std::fs::remove_file(state_file).ok();

    println!();
}
