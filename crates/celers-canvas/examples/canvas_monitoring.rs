//! Monitoring examples: metrics collection, rate limiting, concurrency control

use celers_canvas::{
    ResourceUtilization, WorkflowConcurrencyControl, WorkflowMetricsCollector, WorkflowRateLimiter,
    WorkflowResourceMonitor,
};
use uuid::Uuid;

fn main() {
    println!("=== Monitoring Examples ===\n");

    // Example 1: Metrics Collection
    println!("1. Workflow Metrics Collection:");
    let mut metrics = WorkflowMetricsCollector::new(Uuid::new_v4());

    // Simulate task starts and completions
    for i in 0..8 {
        let task_id = Uuid::new_v4();
        metrics.record_task_start(task_id);
        metrics.record_task_complete(task_id, 100 + i * 50);
    }
    for i in 0..2 {
        let task_id = Uuid::new_v4();
        metrics.record_task_start(task_id);
        metrics.record_task_failure(task_id, 150 + i * 50);
    }
    println!("   Total tasks: {}", metrics.total_tasks);

    metrics.finalize();
    println!("   {}", metrics.summary());
    println!();

    // Example 2: Rate Limiting
    println!("2. Workflow Rate Limiting:");
    let mut rate_limiter = WorkflowRateLimiter::new(5, 1000); // 5 workflows per second
    println!(
        "   Max workflows: {} per {}ms",
        rate_limiter.max_workflows, rate_limiter.window_ms
    );

    // Simulate workflow submissions
    for i in 1..=7 {
        let allowed = rate_limiter.allow_workflow();
        println!(
            "   Workflow {}: {}",
            i,
            if allowed { "Allowed" } else { "Rejected" }
        );
    }
    println!(
        "   Total processed: {}, Rejected: {}",
        rate_limiter.total_workflows, rate_limiter.rejected_workflows
    );
    println!(
        "   Current rate: {:.2} workflows/sec",
        rate_limiter.current_rate()
    );
    println!();

    // Example 3: Concurrency Control
    println!("3. Workflow Concurrency Control:");
    let mut concurrency = WorkflowConcurrencyControl::new(3); // Max 3 concurrent workflows
    println!("   Max concurrent: {}", concurrency.max_concurrent);

    // Simulate workflow lifecycle
    let wf1 = Uuid::new_v4();
    let wf2 = Uuid::new_v4();
    let wf3 = Uuid::new_v4();
    let wf4 = Uuid::new_v4();

    println!("   Starting workflow 1: {}", concurrency.try_start(wf1));
    println!("   Starting workflow 2: {}", concurrency.try_start(wf2));
    println!("   Starting workflow 3: {}", concurrency.try_start(wf3));
    println!(
        "   Starting workflow 4: {} (should fail)",
        concurrency.try_start(wf4)
    );

    println!(
        "   Current concurrent: {}",
        concurrency.current_concurrency()
    );
    println!("   Available slots: {}", concurrency.available_slots());

    concurrency.complete(wf1);
    println!("   After completing workflow 1:");
    println!(
        "     Current concurrent: {}",
        concurrency.current_concurrency()
    );
    println!("     Available slots: {}", concurrency.available_slots());
    println!();

    // Example 4: Resource Monitoring
    println!("4. Workflow Resource Monitoring:");
    let workflow_id = Uuid::new_v4();
    let mut resource_monitor = WorkflowResourceMonitor::new(workflow_id).with_max_history(100); // Keep last 100 samples
    println!("   Max history size: {}", resource_monitor.max_history);

    // Simulate resource snapshots (values are 0.0 to 1.0)
    resource_monitor.record(ResourceUtilization::new(0.505, 0.512, 0.1, 0.2)); // 50.5% CPU, 51.2% memory, etc.
    resource_monitor.record(ResourceUtilization::new(0.652, 0.768, 0.15, 0.25));
    resource_monitor.record(ResourceUtilization::new(0.458, 0.640, 0.12, 0.18));

    if let Some(avg) = resource_monitor.average_utilization(60) {
        println!("   Average CPU: {:.2}%", avg.cpu * 100.0);
        println!("   Average Memory: {:.2}%", avg.memory * 100.0);
    }
    if let Some(peak) = resource_monitor.peak_utilization() {
        println!("   Peak CPU: {:.2}%", peak.cpu * 100.0);
        println!("   Peak Memory: {:.2}%", peak.memory * 100.0);
    }
    println!();

    // Example 5: Combined Monitoring
    println!("5. Combined Monitoring Dashboard:");
    println!("   ┌─────────────────────────────────────────┐");
    println!("   │ Workflow Monitoring Dashboard          │");
    println!("   ├─────────────────────────────────────────┤");
    println!(
        "   │ Active Workflows: {}/{}                  │",
        concurrency.current_concurrency(),
        concurrency.max_concurrent
    );
    println!(
        "   │ Rate: {:.1}/sec (limit: {}/sec)        │",
        rate_limiter.current_rate(),
        rate_limiter.max_workflows
    );
    if let Some(avg) = resource_monitor.average_utilization(60) {
        println!(
            "   │ Avg CPU: {:.1}%                        │",
            avg.cpu * 100.0
        );
        println!(
            "   │ Avg Memory: {:.1}%                     │",
            avg.memory * 100.0
        );
    }
    println!("   └─────────────────────────────────────────┘");
    println!();

    println!("=== All monitoring patterns demonstrated ===");
}
