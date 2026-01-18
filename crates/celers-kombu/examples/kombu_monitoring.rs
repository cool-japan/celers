//! Monitoring and Observability Example
//!
//! This example demonstrates how to use celers-kombu's monitoring utilities
//! and middleware for production observability, including:
//! - Consumer lag analysis and autoscaling recommendations
//! - Message flow velocity tracking
//! - Worker scaling decisions
//! - Message age distribution analysis
//! - Processing capacity estimation
//! - Deadline enforcement
//! - Content type validation
//! - Dynamic routing

use celers_kombu::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== Monitoring and Observability Example ===\n");

    // ============================================================================
    // 1. Consumer Lag Analysis and Autoscaling
    // ============================================================================
    println!("1. Consumer Lag Analysis and Autoscaling");
    println!("----------------------------------------");

    // Scenario: Queue has 5000 messages, consuming at 50 msg/s, producing at 100 msg/s
    let (lag_secs, falling_behind, action) = utils::analyze_consumer_lag(5000, 50, 100);
    println!("Queue size: 5000 messages");
    println!("Consumption rate: 50 msg/s");
    println!("Production rate: 100 msg/s");
    println!("  → Lag: {} seconds", lag_secs);
    println!("  → Falling behind: {}", falling_behind);
    println!("  → Recommended action: {}", action);
    println!();

    // Stable scenario
    let (lag_secs, falling_behind, action) = utils::analyze_consumer_lag(100, 100, 50);
    println!("Queue size: 100 messages");
    println!("Consumption rate: 100 msg/s");
    println!("Production rate: 50 msg/s");
    println!("  → Lag: {} seconds", lag_secs);
    println!("  → Falling behind: {}", falling_behind);
    println!("  → Recommended action: {}", action);
    println!();

    // ============================================================================
    // 2. Message Velocity and Queue Growth Trends
    // ============================================================================
    println!("2. Message Velocity and Queue Growth Trends");
    println!("-------------------------------------------");

    // Queue is growing
    let (velocity, trend) = utils::calculate_message_velocity(1000, 500, 60);
    println!("Queue grew from 500 to 1000 messages in 60 seconds");
    println!("  → Velocity: {} msg/s", velocity);
    println!("  → Trend: {}", trend);
    println!();

    // Queue is shrinking
    let (velocity, trend) = utils::calculate_message_velocity(200, 500, 60);
    println!("Queue shrunk from 500 to 200 messages in 60 seconds");
    println!("  → Velocity: {} msg/s", velocity);
    println!("  → Trend: {}", trend);
    println!();

    // ============================================================================
    // 3. Worker Scaling Recommendations
    // ============================================================================
    println!("3. Worker Scaling Recommendations");
    println!("---------------------------------");

    // Large queue scenario
    let (workers, action) = utils::suggest_worker_scaling(10000, 5, 100, 60);
    println!("Queue: 10000 messages, 5 workers, 100ms/msg, target 60s latency");
    println!("  → Recommended workers: {}", workers);
    println!("  → Scaling action: {}", action);
    println!();

    // Small queue scenario
    let (workers, action) = utils::suggest_worker_scaling(50, 20, 100, 60);
    println!("Queue: 50 messages, 20 workers, 100ms/msg, target 60s latency");
    println!("  → Recommended workers: {}", workers);
    println!("  → Scaling action: {}", action);
    println!();

    // ============================================================================
    // 4. Message Age Distribution (SLA Monitoring)
    // ============================================================================
    println!("4. Message Age Distribution (SLA Monitoring)");
    println!("--------------------------------------------");

    // Simulate message ages
    let message_ages: Vec<u64> = vec![
        1, 2, 3, 5, 8, 10, 12, 15, 18, 20, // Recent messages
        25, 30, 35, 40, 45, 50, 60, 70, // Moderate delay
        120, 180, 240, 300, // Old messages
    ];

    let (p50, p95, p99, max) = utils::calculate_message_age_distribution(&message_ages);
    println!("Message ages in queue:");
    println!("  → p50 (median): {} seconds", p50);
    println!("  → p95: {} seconds", p95);
    println!("  → p99: {} seconds", p99);
    println!("  → Max age: {} seconds", max);

    // SLA check
    let sla_threshold = 60; // 60 second SLA
    if p95 > sla_threshold {
        println!(
            "  ⚠️  WARNING: p95 exceeds SLA threshold of {}s",
            sla_threshold
        );
    } else {
        println!("  ✓ SLA: p95 within {}s threshold", sla_threshold);
    }
    println!();

    // ============================================================================
    // 5. Processing Capacity Estimation
    // ============================================================================
    println!("5. Processing Capacity Estimation");
    println!("---------------------------------");

    let (per_sec, per_min, per_hour) = utils::estimate_processing_capacity(10, 100, 4);
    println!("System: 10 workers, 100ms/msg, 4 concurrent tasks/worker");
    println!("  → Capacity: {} msg/s", per_sec);
    println!("  → Capacity: {} msg/min", per_min);
    println!("  → Capacity: {} msg/hour", per_hour);
    println!();

    // Compare with production rate
    let production_rate = 500; // msg/s
    if per_sec < production_rate {
        println!(
            "  ⚠️  WARNING: Capacity ({} msg/s) < Production ({} msg/s)",
            per_sec, production_rate
        );
        let needed_workers = (production_rate as f64 / (per_sec as f64 / 10.0)).ceil() as usize;
        println!("  → Recommended workers: {}", needed_workers);
    } else {
        println!("  ✓ Capacity sufficient for production load");
    }
    println!();

    // ============================================================================
    // 6. Middleware: Deadline Enforcement
    // ============================================================================
    println!("6. Middleware: Deadline Enforcement");
    println!("-----------------------------------");

    let _deadline_middleware = DeadlineMiddleware::new(Duration::from_secs(300));
    println!("Deadline middleware configured: 5 minute deadline");
    println!("  → Automatically adds x-deadline header to messages");
    println!("  → Marks messages as deadline-exceeded if past deadline");
    println!("  → Useful for: time-sensitive operations, SLA enforcement");
    println!();

    // ============================================================================
    // 7. Middleware: Content Type Validation
    // ============================================================================
    println!("7. Middleware: Content Type Validation");
    println!("--------------------------------------");

    let _content_type_middleware = ContentTypeMiddleware::new(vec![
        "application/json".to_string(),
        "application/msgpack".to_string(),
    ])
    .with_default("application/json".to_string());

    println!("Content type middleware configured:");
    println!("  → Allowed: application/json, application/msgpack");
    println!("  → Default: application/json");
    println!("  → Validates content types on publish");
    println!("  → Warns on unexpected content types during consume");
    println!();

    // ============================================================================
    // 8. Middleware: Dynamic Routing
    // ============================================================================
    println!("8. Middleware: Dynamic Routing");
    println!("------------------------------");

    let _routing_middleware = RoutingKeyMiddleware::from_task_and_priority();
    println!("Routing middleware configured: task name + priority");
    println!("  → Automatically generates routing keys");
    println!("  → Example: tasks.send_email.priority_5");
    println!("  → Enables priority-based routing and load balancing");
    println!();

    // ============================================================================
    // 9. Complete Monitoring Pipeline
    // ============================================================================
    println!("9. Complete Monitoring Pipeline");
    println!("-------------------------------");

    println!("Production monitoring workflow:");
    println!();
    println!("  1. Poll queue metrics every 30 seconds");
    println!("     ├─ Get queue size");
    println!("     ├─ Get consumption/production rates");
    println!("     └─ Calculate message ages");
    println!();
    println!("  2. Analyze consumer lag");
    println!("     ├─ If falling behind → scale_up workers");
    println!("     ├─ If over-provisioned → scale_down workers");
    println!("     └─ If stable → maintain current capacity");
    println!();
    println!("  3. Monitor SLAs");
    println!("     ├─ Check p95/p99 message age");
    println!("     ├─ Alert if exceeding thresholds");
    println!("     └─ Track deadline violations");
    println!();
    println!("  4. Capacity planning");
    println!("     ├─ Estimate required capacity");
    println!("     ├─ Compare with current capacity");
    println!("     └─ Recommend scaling actions");
    println!();
    println!("  5. Message validation");
    println!("     ├─ Enforce content type policies");
    println!("     ├─ Validate message deadlines");
    println!("     └─ Route based on priority");
    println!();

    println!("=== Example Complete ===");
}
