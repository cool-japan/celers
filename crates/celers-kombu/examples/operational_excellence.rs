//! Operational Excellence Example
//!
//! This example demonstrates the v0.4.7 features for production operational excellence:
//! - IdempotencyMiddleware for exactly-once processing
//! - BackoffMiddleware for intelligent retry handling
//! - Operational utility functions for monitoring and alerting
//!
//! Run with: cargo run --example operational_excellence

use celers_kombu::*;
use celers_protocol::Message;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    println!("=== Operational Excellence Example ===\n");

    // ============================================================================
    // Part 1: Idempotency Middleware - Exactly-Once Processing
    // ============================================================================
    println!("1. IDEMPOTENCY MIDDLEWARE - Exactly-Once Processing");
    println!("   Prevents duplicate message processing even with retries\n");

    let idempotency = IdempotencyMiddleware::new(10_000);

    // Simulate a message being processed multiple times (e.g., due to network issues)
    let task_id = Uuid::new_v4();
    let mut msg = Message::new(
        "process_payment".to_string(),
        task_id,
        b"Payment data".to_vec(),
    );

    // First processing - should mark as new
    idempotency.after_consume(&mut msg).await.unwrap();
    let already_processed = msg
        .headers
        .extra
        .get("x-already-processed")
        .unwrap()
        .as_bool()
        .unwrap();
    println!(
        "   First processing: already_processed = {}",
        already_processed
    );

    // Simulate redelivery - should detect as duplicate
    let mut msg2 = Message::new(
        "process_payment".to_string(),
        task_id,
        b"Payment data".to_vec(),
    );
    idempotency.after_consume(&mut msg2).await.unwrap();
    let already_processed = msg2
        .headers
        .extra
        .get("x-already-processed")
        .unwrap()
        .as_bool()
        .unwrap();
    println!("   Redelivery: already_processed = {}", already_processed);
    println!("   ✓ Duplicate processing prevented!\n");

    // ============================================================================
    // Part 2: Backoff Middleware - Intelligent Retry Delays
    // ============================================================================
    println!("2. BACKOFF MIDDLEWARE - Intelligent Retry Delays");
    println!("   Calculates exponential backoff with jitter to prevent thundering herd\n");

    let backoff = BackoffMiddleware::new(
        Duration::from_secs(1),   // Initial delay
        Duration::from_secs(300), // Max delay (5 minutes)
        2.0,                      // Multiplier
    );

    // Simulate retry attempts
    for retry in 0..5 {
        let mut msg = Message::new("retry_task".to_string(), Uuid::new_v4(), b"data".to_vec());
        msg.headers
            .extra
            .insert("retries".to_string(), serde_json::json!(retry));

        backoff.after_consume(&mut msg).await.unwrap();

        let delay_ms = msg
            .headers
            .extra
            .get("x-backoff-delay")
            .unwrap()
            .as_u64()
            .unwrap();

        println!(
            "   Retry {}: backoff delay = {}ms ({:.1}s)",
            retry,
            delay_ms,
            delay_ms as f64 / 1000.0
        );
    }
    println!("   ✓ Progressive backoff with jitter applied!\n");

    // ============================================================================
    // Part 3: Anomaly Detection
    // ============================================================================
    println!("3. ANOMALY DETECTION - Identify Unusual Patterns");
    println!("   Detects spikes or drops in message flow\n");

    // Baseline: normal traffic pattern
    let baseline = vec![1000, 1020, 980, 1010, 990, 1005];

    // Current: sudden spike
    let current_spike = vec![1000, 1010, 1005, 5000]; // Spike!
    let (is_anomaly, severity, description) = utils::detect_anomalies(
        &current_spike,
        &baseline,
        2.0, // Threshold multiplier
    );
    println!("   Baseline avg: ~1000 msgs/min");
    println!("   Current with spike: {:?}", current_spike);
    println!("   Anomaly detected: {}", is_anomaly);
    println!("   Severity: {:.2}", severity);
    println!("   Description: {}", description);

    // Current: sudden drop
    let current_drop = vec![1000, 1010, 200, 180]; // Drop!
    let (is_anomaly, severity, description) =
        utils::detect_anomalies(&current_drop, &baseline, 2.0);
    println!("\n   Current with drop: {:?}", current_drop);
    println!("   Anomaly detected: {}", is_anomaly);
    println!("   Severity: {:.2}", severity);
    println!("   Description: {}\n", description);

    // ============================================================================
    // Part 4: SLA Compliance Monitoring
    // ============================================================================
    println!("4. SLA COMPLIANCE - Track Service Level Agreements");
    println!("   Monitor processing times against SLA targets\n");

    let processing_times = vec![
        100, 150, 120, 180, 110, 200, // Most under 180ms
        250, 130, 140, 190, // A few over
    ];
    let sla_target_ms = 180;

    let (compliance, violations, avg_time) =
        utils::calculate_sla_compliance(&processing_times, sla_target_ms);

    println!("   SLA Target: {}ms", sla_target_ms);
    println!("   Total requests: {}", processing_times.len());
    println!("   Violations: {}", violations);
    println!("   Compliance: {:.1}%", compliance);
    println!("   Avg processing time: {:.1}ms", avg_time);

    if compliance >= 95.0 {
        println!("   ✓ SLA target met!");
    } else {
        println!("   ⚠ SLA target not met - action required");
    }
    println!();

    // ============================================================================
    // Part 5: Error Budget Calculation
    // ============================================================================
    println!("5. ERROR BUDGET - Track Reliability Budget");
    println!("   Monitor error budget consumption for SLO compliance\n");

    let sla_target = 99.9; // 99.9% uptime (three nines)
    let total_requests = 100_000;
    let failed_requests = 50;
    let requests_per_hour = 10_000;

    let (budget_remaining, errors_allowed, hours_to_exhaustion) = utils::calculate_error_budget(
        sla_target,
        total_requests,
        failed_requests,
        requests_per_hour,
    );

    println!(
        "   SLA Target: {}% (allows {}% errors)",
        sla_target,
        100.0 - sla_target
    );
    println!("   Total requests: {}", total_requests);
    println!("   Failed requests: {}", failed_requests);
    println!("   Error budget remaining: {:.1}%", budget_remaining);
    println!("   Errors still allowed: {}", errors_allowed);

    if hours_to_exhaustion.is_finite() {
        println!(
            "   Time to budget exhaustion: {:.1} hours",
            hours_to_exhaustion
        );
    } else {
        println!("   Time to budget exhaustion: ∞ (no errors detected)");
    }

    if budget_remaining > 50.0 {
        println!("   ✓ Plenty of error budget available");
    } else if budget_remaining > 20.0 {
        println!("   ⚠ Error budget getting low");
    } else {
        println!("   🚨 Error budget critical!");
    }
    println!();

    // ============================================================================
    // Part 6: Infrastructure Cost Estimation
    // ============================================================================
    println!("6. COST ESTIMATION - Predict Infrastructure Costs");
    println!("   Estimate costs based on message volume\n");

    let messages_per_day = 1_000_000;
    let cost_per_million = 0.50; // $0.50 per million messages

    let cost_30_days = utils::estimate_infrastructure_cost(messages_per_day, cost_per_million, 30);

    let cost_365_days =
        utils::estimate_infrastructure_cost(messages_per_day, cost_per_million, 365);

    println!("   Message volume: {} msgs/day", messages_per_day);
    println!("   Pricing: ${} per million messages", cost_per_million);
    println!("   Monthly cost (30 days): ${:.2}", cost_30_days);
    println!("   Annual cost (365 days): ${:.2}", cost_365_days);
    println!("   ✓ Cost projections calculated\n");

    // ============================================================================
    // Part 7: Queue Saturation Prediction
    // ============================================================================
    println!("7. CAPACITY PLANNING - Predict Queue Saturation");
    println!("   Forecast when queues will reach capacity\n");

    // Historical queue sizes (growing over time)
    let queue_sizes = vec![1000, 1150, 1300, 1450, 1600, 1750];
    let max_capacity = 3000;
    let hours_per_sample = 1.0;

    let (hours_to_saturation, growth_rate) =
        utils::predict_queue_saturation(&queue_sizes, max_capacity, hours_per_sample);

    println!("   Historical sizes: {:?}", queue_sizes);
    println!("   Max capacity: {}", max_capacity);
    println!("   Growth rate: {:.0} msgs/hour", growth_rate);
    println!("   Time to saturation: {:.1} hours", hours_to_saturation);

    if hours_to_saturation < 24.0 {
        println!("   🚨 Queue will saturate within 24 hours!");
    } else if hours_to_saturation < 72.0 {
        println!("   ⚠ Queue will saturate within 3 days");
    } else {
        println!("   ✓ Sufficient capacity for now");
    }
    println!();

    // ============================================================================
    // Part 8: Combined Middleware Pipeline
    // ============================================================================
    println!("8. PRODUCTION MIDDLEWARE PIPELINE");
    println!("   Combining multiple middleware for robust message processing\n");

    let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::default()));

    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(IdempotencyMiddleware::with_default_cache()))
        .with_middleware(Box::new(BackoffMiddleware::with_defaults()))
        .with_middleware(Box::new(LoggingMiddleware::new("production")))
        .with_middleware(Box::new(MetricsMiddleware::new(metrics.clone())));

    println!("   Middleware chain configured:");
    println!("   - IdempotencyMiddleware (exactly-once processing)");
    println!("   - BackoffMiddleware (intelligent retries)");
    println!("   - LoggingMiddleware (audit trail)");
    println!("   - MetricsMiddleware (statistics collection)");
    println!("   Total middleware: {}", chain.len());

    // Process a message through the pipeline
    let mut msg = Message::new(
        "production_task".to_string(),
        Uuid::new_v4(),
        b"Important data".to_vec(),
    );

    chain.process_before_publish(&mut msg).await.unwrap();
    println!(
        "\n   ✓ Message processed through {} middleware layers",
        chain.len()
    );
    println!("   Message headers: {} entries", msg.headers.extra.len());
    println!();

    // ============================================================================
    // Summary
    // ============================================================================
    println!("=== OPERATIONAL EXCELLENCE SUMMARY ===");
    println!();
    println!("✓ Exactly-once processing with IdempotencyMiddleware");
    println!("✓ Intelligent retry backoff with jitter");
    println!("✓ Real-time anomaly detection");
    println!("✓ SLA compliance monitoring");
    println!("✓ Error budget tracking");
    println!("✓ Infrastructure cost estimation");
    println!("✓ Capacity planning and saturation prediction");
    println!("✓ Production-ready middleware pipeline");
    println!();
    println!("Ready for enterprise deployment!");
}
