// Copyright (c) 2025 CeleRS Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Production Suite Example
//!
//! Comprehensive demonstration of all production features working together:
//! - Circuit breaker for resilience
//! - Cost tracking for budget control
//! - Batch optimization for efficiency
//! - Distributed tracing for observability
//! - Quota management for rate limiting
//! - Performance profiling for monitoring
//! - Multi-queue routing for distribution
//!
//! This example shows how to integrate all production utilities
//! into a cohesive monitoring and management system.

use celers_broker_sqs::{
    batch_optimizer::{BatchOptimizer, OptimizerGoal},
    circuit_breaker::CircuitBreaker,
    profiler::PerformanceProfiler,
    quota_manager::{QuotaConfig, QuotaManager},
    router::{MessageRouter, RoutingRule},
    tracing_util::{generate_correlation_id, TraceContext},
};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== Production Suite Integration Example ===\n");

    // Example 1: Complete Production Setup
    example_complete_setup().await;

    // Example 2: High-Traffic Scenario
    example_high_traffic_scenario();

    // Example 3: Batch Optimization Scenario
    example_batch_optimization();

    // Example 4: Distributed System Simulation
    example_distributed_system();
}

async fn example_complete_setup() {
    println!("--- Example 1: Complete Production Setup ---\n");

    // 1. Circuit Breaker for resilience
    let circuit_breaker = CircuitBreaker::new(5, 30);
    println!("✓ Circuit breaker configured (threshold: 5, timeout: 30s)");

    // 2. Quota Manager for rate limiting
    let quota_config = QuotaConfig::new()
        .with_max_requests_per_second(100)
        .with_daily_budget_usd(50.0)
        .with_alert_threshold(0.85);
    let mut quota_mgr = QuotaManager::new(quota_config);
    println!("✓ Quota manager configured (100 req/s, $50/day budget)");

    // 3. Performance Profiler for latency tracking
    let mut profiler = PerformanceProfiler::new();
    println!("✓ Performance profiler initialized");

    // 4. Message Router for intelligent distribution
    let mut router = MessageRouter::new();
    router.add_rule(RoutingRule::priority_based("high-queue", 8, 10));
    router.add_rule(RoutingRule::priority_based("standard-queue", 0, 7));
    println!("✓ Message router configured (2 queues)");

    // 5. Batch Optimizer for efficiency
    let _batch_optimizer = BatchOptimizer::new(OptimizerGoal::Balanced { cost_weight: 0.5 });
    println!("✓ Batch optimizer initialized");

    println!("\n📊 Production System Ready!\n");

    // Simulate operations
    println!("Simulating 50 operations...");

    for i in 1..=50 {
        // Check quota before operation
        if quota_mgr.can_make_request(1).is_err() {
            println!("  ⚠️  Quota exceeded at operation {}", i);
            break;
        }

        // Check circuit breaker
        if circuit_breaker.state().await == celers_broker_sqs::circuit_breaker::CircuitState::Open {
            println!("  ⚠️  Circuit breaker open at operation {}", i);
            break;
        }

        // Route message
        let metadata = json!({"priority": (i % 10) as u8});
        let _queue = router.route(&metadata);

        // Record operation
        let latency = Duration::from_millis(10 + (i % 20) as u64);
        profiler.record_publish(latency);
        quota_mgr.record_request(1, 0.0004);
        circuit_breaker.record_success().await;
    }

    // Print summary
    println!("\n--- System Status ---");

    let quota_status = quota_mgr.status();
    println!("Quota:");
    println!("  Requests today: {}", quota_status.requests_today);
    println!("  Cost today: ${:.4}", quota_status.cost_today_usd);
    println!(
        "  Alert: {}",
        if quota_status.alert_triggered {
            "YES"
        } else {
            "No"
        }
    );

    let publish_stats = profiler.publish_stats();
    println!("\nPerformance:");
    println!("  Operations: {}", publish_stats.count);
    println!("  P50 latency: {:?}", publish_stats.p50_latency);
    println!("  P95 latency: {:?}", publish_stats.p95_latency);

    let circuit_stats = circuit_breaker.stats().await;
    println!("\nResilience:");
    println!("  Circuit state: {:?}", circuit_breaker.state().await);
    println!(
        "  Success rate: {:.1}%",
        (circuit_stats.success_count as f64
            / (circuit_stats.success_count + circuit_stats.failure_count).max(1) as f64)
            * 100.0
    );

    println!();
}

fn example_high_traffic_scenario() {
    println!("--- Example 2: High-Traffic Scenario ---\n");

    let mut profiler = PerformanceProfiler::new();
    let mut quota_mgr = QuotaManager::new(
        QuotaConfig::new()
            .with_max_requests_per_second(1000)
            .with_max_requests_per_minute(50_000),
    );

    println!("Simulating high-traffic burst (1000 req/s)...\n");

    // Simulate burst of 500 requests
    let start = std::time::Instant::now();
    let mut successful = 0;
    let mut rejected = 0;

    for _ in 0..500 {
        if quota_mgr.can_make_request(1).is_ok() {
            quota_mgr.record_request(1, 0.0004);
            profiler.record_publish(Duration::from_millis(5));
            successful += 1;
        } else {
            rejected += 1;
        }
    }

    let elapsed = start.elapsed();

    println!("Results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Successful: {}", successful);
    println!("  Rejected: {}", rejected);
    println!(
        "  Throughput: {:.0} req/s",
        successful as f64 / elapsed.as_secs_f64()
    );

    let status = quota_mgr.status();
    println!("\nQuota Status:");
    println!("  Last second: {} requests", status.requests_last_second);
    println!("  Last minute: {} requests", status.requests_last_minute);
    println!("  Cost today: ${:.4}", status.cost_today_usd);

    let publish_stats = profiler.publish_stats();
    println!("\nPerformance:");
    println!("  Mean latency: {:?}", publish_stats.mean_latency);
    println!("  P95 latency: {:?}", publish_stats.p95_latency);
    println!("  Throughput: {:.0} ops/s", publish_stats.throughput);

    // Calculate projected monthly cost
    let projected_monthly = status.cost_today_usd * 30.0;
    println!("\nCost Projection:");
    println!("  Projected monthly: ${:.2}", projected_monthly);

    println!();
}

fn example_batch_optimization() {
    println!("--- Example 3: Batch Optimization Scenario ---\n");

    let batch_optimizer = BatchOptimizer::new(OptimizerGoal::MinimizeCost);
    let mut profiler_individual = PerformanceProfiler::new();
    let mut profiler_batch = PerformanceProfiler::new();

    // Analyze queue metrics
    let queue_depth = 5000;
    let avg_message_size = 2048; // 2 KB
    let processing_rate = 100.0; // messages per second

    println!("Queue metrics:");
    println!("  Depth: {} messages", queue_depth);
    println!("  Avg size: {} bytes", avg_message_size);
    println!("  Processing rate: {:.0} msg/s", processing_rate);

    // Get batch optimization recommendation
    let result = batch_optimizer.optimize(queue_depth, avg_message_size, processing_rate);

    println!("\nOptimization recommendation:");
    println!("  Batch size: {}", result.batch_size);
    println!("  Estimated latency: {:.2}ms", result.estimated_latency_ms);
    println!(
        "  Estimated cost per message: ${:.6}",
        result.estimated_cost_per_message
    );
    println!(
        "  Estimated throughput: {:.0} msg/s",
        result.estimated_throughput
    );
    println!("  Reasoning: {}", result.reasoning);

    // Simulate batch vs individual operations
    println!("\n--- Performance Comparison ---");

    // Individual operations (1000 messages)
    println!("Individual operations (1000 messages):");
    for _ in 0..1000 {
        profiler_individual.record_publish(Duration::from_millis(15));
    }

    let individual_stats = profiler_individual.publish_stats();
    println!("  Operations: {}", individual_stats.count);
    println!("  Mean latency: {:?}", individual_stats.mean_latency);
    println!("  Estimated API calls: 1000 (1 per message)");

    // Batch operations (1000 messages in batches of 10)
    println!("\nBatch operations (1000 messages, batch size 10):");
    for _ in 0..100 {
        profiler_batch.record_batch_publish(Duration::from_millis(25));
    }

    let batch_stats = profiler_batch.batch_publish_stats();
    println!("  Batch operations: {}", batch_stats.count);
    println!("  Mean latency: {:?}", batch_stats.mean_latency);
    println!("  Estimated API calls: 100 (10 messages per batch)");

    // Cost calculation (at $0.40 per million requests for standard queue)
    let individual_cost = (1000.0 / 1_000_000.0) * 0.40;
    let batch_cost = (100.0 / 1_000_000.0) * 0.40;
    let savings = individual_cost - batch_cost;
    let savings_pct = (savings / individual_cost) * 100.0;

    println!("\n💰 Cost Analysis:");
    println!("  Individual cost: ${:.6}", individual_cost);
    println!("  Batch cost: ${:.6}", batch_cost);
    println!("  Savings: ${:.6} ({:.1}%)", savings, savings_pct);
    println!("  API call reduction: 90%");

    println!();
}

fn example_distributed_system() {
    println!("--- Example 4: Distributed System Simulation ---\n");

    let mut router = MessageRouter::new();
    let mut profiler = PerformanceProfiler::new();

    // Configure multi-service routing
    router.add_rule(RoutingRule::task_pattern("orders-queue", "order.*"));
    router.add_rule(RoutingRule::task_pattern("payments-queue", "payment.*"));
    router.add_rule(RoutingRule::task_pattern("notifications-queue", "notify.*"));

    println!("Multi-service architecture:");
    println!("  order.* → orders-queue");
    println!("  payment.* → payments-queue");
    println!("  notify.* → notifications-queue\n");

    // Simulate order processing flow
    println!("Processing order workflow...\n");

    // 1. Create order (API Gateway → Orders Service)
    let correlation_id = generate_correlation_id();
    let api_ctx = TraceContext::new()
        .with_correlation_id(&correlation_id)
        .with_span_id("span-001")
        .with_operation("api_create_order")
        .with_service_name("api-gateway");

    let orders_queue = router.route_with_task_name(Some("order.create"), &json!({}));
    profiler.record_publish(Duration::from_millis(15));
    println!(
        "1. API Gateway → {} (correlation: {}...)",
        orders_queue,
        &correlation_id[..8]
    );

    // 2. Process payment (Orders Service → Payment Service)
    let payment_ctx = api_ctx.create_child_span("process_payment");
    let payments_queue = router.route_with_task_name(Some("payment.charge"), &json!({}));
    profiler.record_publish(Duration::from_millis(12));
    println!(
        "2. Order Service → {} (span: {}...)",
        payments_queue,
        &payment_ctx.span_id.as_ref().unwrap()[..8]
    );

    // 3. Send notification (Orders Service → Notification Service)
    let notify_ctx = api_ctx.create_child_span("send_confirmation");
    let notifications_queue = router.route_with_task_name(Some("notify.email"), &json!({}));
    profiler.record_publish(Duration::from_millis(10));
    println!(
        "3. Order Service → {} (span: {}...)",
        notifications_queue,
        &notify_ctx.span_id.as_ref().unwrap()[..8]
    );

    println!("\n--- Distributed Trace ---");
    println!("Correlation ID: {}", correlation_id);
    println!("Span hierarchy:");
    println!("  api_create_order (api-gateway)");
    println!("  ├─ process_payment (payment-service)");
    println!("  └─ send_confirmation (notification-service)");

    println!("\n--- System Performance ---");
    let publish_stats = profiler.publish_stats();
    println!("Operations: {}", publish_stats.count);
    println!("Mean latency: {:?}", publish_stats.mean_latency);
    println!("P95 latency: {:?}", publish_stats.p95_latency);

    println!("\n--- Monitoring Insights ---");
    println!("✓ All services traced with correlation ID");
    println!("✓ Parent-child relationships preserved");
    println!("✓ Messages routed to appropriate queues");
    println!("✓ Performance metrics collected");
    println!("✓ Ready for observability platforms (X-Ray, Jaeger, etc.)");

    println!();
}
