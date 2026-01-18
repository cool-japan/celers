//! Demonstration of v9 Enterprise Production Features
//!
//! This example showcases the advanced production features introduced in v9:
//! - Backpressure Management
//! - Poison Message Detection
//! - Advanced Message Routing
//! - Performance Optimization
//!
//! Run with: `cargo run --example v9_features_demo`

use celers_broker_amqp::{
    backpressure::{BackpressureConfig, BackpressureManager},
    optimization::{OptimizationConfig, PerformanceOptimizer},
    poison_detector::{PoisonDetector, PoisonDetectorConfig},
    router::{MessageRouter, RouteBuilder, RouteCondition, RoutingStrategy},
};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== v9 Enterprise Production Features Demo ===\n");

    // 1. Backpressure Management Demo
    demonstrate_backpressure()?;

    // 2. Poison Message Detection Demo
    demonstrate_poison_detection()?;

    // 3. Advanced Routing Demo
    demonstrate_routing()?;

    // 4. Performance Optimization Demo
    demonstrate_optimization()?;

    println!("\n=== Demo Complete ===");
    Ok(())
}

fn demonstrate_backpressure() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Backpressure Management Demo");
    println!("--------------------------------");

    let config = BackpressureConfig {
        max_queue_depth: 10_000,
        warning_threshold: 0.7,
        critical_threshold: 0.9,
        check_interval: Duration::from_secs(5),
        min_prefetch: 1,
        max_prefetch: 100,
        prefetch_adjustment_factor: 0.2,
    };

    let mut manager = BackpressureManager::new(config);

    // Simulate queue filling up
    println!("Simulating queue filling up...");
    for depth in (1000..=9500).step_by(2000) {
        manager.update_queue_depth(depth);

        let level = manager.calculate_backpressure_level();
        let utilization = manager.queue_utilization();
        let recommended_prefetch = manager.calculate_optimal_prefetch();

        println!(
            "  Queue depth: {}, Utilization: {:.1}%, Level: {:?}, Recommended prefetch: {}",
            depth,
            utilization * 100.0,
            level,
            recommended_prefetch
        );

        if manager.should_apply_backpressure() {
            let action = manager.get_recommended_action();
            println!("    Action: {:?}", action);
        }
    }

    let stats = manager.stats();
    println!("\nBackpressure Statistics:");
    println!("  Trigger count: {}", stats.trigger_count);
    println!("  Prefetch adjustments: {}", stats.prefetch_adjustments);
    println!("  Peak queue depth: {}", stats.peak_queue_depth);
    println!("  Average queue depth: {:.0}", stats.avg_queue_depth);
    println!();

    Ok(())
}

fn demonstrate_poison_detection() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Poison Message Detection Demo");
    println!("--------------------------------");

    let config = PoisonDetectorConfig {
        max_retries: 3,
        retry_window: Duration::from_secs(300),
        failure_rate_threshold: 0.8,
        min_samples: 5,
    };

    let mut detector = PoisonDetector::new(config);

    // Simulate message processing
    println!("Simulating message processing...");

    // Healthy message
    detector.record_success("msg-healthy-1");
    detector.record_success("msg-healthy-2");
    println!("  Processed healthy messages successfully");

    // Poison message with repeated failures
    let poison_msg = "msg-poison-1";
    for i in 1..=4 {
        detector.record_failure(poison_msg, &format!("Error attempt {}", i));
    }
    println!("  Message {} failed {} times", poison_msg, 4);

    // Check if poisoned
    if detector.is_poison(poison_msg) {
        println!("  ⚠️  Message {} is POISONED!", poison_msg);
        if let Some(info) = detector.get_message_info(poison_msg) {
            println!("     Failure count: {}", info.failure_count);
            println!("     Last error: {:?}", info.last_error);
        }
    }

    // Another poison message with different error
    let poison_msg2 = "msg-poison-2";
    for _ in 0..5 {
        detector.record_failure(poison_msg2, "Connection timeout");
    }

    // Pattern detection
    if detector.detect_failure_pattern("Connection timeout") {
        println!("\n  🔍 Detected failure pattern: Connection timeout");
    }

    // Get all poisoned messages
    let poisoned = detector.get_poisoned_messages();
    println!("\nPoisoned Messages: {}", poisoned.len());
    for msg in poisoned {
        println!("  - {}: {} failures", msg.message_id, msg.failure_count);
    }

    // Analytics
    let analytics = detector.analytics();
    println!("\nPoison Detection Analytics:");
    println!("  Total tracked: {}", analytics.total_tracked);
    println!("  Poison count: {}", analytics.poison_count);
    println!("  Total failures: {}", analytics.total_failures);
    println!("  Total successes: {}", analytics.total_successes);
    println!("  Failure rate: {:.1}%", analytics.failure_rate * 100.0);
    println!();

    Ok(())
}

fn demonstrate_routing() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Advanced Message Routing Demo");
    println!("--------------------------------");

    let mut router = MessageRouter::with_strategy("default_queue", RoutingStrategy::FirstMatch);

    // Add routing rules
    println!("Adding routing rules...");

    // Priority-based routing
    let priority_rule = RouteBuilder::new("high_priority")
        .condition(RouteCondition::PriorityRange { min: 7, max: 9 })
        .to_queue("priority_queue")
        .with_weight(1.0)
        .build()?;
    router.add_rule(priority_rule);
    println!("  ✓ High priority rule: Route priority 7-9 -> priority_queue");

    // Size-based routing
    let large_msg_rule = RouteBuilder::new("large_messages")
        .condition(RouteCondition::PayloadSize {
            min: Some(1024),
            max: None,
        })
        .to_queue("large_message_queue")
        .with_weight(1.0)
        .build()?;
    router.add_rule(large_msg_rule);
    println!("  ✓ Large message rule: Route messages >1KB -> large_message_queue");

    // Content-based routing
    let error_rule = RouteBuilder::new("error_messages")
        .condition(RouteCondition::PayloadContains("ERROR".to_string()))
        .to_queue("error_queue")
        .with_weight(1.0)
        .build()?;
    router.add_rule(error_rule);
    println!("  ✓ Error rule: Route messages containing 'ERROR' -> error_queue");

    // Test routing
    println!("\nTesting routing...");

    // Test 1: High priority message
    let queue = router.route(b"test message", Some(9));
    println!("  Priority 9 message -> {}", queue);

    // Test 2: Large message
    let large_payload = vec![0u8; 2048];
    let queue = router.route(&large_payload, Some(5));
    println!("  Large message (2KB) -> {}", queue);

    // Test 3: Error message
    let queue = router.route(b"ERROR: Something went wrong", None);
    println!("  Error message -> {}", queue);

    // Test 4: Default routing
    let queue = router.route(b"normal message", Some(5));
    println!("  Normal message -> {}", queue);

    // Routing statistics
    let stats = router.stats();
    println!("\nRouting Statistics:");
    println!("  Total routed: {}", stats.total_routed);
    println!("  Fallback count: {}", stats.fallback_count);
    println!("  Queue distribution:");
    for (queue, count) in &stats.queue_counts {
        println!("    {}: {} messages", queue, count);
    }
    println!();

    Ok(())
}

fn demonstrate_optimization() -> Result<(), Box<dyn std::error::Error>> {
    println!("4. Performance Optimization Demo");
    println!("--------------------------------");

    let config = OptimizationConfig {
        target_latency_ms: 100.0,
        target_throughput: 1000.0,
        max_memory_bytes: 1024 * 1024 * 1024, // 1GB
        sample_window: 100,
        auto_tune: true,
    };

    let mut optimizer = PerformanceOptimizer::new(config);

    // Simulate performance data
    println!("Recording performance metrics...");

    // Simulate varying latencies
    for i in 0..50 {
        let latency = 50.0 + (i as f64 * 2.0);
        optimizer.record_latency(latency);
    }

    // Simulate throughput
    for i in 0..50 {
        let throughput = 800.0 + (i as f64 * 10.0);
        optimizer.record_throughput(throughput);
    }

    // Simulate memory usage
    for i in 0..50 {
        let memory = 500_000_000 + (i * 5_000_000);
        optimizer.record_memory_usage(memory);
    }

    // Get current metrics
    let metrics = optimizer.metrics();
    println!("\nCurrent Performance Metrics:");
    println!("  Average latency: {:.2}ms", metrics.avg_latency_ms);
    println!("  P95 latency: {:.2}ms", metrics.p95_latency_ms);
    println!("  P99 latency: {:.2}ms", metrics.p99_latency_ms);
    println!("  Average throughput: {:.0} msg/s", metrics.avg_throughput);
    println!("  Peak throughput: {:.0} msg/s", metrics.peak_throughput);
    println!(
        "  Average memory: {:.2} MB",
        metrics.avg_memory_bytes as f64 / 1024.0 / 1024.0
    );
    println!(
        "  Peak memory: {:.2} MB",
        metrics.peak_memory_bytes as f64 / 1024.0 / 1024.0
    );

    // Check if meeting targets
    if optimizer.is_meeting_targets() {
        println!("\n✓ Performance targets met!");
    } else {
        println!("\n⚠️  Performance targets not met");

        // Get recommendations
        let recommendations = optimizer.get_recommendations();
        println!("\nOptimization Recommendations:");
        for rec in recommendations {
            println!("  [{:?}]", rec.category);
            println!("    {}", rec.recommendation);
            println!(
                "    Current: {:.2}, Recommended: {:.2}",
                rec.current_value, rec.recommended_value
            );
            println!("    Expected improvement: {:.1}%", rec.expected_improvement);
        }
    }

    // Performance score
    let score = optimizer.performance_score();
    println!("\nPerformance Score: {:.1}%", score * 100.0);

    // Calculate optimal settings
    let optimal_prefetch = optimizer.calculate_optimal_prefetch();
    let optimal_batch = optimizer.calculate_optimal_batch_size();
    let optimal_channels = optimizer.calculate_optimal_channel_pool(100);
    let optimal_connections = optimizer.calculate_optimal_connection_pool(1000);

    println!("\nOptimal Settings:");
    println!("  Prefetch count: {}", optimal_prefetch);
    println!("  Batch size: {}", optimal_batch);
    println!("  Channel pool: {}", optimal_channels);
    println!("  Connection pool: {}", optimal_connections);

    // Estimate drain time for a queue
    let drain_time = optimizer.estimate_drain_time(5000);
    println!(
        "\nEstimated time to drain 5000 messages: {} seconds",
        drain_time.as_secs()
    );

    Ok(())
}
