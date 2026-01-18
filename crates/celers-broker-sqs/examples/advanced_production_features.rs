//! Advanced Production Features Demo
//!
//! This example demonstrates the integration of advanced production features:
//! - Backpressure management for preventing overload
//! - Poison message detection for isolating failing messages
//! - Cost alert system for budget monitoring
//! - Performance profiling for bottleneck detection
//!
//! Run with:
//! ```bash
//! cargo run --example advanced_production_features
//! ```

use celers_broker_sqs::{
    backpressure::{BackpressureConfig, BackpressureManager},
    cost_alerts::{CostAlertConfig, CostAlertSystem},
    poison_detector::{PoisonConfig, PoisonDetector},
    profiler::PerformanceProfiler,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Advanced Production Features Demo ===\n");

    // 1. Setup Backpressure Management
    println!("1. Setting up Backpressure Management...");
    let backpressure_config = BackpressureConfig::new()
        .with_max_in_flight_messages(50)
        .with_max_processing_time(Duration::from_secs(30))
        .with_throttle_threshold(0.7) // Start throttling at 70%
        .with_stop_threshold(0.9) // Stop consuming at 90%
        .with_adaptive_throttling(true);

    let mut backpressure_mgr = BackpressureManager::new(backpressure_config);
    println!("   ✓ Backpressure manager configured");
    println!("   - Max in-flight: 50 messages");
    println!("   - Throttle at: 70% capacity");
    println!("   - Stop at: 90% capacity\n");

    // 2. Setup Poison Message Detection
    println!("2. Setting up Poison Message Detection...");
    let poison_config = PoisonConfig::new()
        .with_max_failures(3)
        .with_failure_window(Duration::from_secs(300)) // 5 minutes
        .with_auto_isolate(true)
        .with_track_error_patterns(true);

    let mut poison_detector = PoisonDetector::new(poison_config);
    println!("   ✓ Poison detector configured");
    println!("   - Max failures: 3 within 5 minutes");
    println!("   - Auto-isolation: enabled");
    println!("   - Error pattern tracking: enabled\n");

    // 3. Setup Cost Alert System
    println!("3. Setting up Cost Alert System...");
    let cost_alert_config = CostAlertConfig::new()
        .with_daily_warning_threshold(5.0) // Warn at $5/day
        .with_daily_critical_threshold(10.0) // Critical at $10/day
        .with_monthly_warning_threshold(100.0) // Warn at $100/month
        .with_monthly_critical_threshold(200.0); // Critical at $200/month

    let mut cost_alert_system = CostAlertSystem::new(cost_alert_config);

    // Register alert callback
    cost_alert_system.register_callback(Arc::new(|alert| {
        println!(
            "   ⚠️  COST ALERT [{}]: {} - ${:.2}",
            alert.budget_type, alert.level, alert.amount_usd
        );
        println!("      Message: {}", alert.message);
    }));

    println!("   ✓ Cost alert system configured");
    println!("   - Daily warning: $5.00");
    println!("   - Daily critical: $10.00");
    println!("   - Monthly warning: $100.00");
    println!("   - Monthly critical: $200.00\n");

    // 4. Setup Performance Profiler
    println!("4. Setting up Performance Profiler...");
    let mut profiler = PerformanceProfiler::new();
    println!("   ✓ Performance profiler initialized\n");

    // Simulate message processing with all features
    println!("=== Simulating Message Processing ===\n");

    // Process 60 messages to demonstrate various scenarios
    for i in 1..=60 {
        let message_id = format!("msg-{}", i);

        // Check backpressure before processing
        if !backpressure_mgr.should_consume() {
            println!("⏸️  Backpressure: System at capacity, pausing consumption");
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }

        // Start tracking message
        backpressure_mgr.track_message_start(&message_id);

        // Simulate message processing
        let start = Instant::now();
        let processing_result = simulate_message_processing(i);
        let elapsed = start.elapsed();

        // Record profiling metrics
        profiler.record_publish(elapsed);

        // Track cost (assume $0.0004 per message)
        cost_alert_system.track_cost(0.0004);

        match processing_result {
            Ok(_) => {
                // Successful processing
                backpressure_mgr.track_message_complete(&message_id);

                if i % 10 == 0 {
                    println!(
                        "✓ Processed message {} successfully ({:.2}ms)",
                        i,
                        elapsed.as_millis()
                    );
                }
            }
            Err(error) => {
                // Failed processing - track with poison detector
                backpressure_mgr.track_message_failed(&message_id);

                let is_poison = poison_detector.track_failure(&message_id, &error);

                if is_poison {
                    println!(
                        "☠️  Message {} identified as POISON and isolated",
                        message_id
                    );
                } else {
                    println!(
                        "✗ Message {} failed: {} (attempt tracked)",
                        message_id, error
                    );
                }
            }
        }

        // Small delay between messages
        std::thread::sleep(Duration::from_millis(10));
    }

    // Display final statistics
    println!("\n=== Final Statistics ===\n");

    // 1. Backpressure Metrics
    println!("1. Backpressure Metrics:");
    let bp_metrics = backpressure_mgr.metrics();
    println!("   - Current in-flight: {}", bp_metrics.in_flight_count);
    println!("   - Utilization: {:.1}%", bp_metrics.utilization * 100.0);
    println!("   - Throttle count: {}", bp_metrics.throttle_count);
    println!("   - Stop count: {}", bp_metrics.stop_count);
    println!("   - Slow messages: {}", bp_metrics.slow_message_count);
    println!(
        "   - Avg processing time: {:.2}ms",
        bp_metrics.avg_processing_time_ms
    );
    println!(
        "   - P95 processing time: {:.2}ms\n",
        bp_metrics.p95_processing_time_ms
    );

    // 2. Poison Detection Statistics
    println!("2. Poison Detection Statistics:");
    let poison_stats = poison_detector.statistics();
    println!(
        "   - Poison messages detected: {}",
        poison_stats.poison_message_count
    );
    println!(
        "   - Isolated messages: {}",
        poison_stats.isolated_message_count
    );
    println!(
        "   - Total failures tracked: {}",
        poison_stats.total_failures
    );
    println!(
        "   - Messages being tracked: {}",
        poison_stats.tracked_message_count
    );

    if !poison_stats.error_patterns.is_empty() {
        println!("   - Top error patterns:");
        for (idx, pattern) in poison_stats.error_patterns.iter().take(3).enumerate() {
            println!(
                "     {}. {} (count: {})",
                idx + 1,
                pattern.pattern,
                pattern.count
            );
        }
    }
    println!();

    // 3. Cost Alert Statistics
    println!("3. Cost Alert Statistics:");
    println!("   - Daily cost: ${:.4}", cost_alert_system.daily_cost());
    println!(
        "   - Monthly cost: ${:.4}",
        cost_alert_system.monthly_cost()
    );
    println!(
        "   - Within budget: {}",
        if cost_alert_system.is_within_budget() {
            "Yes ✓"
        } else {
            "No ✗"
        }
    );

    let alert_stats = cost_alert_system.alert_statistics();
    println!("   - Total alerts: {}", alert_stats.total_alerts);
    println!("   - Warning alerts: {}", alert_stats.warning_alerts);
    println!("   - Critical alerts: {}", alert_stats.critical_alerts);
    println!();

    // 4. Performance Profiler Statistics
    println!("4. Performance Profiler Statistics:");
    let perf_summary = profiler.summary();
    let stats = &perf_summary.publish;
    println!("   - Total operations: {}", stats.count);
    println!(
        "   - P50 latency: {:.2}ms",
        stats.p50_latency.as_micros() as f64 / 1000.0
    );
    println!(
        "   - P95 latency: {:.2}ms",
        stats.p95_latency.as_micros() as f64 / 1000.0
    );
    println!(
        "   - P99 latency: {:.2}ms",
        stats.p99_latency.as_micros() as f64 / 1000.0
    );
    println!("   - Throughput: {:.2} ops/sec", stats.throughput);

    let bottlenecks = profiler.detect_bottlenecks(100); // 100ms threshold
    if !bottlenecks.is_empty() {
        println!("   - Bottlenecks detected: {}", bottlenecks.join(", "));
    }

    println!("\n=== Demo Complete ===");
    println!("\nKey Takeaways:");
    println!("• Backpressure prevents system overload by throttling at capacity");
    println!("• Poison detection isolates repeatedly failing messages");
    println!("• Cost alerts notify when budget thresholds are exceeded");
    println!("• Performance profiling identifies bottlenecks and latency issues");

    Ok(())
}

/// Simulate message processing with various outcomes
fn simulate_message_processing(message_num: u64) -> Result<(), String> {
    // Simulate some processing time
    let processing_time = if message_num.is_multiple_of(7) {
        Duration::from_millis(50) // Slow message
    } else {
        Duration::from_millis(5) // Normal message
    };

    std::thread::sleep(processing_time);

    // Simulate failures for certain messages
    if message_num.is_multiple_of(15) {
        Err("Database connection timeout".to_string())
    } else if message_num.is_multiple_of(17) {
        Err("Invalid message format".to_string())
    } else if message_num.is_multiple_of(23) {
        Err("External API error".to_string())
    } else {
        Ok(())
    }
}
