//! Message Replay and SLA Monitoring Example
//!
//! This example demonstrates:
//! - Message replay from DLQ
//! - Selective message replay with filters
//! - SLA monitoring and compliance tracking
//! - Real-time latency and throughput tracking
//! - SLA violation detection
//!
//! Run with: cargo run --example replay_and_sla

use celers_broker_sqs::{
    replay::{ReplayConfig, ReplayFilter, ReplayManager, ReplayableMessage},
    sla_monitor::{ComplianceStatus, SlaMonitor, SlaTarget},
};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() {
    println!("=== Message Replay and SLA Monitoring Example ===\n");

    // ========================================
    // Part 1: Message Replay from DLQ
    // ========================================
    println!("📦 Part 1: Message Replay Configuration");
    println!("─────────────────────────────────────────\n");

    // Configure replay settings
    let replay_config = ReplayConfig::new()
        .with_batch_size(10)
        .with_rate_limit(100) // 100 messages/sec max
        .with_retry_failed(true)
        .with_max_retries(3)
        .with_retry_delay(Duration::from_secs(1))
        .with_preserve_attributes(true)
        .with_track_progress(true);

    println!("Replay Configuration:");
    println!("  Batch size: {}", replay_config.batch_size);
    println!("  Rate limit: {} msg/sec", replay_config.rate_limit);
    println!("  Retry failed: {}", replay_config.retry_failed);
    println!("  Max retries: {}", replay_config.max_retries);
    println!();

    let _manager = ReplayManager::new(replay_config);
    println!("✓ Replay manager created\n");

    // ========================================
    // Part 2: Selective Replay with Filters
    // ========================================
    println!("🔍 Part 2: Selective Replay Filters");
    println!("─────────────────────────────────────────\n");

    // Create filters for different scenarios
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Filter 1: Replay only payment tasks from last hour
    let payment_filter = ReplayFilter::new()
        .with_time_range_hours(1)
        .with_task_pattern("tasks.payment.*")
        .with_max_messages(100);

    println!("Filter 1: Recent Payment Tasks");
    println!("  Pattern: tasks.payment.*");
    println!("  Time range: Last 1 hour");
    println!("  Max messages: 100");
    println!();

    // Filter 2: Replay high-failure messages with timeout errors
    let timeout_filter = ReplayFilter::new()
        .with_error_pattern("timeout")
        .with_min_failure_count(3)
        .with_max_messages(50);

    println!("Filter 2: High-Failure Timeout Messages");
    println!("  Error pattern: timeout");
    println!("  Min failures: 3");
    println!("  Max messages: 50");
    println!();

    // Filter 3: Replay specific time range
    let time_filter = ReplayFilter::new()
        .with_time_range(now - 7200, now - 3600) // Between 2 hours and 1 hour ago
        .with_task_pattern("tasks.email.*");

    println!("Filter 3: Specific Time Range Email Tasks");
    println!("  Pattern: tasks.email.*");
    println!("  Time range: 2 hours ago to 1 hour ago");
    println!();

    // ========================================
    // Part 3: Test Filter Matching
    // ========================================
    println!("🧪 Part 3: Filter Matching Examples");
    println!("─────────────────────────────────────────\n");

    // Create sample messages
    let messages = vec![
        ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.payment.process".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 1800, // 30 minutes ago
            error_message: Some("Connection timeout".to_string()),
            failure_count: 5,
        },
        ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.email.send".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 5400, // 90 minutes ago
            error_message: Some("SMTP error".to_string()),
            failure_count: 2,
        },
        ReplayableMessage {
            message_id: "msg-3".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.notification.push".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 7200, // 2 hours ago
            error_message: Some("Rate limit exceeded".to_string()),
            failure_count: 1,
        },
    ];

    println!("Test Messages:");
    for (i, msg) in messages.iter().enumerate() {
        println!("  Message {}: {}", i + 1, msg.task_name);
        println!("    Age: {} minutes", (now - msg.timestamp) / 60);
        println!("    Failures: {}", msg.failure_count);
        if let Some(ref error) = msg.error_message {
            println!("    Error: {}", error);
        }
    }
    println!();

    // Test filters
    println!("Filter Matching Results:");
    println!("  Payment filter:");
    for (i, msg) in messages.iter().enumerate() {
        println!(
            "    Message {}: {}",
            i + 1,
            if payment_filter.matches(msg) {
                "✓ Match"
            } else {
                "✗ No match"
            }
        );
    }

    println!("\n  Timeout filter:");
    for (i, msg) in messages.iter().enumerate() {
        println!(
            "    Message {}: {}",
            i + 1,
            if timeout_filter.matches(msg) {
                "✓ Match"
            } else {
                "✗ No match"
            }
        );
    }

    println!("\n  Time range filter:");
    for (i, msg) in messages.iter().enumerate() {
        println!(
            "    Message {}: {}",
            i + 1,
            if time_filter.matches(msg) {
                "✓ Match"
            } else {
                "✗ No match"
            }
        );
    }
    println!();

    // ========================================
    // Part 4: SLA Monitoring Setup
    // ========================================
    println!("📊 Part 4: SLA Monitoring Configuration");
    println!("─────────────────────────────────────────\n");

    // Define SLA targets for production
    let sla_targets = SlaTarget::production();

    println!("Production SLA Targets:");
    println!(
        "  P50 latency: {:?}",
        sla_targets.latency_p50.unwrap_or_default()
    );
    println!(
        "  P95 latency: {:?}",
        sla_targets.latency_p95.unwrap_or_default()
    );
    println!(
        "  P99 latency: {:?}",
        sla_targets.latency_p99.unwrap_or_default()
    );
    println!(
        "  Max latency: {:?}",
        sla_targets.max_latency.unwrap_or_default()
    );
    println!(
        "  Min throughput: {:.0} msg/sec",
        sla_targets.min_throughput.unwrap_or(0.0)
    );
    println!(
        "  Max error rate: {:.1}%",
        sla_targets.max_error_rate.unwrap_or(0.0) * 100.0
    );
    println!();

    // Create SLA monitor
    let mut sla_monitor = SlaMonitor::new(sla_targets)
        .with_max_records(10000)
        .with_window_duration(Duration::from_secs(60));

    println!("✓ SLA monitor created\n");

    // ========================================
    // Part 5: Simulate Message Processing
    // ========================================
    println!("🔄 Part 5: Simulating Message Processing");
    println!("─────────────────────────────────────────\n");

    // Simulate 100 messages with varying latencies
    println!("Processing 100 messages...");
    for i in 0..100 {
        let latency = if i < 80 {
            // 80% fast messages
            Duration::from_millis(100 + (i % 50) * 10)
        } else if i < 95 {
            // 15% medium messages
            Duration::from_millis(1000 + (i % 20) * 50)
        } else {
            // 5% slow messages
            Duration::from_millis(3000 + (i % 10) * 200)
        };

        // 98% success rate
        let success = i % 50 != 0;

        sla_monitor.record_message(latency, success);

        if (i + 1) % 20 == 0 {
            println!("  Processed {} messages...", i + 1);
        }
    }
    println!("✓ Finished processing\n");

    // ========================================
    // Part 6: Generate SLA Report
    // ========================================
    println!("📈 Part 6: SLA Compliance Report");
    println!("─────────────────────────────────────────\n");

    let report = sla_monitor.generate_report();

    println!("Overall Status: {:?}", report.status);
    println!(
        "Overall Compliance: {:.2}%",
        report.overall_compliance * 100.0
    );
    println!();

    println!("Latency Metrics:");
    println!("  P50: {:?}", report.current_p50);
    println!("  P95: {:?}", report.current_p95);
    println!("  P99: {:?}", report.current_p99);
    if let Some(compliance) = report.latency_compliance {
        println!("  Compliance: {:.2}%", compliance * 100.0);
    }
    println!();

    println!("Throughput Metrics:");
    println!("  Current: {:.2} msg/sec", report.current_throughput);
    if let Some(compliance) = report.throughput_compliance {
        println!("  Compliance: {:.2}%", compliance * 100.0);
    }
    println!();

    println!("Error Rate:");
    println!("  Current: {:.2}%", report.current_error_rate * 100.0);
    if let Some(compliance) = report.error_rate_compliance {
        println!("  Compliance: {:.2}%", compliance * 100.0);
    }
    println!();

    println!("Window Information:");
    println!("  Total messages: {}", report.total_messages);
    println!("  Window duration: {:?}", report.window_duration);
    println!();

    // ========================================
    // Part 7: SLA Violations
    // ========================================
    if !report.violations.is_empty() {
        println!("⚠️  SLA Violations Detected:");
        println!("─────────────────────────────────────────\n");

        for (i, violation) in report.violations.iter().enumerate() {
            println!("Violation {}:", i + 1);
            println!("  Type: {:?}", violation.violation_type);
            println!("  Target: {:.3}", violation.target);
            println!("  Actual: {:.3}", violation.actual);
            println!("  Severity: {:.2}%", violation.severity * 100.0);
            println!();
        }
    } else {
        println!("✅ No SLA violations detected!\n");
    }

    // ========================================
    // Part 8: Critical System SLA
    // ========================================
    println!("🚨 Part 8: Critical System SLA Monitoring");
    println!("─────────────────────────────────────────\n");

    let critical_sla = SlaTarget::critical();
    let mut critical_monitor = SlaMonitor::new(critical_sla);

    println!("Critical SLA Targets:");
    println!("  P99 latency: 2 seconds");
    println!("  Max error rate: 0.1%");
    println!("  Min throughput: 50 msg/sec");
    println!();

    // Simulate critical system processing
    println!("Simulating critical system (50 messages)...");
    for i in 0..50 {
        let latency = Duration::from_millis(100 + (i % 10) * 20);
        critical_monitor.record_message(latency, true);
    }
    println!("✓ Processing complete\n");

    let critical_report = critical_monitor.generate_report();
    println!("Critical System Status: {:?}", critical_report.status);
    println!(
        "Compliance: {:.2}%",
        critical_report.overall_compliance * 100.0
    );

    match critical_report.status {
        ComplianceStatus::Compliant => println!("✅ Meeting all critical SLAs"),
        ComplianceStatus::Warning => println!("⚠️  Approaching SLA violation"),
        ComplianceStatus::Violation => println!("❌ SLA violation detected"),
    }
    println!();

    // ========================================
    // Summary
    // ========================================
    println!("═══════════════════════════════════════════");
    println!("Summary:");
    println!("  ✓ Message replay configuration");
    println!("  ✓ Selective replay filters");
    println!("  ✓ SLA monitoring setup");
    println!("  ✓ Real-time compliance tracking");
    println!("  ✓ Violation detection");
    println!();
    println!("These utilities help with:");
    println!("  • DLQ message recovery");
    println!("  • Selective message reprocessing");
    println!("  • SLA compliance monitoring");
    println!("  • Performance tracking");
    println!("  • Production incident response");
    println!("═══════════════════════════════════════════");
}
