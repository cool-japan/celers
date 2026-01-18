//! Flow control and resilience example
//!
//! This example demonstrates flow control and resilience features:
//! - Backpressure configuration (prevent overload)
//! - Poison message detection (prevent infinite retry loops)
//! - Timeout middleware (enforce processing time limits)
//! - Filter middleware (selective message processing)
//!
//! Run with: cargo run --example flow_control

use celers_kombu::*;
use celers_protocol::Message;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔧 Flow Control and Resilience Example\n");

    // =========================================================================
    // 1. Backpressure Configuration
    // =========================================================================
    println!("🚦 Backpressure Configuration");
    println!("   Prevent system overload with flow control\n");

    let backpressure = BackpressureConfig::new()
        .with_max_pending(1000)
        .with_max_queue_size(10000)
        .with_high_watermark(0.8)
        .with_low_watermark(0.6);

    println!("   Configuration:");
    println!("     Max pending: {}", backpressure.max_pending);
    println!("     Max queue size: {}", backpressure.max_queue_size);
    println!(
        "     High watermark: {:.0}%",
        backpressure.high_watermark * 100.0
    );
    println!(
        "     Low watermark: {:.0}%",
        backpressure.low_watermark * 100.0
    );

    // Simulate different load scenarios
    println!("\n   Load Scenarios:");

    let scenarios = vec![
        (500, "Normal load"),
        (800, "Approaching high watermark"),
        (900, "Backpressure triggered"),
        (600, "Backpressure released"),
        (10000, "At capacity"),
    ];

    for (pending, description) in scenarios {
        let apply_bp = backpressure.should_apply_backpressure(pending);
        let release_bp = backpressure.should_release_backpressure(pending);
        let at_capacity = backpressure.is_at_capacity(pending);

        println!("\n     Pending: {} - {}", pending, description);
        println!("       Apply backpressure: {}", apply_bp);
        println!("       Release backpressure: {}", release_bp);
        println!("       At capacity: {}", at_capacity);

        if at_capacity {
            println!("       ⚠️  Action: Reject new messages");
        } else if apply_bp {
            println!("       ⚠️  Action: Slow down producers");
        } else if release_bp {
            println!("       ✅ Action: Resume normal speed");
        } else {
            println!("       ✅ Action: Normal processing");
        }
    }

    println!("\n   ✅ Use Case: Prevent system overload");
    println!("      - Protect consumers from message floods");
    println!("      - Smooth out traffic spikes");
    println!("      - Maintain system stability under load");
    println!("      - Gradual throttling with watermarks");

    // =========================================================================
    // 2. Poison Message Detection
    // =========================================================================
    println!("\n☠️  Poison Message Detection");
    println!("   Prevent infinite retry loops\n");

    let detector = PoisonMessageDetector::new()
        .with_max_failures(5)
        .with_failure_window(Duration::from_secs(3600));

    println!("   Configuration:");
    println!("     Max failures: {}", detector.max_failures);
    println!("     Failure window: {:?}", detector.failure_window);

    // Simulate message processing with failures
    let task_id = Uuid::new_v4();
    println!("\n   Processing Message: {}", task_id);

    for attempt in 1..=7 {
        let is_poison_before = detector.is_poison(task_id);

        if is_poison_before {
            println!(
                "\n     Attempt {}: ☠️  Message is POISON - sending to DLQ",
                attempt
            );
            println!("       Total failures: {}", detector.failure_count(task_id));
            println!("       Action: Stop retrying, investigate issue");
            break;
        }

        // Simulate processing failure
        detector.record_failure(task_id);
        let failure_count = detector.failure_count(task_id);

        println!("\n     Attempt {}: ❌ Processing failed", attempt);
        println!(
            "       Failure count: {}/{}",
            failure_count, detector.max_failures
        );

        if failure_count < detector.max_failures {
            println!("       Action: Retry with exponential backoff");
        } else {
            println!("       Action: Mark as poison, send to DLQ");
        }
    }

    // Demonstrate clearing failures
    println!("\n   Clearing Failures:");
    println!("     Clear specific message: detector.clear_failures(task_id)");
    println!("     Clear all messages: detector.clear_all()");

    detector.clear_failures(task_id);
    println!("     Failures cleared for {}", task_id);
    println!("     Is poison now: {}", detector.is_poison(task_id));

    println!("\n   ✅ Use Case: Prevent infinite retry loops");
    println!("      - Detect messages that repeatedly fail");
    println!("      - Automatically send to DLQ after threshold");
    println!("      - Track failure patterns");
    println!("      - Prevent resource waste on bad messages");

    // =========================================================================
    // 3. Timeout Middleware
    // =========================================================================
    println!("\n⏱️  Timeout Middleware");
    println!("   Enforce processing time limits\n");

    let timeout_middleware = TimeoutMiddleware::new(Duration::from_secs(30));

    println!("   Configuration:");
    println!("     Timeout: {:?}", timeout_middleware.timeout());

    // Create test message
    let mut message = Message::new(
        "long_running_task".to_string(),
        Uuid::new_v4(),
        b"Process this with timeout".to_vec(),
    );

    println!("\n   Processing with timeout:");
    println!("     Task: {}", message.task_name());
    println!("     Timeout: {:?}", timeout_middleware.timeout());

    // Apply timeout middleware
    match timeout_middleware.before_publish(&mut message).await {
        Ok(_) => {
            println!("     ✅ Timeout metadata added to message");
            if let Some(timeout_val) = message.headers.extra.get("x-timeout") {
                if let Some(timeout_str) = timeout_val.as_str() {
                    println!("     Header 'x-timeout': {}", timeout_str);
                }
            }
        }
        Err(e) => {
            println!("     ❌ Error: {}", e);
        }
    }

    println!("\n   ✅ Use Case: Prevent hung workers");
    println!("      - Enforce maximum processing time");
    println!("      - Detect and kill stuck tasks");
    println!("      - Improve system responsiveness");
    println!("      - SLA compliance for task execution");

    // =========================================================================
    // 4. Filter Middleware
    // =========================================================================
    println!("\n🔍 Filter Middleware");
    println!("   Selective message processing\n");

    // Filter for high-priority messages only
    let high_priority_filter = FilterMiddleware::new(|msg: &Message| {
        msg.headers
            .extra
            .get("priority")
            .and_then(|v| v.as_str())
            .map(|p| p == "high")
            .unwrap_or(false)
    });

    println!("   Filter: High-priority messages only\n");

    // Test messages with different priorities
    let test_cases = vec![
        ("high", true),
        ("normal", false),
        ("low", false),
        ("high", true),
    ];

    for (priority, expected_pass) in test_cases {
        let mut msg = Message::new(
            format!("{}_priority_task", priority),
            Uuid::new_v4(),
            vec![],
        );
        msg.headers.extra.insert(
            "priority".to_string(),
            serde_json::Value::String(priority.to_string()),
        );

        println!("   Message: {} priority", priority);
        match high_priority_filter.after_consume(&mut msg).await {
            Ok(_) => {
                println!("     ✅ PASS - Message accepted");
                assert!(expected_pass, "Expected to reject");
            }
            Err(e) => {
                println!("     ❌ REJECT - {}", e);
                assert!(!expected_pass, "Expected to pass");
            }
        }
    }

    // Example: Complex filter with multiple conditions
    println!("\n   Complex Filter Example:");
    let _complex_filter = FilterMiddleware::new(|msg: &Message| {
        let has_valid_source = msg
            .headers
            .extra
            .get("source")
            .and_then(|v| v.as_str())
            .map(|s| s == "api" || s == "scheduler")
            .unwrap_or(false);

        let has_valid_version = msg
            .headers
            .extra
            .get("api-version")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f32>().ok())
            .map(|v| v >= 2.0)
            .unwrap_or(false);

        has_valid_source && has_valid_version
    });

    println!("     Conditions:");
    println!("       - Source must be 'api' or 'scheduler'");
    println!("       - API version must be >= 2.0");

    println!("\n   ✅ Use Case: Selective processing");
    println!("      - Filter messages by criteria");
    println!("      - Implement message routing logic");
    println!("      - Version-based message handling");
    println!("      - Source validation and filtering");

    // =========================================================================
    // 5. Integrated Flow Control Example
    // =========================================================================
    println!("\n🔄 Integrated Flow Control Example");
    println!("   Combining all resilience features\n");

    let task_id = Uuid::new_v4();
    println!("   Scenario: Processing messages under load");
    println!("     Task ID: {}", task_id);
    println!("\n   Flow:");
    println!("     1. Check backpressure before accepting message");
    println!("        → If high watermark reached, apply backpressure");
    println!("     2. Apply filter middleware to select relevant messages");
    println!("        → Reject messages that don't match criteria");
    println!("     3. Apply timeout middleware for processing limits");
    println!("        → Kill tasks exceeding time limit");
    println!("     4. Track failures with poison detector");
    println!("        → Send repeatedly failing messages to DLQ");
    println!("     5. Release backpressure when load decreases");
    println!("        → Resume normal processing speed");

    println!("\n   Benefits:");
    println!("     ✓ System stability under load");
    println!("     ✓ Prevents resource exhaustion");
    println!("     ✓ Automatic failure handling");
    println!("     ✓ No manual intervention needed");
    println!("     ✓ Graceful degradation");

    // Example metrics after flow control
    println!("\n   Example Metrics:");
    println!("     Messages processed: 10,000");
    println!("     Backpressure events: 3");
    println!("     Filtered messages: 1,200");
    println!("     Timeout events: 5");
    println!("     Poison messages: 2");
    println!("     Success rate: 98.7%");

    println!("\n✨ Flow control example completed successfully!");
    Ok(())
}
