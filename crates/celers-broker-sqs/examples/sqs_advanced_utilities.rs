// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Advanced Utilities Example
//!
//! Demonstrates the three new advanced utility modules:
//! 1. Message Deduplication - Prevent duplicate message processing
//! 2. Advanced DLQ Analytics - Sophisticated error analysis and retry recommendations
//! 3. Observability Hooks - Custom metrics and logging integration
//!
//! Run with: cargo run --example advanced_utilities

use celers_broker_sqs::{
    dedup::{
        generate_content_hash, generate_task_signature, DeduplicationCache, DeduplicationStrategy,
    },
    dlq_analytics::{DlqAnalyzer, DlqMessage},
    hooks::{MetricsCollector, ObservabilityHooks},
};
use serde_json::json;
use std::time::{Duration, SystemTime};

fn main() {
    println!("=== Advanced Utilities Demo ===\n");

    // Example 1: Message Deduplication
    example_deduplication();

    // Example 2: Advanced DLQ Analytics
    example_dlq_analytics();

    // Example 3: Observability Hooks
    example_observability_hooks();

    // Example 4: Combined Usage
    example_combined_usage();
}

fn example_deduplication() {
    println!("--- Example 1: Message Deduplication ---\n");

    // Create a deduplication cache with 5-minute window
    let mut cache = DeduplicationCache::new(Duration::from_secs(300));

    // Simulate message processing with deduplication
    let messages = vec![
        ("msg-1", "tasks.process_order"),
        ("msg-2", "tasks.send_email"),
        ("msg-1", "tasks.process_order"), // Duplicate
        ("msg-3", "tasks.calculate"),
        ("msg-2", "tasks.send_email"), // Duplicate
    ];

    let mut processed = 0;
    let mut duplicates_detected = 0;

    for (msg_id, task_name) in messages {
        if cache.is_duplicate(msg_id, DeduplicationStrategy::MessageId) {
            println!("⚠️  Duplicate detected: {} - {}", msg_id, task_name);
            duplicates_detected += 1;
        } else {
            println!("✓ Processing: {} - {}", msg_id, task_name);
            cache.mark_processed(msg_id);
            processed += 1;
        }
    }

    println!("\nDeduplication Summary:");
    println!("  Messages processed: {}", processed);
    println!("  Duplicates detected: {}", duplicates_detected);
    println!("  Cache size: {}", cache.size());
    println!("  Duplicate count: {}", cache.duplicate_count());

    // Demonstrate content-based deduplication
    println!("\nContent-Based Deduplication:");

    let hash1 = generate_content_hash("tasks.process", &json!({"user_id": 123}));
    let hash2 = generate_content_hash("tasks.process", &json!({"user_id": 123}));
    let hash3 = generate_content_hash("tasks.process", &json!({"user_id": 456}));

    println!("  Same content hashes match: {}", hash1 == hash2);
    println!("  Different content hashes differ: {}", hash1 != hash3);

    // Demonstrate task signature deduplication
    println!("\nTask Signature Deduplication:");

    let sig1 = generate_task_signature(
        "tasks.send_email",
        &json!([]),
        &json!({"to": "user@example.com", "subject": "Hello"}),
    );
    let sig2 = generate_task_signature(
        "tasks.send_email",
        &json!([]),
        &json!({"subject": "Hello", "to": "user@example.com"}), // Different order
    );

    println!("  Signatures (different kwarg order): {}", sig1 == sig2);
    println!();
}

fn example_dlq_analytics() {
    println!("--- Example 2: Advanced DLQ Analytics ---\n");

    let mut analyzer = DlqAnalyzer::new();

    // Simulate DLQ messages with various error patterns
    let dlq_messages = vec![
        ("msg-1", "tasks.process_payment", "Connection timeout", 2),
        ("msg-2", "tasks.validate_data", "Invalid JSON format", 1),
        ("msg-3", "tasks.call_api", "Service unavailable", 5),
        ("msg-4", "tasks.process_order", "Network timeout", 1),
        ("msg-5", "tasks.send_notification", "Rate limit exceeded", 3),
        (
            "msg-6",
            "tasks.validate_user",
            "Validation failed: email format",
            1,
        ),
        ("msg-7", "tasks.process_payment", "Connection timeout", 4),
        ("msg-8", "tasks.call_api", "External service error", 7),
    ];

    for (msg_id, task_name, error_msg, failure_count) in dlq_messages {
        let error_pattern = DlqAnalyzer::classify_error(error_msg);
        let severity = DlqAnalyzer::determine_severity(&error_pattern, failure_count);

        let dlq_message = DlqMessage {
            message_id: msg_id.to_string(),
            task_name: task_name.to_string(),
            error_message: error_msg.to_string(),
            error_pattern: error_pattern.clone(),
            severity,
            failure_count,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        };

        analyzer.add_message(dlq_message);

        println!(
            "Message: {} | Pattern: {:?} | Severity: {:?}",
            msg_id, error_pattern, severity
        );
    }

    // Get statistics
    let stats = analyzer.statistics();
    println!("\nDLQ Analytics Summary:");
    println!("  Total errors: {}", stats.total_errors);
    println!("  Avg failure count: {:.2}", stats.avg_failure_count);
    println!("  Retryable messages: {}", stats.retryable_count);
    println!("  Non-retryable messages: {}", stats.non_retryable_count);

    // Top error patterns
    println!("\nTop Error Patterns:");
    for (pattern, count) in analyzer.top_error_patterns(3) {
        println!("  {:?}: {} occurrences", pattern, count);
    }

    // Top failing tasks
    println!("\nTop Failing Tasks:");
    for (task, count) in analyzer.top_failing_tasks(3) {
        println!("  {}: {} failures", task, count);
    }

    // Get retry recommendations
    println!("\nRetry Recommendations:");
    let retryable = analyzer.get_retryable_messages();
    for msg in retryable.iter().take(3) {
        let recommendation = DlqAnalyzer::recommend_retry(msg);
        println!("  {} ({}):", msg.message_id, msg.task_name);
        println!("    Should retry: {}", recommendation.should_retry);
        println!("    Delay: {}s", recommendation.recommended_delay_secs);
        println!("    Max retries: {}", recommendation.max_retries);
        println!("    Reason: {}", recommendation.reason);
        println!("    Confidence: {:.1}%", recommendation.confidence * 100.0);
    }
    println!();
}

fn example_observability_hooks() {
    println!("--- Example 3: Observability Hooks ---\n");

    // Create metrics collector
    let collector = MetricsCollector::new();

    // Create hooks that update the collector
    let hooks = collector.create_hooks();

    // Simulate broker operations
    println!("Simulating broker operations...\n");

    hooks.trigger_publish("orders-queue", 5, true);
    hooks.trigger_publish("notifications-queue", 10, true);
    hooks.trigger_consume("orders-queue", 5, Duration::from_millis(150));
    hooks.trigger_ack("orders-queue", 5);
    hooks.trigger_consume("notifications-queue", 8, Duration::from_millis(100));
    hooks.trigger_ack("notifications-queue", 8);
    hooks.trigger_error("orders-queue", "Temporary failure");

    println!("Metrics Summary:");
    println!("  Total publishes: {}", collector.total_publishes());
    println!("  Total consumes: {}", collector.total_consumes());
    println!("  Total acknowledgments: {}", collector.total_acks());
    println!("  Total errors: {}", collector.total_errors());
    println!(
        "  Messages published: {}",
        collector.total_messages_published()
    );
    println!(
        "  Messages consumed: {}",
        collector.total_messages_consumed()
    );

    // Custom hooks example
    println!("\nCustom Hooks Example:");

    let mut custom_hooks = ObservabilityHooks::new();

    // Add custom publish hook
    custom_hooks.on_publish(|event| {
        println!(
            "  📤 Published {} messages to {}",
            event.message_count, event.queue_name
        );
    });

    // Add custom consume hook
    custom_hooks.on_consume(|event| {
        if let Some(duration) = event.duration {
            println!(
                "  📥 Consumed {} messages from {} in {:?}",
                event.message_count, event.queue_name, duration
            );
        }
    });

    // Add custom error hook
    custom_hooks.on_error(|event| {
        println!(
            "  ⚠️  Error on {}: {:?}",
            event.queue_name, event.error_message
        );
    });

    println!();
    custom_hooks.trigger_publish("test-queue", 3, true);
    custom_hooks.trigger_consume("test-queue", 3, Duration::from_millis(50));
    custom_hooks.trigger_error("test-queue", "Test error");
    println!();
}

fn example_combined_usage() {
    println!("--- Example 4: Combined Usage ---\n");

    // Setup all three utilities
    let mut dedup_cache = DeduplicationCache::new(Duration::from_secs(300));
    let mut dlq_analyzer = DlqAnalyzer::new();
    let metrics_collector = MetricsCollector::new();
    let hooks = metrics_collector.create_hooks();

    println!("Processing message batch with full observability...\n");

    // Simulate processing messages with deduplication
    let messages = vec![
        ("msg-1", "tasks.process", "success"),
        ("msg-2", "tasks.validate", "timeout"),
        ("msg-1", "tasks.process", "duplicate"),
        ("msg-3", "tasks.transform", "success"),
        ("msg-4", "tasks.validate", "invalid_data"),
    ];

    for (msg_id, task_name, result) in messages {
        // Check for duplicates
        if dedup_cache.is_duplicate(msg_id, DeduplicationStrategy::MessageId) {
            println!("⚠️  Skipping duplicate: {}", msg_id);
            continue;
        }

        dedup_cache.mark_processed(msg_id);

        // Process message
        println!("✓ Processing: {} - {}", msg_id, task_name);
        hooks.trigger_publish("processing-queue", 1, true);

        match result {
            "success" => {
                hooks.trigger_consume("processing-queue", 1, Duration::from_millis(100));
                hooks.trigger_ack("processing-queue", 1);
            }
            error => {
                // Add to DLQ analyzer
                let error_pattern = DlqAnalyzer::classify_error(error);
                let severity = DlqAnalyzer::determine_severity(&error_pattern, 1);

                dlq_analyzer.add_message(DlqMessage {
                    message_id: msg_id.to_string(),
                    task_name: task_name.to_string(),
                    error_message: error.to_string(),
                    error_pattern,
                    severity,
                    failure_count: 1,
                    first_failure: SystemTime::now(),
                    last_failure: SystemTime::now(),
                });

                hooks.trigger_error("processing-queue", error);
            }
        }
    }

    // Final summary
    println!("\n=== Final Summary ===");

    println!("\nDeduplication:");
    println!("  Cache size: {}", dedup_cache.size());
    println!("  Duplicates detected: {}", dedup_cache.duplicate_count());

    println!("\nDLQ Analytics:");
    let stats = dlq_analyzer.statistics();
    println!("  Total errors: {}", stats.total_errors);
    println!("  Retryable: {}", stats.retryable_count);
    println!("  Non-retryable: {}", stats.non_retryable_count);

    println!("\nMetrics:");
    println!(
        "  Total operations: {}",
        metrics_collector.total_publishes()
            + metrics_collector.total_consumes()
            + metrics_collector.total_acks()
    );
    println!("  Errors: {}", metrics_collector.total_errors());
    println!(
        "  Messages processed: {}",
        metrics_collector.total_messages_consumed()
    );

    println!("\n✅ All utilities working together successfully!");
}
