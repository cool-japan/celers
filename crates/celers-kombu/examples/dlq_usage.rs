//! Dead Letter Queue (DLQ) usage example
//!
//! This example demonstrates how to configure and use Dead Letter Queues
//! for handling failed messages.
//!
//! Run with: cargo run --example dlq_usage

use celers_kombu::*;
use std::time::Duration;

fn main() {
    println!("💀 Dead Letter Queue (DLQ) Example\n");

    // 1. Basic DLQ Configuration
    println!("1️⃣  Basic DLQ Configuration");
    let dlq_config = DlqConfig::new("failed_tasks".to_string());
    println!("   Queue Name: {}", dlq_config.queue_name);
    println!("   Max Retries: {:?}", dlq_config.max_retries);
    println!("   TTL: {:?}", dlq_config.ttl);
    println!("   Include Metadata: {}\n", dlq_config.include_metadata);

    // 2. Custom DLQ Configuration
    println!("2️⃣  Custom DLQ Configuration");
    let custom_dlq = DlqConfig::new("custom_dlq".to_string())
        .with_max_retries(5)
        .with_ttl(Duration::from_secs(86400)) // 24 hours
        .with_metadata(true);

    println!("   Queue Name: {}", custom_dlq.queue_name);
    println!("   Max Retries: {:?}", custom_dlq.max_retries);
    println!("   TTL: {:?} (24 hours)", custom_dlq.ttl);
    println!("   Include Metadata: {}\n", custom_dlq.include_metadata);

    // 3. DLQ Configuration without retry limit
    println!("3️⃣  DLQ Without Retry Limit");
    let infinite_dlq = DlqConfig::new("infinite_retry_dlq".to_string()).without_retry_limit();

    println!("   Queue Name: {}", infinite_dlq.queue_name);
    println!(
        "   Max Retries: {:?} (unlimited)\n",
        infinite_dlq.max_retries
    );

    // 4. DLQ Statistics
    println!("4️⃣  DLQ Statistics");
    let mut stats = DlqStats::default();
    println!("   Empty: {}", stats.is_empty());
    println!("   Message Count: {}", stats.message_count);

    // Simulate some DLQ stats
    stats.message_count = 10;
    stats.by_reason.insert("timeout".to_string(), 6);
    stats.by_reason.insert("serialization_error".to_string(), 3);
    stats.by_reason.insert("unknown".to_string(), 1);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    stats.oldest_message_time = Some(now - 3600); // 1 hour ago
    stats.newest_message_time = Some(now);

    println!("\n   Updated Stats:");
    println!("   Empty: {}", stats.is_empty());
    println!("   Message Count: {}", stats.message_count);
    println!(
        "   Oldest Message Age: {:?} seconds",
        stats.oldest_message_age_secs()
    );
    println!("\n   Failures by Reason:");
    for (reason, count) in &stats.by_reason {
        println!("     - {}: {} messages", reason, count);
    }

    // 5. DLQ Best Practices
    println!("\n5️⃣  DLQ Best Practices");
    println!("   ✅ Set appropriate TTL to prevent DLQ from growing indefinitely");
    println!("   ✅ Monitor DLQ size and process failed messages periodically");
    println!("   ✅ Include metadata for debugging (original queue, timestamps, error)");
    println!("   ✅ Set reasonable max_retries to avoid infinite loops");
    println!("   ✅ Implement alerting when DLQ reaches certain threshold");
    println!("   ✅ Use different DLQs for different failure types if needed");

    // 6. Example DLQ Workflow
    println!("\n6️⃣  Example DLQ Workflow");
    println!("   1. Message processing fails");
    println!("   2. Check retry count against max_retries");
    println!("   3. If under limit: Requeue with incremented retry count");
    println!("   4. If over limit: Send to DLQ with failure reason");
    println!("   5. DLQ processor periodically reviews failed messages");
    println!("   6. Fix issues and retry from DLQ, or discard if unrecoverable");

    println!("\n✨ DLQ example completed successfully!");
}
