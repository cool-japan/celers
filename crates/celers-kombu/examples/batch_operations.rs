//! Batch operations example
//!
//! This example demonstrates batch publishing and consuming operations
//! for high-throughput scenarios.
//!
//! Run with: cargo run --example batch_operations

use celers_kombu::*;

fn main() {
    println!("📦 Batch Operations Example\n");

    // 1. Batch Publish Result - Complete Success
    println!("1️⃣  Batch Publish - Complete Success");
    let result = BatchPublishResult::success(100);
    println!("   Total: {}", result.total());
    println!("   Succeeded: {}", result.succeeded);
    println!("   Failed: {}", result.failed);
    println!("   Is Complete Success: {}\n", result.is_complete_success());

    // 2. Batch Publish Result - Partial Success
    println!("2️⃣  Batch Publish - Partial Success");
    let partial = BatchPublishResult {
        succeeded: 80,
        failed: 20,
        errors: std::collections::HashMap::new(),
    };
    println!("   Total: {}", partial.total());
    println!("   Succeeded: {}", partial.succeeded);
    println!("   Failed: {}", partial.failed);
    println!(
        "   Is Complete Success: {}\n",
        partial.is_complete_success()
    );

    // 3. Batch Publish Result - Complete Failure
    println!("3️⃣  Batch Publish - Complete Failure");
    let failure = BatchPublishResult {
        succeeded: 0,
        failed: 50,
        errors: std::collections::HashMap::new(),
    };
    println!("   Total: {}", failure.total());
    println!("   Succeeded: {}", failure.succeeded);
    println!("   Failed: {}", failure.failed);
    println!(
        "   Is Complete Success: {}\n",
        failure.is_complete_success()
    );

    // 4. Batch Size Recommendations
    println!("4️⃣  Batch Size Recommendations");
    let scenarios = vec![
        ("Low Latency", "10-50 messages", "Prioritize response time"),
        (
            "Balanced",
            "50-200 messages",
            "Balance throughput and latency",
        ),
        (
            "High Throughput",
            "200-1000 messages",
            "Maximize throughput",
        ),
        ("Bulk Import", "1000+ messages", "Large data migrations"),
    ];

    for (scenario, size, description) in scenarios {
        println!("   {}: {} - {}", scenario, size, description);
    }

    // 5. Batch Operation Best Practices
    println!("\n5️⃣  Batch Operation Best Practices");
    println!("   ✅ Choose batch size based on message size and network MTU");
    println!("   ✅ Implement timeout for batch operations");
    println!("   ✅ Handle partial failures gracefully");
    println!("   ✅ Monitor batch success rates");
    println!("   ✅ Use batch operations for high-throughput scenarios");
    println!("   ✅ Consider memory constraints when batching");
    println!("   ✅ Implement retry logic for failed batch items");

    // 6. Performance Comparison
    println!("\n6️⃣  Performance Comparison (Estimated)");
    println!("   Individual Publish (1000 messages):");
    println!("     - 1000 network round-trips");
    println!("     - ~5-10 seconds");
    println!("\n   Batch Publish (1000 messages in batches of 100):");
    println!("     - 10 network round-trips");
    println!("     - ~0.5-1 seconds");
    println!("\n   Performance Gain: ~5-10x faster");

    // 7. Error Handling Strategy
    println!("\n7️⃣  Error Handling Strategy");
    println!("   When batch publish returns partial success:");
    println!("     1. Log the failure count and details");
    println!("     2. Retry failed messages individually or in smaller batches");
    println!("     3. Send permanently failed messages to DLQ");
    println!("     4. Update metrics for monitoring");
    println!("     5. Alert if failure rate exceeds threshold");

    println!("\n✨ Batch operations example completed successfully!");
}
