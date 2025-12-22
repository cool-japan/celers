//! Utilities showcase example
//!
//! This example demonstrates practical usage of celers-kombu utility functions
//! for capacity planning, optimization, and monitoring.
//!
//! Run with: cargo run --example utilities_showcase

use celers_kombu::utils::*;
use celers_kombu::*;

fn main() {
    println!("🛠️  Utilities Showcase\n");
    println!("Demonstrating practical usage of celers-kombu utility functions\n");

    // =========================================================================
    // 1. Batch Optimization
    // =========================================================================
    println!("📦 Batch Optimization");
    println!("   Calculate optimal batch sizes for different workloads\n");

    // Small messages, high throughput
    let batch_size_small = calculate_optimal_batch_size(
        512,   // 512 bytes per message
        10000, // 10K messages/sec target
        1000,  // max batch size
    );
    println!("   Small Messages (512B, 10K msg/sec):");
    println!("     Recommended batch size: {}", batch_size_small);

    // Large messages, moderate throughput
    let batch_size_large = calculate_optimal_batch_size(
        102400, // 100KB per message
        100,    // 100 messages/sec
        50,     // max batch size
    );
    println!("\n   Large Messages (100KB, 100 msg/sec):");
    println!("     Recommended batch size: {}", batch_size_large);

    // Calculate optimal worker count
    let workers = calculate_optimal_workers(
        5000, // queue size
        200,  // 200ms avg processing time
        60,   // target drain time 60 secs
        16,   // max workers
    );
    println!("\n   Worker Optimization:");
    println!("     Queue size: 5000, Processing time: 200ms");
    println!("     Target drain time: 60s, Max workers: 16");
    println!("     Recommended workers: {}", workers);

    // =========================================================================
    // 2. Routing Pattern Matching
    // =========================================================================
    println!("\n🔀 Routing Pattern Matching");
    println!("   AMQP-style topic routing with wildcards\n");

    let test_cases = vec![
        ("stock.usd.nyse", "stock.*.nyse", true),
        ("stock.eur.london", "stock.#", true),
        ("quick.orange.rabbit", "*.orange.*", true),
        ("quick.brown.fox", "*.orange.*", false),
        ("order.created.payment", "order.*.#", true),
    ];

    for (routing_key, pattern, expected) in test_cases {
        let matched = match_routing_pattern(routing_key, pattern);
        let status = if matched == expected { "✓" } else { "✗" };
        println!(
            "   {} '{}' matches '{}': {}",
            status, routing_key, pattern, matched
        );
    }

    // Direct routing
    println!("\n   Direct Routing:");
    println!(
        "     'orders' == 'orders': {}",
        match_direct_routing("orders", "orders")
    );
    println!(
        "     'orders' == 'tasks': {}",
        match_direct_routing("orders", "tasks")
    );

    // Fanout always matches
    println!("\n   Fanout Routing:");
    println!("     Always matches: {}", match_fanout_routing("any.key"));

    // =========================================================================
    // 3. Performance Analysis
    // =========================================================================
    println!("\n📊 Performance Analysis");
    println!("   Analyze broker metrics and performance\n");

    let metrics = BrokerMetrics {
        messages_published: 10000,
        messages_consumed: 9500,
        messages_acknowledged: 9000,
        messages_rejected: 500,
        publish_errors: 50,
        consume_errors: 100,
        active_connections: 5,
        connection_attempts: 10,
        connection_failures: 1,
    };

    let perf = analyze_broker_performance(&metrics);
    println!("   Broker Performance:");
    println!("     Success Rate: {:.2}%", perf.0 * 100.0);
    println!("     Error Rate: {:.2}%", perf.1 * 100.0);
    println!("     Acknowledgment Rate: {:.2}%", perf.2 * 100.0);

    // Throughput calculation
    let throughput = calculate_throughput(9500, 60.0);
    println!("\n   Throughput:");
    println!("     9500 messages in 60 seconds");
    println!("     Rate: {:.2} msg/sec", throughput);

    // Average latency
    let latency_samples = vec![10, 20, 15, 25, 30, 18, 22];
    let total_latency: u64 = latency_samples.iter().sum();
    let avg_latency = calculate_avg_latency(total_latency as f64, latency_samples.len() as u64);
    println!("\n   Latency:");
    println!("     Samples: {:?} ms", latency_samples);
    println!(
        "     Total: {} ms, Count: {}",
        total_latency,
        latency_samples.len()
    );
    println!("     Average: {:.2} ms", avg_latency);

    // =========================================================================
    // 4. Queue Health Monitoring
    // =========================================================================
    println!("\n🏥 Queue Health Monitoring");
    println!("   Monitor queue health and estimate drain times\n");

    let health = analyze_queue_health(1500, 1000, 5000);
    println!("   Queue: 1500 messages (thresholds: 1000 warn, 5000 max)");
    match health {
        "healthy" => println!("     Status: ✓ Healthy"),
        "warning" => println!("     Status: ⚠ Warning (above threshold)"),
        "critical" => println!("     Status: ✗ Critical (above max threshold)"),
        _ => println!("     Status: Unknown"),
    }

    let drain_time = estimate_drain_time(1500, 100);
    println!("\n   Drain Time Estimation:");
    println!("     1500 messages at 100 msg/sec");
    println!("     Estimated time: {:.1} seconds", drain_time);

    // Memory estimation
    let queue_memory = estimate_queue_memory(1000, 5120);
    println!("\n   Memory Estimation:");
    println!("     1000 messages × 5KB avg");
    println!(
        "     Estimated memory: {:.2} MB",
        queue_memory as f64 / 1024.0 / 1024.0
    );

    // =========================================================================
    // 5. Load Balancing
    // =========================================================================
    println!("\n⚖️  Load Balancing");
    println!("   Distribute load across multiple queues\n");

    let queue_sizes = vec![100, 500, 200, 800, 300];
    let distribution = calculate_load_distribution(&queue_sizes, 10);
    println!("   Queue Sizes: {:?}", queue_sizes);
    println!("   Total Workers: 10");
    println!("   Recommended Distribution:");
    for (queue_idx, workers) in &distribution {
        println!("     Queue {}: {} workers", queue_idx, workers);
    }

    // =========================================================================
    // 6. Circuit Breaker Analysis
    // =========================================================================
    println!("\n🛡️  Circuit Breaker Analysis");
    println!("   Analyze circuit breaker state and health\n");

    let failures = 50;
    let successes = 950;
    let total_requests = 1000;

    let (health_score, action) = analyze_circuit_breaker(failures, successes, total_requests);
    println!("   Circuit Breaker:");
    println!("     Success: 950/1000 (95.0%)");
    println!("     Health Score: {:.2}", health_score);
    println!("     Recommended Action: {}", action);

    // =========================================================================
    // 7. Connection Pool Analysis
    // =========================================================================
    println!("\n💧 Connection Pool Analysis");
    println!("   Monitor pool health and efficiency\n");

    let active_conns = 15;
    let idle_conns = 5;
    let max_conns = 20;
    let total_reqs = 10000;
    let timeout_count = 5;

    let (health_score, status) = analyze_pool_health(
        active_conns,
        idle_conns,
        max_conns,
        total_reqs,
        timeout_count,
    );
    println!("   Connection Pool:");
    println!(
        "     Active: {}, Idle: {}, Max: {}",
        active_conns, idle_conns, max_conns
    );
    println!("     Health Score: {:.2}", health_score);
    println!("     Status: {}", status);

    // =========================================================================
    // 8. Exponential Backoff
    // =========================================================================
    println!("\n⏱️  Exponential Backoff");
    println!("   Calculate retry delays with jitter\n");

    for attempt in 0..5 {
        let delay_ms = calculate_backoff_delay(attempt, 100, 60000, 0.1);
        println!("     Attempt {}: {} ms", attempt, delay_ms);
    }

    // =========================================================================
    // 9. Message Size Estimation
    // =========================================================================
    println!("\n📏 Message Size Estimation");
    println!("   Estimate serialized message sizes\n");

    use celers_protocol::Message;
    use uuid::Uuid;

    let sample_message = Message::new(
        "process_payment_task".to_string(),
        Uuid::new_v4(),
        vec![0u8; 1024],
    );
    let estimated_size = estimate_message_size(&sample_message);
    println!("   Task: 'process_payment_task'");
    println!("   Body: 1024 bytes");
    println!("   Estimated total: {} bytes", estimated_size);

    // =========================================================================
    // 10. Deduplication ID Generation
    // =========================================================================
    println!("\n🔑 Deduplication ID");
    println!("   Generate stable message IDs for deduplication\n");

    let task_name = "process_payment";
    let args = b"order123,user456";
    let dedup_id = generate_deduplication_id(task_name, args);
    println!("   Task: '{}'", task_name);
    println!("   Args: {}", String::from_utf8_lossy(args));
    println!("   Dedup ID: {}", dedup_id);

    // =========================================================================
    // 11. Priority Scoring
    // =========================================================================
    println!("\n🏆 Priority Scoring");
    println!("   Calculate message priority scores\n");

    let score_high = calculate_priority_score(Priority::High, 30, 0);
    let score_normal = calculate_priority_score(Priority::Normal, 60, 2);
    let score_low = calculate_priority_score(Priority::Low, 15, 5);

    println!("   High priority, 30s old, 0 retries: {:.2}", score_high);
    println!(
        "   Normal priority, 60s old, 2 retries: {:.2}",
        score_normal
    );
    println!("   Low priority, 15s old, 5 retries: {:.2}", score_low);

    // =========================================================================
    // 12. Stale Message Detection
    // =========================================================================
    println!("\n🕐 Stale Message Detection");
    println!("   Identify messages exceeding age threshold\n");

    let message_ages = vec![30, 150, 45, 200, 10, 180, 25];
    let stale = identify_stale_messages(&message_ages, 100);
    println!("   Message ages (seconds): {:?}", message_ages);
    println!("   Threshold: 100 seconds");
    println!("   Stale message indices: {:?}", stale);

    // =========================================================================
    // 13. Batch Grouping
    // =========================================================================
    println!("\n📦 Batch Grouping");
    println!("   Suggest optimal message batches by size\n");

    let message_sizes = vec![1024, 2048, 512, 4096, 1536, 3072, 768];
    let groups = suggest_batch_groups(&message_sizes, 8192);
    println!("   Message sizes: {:?}", message_sizes);
    println!("   Max batch size: 8192 bytes");
    println!("   Suggested groups: {:?}", groups);

    println!("\n✅ Utilities showcase complete!");
    println!("\nThese utilities help with:");
    println!("  • Capacity planning and optimization");
    println!("  • Performance monitoring and analysis");
    println!("  • Load balancing and distribution");
    println!("  • Resilience and circuit breaking");
    println!("  • Message routing and prioritization");
    println!("  • Cost estimation and prediction");
}
