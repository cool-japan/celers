//! Production Patterns - Advanced Integration Example
//!
//! This example demonstrates how to combine multiple v6 features to build
//! a resilient, observable, and performant message processing system.
//!
//! Features demonstrated:
//! - Circuit breaker for connection resilience
//! - Exponential backoff retry with jitter
//! - Message compression for bandwidth optimization
//! - Topology validation before deployment
//! - Message tracing for observability
//! - Consumer group load balancing
//!
//! This represents a production-ready pattern for high-reliability systems.
//!
//! Run with: cargo run --example production_patterns

use celers_broker_amqp::{
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    compression::{compress_message, decompress_message, CompressionStats},
    consumer_groups::{ConsumerGroup, ConsumerInfo, LoadBalancingStrategy},
    monitoring::{analyze_amqp_consumer_lag, assess_amqp_queue_health},
    retry::{ExponentialBackoff, Jitter, RetryExecutor},
    topology::{
        analyze_topology_issues, BindingDefinition, ExchangeDefinition, QueueDefinition,
        TopologyValidator,
    },
    tracing_util::{MessageFlowAnalyzer, TraceEvent},
    utilities::{
        calculate_optimal_amqp_batch_size, calculate_optimal_amqp_prefetch,
        estimate_amqp_queue_memory,
    },
    AmqpExchangeType, QueueType,
};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏭 Production Patterns - Advanced Integration Example\n");

    // Phase 1: Topology Validation
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Phase 1: Topology Validation");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let _topology = validate_production_topology()?;
    println!("✓ Topology validated successfully\n");

    // Phase 2: Resource Planning
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Phase 2: Resource Planning & Optimization");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    plan_resources();
    println!();

    // Phase 3: Resilient Message Processing
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🛡️  Phase 3: Resilient Message Processing");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    resilient_message_processing().await?;
    println!();

    // Phase 4: Compressed Message Handling
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 Phase 4: Compressed Message Pipeline");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    compressed_message_pipeline()?;
    println!();

    // Phase 5: Consumer Group Orchestration
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("👥 Phase 5: Consumer Group Orchestration");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    orchestrate_consumer_groups()?;
    println!();

    // Phase 6: Message Flow Observability
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Phase 6: Message Flow Observability");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    observe_message_flow().await?;
    println!();

    // Phase 7: Health Assessment
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🏥 Phase 7: System Health Assessment");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    assess_system_health();
    println!();

    println!("✅ All production patterns demonstrated successfully!");
    println!("\n💡 Key Takeaways:");
    println!("  1. Validate topology before deployment");
    println!("  2. Use circuit breakers for external dependencies");
    println!("  3. Apply exponential backoff with jitter for retries");
    println!("  4. Compress large messages to save bandwidth");
    println!("  5. Implement consumer groups for load balancing");
    println!("  6. Track message flow for observability");
    println!("  7. Monitor queue health and consumer lag");

    Ok(())
}

/// Phase 1: Validate production topology
fn validate_production_topology() -> Result<TopologyValidator, Box<dyn std::error::Error>> {
    let mut validator = TopologyValidator::new();

    // Define production exchanges
    validator.add_exchange(ExchangeDefinition {
        name: "tasks".to_string(),
        exchange_type: AmqpExchangeType::Topic,
        durable: true,
        auto_delete: false,
    })?;

    validator.add_exchange(ExchangeDefinition {
        name: "tasks.dlx".to_string(),
        exchange_type: AmqpExchangeType::Direct,
        durable: true,
        auto_delete: false,
    })?;

    // Define production queues
    let queues = vec![
        ("tasks.high_priority", "tasks.high.#"),
        ("tasks.normal_priority", "tasks.normal.#"),
        ("tasks.low_priority", "tasks.low.#"),
        ("tasks.dlq", "dlq"),
    ];

    for (queue_name, routing_key) in queues {
        let exchange = if queue_name == "tasks.dlq" {
            "tasks.dlx"
        } else {
            "tasks"
        };

        validator.add_queue(QueueDefinition {
            name: queue_name.to_string(),
            durable: true,
            auto_delete: false,
            bindings: vec![BindingDefinition {
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
            }],
        })?;
    }

    // Validate the topology
    validator.validate()?;

    let summary = validator.summary();
    println!("📋 Topology Summary:");
    println!("  - Exchanges: {}", summary.total_exchanges);
    println!("  - Queues: {}", summary.total_queues);
    println!("  - Bindings: {}", summary.total_bindings);

    // Check for issues
    let issues = analyze_topology_issues(&validator);
    if issues.is_empty() {
        println!("  - Issues: None ✓");
    } else {
        println!("  - Issues found:");
        for issue in &issues {
            println!("    ⚠️  {}", issue);
        }
    }

    Ok(validator)
}

/// Phase 2: Plan resources and optimize configuration
fn plan_resources() {
    // Calculate optimal settings for production
    let avg_msg_size = 2048; // 2KB average message
    let target_latency_ms = 100;
    let queue_depth = 10000;

    let batch_size =
        calculate_optimal_amqp_batch_size(queue_depth, avg_msg_size, target_latency_ms);
    println!("📊 Resource Planning:");
    println!("  - Optimal batch size: {} messages", batch_size);

    let num_consumers = 5;
    let avg_process_time_ms = 50;
    let prefetch =
        calculate_optimal_amqp_prefetch(num_consumers, target_latency_ms, avg_process_time_ms);
    println!("  - Optimal prefetch: {} messages/consumer", prefetch);

    let memory = estimate_amqp_queue_memory(queue_depth, avg_msg_size, QueueType::Quorum);
    println!(
        "  - Estimated memory: {:.2} MB",
        memory as f64 / 1_048_576.0
    );
    println!("  - Recommended queue type: Quorum (for HA)");
}

/// Phase 3: Demonstrate resilient message processing with circuit breaker and retry
async fn resilient_message_processing() -> Result<(), Box<dyn std::error::Error>> {
    // Create circuit breaker for external API calls
    let cb_config = CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        timeout: Duration::from_secs(30),
        half_open_max_calls: 3,
    };
    let circuit_breaker = CircuitBreaker::new(cb_config);

    // Create retry strategy with exponential backoff
    let retry_strategy = ExponentialBackoff::new(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(10))
        .with_max_retries(3)
        .with_jitter(Jitter::Decorrelated);

    let executor = RetryExecutor::new(retry_strategy);

    println!("🛡️  Processing messages with resilience patterns:");

    // Simulate processing 5 messages
    let mut successful = 0;
    let mut failed = 0;

    for i in 1..=5 {
        let message_id = format!("msg-{}", i);

        // Simulate processing with circuit breaker and retry
        let result = circuit_breaker
            .call(async {
                // Simulate occasional failures (20% failure rate)
                if i == 3 {
                    Err("Simulated transient failure".to_string())
                } else {
                    Ok(())
                }
            })
            .await;

        match result {
            Ok(_) => {
                successful += 1;
                println!("  ✓ {} processed successfully", message_id);
            }
            Err(_e) => {
                // Retry with exponential backoff
                println!("  ⚠️  {} failed, retrying with backoff...", message_id);

                let retry_result = executor
                    .execute(|| {
                        Box::pin(async {
                            // Simulate recovery on retry
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            Ok::<(), String>(())
                        })
                    })
                    .await;

                match retry_result {
                    Ok(_) => {
                        successful += 1;
                        println!("  ✓ {} recovered on retry", message_id);
                    }
                    Err(_) => {
                        failed += 1;
                        println!("  ✗ {} failed after retries", message_id);
                    }
                }
            }
        }
    }

    println!("\n📊 Processing Results:");
    println!("  - Successful: {}", successful);
    println!("  - Failed: {}", failed);
    println!(
        "  - Success rate: {:.1}%",
        (successful as f64 / 5.0) * 100.0
    );

    let cb_metrics = circuit_breaker.get_metrics().await;
    println!("\n🔌 Circuit Breaker Status:");
    println!("  - State: {:?}", cb_metrics.state);
    println!("  - Failures: {}", cb_metrics.failure_count);
    println!("  - Successes: {}", cb_metrics.success_count);

    Ok(())
}

/// Phase 4: Demonstrate compressed message pipeline
fn compressed_message_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let mut stats = CompressionStats::new();

    // Simulate processing large messages
    let messages = vec![
        (
            "user_profile",
            serde_json::json!({"id": 1, "name": "Alice", "data": "x".repeat(1000)}),
        ),
        (
            "analytics_event",
            serde_json::json!({"event": "page_view", "timestamp": 1234567890, "details": "y".repeat(2000)}),
        ),
        (
            "log_entry",
            serde_json::json!({"level": "info", "message": "z".repeat(1500), "context": {}}),
        ),
    ];

    println!("📦 Compressed Message Pipeline:");

    for (msg_type, payload) in messages {
        let data = serde_json::to_vec(&payload)?;
        let original_size = data.len();

        // Use Zstd for better compression
        let compressed =
            compress_message(&data, celers_protocol::compression::CompressionType::Zstd)?;
        stats.record_compression(original_size, compressed.len());

        // Verify decompression
        let decompressed = decompress_message(
            &compressed,
            celers_protocol::compression::CompressionType::Zstd,
        )?;
        assert_eq!(data, decompressed);

        let ratio = original_size as f64 / compressed.len() as f64;
        println!(
            "  - {}: {} bytes → {} bytes ({:.2}x compression)",
            msg_type,
            original_size,
            compressed.len(),
            ratio
        );
    }

    println!("\n📊 Compression Statistics:");
    println!("  - Messages compressed: {}", stats.messages_compressed);
    println!(
        "  - Average compression: {:.2}x",
        stats.avg_compression_ratio
    );
    println!("  - Total savings: {:.1}%", stats.savings_percentage());
    println!(
        "  - Bytes saved: {} bytes",
        stats.original_bytes - stats.compressed_bytes
    );

    Ok(())
}

/// Phase 5: Orchestrate consumer groups with load balancing
fn orchestrate_consumer_groups() -> Result<(), Box<dyn std::error::Error>> {
    // Create consumer group with least connections strategy
    let mut group = ConsumerGroup::new(
        "production_workers".to_string(),
        LoadBalancingStrategy::LeastConnections,
    );

    // Add consumers with different capacities
    let consumers = vec![
        ("worker-1", 10, 8), // prefetch=10, priority=8 (high performance)
        ("worker-2", 15, 5), // prefetch=15, priority=5 (medium)
        ("worker-3", 8, 3),  // prefetch=8, priority=3 (lower capacity)
        ("worker-4", 12, 6), // prefetch=12, priority=6 (medium-high)
    ];

    for (id, prefetch, priority) in consumers {
        let consumer = ConsumerInfo::new(id.to_string(), "tasks.high_priority".to_string())
            .with_prefetch(prefetch)
            .with_priority(priority);
        group.add_consumer(consumer);
        println!(
            "  ✓ Added {} (prefetch={}, priority={})",
            id, prefetch, priority
        );
    }

    // Simulate message distribution
    println!("\n🔄 Distributing 20 messages:");
    let mut distribution: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for i in 1..=20 {
        if let Some(consumer_id) = group.next_consumer() {
            *distribution.entry(consumer_id.clone()).or_insert(0) += 1;

            // Record message processed
            if let Some(consumer) = group.get_consumer_mut(&consumer_id) {
                consumer.record_message_processed();
            }

            if i % 5 == 0 {
                print!(".");
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
        }
    }
    println!(" Done!\n");

    println!("📊 Distribution Results:");
    for (worker_id, count) in distribution {
        println!("  - {}: {} messages", worker_id, count);
    }

    let stats = group.get_statistics();
    println!("\n📈 Group Statistics:");
    println!(
        "  - Active consumers: {}/{}",
        stats.active_consumers, stats.total_consumers
    );
    println!("  - Total processed: {}", stats.total_messages_processed);
    println!(
        "  - Avg per consumer: {:.1}",
        stats.avg_messages_per_consumer
    );
    println!("  - Utilization: {:.1}%", stats.utilization());
    println!(
        "  - Health: {}",
        if stats.is_healthy() {
            "✓ Healthy"
        } else {
            "✗ Unhealthy"
        }
    );

    Ok(())
}

/// Phase 6: Observe message flow and analyze patterns
async fn observe_message_flow() -> Result<(), Box<dyn std::error::Error>> {
    let mut analyzer = MessageFlowAnalyzer::new();

    println!("📊 Tracking message flow (30 messages):");

    let start_time = Instant::now();

    for i in 1..=30 {
        let msg_id = format!("msg-{:03}", i);
        let now = Instant::now();

        // Published
        analyzer.record_event(
            msg_id.clone(),
            TraceEvent::Published {
                queue: "tasks.high_priority".to_string(),
                timestamp: now,
            },
        );

        // Simulate processing delay (5-15ms)
        let delay = 5 + (i % 10);
        tokio::time::sleep(Duration::from_millis(delay)).await;

        // Consumed
        analyzer.record_event(
            msg_id.clone(),
            TraceEvent::Consumed {
                queue: "tasks.high_priority".to_string(),
                timestamp: Instant::now(),
            },
        );

        // Result: 95% success, 5% rejection
        if i % 20 == 0 {
            analyzer.record_event(
                msg_id.clone(),
                TraceEvent::Rejected {
                    requeue: false,
                    timestamp: Instant::now(),
                },
            );
        } else {
            analyzer.record_event(
                msg_id.clone(),
                TraceEvent::Acknowledged {
                    timestamp: Instant::now(),
                },
            );
        }

        if i % 10 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    let elapsed = start_time.elapsed();
    println!(" Done in {:.2}ms\n", elapsed.as_millis());

    // Analyze flow
    let insights = analyzer.analyze();
    println!("📈 Flow Insights:");
    println!("  - Total messages: {}", insights.total_messages);
    println!("  - Successful: {}", insights.successful_messages);
    println!("  - Rejected: {}", insights.rejected_messages);
    println!("  - Success rate: {:.1}%", insights.success_rate);
    println!("  - Health status: {}", insights.health_status());

    if let Some(avg_time) = insights.average_processing_time {
        println!("  - Avg processing time: {:.2}ms", avg_time.as_millis());
    }

    Ok(())
}

/// Phase 7: Assess overall system health
fn assess_system_health() {
    println!("🏥 System Health Assessment:");

    // Simulate queue metrics
    let queue_depth = 1500_usize;
    let consumers = 5_usize;
    let processing_rate = 80.0_f64; // msgs/sec
    let memory_bytes = 5_000_000_usize; // 5MB

    // Analyze consumer lag
    let lag_analysis = analyze_amqp_consumer_lag(queue_depth, processing_rate, 60);
    println!("\n📊 Consumer Lag Analysis:");
    println!("  - Queue size: {} messages", lag_analysis.queue_size);
    println!(
        "  - Processing rate: {:.1} msg/sec",
        lag_analysis.processing_rate
    );
    println!("  - Current lag: {:.1} seconds", lag_analysis.lag_seconds);
    println!(
        "  - Is lagging: {}",
        if lag_analysis.is_lagging {
            "Yes ⚠️"
        } else {
            "No ✓"
        }
    );

    use celers_broker_amqp::monitoring::ScalingRecommendation;
    match lag_analysis.recommendation {
        ScalingRecommendation::ScaleUp { additional_workers } => {
            println!(
                "  - ⚠️  Recommendation: Scale up by {} workers",
                additional_workers
            );
        }
        ScalingRecommendation::ScaleDown { workers_to_remove } => {
            println!(
                "  - 💡 Recommendation: Can scale down by {} workers",
                workers_to_remove
            );
        }
        ScalingRecommendation::Optimal => {
            println!("  - ✓ Recommendation: Current worker count is optimal");
        }
    }

    // Assess queue health
    let health = assess_amqp_queue_health(
        "tasks.high_priority",
        queue_depth,
        consumers,
        processing_rate,
        memory_bytes,
    );

    println!("\n🏥 Queue Health:");
    println!("  - Overall status: {:?}", health.health);
    println!("  - Queue depth: {} messages", health.total_messages);
    println!("  - Consumers: {}", health.consumer_count);
    println!("  - Processing rate: {:.1} msg/sec", health.processing_rate);

    if !health.issues.is_empty() {
        println!("  - Issues:");
        for issue in &health.issues {
            println!("    ⚠️  {}", issue);
        }
    }

    if !health.recommendations.is_empty() {
        println!("  - Recommendations:");
        for rec in &health.recommendations {
            println!("    💡 {}", rec);
        }
    }
}
