//! Demonstration of v6 features
//!
//! This example showcases all the new v6 features:
//! - Circuit breaker pattern for resilience
//! - Advanced retry strategies with exponential backoff
//! - Message compression (gzip/zstd)
//! - Topology validation and analysis
//! - Message tracing and observability
//! - Consumer group management
//!
//! Run with: cargo run --example v6_features_demo

use celers_broker_amqp::{
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    compression::{compress_message, decompress_message, CompressionCodec, CompressionStats},
    consumer_groups::{ConsumerGroup, ConsumerInfo, LoadBalancingStrategy},
    retry::{ExponentialBackoff, Jitter, RetryStrategy},
    topology::{
        analyze_topology_issues, calculate_topology_complexity, validate_queue_naming,
        BindingDefinition, ExchangeDefinition, QueueDefinition, TopologyValidator,
    },
    tracing_util::{MessageFlowAnalyzer, TraceEvent},
    AmqpExchangeType,
};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 CeleRS AMQP Broker - v6 Features Demonstration\n");

    // 1. Circuit Breaker Pattern
    demonstrate_circuit_breaker().await?;

    // 2. Advanced Retry Strategies
    demonstrate_retry_strategies().await?;

    // 3. Message Compression
    demonstrate_compression()?;

    // 4. Topology Validation
    demonstrate_topology_validation()?;

    // 5. Message Tracing
    demonstrate_message_tracing().await?;

    // 6. Consumer Group Management
    demonstrate_consumer_groups()?;

    println!("\n✅ All v6 features demonstrated successfully!");
    Ok(())
}

async fn demonstrate_circuit_breaker() -> Result<(), Box<dyn std::error::Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🛡️  Circuit Breaker Pattern");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        timeout: Duration::from_secs(5),
        half_open_max_calls: 2,
    };

    let breaker = CircuitBreaker::new(config);
    println!("✓ Created circuit breaker (failure_threshold: 3, success_threshold: 2)");

    // Simulate successful operation
    let result = breaker.call(async { Ok::<(), String>(()) }).await;
    println!("✓ Successful operation: {:?}", result);

    // Simulate failures to open the circuit
    println!("\n⚠️  Simulating failures to trigger circuit opening...");
    for i in 1..=3 {
        let _result = breaker
            .call(async { Err::<(), String>("Simulated failure".to_string()) })
            .await;
        println!("  Failure {}/3", i);
    }

    let state = breaker.get_state().await;
    println!("✓ Circuit state: {:?}", state);

    let metrics = breaker.get_metrics().await;
    println!(
        "✓ Metrics - Failures: {}, State: {:?}",
        metrics.failure_count, metrics.state
    );

    println!();
    Ok(())
}

async fn demonstrate_retry_strategies() -> Result<(), Box<dyn std::error::Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔄 Advanced Retry Strategies");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Exponential backoff with full jitter
    let strategy = ExponentialBackoff::new(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(10))
        .with_max_retries(5)
        .with_jitter(Jitter::Full);

    println!("✓ Created exponential backoff strategy:");
    println!("  - Base delay: 100ms");
    println!("  - Max delay: 10s");
    println!("  - Max retries: 5");
    println!("  - Jitter: Full");

    println!("\n📊 Retry delays (with jitter):");
    for attempt in 0..5 {
        let delay = strategy.next_delay(attempt);
        println!("  Attempt {}: ~{:.2}ms", attempt + 1, delay.as_millis());
    }

    // Demonstrate different jitter strategies
    println!("\n📊 Comparing jitter strategies (attempt 3):");
    let jitter_types = [
        Jitter::None,
        Jitter::Full,
        Jitter::Equal,
        Jitter::Decorrelated,
    ];

    for jitter in jitter_types {
        let strategy = ExponentialBackoff::new(Duration::from_millis(100)).with_jitter(jitter);
        let delay = strategy.next_delay(2);
        println!("  {:?}: {:.2}ms", jitter, delay.as_millis());
    }

    println!();
    Ok(())
}

fn demonstrate_compression() -> Result<(), Box<dyn std::error::Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 Message Compression");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Create test data
    let data = b"Hello, World! This is a test message for compression. ".repeat(50);
    let original_size = data.len();

    println!("✓ Original message size: {} bytes", original_size);

    // Test Gzip compression
    let gzip_compressed = compress_message(&data, CompressionCodec::Gzip)?;
    let gzip_ratio = original_size as f64 / gzip_compressed.len() as f64;
    println!("\n📊 Gzip compression:");
    println!("  - Compressed size: {} bytes", gzip_compressed.len());
    println!("  - Compression ratio: {:.2}x", gzip_ratio);
    println!("  - Space saved: {:.1}%", (1.0 - 1.0 / gzip_ratio) * 100.0);

    // Test Zstd compression
    let zstd_compressed = compress_message(&data, CompressionCodec::Zstd)?;
    let zstd_ratio = original_size as f64 / zstd_compressed.len() as f64;
    println!("\n📊 Zstd compression:");
    println!("  - Compressed size: {} bytes", zstd_compressed.len());
    println!("  - Compression ratio: {:.2}x", zstd_ratio);
    println!("  - Space saved: {:.1}%", (1.0 - 1.0 / zstd_ratio) * 100.0);

    // Verify decompression
    let decompressed = decompress_message(&gzip_compressed, CompressionCodec::Gzip)?;
    assert_eq!(data, decompressed);
    println!("\n✓ Decompression verified successfully");

    // Track compression statistics
    let mut stats = CompressionStats::new();
    stats.record_compression(original_size, gzip_compressed.len());
    stats.record_compression(original_size, zstd_compressed.len());

    println!("\n📊 Compression statistics:");
    println!("  - Messages compressed: {}", stats.messages_compressed);
    println!("  - Total original: {} bytes", stats.original_bytes);
    println!("  - Total compressed: {} bytes", stats.compressed_bytes);
    println!("  - Average ratio: {:.2}x", stats.avg_compression_ratio);
    println!("  - Total savings: {:.1}%", stats.savings_percentage());

    println!();
    Ok(())
}

fn demonstrate_topology_validation() -> Result<(), Box<dyn std::error::Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔍 Topology Validation & Analysis");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut validator = TopologyValidator::new();

    // Add exchanges
    let task_exchange = ExchangeDefinition {
        name: "tasks".to_string(),
        exchange_type: AmqpExchangeType::Direct,
        durable: true,
        auto_delete: false,
    };
    validator.add_exchange(task_exchange)?;
    println!("✓ Added exchange: tasks (direct, durable)");

    let events_exchange = ExchangeDefinition {
        name: "events".to_string(),
        exchange_type: AmqpExchangeType::Fanout,
        durable: true,
        auto_delete: false,
    };
    validator.add_exchange(events_exchange)?;
    println!("✓ Added exchange: events (fanout, durable)");

    // Add queues
    let high_priority = QueueDefinition {
        name: "tasks.high".to_string(),
        durable: true,
        auto_delete: false,
        bindings: vec![BindingDefinition {
            exchange: "tasks".to_string(),
            routing_key: "high".to_string(),
        }],
    };
    validator.add_queue(high_priority)?;
    println!("✓ Added queue: tasks.high (bound to tasks/high)");

    let low_priority = QueueDefinition {
        name: "tasks.low".to_string(),
        durable: true,
        auto_delete: false,
        bindings: vec![BindingDefinition {
            exchange: "tasks".to_string(),
            routing_key: "low".to_string(),
        }],
    };
    validator.add_queue(low_priority)?;
    println!("✓ Added queue: tasks.low (bound to tasks/low)");

    // Add unbound queue for demonstration
    let orphan_queue = QueueDefinition {
        name: "orphan_queue".to_string(),
        durable: false,
        auto_delete: true,
        bindings: vec![],
    };
    validator.add_queue(orphan_queue)?;
    println!("✓ Added queue: orphan_queue (unbound)");

    // Validate topology
    println!("\n🔍 Validating topology...");
    validator.validate()?;
    println!("✓ Topology validation passed");

    // Get summary
    let summary = validator.summary();
    println!("\n📊 Topology summary:");
    println!("  - Total exchanges: {}", summary.total_exchanges);
    println!("  - Total queues: {}", summary.total_queues);
    println!("  - Total bindings: {}", summary.total_bindings);
    println!("  - Durable exchanges: {}", summary.durable_exchanges);
    println!("  - Durable queues: {}", summary.durable_queues);

    // Calculate complexity
    let complexity = calculate_topology_complexity(
        summary.total_exchanges,
        summary.total_queues,
        summary.total_bindings,
    );
    println!("  - Complexity score: {:.1}/100", complexity);

    // Find issues
    let issues = analyze_topology_issues(&validator);
    if !issues.is_empty() {
        println!("\n⚠️  Issues found:");
        for issue in &issues {
            println!("  - {}", issue);
        }
    }

    // Validate naming conventions
    println!("\n📝 Naming convention validation:");
    assert!(validate_queue_naming("tasks.high", "tasks.*"));
    assert!(validate_queue_naming("tasks.low", "tasks.*"));
    println!("  ✓ All queue names follow 'tasks.*' pattern");

    println!();
    Ok(())
}

async fn demonstrate_message_tracing() -> Result<(), Box<dyn std::error::Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Message Tracing & Observability");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut analyzer = MessageFlowAnalyzer::new();

    // Simulate message processing for 10 messages
    println!("🔄 Simulating message processing...");
    let start_time = Instant::now();

    for i in 1..=10 {
        let msg_id = format!("msg-{}", i);
        let now = Instant::now();

        // Published
        analyzer.record_event(
            msg_id.clone(),
            TraceEvent::Published {
                queue: "task_queue".to_string(),
                timestamp: now,
            },
        );

        // Simulate processing delay
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Consumed
        analyzer.record_event(
            msg_id.clone(),
            TraceEvent::Consumed {
                queue: "task_queue".to_string(),
                timestamp: Instant::now(),
            },
        );

        // Acknowledged (90% success rate)
        if i <= 9 {
            analyzer.record_event(
                msg_id.clone(),
                TraceEvent::Acknowledged {
                    timestamp: Instant::now(),
                },
            );
        } else {
            // Simulate rejection
            analyzer.record_event(
                msg_id.clone(),
                TraceEvent::Rejected {
                    requeue: false,
                    timestamp: Instant::now(),
                },
            );
        }

        if i % 3 == 0 {
            println!("  Processed {}/10 messages", i);
        }
    }

    let elapsed = start_time.elapsed();
    println!("✓ Processed 10 messages in {:.2}ms", elapsed.as_millis());

    // Analyze message flow
    let insights = analyzer.analyze();
    println!("\n📊 Message flow insights:");
    println!("  - Total messages: {}", insights.total_messages);
    println!("  - Successful: {}", insights.successful_messages);
    println!("  - Rejected: {}", insights.rejected_messages);
    println!("  - Dead-lettered: {}", insights.dead_lettered_messages);
    println!("  - Success rate: {:.1}%", insights.success_rate);
    println!("  - Rejection rate: {:.1}%", insights.rejection_rate);
    println!("  - Health status: {}", insights.health_status());

    if let Some(avg_time) = insights.average_processing_time {
        println!("  - Avg processing time: {:.2}ms", avg_time.as_millis());
    }

    println!();
    Ok(())
}

fn demonstrate_consumer_groups() -> Result<(), Box<dyn std::error::Error>> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("👥 Consumer Group Management");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Create consumer group with round-robin strategy
    let mut group = ConsumerGroup::new(
        "task_workers".to_string(),
        LoadBalancingStrategy::RoundRobin,
    );
    println!("✓ Created consumer group: task_workers (round-robin)");

    // Add consumers
    let consumer1 = ConsumerInfo::new("worker-1".to_string(), "task_queue".to_string())
        .with_prefetch(10)
        .with_priority(5);
    group.add_consumer(consumer1);
    println!("  ✓ Added worker-1 (prefetch: 10, priority: 5)");

    let consumer2 = ConsumerInfo::new("worker-2".to_string(), "task_queue".to_string())
        .with_prefetch(15)
        .with_priority(3);
    group.add_consumer(consumer2);
    println!("  ✓ Added worker-2 (prefetch: 15, priority: 3)");

    let consumer3 = ConsumerInfo::new("worker-3".to_string(), "task_queue".to_string())
        .with_prefetch(20)
        .with_priority(8);
    group.add_consumer(consumer3);
    println!("  ✓ Added worker-3 (prefetch: 20, priority: 8)");

    // Simulate message distribution
    println!("\n🔄 Distributing 10 messages (round-robin):");
    for i in 1..=10 {
        if let Some(consumer_id) = group.next_consumer() {
            println!("  Message {} → {}", i, consumer_id);

            // Record message processed
            if let Some(consumer) = group.get_consumer_mut(&consumer_id) {
                consumer.record_message_processed();
            }
        }
    }

    // Get group statistics
    let stats = group.get_statistics();
    println!("\n📊 Group statistics:");
    println!("  - Total consumers: {}", stats.total_consumers);
    println!("  - Active consumers: {}", stats.active_consumers);
    println!("  - Inactive consumers: {}", stats.inactive_consumers);
    println!(
        "  - Total messages processed: {}",
        stats.total_messages_processed
    );
    println!(
        "  - Avg messages per consumer: {:.1}",
        stats.avg_messages_per_consumer
    );
    println!("  - Utilization: {:.1}%", stats.utilization());
    println!("  - Strategy: {}", stats.strategy);
    println!(
        "  - Health: {}",
        if stats.is_healthy() {
            "✓ Healthy"
        } else {
            "✗ Unhealthy"
        }
    );

    // Demonstrate other load balancing strategies
    println!("\n📊 Testing other load balancing strategies:");

    let strategies = [
        LoadBalancingStrategy::LeastConnections,
        LoadBalancingStrategy::Priority,
        LoadBalancingStrategy::Random,
    ];

    for strategy in strategies {
        let mut test_group = ConsumerGroup::new("test_group".to_string(), strategy);
        test_group
            .add_consumer(ConsumerInfo::new("w1".to_string(), "q".to_string()).with_priority(10));
        test_group
            .add_consumer(ConsumerInfo::new("w2".to_string(), "q".to_string()).with_priority(5));

        if let Some(selected) = test_group.next_consumer() {
            println!("  {:?} → selected: {}", strategy, selected);
        }
    }

    println!();
    Ok(())
}
