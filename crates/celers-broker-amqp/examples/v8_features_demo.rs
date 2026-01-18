//! V8 Features Demo - Advanced Production Capabilities
//!
//! This example demonstrates the v8 enhancements for advanced production systems:
//! - Lifecycle hooks for extensibility and validation
//! - Dead Letter Exchange analytics for failure analysis
//! - Adaptive batch optimization for dynamic performance tuning
//! - Performance profiling with percentile analysis
//!
//! These features enable robust, observable, and self-optimizing message processing.
//!
//! Run with: cargo run --example v8_features_demo

use async_trait::async_trait;
use celers_broker_amqp::{
    batch_optimizer::{BatchOptimizer, BatchOptimizerConfig},
    dlx_analytics::{DlxAnalyzer, DlxEvent, FailureReason},
    hooks::{AckStatus, AmqpHookRegistry, HookContext, HookResult, PublishHook},
    profiler::{AmqpProfiler, OperationType},
};
use celers_protocol::{builder::MessageBuilder, Message};
use std::time::{Duration, Instant, SystemTime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 V8 Features Demo - Advanced Production Capabilities\n");

    // Phase 1: Lifecycle Hooks
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🪝 Phase 1: Lifecycle Hooks");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_hooks().await?;
    println!();

    // Phase 2: DLX Analytics
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Phase 2: Dead Letter Exchange Analytics");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_dlx_analytics().await?;
    println!();

    // Phase 3: Adaptive Batching
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⚡ Phase 3: Adaptive Batch Optimization");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_adaptive_batching().await?;
    println!();

    // Phase 4: Performance Profiling
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📈 Phase 4: Performance Profiling");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_profiling().await?;
    println!();

    println!("✅ All v8 features demonstrated successfully!");
    println!("\n💡 Key Takeaways:");
    println!("  1. Use hooks for validation, enrichment, and observability");
    println!("  2. Monitor DLX patterns to identify and fix systemic issues");
    println!("  3. Enable adaptive batching for self-optimizing performance");
    println!("  4. Profile operations to identify bottlenecks and optimize");

    Ok(())
}

/// Demonstrate lifecycle hooks
async fn demonstrate_hooks() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Setting up lifecycle hooks");

    // Create hook registry
    let registry = AmqpHookRegistry::new();

    // Add validation hook
    registry.add_publish_hook(Box::new(PayloadSizeValidator { max_size: 100_000 }));

    println!("   ✓ Added payload size validator hook");

    // Test valid message
    let mut message = MessageBuilder::new("test.task")
        .args(vec![serde_json::json!({"data": "small payload"})])
        .build()?;

    let ctx = HookContext::new("test_queue".to_string());

    println!("\n2️⃣  Testing hook execution");

    match registry.execute_before_publish(&mut message, &ctx).await {
        Ok(()) => println!("   ✓ Valid message passed validation"),
        Err(e) => println!("   ✗ Validation failed: {}", e),
    }

    // Test acknowledgment hook
    let ack_status = AckStatus::Acknowledged { delivery_tag: 123 };
    registry
        .execute_on_acknowledgment(&ack_status, &ctx)
        .await
        .map_err(|e| format!("{}", e))?;
    println!("   ✓ Acknowledgment hook executed");

    println!("\n📊 Hook Registry Stats:");
    println!("   Publish hooks: {}", registry.publish_hook_count());
    println!("   Consume hooks: {}", registry.consume_hook_count());
    println!("   Ack hooks: {}", registry.acknowledgment_hook_count());

    Ok(())
}

/// Custom hook for validating payload sizes
struct PayloadSizeValidator {
    max_size: usize,
}

#[async_trait]
impl PublishHook for PayloadSizeValidator {
    async fn before_publish(&self, message: &mut Message, _ctx: &HookContext) -> HookResult<()> {
        let payload_size = serde_json::to_vec(&message.body)
            .map(|v| v.len())
            .unwrap_or(0);

        if payload_size > self.max_size {
            return Err(format!(
                "Payload size {} exceeds maximum {}",
                payload_size, self.max_size
            )
            .into());
        }
        Ok(())
    }
}

/// Demonstrate DLX analytics
async fn demonstrate_dlx_analytics() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Creating DLX analyzer");

    let analyzer = DlxAnalyzer::new("orders_dlx");

    // Simulate dead letter events
    println!("   Recording dead letter events...");

    // Rejected messages
    for i in 0..5 {
        analyzer
            .record_event(DlxEvent {
                message_id: format!("msg-rejected-{}", i),
                original_queue: "orders".to_string(),
                dlx_queue: "orders_dlx".to_string(),
                reason: FailureReason::Rejected,
                timestamp: SystemTime::now(),
                retry_count: 2,
                payload_size: 1024,
            })
            .await;
    }

    // Expired messages
    for i in 0..3 {
        analyzer
            .record_event(DlxEvent {
                message_id: format!("msg-expired-{}", i),
                original_queue: "orders".to_string(),
                dlx_queue: "orders_dlx".to_string(),
                reason: FailureReason::Expired,
                timestamp: SystemTime::now(),
                retry_count: 0,
                payload_size: 512,
            })
            .await;
    }

    println!("   ✓ Recorded {} events", analyzer.event_count().await);

    println!("\n2️⃣  Analyzing DLX patterns");

    let insights = analyzer.analyze().await;

    println!("\n📊 DLX Insights:");
    println!("   Total dead lettered: {}", insights.total_dead_lettered);
    println!("   Health status: {:?}", insights.health_status);
    println!(
        "   Most common failure: {:?}",
        insights.most_common_failure_reason
    );
    println!(
        "   Avg retry attempts: {:.1}",
        insights.average_retry_attempts
    );
    println!("   Recoverable: {:.0}%", insights.recoverable_percentage);

    println!("\n💡 Recommendations:");
    for (i, rec) in insights.recommendations.iter().enumerate() {
        println!("   {}. {}", i + 1, rec);
    }

    Ok(())
}

/// Demonstrate adaptive batch optimization
async fn demonstrate_adaptive_batching() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Creating batch optimizer");

    let config = BatchOptimizerConfig {
        min_batch_size: 10,
        max_batch_size: 500,
        target_latency: Duration::from_millis(100),
        max_acceptable_latency: Duration::from_millis(300),
        window_size: 50,
        adjustment_step: 0.15,
    };

    let optimizer = BatchOptimizer::with_config(config);

    println!(
        "   Initial batch size: {}",
        optimizer.get_optimal_batch_size().await
    );

    println!("\n2️⃣  Simulating operations and learning");

    // Simulate good performance
    for _ in 0..15 {
        optimizer
            .record_batch_operation(100, Duration::from_millis(80), true)
            .await;
    }

    let metrics = optimizer.get_metrics().await;
    println!("\n   After 15 successful operations:");
    println!("   Optimal batch size: {}", metrics.optimal_batch_size);
    println!("   Avg latency: {:?}", metrics.avg_latency);
    println!("   Success rate: {:.1}%", metrics.success_rate * 100.0);
    println!("   Throughput: {:.1} ops/sec", metrics.throughput);

    println!("\n3️⃣  Adapting to queue state");

    // Deep queue - should increase batch size
    let deep_queue_size = optimizer.adjust_for_queue_state(5000, 10).await;
    println!(
        "   Deep queue (5000 msgs): batch size = {}",
        deep_queue_size
    );

    // Shallow queue - should decrease batch size
    let shallow_queue_size = optimizer.adjust_for_queue_state(50, 10).await;
    println!(
        "   Shallow queue (50 msgs): batch size = {}",
        shallow_queue_size
    );

    Ok(())
}

/// Demonstrate performance profiling
async fn demonstrate_profiling() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Creating profiler");

    let profiler = AmqpProfiler::new();

    println!("   Recording operations...");

    // Simulate various operations
    for _ in 0..20 {
        let start = Instant::now();
        tokio::time::sleep(Duration::from_millis(10)).await;
        profiler
            .record_operation(OperationType::Publish, start.elapsed(), 1024, true)
            .await;
    }

    for _ in 0..15 {
        let start = Instant::now();
        tokio::time::sleep(Duration::from_millis(5)).await;
        profiler
            .record_operation(OperationType::Consume, start.elapsed(), 512, true)
            .await;
    }

    for _ in 0..10 {
        profiler
            .record_operation_simple(OperationType::Acknowledge, Duration::from_millis(2), true)
            .await;
    }

    println!("   ✓ Recorded {} operations", profiler.record_count().await);

    println!("\n2️⃣  Generating performance report");

    let report = profiler.generate_report().await;

    println!("\n📊 Performance Report:");
    println!("   Total operations: {}", report.total_operations);
    println!("   Avg latency: {:?}", report.avg_latency);
    println!("   p95 latency: {:?}", report.p95_latency);
    println!("   Throughput: {:.1} ops/sec", report.overall_throughput);

    if let Some(slowest) = report.slowest_operation {
        println!("   Slowest operation: {}", slowest.name());
    }

    if let Some(fastest) = report.fastest_operation {
        println!("   Fastest operation: {}", fastest.name());
    }

    println!("\n   Operation Breakdown:");
    for (op_type, stats) in report.operation_stats.iter() {
        println!(
            "   - {}: {} ops, avg {:?}, p95 {:?}",
            op_type.name(),
            stats.total_operations,
            stats.avg_latency,
            stats.p95_latency
        );
    }

    println!("\n💡 Recommendations:");
    for (i, rec) in report.recommendations.iter().enumerate() {
        println!("   {}. {}", i + 1, rec);
    }

    Ok(())
}
