//! Distributed Tracing Example
//!
//! Demonstrates W3C Trace Context propagation for distributed tracing
//! with OpenTelemetry, Jaeger, and Zipkin compatibility.
//!
//! This example shows:
//! - Creating trace contexts from HTTP headers
//! - Enqueueing tasks with trace context
//! - Extracting trace context from tasks
//! - Propagating trace to child tasks
//! - Creating child spans for nested operations
//!
//! Run with:
//! ```bash
//! cargo run --example distributed_tracing
//! ```

use celers_broker_sql::{MysqlBroker, TraceContext};
use celers_core::SerializedTask;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    println!("=== Distributed Tracing Example ===\n");

    // Connect to MySQL
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers".to_string());

    let broker = MysqlBroker::new(&database_url).await?;
    broker.migrate().await?;

    println!("✓ Connected to MySQL and ran migrations\n");

    // Demo 1: Create trace context from HTTP header (W3C traceparent)
    demo_traceparent_parsing()?;

    // Demo 2: Enqueue task with trace context
    demo_enqueue_with_trace(&broker).await?;

    // Demo 3: Extract trace context and create child span
    demo_child_span_propagation(&broker).await?;

    // Demo 4: Multi-level task chain with trace propagation
    demo_task_chain_tracing(&broker).await?;

    // Demo 5: Sampling decision
    demo_sampling()?;

    println!("\n=== All Demos Completed Successfully ===");
    Ok(())
}

/// Demo 1: Parse W3C traceparent header from HTTP request
fn demo_traceparent_parsing() -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 1: Parsing W3C Traceparent Header");
    println!("---------------------------------------");

    // Simulate receiving a traceparent header from incoming HTTP request
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    println!("Incoming traceparent: {}", traceparent);

    let trace_ctx = TraceContext::from_traceparent(traceparent)?;
    println!("Parsed trace context:");
    println!("  Trace ID: {}", trace_ctx.trace_id);
    println!("  Span ID:  {}", trace_ctx.span_id);
    println!(
        "  Flags:    {} (sampled: {})",
        trace_ctx.trace_flags,
        trace_ctx.is_sampled()
    );

    // Convert back to traceparent
    let regenerated = trace_ctx.to_traceparent();
    println!("  Regenerated: {}", regenerated);
    assert_eq!(traceparent, regenerated);

    println!("✓ Successfully parsed and regenerated traceparent\n");
    Ok(())
}

/// Demo 2: Enqueue task with distributed trace context
async fn demo_enqueue_with_trace(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 2: Enqueue Task with Trace Context");
    println!("----------------------------------------");

    // Create a trace context (typically from incoming request)
    let trace_ctx = TraceContext::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    println!("Created trace context:");
    println!("  Trace ID: {}", trace_ctx.trace_id);
    println!("  Span ID:  {}", trace_ctx.span_id);

    // Enqueue task with trace context
    let task = SerializedTask::new("process_payment".to_string(), b"payment_data".to_vec());
    let task_id = broker
        .enqueue_with_trace_context(task, trace_ctx.clone())
        .await?;
    println!("✓ Enqueued task with trace: {}", task_id);

    // Extract trace context back
    if let Some(extracted) = broker.extract_trace_context(&task_id).await? {
        println!("✓ Extracted trace context from task:");
        println!("  Trace ID: {}", extracted.trace_id);
        println!("  Span ID:  {}", extracted.span_id);
        assert_eq!(extracted.trace_id, trace_ctx.trace_id);
    }

    println!();
    Ok(())
}

/// Demo 3: Create child span for nested operation
async fn demo_child_span_propagation(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 3: Child Span Propagation");
    println!("-------------------------------");

    // Parent task with trace
    let parent_trace = TraceContext::new("a1b2c3d4e5f6789012345678abcdef01", "1234567890abcdef");
    println!("Parent trace:");
    println!("  Trace ID: {}", parent_trace.trace_id);
    println!("  Span ID:  {}", parent_trace.span_id);

    let parent_task = SerializedTask::new("parent_task".to_string(), vec![1, 2, 3]);
    let parent_id = broker
        .enqueue_with_trace_context(parent_task, parent_trace.clone())
        .await?;
    println!("✓ Enqueued parent task: {}", parent_id);

    // Create child span
    let child_trace = parent_trace.create_child_span();
    println!("\nChild span created:");
    println!("  Trace ID: {} (same as parent)", child_trace.trace_id);
    println!(
        "  Span ID:  {} (different from parent)",
        child_trace.span_id
    );

    // Enqueue child task with child span
    let child_task = SerializedTask::new("child_task".to_string(), vec![4, 5, 6]);
    let child_id = broker
        .enqueue_with_trace_context(child_task, child_trace.clone())
        .await?;
    println!("✓ Enqueued child task: {}", child_id);

    // Verify trace IDs match but span IDs differ
    assert_eq!(parent_trace.trace_id, child_trace.trace_id);
    assert_ne!(parent_trace.span_id, child_trace.span_id);
    println!("✓ Verified parent-child relationship\n");

    Ok(())
}

/// Demo 4: Multi-level task chain with automatic trace propagation
async fn demo_task_chain_tracing(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 4: Task Chain with Trace Propagation");
    println!("------------------------------------------");

    // Create root trace (from incoming API request)
    let root_trace = TraceContext::new("fedcba9876543210fedcba9876543210", "fedcba9876543210");
    println!("Root trace (from API request):");
    println!("  Trace ID: {}", root_trace.trace_id);
    println!("  Span ID:  {}", root_trace.span_id);

    // Level 1: Enqueue root task
    let root_task = SerializedTask::new("api_handler".to_string(), b"request_data".to_vec());
    let root_id = broker
        .enqueue_with_trace_context(root_task, root_trace.clone())
        .await?;
    println!("✓ Level 1 task enqueued: {}", root_id);

    // Level 2: Enqueue child task using automatic trace propagation
    let level2_task = SerializedTask::new("data_processor".to_string(), b"data".to_vec());
    let level2_id = broker
        .enqueue_with_parent_trace(&root_id, level2_task)
        .await?;
    println!("✓ Level 2 task enqueued: {}", level2_id);

    // Level 3: Enqueue grandchild task
    let level3_task =
        SerializedTask::new("send_notification".to_string(), b"notification".to_vec());
    let level3_id = broker
        .enqueue_with_parent_trace(&level2_id, level3_task)
        .await?;
    println!("✓ Level 3 task enqueued: {}", level3_id);

    // Verify all tasks share the same trace ID
    let root_ctx = broker.extract_trace_context(&root_id).await?.unwrap();
    let level2_ctx = broker.extract_trace_context(&level2_id).await?.unwrap();
    let level3_ctx = broker.extract_trace_context(&level3_id).await?.unwrap();

    println!("\nTrace chain verification:");
    println!(
        "  Root:    Trace={}, Span={}",
        root_ctx.trace_id, root_ctx.span_id
    );
    println!(
        "  Level 2: Trace={}, Span={}",
        level2_ctx.trace_id, level2_ctx.span_id
    );
    println!(
        "  Level 3: Trace={}, Span={}",
        level3_ctx.trace_id, level3_ctx.span_id
    );

    // All should share the same trace ID
    assert_eq!(root_ctx.trace_id, level2_ctx.trace_id);
    assert_eq!(root_ctx.trace_id, level3_ctx.trace_id);

    // But have different span IDs
    assert_ne!(root_ctx.span_id, level2_ctx.span_id);
    assert_ne!(root_ctx.span_id, level3_ctx.span_id);
    assert_ne!(level2_ctx.span_id, level3_ctx.span_id);

    println!("✓ All tasks share trace ID with unique span IDs\n");
    Ok(())
}

/// Demo 5: Trace sampling decision
fn demo_sampling() -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 5: Trace Sampling");
    println!("----------------------");

    // Sampled trace (flags = "01")
    let sampled = TraceContext::new("trace1", "span1");
    println!("Sampled trace:");
    println!("  Flags: {}", sampled.trace_flags);
    println!("  Is sampled: {}", sampled.is_sampled());
    assert!(sampled.is_sampled());

    // You could create an unsampled trace by manually setting flags
    // In production, sampling decisions are typically made by the tracing system
    println!("✓ Sampling decision verified\n");

    Ok(())
}

/// Example: Simulating real-world microservices scenario
#[allow(dead_code)]
async fn microservices_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Microservices Trace Propagation Scenario ===\n");

    let broker = MysqlBroker::new("mysql://localhost/celers").await?;

    // Scenario: E-commerce checkout flow
    // API Gateway -> Order Service -> Payment Service -> Notification Service

    // 1. API Gateway receives request with traceparent header
    let incoming_traceparent = "00-a1b2c3d4e5f6789012345678abcdef01-1111111111111111-01";
    let api_trace = TraceContext::from_traceparent(incoming_traceparent)?;

    println!("1. API Gateway receives request");
    println!("   Trace ID: {}", api_trace.trace_id);

    // 2. Order Service task
    let order_task = SerializedTask::new("create_order".to_string(), b"order_data".to_vec());
    let order_id = broker
        .enqueue_with_trace_context(order_task, api_trace.clone())
        .await?;
    println!("2. Order task enqueued: {}", order_id);

    // 3. Payment Service task (child of order)
    let payment_task = SerializedTask::new("process_payment".to_string(), b"payment".to_vec());
    let payment_id = broker
        .enqueue_with_parent_trace(&order_id, payment_task)
        .await?;
    println!("3. Payment task enqueued: {}", payment_id);

    // 4. Notification Service task (child of payment)
    let notification_task = SerializedTask::new("send_confirmation".to_string(), b"email".to_vec());
    let notification_id = broker
        .enqueue_with_parent_trace(&payment_id, notification_task)
        .await?;
    println!("4. Notification task enqueued: {}", notification_id);

    println!("\n✓ Complete trace chain created across microservices");
    println!("   All tasks share trace ID: {}", api_trace.trace_id);
    println!("   View in Jaeger/Zipkin to see the complete request flow!");

    Ok(())
}
