// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Distributed Tracing Example
//!
//! Demonstrates how to use the distributed tracing utilities to track
//! message flows across multiple services and queues.
//!
//! This example shows:
//! - Correlation ID generation and propagation
//! - AWS X-Ray trace context integration
//! - Parent-child span relationships
//! - Message flow tracking
//! - End-to-end request tracing

use celers_broker_sqs::tracing_util::{generate_correlation_id, MessageFlowTracker, TraceContext};

fn main() {
    println!("=== Distributed Tracing Example ===\n");

    // Example 1: Basic Correlation ID
    example_basic_correlation_id();

    // Example 2: AWS X-Ray Integration
    example_xray_integration();

    // Example 3: Parent-Child Span Relationships
    example_span_relationships();

    // Example 4: Message Flow Tracking
    example_message_flow_tracking();

    // Example 5: Multi-Service Request Tracing
    example_multi_service_tracing();
}

fn example_basic_correlation_id() {
    println!("--- Example 1: Basic Correlation ID ---");

    // Generate a correlation ID for a new request
    let correlation_id = generate_correlation_id();
    println!("Generated correlation ID: {}", correlation_id);

    // Create a trace context with basic information
    let trace_ctx = TraceContext::new()
        .with_correlation_id(&correlation_id)
        .with_operation("process_order")
        .with_service_name("order-service")
        .with_metadata("user_id", "12345")
        .with_metadata("tenant_id", "acme-corp");

    println!("Trace context created:");
    println!("  Correlation ID: {}", trace_ctx.correlation_id);
    println!("  Operation: {:?}", trace_ctx.operation);
    println!("  Service: {:?}", trace_ctx.service_name);
    println!("  Metadata: {:?}", trace_ctx.metadata);

    // Serialize for SQS message attributes
    let header = trace_ctx.to_header();
    println!("  Serialized header length: {} bytes", header.len());

    // Deserialize from header
    let restored = TraceContext::from_header(&header).unwrap();
    println!(
        "  Successfully restored from header: {}",
        restored.correlation_id
    );

    println!();
}

fn example_xray_integration() {
    println!("--- Example 2: AWS X-Ray Integration ---");

    // Generate X-Ray trace ID
    // Format: 1-{epoch_time}-{24_hex_chars}
    let epoch_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let xray_trace_id = format!("1-{:x}-{:024x}", epoch_time, 0x123456789abcdefu64);

    // Create trace context with X-Ray trace ID
    let trace_ctx = TraceContext::new()
        .with_xray_trace_id(&xray_trace_id)
        .with_span_id("abc123def456")
        .with_operation("send_email")
        .with_service_name("notification-service");

    println!("X-Ray trace context:");
    println!("  Trace ID: {:?}", trace_ctx.trace_id);
    println!("  Span ID: {:?}", trace_ctx.span_id);
    println!("  Operation: {:?}", trace_ctx.operation);

    // This trace context can be passed to downstream services
    let header = trace_ctx.to_header();
    println!(
        "  Ready to propagate to downstream service (header: {} bytes)",
        header.len()
    );

    println!();
}

fn example_span_relationships() {
    println!("--- Example 3: Parent-Child Span Relationships ---");

    // Create parent span
    let parent_span = TraceContext::new()
        .with_span_id("parent-span-001")
        .with_operation("process_payment")
        .with_service_name("payment-service")
        .with_metadata("order_id", "ORD-12345")
        .with_metadata("amount", "99.99");

    println!("Parent span:");
    println!("  Span ID: {:?}", parent_span.span_id);
    println!("  Operation: {:?}", parent_span.operation);

    // Create child span for validation
    let validation_span = parent_span.create_child_span("validate_payment");
    println!("\nChild span (validation):");
    println!("  Span ID: {:?}", validation_span.span_id);
    println!("  Parent Span ID: {:?}", validation_span.parent_span_id);
    println!("  Operation: {:?}", validation_span.operation);
    println!(
        "  Correlation ID: {} (inherited)",
        validation_span.correlation_id
    );

    // Create another child span for processing
    let processing_span = parent_span.create_child_span("charge_payment");
    println!("\nChild span (processing):");
    println!("  Span ID: {:?}", processing_span.span_id);
    println!("  Parent Span ID: {:?}", processing_span.parent_span_id);
    println!("  Operation: {:?}", processing_span.operation);

    // Create grandchild span
    let grandchild_span = processing_span.create_child_span("verify_3ds");
    println!("\nGrandchild span (3DS verification):");
    println!("  Span ID: {:?}", grandchild_span.span_id);
    println!("  Parent Span ID: {:?}", grandchild_span.parent_span_id);
    println!("  Operation: {:?}", grandchild_span.operation);

    println!();
}

fn example_message_flow_tracking() {
    println!("--- Example 4: Message Flow Tracking ---");

    let correlation_id = generate_correlation_id();

    // Track message flow across multiple queues
    let mut flow_tracker = MessageFlowTracker::new(&correlation_id, "orders-queue", "publish");

    println!("Message flow started:");
    println!("  Correlation ID: {}", flow_tracker.correlation_id);
    println!("  Initial queue: orders-queue");

    // Simulate processing delays
    std::thread::sleep(std::time::Duration::from_millis(50));
    flow_tracker.record_transition("payment-queue", "consume_and_process");

    std::thread::sleep(std::time::Duration::from_millis(30));
    flow_tracker.record_transition("notification-queue", "publish");

    std::thread::sleep(std::time::Duration::from_millis(20));
    flow_tracker.record_transition("notification-queue", "consume");

    // Print flow summary
    println!("\n{}", flow_tracker.summary());
    println!("\nDetailed flow:");
    for (i, (queue, operation)) in flow_tracker
        .queue_flow
        .iter()
        .zip(flow_tracker.operations.iter())
        .enumerate()
    {
        if i > 0 {
            if let Some(duration) = flow_tracker.transition_duration_ms(i - 1, i) {
                println!(
                    "  {} → {} (+{}ms) - {}",
                    flow_tracker.queue_flow[i - 1],
                    queue,
                    duration,
                    operation
                );
            }
        } else {
            println!("  {} - {}", queue, operation);
        }
    }

    println!();
}

fn example_multi_service_tracing() {
    println!("--- Example 5: Multi-Service Request Tracing ---");

    // Simulate a request flowing through multiple services
    let correlation_id = generate_correlation_id();
    println!("Request ID: {}\n", correlation_id);

    // Service 1: API Gateway
    let api_ctx = TraceContext::new()
        .with_correlation_id(&correlation_id)
        .with_span_id("span-001")
        .with_operation("api_request")
        .with_service_name("api-gateway")
        .with_metadata("endpoint", "/orders")
        .with_metadata("method", "POST");

    println!("1. API Gateway ({}ms elapsed)", api_ctx.elapsed_ms());
    println!("   Span: {:?}", api_ctx.span_id);
    println!("   Operation: {:?}", api_ctx.operation);

    std::thread::sleep(std::time::Duration::from_millis(10));

    // Service 2: Order Service
    let order_ctx = api_ctx.create_child_span("create_order");
    println!("\n2. Order Service ({}ms elapsed)", order_ctx.elapsed_ms());
    println!("   Span: {:?}", order_ctx.span_id);
    println!("   Parent Span: {:?}", order_ctx.parent_span_id);
    println!("   Operation: {:?}", order_ctx.operation);

    std::thread::sleep(std::time::Duration::from_millis(25));

    // Service 3: Payment Service
    let payment_ctx = order_ctx.create_child_span("process_payment");
    println!(
        "\n3. Payment Service ({}ms elapsed)",
        payment_ctx.elapsed_ms()
    );
    println!("   Span: {:?}", payment_ctx.span_id);
    println!("   Parent Span: {:?}", payment_ctx.parent_span_id);
    println!("   Operation: {:?}", payment_ctx.operation);

    std::thread::sleep(std::time::Duration::from_millis(40));

    // Service 4: Inventory Service
    let inventory_ctx = order_ctx.create_child_span("reserve_inventory");
    println!(
        "\n4. Inventory Service ({}ms elapsed)",
        inventory_ctx.elapsed_ms()
    );
    println!("   Span: {:?}", inventory_ctx.span_id);
    println!("   Parent Span: {:?}", inventory_ctx.parent_span_id);
    println!("   Operation: {:?}", inventory_ctx.operation);

    std::thread::sleep(std::time::Duration::from_millis(15));

    // Service 5: Notification Service
    let notification_ctx = order_ctx.create_child_span("send_confirmation");
    println!(
        "\n5. Notification Service ({}ms elapsed)",
        notification_ctx.elapsed_ms()
    );
    println!("   Span: {:?}", notification_ctx.span_id);
    println!("   Parent Span: {:?}", notification_ctx.parent_span_id);
    println!("   Operation: {:?}", notification_ctx.operation);

    println!("\nRequest completed!");
    println!("Total elapsed time: {}ms", api_ctx.elapsed_ms());
    println!("Correlation ID: {}", correlation_id);

    // Show how to use metadata for analysis
    println!("\n--- Trace Analysis ---");
    println!("Services involved: 5");
    println!("Total spans: 5");
    println!("Span hierarchy:");
    println!("  api_request (api-gateway)");
    println!("  └─ create_order (order-service)");
    println!("     ├─ process_payment (payment-service)");
    println!("     ├─ reserve_inventory (inventory-service)");
    println!("     └─ send_confirmation (notification-service)");

    println!();
}
