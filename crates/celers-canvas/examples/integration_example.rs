//! Comprehensive integration example: E-commerce Order Processing
//!
//! This example demonstrates a complete order processing workflow that combines:
//! - Chain: Sequential steps in order fulfillment
//! - Group: Parallel processing for inventory and payment
//! - Chord: Parallel validation with aggregation
//! - Error handling: Compensation workflows for rollback
//! - State management: Checkpointing and recovery
//! - Monitoring: Metrics collection and rate limiting

use celers_canvas::{
    Chain, Chord, CompensationWorkflow, ErrorPropagationMode, Group, PartialFailureTracker, Saga,
    SagaIsolation, Signature, WorkflowCheckpoint, WorkflowConcurrencyControl,
    WorkflowMetricsCollector, WorkflowRateLimiter, WorkflowRecoveryPolicy, WorkflowState,
    WorkflowStatus,
};
use serde_json::json;
use uuid::Uuid;

fn main() {
    println!("=== E-Commerce Order Processing Integration Example ===\n");

    // Simulate an order ID
    let order_id = Uuid::new_v4();
    println!("Processing Order: {}\n", order_id);

    // STEP 1: Order Validation Chain
    println!("1. Order Validation Pipeline:");
    let validation_chain = Chain::new()
        .then(
            "validate_customer",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .then(
            "validate_items",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .then(
            "validate_address",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .then(
            "calculate_totals",
            vec![json!({"order_id": order_id.to_string()})],
        );
    println!("   Validation steps: {}", validation_chain.len());
    println!("   Pipeline: {}\n", validation_chain);

    // STEP 2: Parallel Inventory and Payment Checks
    println!("2. Parallel Inventory & Payment Checks:");
    let parallel_checks = Group::new()
        .add(
            "check_inventory",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .add(
            "authorize_payment",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .add(
            "check_fraud",
            vec![json!({"order_id": order_id.to_string()})],
        );
    println!("   Parallel tasks: {}", parallel_checks.len());
    println!("   Group: {}\n", parallel_checks);

    // STEP 3: Multi-Warehouse Fulfillment with Chord
    println!("3. Multi-Warehouse Fulfillment (Map-Reduce):");
    let warehouse_tasks = Group::new()
        .add(
            "check_warehouse_east",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .add(
            "check_warehouse_west",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .add(
            "check_warehouse_central",
            vec![json!({"order_id": order_id.to_string()})],
        );
    let fulfillment_chord = Chord::new(
        warehouse_tasks,
        Signature::new("select_best_warehouse".to_string())
            .with_args(vec![json!({"order_id": order_id.to_string()})]),
    );
    println!("   Warehouses checked: 3");
    println!("   Chord: {}\n", fulfillment_chord);

    // STEP 4: Saga Pattern for Transactional Consistency
    println!("4. Saga Pattern for Order Processing:");
    let order_saga = CompensationWorkflow::new()
        .step(
            Signature::new("reserve_inventory".to_string())
                .with_args(vec![json!({"order_id": order_id.to_string()})]),
            Signature::new("release_inventory".to_string())
                .with_args(vec![json!({"order_id": order_id.to_string()})]),
        )
        .step(
            Signature::new("charge_payment".to_string())
                .with_args(vec![json!({"order_id": order_id.to_string()})]),
            Signature::new("refund_payment".to_string())
                .with_args(vec![json!({"order_id": order_id.to_string()})]),
        )
        .step(
            Signature::new("create_shipment".to_string())
                .with_args(vec![json!({"order_id": order_id.to_string()})]),
            Signature::new("cancel_shipment".to_string())
                .with_args(vec![json!({"order_id": order_id.to_string()})]),
        );
    let saga = Saga::new(order_saga).with_isolation(SagaIsolation::Serializable);
    println!("   Transaction steps: {}", saga.workflow.forward.len());
    println!(
        "   Compensation steps: {}",
        saga.workflow.compensations.len()
    );
    println!("   Isolation: {:?}\n", saga.isolation);

    // STEP 5: Workflow State Management
    println!("5. Workflow State Tracking:");
    let mut workflow_state = WorkflowState::new(order_id, 10);
    workflow_state.status = WorkflowStatus::Running;
    workflow_state.current_stage = Some("processing".to_string());

    // Simulate progress
    for i in 0..7 {
        workflow_state.mark_completed();
        if i == 3 {
            println!(
                "   Progress: {:.1}% - Inventory reserved",
                workflow_state.progress()
            );
        }
    }
    println!("   Final progress: {:.1}%", workflow_state.progress());
    println!("   Status: {:?}\n", workflow_state.status);

    // STEP 6: Checkpointing for Recovery
    println!("6. Workflow Checkpointing:");
    let checkpoint = WorkflowCheckpoint::new(order_id, workflow_state.clone());
    println!("   Checkpoint created at: {}", checkpoint.timestamp);
    println!("   Can recover from this point if failure occurs\n");

    let _recovery_policy = WorkflowRecoveryPolicy::auto_recover().with_max_checkpoint_age(3600);
    println!("   Recovery policy: Auto-recover enabled");
    println!("   Max checkpoint age: 3600 seconds\n");

    // STEP 7: Error Tracking with Partial Failures
    println!("7. Partial Failure Handling:");
    let mut failure_tracker = PartialFailureTracker::new(10);

    // Simulate some failures
    failure_tracker.record_failure(Uuid::new_v4(), "Warehouse timeout".to_string());
    failure_tracker.record_success(Uuid::new_v4());
    failure_tracker.record_success(Uuid::new_v4());

    let error_mode = ErrorPropagationMode::PartialFailure {
        max_failures: 3,
        max_failure_rate: Some(0.3),
    };

    println!("   Failed tasks: {}", failure_tracker.failed_tasks);
    println!("   Successful tasks: {}", failure_tracker.successful_tasks);
    println!(
        "   Failure rate: {:.1}%",
        failure_tracker.failure_rate() * 100.0
    );
    println!(
        "   Exceeds threshold: {}\n",
        failure_tracker.exceeds_threshold(&error_mode)
    );

    // STEP 8: Metrics Collection
    println!("8. Performance Metrics:");
    let mut metrics = WorkflowMetricsCollector::new(order_id);

    // Simulate task execution
    for i in 0..10 {
        let task_id = Uuid::new_v4();
        metrics.record_task_start(task_id);
        metrics.record_task_complete(task_id, 50 + (i * 20));
    }

    metrics.finalize();
    println!("   {}\n", metrics.summary());

    // STEP 9: Rate Limiting
    println!("9. Order Rate Limiting:");
    let mut rate_limiter = WorkflowRateLimiter::new(100, 60000); // 100 orders per minute
    println!("   Max orders: {} per minute", rate_limiter.max_workflows);
    println!(
        "   Current order allowed: {}\n",
        rate_limiter.allow_workflow()
    );

    // STEP 10: Concurrency Control
    println!("10. Concurrent Order Processing:");
    let mut concurrency = WorkflowConcurrencyControl::new(50); // Max 50 concurrent orders

    let order1 = Uuid::new_v4();
    let order2 = Uuid::new_v4();
    concurrency.try_start(order1);
    concurrency.try_start(order2);

    println!("   Max concurrent orders: {}", concurrency.max_concurrent);
    println!(
        "   Currently processing: {}",
        concurrency.current_concurrency()
    );
    println!("   Available slots: {}\n", concurrency.available_slots());

    // STEP 11: Complete Order Processing Chain
    println!("11. Complete Order Processing Workflow:");
    let complete_workflow = Chain::new()
        .then(
            "receive_order",
            vec![json!({"order_id": order_id.to_string()})],
        )
        .then("validate_order", vec![])
        .then("check_inventory_and_payment", vec![])
        .then("reserve_resources", vec![])
        .then("process_payment", vec![])
        .then("create_shipment", vec![])
        .then("send_confirmation", vec![])
        .then("update_analytics", vec![]);

    println!("   Total workflow steps: {}", complete_workflow.len());
    println!("   Workflow: {}\n", complete_workflow);

    // Summary
    println!("=== Order Processing Summary ===");
    println!("Order ID: {}", order_id);
    println!("Validation: ✓ Complete");
    println!("Inventory Check: ✓ Complete");
    println!("Payment Authorization: ✓ Complete");
    println!("Warehouse Selection: ✓ Complete");
    println!("Saga Transaction: ✓ Complete");
    println!("State Checkpoint: ✓ Saved");
    println!("Metrics: ✓ Collected");
    println!("Rate Limit: ✓ Within limits");
    println!("Concurrency: ✓ Controlled");
    println!("\nOrder processing workflow configured successfully!");
    println!("\nKey Features Demonstrated:");
    println!("  • Sequential validation pipeline (Chain)");
    println!("  • Parallel resource checks (Group)");
    println!("  • Multi-warehouse optimization (Chord)");
    println!("  • Transactional consistency (Saga)");
    println!("  • State persistence (Checkpointing)");
    println!("  • Error recovery (Partial failures)");
    println!("  • Performance tracking (Metrics)");
    println!("  • Load management (Rate limiting & Concurrency)");
}
