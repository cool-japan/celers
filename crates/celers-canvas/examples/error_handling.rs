//! Error handling examples: compensation workflows, saga pattern, error propagation

use celers_canvas::{
    CompensationWorkflow, ErrorPropagationMode, PartialFailureTracker, Saga, SagaIsolation,
    Signature, WorkflowErrorHandler,
};

fn main() {
    println!("=== Error Handling Examples ===\n");

    // Example 1: Workflow Error Handler
    println!("1. Workflow Error Handler:");
    let error_handler = WorkflowErrorHandler::new(Signature::new("handle_error".to_string()));
    println!("   Handler task: {}", error_handler.handler.task);
    println!("   Suppress error: {}", error_handler.suppress);
    println!();

    // Example 2: Error Handler for Specific Errors
    println!("2. Error Handler for Specific Errors:");
    let specific_handler =
        WorkflowErrorHandler::new(Signature::new("handle_network_error".to_string()))
            .for_errors(vec!["NetworkError".to_string(), "TimeoutError".to_string()])
            .suppress_error();
    println!("   Handler task: {}", specific_handler.handler.task);
    println!("   Error types: {:?}", specific_handler.error_types);
    println!("   Suppress: {}", specific_handler.suppress);
    println!();

    // Example 3: Compensation Workflow
    println!("3. Compensation Workflow:");
    let compensation = CompensationWorkflow::new().step(
        Signature::new("charge_payment".to_string()),
        Signature::new("refund_payment".to_string()),
    );
    println!("   Forward steps: {}", compensation.forward.len());
    println!(
        "   Compensation steps: {}",
        compensation.compensations.len()
    );
    println!();

    // Example 4: Saga Pattern
    println!("4. Saga Pattern (Distributed Transaction):");
    let compensation_workflow = CompensationWorkflow::new()
        .step(
            Signature::new("reserve_inventory".to_string()),
            Signature::new("release_inventory".to_string()),
        )
        .step(
            Signature::new("charge_customer".to_string()),
            Signature::new("refund_customer".to_string()),
        )
        .step(
            Signature::new("notify_shipping".to_string()),
            Signature::new("cancel_shipping".to_string()),
        );
    let saga = Saga::new(compensation_workflow).with_isolation(SagaIsolation::Serializable);
    println!("   Forward steps: {}", saga.workflow.forward.len());
    println!(
        "   Compensation steps: {}",
        saga.workflow.compensations.len()
    );
    println!("   Isolation: {:?}", saga.isolation);
    println!();

    // Example 5: Error Propagation Modes
    println!("5. Error Propagation Modes:");
    println!("   - StopOnFirstError: Immediately stop on first failure");
    println!("   - ContinueOnError: Continue executing remaining tasks");
    println!("   - PartialFailure: Track failures but continue");
    let _stop_mode = ErrorPropagationMode::StopOnFirstError;
    let _continue_mode = ErrorPropagationMode::ContinueOnError;
    let partial_mode = ErrorPropagationMode::PartialFailure {
        max_failures: 3,
        max_failure_rate: Some(0.25),
    };
    println!("   Partial failure mode configured:");
    if let ErrorPropagationMode::PartialFailure {
        max_failures,
        max_failure_rate,
    } = partial_mode
    {
        println!("     Max failures: {}", max_failures);
        if let Some(rate) = max_failure_rate {
            println!("     Max failure rate: {}%", rate * 100.0);
        }
    }
    println!();

    // Example 6: Partial Failure Tracker
    println!("6. Partial Failure Tracker:");
    let mut tracker = PartialFailureTracker::new(10);
    println!("   Total tasks: {}", tracker.total_tasks);
    println!("   Recording some failures...");
    use uuid::Uuid;
    tracker.record_failure(Uuid::new_v4(), "Error 1".to_string());
    tracker.record_failure(Uuid::new_v4(), "Error 2".to_string());
    println!("   Failed tasks: {}", tracker.failed_tasks);
    println!("   Failure rate: {:.2}%", tracker.failure_rate() * 100.0);
    println!(
        "   Exceeds threshold (StopOnFirstError): {}",
        tracker.exceeds_threshold(&ErrorPropagationMode::StopOnFirstError)
    );
    println!();

    // Example 7: Complex Error Handling Workflow
    println!("7. Complex Error Handling Strategy:");
    println!("   Step 1: Try primary database");
    println!("   Step 2: On error, try replica database (fallback)");
    println!("   Step 3: On error, try cache (second fallback)");
    println!("   Step 4: On error, return default value (final fallback)");
    println!();

    println!("=== All error handling patterns demonstrated ===");
}
