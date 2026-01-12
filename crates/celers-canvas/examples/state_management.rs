//! State management examples: tracking, checkpointing, recovery

use celers_canvas::{WorkflowCheckpoint, WorkflowRecoveryPolicy, WorkflowState, WorkflowStatus};
use uuid::Uuid;

fn main() {
    println!("=== State Management Examples ===\n");

    // Example 1: Workflow State
    println!("1. Workflow State Tracking:");
    let workflow_id = Uuid::new_v4();
    let mut state = WorkflowState::new(workflow_id, 5);
    println!("   Workflow ID: {}", state.workflow_id);
    println!("   Status: {:?}", state.status);
    println!("   Total tasks: {}", state.total_tasks);
    println!("   Completed: {}", state.completed_tasks);
    println!();

    // Example 2: Updating State
    println!("2. Updating Workflow State:");
    state.mark_completed();
    state.mark_completed();
    println!("   Progress: {:.2}%", state.progress());
    println!(
        "   Completed: {}/{}",
        state.completed_tasks, state.total_tasks
    );
    println!();

    // Example 3: Workflow Checkpointing
    println!("3. Workflow Checkpointing:");
    let checkpoint_state = WorkflowState::new(workflow_id, 10);
    let checkpoint = WorkflowCheckpoint::new(workflow_id, checkpoint_state);
    println!("   Timestamp: {}", checkpoint.timestamp);
    println!("   Workflow ID: {}", checkpoint.workflow_id);
    println!("   Version: {}", checkpoint.version);
    println!();

    // Example 4: Recovery Policy
    println!("4. Workflow Recovery Policy:");
    let auto_recovery = WorkflowRecoveryPolicy::auto_recover().with_max_checkpoint_age(3600);
    println!("   Auto recovery: {}", auto_recovery.auto_recovery);
    println!(
        "   Resume from checkpoint: {}",
        auto_recovery.resume_from_checkpoint
    );
    println!("   Replay failed: {}", auto_recovery.replay_failed);
    println!();

    let manual_recovery = WorkflowRecoveryPolicy::manual();
    println!(
        "   Manual policy - Auto recovery: {}",
        manual_recovery.auto_recovery
    );
    println!();

    // Example 5: Recovery Strategies
    println!("5. Recovery Strategies:");
    println!("   Auto-recovery policy:");
    println!("     - Automatically resumes from checkpoint");
    println!("     - Replays failed tasks");
    println!("   Manual policy:");
    println!("     - Requires explicit recovery initiation");
    println!("     - Gives more control over recovery process");
    println!();

    // Example 6: Workflow Status Transitions
    println!("6. Workflow Status Transitions:");
    let mut workflow = WorkflowState::new(Uuid::new_v4(), 10);
    println!("   Initial: {:?}", workflow.status);

    workflow.status = WorkflowStatus::Running;
    println!("   After start: {:?}", workflow.status);

    workflow.mark_completed();
    workflow.mark_completed();
    workflow.mark_completed();
    println!(
        "   In progress: {:?} ({:.2}% complete)",
        workflow.status,
        workflow.progress()
    );

    // Simulate completion
    for _ in 0..7 {
        workflow.mark_completed();
    }
    if workflow.completed_tasks == workflow.total_tasks {
        workflow.status = WorkflowStatus::Success;
    }
    println!("   Final: {:?}", workflow.status);
    println!();

    // Example 7: Checkpoint with Rich Metadata
    println!("7. Checkpoint with Rich Metadata:");
    let mut detailed_state = WorkflowState::new(workflow_id, 5000);
    detailed_state.completed_tasks = 1500;
    detailed_state.current_stage = Some("data_processing".to_string());
    detailed_state.set_result("sum".to_string(), serde_json::json!(45000));
    detailed_state.set_result("count".to_string(), serde_json::json!(1500));
    detailed_state.set_result("average".to_string(), serde_json::json!(30.0));

    let detailed_checkpoint = WorkflowCheckpoint::new(workflow_id, detailed_state);
    println!("   Checkpoint contains:");
    println!(
        "   - Current stage: {:?}",
        detailed_checkpoint.state.current_stage
    );
    println!(
        "   - Progress: {:.2}%",
        detailed_checkpoint.state.progress()
    );
    println!(
        "   - Intermediate results: {}",
        detailed_checkpoint.state.intermediate_results.len()
    );
    println!(
        "   - Completed tasks: {}/{}",
        detailed_checkpoint.state.completed_tasks, detailed_checkpoint.state.total_tasks
    );
    println!();

    println!("=== State management patterns demonstrated ===");
}
