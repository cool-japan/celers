//! AsyncResult API Example
//!
//! This example demonstrates how to use the AsyncResult API for:
//! - Querying task results
//! - Checking task state
//! - Waiting for task completion with timeout
//! - Error handling
//!
//! # Running this example
//!
//! The example runs with a mock backend by default, demonstrating
//! the API without requiring external services.
//!
//! For real backends:
//! - Redis: `cargo run --example async_result --features backend-redis`
//! - PostgreSQL: `cargo run --example async_result --features backend-db`
//! - gRPC: `cargo run --example async_result --features backend-rpc`

use async_trait::async_trait;
use celers::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ============================================================================
// Mock Backend for demonstration (no external dependencies)
// ============================================================================

/// In-memory result store for demonstration purposes
#[derive(Clone)]
struct MockResultStore {
    results: Arc<Mutex<HashMap<Uuid, TaskResultValue>>>,
}

impl MockResultStore {
    fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Simulate storing a task result (used for demo setup)
    fn simulate_result(&self, task_id: Uuid, result: TaskResultValue) {
        self.results.lock().unwrap().insert(task_id, result);
    }
}

#[async_trait]
impl ResultStore for MockResultStore {
    async fn store_result(
        &self,
        task_id: celers_core::TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        self.results.lock().unwrap().insert(task_id, result);
        Ok(())
    }

    async fn get_result(
        &self,
        task_id: celers_core::TaskId,
    ) -> celers_core::Result<Option<TaskResultValue>> {
        Ok(self.results.lock().unwrap().get(&task_id).cloned())
    }

    async fn get_state(
        &self,
        task_id: celers_core::TaskId,
    ) -> celers_core::Result<celers_core::TaskState> {
        let result = self.results.lock().unwrap().get(&task_id).cloned();
        Ok(match result {
            Some(TaskResultValue::Pending) => TaskState::Pending,
            Some(TaskResultValue::Received) => TaskState::Reserved,
            Some(TaskResultValue::Started) => TaskState::Running,
            Some(TaskResultValue::Success(v)) => {
                TaskState::Succeeded(serde_json::to_vec(&v).unwrap_or_default())
            }
            Some(TaskResultValue::Failure { error, .. }) => TaskState::Failed(error),
            Some(TaskResultValue::Revoked) => TaskState::Failed("Revoked".to_string()),
            Some(TaskResultValue::Retry { attempt, .. }) => TaskState::Retrying(attempt),
            Some(TaskResultValue::Rejected { reason }) => TaskState::Failed(reason),
            None => TaskState::Pending,
        })
    }

    async fn forget(&self, task_id: celers_core::TaskId) -> celers_core::Result<()> {
        self.results.lock().unwrap().remove(&task_id);
        Ok(())
    }

    async fn has_result(&self, task_id: celers_core::TaskId) -> celers_core::Result<bool> {
        Ok(self.results.lock().unwrap().contains_key(&task_id))
    }
}

// ============================================================================
// Example demonstrations
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== CeleRS AsyncResult API Example ===\n");

    // Create mock backend
    let backend = MockResultStore::new();

    // ========================================================================
    // Example 1: Successful Task
    // ========================================================================
    println!("1. Successful Task Example");
    println!("   -----------------------");

    let task_id = Uuid::new_v4();
    backend.simulate_result(
        task_id,
        TaskResultValue::Success(json!({"sum": 42, "message": "Task completed!"})),
    );

    let result = AsyncResult::new(task_id, backend.clone());

    println!("   Task ID: {}", result.task_id());
    println!("   Ready: {}", result.ready().await?);
    println!("   Successful: {}", result.successful().await?);
    println!("   Failed: {}", result.failed().await?);
    println!("   State: {:?}", result.state().await?);

    // Get the result with timeout
    match result.get(Some(Duration::from_secs(5))).await {
        Ok(Some(value)) => println!("   Result: {}\n", value),
        Ok(None) => println!("   Result: None\n"),
        Err(e) => println!("   Error: {}\n", e),
    }

    // ========================================================================
    // Example 2: Failed Task
    // ========================================================================
    println!("2. Failed Task Example");
    println!("   --------------------");

    let failed_task_id = Uuid::new_v4();
    backend.simulate_result(
        failed_task_id,
        TaskResultValue::Failure {
            error: "Division by zero".to_string(),
            traceback: Some("at line 42 in math.rs".to_string()),
        },
    );

    let failed_result = AsyncResult::new(failed_task_id, backend.clone());

    println!("   Task ID: {}", failed_result.task_id());
    println!("   Ready: {}", failed_result.ready().await?);
    println!("   Successful: {}", failed_result.successful().await?);
    println!("   Failed: {}", failed_result.failed().await?);
    println!("   State: {:?}", failed_result.state().await?);

    // Get traceback
    if let Some(tb) = failed_result.traceback().await? {
        println!("   Traceback: {}", tb);
    }

    // Attempting to get result will return error
    match failed_result.get(Some(Duration::from_secs(1))).await {
        Ok(v) => println!("   Result: {:?}", v),
        Err(e) => println!("   Error (expected): {}\n", e),
    }

    // ========================================================================
    // Example 3: Pending Task
    // ========================================================================
    println!("3. Pending Task Example");
    println!("   ---------------------");

    let pending_task_id = Uuid::new_v4();
    backend.simulate_result(pending_task_id, TaskResultValue::Pending);

    let pending_result = AsyncResult::new(pending_task_id, backend.clone());

    println!("   Task ID: {}", pending_result.task_id());
    println!("   Ready: {}", pending_result.ready().await?);
    println!("   State: {:?}", pending_result.state().await?);

    // Get with short timeout will fail
    match pending_result.get(Some(Duration::from_millis(200))).await {
        Ok(_) => println!("   Got result unexpectedly"),
        Err(e) => println!("   Timeout (expected): {}\n", e),
    }

    // ========================================================================
    // Example 4: Task Revocation
    // ========================================================================
    println!("4. Task Revocation Example");
    println!("   ------------------------");

    let revoke_task_id = Uuid::new_v4();
    backend.simulate_result(
        revoke_task_id,
        TaskResultValue::Success(json!({"value": "will be revoked"})),
    );

    let revoke_result = AsyncResult::new(revoke_task_id, backend.clone());

    println!("   Before revoke - Ready: {}", revoke_result.ready().await?);

    // Revoke the task
    revoke_result.revoke().await?;
    println!("   Task revoked");

    // Check state after revoke
    let info = revoke_result.info().await?;
    println!("   After revoke - Info: {:?}\n", info);

    // ========================================================================
    // Example 5: Forget (Delete) Result
    // ========================================================================
    println!("5. Forget Result Example");
    println!("   ----------------------");

    let forget_task_id = Uuid::new_v4();
    backend.simulate_result(
        forget_task_id,
        TaskResultValue::Success(json!({"data": "temporary"})),
    );

    let forget_result = AsyncResult::new(forget_task_id, backend.clone());

    println!(
        "   Has result before forget: {}",
        backend.has_result(forget_task_id).await?
    );

    // Forget (delete) the result
    forget_result.forget().await?;
    println!("   Result forgotten");

    println!(
        "   Has result after forget: {}\n",
        backend.has_result(forget_task_id).await?
    );

    // ========================================================================
    // Example 6: Retry State
    // ========================================================================
    println!("6. Retry State Example");
    println!("   --------------------");

    let retry_task_id = Uuid::new_v4();
    backend.simulate_result(
        retry_task_id,
        TaskResultValue::Retry {
            attempt: 2,
            max_retries: 5,
        },
    );

    let retry_result = AsyncResult::new(retry_task_id, backend.clone());

    println!("   Task ID: {}", retry_result.task_id());
    println!("   Ready: {}", retry_result.ready().await?);
    println!("   State: {:?}", retry_result.state().await?);
    println!("   Info: {:?}\n", retry_result.info().await?);

    // ========================================================================
    // Example 7: Using wait() convenience method
    // ========================================================================
    println!("7. Wait Convenience Method");
    println!("   ------------------------");

    let wait_task_id = Uuid::new_v4();
    backend.simulate_result(
        wait_task_id,
        TaskResultValue::Success(json!({"computed": 12345})),
    );

    let wait_result = AsyncResult::new(wait_task_id, backend.clone());

    match wait_result.wait(Some(Duration::from_secs(5))).await {
        Ok(value) => println!("   Wait returned: {}\n", value),
        Err(e) => println!("   Wait error: {}\n", e),
    }

    // ========================================================================
    // Example 8: Parent-Child Relationships
    // ========================================================================
    println!("8. Parent-Child Task Relationship");
    println!("   -------------------------------");

    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();

    backend.simulate_result(
        parent_id,
        TaskResultValue::Success(json!({"step": "parent completed"})),
    );
    backend.simulate_result(
        child_id,
        TaskResultValue::Success(json!({"step": "child completed"})),
    );

    let parent_result = AsyncResult::new(parent_id, backend.clone());
    let child_result = AsyncResult::with_parent(child_id, backend.clone(), parent_result.clone());

    println!("   Child task ID: {}", child_result.task_id());
    println!("   Has parent: {}", child_result.parent().is_some());
    if let Some(parent) = child_result.parent() {
        println!("   Parent task ID: {}", parent.task_id());
        println!("   Parent ready: {}\n", parent.ready().await?);
    }

    // ========================================================================
    // Example 9: Group/Chord with Children
    // ========================================================================
    println!("9. Group/Chord with Children (Multiple Results)");
    println!("   ----------------------------------------------");

    // Simulate a group of tasks (like Celery's group primitive)
    let group_id = Uuid::new_v4();
    let task1_id = Uuid::new_v4();
    let task2_id = Uuid::new_v4();
    let task3_id = Uuid::new_v4();

    backend.simulate_result(
        group_id,
        TaskResultValue::Success(json!({"group": "aggregated result"})),
    );
    backend.simulate_result(task1_id, TaskResultValue::Success(json!(10)));
    backend.simulate_result(task2_id, TaskResultValue::Success(json!(20)));
    backend.simulate_result(task3_id, TaskResultValue::Success(json!(30)));

    // Create child results
    let child1 = AsyncResult::new(task1_id, backend.clone());
    let child2 = AsyncResult::new(task2_id, backend.clone());
    let child3 = AsyncResult::new(task3_id, backend.clone());

    // Create group result with children
    let group_result =
        AsyncResult::with_children(group_id, backend.clone(), vec![child1, child2, child3]);

    println!("   Group ID: {}", group_result.task_id());
    println!("   Number of children: {}", group_result.children().len());
    println!(
        "   All children ready: {}",
        group_result.children_ready().await?
    );

    // Collect all child results
    let child_results = group_result
        .collect_children(Some(Duration::from_secs(5)))
        .await?;
    println!("   Child results: {:?}", child_results);

    // Access individual children
    for (i, child) in group_result.children().iter().enumerate() {
        println!("     Child {}: task_id={}", i + 1, child.task_id());
    }
    println!();

    // ========================================================================
    // Example 10: Dynamic Child Addition
    // ========================================================================
    println!("10. Dynamic Child Addition");
    println!("    -----------------------");

    let dynamic_parent_id = Uuid::new_v4();
    let dynamic_child_id = Uuid::new_v4();

    backend.simulate_result(
        dynamic_child_id,
        TaskResultValue::Success(json!({"added": "dynamically"})),
    );

    let mut dynamic_result = AsyncResult::new(dynamic_parent_id, backend.clone());
    println!(
        "    Initial children count: {}",
        dynamic_result.children().len()
    );

    // Add child dynamically
    let dynamic_child = AsyncResult::new(dynamic_child_id, backend.clone());
    dynamic_result.add_child(dynamic_child);
    println!("    After add_child: {}", dynamic_result.children().len());
    println!(
        "    Child task ID: {}\n",
        dynamic_result.children()[0].task_id()
    );

    // ========================================================================
    // Summary
    // ========================================================================
    println!("=== Example Complete ===");
    println!();
    println!("AsyncResult API methods demonstrated:");
    println!("  - new(task_id, backend) - Create result handle");
    println!("  - with_parent() - Create with parent relationship");
    println!("  - with_children() - Create with child results (group/chord)");
    println!("  - task_id() - Get task ID");
    println!("  - ready() - Check if task is complete");
    println!("  - successful() - Check if task succeeded");
    println!("  - failed() - Check if task failed");
    println!("  - state() - Get current task state");
    println!("  - info() - Get task metadata/result value");
    println!("  - get(timeout) - Wait for result with timeout");
    println!("  - wait(timeout) - Wait and return value");
    println!("  - result() - Get result without blocking");
    println!("  - traceback() - Get error traceback");
    println!("  - revoke() - Revoke/cancel task");
    println!("  - forget() - Delete result from store");
    println!("  - parent() - Get parent result handle");
    println!("  - children() - Get child result handles");
    println!("  - add_child() - Dynamically add child");
    println!("  - children_ready() - Check if all children complete");
    println!("  - collect_children() - Get all child results");
    println!();
    println!("Supported backends:");
    println!("  - RedisResultBackend (feature: backend-redis)");
    println!("  - PostgresResultBackend (feature: backend-db)");
    println!("  - MysqlResultBackend (feature: backend-db)");
    println!("  - GrpcResultBackend (feature: backend-rpc)");
    println!();
    println!("For real backend usage, ensure the backend service is running");
    println!("and configure connection parameters accordingly.");

    Ok(())
}
