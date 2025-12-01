//! Workflow support for Canvas primitives
//!
//! This module provides worker integration for Chain and Chord workflows.

use celers_core::{Broker, SerializedTask};
use tracing::{debug, error, info, warn};

#[cfg(feature = "workflows")]
use celers_backend_redis::ResultBackend;

#[cfg(not(feature = "workflows"))]
pub trait ResultBackend {}

#[allow(dead_code)]
use serde_json;

/// Handle task completion for workflow primitives
///
/// This function should be called after a task completes successfully.
/// It checks for workflow metadata (chain links, chord membership) and
/// triggers appropriate workflow actions.
///
/// # Chain Callback Execution
///
/// If the task has a `link` in its options (stored in task metadata),
/// enqueue the next task in the chain with the current result.
///
/// # Chord Barrier Synchronization
///
/// If the task has a `chord_id` in its metadata, increment the completion
/// counter in the result backend. When all tasks complete, trigger the callback.
pub async fn handle_workflow_completion<B: Broker>(
    task: &SerializedTask,
    _result: &[u8],
    broker: &B,
    #[cfg(feature = "workflows")] backend: Option<&mut dyn ResultBackend>,
) -> Result<(), WorkflowError> {
    let task_id = task.metadata.id;

    // Handle chord completion (barrier synchronization)
    #[cfg(feature = "workflows")]
    if let Some(chord_id) = task.metadata.chord_id {
        debug!("Task {} is part of chord {}", task_id, chord_id);

        if let Some(backend) = backend {
            // Atomically increment the completion counter
            let count = backend.chord_complete_task(chord_id).await.map_err(|e| {
                WorkflowError::Backend(format!("Failed to increment chord counter: {}", e))
            })?;

            // Get chord state to check if all tasks are complete
            let state = backend
                .chord_get_state(chord_id)
                .await
                .map_err(|e| WorkflowError::Backend(format!("Failed to get chord state: {}", e)))?;

            if let Some(state) = state {
                if count >= state.total {
                    info!(
                        "Chord {} complete ({}/{}) - ready to trigger callback",
                        chord_id, count, state.total
                    );

                    // Enqueue callback task if specified
                    if let Some(callback_name) = state.callback {
                        info!("Enqueuing chord callback task: {}", callback_name);

                        // Create callback task with aggregated results
                        // For now, we just trigger it without collecting individual results
                        let callback_args = serde_json::json!({
                            "args": [],
                            "kwargs": {}
                        });
                        let args_bytes = serde_json::to_vec(&callback_args)
                            .map_err(|e| WorkflowError::Serialization(e.to_string()))?;

                        let callback_task =
                            celers_core::SerializedTask::new(callback_name, args_bytes);

                        broker
                            .enqueue(callback_task)
                            .await
                            .map_err(|e| WorkflowError::Broker(e.to_string()))?;

                        info!("Chord callback enqueued successfully");
                    }
                } else {
                    debug!(
                        "Chord {} progress: {}/{} tasks complete",
                        chord_id, count, state.total
                    );
                }
            } else {
                warn!("Chord state not found for chord_id {}", chord_id);
            }
        } else {
            warn!("Task has chord_id but no backend configured - cannot track completion");
        }
    }

    // Handle chain callback (sequential execution)
    // Note: The link information is stored in the task metadata during enqueue
    // In the current implementation, links are handled at enqueue time by
    // setting up the chain structure. This is a placeholder for future
    // dynamic link handling if needed.

    Ok(())
}

/// Workflow handling errors
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}
