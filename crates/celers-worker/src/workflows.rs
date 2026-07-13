//! Workflow support for Canvas primitives
//!
//! This module provides worker integration for Chain and Chord workflows.

use celers_core::{Broker, SerializedTask};
use tracing::{debug, info, warn};

#[cfg(feature = "workflows")]
use celers_backend_redis::ResultBackend;

#[cfg(not(feature = "workflows"))]
pub trait ResultBackend {}

/// Handle task completion for workflow primitives
///
/// This function should be called after a task completes successfully.
/// It checks for workflow metadata (chain links, chord membership) and
/// triggers appropriate workflow actions.
///
/// # Chain Callback Execution
///
/// If the task has an `on_success_link` in its metadata, enqueue the next
/// task in the chain with the current task's result bytes as the payload.
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

                        // Collect the individual header-task results so the chord
                        // callback receives them as its first positional argument,
                        // matching Celery semantics. Results are returned in the
                        // chord's `task_ids` order by the backend.
                        let partial_results = backend
                            .chord_get_partial_results(chord_id)
                            .await
                            .map_err(|e| {
                                WorkflowError::Backend(format!(
                                    "Failed to collect chord results: {}",
                                    e
                                ))
                            })?;

                        // Convert each task's result into its return value. Missing,
                        // pending, or failed results are represented as JSON `null`
                        // so a single absent result does not abort the callback.
                        let result_values: Vec<serde_json::Value> = partial_results
                            .into_iter()
                            .map(|(task_id, meta)| match meta {
                                Some(meta) => match meta.result.success_value() {
                                    Some(value) => value.clone(),
                                    None => {
                                        debug!(
                                            "Chord {} task {} has no success value ({}); using null",
                                            chord_id, task_id, meta.result
                                        );
                                        serde_json::Value::Null
                                    }
                                },
                                None => {
                                    debug!(
                                        "Chord {} task {} result missing; using null",
                                        chord_id, task_id
                                    );
                                    serde_json::Value::Null
                                }
                            })
                            .collect();

                        // Create callback task with aggregated results: the list of
                        // header-task results becomes the first positional argument.
                        let callback_args = serde_json::json!({
                            "args": [serde_json::Value::Array(result_values)],
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

    // Handle chain callback (Celery-style on_success link)
    // If this task has an on_success_link, enqueue the next task in the chain,
    // passing the current task's result bytes as the payload.
    if let Some(ref link_name) = task.metadata.on_success_link {
        debug!(
            "Task {} has on_success_link to '{}', enqueuing chain continuation",
            task_id, link_name
        );

        // Build the next task; its payload is this task's result bytes
        let next_task = celers_core::SerializedTask::new(link_name.clone(), _result.to_vec());

        broker.enqueue(next_task).await.map_err(|e| {
            WorkflowError::Broker(format!(
                "Failed to enqueue chain link '{}': {}",
                link_name, e
            ))
        })?;

        info!("Chain link '{}' enqueued from task {}", link_name, task_id);
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;

    /// Verify that `on_success_link` roundtrips correctly through
    /// `SerializedTask` serialization/deserialization.
    #[test]
    fn test_workflow_no_link_serialization_roundtrip() {
        let task = SerializedTask::new("my_task".to_string(), b"payload".to_vec());

        // No on_success_link set — field must be absent after serde roundtrip
        let json = serde_json::to_string(&task).expect("serialize");
        let restored: SerializedTask = serde_json::from_str(&json).expect("deserialize");

        assert!(
            restored.metadata.on_success_link.is_none(),
            "on_success_link should be None when never set"
        );
        // The field must be absent from the JSON (skip_serializing_if)
        assert!(
            !json.contains("on_success_link"),
            "on_success_link should be omitted from JSON when None"
        );
    }

    /// Verify that a task with `on_success_link = "next_task"` roundtrips the
    /// field correctly, preserving the link name through serde JSON.
    #[test]
    fn test_workflow_chain_link_serialization_roundtrip() {
        let task = SerializedTask::new("step_a".to_string(), b"result_bytes".to_vec())
            .with_on_success_link("next_task".to_string());

        assert_eq!(
            task.metadata.on_success_link.as_deref(),
            Some("next_task"),
            "on_success_link must be Some(\"next_task\") before serialization"
        );

        let json = serde_json::to_string(&task).expect("serialize");

        // The field must be present in the JSON
        assert!(
            json.contains("on_success_link"),
            "on_success_link must appear in JSON when set"
        );
        assert!(
            json.contains("next_task"),
            "link name must appear in serialized JSON"
        );

        let restored: SerializedTask = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            restored.metadata.on_success_link.as_deref(),
            Some("next_task"),
            "on_success_link must survive a serde roundtrip"
        );
        assert_eq!(
            restored.metadata.name, "step_a",
            "task name must survive a serde roundtrip"
        );
    }

    /// Tests for real chord result aggregation in `handle_workflow_completion`.
    ///
    /// These require the `workflows` feature because they exercise the real
    /// [`celers_backend_redis::ResultBackend`] trait.
    #[cfg(feature = "workflows")]
    mod chord_aggregation {
        use super::*;
        use async_trait::async_trait;
        use celers_backend_redis::{
            ChordState, Result as BackendResult, ResultBackend, TaskMeta, TaskResult,
        };
        use celers_core::Broker;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use uuid::Uuid;

        /// Broker mock that records every enqueued task for later assertions.
        #[derive(Clone, Default)]
        struct RecordingBroker {
            enqueued: Arc<Mutex<Vec<SerializedTask>>>,
        }

        impl RecordingBroker {
            fn enqueued_tasks(&self) -> Vec<SerializedTask> {
                self.enqueued
                    .lock()
                    .expect("recording broker mutex poisoned")
                    .clone()
            }
        }

        #[async_trait]
        impl Broker for RecordingBroker {
            async fn enqueue(
                &self,
                task: SerializedTask,
            ) -> celers_core::Result<celers_core::TaskId> {
                let id = task.metadata.id;
                self.enqueued
                    .lock()
                    .expect("recording broker mutex poisoned")
                    .push(task);
                Ok(id)
            }

            async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
                Ok(None)
            }

            async fn ack(
                &self,
                _task_id: &celers_core::TaskId,
                _receipt_handle: Option<&str>,
            ) -> celers_core::Result<()> {
                Ok(())
            }

            async fn reject(
                &self,
                _task_id: &celers_core::TaskId,
                _receipt_handle: Option<&str>,
                _requeue: bool,
            ) -> celers_core::Result<()> {
                Ok(())
            }

            async fn queue_size(&self) -> celers_core::Result<usize> {
                Ok(0)
            }

            async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
                Ok(false)
            }
        }

        /// In-memory `ResultBackend` mock returning canned chord state and
        /// per-task results. The chord is reported as fully complete so the
        /// callback path is exercised.
        struct MockChordBackend {
            state: ChordState,
            /// Per-task-id result metadata; absence means a missing result.
            results: HashMap<Uuid, TaskMeta>,
        }

        #[async_trait]
        impl ResultBackend for MockChordBackend {
            async fn store_result(
                &mut self,
                _task_id: Uuid,
                _meta: &TaskMeta,
            ) -> BackendResult<()> {
                Ok(())
            }

            async fn get_result(&mut self, task_id: Uuid) -> BackendResult<Option<TaskMeta>> {
                Ok(self.results.get(&task_id).cloned())
            }

            async fn delete_result(&mut self, _task_id: Uuid) -> BackendResult<()> {
                Ok(())
            }

            async fn set_expiration(
                &mut self,
                _task_id: Uuid,
                _ttl: Duration,
            ) -> BackendResult<()> {
                Ok(())
            }

            async fn chord_init(&mut self, state: ChordState) -> BackendResult<()> {
                self.state = state;
                Ok(())
            }

            async fn chord_complete_task(&mut self, _chord_id: Uuid) -> BackendResult<usize> {
                // Report the chord as fully complete to trigger the callback.
                Ok(self.state.total)
            }

            async fn chord_get_state(
                &mut self,
                _chord_id: Uuid,
            ) -> BackendResult<Option<ChordState>> {
                Ok(Some(self.state.clone()))
            }
        }

        /// Build a `TaskMeta` whose result is a successful JSON value.
        fn success_meta(task_id: Uuid, value: serde_json::Value) -> TaskMeta {
            let mut meta = TaskMeta::new(task_id, "header_task".to_string());
            meta.result = TaskResult::Success(value);
            meta
        }

        /// A chord with three header tasks (the middle one's result is missing)
        /// must enqueue the callback with the results collected in `task_ids`
        /// order, substituting JSON `null` for the missing result.
        #[tokio::test]
        async fn chord_callback_receives_aggregated_results_in_order() {
            let chord_id = Uuid::new_v4();
            let id_a = Uuid::new_v4();
            let id_b = Uuid::new_v4();
            let id_c = Uuid::new_v4();

            let state = ChordState::new(chord_id, 3, vec![id_a, id_b, id_c])
                .with_callback("aggregate_callback".to_string());

            // Only the first and third tasks have stored results; the middle one
            // is intentionally missing to verify the null-substitution behaviour.
            let mut results = HashMap::new();
            results.insert(id_a, success_meta(id_a, serde_json::json!(10)));
            results.insert(id_c, success_meta(id_c, serde_json::json!("three")));

            let mut backend = MockChordBackend { state, results };

            // The completing task carries the chord_id so the chord branch runs.
            let completing = SerializedTask::new("header_task".to_string(), b"ignored".to_vec())
                .with_chord_id(chord_id);

            let broker = RecordingBroker::default();

            handle_workflow_completion(&completing, b"ignored", &broker, Some(&mut backend))
                .await
                .expect("chord completion handling should succeed");

            let enqueued = broker.enqueued_tasks();
            assert_eq!(
                enqueued.len(),
                1,
                "exactly one chord callback task should be enqueued"
            );

            let callback = &enqueued[0];
            assert_eq!(
                callback.metadata.name, "aggregate_callback",
                "the enqueued task must be the chord callback"
            );

            let payload: serde_json::Value =
                serde_json::from_slice(&callback.payload).expect("callback payload must be JSON");

            // Celery semantics: the callback's first positional argument is the
            // ordered list of header-task results.
            let args = payload
                .get("args")
                .and_then(|a| a.as_array())
                .expect("callback args must be a JSON array");
            assert_eq!(args.len(), 1, "callback receives a single positional arg");

            let results_arg = args[0]
                .as_array()
                .expect("first positional arg must be the results list");
            assert_eq!(
                results_arg,
                &vec![
                    serde_json::json!(10),
                    serde_json::Value::Null,
                    serde_json::json!("three"),
                ],
                "results must be aggregated in task_ids order with null for the missing one"
            );

            // kwargs is present and empty.
            assert_eq!(
                payload.get("kwargs"),
                Some(&serde_json::json!({})),
                "callback kwargs must be an empty object"
            );
        }

        /// A failed header-task result must be aggregated as JSON `null` rather
        /// than aborting the whole callback.
        #[tokio::test]
        async fn chord_callback_substitutes_null_for_failed_result() {
            let chord_id = Uuid::new_v4();
            let id_a = Uuid::new_v4();
            let id_b = Uuid::new_v4();

            let state = ChordState::new(chord_id, 2, vec![id_a, id_b])
                .with_callback("aggregate_callback".to_string());

            let mut results = HashMap::new();
            results.insert(id_a, success_meta(id_a, serde_json::json!({"ok": true})));
            // id_b failed.
            let mut failed = TaskMeta::new(id_b, "header_task".to_string());
            failed.result = TaskResult::Failure("boom".to_string());
            results.insert(id_b, failed);

            let mut backend = MockChordBackend { state, results };

            let completing = SerializedTask::new("header_task".to_string(), b"ignored".to_vec())
                .with_chord_id(chord_id);
            let broker = RecordingBroker::default();

            handle_workflow_completion(&completing, b"ignored", &broker, Some(&mut backend))
                .await
                .expect("chord completion handling should succeed despite a failed task");

            let enqueued = broker.enqueued_tasks();
            assert_eq!(enqueued.len(), 1, "callback must still be enqueued");

            let payload: serde_json::Value =
                serde_json::from_slice(&enqueued[0].payload).expect("payload must be JSON");
            let results_arg = payload["args"][0]
                .as_array()
                .expect("results list expected");
            assert_eq!(
                results_arg,
                &vec![serde_json::json!({"ok": true}), serde_json::Value::Null],
                "failed task result must become null while successes are preserved"
            );
        }
    }
}
