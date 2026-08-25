//! Regression tests for the checkpoint ambient API: a retried task resumes
//! from what its previous attempt saved, the store is cleared on success,
//! and the helpers are no-ops (not errors) without a manager installed.

use super::doubles::{serialized, wait_until, Empty, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, NoOpEventEmitter, Result, Task, TaskRegistry};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// --------------------------------------------------------------------------

/// Fails on its first attempt, having checkpointed its progress; on the retry
/// it resumes from the checkpoint and succeeds.
struct ResumableTask {
    /// Progress observed at the start of each attempt.
    resumed_from: Arc<Mutex<Vec<u64>>>,
    attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for ResumableTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        let resume_from = crate::execution_context::load_checkpoint()
            .await
            .and_then(|checkpoint| String::from_utf8(checkpoint.data).ok())
            .and_then(|text| text.parse::<u64>().ok())
            .unwrap_or(0);
        self.resumed_from.lock().expect("lock").push(resume_from);

        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        if attempt == 0 {
            // Record progress, then fail so the worker retries us.
            crate::execution_context::save_checkpoint(b"42".to_vec())
                .await
                .expect("checkpoint saves");
            return Err(celers_core::CelersError::TaskExecution(
                "interrupted".to_string(),
            ));
        }
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "resumable_task"
    }
}

// --------------------------------------------------------------------------

/// A checkpoint written by one attempt is visible to the next, and the store is
/// emptied once the task finally succeeds.
#[tokio::test]
async fn test_checkpoints_resume_a_retried_task_and_are_cleared_on_success() {
    let resumed_from = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(ResumableTask {
            resumed_from: Arc::clone(&resumed_from),
            attempts: Arc::clone(&attempts),
        })
        .await;

    let task = serialized("resumable_task").with_max_retries(2);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let checkpoints = Arc::new(crate::checkpoint::CheckpointManager::new(
        crate::checkpoint::CheckpointConfig::new(),
    ));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_checkpoints(Arc::clone(&checkpoints));
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the retry to run", || attempts.load(Ordering::Relaxed) == 2).await;

    // Note the retry carries a *new* delivery of the same task id, so the
    // checkpoint key is stable across attempts.
    wait_until("the successful attempt to be acked", || {
        !broker.acked().is_empty()
    })
    .await;

    let observed = resumed_from.lock().expect("lock").clone();
    assert_eq!(
        observed,
        vec![0, 42],
        "the retry must resume from the checkpoint the first attempt wrote"
    );

    let mut cleared = false;
    for _ in 0..200 {
        if !checkpoints.has_checkpoint(&task_id.to_string()).await {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        cleared,
        "a completed task's checkpoints must not be left behind"
    );

    handle.shutdown().await.expect("shutdown");
}

/// Without a manager installed the ambient helpers are no-ops rather than
/// errors, so task code can call them unconditionally.
#[tokio::test]
async fn test_checkpoint_helpers_are_noops_without_a_manager() {
    assert!(crate::execution_context::current_checkpoints().is_none());
    assert!(crate::execution_context::load_checkpoint().await.is_none());
    assert!(
        !crate::execution_context::save_checkpoint(b"ignored".to_vec())
            .await
            .expect("a no-op cannot fail"),
        "reports that nothing was stored"
    );
}
