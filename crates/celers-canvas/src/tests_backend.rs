//! In-process [`ResultBackend`] used by the canvas test-suite.
//!
//! Chord barriers are only observable through a result backend, so testing that
//! `Chord::apply` registers the barrier *before* enqueuing (and that the
//! callback is never enqueued directly) needs one. This module provides a
//! dependency-free stand-in that records exactly what the canvas wrote, so the
//! tests can assert on the barrier itself rather than on log output.

use async_trait::async_trait;
use celers_backend_redis::{ChordState, Result as BackendResult, ResultBackend, TaskMeta};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// A minimal in-memory result backend.
#[derive(Debug, Default)]
pub(crate) struct MockResultBackend {
    /// Chord states keyed by chord id, in the order `chord_init` saw them.
    chords: Vec<ChordState>,
    /// Stored task results.
    results: HashMap<Uuid, TaskMeta>,
    /// Per-chord completion counters.
    completed: HashMap<Uuid, usize>,
}

impl MockResultBackend {
    /// Create an empty backend.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The single chord state registered so far.
    ///
    /// Panics (test-only) when the number of registered chords is not exactly
    /// one, which is itself the assertion the callers care about.
    pub(crate) fn only_state(&self) -> ChordState {
        assert_eq!(
            self.chords.len(),
            1,
            "expected exactly one chord to be registered"
        );
        self.chords[0].clone()
    }

    /// Every chord state registered so far.
    #[allow(dead_code)]
    pub(crate) fn states(&self) -> &[ChordState] {
        &self.chords
    }

    /// Seed a task result so `chord_get_partial_results` can return it.
    #[allow(dead_code)]
    pub(crate) fn seed_result(&mut self, task_id: Uuid, meta: TaskMeta) {
        self.results.insert(task_id, meta);
    }
}

#[async_trait]
impl ResultBackend for MockResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> BackendResult<()> {
        self.results.insert(task_id, meta.clone());
        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> BackendResult<Option<TaskMeta>> {
        Ok(self.results.get(&task_id).cloned())
    }

    async fn delete_result(&mut self, task_id: Uuid) -> BackendResult<()> {
        self.results.remove(&task_id);
        Ok(())
    }

    async fn set_expiration(&mut self, _task_id: Uuid, _ttl: Duration) -> BackendResult<()> {
        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> BackendResult<()> {
        if let Some(existing) = self
            .chords
            .iter_mut()
            .find(|s| s.chord_id == state.chord_id)
        {
            *existing = state;
        } else {
            self.chords.push(state);
        }
        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> BackendResult<usize> {
        let counter = self.completed.entry(chord_id).or_insert(0);
        *counter += 1;
        Ok(*counter)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> BackendResult<Option<ChordState>> {
        // The completion count is kept apart from the registered state — as it
        // is in `RedisResultBackend`, where it lives in its own key so it can be
        // incremented atomically — and merged back in here. Without the merge
        // `is_complete()`/`remaining()` would answer from the count the barrier
        // was *registered* with (always zero) rather than the one it has
        // reached, which is the very decision the worker makes on it.
        Ok(self
            .chords
            .iter()
            .find(|state| state.chord_id == chord_id)
            .cloned()
            .map(|mut state| {
                state.completed = self.completed.get(&chord_id).copied().unwrap_or(0);
                state
            }))
    }
}
