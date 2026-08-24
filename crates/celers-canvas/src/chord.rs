use crate::{CanvasError, Group, Signature};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "backend-redis")]
use crate::dispatch;
#[cfg(feature = "backend-redis")]
use celers_backend_redis::{ChordState, ResultBackend};

/// Message used when a chord is applied without a result backend.
///
/// A chord is defined by its barrier: the callback must run once, after every
/// header task has finished, with their results. That barrier needs shared
/// state the worker can count against, which is what the result backend
/// provides. Without one there is nothing to count, so `apply` refuses rather
/// than degrading into a plain group and reporting success.
#[cfg(not(feature = "backend-redis"))]
pub(crate) const CHORD_REQUIRES_BACKEND: &str = "Chord requires a result backend for barrier \
     synchronisation: the callback can only be triggered once every header task has completed. \
     Enable the `backend-redis` feature and call `Chord::apply(broker, backend)`, or use \
     `Chord::apply_header_only` to dispatch just the header and coordinate the callback yourself.";

/// Chord: Parallel execution with callback
///
/// (task1 | task2 | task3) -> callback([result1, result2, result3])
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chord {
    /// Header (parallel tasks)
    pub header: Group,

    /// Body (callback task)
    pub body: Signature,
}

impl Chord {
    pub fn new(header: Group, body: Signature) -> Self {
        Self { header, body }
    }

    /// Apply the chord: register the barrier, then fan the header out.
    ///
    /// The order matters. Every header task is built **first** so its id is
    /// known, the ids are recorded in the [`ChordState`] together with the
    /// callback, the state is written to the backend, and only then are the
    /// tasks enqueued. Registering before enqueuing closes two holes:
    ///
    /// * A header task cannot complete before its chord state exists, so the
    ///   worker's barrier can never miss a completion.
    /// * `ChordState::task_ids` is populated, so `chord_get_partial_results`
    ///   returns the header results (in declaration order) instead of an empty
    ///   list — the callback receives the real results.
    ///
    /// The callback itself is **not** enqueued here. It is enqueued by the
    /// worker when the completion counter reaches `total`, which is the whole
    /// point of a chord.
    ///
    /// Returns the chord id.
    #[cfg(feature = "backend-redis")]
    pub async fn apply<B: Broker, R: ResultBackend>(
        self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        if self.header.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "Chord header cannot be empty".to_string(),
            ));
        }

        let chord_id = Uuid::new_v4();
        Self::register_and_dispatch(broker, backend, chord_id, &self.header, &self.body).await?;

        Ok(chord_id)
    }

    /// Register a chord barrier for `header`/`body` under `chord_id` and
    /// dispatch the header tasks.
    ///
    /// Shared by [`Chord::apply`] and the nested-workflow machinery, which
    /// needs to establish the very same barrier for a
    /// [`CanvasElement::Chord`](crate::CanvasElement::Chord).
    ///
    /// Returns the ids of the header tasks in declaration order.
    #[cfg(feature = "backend-redis")]
    pub(crate) async fn register_and_dispatch<B: Broker, R: ResultBackend>(
        broker: &B,
        backend: &mut R,
        chord_id: Uuid,
        header: &Group,
        body: &Signature,
    ) -> Result<Vec<Uuid>, CanvasError> {
        if header.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "Chord header cannot be empty".to_string(),
            ));
        }

        // Build every header task up front so the ids are known before the
        // barrier is written; the group id doubles as the chord id so the
        // fan-out is trackable as a group too.
        let built = dispatch::build_fanout(&header.tasks, Some(chord_id), Some(chord_id))?;
        let task_ids: Vec<Uuid> = built.iter().map(|(task, _)| task.metadata.id).collect();

        // Register the barrier BEFORE anything can complete against it.
        let chord_state = ChordState::new(chord_id, task_ids.len(), task_ids.clone())
            .with_callback(body.task.clone());

        backend
            .chord_init(chord_state)
            .await
            .map_err(|e| CanvasError::Broker(format!("Failed to initialize chord: {}", e)))?;

        dispatch::dispatch_all(broker, built).await?;

        Ok(task_ids)
    }

    /// Apply the chord without a result backend.
    ///
    /// This always fails: a chord's defining behaviour is its barrier, and the
    /// barrier needs shared state the worker can count against. Silently
    /// enqueuing only the header (which is what this used to do) turns a chord
    /// into a group and drops the callback while still reporting success, so it
    /// refuses instead.
    ///
    /// Use [`Chord::apply_header_only`] if dispatching just the header — with
    /// the callback coordinated by hand — is genuinely what you want.
    #[cfg(not(feature = "backend-redis"))]
    pub async fn apply<B: Broker>(self, _broker: &B) -> Result<Uuid, CanvasError> {
        Err(CanvasError::Invalid(CHORD_REQUIRES_BACKEND.to_string()))
    }

    /// Dispatch only the chord's header group, without any barrier.
    ///
    /// The callback is **not** enqueued and no chord state is created; the
    /// caller is responsible for collecting the header results and invoking the
    /// callback. This is the honest form of "a chord without a result backend":
    /// it returns the header's group id, not a chord id, and never pretends the
    /// callback was scheduled.
    pub async fn apply_header_only<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.header.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "Chord header cannot be empty".to_string(),
            ));
        }

        self.header.apply(broker).await
    }
}

impl std::fmt::Display for Chord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chord[{} tasks] -> callback({})",
            self.header.tasks.len(),
            self.body.task
        )
    }
}

/// Map: Apply task to multiple arguments
///
/// map(task, [args1, args2, args3]) -> [result1, result2, result3]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Map {
    /// Task to apply
    pub task: Signature,

    /// List of argument sets
    pub argsets: Vec<Vec<serde_json::Value>>,
}

impl Map {
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self { task, argsets }
    }

    /// Expand the map into the equivalent [`Group`] of per-argset tasks
    pub fn to_group(&self) -> Group {
        let mut group = Group::new();

        for args in &self.argsets {
            let mut sig = self.task.clone();
            sig.args = args.clone();
            group = group.add_signature(sig);
        }

        group
    }

    /// Apply the map by creating a group of tasks with different arguments
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        self.to_group().apply(broker).await
    }

    /// Check if map is empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets (and thus tasks)
    pub fn len(&self) -> usize {
        self.argsets.len()
    }
}

impl std::fmt::Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Map[task={}, {} argsets]",
            self.task.task,
            self.argsets.len()
        )
    }
}

/// Starmap: Like map but unpacks arguments
///
/// starmap(task, [(a1, b1), (a2, b2)]) -> [task(a1, b1), task(a2, b2)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Starmap {
    /// Task to apply
    pub task: Signature,

    /// List of argument tuples
    pub argsets: Vec<Vec<serde_json::Value>>,
}

impl Starmap {
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self { task, argsets }
    }

    /// Apply the starmap by creating a group of tasks with unpacked arguments
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        // Starmap is the same as Map - the unpacking happens in task execution
        let map = Map::new(self.task, self.argsets);
        map.apply(broker).await
    }

    /// Check if starmap is empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets (and thus tasks)
    pub fn len(&self) -> usize {
        self.argsets.len()
    }
}

impl std::fmt::Display for Starmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Starmap[task={}, {} argsets]",
            self.task.task,
            self.argsets.len()
        )
    }
}

/// Chunks: Split iterable into chunks for parallel processing
///
/// chunks(task, items, chunk_size) -> Group of tasks, each processing a chunk
///
/// # Example
/// ```
/// use celers_canvas::{Chunks, Signature};
///
/// let task = Signature::new("process_batch".to_string());
/// let items: Vec<serde_json::Value> = (0..100).map(|i| serde_json::json!(i)).collect();
///
/// // Process 100 items in chunks of 10 (creates 10 parallel tasks)
/// let chunks = Chunks::new(task, items, 10);
/// assert_eq!(chunks.num_chunks(), 10);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chunks {
    /// Task to apply to each chunk
    pub task: Signature,

    /// Items to split into chunks
    pub items: Vec<serde_json::Value>,

    /// Size of each chunk
    pub chunk_size: usize,
}

impl Chunks {
    /// Create a new Chunks workflow
    ///
    /// # Arguments
    /// * `task` - The task signature to apply to each chunk
    /// * `items` - Items to split into chunks
    /// * `chunk_size` - Number of items per chunk
    pub fn new(task: Signature, items: Vec<serde_json::Value>, chunk_size: usize) -> Self {
        Self {
            task,
            items,
            chunk_size: chunk_size.max(1), // Minimum chunk size of 1
        }
    }

    /// Get the number of chunks that will be created
    pub fn num_chunks(&self) -> usize {
        if self.items.is_empty() {
            0
        } else {
            self.items.len().div_ceil(self.chunk_size)
        }
    }

    /// Check if chunks is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get total number of items
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Convert to a Group for execution
    pub fn to_group(&self) -> Group {
        let mut group = Group::new();

        for chunk in self.items.chunks(self.chunk_size) {
            let mut sig = self.task.clone();
            sig.args = vec![serde_json::json!(chunk)];
            group = group.add_signature(sig);
        }

        group
    }

    /// Apply the chunks by creating a group of tasks
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.items.is_empty() {
            return Err(CanvasError::Invalid("Chunks cannot be empty".to_string()));
        }

        self.to_group().apply(broker).await
    }
}

impl std::fmt::Display for Chunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunks[task={}, {} items, chunk_size={}, {} chunks]",
            self.task.task,
            self.items.len(),
            self.chunk_size,
            self.num_chunks()
        )
    }
}

/// XMap: Map with exception handling
///
/// Like Map, but continues processing even if some tasks fail.
/// Failed tasks are tracked separately.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XMap {
    /// Task to apply
    pub task: Signature,

    /// List of argument sets
    pub argsets: Vec<Vec<serde_json::Value>>,

    /// Whether to stop on first error
    pub fail_fast: bool,
}

impl XMap {
    /// Create a new XMap workflow
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self {
            task,
            argsets,
            fail_fast: false,
        }
    }

    /// Set fail-fast behavior (stop on first error)
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets
    pub fn len(&self) -> usize {
        self.argsets.len()
    }

    /// Apply the xmap by creating a group of tasks
    ///
    /// Note: Exception handling is done at the result collection level,
    /// not during task submission.
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        let map = Map::new(self.task, self.argsets);
        map.apply(broker).await
    }
}

impl std::fmt::Display for XMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XMap[task={}, {} argsets, fail_fast={}]",
            self.task.task,
            self.argsets.len(),
            self.fail_fast
        )
    }
}

/// XStarmap: Starmap with exception handling
///
/// Like Starmap, but continues processing even if some tasks fail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XStarmap {
    /// Task to apply
    pub task: Signature,

    /// List of argument tuples
    pub argsets: Vec<Vec<serde_json::Value>>,

    /// Whether to stop on first error
    pub fail_fast: bool,
}

impl XStarmap {
    /// Create a new XStarmap workflow
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self {
            task,
            argsets,
            fail_fast: false,
        }
    }

    /// Set fail-fast behavior (stop on first error)
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets
    pub fn len(&self) -> usize {
        self.argsets.len()
    }

    /// Apply the xstarmap by creating a group of tasks
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        let starmap = Starmap::new(self.task, self.argsets);
        starmap.apply(broker).await
    }
}

impl std::fmt::Display for XStarmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XStarmap[task={}, {} argsets, fail_fast={}]",
            self.task.task,
            self.argsets.len(),
            self.fail_fast
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;
    use std::sync::{Arc, Mutex};

    /// Broker that records every enqueued task, in order.
    #[derive(Clone, Default)]
    struct RecordingBroker {
        tasks: Arc<Mutex<Vec<SerializedTask>>>,
    }

    impl RecordingBroker {
        fn tasks(&self) -> Vec<SerializedTask> {
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn names(&self) -> Vec<String> {
            self.tasks()
                .into_iter()
                .map(|task| task.metadata.name)
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl Broker for RecordingBroker {
        async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
            let id = task.metadata.id;
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
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
            Ok(self.tasks().len())
        }

        async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
            Ok(false)
        }
    }

    /// `Map::to_group` must expand one argset per member, preserving order.
    #[test]
    fn map_expands_to_one_task_per_argset() {
        let map = Map::new(
            Signature::new("process".to_string()),
            vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(3)],
            ],
        );

        let group = map.to_group();
        assert_eq!(group.tasks.len(), 3);
        assert_eq!(group.tasks[1].args, vec![serde_json::json!(2)]);
    }

    /// Without a result backend a chord cannot establish its barrier, so
    /// `apply` must fail loudly instead of quietly degrading into a group and
    /// dropping the callback while reporting success.
    #[cfg(not(feature = "backend-redis"))]
    #[tokio::test]
    async fn apply_without_backend_refuses_instead_of_dropping_the_callback() {
        let broker = RecordingBroker::default();

        let chord = Chord::new(
            Group::new().add("h1", vec![]).add("h2", vec![]),
            Signature::new("aggregate".to_string()),
        );

        let err = chord
            .apply(&broker)
            .await
            .expect_err("a chord without a barrier must not report success");
        assert!(err.is_invalid());
        assert!(
            err.to_string().contains("result backend"),
            "the error must say what is missing, got: {err}"
        );
        assert!(
            broker.names().is_empty(),
            "nothing may be enqueued when the chord cannot be honoured"
        );
    }

    /// The honest no-barrier alternative dispatches only the header and never
    /// pretends the callback was scheduled.
    #[tokio::test]
    async fn apply_header_only_dispatches_header_and_never_the_callback() {
        let broker = RecordingBroker::default();

        let chord = Chord::new(
            Group::new().add("h1", vec![]).add("h2", vec![]),
            Signature::new("aggregate".to_string()),
        );

        chord
            .apply_header_only(&broker)
            .await
            .expect("header-only dispatch succeeds");

        assert_eq!(broker.names(), vec!["h1".to_string(), "h2".to_string()]);
        assert!(
            !broker.names().contains(&"aggregate".to_string()),
            "the callback must never be enqueued by a header-only dispatch"
        );
    }

    /// An empty header is not a chord.
    #[tokio::test]
    async fn empty_header_is_rejected() {
        let broker = RecordingBroker::default();
        let chord = Chord::new(Group::new(), Signature::new("cb".to_string()));

        assert!(chord.apply_header_only(&broker).await.is_err());
        assert!(broker.names().is_empty());
    }

    /// Barrier behaviour, which is only observable through a result backend.
    #[cfg(feature = "backend-redis")]
    mod barrier {
        use super::*;
        use crate::tests_backend::MockResultBackend;
        use celers_backend_redis::{ResultBackend, TaskMeta, TaskResult};

        /// `apply` must register the barrier with every header task id, stamp
        /// `chord_id` on the header tasks, and NOT enqueue the callback.
        #[tokio::test]
        async fn apply_registers_the_barrier_before_enqueuing_and_holds_the_callback() {
            let broker = RecordingBroker::default();
            let mut backend = MockResultBackend::new();

            let chord = Chord::new(
                Group::new()
                    .add("h1", vec![serde_json::json!(1)])
                    .add("h2", vec![serde_json::json!(2)])
                    .add("h3", vec![serde_json::json!(3)]),
                Signature::new("aggregate".to_string()),
            );

            let chord_id = chord.apply(&broker, &mut backend).await.expect("apply");

            assert_eq!(
                broker.names(),
                vec!["h1".to_string(), "h2".to_string(), "h3".to_string()],
                "only the header is enqueued; the callback waits for the barrier"
            );

            let state = backend.only_state();
            assert_eq!(state.chord_id, chord_id);
            assert_eq!(state.total, 3);
            assert_eq!(
                state.callback.as_deref(),
                Some("aggregate"),
                "the callback must be recorded in the barrier"
            );

            let enqueued_ids: Vec<_> = broker
                .tasks()
                .into_iter()
                .map(|task| task.metadata.id)
                .collect();
            assert_eq!(
                state.task_ids, enqueued_ids,
                "the barrier must record the real header task ids, in declaration order"
            );

            for task in broker.tasks() {
                assert_eq!(
                    task.metadata.chord_id,
                    Some(chord_id),
                    "every header task must carry the chord id so the worker can count it"
                );
            }
        }

        /// With the ids recorded, the backend returns the header results in the
        /// chord's declaration order — which is what the worker hands to the
        /// callback. With an empty `task_ids` (the old behaviour) this returned
        /// nothing at all.
        #[tokio::test]
        async fn partial_results_come_back_in_declaration_order() {
            let broker = RecordingBroker::default();
            let mut backend = MockResultBackend::new();

            let chord = Chord::new(
                Group::new()
                    .add("h1", vec![])
                    .add("h2", vec![])
                    .add("h3", vec![]),
                Signature::new("aggregate".to_string()),
            );

            let chord_id = chord.apply(&broker, &mut backend).await.expect("apply");

            // Store a distinct result per header task, out of order, to prove
            // the ordering comes from `task_ids` rather than from arrival.
            let ids: Vec<_> = broker
                .tasks()
                .into_iter()
                .map(|task| task.metadata.id)
                .collect();
            for (index, id) in [(2usize, ids[2]), (0, ids[0]), (1, ids[1])] {
                let mut meta = TaskMeta::new(id, "header".to_string());
                meta.result = TaskResult::Success(serde_json::json!(index));
                backend.store_result(id, &meta).await.expect("store");
            }

            let partial = backend
                .chord_get_partial_results(chord_id)
                .await
                .expect("partial results");

            let values: Vec<serde_json::Value> = partial
                .into_iter()
                .map(|(_, meta)| {
                    meta.and_then(|m| m.result.success_value().cloned())
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();

            assert_eq!(
                values,
                vec![
                    serde_json::json!(0),
                    serde_json::json!(1),
                    serde_json::json!(2)
                ],
                "results must be ordered by the chord's header declaration order"
            );
        }

        /// An empty header is rejected before the barrier is written.
        #[tokio::test]
        async fn empty_header_registers_no_barrier() {
            let broker = RecordingBroker::default();
            let mut backend = MockResultBackend::new();

            let chord = Chord::new(Group::new(), Signature::new("cb".to_string()));
            assert!(chord.apply(&broker, &mut backend).await.is_err());

            assert!(backend.states().is_empty(), "no barrier may be registered");
            assert!(broker.names().is_empty());
        }
    }
}
