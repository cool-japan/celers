use crate::{CanvasError, Group, Signature};
use celers_core::Broker;
#[cfg(feature = "backend-redis")]
use celers_core::SerializedTask;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "backend-redis")]
use celers_backend_redis::{ChordState, ResultBackend};

#[cfg(feature = "backend-redis")]
use chrono::Utc;

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

    /// Apply the chord by initializing state and enqueuing header tasks
    #[cfg(feature = "backend-redis")]
    pub async fn apply<B: Broker, R: ResultBackend>(
        mut self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        if self.header.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "Chord header cannot be empty".to_string(),
            ));
        }

        let chord_id = Uuid::new_v4();
        let total = self.header.tasks.len();

        // Initialize chord state in backend
        let chord_state = ChordState {
            chord_id,
            total,
            completed: 0,
            callback: Some(self.body.task.clone()),
            task_ids: Vec::new(),
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        };

        backend
            .chord_init(chord_state)
            .await
            .map_err(|e| CanvasError::Broker(format!("Failed to initialize chord: {}", e)))?;

        // Enqueue all header tasks with chord_id
        for sig in &mut self.header.tasks {
            let args_json = serde_json::json!({
                "args": sig.args,
                "kwargs": sig.kwargs
            });
            let args_bytes = serde_json::to_vec(&args_json)
                .map_err(|e| CanvasError::Serialization(e.to_string()))?;

            let mut task = SerializedTask::new(sig.task.clone(), args_bytes);

            if let Some(priority) = sig.options.priority {
                task = task.with_priority(priority.into());
            }

            // Set chord_id so worker knows to update chord state on completion
            task.metadata.chord_id = Some(chord_id);

            broker
                .enqueue(task)
                .await
                .map_err(|e| CanvasError::Broker(e.to_string()))?;
        }

        Ok(chord_id)
    }

    /// Apply the chord without a result backend (simpler version)
    #[cfg(not(feature = "backend-redis"))]
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        // Without backend, we can only enqueue header tasks
        // Manual coordination required
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

    /// Apply the map by creating a group of tasks with different arguments
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        let mut group = Group::new();

        for args in self.argsets {
            let mut sig = self.task.clone();
            sig.args = args;
            group = group.add_signature(sig);
        }

        group.apply(broker).await
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
