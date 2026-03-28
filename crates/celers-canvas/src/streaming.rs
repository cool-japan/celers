use crate::Signature;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Streaming map-reduce for processing large datasets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingMapReduce {
    /// Map task signature
    pub map_task: Signature,
    /// Reduce task signature
    pub reduce_task: Signature,
    /// Chunk size for streaming
    pub chunk_size: usize,
    /// Buffer size for intermediate results
    pub buffer_size: usize,
    /// Enable backpressure control
    pub backpressure: bool,
}

impl StreamingMapReduce {
    /// Create a new streaming map-reduce
    pub fn new(map_task: Signature, reduce_task: Signature) -> Self {
        Self {
            map_task,
            reduce_task,
            chunk_size: 100,
            buffer_size: 1000,
            backpressure: true,
        }
    }

    /// Set chunk size
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Enable/disable backpressure
    pub fn with_backpressure(mut self, enabled: bool) -> Self {
        self.backpressure = enabled;
        self
    }
}

impl std::fmt::Display for StreamingMapReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamingMapReduce[map={}, reduce={}, chunk_size={}, buffer_size={}]",
            self.map_task.task, self.reduce_task.task, self.chunk_size, self.buffer_size
        )
    }
}

// ============================================================================
// Reactive Workflows
// ============================================================================

/// Observable value that can trigger reactive workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observable<T> {
    /// Current value
    pub value: T,
    /// Subscribers (workflow IDs)
    #[serde(skip)]
    pub subscribers: Vec<Uuid>,
    /// Value change history
    pub history: Vec<(T, u64)>, // (value, timestamp)
}

impl<T: Clone> Observable<T> {
    /// Create a new observable with initial value
    pub fn new(value: T) -> Self {
        Self {
            value,
            subscribers: Vec::new(),
            history: Vec::new(),
        }
    }

    /// Subscribe a workflow to this observable
    pub fn subscribe(&mut self, workflow_id: Uuid) {
        if !self.subscribers.contains(&workflow_id) {
            self.subscribers.push(workflow_id);
        }
    }

    /// Unsubscribe a workflow
    pub fn unsubscribe(&mut self, workflow_id: &Uuid) {
        self.subscribers.retain(|id| id != workflow_id);
    }

    /// Update the value and notify subscribers
    pub fn set(&mut self, value: T) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.history.push((self.value.clone(), timestamp));
        self.value = value;
    }

    /// Get current value
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Get subscriber count
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
}

/// Reactive workflow that responds to observable changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactiveWorkflow {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Observable IDs being watched
    pub watched_observables: Vec<String>,
    /// Reaction task
    pub reaction_task: Signature,
    /// Debounce time (milliseconds)
    pub debounce_ms: Option<u64>,
    /// Throttle time (milliseconds)
    pub throttle_ms: Option<u64>,
    /// Filter condition (optional)
    pub filter: Option<String>,
}

impl ReactiveWorkflow {
    /// Create a new reactive workflow
    pub fn new(reaction_task: Signature) -> Self {
        Self {
            workflow_id: Uuid::new_v4(),
            watched_observables: Vec::new(),
            reaction_task,
            debounce_ms: None,
            throttle_ms: None,
            filter: None,
        }
    }

    /// Watch an observable
    pub fn watch(mut self, observable_id: impl Into<String>) -> Self {
        self.watched_observables.push(observable_id.into());
        self
    }

    /// Set debounce time (delay reaction until changes stop)
    pub fn with_debounce(mut self, milliseconds: u64) -> Self {
        self.debounce_ms = Some(milliseconds);
        self
    }

    /// Set throttle time (limit reaction frequency)
    pub fn with_throttle(mut self, milliseconds: u64) -> Self {
        self.throttle_ms = Some(milliseconds);
        self
    }

    /// Set filter condition
    pub fn with_filter(mut self, condition: impl Into<String>) -> Self {
        self.filter = Some(condition.into());
        self
    }
}

impl std::fmt::Display for ReactiveWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReactiveWorkflow[id={}, watching={}, reaction={}]",
            self.workflow_id,
            self.watched_observables.len(),
            self.reaction_task.task
        )
    }
}

/// Stream operator for reactive data processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamOperator {
    /// Map values
    Map,
    /// Filter values
    Filter,
    /// Reduce values
    Reduce,
    /// Scan (accumulate)
    Scan,
    /// Take first N values
    Take,
    /// Skip first N values
    Skip,
    /// Debounce
    Debounce,
    /// Throttle
    Throttle,
}

impl std::fmt::Display for StreamOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Map => write!(f, "Map"),
            Self::Filter => write!(f, "Filter"),
            Self::Reduce => write!(f, "Reduce"),
            Self::Scan => write!(f, "Scan"),
            Self::Take => write!(f, "Take"),
            Self::Skip => write!(f, "Skip"),
            Self::Debounce => write!(f, "Debounce"),
            Self::Throttle => write!(f, "Throttle"),
        }
    }
}

/// Reactive stream for data flow processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactiveStream {
    /// Stream ID
    pub stream_id: Uuid,
    /// Source observable ID
    pub source_id: String,
    /// Stream operators
    pub operators: Vec<(StreamOperator, serde_json::Value)>,
    /// Subscriber workflows
    #[serde(skip)]
    pub subscribers: Vec<Uuid>,
}

impl ReactiveStream {
    /// Create a new reactive stream
    pub fn new(source_id: impl Into<String>) -> Self {
        Self {
            stream_id: Uuid::new_v4(),
            source_id: source_id.into(),
            operators: Vec::new(),
            subscribers: Vec::new(),
        }
    }

    /// Add a map operator
    pub fn map(mut self, transform: serde_json::Value) -> Self {
        self.operators.push((StreamOperator::Map, transform));
        self
    }

    /// Add a filter operator
    pub fn filter(mut self, condition: serde_json::Value) -> Self {
        self.operators.push((StreamOperator::Filter, condition));
        self
    }

    /// Add a take operator
    pub fn take(mut self, count: usize) -> Self {
        self.operators
            .push((StreamOperator::Take, serde_json::json!(count)));
        self
    }

    /// Add a skip operator
    pub fn skip(mut self, count: usize) -> Self {
        self.operators
            .push((StreamOperator::Skip, serde_json::json!(count)));
        self
    }

    /// Add a debounce operator
    pub fn debounce(mut self, milliseconds: u64) -> Self {
        self.operators
            .push((StreamOperator::Debounce, serde_json::json!(milliseconds)));
        self
    }

    /// Add a throttle operator
    pub fn throttle(mut self, milliseconds: u64) -> Self {
        self.operators
            .push((StreamOperator::Throttle, serde_json::json!(milliseconds)));
        self
    }

    /// Subscribe a workflow to this stream
    pub fn subscribe(&mut self, workflow_id: Uuid) {
        if !self.subscribers.contains(&workflow_id) {
            self.subscribers.push(workflow_id);
        }
    }
}

impl std::fmt::Display for ReactiveStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReactiveStream[id={}, source={}, operators={}]",
            self.stream_id,
            self.source_id,
            self.operators.len()
        )
    }
}
