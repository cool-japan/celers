use crate::{Broker, SerializedTask};
use async_trait::async_trait;
use celers_core::broker::BrokerMessage;
use celers_core::error::CelersError;
use celers_core::task::TaskId;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, CelersError>;

/// Mock broker for testing
///
/// This broker stores tasks in memory and provides inspection capabilities
/// for testing task execution.
#[derive(Clone)]
pub struct MockBroker {
    queue: Arc<Mutex<VecDeque<SerializedTask>>>,
    published_tasks: Arc<Mutex<Vec<SerializedTask>>>,
}

impl MockBroker {
    /// Create a new mock broker
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            published_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the number of tasks in the queue
    pub fn queue_len(&self) -> usize {
        self.queue
            .lock()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Get all published tasks
    pub fn published_tasks(&self) -> Vec<SerializedTask> {
        self.published_tasks
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    /// Clear all tasks
    pub fn clear(&self) {
        self.queue
            .lock()
            .expect("lock should not be poisoned")
            .clear();
        self.published_tasks
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }

    /// Push a task to the front of the queue (for testing)
    pub fn push_task(&self, task: SerializedTask) {
        self.queue
            .lock()
            .expect("lock should not be poisoned")
            .push_back(task);
    }
}

impl Default for MockBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Broker for MockBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        self.published_tasks
            .lock()
            .expect("lock should not be poisoned")
            .push(task.clone());
        self.queue
            .lock()
            .expect("lock should not be poisoned")
            .push_back(task);
        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        let task = self
            .queue
            .lock()
            .expect("lock should not be poisoned")
            .pop_front();
        Ok(task.map(BrokerMessage::new))
    }

    async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(self
            .queue
            .lock()
            .expect("lock should not be poisoned")
            .len())
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let mut queue = self.queue.lock().expect("lock should not be poisoned");
        let original_len = queue.len();
        queue.retain(|t| &t.metadata.id != task_id);
        Ok(queue.len() < original_len)
    }
}

/// Task builder for testing
pub struct TaskBuilder {
    name: String,
    id: Option<String>,
    max_retries: u32,
    payload: Vec<u8>,
}

impl TaskBuilder {
    /// Create a new task builder
    pub fn new(task_name: &str) -> Self {
        Self {
            name: task_name.to_string(),
            id: None,
            max_retries: 0,
            payload: Vec::new(),
        }
    }

    /// Set task ID
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Set max retries
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set payload
    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    /// Build the task
    pub fn build(self) -> SerializedTask {
        use uuid::Uuid;

        let mut task = SerializedTask::new(self.name, self.payload);
        if let Some(id) = self.id {
            task.metadata.id = Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::new_v4());
        }
        task.metadata.max_retries = self.max_retries;
        task
    }
}

/// Helper to create a simple test task
pub fn create_test_task(name: &str) -> SerializedTask {
    TaskBuilder::new(name).build()
}

/// Task debugger for inspecting task state and execution
pub struct TaskDebugger {
    task_history: Arc<Mutex<Vec<TaskDebugInfo>>>,
}

/// Debug information for a task
#[derive(Debug, Clone)]
pub struct TaskDebugInfo {
    /// Unique identifier for the task
    pub task_id: String,
    /// Name of the task
    pub task_name: String,
    /// Current state of the task
    pub state: String,
    /// Timestamp when this debug info was captured
    pub timestamp: std::time::SystemTime,
    /// Additional metadata about the task
    pub metadata: std::collections::HashMap<String, String>,
}

impl TaskDebugger {
    /// Create a new task debugger
    pub fn new() -> Self {
        Self {
            task_history: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record task execution
    pub fn record_task(&self, task: &SerializedTask, state: &str) {
        let mut history = self
            .task_history
            .lock()
            .expect("lock should not be poisoned");
        history.push(TaskDebugInfo {
            task_id: task.metadata.id.to_string(),
            task_name: task.metadata.name.clone(),
            state: state.to_string(),
            timestamp: std::time::SystemTime::now(),
            metadata: std::collections::HashMap::new(),
        });
    }

    /// Get task history
    pub fn history(&self) -> Vec<TaskDebugInfo> {
        self.task_history
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    /// Clear history
    pub fn clear(&self) {
        self.task_history
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }

    /// Get tasks by state
    pub fn tasks_by_state(&self, state: &str) -> Vec<TaskDebugInfo> {
        self.task_history
            .lock()
            .unwrap()
            .iter()
            .filter(|info| info.state == state)
            .cloned()
            .collect()
    }

    /// Print task history in a formatted table
    pub fn print_history(&self) {
        let history = self.history();
        println!(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                            Task Execution History                             ║"
        );
        println!(
            "╚══════════════════════════════════════════════════════════════════════════════╝\n"
        );

        for (idx, info) in history.iter().enumerate() {
            println!("Task #{}", idx + 1);
            println!("  ID:        {}", info.task_id);
            println!("  Name:      {}", info.task_name);
            println!("  State:     {}", info.state);
            println!("  Timestamp: {:?}", info.timestamp);
            if !info.metadata.is_empty() {
                println!("  Metadata:");
                for (key, value) in &info.metadata {
                    println!("    {}: {}", key, value);
                }
            }
            println!();
        }
    }
}

impl Default for TaskDebugger {
    fn default() -> Self {
        Self::new()
    }
}

/// Event tracker for debugging task events
pub struct EventTracker {
    events: Arc<Mutex<Vec<TrackedEvent>>>,
}

/// Tracked event information
#[derive(Debug, Clone)]
pub struct TrackedEvent {
    /// Type of the event (e.g., "task_received", "task_started", "task_completed")
    pub event_type: String,
    /// Optional task ID associated with this event
    pub task_id: Option<String>,
    /// Human-readable message describing the event
    pub message: String,
    /// Timestamp when this event occurred
    pub timestamp: std::time::SystemTime,
}

impl EventTracker {
    /// Create a new event tracker
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Track an event
    pub fn track(&self, event_type: &str, task_id: Option<String>, message: String) {
        let mut events = self.events.lock().expect("lock should not be poisoned");
        events.push(TrackedEvent {
            event_type: event_type.to_string(),
            task_id,
            message,
            timestamp: std::time::SystemTime::now(),
        });
    }

    /// Get all events
    pub fn events(&self) -> Vec<TrackedEvent> {
        self.events
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    /// Get events by type
    pub fn events_by_type(&self, event_type: &str) -> Vec<TrackedEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.event_type == event_type)
            .cloned()
            .collect()
    }

    /// Clear events
    pub fn clear(&self) {
        self.events
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }

    /// Print events in a formatted table
    pub fn print_events(&self) {
        let events = self.events();
        println!(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                              Event Log                                        ║"
        );
        println!(
            "╚══════════════════════════════════════════════════════════════════════════════╝\n"
        );

        for (idx, event) in events.iter().enumerate() {
            println!("Event #{}", idx + 1);
            println!("  Type:      {}", event.event_type);
            if let Some(ref task_id) = event.task_id {
                println!("  Task ID:   {}", task_id);
            }
            println!("  Message:   {}", event.message);
            println!("  Timestamp: {:?}", event.timestamp);
            println!();
        }
    }
}

impl Default for EventTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance profiler for debugging task execution time
pub struct PerformanceProfiler {
    measurements: Arc<Mutex<Vec<PerformanceMeasurement>>>,
}

/// Performance measurement data
#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    /// Name of the operation being measured
    pub name: String,
    /// Duration of the operation in milliseconds
    pub duration_ms: u128,
    /// Timestamp when this measurement was taken
    pub timestamp: std::time::SystemTime,
    /// Additional metadata about the measurement
    pub metadata: std::collections::HashMap<String, String>,
}

impl PerformanceProfiler {
    /// Create a new performance profiler
    pub fn new() -> Self {
        Self {
            measurements: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Start a measurement
    pub fn start_measurement(&self, name: &str) -> MeasurementGuard {
        MeasurementGuard {
            name: name.to_string(),
            start: std::time::Instant::now(),
            profiler: self.clone(),
        }
    }

    /// Record a measurement
    fn record(&self, name: String, duration_ms: u128) {
        let mut measurements = self
            .measurements
            .lock()
            .expect("lock should not be poisoned");
        measurements.push(PerformanceMeasurement {
            name,
            duration_ms,
            timestamp: std::time::SystemTime::now(),
            metadata: std::collections::HashMap::new(),
        });
    }

    /// Get all measurements
    pub fn measurements(&self) -> Vec<PerformanceMeasurement> {
        self.measurements
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    /// Clear measurements
    pub fn clear(&self) {
        self.measurements
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }

    /// Get average duration for a measurement name
    pub fn average_duration(&self, name: &str) -> Option<u128> {
        let measurements = self
            .measurements
            .lock()
            .expect("lock should not be poisoned");
        let matching: Vec<_> = measurements
            .iter()
            .filter(|m| m.name == name)
            .map(|m| m.duration_ms)
            .collect();

        if matching.is_empty() {
            None
        } else {
            Some(matching.iter().sum::<u128>() / matching.len() as u128)
        }
    }

    /// Print performance summary
    pub fn print_summary(&self) {
        let measurements = self.measurements();
        println!(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                          Performance Summary                                  ║"
        );
        println!(
            "╚══════════════════════════════════════════════════════════════════════════════╝\n"
        );

        // Group by name
        let mut grouped: std::collections::HashMap<String, Vec<u128>> =
            std::collections::HashMap::new();

        for m in measurements {
            grouped.entry(m.name).or_default().push(m.duration_ms);
        }

        for (name, durations) in grouped {
            let count = durations.len();
            let total: u128 = durations.iter().sum();
            let avg = total / count as u128;
            let min = *durations
                .iter()
                .min()
                .expect("collection validated to be non-empty");
            let max = *durations
                .iter()
                .max()
                .expect("collection validated to be non-empty");

            println!("{}", name);
            println!("  Count: {}", count);
            println!("  Avg:   {} ms", avg);
            println!("  Min:   {} ms", min);
            println!("  Max:   {} ms", max);
            println!("  Total: {} ms", total);
            println!();
        }
    }
}

impl Clone for PerformanceProfiler {
    fn clone(&self) -> Self {
        Self {
            measurements: Arc::clone(&self.measurements),
        }
    }
}

impl Default for PerformanceProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for performance measurements
pub struct MeasurementGuard {
    name: String,
    start: std::time::Instant,
    profiler: PerformanceProfiler,
}

impl Drop for MeasurementGuard {
    fn drop(&mut self) {
        let duration_ms = self.start.elapsed().as_millis();
        self.profiler.record(self.name.clone(), duration_ms);
    }
}

/// Queue inspector for debugging broker queues
pub struct QueueInspector {
    snapshots: Arc<Mutex<Vec<QueueSnapshot>>>,
}

/// Snapshot of queue state
#[derive(Debug, Clone)]
pub struct QueueSnapshot {
    /// Number of tasks in the queue at the time of snapshot
    pub queue_size: usize,
    /// Timestamp when this snapshot was taken
    pub timestamp: std::time::SystemTime,
    /// Additional metadata about the queue state
    pub metadata: std::collections::HashMap<String, String>,
}

impl QueueInspector {
    /// Create a new queue inspector
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Take a snapshot of the queue
    pub async fn snapshot(&self, broker: &MockBroker) {
        let size = broker.queue_len();
        let mut snapshots = self.snapshots.lock().expect("lock should not be poisoned");
        snapshots.push(QueueSnapshot {
            queue_size: size,
            timestamp: std::time::SystemTime::now(),
            metadata: std::collections::HashMap::new(),
        });
    }

    /// Get all snapshots
    pub fn snapshots(&self) -> Vec<QueueSnapshot> {
        self.snapshots
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    /// Clear snapshots
    pub fn clear(&self) {
        self.snapshots
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }

    /// Print queue history
    pub fn print_history(&self) {
        let snapshots = self.snapshots();
        println!(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                            Queue Size History                                 ║"
        );
        println!(
            "╚══════════════════════════════════════════════════════════════════════════════╝\n"
        );

        for (idx, snapshot) in snapshots.iter().enumerate() {
            println!("Snapshot #{}", idx + 1);
            println!("  Queue Size: {}", snapshot.queue_size);
            println!("  Timestamp:  {:?}", snapshot.timestamp);
            println!();
        }
    }
}

impl Default for QueueInspector {
    fn default() -> Self {
        Self::new()
    }
}
