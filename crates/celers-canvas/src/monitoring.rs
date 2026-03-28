use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Resource utilization metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilization {
    /// CPU utilization (0.0 to 1.0)
    pub cpu: f64,
    /// Memory utilization (0.0 to 1.0)
    pub memory: f64,
    /// Disk I/O utilization (0.0 to 1.0)
    pub disk_io: f64,
    /// Network I/O utilization (0.0 to 1.0)
    pub network_io: f64,
    /// Timestamp
    pub timestamp: u64,
}

impl ResourceUtilization {
    /// Create a new resource utilization snapshot
    pub fn new(cpu: f64, memory: f64, disk_io: f64, network_io: f64) -> Self {
        Self {
            cpu: cpu.clamp(0.0, 1.0),
            memory: memory.clamp(0.0, 1.0),
            disk_io: disk_io.clamp(0.0, 1.0),
            network_io: network_io.clamp(0.0, 1.0),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Get overall utilization (average of all metrics)
    pub fn overall(&self) -> f64 {
        (self.cpu + self.memory + self.disk_io + self.network_io) / 4.0
    }

    /// Check if any resource is over threshold
    pub fn is_overloaded(&self, threshold: f64) -> bool {
        self.cpu > threshold
            || self.memory > threshold
            || self.disk_io > threshold
            || self.network_io > threshold
    }

    /// Get the most utilized resource
    pub fn bottleneck(&self) -> &'static str {
        let max = self
            .cpu
            .max(self.memory)
            .max(self.disk_io)
            .max(self.network_io);
        if (max - self.cpu).abs() < f64::EPSILON {
            "cpu"
        } else if (max - self.memory).abs() < f64::EPSILON {
            "memory"
        } else if (max - self.disk_io).abs() < f64::EPSILON {
            "disk_io"
        } else {
            "network_io"
        }
    }
}

impl std::fmt::Display for ResourceUtilization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ResourceUtilization[cpu={:.2}, mem={:.2}, disk={:.2}, net={:.2}, overall={:.2}]",
            self.cpu,
            self.memory,
            self.disk_io,
            self.network_io,
            self.overall()
        )
    }
}

/// Workflow resource monitor
#[derive(Debug, Clone)]
pub struct WorkflowResourceMonitor {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Resource utilization history
    pub history: Vec<ResourceUtilization>,
    /// Maximum history size
    pub max_history: usize,
    /// Sampling interval (seconds)
    pub sampling_interval: u64,
}

impl WorkflowResourceMonitor {
    /// Create a new resource monitor
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            history: Vec::new(),
            max_history: 1000,
            sampling_interval: 5,
        }
    }

    /// Set maximum history size
    pub fn with_max_history(mut self, max: usize) -> Self {
        self.max_history = max;
        self
    }

    /// Set sampling interval
    pub fn with_sampling_interval(mut self, seconds: u64) -> Self {
        self.sampling_interval = seconds;
        self
    }

    /// Record resource utilization
    pub fn record(&mut self, utilization: ResourceUtilization) {
        self.history.push(utilization);
        // Trim history if needed
        if self.history.len() > self.max_history {
            self.history
                .drain(0..(self.history.len() - self.max_history));
        }
    }

    /// Get average utilization over time window (seconds)
    pub fn average_utilization(&self, window_seconds: u64) -> Option<ResourceUtilization> {
        if self.history.is_empty() {
            return None;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now.saturating_sub(window_seconds);

        let recent: Vec<_> = self
            .history
            .iter()
            .filter(|u| u.timestamp >= cutoff)
            .collect();

        if recent.is_empty() {
            return None;
        }

        let sum_cpu: f64 = recent.iter().map(|u| u.cpu).sum();
        let sum_memory: f64 = recent.iter().map(|u| u.memory).sum();
        let sum_disk: f64 = recent.iter().map(|u| u.disk_io).sum();
        let sum_network: f64 = recent.iter().map(|u| u.network_io).sum();
        let count = recent.len() as f64;

        Some(ResourceUtilization::new(
            sum_cpu / count,
            sum_memory / count,
            sum_disk / count,
            sum_network / count,
        ))
    }

    /// Get peak utilization
    pub fn peak_utilization(&self) -> Option<&ResourceUtilization> {
        self.history.iter().max_by(|a, b| {
            a.overall()
                .partial_cmp(&b.overall())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }

    /// Clear history
    pub fn clear(&mut self) {
        self.history.clear();
    }
}

impl std::fmt::Display for WorkflowResourceMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowResourceMonitor[workflow={}, samples={}]",
            self.workflow_id,
            self.history.len()
        )
    }
}

// ============================================================================
// Workflow Testing Framework
// ============================================================================

/// Mock task result for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockTaskResult {
    /// Task name
    pub task_name: String,
    /// Result value
    pub result: serde_json::Value,
    /// Execution delay (milliseconds)
    pub delay_ms: u64,
    /// Whether to fail
    pub should_fail: bool,
    /// Failure message
    pub failure_message: Option<String>,
}

impl MockTaskResult {
    /// Create a successful mock result
    pub fn success(task_name: impl Into<String>, result: serde_json::Value) -> Self {
        Self {
            task_name: task_name.into(),
            result,
            delay_ms: 0,
            should_fail: false,
            failure_message: None,
        }
    }

    /// Create a failing mock result
    pub fn failure(task_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            task_name: task_name.into(),
            result: serde_json::Value::Null,
            delay_ms: 0,
            should_fail: true,
            failure_message: Some(message.into()),
        }
    }

    /// Set execution delay
    pub fn with_delay(mut self, milliseconds: u64) -> Self {
        self.delay_ms = milliseconds;
        self
    }
}

/// Mock task executor for testing workflows
#[derive(Debug, Clone)]
pub struct MockTaskExecutor {
    /// Mock results by task name
    pub mock_results: HashMap<String, MockTaskResult>,
    /// Execution history
    pub execution_history: Vec<(String, u64)>, // (task_name, timestamp)
}

impl MockTaskExecutor {
    /// Create a new mock executor
    pub fn new() -> Self {
        Self {
            mock_results: HashMap::new(),
            execution_history: Vec::new(),
        }
    }

    /// Register a mock result for a task
    pub fn register(&mut self, result: MockTaskResult) {
        self.mock_results.insert(result.task_name.clone(), result);
    }

    /// Execute a mock task
    pub fn execute(&mut self, task_name: &str) -> Result<serde_json::Value, String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.execution_history
            .push((task_name.to_string(), timestamp));

        if let Some(mock_result) = self.mock_results.get(task_name) {
            // Simulate delay
            if mock_result.delay_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(mock_result.delay_ms));
            }

            if mock_result.should_fail {
                Err(mock_result
                    .failure_message
                    .clone()
                    .unwrap_or_else(|| "Mock task failed".to_string()))
            } else {
                Ok(mock_result.result.clone())
            }
        } else {
            Err(format!("No mock result registered for task: {}", task_name))
        }
    }

    /// Get execution count for a task
    pub fn execution_count(&self, task_name: &str) -> usize {
        self.execution_history
            .iter()
            .filter(|(name, _)| name == task_name)
            .count()
    }

    /// Clear execution history
    pub fn clear_history(&mut self) {
        self.execution_history.clear();
    }
}

impl Default for MockTaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Test data injector for workflow testing
#[derive(Debug, Clone)]
pub struct TestDataInjector {
    /// Injected data by key
    pub data: HashMap<String, serde_json::Value>,
}

impl TestDataInjector {
    /// Create a new test data injector
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Inject test data
    pub fn inject(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.data.insert(key.into(), value);
    }

    /// Get injected data
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.data.get(key)
    }

    /// Clear all injected data
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for TestDataInjector {
    fn default() -> Self {
        Self::new()
    }
}
