use crate::{Chain, Group, Map, Signature, WorkflowEvent, WorkflowStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Real-time workflow event stream
#[derive(Debug, Clone)]
pub struct WorkflowEventStream {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Event buffer
    pub events: Vec<(u64, WorkflowEvent)>,
    /// Maximum buffer size
    pub max_buffer_size: usize,
}

impl WorkflowEventStream {
    /// Create new event stream
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            events: Vec::new(),
            max_buffer_size: 1000,
        }
    }

    /// Set maximum buffer size
    pub fn with_max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// Push event
    pub fn push(&mut self, event: WorkflowEvent) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.events.push((timestamp, event));

        // Trim buffer if needed
        if self.events.len() > self.max_buffer_size {
            self.events.remove(0);
        }
    }

    /// Get events since timestamp
    pub fn events_since(&self, timestamp: u64) -> Vec<&(u64, WorkflowEvent)> {
        self.events
            .iter()
            .filter(|(ts, _)| *ts > timestamp)
            .collect()
    }

    /// Get all events
    pub fn all_events(&self) -> &[(u64, WorkflowEvent)] {
        &self.events
    }

    /// Clear events
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Export to JSON for Server-Sent Events (SSE)
    pub fn to_sse_format(&self) -> Vec<String> {
        self.events
            .iter()
            .map(|(ts, event)| {
                format!(
                    "event: workflow\ndata: {{\"timestamp\": {}, \"event\": \"{}\"}}\n\n",
                    ts, event
                )
            })
            .collect()
    }
}

// ============================================================================
// Production-Ready Enhancements
// ============================================================================

/// Workflow metrics collector for automatic metrics gathering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowMetricsCollector {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Start time
    pub start_time: u64,
    /// End time
    pub end_time: Option<u64>,
    /// Total tasks
    pub total_tasks: usize,
    /// Completed tasks
    pub completed_tasks: usize,
    /// Failed tasks
    pub failed_tasks: usize,
    /// Task execution times (task_id -> duration_ms)
    pub task_durations: HashMap<Uuid, u64>,
    /// Task retry counts (task_id -> retry_count)
    pub task_retries: HashMap<Uuid, usize>,
    /// Total workflow duration in milliseconds
    pub total_duration: Option<u64>,
    /// Average task duration
    pub avg_task_duration: Option<f64>,
    /// Success rate (0.0 to 1.0)
    pub success_rate: Option<f64>,
}

impl WorkflowMetricsCollector {
    /// Create new metrics collector
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            end_time: None,
            total_tasks: 0,
            completed_tasks: 0,
            failed_tasks: 0,
            task_durations: HashMap::new(),
            task_retries: HashMap::new(),
            total_duration: None,
            avg_task_duration: None,
            success_rate: None,
        }
    }

    /// Record task start
    pub fn record_task_start(&mut self, task_id: Uuid) {
        self.total_tasks += 1;
        self.task_durations.insert(task_id, 0);
    }

    /// Record task completion
    pub fn record_task_complete(&mut self, task_id: Uuid, duration_ms: u64) {
        self.completed_tasks += 1;
        self.task_durations.insert(task_id, duration_ms);
    }

    /// Record task failure
    pub fn record_task_failure(&mut self, task_id: Uuid, duration_ms: u64) {
        self.failed_tasks += 1;
        self.task_durations.insert(task_id, duration_ms);
    }

    /// Record task retry
    pub fn record_task_retry(&mut self, task_id: Uuid) {
        *self.task_retries.entry(task_id).or_insert(0) += 1;
    }

    /// Finalize metrics
    pub fn finalize(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.end_time = Some(now);
        self.total_duration = Some(now.saturating_sub(self.start_time));

        // Calculate average task duration
        if !self.task_durations.is_empty() {
            let sum: u64 = self.task_durations.values().sum();
            self.avg_task_duration = Some(sum as f64 / self.task_durations.len() as f64);
        }

        // Calculate success rate
        if self.total_tasks > 0 {
            self.success_rate = Some(self.completed_tasks as f64 / self.total_tasks as f64);
        }
    }

    /// Get metrics summary
    pub fn summary(&self) -> String {
        format!(
            "WorkflowMetrics[id={}, total={}, completed={}, failed={}, success_rate={:.2}%, avg_duration={:.2}ms]",
            self.workflow_id,
            self.total_tasks,
            self.completed_tasks,
            self.failed_tasks,
            self.success_rate.unwrap_or(0.0) * 100.0,
            self.avg_task_duration.unwrap_or(0.0)
        )
    }
}

impl std::fmt::Display for WorkflowMetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// Workflow rate limiter for controlling execution rate
#[derive(Debug, Clone)]
pub struct WorkflowRateLimiter {
    /// Maximum workflows per time window
    pub max_workflows: usize,
    /// Time window in milliseconds
    pub window_ms: u64,
    /// Workflow timestamps
    pub workflow_timestamps: Vec<u64>,
    /// Total workflows processed
    pub total_workflows: usize,
    /// Total workflows rejected
    pub rejected_workflows: usize,
}

impl WorkflowRateLimiter {
    /// Create new rate limiter
    pub fn new(max_workflows: usize, window_ms: u64) -> Self {
        Self {
            max_workflows,
            window_ms,
            workflow_timestamps: Vec::new(),
            total_workflows: 0,
            rejected_workflows: 0,
        }
    }

    /// Check if workflow can be executed
    pub fn allow_workflow(&mut self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Remove old timestamps outside the window
        self.workflow_timestamps
            .retain(|&ts| now.saturating_sub(ts) < self.window_ms);

        // Check if we can allow this workflow
        if self.workflow_timestamps.len() < self.max_workflows {
            self.workflow_timestamps.push(now);
            self.total_workflows += 1;
            true
        } else {
            self.rejected_workflows += 1;
            false
        }
    }

    /// Get current rate (workflows per second)
    pub fn current_rate(&self) -> f64 {
        if self.workflow_timestamps.is_empty() {
            return 0.0;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let active_timestamps: Vec<_> = self
            .workflow_timestamps
            .iter()
            .filter(|&&ts| now.saturating_sub(ts) < self.window_ms)
            .collect();

        if active_timestamps.is_empty() {
            return 0.0;
        }

        active_timestamps.len() as f64 / (self.window_ms as f64 / 1000.0)
    }

    /// Reset rate limiter
    pub fn reset(&mut self) {
        self.workflow_timestamps.clear();
    }

    /// Get rejection rate
    pub fn rejection_rate(&self) -> f64 {
        if self.total_workflows == 0 {
            0.0
        } else {
            self.rejected_workflows as f64 / self.total_workflows as f64
        }
    }
}

impl std::fmt::Display for WorkflowRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RateLimiter[max={}/{}ms, current_rate={:.2}/s, rejected={}]",
            self.max_workflows,
            self.window_ms,
            self.current_rate(),
            self.rejected_workflows
        )
    }
}

/// Workflow concurrency control for limiting concurrent workflows
#[derive(Debug, Clone)]
pub struct WorkflowConcurrencyControl {
    /// Maximum concurrent workflows
    pub max_concurrent: usize,
    /// Currently active workflows
    pub active_workflows: HashMap<Uuid, u64>,
    /// Total workflows started
    pub total_started: usize,
    /// Total workflows completed
    pub total_completed: usize,
    /// Peak concurrency reached
    pub peak_concurrency: usize,
}

impl WorkflowConcurrencyControl {
    /// Create new concurrency control
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            active_workflows: HashMap::new(),
            total_started: 0,
            total_completed: 0,
            peak_concurrency: 0,
        }
    }

    /// Try to start a workflow
    pub fn try_start(&mut self, workflow_id: Uuid) -> bool {
        if self.active_workflows.len() >= self.max_concurrent {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.active_workflows.insert(workflow_id, now);
        self.total_started += 1;

        // Update peak concurrency
        if self.active_workflows.len() > self.peak_concurrency {
            self.peak_concurrency = self.active_workflows.len();
        }

        true
    }

    /// Complete a workflow
    pub fn complete(&mut self, workflow_id: Uuid) -> bool {
        if self.active_workflows.remove(&workflow_id).is_some() {
            self.total_completed += 1;
            true
        } else {
            false
        }
    }

    /// Get current concurrency
    pub fn current_concurrency(&self) -> usize {
        self.active_workflows.len()
    }

    /// Get available slots
    pub fn available_slots(&self) -> usize {
        self.max_concurrent
            .saturating_sub(self.active_workflows.len())
    }

    /// Check if at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.active_workflows.len() >= self.max_concurrent
    }

    /// Get average workflow duration
    pub fn avg_workflow_duration(&self) -> Option<f64> {
        if self.total_completed == 0 {
            return None;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let total_duration: u64 = self
            .active_workflows
            .values()
            .map(|&start_time| now.saturating_sub(start_time))
            .sum();

        Some(total_duration as f64 / self.total_completed as f64)
    }
}

impl std::fmt::Display for WorkflowConcurrencyControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConcurrencyControl[max={}, active={}, peak={}, available={}]",
            self.max_concurrent,
            self.current_concurrency(),
            self.peak_concurrency,
            self.available_slots()
        )
    }
}

/// Workflow composition helpers for easier workflow building
#[derive(Debug, Clone)]
pub struct WorkflowBuilder {
    /// Workflow name
    pub name: String,
    /// Workflow description
    pub description: Option<String>,
    /// Workflow tags
    pub tags: Vec<String>,
    /// Workflow metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl WorkflowBuilder {
    /// Create new workflow builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            tags: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add tag
    pub fn add_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add metadata
    pub fn add_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Build a chain workflow
    pub fn chain(self) -> Chain {
        Chain::new()
    }

    /// Build a group workflow
    pub fn group(self) -> Group {
        Group::new()
    }

    /// Build a map workflow
    pub fn map(self, task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Map {
        Map::new(task, argsets)
    }
}

/// Workflow registry for tracking and managing workflows
#[derive(Debug, Clone)]
pub struct WorkflowRegistry {
    /// Registered workflows (workflow_id -> workflow_name)
    pub workflows: HashMap<Uuid, String>,
    /// Workflow metadata
    pub metadata: HashMap<Uuid, HashMap<String, serde_json::Value>>,
    /// Workflow states
    pub states: HashMap<Uuid, WorkflowStatus>,
    /// Workflow start times
    pub start_times: HashMap<Uuid, u64>,
    /// Workflow tags
    pub tags: HashMap<String, Vec<Uuid>>,
}

impl WorkflowRegistry {
    /// Create new workflow registry
    pub fn new() -> Self {
        Self {
            workflows: HashMap::new(),
            metadata: HashMap::new(),
            states: HashMap::new(),
            start_times: HashMap::new(),
            tags: HashMap::new(),
        }
    }

    /// Register a workflow
    pub fn register(
        &mut self,
        workflow_id: Uuid,
        name: String,
        metadata: HashMap<String, serde_json::Value>,
    ) {
        self.workflows.insert(workflow_id, name);
        self.metadata.insert(workflow_id, metadata);
        self.states.insert(workflow_id, WorkflowStatus::Pending);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.start_times.insert(workflow_id, now);
    }

    /// Update workflow state
    pub fn update_state(&mut self, workflow_id: Uuid, state: WorkflowStatus) {
        self.states.insert(workflow_id, state);
    }

    /// Add tag to workflow
    pub fn add_tag(&mut self, workflow_id: Uuid, tag: String) {
        self.tags.entry(tag).or_default().push(workflow_id);
    }

    /// Get workflows by tag
    pub fn get_by_tag(&self, tag: &str) -> Vec<Uuid> {
        self.tags.get(tag).cloned().unwrap_or_default()
    }

    /// Get workflow state
    pub fn get_state(&self, workflow_id: &Uuid) -> Option<&WorkflowStatus> {
        self.states.get(workflow_id)
    }

    /// Get workflow name
    pub fn get_name(&self, workflow_id: &Uuid) -> Option<&str> {
        self.workflows.get(workflow_id).map(|s| s.as_str())
    }

    /// Get workflow metadata
    pub fn get_metadata(&self, workflow_id: &Uuid) -> Option<&HashMap<String, serde_json::Value>> {
        self.metadata.get(workflow_id)
    }

    /// Remove workflow
    pub fn remove(&mut self, workflow_id: &Uuid) -> bool {
        let removed = self.workflows.remove(workflow_id).is_some();
        self.metadata.remove(workflow_id);
        self.states.remove(workflow_id);
        self.start_times.remove(workflow_id);

        // Remove from tags
        for workflows in self.tags.values_mut() {
            workflows.retain(|id| id != workflow_id);
        }

        removed
    }

    /// Get workflow count
    pub fn count(&self) -> usize {
        self.workflows.len()
    }

    /// Get workflows by state
    pub fn get_by_state(&self, state: &WorkflowStatus) -> Vec<Uuid> {
        self.states
            .iter()
            .filter(|(_, s)| *s == state)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Clear all workflows
    pub fn clear(&mut self) {
        self.workflows.clear();
        self.metadata.clear();
        self.states.clear();
        self.start_times.clear();
        self.tags.clear();
    }

    /// Get all workflow IDs
    pub fn all_workflow_ids(&self) -> Vec<Uuid> {
        self.workflows.keys().copied().collect()
    }

    /// Get workflows by name pattern (contains)
    pub fn find_by_name(&self, pattern: &str) -> Vec<Uuid> {
        self.workflows
            .iter()
            .filter(|(_, name)| name.contains(pattern))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get workflows older than duration (in milliseconds)
    pub fn get_older_than(&self, duration_ms: u64) -> Vec<Uuid> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.start_times
            .iter()
            .filter(|(_, &start_time)| now.saturating_sub(start_time) > duration_ms)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get workflow age in milliseconds
    pub fn get_age(&self, workflow_id: &Uuid) -> Option<u64> {
        self.start_times.get(workflow_id).map(|&start_time| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            now.saturating_sub(start_time)
        })
    }

    /// Check if workflow exists
    pub fn contains(&self, workflow_id: &Uuid) -> bool {
        self.workflows.contains_key(workflow_id)
    }

    /// Get all tags
    pub fn all_tags(&self) -> Vec<String> {
        self.tags.keys().cloned().collect()
    }

    /// Get workflows with multiple tags (all tags must match)
    pub fn get_by_tags_all(&self, tags: &[&str]) -> Vec<Uuid> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut result: Option<Vec<Uuid>> = None;

        for tag in tags {
            let tagged = self.get_by_tag(tag);
            result = match result {
                None => Some(tagged),
                Some(current) => {
                    // Intersection
                    Some(
                        current
                            .into_iter()
                            .filter(|id| tagged.contains(id))
                            .collect(),
                    )
                }
            };
        }

        result.unwrap_or_default()
    }

    /// Get workflows with any of the tags (OR operation)
    pub fn get_by_tags_any(&self, tags: &[&str]) -> Vec<Uuid> {
        let mut result = Vec::new();
        for tag in tags {
            result.extend(self.get_by_tag(tag));
        }
        // Remove duplicates
        result.sort();
        result.dedup();
        result
    }

    /// Get count by state
    pub fn count_by_state(&self, state: &WorkflowStatus) -> usize {
        self.states.iter().filter(|(_, s)| *s == state).count()
    }

    /// Get running workflows count
    pub fn running_count(&self) -> usize {
        self.count_by_state(&WorkflowStatus::Running)
    }

    /// Get pending workflows count
    pub fn pending_count(&self) -> usize {
        self.count_by_state(&WorkflowStatus::Pending)
    }

    /// Get completed workflows count (success + failed)
    pub fn completed_count(&self) -> usize {
        self.count_by_state(&WorkflowStatus::Success) + self.count_by_state(&WorkflowStatus::Failed)
    }
}

impl Default for WorkflowRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowRegistry[total={}, pending={}, running={}, success={}, failed={}]",
            self.count(),
            self.get_by_state(&WorkflowStatus::Pending).len(),
            self.get_by_state(&WorkflowStatus::Running).len(),
            self.get_by_state(&WorkflowStatus::Success).len(),
            self.get_by_state(&WorkflowStatus::Failed).len()
        )
    }
}
