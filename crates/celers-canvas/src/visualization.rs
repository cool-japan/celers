use crate::{Chain, Chord, Group, WorkflowCheckpoint, WorkflowEvent, WorkflowState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Workflow execution snapshot for time-travel debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSnapshot {
    /// Snapshot ID
    pub snapshot_id: Uuid,
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Snapshot timestamp
    pub timestamp: u64,
    /// Workflow state at snapshot
    pub state: WorkflowState,
    /// Completed task IDs
    pub completed_tasks: Vec<Uuid>,
    /// Task results
    pub task_results: HashMap<Uuid, serde_json::Value>,
    /// Checkpoint data
    pub checkpoint: Option<WorkflowCheckpoint>,
}

impl WorkflowSnapshot {
    /// Create a new workflow snapshot
    pub fn new(workflow_id: Uuid, state: WorkflowState) -> Self {
        Self {
            snapshot_id: Uuid::new_v4(),
            workflow_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            state,
            completed_tasks: Vec::new(),
            task_results: HashMap::new(),
            checkpoint: None,
        }
    }

    /// Record task completion
    pub fn record_task(&mut self, task_id: Uuid, result: serde_json::Value) {
        self.completed_tasks.push(task_id);
        self.task_results.insert(task_id, result);
    }

    /// Attach checkpoint
    pub fn with_checkpoint(mut self, checkpoint: WorkflowCheckpoint) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }
}

/// Time-travel debugger for workflow replay
#[derive(Debug, Clone)]
pub struct TimeTravelDebugger {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Snapshots history
    pub snapshots: Vec<WorkflowSnapshot>,
    /// Current snapshot index
    pub current_index: usize,
    /// Step mode enabled
    pub step_mode: bool,
}

impl TimeTravelDebugger {
    /// Create a new time-travel debugger
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            snapshots: Vec::new(),
            current_index: 0,
            step_mode: false,
        }
    }

    /// Record a snapshot
    pub fn record_snapshot(&mut self, snapshot: WorkflowSnapshot) {
        self.snapshots.push(snapshot);
        self.current_index = self.snapshots.len() - 1;
    }

    /// Replay from a specific snapshot
    pub fn replay_from(&mut self, snapshot_index: usize) -> Option<&WorkflowSnapshot> {
        if snapshot_index < self.snapshots.len() {
            self.current_index = snapshot_index;
            self.snapshots.get(snapshot_index)
        } else {
            None
        }
    }

    /// Step forward one snapshot
    pub fn step_forward(&mut self) -> Option<&WorkflowSnapshot> {
        if self.current_index + 1 < self.snapshots.len() {
            self.current_index += 1;
            self.snapshots.get(self.current_index)
        } else {
            None
        }
    }

    /// Step backward one snapshot
    pub fn step_backward(&mut self) -> Option<&WorkflowSnapshot> {
        if self.current_index > 0 {
            self.current_index -= 1;
            self.snapshots.get(self.current_index)
        } else {
            None
        }
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&WorkflowSnapshot> {
        self.snapshots.get(self.current_index)
    }

    /// Enable step-by-step execution mode
    pub fn enable_step_mode(&mut self) {
        self.step_mode = true;
    }

    /// Disable step-by-step execution mode
    pub fn disable_step_mode(&mut self) {
        self.step_mode = false;
    }

    /// Get snapshot count
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    /// Clear all snapshots
    pub fn clear(&mut self) {
        self.snapshots.clear();
        self.current_index = 0;
    }
}

impl std::fmt::Display for TimeTravelDebugger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TimeTravelDebugger[workflow={}, snapshots={}, current={}]",
            self.workflow_id,
            self.snapshots.len(),
            self.current_index
        )
    }
}

// ============================================================================
// Workflow Visualization Support
// ============================================================================

/// Visual theme for workflow visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualTheme {
    /// Theme name
    pub name: String,
    /// Colors for task states
    pub colors: HashMap<String, String>,
    /// Node shapes by task type
    pub shapes: HashMap<String, String>,
    /// Edge styles
    pub edge_styles: HashMap<String, String>,
    /// Font settings
    pub font_family: String,
    pub font_size: u8,
}

impl VisualTheme {
    /// Create default light theme
    pub fn light() -> Self {
        let mut colors = HashMap::new();
        colors.insert("pending".to_string(), "#E0E0E0".to_string());
        colors.insert("running".to_string(), "#2196F3".to_string());
        colors.insert("completed".to_string(), "#4CAF50".to_string());
        colors.insert("failed".to_string(), "#F44336".to_string());
        colors.insert("cancelled".to_string(), "#FF9800".to_string());

        let mut shapes = HashMap::new();
        shapes.insert("task".to_string(), "box".to_string());
        shapes.insert("group".to_string(), "ellipse".to_string());
        shapes.insert("chord".to_string(), "diamond".to_string());

        let mut edge_styles = HashMap::new();
        edge_styles.insert("chain".to_string(), "solid".to_string());
        edge_styles.insert("callback".to_string(), "dashed".to_string());
        edge_styles.insert("error".to_string(), "dotted".to_string());

        Self {
            name: "light".to_string(),
            colors,
            shapes,
            edge_styles,
            font_family: "Arial".to_string(),
            font_size: 12,
        }
    }

    /// Create dark theme
    pub fn dark() -> Self {
        let mut colors = HashMap::new();
        colors.insert("pending".to_string(), "#424242".to_string());
        colors.insert("running".to_string(), "#1976D2".to_string());
        colors.insert("completed".to_string(), "#388E3C".to_string());
        colors.insert("failed".to_string(), "#D32F2F".to_string());
        colors.insert("cancelled".to_string(), "#F57C00".to_string());

        let mut shapes = HashMap::new();
        shapes.insert("task".to_string(), "box".to_string());
        shapes.insert("group".to_string(), "ellipse".to_string());
        shapes.insert("chord".to_string(), "diamond".to_string());

        let mut edge_styles = HashMap::new();
        edge_styles.insert("chain".to_string(), "solid".to_string());
        edge_styles.insert("callback".to_string(), "dashed".to_string());
        edge_styles.insert("error".to_string(), "dotted".to_string());

        Self {
            name: "dark".to_string(),
            colors,
            shapes,
            edge_styles,
            font_family: "Arial".to_string(),
            font_size: 12,
        }
    }

    /// Get color for state
    pub fn color_for_state(&self, state: &str) -> Option<&str> {
        self.colors.get(state).map(|s| s.as_str())
    }

    /// Get shape for task type
    pub fn shape_for_type(&self, task_type: &str) -> Option<&str> {
        self.shapes.get(task_type).map(|s| s.as_str())
    }
}

impl Default for VisualTheme {
    fn default() -> Self {
        Self::light()
    }
}

/// Task visual metadata for UI rendering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskVisualMetadata {
    /// Task ID
    pub task_id: Uuid,
    /// Task name
    pub task_name: String,
    /// Current state (pending, running, completed, failed, cancelled)
    pub state: String,
    /// Progress percentage (0-100)
    pub progress: f64,
    /// Visual position (x, y) for layout
    pub position: Option<(f64, f64)>,
    /// Node color (CSS color)
    pub color: String,
    /// Node shape
    pub shape: String,
    /// CSS classes for styling
    pub css_classes: Vec<String>,
    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl TaskVisualMetadata {
    /// Create new task visual metadata
    pub fn new(task_id: Uuid, task_name: String, state: String) -> Self {
        Self {
            task_id,
            task_name,
            state: state.clone(),
            progress: 0.0,
            position: None,
            color: Self::default_color_for_state(&state),
            shape: "box".to_string(),
            css_classes: vec![format!("task-{}", state)],
            metadata: HashMap::new(),
        }
    }

    /// Default color for state
    fn default_color_for_state(state: &str) -> String {
        match state {
            "pending" => "#E0E0E0",
            "running" => "#2196F3",
            "completed" => "#4CAF50",
            "failed" => "#F44336",
            "cancelled" => "#FF9800",
            _ => "#9E9E9E",
        }
        .to_string()
    }

    /// Set progress
    pub fn with_progress(mut self, progress: f64) -> Self {
        self.progress = progress.clamp(0.0, 100.0);
        self
    }

    /// Set position
    pub fn with_position(mut self, x: f64, y: f64) -> Self {
        self.position = Some((x, y));
        self
    }

    /// Set color
    pub fn with_color(mut self, color: String) -> Self {
        self.color = color;
        self
    }

    /// Add CSS class
    pub fn add_css_class(&mut self, class: String) {
        if !self.css_classes.contains(&class) {
            self.css_classes.push(class);
        }
    }

    /// Add metadata
    pub fn add_metadata(&mut self, key: String, value: serde_json::Value) {
        self.metadata.insert(key, value);
    }
}

/// Workflow visualization data with rich metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowVisualizationData {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Workflow name
    pub workflow_name: String,
    /// Workflow state
    pub state: WorkflowState,
    /// Task visual metadata
    pub tasks: Vec<TaskVisualMetadata>,
    /// Edge connections (from_task_id, to_task_id, edge_type)
    pub edges: Vec<(Uuid, Uuid, String)>,
    /// Visual theme
    pub theme: VisualTheme,
    /// Layout algorithm hint (hierarchical, force, circular, etc.)
    pub layout_hint: String,
    /// Viewport dimensions (width, height)
    pub viewport: (f64, f64),
}

impl WorkflowVisualizationData {
    /// Create new visualization data
    pub fn new(workflow_id: Uuid, workflow_name: String, state: WorkflowState) -> Self {
        Self {
            workflow_id,
            workflow_name,
            state,
            tasks: Vec::new(),
            edges: Vec::new(),
            theme: VisualTheme::default(),
            layout_hint: "hierarchical".to_string(),
            viewport: (1000.0, 600.0),
        }
    }

    /// Add task metadata
    pub fn add_task(&mut self, task: TaskVisualMetadata) {
        self.tasks.push(task);
    }

    /// Add edge
    pub fn add_edge(&mut self, from: Uuid, to: Uuid, edge_type: String) {
        self.edges.push((from, to, edge_type));
    }

    /// Set theme
    pub fn with_theme(mut self, theme: VisualTheme) -> Self {
        self.theme = theme;
        self
    }

    /// Set layout hint
    pub fn with_layout(mut self, layout_hint: String) -> Self {
        self.layout_hint = layout_hint;
        self
    }

    /// Export to JSON for frontend
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Export to vis.js format
    pub fn to_visjs_format(&self) -> serde_json::Value {
        let nodes: Vec<serde_json::Value> = self
            .tasks
            .iter()
            .map(|task| {
                serde_json::json!({
                    "id": task.task_id.to_string(),
                    "label": task.task_name,
                    "color": task.color,
                    "shape": task.shape,
                    "title": format!("{} ({})", task.task_name, task.state),
                    "value": task.progress,
                })
            })
            .collect();

        let edges: Vec<serde_json::Value> = self
            .edges
            .iter()
            .map(|(from, to, edge_type)| {
                serde_json::json!({
                    "from": from.to_string(),
                    "to": to.to_string(),
                    "arrows": "to",
                    "dashes": edge_type == "callback",
                })
            })
            .collect();

        serde_json::json!({
            "nodes": nodes,
            "edges": edges,
        })
    }
}

/// Execution timeline entry for Gantt chart visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    /// Task ID
    pub task_id: Uuid,
    /// Task name
    pub task_name: String,
    /// Start time (Unix timestamp in milliseconds)
    pub start_time: u64,
    /// End time (Unix timestamp in milliseconds)
    pub end_time: Option<u64>,
    /// Duration in milliseconds
    pub duration: Option<u64>,
    /// Task state
    pub state: String,
    /// Worker ID that executed the task
    pub worker_id: Option<String>,
    /// Parent task ID (for nested workflows)
    pub parent_id: Option<Uuid>,
    /// Color for visualization
    pub color: String,
}

impl TimelineEntry {
    /// Create new timeline entry
    pub fn new(task_id: Uuid, task_name: String, start_time: u64) -> Self {
        Self {
            task_id,
            task_name,
            start_time,
            end_time: None,
            duration: None,
            state: "running".to_string(),
            worker_id: None,
            parent_id: None,
            color: "#2196F3".to_string(),
        }
    }

    /// Mark task as completed
    pub fn complete(&mut self, end_time: u64) {
        self.end_time = Some(end_time);
        self.duration = Some(end_time.saturating_sub(self.start_time));
        self.state = "completed".to_string();
        self.color = "#4CAF50".to_string();
    }

    /// Mark task as failed
    pub fn fail(&mut self, end_time: u64) {
        self.end_time = Some(end_time);
        self.duration = Some(end_time.saturating_sub(self.start_time));
        self.state = "failed".to_string();
        self.color = "#F44336".to_string();
    }

    /// Set worker ID
    pub fn with_worker(mut self, worker_id: String) -> Self {
        self.worker_id = Some(worker_id);
        self
    }

    /// Set parent ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

/// Execution timeline for workflow visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTimeline {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Timeline entries
    pub entries: Vec<TimelineEntry>,
    /// Workflow start time
    pub workflow_start: u64,
    /// Workflow end time
    pub workflow_end: Option<u64>,
}

impl ExecutionTimeline {
    /// Create new execution timeline
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            entries: Vec::new(),
            workflow_start: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            workflow_end: None,
        }
    }

    /// Add timeline entry
    pub fn add_entry(&mut self, entry: TimelineEntry) {
        self.entries.push(entry);
    }

    /// Start task
    pub fn start_task(&mut self, task_id: Uuid, task_name: String) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let entry = TimelineEntry::new(task_id, task_name, now);
        self.entries.push(entry);
        self.entries.len() - 1
    }

    /// Complete task
    pub fn complete_task(&mut self, index: usize) {
        if let Some(entry) = self.entries.get_mut(index) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            entry.complete(now);
        }
    }

    /// Fail task
    pub fn fail_task(&mut self, index: usize) {
        if let Some(entry) = self.entries.get_mut(index) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            entry.fail(now);
        }
    }

    /// Mark workflow as complete
    pub fn complete_workflow(&mut self) {
        self.workflow_end = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
    }

    /// Export to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Export to Google Charts Timeline format
    pub fn to_google_charts_format(&self) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = self
            .entries
            .iter()
            .map(|entry| {
                serde_json::json!([
                    entry.task_name,
                    entry.task_name,
                    entry.start_time,
                    entry.end_time.unwrap_or(entry.start_time),
                ])
            })
            .collect();

        serde_json::json!({
            "cols": [
                {"id": "", "label": "Task ID", "type": "string"},
                {"id": "", "label": "Task Name", "type": "string"},
                {"id": "", "label": "Start", "type": "number"},
                {"id": "", "label": "End", "type": "number"}
            ],
            "rows": rows.iter().map(|row| serde_json::json!({"c": row})).collect::<Vec<_>>()
        })
    }
}

/// Animation frame for execution visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnimationFrame {
    /// Frame number
    pub frame_number: usize,
    /// Timestamp
    pub timestamp: u64,
    /// Workflow state at this frame
    pub workflow_state: WorkflowState,
    /// Task states (task_id -> state)
    pub task_states: HashMap<Uuid, String>,
    /// Active tasks at this frame
    pub active_tasks: Vec<Uuid>,
    /// Completed tasks at this frame
    pub completed_tasks: Vec<Uuid>,
    /// Events that occurred in this frame
    pub events: Vec<WorkflowEvent>,
}

impl AnimationFrame {
    /// Create new animation frame
    pub fn new(frame_number: usize, workflow_state: WorkflowState) -> Self {
        Self {
            frame_number,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            workflow_state,
            task_states: HashMap::new(),
            active_tasks: Vec::new(),
            completed_tasks: Vec::new(),
            events: Vec::new(),
        }
    }

    /// Set task state
    pub fn set_task_state(&mut self, task_id: Uuid, state: String) {
        self.task_states.insert(task_id, state);
    }

    /// Add active task
    pub fn add_active_task(&mut self, task_id: Uuid) {
        if !self.active_tasks.contains(&task_id) {
            self.active_tasks.push(task_id);
        }
    }

    /// Add completed task
    pub fn add_completed_task(&mut self, task_id: Uuid) {
        if !self.completed_tasks.contains(&task_id) {
            self.completed_tasks.push(task_id);
        }
        // Remove from active tasks
        self.active_tasks.retain(|id| id != &task_id);
    }

    /// Add event
    pub fn add_event(&mut self, event: WorkflowEvent) {
        self.events.push(event);
    }
}

/// Workflow animation sequence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowAnimation {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Animation frames
    pub frames: Vec<AnimationFrame>,
    /// Frame duration in milliseconds
    pub frame_duration: u64,
    /// Total duration
    pub total_duration: u64,
}

impl WorkflowAnimation {
    /// Create new workflow animation
    pub fn new(workflow_id: Uuid, frame_duration: u64) -> Self {
        Self {
            workflow_id,
            frames: Vec::new(),
            frame_duration,
            total_duration: 0,
        }
    }

    /// Add frame
    pub fn add_frame(&mut self, frame: AnimationFrame) {
        self.frames.push(frame);
        self.total_duration = self.frames.len() as u64 * self.frame_duration;
    }

    /// Get frame at index
    pub fn get_frame(&self, index: usize) -> Option<&AnimationFrame> {
        self.frames.get(index)
    }

    /// Get frame count
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Export to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// DAG export with execution state overlay
pub trait DagExportWithState {
    /// Export DAG with current execution state
    fn to_dot_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String;

    /// Export Mermaid diagram with execution state
    fn to_mermaid_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String;

    /// Export to JSON with execution state
    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error>;
}

impl DagExportWithState for Chain {
    fn to_dot_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut dot = String::from("digraph Chain {\n");
        dot.push_str("  rankdir=LR;\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let color = match state {
                "completed" => "#4CAF50",
                "running" => "#2196F3",
                "failed" => "#F44336",
                _ => "#E0E0E0",
            };

            dot.push_str(&format!(
                "  task{} [label=\"{}\" style=filled fillcolor=\"{}\"];\n",
                i, sig.task, color
            ));

            if i > 0 {
                dot.push_str(&format!("  task{} -> task{};\n", i - 1, i));
            }
        }

        dot.push('}');
        dot
    }

    fn to_mermaid_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut mmd = String::from("graph LR\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let style_class = match state {
                "completed" => "completed",
                "running" => "running",
                "failed" => "failed",
                _ => "pending",
            };

            mmd.push_str(&format!(
                "  task{}[\"{}\"]:::{}\n",
                i, sig.task, style_class
            ));

            if i > 0 {
                mmd.push_str(&format!("  task{} --> task{}\n", i - 1, i));
            }
        }

        // Add style definitions
        mmd.push_str("\n  classDef completed fill:#4CAF50,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef running fill:#2196F3,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef failed fill:#F44336,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef pending fill:#E0E0E0,stroke:#333,stroke-width:2px\n");

        mmd
    }

    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let task_state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");

            nodes.push(serde_json::json!({
                "id": format!("task{}", i),
                "label": sig.task,
                "state": task_state,
                "task_id": task_id,
            }));

            if i > 0 {
                edges.push(serde_json::json!({
                    "from": format!("task{}", i - 1),
                    "to": format!("task{}", i),
                }));
            }
        }

        let result = serde_json::json!({
            "type": "chain",
            "workflow_state": state,
            "nodes": nodes,
            "edges": edges,
        });

        serde_json::to_string_pretty(&result)
    }
}

impl DagExportWithState for Group {
    fn to_dot_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut dot = String::from("digraph Group {\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let color = match state {
                "completed" => "#4CAF50",
                "running" => "#2196F3",
                "failed" => "#F44336",
                _ => "#E0E0E0",
            };

            dot.push_str(&format!(
                "  task{} [label=\"{}\" style=filled fillcolor=\"{}\"];\n",
                i, sig.task, color
            ));
        }

        dot.push('}');
        dot
    }

    fn to_mermaid_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut mmd = String::from("graph TB\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let style_class = match state {
                "completed" => "completed",
                "running" => "running",
                "failed" => "failed",
                _ => "pending",
            };

            mmd.push_str(&format!(
                "  task{}[\"{}\"]:::{}\n",
                i, sig.task, style_class
            ));
        }

        // Add style definitions
        mmd.push_str("\n  classDef completed fill:#4CAF50,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef running fill:#2196F3,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef failed fill:#F44336,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef pending fill:#E0E0E0,stroke:#333,stroke-width:2px\n");

        mmd
    }

    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error> {
        let mut nodes = Vec::new();

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let task_state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");

            nodes.push(serde_json::json!({
                "id": format!("task{}", i),
                "label": sig.task,
                "state": task_state,
                "task_id": task_id,
            }));
        }

        let result = serde_json::json!({
            "type": "group",
            "workflow_state": state,
            "nodes": nodes,
            "edges": [],
        });

        serde_json::to_string_pretty(&result)
    }
}

impl DagExportWithState for Chord {
    fn to_dot_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut dot = String::from("digraph Chord {\n");
        dot.push_str("  rankdir=LR;\n");

        // Header tasks
        for (i, sig) in self.header.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let color = match state {
                "completed" => "#4CAF50",
                "running" => "#2196F3",
                "failed" => "#F44336",
                _ => "#E0E0E0",
            };

            dot.push_str(&format!(
                "  task{} [label=\"{}\" style=filled fillcolor=\"{}\"];\n",
                i, sig.task, color
            ));
        }

        // Body (callback)
        let task_id = self.body.options.task_id.unwrap_or_else(Uuid::new_v4);
        let state = task_states
            .get(&task_id)
            .map(|s| s.as_str())
            .unwrap_or("pending");
        let color = match state {
            "completed" => "#4CAF50",
            "running" => "#2196F3",
            "failed" => "#F44336",
            _ => "#E0E0E0",
        };

        dot.push_str(&format!(
            "  callback [label=\"{}\" shape=diamond style=filled fillcolor=\"{}\"];\n",
            self.body.task, color
        ));

        for i in 0..self.header.tasks.len() {
            dot.push_str(&format!("  task{} -> callback;\n", i));
        }

        dot.push('}');
        dot
    }

    fn to_mermaid_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut mmd = String::from("graph TB\n");

        // Header tasks
        for (i, sig) in self.header.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let style_class = match state {
                "completed" => "completed",
                "running" => "running",
                "failed" => "failed",
                _ => "pending",
            };

            mmd.push_str(&format!(
                "  task{}[\"{}\"]:::{}\n",
                i, sig.task, style_class
            ));
        }

        // Body (callback)
        let task_id = self.body.options.task_id.unwrap_or_else(Uuid::new_v4);
        let state = task_states
            .get(&task_id)
            .map(|s| s.as_str())
            .unwrap_or("pending");
        let style_class = match state {
            "completed" => "completed",
            "running" => "running",
            "failed" => "failed",
            _ => "pending",
        };

        mmd.push_str(&format!(
            "  callback{{\"{}\"}}:::{}\n",
            self.body.task, style_class
        ));

        for i in 0..self.header.tasks.len() {
            mmd.push_str(&format!("  task{} --> callback\n", i));
        }

        // Add style definitions
        mmd.push_str("\n  classDef completed fill:#4CAF50,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef running fill:#2196F3,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef failed fill:#F44336,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef pending fill:#E0E0E0,stroke:#333,stroke-width:2px\n");

        mmd
    }

    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Header tasks
        for (i, sig) in self.header.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let task_state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");

            nodes.push(serde_json::json!({
                "id": format!("task{}", i),
                "label": sig.task,
                "state": task_state,
                "task_id": task_id,
            }));
        }

        // Body (callback)
        let task_id = self.body.options.task_id.unwrap_or_else(Uuid::new_v4);
        let task_state = task_states
            .get(&task_id)
            .map(|s| s.as_str())
            .unwrap_or("pending");

        nodes.push(serde_json::json!({
            "id": "callback",
            "label": self.body.task,
            "state": task_state,
            "task_id": task_id,
            "shape": "diamond",
        }));

        for i in 0..self.header.tasks.len() {
            edges.push(serde_json::json!({
                "from": format!("task{}", i),
                "to": "callback",
            }));
        }

        let result = serde_json::json!({
            "type": "chord",
            "workflow_state": state,
            "nodes": nodes,
            "edges": edges,
        });

        serde_json::to_string_pretty(&result)
    }
}
