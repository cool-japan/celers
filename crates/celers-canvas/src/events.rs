use crate::Signature;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Workflow event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowEvent {
    /// Task completed
    TaskCompleted { task_id: Uuid },
    /// Task failed
    TaskFailed { task_id: Uuid, error: String },
    /// Workflow started
    WorkflowStarted { workflow_id: Uuid },
    /// Workflow completed
    WorkflowCompleted { workflow_id: Uuid },
    /// Workflow failed
    WorkflowFailed { workflow_id: Uuid, error: String },
    /// Custom event
    Custom { event_type: String, data: String },
}

impl std::fmt::Display for WorkflowEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TaskCompleted { task_id } => write!(f, "TaskCompleted[{}]", task_id),
            Self::TaskFailed { task_id, .. } => write!(f, "TaskFailed[{}]", task_id),
            Self::WorkflowStarted { workflow_id } => write!(f, "WorkflowStarted[{}]", workflow_id),
            Self::WorkflowCompleted { workflow_id } => {
                write!(f, "WorkflowCompleted[{}]", workflow_id)
            }
            Self::WorkflowFailed { workflow_id, .. } => {
                write!(f, "WorkflowFailed[{}]", workflow_id)
            }
            Self::Custom { event_type, .. } => write!(f, "Custom[{}]", event_type),
        }
    }
}

/// Event handler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHandler {
    /// Event type to handle
    pub event_type: String,
    /// Task to execute on event
    pub handler_task: Signature,
    /// Event filter (optional)
    pub filter: Option<String>,
}

impl EventHandler {
    /// Create a new event handler
    pub fn new(event_type: impl Into<String>, handler_task: Signature) -> Self {
        Self {
            event_type: event_type.into(),
            handler_task,
            filter: None,
        }
    }

    /// Set event filter
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

impl std::fmt::Display for EventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventHandler[event={}, handler={}]",
            self.event_type, self.handler_task.task
        )
    }
}

/// Event-driven workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDrivenWorkflow {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Event handlers
    pub handlers: Vec<EventHandler>,
    /// Whether workflow is active
    pub active: bool,
}

impl EventDrivenWorkflow {
    /// Create a new event-driven workflow
    pub fn new() -> Self {
        Self {
            workflow_id: Uuid::new_v4(),
            handlers: Vec::new(),
            active: true,
        }
    }

    /// Add an event handler
    pub fn on_event(mut self, handler: EventHandler) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Add handler for task completion
    pub fn on_task_completed(self, task: Signature) -> Self {
        self.on_event(EventHandler::new("TaskCompleted", task))
    }

    /// Add handler for task failure
    pub fn on_task_failed(self, task: Signature) -> Self {
        self.on_event(EventHandler::new("TaskFailed", task))
    }

    /// Activate workflow
    pub fn activate(mut self) -> Self {
        self.active = true;
        self
    }

    /// Deactivate workflow
    pub fn deactivate(mut self) -> Self {
        self.active = false;
        self
    }

    /// Check if workflow has handlers
    pub fn has_handlers(&self) -> bool {
        !self.handlers.is_empty()
    }
}

impl Default for EventDrivenWorkflow {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for EventDrivenWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventDrivenWorkflow[id={}, handlers={}]",
            self.workflow_id,
            self.handlers.len()
        )?;
        if !self.active {
            write!(f, " (inactive)")?;
        }
        Ok(())
    }
}
