//! Workflow types, task hooks, and builder patterns
//!
//! This module contains workflow orchestration types including DAG workflows,
//! stage-based execution, task lifecycle hooks, and builder patterns.

use celers_core::{Result, SerializedTask};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Workflow state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowState::Pending => write!(f, "pending"),
            WorkflowState::Running => write!(f, "running"),
            WorkflowState::Completed => write!(f, "completed"),
            WorkflowState::Failed => write!(f, "failed"),
            WorkflowState::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Workflow stage state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StageState {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

impl std::fmt::Display for StageState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageState::Pending => write!(f, "pending"),
            StageState::Running => write!(f, "running"),
            StageState::Completed => write!(f, "completed"),
            StageState::Failed => write!(f, "failed"),
            StageState::Skipped => write!(f, "skipped"),
        }
    }
}

/// Workflow definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: Uuid,
    pub workflow_name: String,
    pub state: WorkflowState,
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

/// Workflow stage for parallel execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStage {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub stage_number: i32,
    pub stage_name: String,
    pub state: StageState,
    pub task_count: i32,
    pub completed_count: i32,
    pub failed_count: i32,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Task dependency for DAG execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    pub id: Uuid,
    pub task_id: Uuid,
    pub parent_task_id: Uuid,
    pub workflow_id: Option<Uuid>,
    pub stage_id: Option<Uuid>,
    pub satisfied: bool,
    pub created_at: DateTime<Utc>,
    pub satisfied_at: Option<DateTime<Utc>>,
}

/// Workflow statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatistics {
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub workflow_state: WorkflowState,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub total_stages: i64,
    pub completed_stages: i64,
    pub failed_stages: i64,
    pub running_stages: i64,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub duration_secs: Option<i64>,
}

/// Workflow builder for creating complex DAG workflows
#[derive(Debug, Clone)]
pub struct WorkflowBuilder {
    workflow_name: String,
    stages: Vec<WorkflowStageBuilder>,
}

/// Workflow stage builder
#[derive(Debug, Clone)]
pub struct WorkflowStageBuilder {
    stage_name: String,
    tasks: Vec<SerializedTask>,
    dependencies: Vec<String>, // Stage names this stage depends on
}

impl WorkflowBuilder {
    /// Create a new workflow builder
    pub fn new(workflow_name: String) -> Self {
        Self {
            workflow_name,
            stages: Vec::new(),
        }
    }

    /// Add a new stage to the workflow
    pub fn add_stage(mut self, stage_name: String) -> Self {
        self.stages.push(WorkflowStageBuilder {
            stage_name,
            tasks: Vec::new(),
            dependencies: Vec::new(),
        });
        self
    }

    /// Add a task to the current stage
    pub fn add_task_to_stage(mut self, task: SerializedTask) -> Self {
        if let Some(stage) = self.stages.last_mut() {
            stage.tasks.push(task);
        }
        self
    }

    /// Add dependencies to the current stage
    pub fn add_stage_dependencies(mut self, dependencies: Vec<String>) -> Self {
        if let Some(stage) = self.stages.last_mut() {
            stage.dependencies = dependencies;
        }
        self
    }

    /// Get the workflow name
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// Get the stages
    pub fn stages(&self) -> &[WorkflowStageBuilder] {
        &self.stages
    }
}

impl WorkflowStageBuilder {
    /// Get the stage name
    pub fn stage_name(&self) -> &str {
        &self.stage_name
    }

    /// Get the tasks in this stage
    pub fn tasks(&self) -> &[SerializedTask] {
        &self.tasks
    }

    /// Get the dependencies
    pub fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
}

/// Type alias for async lifecycle hook functions
///
/// Hooks are async functions that take a hook context and serialized task,
/// and return a Result. They can be used to inject custom logic at various
/// points in the task lifecycle.
pub type HookFn = Arc<
    dyn Fn(
            &HookContext,
            &SerializedTask,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Context passed to lifecycle hooks
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Queue name
    pub queue_name: String,
    /// Task ID (if available)
    pub task_id: Option<Uuid>,
    /// Current timestamp
    pub timestamp: DateTime<Utc>,
    /// Additional metadata
    pub metadata: serde_json::Value,
}

/// Task lifecycle hook enum
#[derive(Clone)]
pub enum TaskHook {
    /// Called before a task is enqueued
    BeforeEnqueue(HookFn),
    /// Called after a task is successfully enqueued
    AfterEnqueue(HookFn),
    /// Called before a task is dequeued (reserved for future use)
    BeforeDequeue(HookFn),
    /// Called after a task is dequeued
    AfterDequeue(HookFn),
    /// Called before a task is acknowledged
    BeforeAck(HookFn),
    /// Called after a task is acknowledged
    AfterAck(HookFn),
    /// Called before a task is rejected
    BeforeReject(HookFn),
    /// Called after a task is rejected
    AfterReject(HookFn),
}

/// Container for all registered hooks
#[derive(Clone, Default)]
pub struct TaskHooks {
    pub(crate) before_enqueue: Vec<HookFn>,
    pub(crate) after_enqueue: Vec<HookFn>,
    pub(crate) before_dequeue: Vec<HookFn>,
    pub(crate) after_dequeue: Vec<HookFn>,
    pub(crate) before_ack: Vec<HookFn>,
    pub(crate) after_ack: Vec<HookFn>,
    pub(crate) before_reject: Vec<HookFn>,
    pub(crate) after_reject: Vec<HookFn>,
}

impl TaskHooks {
    /// Create empty hooks container
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a hook
    pub fn add(&mut self, hook: TaskHook) {
        match hook {
            TaskHook::BeforeEnqueue(f) => self.before_enqueue.push(f),
            TaskHook::AfterEnqueue(f) => self.after_enqueue.push(f),
            TaskHook::BeforeDequeue(f) => self.before_dequeue.push(f),
            TaskHook::AfterDequeue(f) => self.after_dequeue.push(f),
            TaskHook::BeforeAck(f) => self.before_ack.push(f),
            TaskHook::AfterAck(f) => self.after_ack.push(f),
            TaskHook::BeforeReject(f) => self.before_reject.push(f),
            TaskHook::AfterReject(f) => self.after_reject.push(f),
        }
    }

    /// Clear all hooks
    pub fn clear(&mut self) {
        self.before_enqueue.clear();
        self.after_enqueue.clear();
        self.before_dequeue.clear();
        self.after_dequeue.clear();
        self.before_ack.clear();
        self.after_ack.clear();
        self.before_reject.clear();
        self.after_reject.clear();
    }

    /// Execute before_enqueue hooks
    pub(crate) async fn run_before_enqueue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_enqueue {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_enqueue hooks
    pub(crate) async fn run_after_enqueue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_enqueue {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute before_ack hooks
    #[allow(dead_code)]
    pub(crate) async fn run_before_ack(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_ack {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_ack hooks
    #[allow(dead_code)]
    pub(crate) async fn run_after_ack(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_ack {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute before_reject hooks
    #[allow(dead_code)]
    pub(crate) async fn run_before_reject(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.before_reject {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_reject hooks
    #[allow(dead_code)]
    pub(crate) async fn run_after_reject(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_reject {
            hook(ctx, task).await?;
        }
        Ok(())
    }

    /// Execute after_dequeue hooks
    #[allow(dead_code)]
    pub(crate) async fn run_after_dequeue(
        &self,
        ctx: &HookContext,
        task: &SerializedTask,
    ) -> Result<()> {
        for hook in &self.after_dequeue {
            hook(ctx, task).await?;
        }
        Ok(())
    }
}
