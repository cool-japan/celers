use crate::{Chain, Chord, Group};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Workflow optimization pass
#[derive(Debug, Clone, PartialEq)]
pub enum OptimizationPass {
    /// Common subexpression elimination
    CommonSubexpressionElimination,
    /// Dead code elimination
    DeadCodeElimination,
    /// Task fusion (combine sequential tasks)
    TaskFusion,
    /// Parallel task scheduling optimization
    ParallelScheduling,
    /// Resource allocation optimization
    ResourceOptimization,
}

impl std::fmt::Display for OptimizationPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommonSubexpressionElimination => write!(f, "CSE"),
            Self::DeadCodeElimination => write!(f, "DCE"),
            Self::TaskFusion => write!(f, "TaskFusion"),
            Self::ParallelScheduling => write!(f, "ParallelScheduling"),
            Self::ResourceOptimization => write!(f, "ResourceOptimization"),
        }
    }
}

/// Workflow compiler for optimization
#[derive(Debug, Clone)]
pub struct WorkflowCompiler {
    /// Optimization passes to apply
    pub passes: Vec<OptimizationPass>,
    /// Whether to enable aggressive optimizations
    pub aggressive: bool,
}

impl WorkflowCompiler {
    /// Create a new workflow compiler
    pub fn new() -> Self {
        Self {
            passes: vec![
                OptimizationPass::DeadCodeElimination,
                OptimizationPass::CommonSubexpressionElimination,
            ],
            aggressive: false,
        }
    }

    /// Enable aggressive optimizations
    pub fn aggressive(mut self) -> Self {
        self.aggressive = true;
        self.passes.push(OptimizationPass::TaskFusion);
        self.passes.push(OptimizationPass::ParallelScheduling);
        self.passes.push(OptimizationPass::ResourceOptimization);
        self
    }

    /// Add optimization pass
    pub fn add_pass(mut self, pass: OptimizationPass) -> Self {
        if !self.passes.contains(&pass) {
            self.passes.push(pass);
        }
        self
    }

    /// Optimize a chain by applying configured optimization passes
    pub fn optimize_chain(&self, chain: &Chain) -> Chain {
        let mut optimized = chain.clone();

        for pass in &self.passes {
            optimized = match pass {
                OptimizationPass::CommonSubexpressionElimination => {
                    self.apply_cse_chain(&optimized)
                }
                OptimizationPass::DeadCodeElimination => self.apply_dce_chain(&optimized),
                OptimizationPass::TaskFusion => self.apply_task_fusion(&optimized),
                OptimizationPass::ParallelScheduling => {
                    // Chains are sequential, no parallel scheduling
                    optimized
                }
                OptimizationPass::ResourceOptimization => {
                    self.apply_resource_optimization_chain(&optimized)
                }
            };
        }

        optimized
    }

    /// Optimize a group by applying configured optimization passes
    pub fn optimize_group(&self, group: &Group) -> Group {
        let mut optimized = group.clone();

        for pass in &self.passes {
            optimized = match pass {
                OptimizationPass::CommonSubexpressionElimination => {
                    self.apply_cse_group(&optimized)
                }
                OptimizationPass::DeadCodeElimination => self.apply_dce_group(&optimized),
                OptimizationPass::TaskFusion => {
                    // Groups are parallel, no task fusion
                    optimized
                }
                OptimizationPass::ParallelScheduling => self.apply_parallel_scheduling(&optimized),
                OptimizationPass::ResourceOptimization => {
                    self.apply_resource_optimization_group(&optimized)
                }
            };
        }

        optimized
    }

    /// Optimize a chord by applying configured optimization passes
    pub fn optimize_chord(&self, chord: &Chord) -> Chord {
        let optimized_group = self.optimize_group(&chord.header);
        Chord {
            header: optimized_group,
            body: chord.body.clone(),
        }
    }

    // Helper methods for optimization passes

    /// Common Subexpression Elimination for chains
    fn apply_cse_chain(&self, chain: &Chain) -> Chain {
        let mut seen = HashMap::new();
        let mut optimized_tasks = Vec::new();

        for (idx, task) in chain.tasks.iter().enumerate() {
            // Create a key for deduplication (task name + args)
            let key = format!(
                "{}:{}:{}",
                task.task,
                serde_json::to_string(&task.args).unwrap_or_default(),
                serde_json::to_string(&task.kwargs).unwrap_or_default()
            );

            if let Some(&prev_idx) = seen.get(&key) {
                // Skip duplicate if aggressive mode
                if self.aggressive && prev_idx < idx {
                    continue;
                }
            } else {
                seen.insert(key, idx);
            }

            optimized_tasks.push(task.clone());
        }

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Common Subexpression Elimination for groups
    fn apply_cse_group(&self, group: &Group) -> Group {
        let mut seen = HashMap::new();
        let mut optimized_tasks = Vec::new();

        for task in &group.tasks {
            // Create a key for deduplication (task name + args)
            let key = format!(
                "{}:{}:{}",
                task.task,
                serde_json::to_string(&task.args).unwrap_or_default(),
                serde_json::to_string(&task.kwargs).unwrap_or_default()
            );

            if let std::collections::hash_map::Entry::Vacant(e) = seen.entry(key) {
                e.insert(true);
                optimized_tasks.push(task.clone());
            } else {
                // Skip duplicate in aggressive mode
                if !self.aggressive {
                    optimized_tasks.push(task.clone());
                }
            }
        }

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }

    /// Dead Code Elimination for chains
    fn apply_dce_chain(&self, chain: &Chain) -> Chain {
        // Remove tasks that have no effect (e.g., empty task names)
        let optimized_tasks: Vec<_> = chain
            .tasks
            .iter()
            .filter(|task| !task.task.is_empty())
            .cloned()
            .collect();

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Dead Code Elimination for groups
    fn apply_dce_group(&self, group: &Group) -> Group {
        // Remove tasks that have no effect (e.g., empty task names)
        let optimized_tasks: Vec<_> = group
            .tasks
            .iter()
            .filter(|task| !task.task.is_empty())
            .cloned()
            .collect();

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }

    /// Task Fusion for chains (combine similar sequential tasks)
    fn apply_task_fusion(&self, chain: &Chain) -> Chain {
        if !self.aggressive || chain.tasks.len() < 2 {
            return chain.clone();
        }

        let mut optimized_tasks = Vec::new();
        let mut i = 0;

        while i < chain.tasks.len() {
            let current = &chain.tasks[i];

            // Check if next task can be fused (same task name, immutable)
            if i + 1 < chain.tasks.len() {
                let next = &chain.tasks[i + 1];

                if current.task == next.task
                    && current.immutable
                    && next.immutable
                    && current.options.priority == next.options.priority
                {
                    // Fuse tasks: combine args
                    let mut fused = current.clone();
                    fused.args.extend(next.args.clone());
                    optimized_tasks.push(fused);
                    i += 2; // Skip both tasks
                    continue;
                }
            }

            optimized_tasks.push(current.clone());
            i += 1;
        }

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Parallel Scheduling optimization for groups
    fn apply_parallel_scheduling(&self, group: &Group) -> Group {
        let mut optimized_tasks = group.tasks.clone();

        // Sort tasks by priority (higher priority first)
        optimized_tasks.sort_by(|a, b| {
            let a_priority = a.options.priority.unwrap_or(0);
            let b_priority = b.options.priority.unwrap_or(0);
            b_priority.cmp(&a_priority)
        });

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }

    /// Resource Optimization for chains
    fn apply_resource_optimization_chain(&self, chain: &Chain) -> Chain {
        // Group tasks by queue to improve resource utilization
        let mut optimized_tasks = chain.tasks.clone();

        if self.aggressive {
            // Reorder tasks to group by queue while maintaining dependencies
            optimized_tasks.sort_by(|a, b| {
                let a_queue = a.options.queue.as_deref().unwrap_or("");
                let b_queue = b.options.queue.as_deref().unwrap_or("");
                a_queue.cmp(b_queue)
            });
        }

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Resource Optimization for groups
    fn apply_resource_optimization_group(&self, group: &Group) -> Group {
        // Balance tasks across queues
        let mut optimized_tasks = group.tasks.clone();

        // Sort by queue to improve locality
        optimized_tasks.sort_by(|a, b| {
            let a_queue = a.options.queue.as_deref().unwrap_or("");
            let b_queue = b.options.queue.as_deref().unwrap_or("");
            a_queue.cmp(b_queue)
        });

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }
}

impl Default for WorkflowCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowCompiler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowCompiler[")?;
        for (i, pass) in self.passes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", pass)?;
        }
        if self.aggressive {
            write!(f, " aggressive")?;
        }
        write!(f, "]")
    }
}

// ============================================================================
// Type-Safe Result Passing
// ============================================================================

/// Type-safe result wrapper for workflow results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedResult<T> {
    /// Result value
    pub value: T,
    /// Result type name for validation
    pub type_name: String,
    /// Result metadata
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl<T: Serialize> TypedResult<T> {
    /// Create a new typed result
    pub fn new(value: T) -> Self {
        Self {
            value,
            type_name: std::any::type_name::<T>().to_string(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the result
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Get the type name
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}

impl<T: std::fmt::Display> std::fmt::Display for TypedResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TypedResult[type={}, value={}]",
            self.type_name, self.value
        )
    }
}

/// Type validator for result passing
#[derive(Debug, Clone)]
pub struct TypeValidator {
    /// Expected type name
    pub expected_type: String,
    /// Whether to allow compatible types
    pub allow_compatible: bool,
}

impl TypeValidator {
    /// Create a new type validator
    pub fn new(expected_type: impl Into<String>) -> Self {
        Self {
            expected_type: expected_type.into(),
            allow_compatible: false,
        }
    }

    /// Allow compatible types
    pub fn allow_compatible(mut self) -> Self {
        self.allow_compatible = true;
        self
    }

    /// Validate a type name
    pub fn validate(&self, actual_type: &str) -> bool {
        if actual_type == self.expected_type {
            return true;
        }
        if self.allow_compatible {
            self.is_compatible(actual_type)
        } else {
            false
        }
    }

    /// Check if types are compatible
    fn is_compatible(&self, actual_type: &str) -> bool {
        // Simple compatibility check (can be extended)
        if self.expected_type.contains("Option") && actual_type != "None" {
            return true;
        }
        if self.expected_type == "serde_json::Value" {
            return true;
        }
        false
    }
}

impl std::fmt::Display for TypeValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypeValidator[expected={}]", self.expected_type)?;
        if self.allow_compatible {
            write!(f, " (allow_compatible)")?;
        }
        Ok(())
    }
}

// ============================================================================
// Data Dependencies
// ============================================================================

/// Task dependency specification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TaskDependency {
    /// Task ID that this task depends on
    pub task_id: Uuid,
    /// Output key to use from the dependency (optional)
    pub output_key: Option<String>,
    /// Whether this dependency is optional
    #[serde(default)]
    pub optional: bool,
}

impl TaskDependency {
    /// Create a new task dependency
    pub fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            output_key: None,
            optional: false,
        }
    }

    /// Set output key
    pub fn with_output_key(mut self, key: impl Into<String>) -> Self {
        self.output_key = Some(key.into());
        self
    }

    /// Mark as optional
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }
}

impl std::fmt::Display for TaskDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskDependency[{}]", self.task_id)?;
        if let Some(ref key) = self.output_key {
            write!(f, " output={}", key)?;
        }
        if self.optional {
            write!(f, " (optional)")?;
        }
        Ok(())
    }
}
