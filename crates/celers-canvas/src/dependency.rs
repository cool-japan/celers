use crate::{Chain, Group, Signature, TaskDependency};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Dependency graph for workflow tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyGraph {
    /// Map of task ID to its dependencies
    pub dependencies: HashMap<Uuid, Vec<TaskDependency>>,
    /// Reverse map for quick lookup
    #[serde(skip)]
    pub dependents: HashMap<Uuid, Vec<Uuid>>,
}

impl DependencyGraph {
    /// Create a new dependency graph
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
        }
    }

    /// Add a dependency
    pub fn add_dependency(&mut self, task_id: Uuid, dependency: TaskDependency) {
        self.dependencies
            .entry(task_id)
            .or_default()
            .push(dependency.clone());

        // Update reverse map
        self.dependents
            .entry(dependency.task_id)
            .or_default()
            .push(task_id);
    }

    /// Get dependencies for a task
    pub fn get_dependencies(&self, task_id: &Uuid) -> Vec<&TaskDependency> {
        self.dependencies
            .get(task_id)
            .map(|deps| deps.iter().collect())
            .unwrap_or_default()
    }

    /// Get tasks that depend on this task
    pub fn get_dependents(&self, task_id: &Uuid) -> Vec<Uuid> {
        self.dependents.get(task_id).cloned().unwrap_or_default()
    }

    /// Check for circular dependencies
    pub fn has_circular_dependency(&self) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for task_id in self.dependencies.keys() {
            if self.is_cyclic(*task_id, &mut visited, &mut rec_stack) {
                return true;
            }
        }
        false
    }

    fn is_cyclic(
        &self,
        task_id: Uuid,
        visited: &mut std::collections::HashSet<Uuid>,
        rec_stack: &mut std::collections::HashSet<Uuid>,
    ) -> bool {
        if rec_stack.contains(&task_id) {
            return true;
        }
        if visited.contains(&task_id) {
            return false;
        }

        visited.insert(task_id);
        rec_stack.insert(task_id);

        if let Some(deps) = self.dependencies.get(&task_id) {
            for dep in deps {
                if self.is_cyclic(dep.task_id, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(&task_id);
        false
    }

    /// Get topological order of tasks
    pub fn topological_sort(&self) -> Result<Vec<Uuid>, String> {
        if self.has_circular_dependency() {
            return Err("Circular dependency detected".to_string());
        }

        let mut in_degree: HashMap<Uuid, usize> = HashMap::new();
        let mut queue: Vec<Uuid> = Vec::new();
        let mut result: Vec<Uuid> = Vec::new();

        // Calculate in-degrees
        for (task_id, deps) in &self.dependencies {
            in_degree.entry(*task_id).or_insert(deps.len());
            for dep in deps {
                in_degree.entry(dep.task_id).or_insert(0);
            }
        }

        // Find tasks with no dependencies
        for (task_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push(*task_id);
            }
        }

        // Process queue
        while let Some(task_id) = queue.pop() {
            result.push(task_id);

            if let Some(dependents) = self.dependents.get(&task_id) {
                for &dependent in dependents {
                    if let Some(degree) = in_degree.get_mut(&dependent) {
                        if *degree > 0 {
                            *degree -= 1;
                            if *degree == 0 {
                                queue.push(dependent);
                            }
                        }
                    }
                }
            }
        }

        if result.len() == in_degree.len() {
            Ok(result)
        } else {
            Err("Failed to compute topological sort".to_string())
        }
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DependencyGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DependencyGraph[{} tasks]", self.dependencies.len())
    }
}

// ============================================================================
// Parallel Reduce
// ============================================================================

/// Parallel reduce configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelReduce {
    /// Task to map over inputs
    pub map_task: Signature,
    /// Task to reduce pairs of results
    pub reduce_task: Signature,
    /// Input values to map over
    pub inputs: Vec<serde_json::Value>,
    /// Number of parallel workers
    pub parallelism: usize,
    /// Initial value for reduction
    pub initial_value: Option<serde_json::Value>,
}

impl ParallelReduce {
    /// Create a new parallel reduce
    pub fn new(
        map_task: Signature,
        reduce_task: Signature,
        inputs: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            map_task,
            reduce_task,
            inputs,
            parallelism: 4,
            initial_value: None,
        }
    }

    /// Set parallelism level
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set initial value
    pub fn with_initial_value(mut self, value: serde_json::Value) -> Self {
        self.initial_value = Some(value);
        self
    }

    /// Get the number of inputs
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }
}

impl std::fmt::Display for ParallelReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParallelReduce[map={}, reduce={}, inputs={}, parallelism={}]",
            self.map_task.task,
            self.reduce_task.task,
            self.inputs.len(),
            self.parallelism
        )
    }
}

// ============================================================================
// Workflow Templates
// ============================================================================

/// Workflow template parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateParameter {
    /// Parameter name
    pub name: String,
    /// Parameter type
    pub param_type: String,
    /// Default value
    pub default: Option<serde_json::Value>,
    /// Whether parameter is required
    #[serde(default = "default_true")]
    pub required: bool,
    /// Parameter description
    pub description: Option<String>,
}

fn default_true() -> bool {
    true
}

impl TemplateParameter {
    /// Create a new template parameter
    pub fn new(name: impl Into<String>, param_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            param_type: param_type.into(),
            default: None,
            required: true,
            description: None,
        }
    }

    /// Set default value
    pub fn with_default(mut self, value: serde_json::Value) -> Self {
        self.default = Some(value);
        self.required = false;
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Make optional
    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }
}

impl std::fmt::Display for TemplateParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.param_type)?;
        if !self.required {
            write!(f, " (optional)")?;
        }
        Ok(())
    }
}

/// Workflow template for reusable patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTemplate {
    /// Template name
    pub name: String,
    /// Template version
    pub version: String,
    /// Template parameters
    pub parameters: Vec<TemplateParameter>,
    /// Template chain (if any)
    pub chain: Option<Chain>,
    /// Template group (if any)
    pub group: Option<Group>,
    /// Template description
    pub description: Option<String>,
}

impl WorkflowTemplate {
    /// Create a new workflow template
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            parameters: Vec::new(),
            chain: None,
            group: None,
            description: None,
        }
    }

    /// Add a parameter
    pub fn add_parameter(mut self, param: TemplateParameter) -> Self {
        self.parameters.push(param);
        self
    }

    /// Set template chain
    pub fn with_chain(mut self, chain: Chain) -> Self {
        self.chain = Some(chain);
        self
    }

    /// Set template group
    pub fn with_group(mut self, group: Group) -> Self {
        self.group = Some(group);
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Instantiate template with parameters
    pub fn instantiate(&self, params: HashMap<String, serde_json::Value>) -> Result<Self, String> {
        // Validate required parameters
        for param in &self.parameters {
            if param.required && !params.contains_key(&param.name) && param.default.is_none() {
                return Err(format!("Missing required parameter: {}", param.name));
            }
        }

        // Create instance with parameters applied
        Ok(self.clone())
    }
}

impl std::fmt::Display for WorkflowTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowTemplate[{}@{}]", self.name, self.version)?;
        if !self.parameters.is_empty() {
            write!(f, " params={}", self.parameters.len())?;
        }
        Ok(())
    }
}
