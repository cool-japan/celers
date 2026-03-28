use crate::{Chain, Chord, Group};

/// Validation error type
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error message
    pub message: String,
    /// Task name where error occurred (if applicable)
    pub task_name: Option<String>,
}

impl ValidationError {
    /// Creates a new validation error
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            task_name: None,
        }
    }

    /// Creates a validation error with task context
    pub fn with_task(message: impl Into<String>, task_name: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            task_name: Some(task_name.into()),
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(task) = &self.task_name {
            write!(f, "[{}] {}", task, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

/// Validates a chain workflow
///
/// Checks for common issues like empty chains, invalid task names, etc.
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_validation::validate_chain;
/// use celers::Chain;
///
/// let chain = Chain::new()
///     .then("task1", vec![])
///     .then("task2", vec![]);
///
/// match validate_chain(&chain) {
///     Ok(_) => println!("Chain is valid"),
///     Err(errors) => {
///         for err in errors {
///             eprintln!("Error: {}", err);
///         }
///     }
/// }
/// ```
pub fn validate_chain(chain: &Chain) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    if chain.tasks.is_empty() {
        errors.push(ValidationError::new("Chain must contain at least one task"));
    }

    for task in &chain.tasks {
        if task.task.is_empty() {
            errors.push(ValidationError::new("Task name cannot be empty"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validates a group workflow
///
/// Checks for common issues in parallel task groups.
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_validation::validate_group;
/// use celers::Group;
///
/// let group = Group::new()
///     .add("task1", vec![])
///     .add("task2", vec![]);
///
/// validate_group(&group)?;
/// ```
pub fn validate_group(group: &Group) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    if group.tasks.is_empty() {
        errors.push(ValidationError::new("Group must contain at least one task"));
    }

    for task in &group.tasks {
        if task.task.is_empty() {
            errors.push(ValidationError::new("Task name cannot be empty"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validates a chord workflow
///
/// Checks that both header and body are properly configured.
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_validation::validate_chord;
/// use celers::{Chord, Group, Signature};
///
/// let chord = Chord {
///     header: Group::new().add("task1", vec![]),
///     body: Signature::new("callback".to_string()),
/// };
///
/// validate_chord(&chord)?;
/// ```
pub fn validate_chord(chord: &Chord) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    // Validate header group
    if let Err(mut group_errors) = validate_group(&chord.header) {
        errors.append(&mut group_errors);
    }

    // Validate body callback
    if chord.body.task.is_empty() {
        errors.push(ValidationError::with_task(
            "Callback task name cannot be empty",
            "body",
        ));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Checks if a workflow is likely to cause performance issues
///
/// Warns about potentially problematic configurations.
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_validation::check_performance_concerns;
/// use celers::Group;
///
/// let large_group = Group::new();
/// for i in 0..1000 {
///     large_group.add(&format!("task_{}", i), vec![]);
/// }
///
/// if let Some(warnings) = check_performance_concerns_group(&large_group) {
///     for warning in warnings {
///         println!("Warning: {}", warning);
///     }
/// }
/// ```
pub fn check_performance_concerns_group(group: &Group) -> Option<Vec<String>> {
    let mut warnings = Vec::new();

    if group.tasks.len() > 100 {
        warnings.push(format!(
            "Group contains {} tasks, which may cause performance issues. Consider batching.",
            group.tasks.len()
        ));
    }

    if warnings.is_empty() {
        None
    } else {
        Some(warnings)
    }
}

/// Checks chain for performance concerns
pub fn check_performance_concerns_chain(chain: &Chain) -> Option<Vec<String>> {
    let mut warnings = Vec::new();

    if chain.tasks.len() > 50 {
        warnings.push(format!(
            "Chain contains {} sequential tasks, which may cause long latency. Consider parallelizing.",
            chain.tasks.len()
        ));
    }

    if warnings.is_empty() {
        None
    } else {
        Some(warnings)
    }
}
