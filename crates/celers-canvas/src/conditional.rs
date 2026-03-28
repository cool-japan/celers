use crate::Signature;
use serde::{Deserialize, Serialize};

/// Condition for conditional workflow branching
///
/// Represents a condition that determines which branch of a workflow to execute.
/// Conditions can be evaluated against the result of a previous task or against
/// static values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    /// Always true - execute the then branch
    Always,

    /// Always false - execute the else branch
    Never,

    /// Check if a value equals the expected value
    Equals {
        /// Field path to extract from result (e.g., "status" or "data.count")
        field: Option<String>,
        /// Expected value
        value: serde_json::Value,
    },

    /// Check if a value is not equal to the expected value
    NotEquals {
        /// Field path to extract from result
        field: Option<String>,
        /// Value to compare against
        value: serde_json::Value,
    },

    /// Check if a numeric value is greater than threshold
    GreaterThan {
        /// Field path to extract from result
        field: Option<String>,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a numeric value is less than threshold
    LessThan {
        /// Field path to extract from result
        field: Option<String>,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a value is truthy (not null, not false, not 0, not empty)
    Truthy {
        /// Field path to extract from result
        field: Option<String>,
    },

    /// Check if a value is falsy (null, false, 0, or empty)
    Falsy {
        /// Field path to extract from result
        field: Option<String>,
    },

    /// Check if a value contains a substring or element
    Contains {
        /// Field path to extract from result
        field: Option<String>,
        /// Value to search for
        value: serde_json::Value,
    },

    /// Check if a value matches a regex pattern
    Matches {
        /// Field path to extract from result
        field: Option<String>,
        /// Regex pattern
        pattern: String,
    },

    /// Logical AND of multiple conditions
    And(Vec<Condition>),

    /// Logical OR of multiple conditions
    Or(Vec<Condition>),

    /// Logical NOT of a condition
    Not(Box<Condition>),

    /// Custom condition evaluated by a task
    /// The task should return a boolean result
    Custom {
        /// Task name that evaluates the condition
        task: String,
        /// Arguments for the condition task
        args: Vec<serde_json::Value>,
    },
}

impl Condition {
    /// Create an always-true condition
    pub fn always() -> Self {
        Self::Always
    }

    /// Create an always-false condition
    pub fn never() -> Self {
        Self::Never
    }

    /// Create an equals condition
    pub fn equals(value: serde_json::Value) -> Self {
        Self::Equals { field: None, value }
    }

    /// Create an equals condition on a specific field
    pub fn field_equals(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self::Equals {
            field: Some(field.into()),
            value,
        }
    }

    /// Create a not-equals condition
    pub fn not_equals(value: serde_json::Value) -> Self {
        Self::NotEquals { field: None, value }
    }

    /// Create a greater-than condition
    pub fn greater_than(threshold: f64) -> Self {
        Self::GreaterThan {
            field: None,
            threshold,
        }
    }

    /// Create a greater-than condition on a specific field
    pub fn field_greater_than(field: impl Into<String>, threshold: f64) -> Self {
        Self::GreaterThan {
            field: Some(field.into()),
            threshold,
        }
    }

    /// Create a less-than condition
    pub fn less_than(threshold: f64) -> Self {
        Self::LessThan {
            field: None,
            threshold,
        }
    }

    /// Create a truthy condition
    pub fn truthy() -> Self {
        Self::Truthy { field: None }
    }

    /// Create a truthy condition on a specific field
    pub fn field_truthy(field: impl Into<String>) -> Self {
        Self::Truthy {
            field: Some(field.into()),
        }
    }

    /// Create a falsy condition
    pub fn falsy() -> Self {
        Self::Falsy { field: None }
    }

    /// Create a contains condition
    pub fn contains(value: serde_json::Value) -> Self {
        Self::Contains { field: None, value }
    }

    /// Create a regex match condition
    pub fn matches(pattern: impl Into<String>) -> Self {
        Self::Matches {
            field: None,
            pattern: pattern.into(),
        }
    }

    /// Create a custom task-based condition
    pub fn custom(task: impl Into<String>, args: Vec<serde_json::Value>) -> Self {
        Self::Custom {
            task: task.into(),
            args,
        }
    }

    /// Combine with AND
    pub fn and(self, other: Condition) -> Self {
        match self {
            Self::And(mut conditions) => {
                conditions.push(other);
                Self::And(conditions)
            }
            _ => Self::And(vec![self, other]),
        }
    }

    /// Combine with OR
    pub fn or(self, other: Condition) -> Self {
        match self {
            Self::Or(mut conditions) => {
                conditions.push(other);
                Self::Or(conditions)
            }
            _ => Self::Or(vec![self, other]),
        }
    }

    /// Negate the condition
    pub fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }

    /// Evaluate the condition against a JSON value
    pub fn evaluate(&self, value: &serde_json::Value) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Equals {
                field,
                value: expected,
            } => {
                let actual = extract_field(value, field.as_deref());
                actual == *expected
            }
            Self::NotEquals {
                field,
                value: expected,
            } => {
                let actual = extract_field(value, field.as_deref());
                actual != *expected
            }
            Self::GreaterThan { field, threshold } => {
                let actual = extract_field(value, field.as_deref());
                actual.as_f64().is_some_and(|v| v > *threshold)
            }
            Self::LessThan { field, threshold } => {
                let actual = extract_field(value, field.as_deref());
                actual.as_f64().is_some_and(|v| v < *threshold)
            }
            Self::Truthy { field } => {
                let actual = extract_field(value, field.as_deref());
                is_truthy(&actual)
            }
            Self::Falsy { field } => {
                let actual = extract_field(value, field.as_deref());
                !is_truthy(&actual)
            }
            Self::Contains {
                field,
                value: needle,
            } => {
                let actual = extract_field(value, field.as_deref());
                contains_value(&actual, needle)
            }
            Self::Matches { field, pattern } => {
                let actual = extract_field(value, field.as_deref());
                if let Some(s) = actual.as_str() {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            Self::And(conditions) => conditions.iter().all(|c| c.evaluate(value)),
            Self::Or(conditions) => conditions.iter().any(|c| c.evaluate(value)),
            Self::Not(condition) => !condition.evaluate(value),
            Self::Custom { .. } => {
                // Custom conditions cannot be evaluated synchronously
                // They need to be evaluated by executing a task
                false
            }
        }
    }

    /// Check if this is a custom condition that requires task execution
    pub fn is_custom(&self) -> bool {
        match self {
            Self::Custom { .. } => true,
            Self::And(conditions) => conditions.iter().any(|c| c.is_custom()),
            Self::Or(conditions) => conditions.iter().any(|c| c.is_custom()),
            Self::Not(condition) => condition.is_custom(),
            _ => false,
        }
    }
}

/// Extract a field from a JSON value using dot notation
fn extract_field(value: &serde_json::Value, field: Option<&str>) -> serde_json::Value {
    match field {
        None => value.clone(),
        Some(path) => {
            let mut current = value;
            for part in path.split('.') {
                current = match current {
                    serde_json::Value::Object(map) => {
                        map.get(part).unwrap_or(&serde_json::Value::Null)
                    }
                    serde_json::Value::Array(arr) => {
                        if let Ok(idx) = part.parse::<usize>() {
                            arr.get(idx).unwrap_or(&serde_json::Value::Null)
                        } else {
                            &serde_json::Value::Null
                        }
                    }
                    _ => &serde_json::Value::Null,
                };
            }
            current.clone()
        }
    }
}

/// Check if a JSON value is truthy
fn is_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|v| v != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(o) => !o.is_empty(),
    }
}

/// Check if a JSON value contains another value
fn contains_value(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    match haystack {
        serde_json::Value::String(s) => {
            if let Some(needle_str) = needle.as_str() {
                s.contains(needle_str)
            } else {
                false
            }
        }
        serde_json::Value::Array(arr) => arr.contains(needle),
        serde_json::Value::Object(map) => {
            if let Some(key) = needle.as_str() {
                map.contains_key(key)
            } else {
                false
            }
        }
        _ => false,
    }
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "always"),
            Self::Never => write!(f, "never"),
            Self::Equals { field, value } => {
                if let Some(field) = field {
                    write!(f, "{} == {}", field, value)
                } else {
                    write!(f, "result == {}", value)
                }
            }
            Self::NotEquals { field, value } => {
                if let Some(field) = field {
                    write!(f, "{} != {}", field, value)
                } else {
                    write!(f, "result != {}", value)
                }
            }
            Self::GreaterThan { field, threshold } => {
                if let Some(field) = field {
                    write!(f, "{} > {}", field, threshold)
                } else {
                    write!(f, "result > {}", threshold)
                }
            }
            Self::LessThan { field, threshold } => {
                if let Some(field) = field {
                    write!(f, "{} < {}", field, threshold)
                } else {
                    write!(f, "result < {}", threshold)
                }
            }
            Self::Truthy { field } => {
                if let Some(field) = field {
                    write!(f, "truthy({})", field)
                } else {
                    write!(f, "truthy(result)")
                }
            }
            Self::Falsy { field } => {
                if let Some(field) = field {
                    write!(f, "falsy({})", field)
                } else {
                    write!(f, "falsy(result)")
                }
            }
            Self::Contains { field, value } => {
                if let Some(field) = field {
                    write!(f, "{} contains {}", field, value)
                } else {
                    write!(f, "result contains {}", value)
                }
            }
            Self::Matches { field, pattern } => {
                if let Some(field) = field {
                    write!(f, "{} matches /{}/", field, pattern)
                } else {
                    write!(f, "result matches /{}/", pattern)
                }
            }
            Self::And(conditions) => {
                let parts: Vec<String> = conditions.iter().map(|c| format!("{}", c)).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            Self::Or(conditions) => {
                let parts: Vec<String> = conditions.iter().map(|c| format!("{}", c)).collect();
                write!(f, "({})", parts.join(" OR "))
            }
            Self::Not(condition) => write!(f, "NOT ({})", condition),
            Self::Custom { task, .. } => write!(f, "custom({})", task),
        }
    }
}

/// Branch: Conditional workflow execution (if/else)
///
/// Executes different tasks/workflows based on a condition evaluated against
/// the result of a previous task.
///
/// # Example
/// ```
/// use celers_canvas::{Branch, Condition, Signature};
///
/// // Execute different tasks based on result value
/// let branch = Branch::new(
///     Condition::field_greater_than("count", 100.0),
///     Signature::new("process_large_batch".to_string()),
/// )
/// .otherwise(Signature::new("process_small_batch".to_string()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    /// Condition to evaluate
    pub condition: Condition,

    /// Task/workflow to execute if condition is true
    pub then_branch: Box<Signature>,

    /// Optional task/workflow to execute if condition is false
    pub else_branch: Option<Box<Signature>>,

    /// Pass the condition input to the branch tasks
    pub pass_result: bool,
}

impl Branch {
    /// Create a new Branch workflow
    ///
    /// # Arguments
    /// * `condition` - The condition to evaluate
    /// * `then_sig` - The signature to execute if condition is true
    pub fn new(condition: Condition, then_sig: Signature) -> Self {
        Self {
            condition,
            then_branch: Box::new(then_sig),
            else_branch: None,
            pass_result: true,
        }
    }

    /// Set the else branch
    pub fn otherwise(mut self, else_sig: Signature) -> Self {
        self.else_branch = Some(Box::new(else_sig));
        self
    }

    /// Alias for `otherwise`
    pub fn else_do(self, else_sig: Signature) -> Self {
        self.otherwise(else_sig)
    }

    /// Set whether to pass the input result to branch tasks
    pub fn with_pass_result(mut self, pass: bool) -> Self {
        self.pass_result = pass;
        self
    }

    /// Check if there's an else branch
    pub fn has_else(&self) -> bool {
        self.else_branch.is_some()
    }

    /// Evaluate the branch condition and return the appropriate signature
    ///
    /// Returns Some(signature) for the branch to execute, or None if condition
    /// is false and there's no else branch.
    pub fn evaluate(&self, result: &serde_json::Value) -> Option<Signature> {
        let should_then = self.condition.evaluate(result);

        let sig = if should_then {
            Some((*self.then_branch).clone())
        } else {
            self.else_branch.as_ref().map(|s| (**s).clone())
        };

        // Pass the result as the first argument if enabled
        if let Some(mut sig) = sig {
            if self.pass_result && !sig.immutable {
                sig.args.insert(0, result.clone());
            }
            Some(sig)
        } else {
            None
        }
    }
}

impl std::fmt::Display for Branch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(else_branch) = &self.else_branch {
            write!(
                f,
                "Branch[if {} then {} else {}]",
                self.condition, self.then_branch.task, else_branch.task
            )
        } else {
            write!(
                f,
                "Branch[if {} then {}]",
                self.condition, self.then_branch.task
            )
        }
    }
}

/// Maybe: Optional task execution based on condition
///
/// A simplified Branch that either executes a task or does nothing.
/// This is essentially `Branch` without an else clause.
///
/// # Example
/// ```
/// use celers_canvas::{Maybe, Condition, Signature};
///
/// // Only send notification if count > 0
/// let maybe = Maybe::new(
///     Condition::field_greater_than("count", 0.0),
///     Signature::new("send_notification".to_string()),
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Maybe {
    /// Condition to evaluate
    pub condition: Condition,

    /// Task to execute if condition is true
    pub task: Signature,

    /// Pass the condition input to the task
    pub pass_result: bool,
}

impl Maybe {
    /// Create a new Maybe workflow
    pub fn new(condition: Condition, task: Signature) -> Self {
        Self {
            condition,
            task,
            pass_result: true,
        }
    }

    /// Set whether to pass the input result to the task
    pub fn with_pass_result(mut self, pass: bool) -> Self {
        self.pass_result = pass;
        self
    }

    /// Evaluate and return the task if condition is met
    pub fn evaluate(&self, result: &serde_json::Value) -> Option<Signature> {
        if self.condition.evaluate(result) {
            let mut task = self.task.clone();
            if self.pass_result && !task.immutable {
                task.args.insert(0, result.clone());
            }
            Some(task)
        } else {
            None
        }
    }

    /// Convert to a Branch (for unified handling)
    pub fn to_branch(self) -> Branch {
        Branch {
            condition: self.condition,
            then_branch: Box::new(self.task),
            else_branch: None,
            pass_result: self.pass_result,
        }
    }
}

impl std::fmt::Display for Maybe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Maybe[if {} then {}]", self.condition, self.task.task)
    }
}

/// Switch: Multi-way conditional branching
///
/// Like a switch/case statement - evaluates multiple conditions and executes
/// the first matching branch.
///
/// # Example
/// ```
/// use celers_canvas::{Switch, Condition, Signature};
///
/// let switch = Switch::new()
///     .case(
///         Condition::field_equals("status", serde_json::json!("pending")),
///         Signature::new("process_pending".to_string()),
///     )
///     .case(
///         Condition::field_equals("status", serde_json::json!("approved")),
///         Signature::new("process_approved".to_string()),
///     )
///     .default(Signature::new("process_unknown".to_string()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Switch {
    /// List of (condition, signature) pairs
    pub cases: Vec<(Condition, Signature)>,

    /// Default signature if no conditions match
    pub default: Option<Signature>,

    /// Pass the result to branch tasks
    pub pass_result: bool,
}

impl Switch {
    /// Create a new empty Switch
    pub fn new() -> Self {
        Self {
            cases: Vec::new(),
            default: None,
            pass_result: true,
        }
    }

    /// Add a case to the switch
    pub fn case(mut self, condition: Condition, task: Signature) -> Self {
        self.cases.push((condition, task));
        self
    }

    /// Set the default case
    pub fn default(mut self, task: Signature) -> Self {
        self.default = Some(task);
        self
    }

    /// Set whether to pass result to branch tasks
    pub fn with_pass_result(mut self, pass: bool) -> Self {
        self.pass_result = pass;
        self
    }

    /// Check if switch is empty (no cases)
    pub fn is_empty(&self) -> bool {
        self.cases.is_empty()
    }

    /// Get number of cases
    pub fn len(&self) -> usize {
        self.cases.len()
    }

    /// Evaluate and return the matching task
    pub fn evaluate(&self, result: &serde_json::Value) -> Option<Signature> {
        // Find first matching case
        for (condition, task) in &self.cases {
            if condition.evaluate(result) {
                let mut task = task.clone();
                if self.pass_result && !task.immutable {
                    task.args.insert(0, result.clone());
                }
                return Some(task);
            }
        }

        // Return default if no match
        if let Some(default) = &self.default {
            let mut task = default.clone();
            if self.pass_result && !task.immutable {
                task.args.insert(0, result.clone());
            }
            Some(task)
        } else {
            None
        }
    }
}

impl Default for Switch {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Switch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let case_strs: Vec<String> = self
            .cases
            .iter()
            .map(|(c, t)| format!("{} => {}", c, t.task))
            .collect();

        if let Some(default) = &self.default {
            write!(
                f,
                "Switch[{}, default => {}]",
                case_strs.join(", "),
                default.task
            )
        } else {
            write!(f, "Switch[{}]", case_strs.join(", "))
        }
    }
}
