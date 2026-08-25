//! Task result message format
//!
//! This module provides the Celery-compatible result message format for
//! storing and retrieving task results.
//!
//! # Result States
//!
//! - `PENDING` - Task is waiting for execution
//! - `RECEIVED` - Task was received by a worker
//! - `STARTED` - Task execution started
//! - `SUCCESS` - Task completed successfully
//! - `FAILURE` - Task execution failed
//! - `RETRY` - Task is being retried
//! - `REVOKED` - Task was revoked
//!
//! # Example
//!
//! ```
//! use celers_protocol::result::{ResultMessage, TaskStatus};
//! use uuid::Uuid;
//! use serde_json::json;
//!
//! let task_id = Uuid::new_v4();
//! let result = ResultMessage::success(task_id, json!(42));
//! assert!(result.is_success());
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Task execution status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskStatus {
    /// Task is waiting for execution
    #[default]
    Pending,
    /// Task was received by a worker
    Received,
    /// Task execution started
    Started,
    /// Task completed successfully
    Success,
    /// Task execution failed
    Failure,
    /// Task is being retried
    Retry,
    /// Task was revoked
    Revoked,
}

impl TaskStatus {
    /// Check if this is a terminal state (no more transitions)
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Success | TaskStatus::Failure | TaskStatus::Revoked
        )
    }

    /// Check if this is a successful state
    #[inline]
    pub fn is_success(&self) -> bool {
        matches!(self, TaskStatus::Success)
    }

    /// Check if this is a failure state
    #[inline]
    pub fn is_failure(&self) -> bool {
        matches!(self, TaskStatus::Failure)
    }

    /// Check if this is a ready state (has a result)
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.is_terminal()
    }

    /// Get the string representation
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "PENDING",
            TaskStatus::Received => "RECEIVED",
            TaskStatus::Started => "STARTED",
            TaskStatus::Success => "SUCCESS",
            TaskStatus::Failure => "FAILURE",
            TaskStatus::Retry => "RETRY",
            TaskStatus::Revoked => "REVOKED",
        }
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(TaskStatus::Pending),
            "RECEIVED" => Ok(TaskStatus::Received),
            "STARTED" => Ok(TaskStatus::Started),
            "SUCCESS" => Ok(TaskStatus::Success),
            "FAILURE" => Ok(TaskStatus::Failure),
            "RETRY" => Ok(TaskStatus::Retry),
            "REVOKED" => Ok(TaskStatus::Revoked),
            _ => Err(format!("Invalid task status: {}", s)),
        }
    }
}

/// Exception information for failed tasks
///
/// This is Celery's exception dict, the value stored under the `result` key of
/// a `FAILURE` meta record:
///
/// ```text
/// {"exc_type": "ValueError", "exc_message": ["bad input"], "exc_module": "builtins"}
/// ```
///
/// `celery.backends.base.Backend.exception_to_python` reads all three keys:
/// `exc_module` + `exc_type` are used to import (or synthesize) the exception
/// class, and `exc_message` is splatted into the constructor as `*args` -- which
/// is why it is a **list** on the wire even for a single message.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExceptionInfo {
    /// Exception type name (e.g. `"ValueError"`)
    #[serde(rename = "exc_type")]
    pub exc_type: String,

    /// Python's `exc.args`, verbatim.
    ///
    /// A list, because `celery.backends.base.Backend.exception_to_python`
    /// splats it into the exception constructor as `*args` -- `raise
    /// ValueError("a", "b")` travels as `["a", "b"]` and must be reconstructed
    /// as two arguments, not one. The entries are [`serde_json::Value`]s
    /// because Python's arguments need not be strings (`OSError(2, "no such
    /// file")` is an int and a string).
    ///
    /// Regression: this used to be a `String` that joined a multi-element list
    /// with `", "`, so a Python client rebuilding the exception from a CeleRS
    /// record got one argument where the raiser passed several, and an integer
    /// argument came back as text. [`ExceptionInfo::message`] renders the same
    /// joined form for display, without the wire format losing anything.
    ///
    /// Deserialization also accepts a bare string (the legacy CeleRS form) and
    /// `null`.
    #[serde(
        rename = "exc_message",
        deserialize_with = "deserialize_exc_message",
        default
    )]
    pub exc_message: Vec<serde_json::Value>,

    /// Python module the exception class lives in (e.g. `"builtins"`).
    ///
    /// Required by `celery.utils.serialization.create_exception_cls` to
    /// reconstruct the original exception type; a `null` makes Celery fall back
    /// to a synthesized class. The key is always emitted, matching Celery's own
    /// `prepare_exception` output.
    #[serde(default)]
    pub exc_module: Option<String>,

    /// Full traceback (if available)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
}

/// Deserialize `exc.args` from the list form, the legacy bare-string form, or
/// `null`.
fn deserialize_exc_message<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<serde_json::Value>, D::Error> {
    match serde_json::Value::deserialize(deserializer)? {
        // Celery's own shape.
        serde_json::Value::Array(items) => Ok(items),
        // What CeleRS wrote before `exc_message` modelled the argument list,
        // and what a hand-written record is likely to carry.
        serde_json::Value::Null => Ok(Vec::new()),
        // A bare string (or any other scalar) is a single argument.
        other => Ok(vec![other]),
    }
}

/// Render one `exc.args` entry the way it reads in a message.
fn exc_arg_text(argument: &serde_json::Value) -> String {
    match argument {
        // A string argument is *the* message; quoting it would be noise.
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

impl ExceptionInfo {
    /// Create new exception info from a single message argument.
    ///
    /// The message becomes the one entry of [`ExceptionInfo::exc_message`],
    /// which is Python's `exc.args`. Use [`ExceptionInfo::with_args`] for an
    /// exception raised with several arguments.
    pub fn new(exc_type: impl Into<String>, exc_message: impl Into<String>) -> Self {
        Self {
            exc_type: exc_type.into(),
            exc_message: vec![serde_json::Value::String(exc_message.into())],
            exc_module: None,
            traceback: None,
        }
    }

    /// Set the whole `exc.args` list (builder pattern).
    ///
    /// This is what `raise ValueError("a", "b")` puts on the wire, and what
    /// Celery splats back into the exception constructor.
    #[must_use]
    pub fn with_args(mut self, args: Vec<serde_json::Value>) -> Self {
        self.exc_message = args;
        self
    }

    /// The exception's arguments rendered as one human-readable message.
    ///
    /// Multiple arguments are joined with `", "`, the way Python's own
    /// `str(exc)` renders a multi-argument exception's tuple contents in a log
    /// line. Nothing on the wire depends on this: it exists so callers that
    /// want a single string do not have to reach into the list.
    pub fn message(&self) -> String {
        match self.exc_message.as_slice() {
            [] => String::new(),
            [only] => exc_arg_text(only),
            many => many.iter().map(exc_arg_text).collect::<Vec<_>>().join(", "),
        }
    }

    /// Set the traceback
    #[must_use]
    pub fn with_traceback(mut self, traceback: impl Into<String>) -> Self {
        self.traceback = Some(traceback.into());
        self
    }

    /// Set the Python module the exception class belongs to.
    ///
    /// Use `"builtins"` for the standard Python exceptions (`ValueError`,
    /// `RuntimeError`, ...) so that a Python client's `AsyncResult.get()`
    /// re-raises the real type instead of a synthesized stand-in.
    #[must_use]
    pub fn with_exc_module(mut self, exc_module: impl Into<String>) -> Self {
        self.exc_module = Some(exc_module.into());
        self
    }

    /// Rebuild an [`ExceptionInfo`] from Celery's exception dict.
    ///
    /// Returns [`None`] when the value is not an exception dict (no `exc_type`
    /// string), so an ordinary task result stored on a `FAILURE` record is left
    /// untouched.
    fn from_celery_value(value: &serde_json::Value) -> Option<Self> {
        let object = value.as_object()?;
        if !object
            .get("exc_type")
            .is_some_and(serde_json::Value::is_string)
        {
            return None;
        }
        serde_json::from_value(value.clone()).ok()
    }
}

/// One entry of a result record's `children` list.
///
/// Celery does not store child *ids*: it stores whole result trees. A child is
/// rendered by `celery.result.AsyncResult.as_tuple`, which is
///
/// ```python
/// def as_tuple(self):
///     parent = self.parent
///     return (self.id, parent and parent.as_tuple()), None
/// ```
///
/// -- so on the wire one child looks like
///
/// ```text
/// [[task_id, parent_or_null], null]
/// ```
///
/// where `parent_or_null` is recursively a whole node of the same shape, and
/// the trailing slot holds a [`GroupResult`'s][group] own results (a list)
/// instead of `null`. Any retried task, and any task with a chain or group
/// parent, carries one, so this is not an exotic shape: a `children` list of
/// bare id strings is not something Celery ever writes.
///
/// [group]: https://docs.celeryq.dev/en/stable/reference/celery.result.html
///
/// # Reading
///
/// [`Deserialize`] accepts every form `celery.result.result_from_tuple`
/// accepts, plus the legacy CeleRS form:
///
/// | wire | meaning |
/// |---|---|
/// | `"<uuid>"` | legacy CeleRS: a bare child id |
/// | `[[id, parent], null]` | an `AsyncResult` with a parent chain |
/// | `[[id, parent], [child, ...]]` | a `GroupResult` and its members |
/// | `[id, nodes]` | Celery's short form: the id without a parent |
///
/// Note that the *second* slot of a two-element node is always the group's
/// results, never the parent -- mirroring `result_from_tuple`, which unpacks
/// `res, nodes = r` and only then splits `res` into `(id, parent)`.
///
/// [`Serialize`] always writes the canonical `[[id, parent], children]` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultChild {
    /// The child task's id.
    pub task_id: Uuid,

    /// The child's own parent result, if it has one.
    ///
    /// Boxed because the shape is recursive: a chain three tasks deep carries
    /// three nested nodes.
    pub parent: Option<Box<ResultChild>>,

    /// A `GroupResult`'s members.
    ///
    /// [`None`] for an ordinary `AsyncResult` -- which is *not* the same as
    /// `Some(vec![])`, an empty group, so the distinction is preserved rather
    /// than normalized away.
    pub children: Option<Vec<ResultChild>>,
}

impl ResultChild {
    /// A plain child result: no parent, not a group.
    pub fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            parent: None,
            children: None,
        }
    }

    /// Attach the parent result this child was spawned from (builder pattern).
    #[must_use]
    pub fn with_parent(mut self, parent: ResultChild) -> Self {
        self.parent = Some(Box::new(parent));
        self
    }

    /// Make this node a `GroupResult` carrying `children` (builder pattern).
    #[must_use]
    pub fn with_children(mut self, children: Vec<ResultChild>) -> Self {
        self.children = Some(children);
        self
    }

    /// Whether this node is a group (`GroupResult.as_tuple`) rather than a
    /// single result.
    #[inline]
    pub fn is_group(&self) -> bool {
        self.children.is_some()
    }

    /// The ids of this node and every node below it, parents included.
    ///
    /// Depth-first, this node first. Useful for the common case of "which
    /// tasks does this record reference?" without walking the tree by hand.
    pub fn descendant_ids(&self) -> Vec<Uuid> {
        let mut ids = Vec::new();
        self.collect_ids(&mut ids);
        ids
    }

    fn collect_ids(&self, ids: &mut Vec<Uuid>) {
        ids.push(self.task_id);
        if let Some(parent) = self.parent.as_ref() {
            parent.collect_ids(ids);
        }
        for child in self.children.iter().flatten() {
            child.collect_ids(ids);
        }
    }

    /// Parse one `children` entry; see the type-level table.
    fn from_celery_value(value: &serde_json::Value) -> Result<Self, String> {
        match value {
            // Legacy CeleRS: `children` was a list of bare ids.
            serde_json::Value::String(id) => Ok(Self::new(parse_child_id(id)?)),
            serde_json::Value::Array(node) => {
                let [head, nodes] = node.as_slice() else {
                    return Err(format!(
                        "a result child is a 2-element [result, group_results] \
                         node, got {} element(s)",
                        node.len()
                    ));
                };
                // `result_from_tuple`: `id, parent = res if isinstance(res,
                // (list, tuple)) else (res, None)`.
                let (task_id, parent) = match head {
                    serde_json::Value::String(id) => (parse_child_id(id)?, None),
                    serde_json::Value::Array(pair) => {
                        let [id, parent] = pair.as_slice() else {
                            return Err(format!(
                                "a result child's head is the pair [id, parent], \
                                 got {} element(s)",
                                pair.len()
                            ));
                        };
                        let id = id
                            .as_str()
                            .ok_or_else(|| format!("a result child's id must be a string: {id}"))?;
                        let parent = match parent {
                            serde_json::Value::Null => None,
                            other => Some(Box::new(Self::from_celery_value(other)?)),
                        };
                        (parse_child_id(id)?, parent)
                    }
                    other => {
                        return Err(format!(
                            "a result child's head is an id or an [id, parent] \
                             pair, got {other}"
                        ))
                    }
                };
                let children = match nodes {
                    serde_json::Value::Null => None,
                    serde_json::Value::Array(members) => Some(
                        members
                            .iter()
                            .map(Self::from_celery_value)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                    other => {
                        return Err(format!(
                            "a result child's group results are a list or null, \
                             got {other}"
                        ))
                    }
                };
                Ok(Self {
                    task_id,
                    parent,
                    children,
                })
            }
            other => Err(format!(
                "a result child is an id string or a [result, group_results] \
                 node, got {other}"
            )),
        }
    }
}

/// Parse a task id out of a result tuple, naming the offender on failure.
fn parse_child_id(id: &str) -> Result<Uuid, String> {
    id.parse::<Uuid>()
        .map_err(|e| format!("a result child's id must be a UUID ({id:?}): {e}"))
}

impl From<Uuid> for ResultChild {
    fn from(task_id: Uuid) -> Self {
        Self::new(task_id)
    }
}

/// The `[id, parent]` head of a serialized [`ResultChild`].
struct ResultChildHead<'a>(&'a ResultChild);

impl Serialize for ResultChildHead<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut head = serializer.serialize_seq(Some(2))?;
        head.serialize_element(&self.0.task_id)?;
        head.serialize_element(&self.0.parent)?;
        head.end()
    }
}

impl Serialize for ResultChild {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut node = serializer.serialize_seq(Some(2))?;
        node.serialize_element(&ResultChildHead(self))?;
        node.serialize_element(&self.children)?;
        node.end()
    }
}

impl<'de> Deserialize<'de> for ResultChild {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_celery_value(&value).map_err(serde::de::Error::custom)
    }
}

/// Deserialize a record's `children` list.
///
/// `null` is accepted as "no children": Celery's own
/// `Backend.current_task_children` returns `None` when a result is stored
/// outside a task context (a client calling `mark_as_failure`, a chord
/// callback), so `{"children": null}` is a record a real backend writes.
fn deserialize_children<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ResultChild>, D::Error> {
    Option::<Vec<ResultChild>>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// Task result message (Celery-compatible format)
///
/// # Failure format
///
/// Celery's result backend stores a failed task as
///
/// ```text
/// {"status": "FAILURE",
///  "result": {"exc_type": "ValueError", "exc_message": ["bad input"], "exc_module": "builtins"},
///  "traceback": "Traceback (most recent call last): ...",
///  "children": []}
/// ```
///
/// -- the exception dict lives **inside** `result`, because
/// `AsyncResult.get()` calls `meta['result']` through `exception_to_python`.
/// A record whose `result` is `null` therefore cannot re-raise anything on the
/// Python side.
///
/// [`ResultMessage`] keeps the typed [`ResultMessage::exception`] field for
/// Rust-side ergonomics *and* mirrors it into `result` when the status is
/// [`TaskStatus::Failure`], so both consumers are served by one record.
/// Deserialization reverses the mirroring: a `FAILURE` record whose `result`
/// holds an exception dict is parsed back into the typed field.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultMessage {
    /// Task ID
    pub task_id: Uuid,

    /// Task status
    pub status: TaskStatus,

    /// Result value (for SUCCESS)
    ///
    /// On the wire this key also carries the exception dict for `FAILURE`; see
    /// the type-level docs.
    pub result: Option<serde_json::Value>,

    /// Traceback (for FAILURE)
    pub traceback: Option<String>,

    /// Exception info (for FAILURE)
    pub exception: Option<ExceptionInfo>,

    /// Timestamp when result was created
    pub date_done: Option<DateTime<Utc>>,

    /// Task name
    pub task: Option<String>,

    /// Worker that executed the task
    pub worker: Option<String>,

    /// Retry count
    pub retries: Option<u32>,

    /// Parent task ID (for workflows)
    pub parent_id: Option<Uuid>,

    /// Root task ID (for workflows)
    pub root_id: Option<Uuid>,

    /// Group ID (for grouped tasks)
    pub group_id: Option<Uuid>,

    /// The result trees this task spawned; see [`ResultChild`].
    ///
    /// Celery stores whole `AsyncResult.as_tuple()` nodes here, not bare ids.
    pub children: Vec<ResultChild>,

    /// Additional metadata
    pub meta: HashMap<String, serde_json::Value>,
}

/// Serialization shape of [`ResultMessage`].
///
/// Borrows from the live message; the only computed field is `result`, which
/// carries the exception dict on `FAILURE`.
#[derive(Serialize)]
struct ResultMessageRepr<'a> {
    task_id: &'a Uuid,
    status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ResultField<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    traceback: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception: Option<&'a ExceptionInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    date_done: Option<&'a DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<&'a Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_id: Option<&'a Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_id: Option<&'a Uuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    children: &'a Vec<ResultChild>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    meta: &'a HashMap<String, serde_json::Value>,
}

/// The `result` key: an exception dict for `FAILURE`, otherwise the value.
#[derive(Serialize)]
#[serde(untagged)]
enum ResultField<'a> {
    /// Celery's `{exc_type, exc_message, exc_module}` dict.
    Exception(&'a ExceptionInfo),
    /// An ordinary task return value.
    Value(&'a serde_json::Value),
}

/// Deserialization shape of [`ResultMessage`].
#[derive(Deserialize)]
struct ResultMessageDe {
    task_id: Uuid,
    status: TaskStatus,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    traceback: Option<String>,
    #[serde(default)]
    exception: Option<ExceptionInfo>,
    #[serde(default)]
    date_done: Option<DateTime<Utc>>,
    #[serde(default)]
    task: Option<String>,
    #[serde(default)]
    worker: Option<String>,
    #[serde(default)]
    retries: Option<u32>,
    #[serde(default)]
    parent_id: Option<Uuid>,
    #[serde(default)]
    root_id: Option<Uuid>,
    #[serde(default)]
    group_id: Option<Uuid>,
    #[serde(default, deserialize_with = "deserialize_children")]
    children: Vec<ResultChild>,
    #[serde(default)]
    meta: HashMap<String, serde_json::Value>,
}

impl Serialize for ResultMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // On FAILURE the exception dict *is* the result, which is what
        // `AsyncResult.get()` feeds to `exception_to_python`.
        let result = match (self.status, self.exception.as_ref()) {
            (TaskStatus::Failure, Some(exception)) => Some(ResultField::Exception(exception)),
            _ => self.result.as_ref().map(ResultField::Value),
        };

        // Celery keeps the traceback as a sibling string; fall back to the one
        // captured on the exception so it is never silently dropped.
        let traceback = self
            .traceback
            .as_ref()
            .or_else(|| self.exception.as_ref().and_then(|e| e.traceback.as_ref()));

        ResultMessageRepr {
            task_id: &self.task_id,
            status: self.status,
            result,
            traceback,
            exception: self.exception.as_ref(),
            date_done: self.date_done.as_ref(),
            task: self.task.as_ref(),
            worker: self.worker.as_ref(),
            retries: self.retries,
            parent_id: self.parent_id.as_ref(),
            root_id: self.root_id.as_ref(),
            group_id: self.group_id.as_ref(),
            children: &self.children,
            meta: &self.meta,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ResultMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = ResultMessageDe::deserialize(deserializer)?;

        let mut message = Self {
            task_id: repr.task_id,
            status: repr.status,
            result: repr.result,
            traceback: repr.traceback,
            exception: repr.exception,
            date_done: repr.date_done,
            task: repr.task,
            worker: repr.worker,
            retries: repr.retries,
            parent_id: repr.parent_id,
            root_id: repr.root_id,
            group_id: repr.group_id,
            children: repr.children,
            meta: repr.meta,
        };

        // Undo the failure mirroring: on the wire a FAILURE record carries the
        // exception dict in `result`, whether it was written by Celery or by
        // the `Serialize` impl above.
        if message.status == TaskStatus::Failure {
            if let Some(value) = message.result.take() {
                match ExceptionInfo::from_celery_value(&value) {
                    Some(mut exception) => {
                        if message.exception.is_none() {
                            // Celery keeps the traceback as a sibling key.
                            if exception.traceback.is_none() {
                                exception.traceback = message.traceback.clone();
                            }
                            message.exception = Some(exception);
                        }
                        // Otherwise `result` was just the mirror of the typed
                        // `exception` field: drop it so the round-trip is
                        // lossless.
                    }
                    // Not an exception dict -- keep it as an ordinary result.
                    None => message.result = Some(value),
                }
            }
        }

        Ok(message)
    }
}

impl ResultMessage {
    /// Create a new result message
    pub fn new(task_id: Uuid, status: TaskStatus) -> Self {
        Self {
            task_id,
            status,
            result: None,
            traceback: None,
            exception: None,
            date_done: None,
            task: None,
            worker: None,
            retries: None,
            parent_id: None,
            root_id: None,
            group_id: None,
            children: Vec::new(),
            meta: HashMap::new(),
        }
    }

    /// Create a pending result
    pub fn pending(task_id: Uuid) -> Self {
        Self::new(task_id, TaskStatus::Pending)
    }

    /// Create a successful result
    pub fn success(task_id: Uuid, result: serde_json::Value) -> Self {
        Self {
            result: Some(result),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Success)
        }
    }

    /// Create a failure result
    pub fn failure(task_id: Uuid, exc_type: &str, exc_message: &str) -> Self {
        Self {
            exception: Some(ExceptionInfo::new(exc_type, exc_message)),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Failure)
        }
    }

    /// Create a failure result from a fully-specified exception.
    ///
    /// Prefer this when the exception's Python module is known
    /// ([`ExceptionInfo::with_exc_module`]), since it lets a Python client
    /// re-raise the original exception type rather than a synthesized one.
    pub fn failure_with_exception(task_id: Uuid, exception: ExceptionInfo) -> Self {
        Self {
            traceback: exception.traceback.clone(),
            exception: Some(exception),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Failure)
        }
    }

    /// Create a failure result with traceback
    pub fn failure_with_traceback(
        task_id: Uuid,
        exc_type: &str,
        exc_message: &str,
        traceback: &str,
    ) -> Self {
        Self {
            exception: Some(ExceptionInfo::new(exc_type, exc_message).with_traceback(traceback)),
            traceback: Some(traceback.to_string()),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Failure)
        }
    }

    /// Create a retry result
    pub fn retry(task_id: Uuid, retries: u32) -> Self {
        Self {
            retries: Some(retries),
            ..Self::new(task_id, TaskStatus::Retry)
        }
    }

    /// Create a revoked result
    pub fn revoked(task_id: Uuid) -> Self {
        Self {
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Revoked)
        }
    }

    /// Create a started result
    pub fn started(task_id: Uuid) -> Self {
        Self::new(task_id, TaskStatus::Started)
    }

    /// Create a received result
    pub fn received(task_id: Uuid) -> Self {
        Self::new(task_id, TaskStatus::Received)
    }

    /// Set the task name
    #[must_use]
    pub fn with_task(mut self, task: impl Into<String>) -> Self {
        self.task = Some(task.into());
        self
    }

    /// Set the worker name
    #[must_use]
    pub fn with_worker(mut self, worker: impl Into<String>) -> Self {
        self.worker = Some(worker.into());
        self
    }

    /// Set the parent task ID
    #[must_use]
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set the root task ID
    #[must_use]
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Set the group ID
    #[must_use]
    pub fn with_group(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Add a child task ID
    ///
    /// The id becomes a leaf [`ResultChild`] (no parent, not a group), which is
    /// what a task that simply spawned another one records. Use
    /// [`ResultMessage::with_child_result`] to attach a whole tree.
    #[must_use]
    pub fn with_child(mut self, child_id: Uuid) -> Self {
        self.children.push(ResultChild::new(child_id));
        self
    }

    /// Add a child result tree; see [`ResultChild`].
    #[must_use]
    pub fn with_child_result(mut self, child: ResultChild) -> Self {
        self.children.push(child);
        self
    }

    /// Set children task IDs
    #[must_use]
    pub fn with_children(mut self, children: Vec<Uuid>) -> Self {
        self.children = children.into_iter().map(ResultChild::new).collect();
        self
    }

    /// Set the children as whole result trees; see [`ResultChild`].
    #[must_use]
    pub fn with_child_results(mut self, children: Vec<ResultChild>) -> Self {
        self.children = children;
        self
    }

    /// The ids of the direct children, ignoring their parent chains and group
    /// members.
    pub fn child_ids(&self) -> Vec<Uuid> {
        self.children.iter().map(|child| child.task_id).collect()
    }

    /// Add metadata
    #[must_use]
    pub fn with_meta(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.meta.insert(key.into(), value);
        self
    }

    /// Set retry count
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Set completion timestamp
    #[must_use]
    pub fn with_date_done(mut self, date_done: DateTime<Utc>) -> Self {
        self.date_done = Some(date_done);
        self
    }

    /// Add a single metadata entry (mutable)
    pub fn add_meta(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.meta.insert(key.into(), value);
    }

    /// Get a metadata value by key
    #[inline]
    pub fn get_meta(&self, key: &str) -> Option<&serde_json::Value> {
        self.meta.get(key)
    }

    /// Check if metadata key exists
    #[inline]
    pub fn has_meta(&self, key: &str) -> bool {
        self.meta.contains_key(key)
    }

    /// Get the number of metadata entries
    #[inline]
    pub fn meta_len(&self) -> usize {
        self.meta.len()
    }

    /// Get the retry count (defaults to 0 if not set)
    #[inline]
    pub fn retry_count(&self) -> u32 {
        self.retries.unwrap_or(0)
    }

    /// Check if the result is ready
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.status.is_ready()
    }

    /// Check if the task succeeded
    #[inline]
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Check if the task failed
    #[inline]
    pub fn is_failure(&self) -> bool {
        self.status.is_failure()
    }

    /// Get the result value (if success)
    #[inline]
    pub fn get_result(&self) -> Option<&serde_json::Value> {
        if self.is_success() {
            self.result.as_ref()
        } else {
            None
        }
    }

    /// Get the exception info (if failure)
    #[inline]
    pub fn get_exception(&self) -> Option<&ExceptionInfo> {
        if self.is_failure() {
            self.exception.as_ref()
        } else {
            None
        }
    }

    /// Serialize to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_task_status_is_terminal() {
        assert!(!TaskStatus::Pending.is_terminal());
        assert!(!TaskStatus::Received.is_terminal());
        assert!(!TaskStatus::Started.is_terminal());
        assert!(TaskStatus::Success.is_terminal());
        assert!(TaskStatus::Failure.is_terminal());
        assert!(!TaskStatus::Retry.is_terminal());
        assert!(TaskStatus::Revoked.is_terminal());
    }

    #[test]
    fn test_task_status_as_str() {
        assert_eq!(TaskStatus::Pending.as_str(), "PENDING");
        assert_eq!(TaskStatus::Success.as_str(), "SUCCESS");
        assert_eq!(TaskStatus::Failure.as_str(), "FAILURE");
    }

    #[test]
    fn test_task_status_display() {
        assert_eq!(TaskStatus::Success.to_string(), "SUCCESS");
        assert_eq!(TaskStatus::Failure.to_string(), "FAILURE");
    }

    #[test]
    fn test_task_status_default() {
        assert_eq!(TaskStatus::default(), TaskStatus::Pending);
    }

    #[test]
    fn test_task_status_from_str() {
        use std::str::FromStr;

        assert_eq!(
            TaskStatus::from_str("PENDING").unwrap(),
            TaskStatus::Pending
        );
        assert_eq!(
            TaskStatus::from_str("pending").unwrap(),
            TaskStatus::Pending
        );
        assert_eq!(
            TaskStatus::from_str("RECEIVED").unwrap(),
            TaskStatus::Received
        );
        assert_eq!(
            TaskStatus::from_str("STARTED").unwrap(),
            TaskStatus::Started
        );
        assert_eq!(
            TaskStatus::from_str("SUCCESS").unwrap(),
            TaskStatus::Success
        );
        assert_eq!(
            TaskStatus::from_str("success").unwrap(),
            TaskStatus::Success
        );
        assert_eq!(
            TaskStatus::from_str("FAILURE").unwrap(),
            TaskStatus::Failure
        );
        assert_eq!(TaskStatus::from_str("RETRY").unwrap(), TaskStatus::Retry);
        assert_eq!(
            TaskStatus::from_str("REVOKED").unwrap(),
            TaskStatus::Revoked
        );

        assert!(TaskStatus::from_str("INVALID").is_err());
        assert!(TaskStatus::from_str("").is_err());
    }

    #[test]
    fn test_result_message_success() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::success(task_id, json!({"answer": 42}));

        assert_eq!(result.task_id, task_id);
        assert!(result.is_success());
        assert!(result.is_ready());
        assert!(!result.is_failure());
        assert!(result.date_done.is_some());
        assert_eq!(result.get_result(), Some(&json!({"answer": 42})));
    }

    #[test]
    fn test_result_message_failure() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::failure(task_id, "ValueError", "Invalid input");

        assert_eq!(result.task_id, task_id);
        assert!(result.is_failure());
        assert!(result.is_ready());
        assert!(!result.is_success());
        assert!(result.date_done.is_some());

        let exc = result.get_exception().unwrap();
        assert_eq!(exc.exc_type, "ValueError");
        assert_eq!(exc.exc_message, vec![json!("Invalid input")]);
        assert_eq!(exc.message(), "Invalid input");
    }

    #[test]
    fn test_result_message_failure_with_traceback() {
        let task_id = Uuid::new_v4();
        let traceback = "Traceback (most recent call last):\n  File \"test.py\"...";
        let result = ResultMessage::failure_with_traceback(
            task_id,
            "RuntimeError",
            "Test failed",
            traceback,
        );

        assert!(result.is_failure());
        assert_eq!(result.traceback, Some(traceback.to_string()));
        assert_eq!(
            result.exception.as_ref().unwrap().traceback,
            Some(traceback.to_string())
        );
    }

    #[test]
    fn test_result_message_pending() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::pending(task_id);

        assert_eq!(result.status, TaskStatus::Pending);
        assert!(!result.is_ready());
    }

    #[test]
    fn test_result_message_retry() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::retry(task_id, 3);

        assert_eq!(result.status, TaskStatus::Retry);
        assert_eq!(result.retries, Some(3));
        assert!(!result.is_ready());
    }

    #[test]
    fn test_result_message_revoked() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::revoked(task_id);

        assert_eq!(result.status, TaskStatus::Revoked);
        assert!(result.is_ready());
        assert!(result.date_done.is_some());
    }

    #[test]
    fn test_result_message_builders() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let group_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();

        let result = ResultMessage::success(task_id, json!(100))
            .with_task("tasks.add")
            .with_worker("worker-1")
            .with_parent(parent_id)
            .with_root(root_id)
            .with_group(group_id)
            .with_child(child_id)
            .with_meta("custom", json!("value"));

        assert_eq!(result.task, Some("tasks.add".to_string()));
        assert_eq!(result.worker, Some("worker-1".to_string()));
        assert_eq!(result.parent_id, Some(parent_id));
        assert_eq!(result.root_id, Some(root_id));
        assert_eq!(result.group_id, Some(group_id));
        assert_eq!(result.child_ids(), vec![child_id]);
        assert_eq!(result.children, vec![ResultChild::new(child_id)]);
        assert_eq!(result.meta.get("custom"), Some(&json!("value")));
    }

    #[test]
    fn test_result_message_json_round_trip() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::success(task_id, json!({"data": [1, 2, 3]}))
            .with_task("tasks.process")
            .with_worker("worker-2");

        let json_bytes = result.to_json().unwrap();
        let decoded = ResultMessage::from_json(&json_bytes).unwrap();

        assert_eq!(decoded.task_id, task_id);
        assert_eq!(decoded.status, TaskStatus::Success);
        assert_eq!(decoded.task, Some("tasks.process".to_string()));
        assert_eq!(decoded.worker, Some("worker-2".to_string()));
    }

    #[test]
    fn test_result_message_serialization_format() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::success(task_id, json!(42));

        let json_str = serde_json::to_string(&result).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Verify Celery-compatible field names
        assert!(value.get("task_id").is_some());
        assert!(value.get("status").is_some());
        assert!(value.get("result").is_some());
        assert_eq!(value["status"], "SUCCESS");
    }

    /// Golden fixture: the meta record Celery's result backend writes for a
    /// failed task (`celery.backends.base.Backend._get_result_meta`).
    ///
    /// Field semantics:
    /// * `result` -- the **exception dict**, not `null`. `AsyncResult.get()`
    ///   passes `meta['result']` to `exception_to_python`, so a failure whose
    ///   `result` is null cannot re-raise anything.
    /// * `exc_message` -- a list, because it is splatted into the exception
    ///   constructor as `*args`.
    /// * `exc_module` -- used by `create_exception_cls` to import the real
    ///   exception class.
    /// * `traceback` -- a plain string, a sibling of `result`.
    const CELERY_FAILURE_META: &str = r#"{
        "status": "FAILURE",
        "result": {
            "exc_type": "ValueError",
            "exc_message": ["Invalid input"],
            "exc_module": "builtins"
        },
        "traceback": "Traceback (most recent call last):\n  File \"tasks.py\", line 7\nValueError: Invalid input",
        "children": [],
        "task_id": "6d5b1f1e-6a4f-4a3c-9b6c-1f8f5f1a2b3c",
        "date_done": "2024-01-01T00:00:00Z"
    }"#;

    /// Regression: a failure written by CeleRS used to leave `result` at
    /// `None` and put the exception in a separate `exception` key with
    /// `exc_message` as a plain string and no `exc_module`, so a Python
    /// `AsyncResult.get()` saw `result = None` and could not reconstruct the
    /// exception.
    #[test]
    fn test_failure_serializes_in_celery_result_backend_format() {
        let task_id = Uuid::new_v4();
        let traceback = "Traceback (most recent call last):\n  File \"tasks.py\", line 7";
        let result = ResultMessage::failure_with_exception(
            task_id,
            ExceptionInfo::new("ValueError", "Invalid input")
                .with_exc_module("builtins")
                .with_traceback(traceback),
        );

        let value: serde_json::Value =
            serde_json::from_slice(&result.to_json().expect("serialize")).expect("parse");

        assert_eq!(value["status"], json!("FAILURE"));
        // The exception dict lives *in* `result`.
        assert_eq!(value["result"]["exc_type"], json!("ValueError"));
        assert_eq!(value["result"]["exc_module"], json!("builtins"));
        // `exc_message` is a list (Python's `exc.args`).
        assert_eq!(value["result"]["exc_message"], json!(["Invalid input"]));
        // The traceback is a sibling string.
        assert_eq!(value["traceback"], json!(traceback));
        assert_eq!(value["task_id"], json!(task_id.to_string()));
    }

    /// A traceback set only on the exception must still surface as the
    /// top-level `traceback` key Celery reads.
    #[test]
    fn test_failure_traceback_is_never_dropped() {
        let exception =
            ExceptionInfo::new("RuntimeError", "boom").with_traceback("Traceback: boom");
        let mut result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Failure);
        result.exception = Some(exception);

        let value: serde_json::Value =
            serde_json::from_slice(&result.to_json().expect("serialize")).expect("parse");
        assert_eq!(value["traceback"], json!("Traceback: boom"));
    }

    /// A failure record written by Python Celery must parse back into the
    /// typed representation.
    #[test]
    fn test_parses_celery_failure_meta() {
        let result = ResultMessage::from_json(CELERY_FAILURE_META.as_bytes())
            .expect("a real Celery FAILURE meta must deserialize");

        assert!(result.is_failure());
        assert_eq!(
            result.task_id,
            Uuid::parse_str("6d5b1f1e-6a4f-4a3c-9b6c-1f8f5f1a2b3c").expect("uuid")
        );

        let exception = result.get_exception().expect("exception reconstructed");
        assert_eq!(exception.exc_type, "ValueError");
        // `exc_message` is Python's `exc.args`, kept as the list it is.
        assert_eq!(exception.exc_message, vec![json!("Invalid input")]);
        assert_eq!(exception.message(), "Invalid input");
        assert_eq!(exception.exc_module.as_deref(), Some("builtins"));
        // The sibling traceback is attached to the exception too.
        assert!(exception
            .traceback
            .as_deref()
            .is_some_and(|tb| tb.contains("ValueError: Invalid input")));

        // `result` no longer holds the exception dict once it is typed.
        assert_eq!(result.result, None);
    }

    /// A multi-argument Python exception (`exc.args` with several entries)
    /// and the legacy bare-string form must both be accepted.
    #[test]
    fn test_exc_message_accepts_list_and_string_forms() {
        let multi: ExceptionInfo = serde_json::from_str(
            r#"{"exc_type":"TypeError","exc_message":["expected int","got str"],"exc_module":"builtins"}"#,
        )
        .expect("multi-arg exc_message must parse");
        assert_eq!(
            multi.exc_message,
            vec![json!("expected int"), json!("got str")]
        );
        assert_eq!(multi.message(), "expected int, got str");

        let legacy: ExceptionInfo =
            serde_json::from_str(r#"{"exc_type":"ValueError","exc_message":"plain string"}"#)
                .expect("legacy string exc_message must parse");
        assert_eq!(legacy.exc_message, vec![json!("plain string")]);
        assert_eq!(legacy.exc_module, None);

        let empty: ExceptionInfo =
            serde_json::from_str(r#"{"exc_type":"RuntimeError","exc_message":null}"#)
                .expect("a null exc_message must parse");
        assert!(empty.exc_message.is_empty());
        assert_eq!(empty.message(), "");
    }

    /// Regression: `exc_message` was a `String` that joined a multi-element
    /// list with `", "`, so `raise ValueError("a", "b")` came back out of
    /// CeleRS as the *single* argument `"a, b"` -- and Celery splats
    /// `exc_message` into the exception constructor, so a Python client
    /// rebuilt a different exception than the one that was raised.
    #[test]
    fn test_multi_argument_exceptions_keep_their_argument_boundaries() {
        let record = r#"{
            "status": "FAILURE",
            "task_id": "6d5b1f1e-6a4f-4a3c-9b6c-1f8f5f1a2b3c",
            "result": {
                "exc_type": "OSError",
                "exc_message": [2, "no such file"],
                "exc_module": "builtins"
            }
        }"#;

        let parsed = ResultMessage::from_json(record.as_bytes()).expect("deserialize");
        let exception = parsed.get_exception().expect("exception");
        // Two arguments, and the integer is still an integer.
        assert_eq!(exception.exc_message, vec![json!(2), json!("no such file")]);
        assert_eq!(exception.message(), "2, no such file");

        // Re-serialized, the argument boundaries survive for the Python side.
        let value: serde_json::Value =
            serde_json::from_slice(&parsed.to_json().expect("serialize")).expect("parse");
        assert_eq!(value["result"]["exc_message"], json!([2, "no such file"]));
    }

    /// An exception built in Rust with several arguments serializes the same
    /// way Celery would have written it.
    #[test]
    fn test_exception_with_args_serializes_as_a_list() {
        let result = ResultMessage::failure_with_exception(
            Uuid::new_v4(),
            ExceptionInfo::new("OSError", "ignored")
                .with_args(vec![json!(2), json!("no such file")])
                .with_exc_module("builtins"),
        );

        let value: serde_json::Value =
            serde_json::from_slice(&result.to_json().expect("serialize")).expect("parse");
        assert_eq!(value["result"]["exc_message"], json!([2, "no such file"]));
    }

    /// The failure mirroring must be lossless in both directions.
    #[test]
    fn test_failure_round_trip_is_lossless() {
        let original = ResultMessage::failure_with_traceback(
            Uuid::new_v4(),
            "RuntimeError",
            "Test failed",
            "Traceback (most recent call last): ...",
        )
        .with_task("tasks.process")
        .with_worker("worker-1");

        let decoded =
            ResultMessage::from_json(&original.to_json().expect("serialize")).expect("deserialize");

        assert_eq!(decoded, original);
    }

    /// A `FAILURE` record whose `result` is an ordinary value (not an
    /// exception dict) must not be mistaken for an exception.
    #[test]
    fn test_failure_with_non_exception_result_is_preserved() {
        let decoded = ResultMessage::from_json(
            br#"{"task_id":"6d5b1f1e-6a4f-4a3c-9b6c-1f8f5f1a2b3c","status":"FAILURE","result":{"partial":42}}"#,
        )
        .expect("deserialize");

        assert_eq!(decoded.exception, None);
        assert_eq!(decoded.result, Some(json!({"partial": 42})));
    }

    /// Success records are untouched by the failure mirroring.
    #[test]
    fn test_success_result_is_the_plain_value() {
        let result = ResultMessage::success(Uuid::new_v4(), json!({"answer": 42}));
        let value: serde_json::Value =
            serde_json::from_slice(&result.to_json().expect("serialize")).expect("parse");

        assert_eq!(value["status"], json!("SUCCESS"));
        assert_eq!(value["result"], json!({"answer": 42}));
        assert!(value.get("exception").is_none());

        let decoded =
            ResultMessage::from_json(&result.to_json().expect("serialize")).expect("deserialize");
        assert_eq!(decoded, result);
    }

    #[test]
    fn test_exception_info() {
        let exc =
            ExceptionInfo::new("TypeError", "Expected int, got str").with_traceback("at line 42");

        assert_eq!(exc.exc_type, "TypeError");
        assert_eq!(exc.exc_message, vec![json!("Expected int, got str")]);
        assert_eq!(exc.message(), "Expected int, got str");
        assert_eq!(exc.traceback, Some("at line 42".to_string()));
    }

    #[test]
    fn test_exception_info_default() {
        let exc = ExceptionInfo::default();

        assert_eq!(exc.exc_type, "");
        assert!(exc.exc_message.is_empty());
        assert_eq!(exc.message(), "");
        assert_eq!(exc.traceback, None);

        // Test that default can be used in builder patterns
        let exc_builder = ExceptionInfo::default().with_traceback("some traceback");

        assert_eq!(exc_builder.traceback, Some("some traceback".to_string()));
    }

    #[test]
    fn test_with_children() {
        let task_id = Uuid::new_v4();
        let children = vec![Uuid::new_v4(), Uuid::new_v4()];

        let result = ResultMessage::success(task_id, json!(null)).with_children(children.clone());

        assert_eq!(result.child_ids(), children);
        // Bare ids become leaf nodes: no parent, not a group.
        assert!(result.children.iter().all(|child| child.parent.is_none()));
        assert!(result.children.iter().all(|child| !child.is_group()));
    }

    /// The `children` list Celery writes is a list of *result trees*, not of
    /// ids: `AsyncResult.as_tuple()` renders one child as
    /// `((task_id, parent_tuple), None)`.
    ///
    /// Regression: `children` was `Vec<Uuid>`, so `from_json` rejected the
    /// record of every retried, chained or grouped task outright.
    #[test]
    fn test_parses_a_celery_record_with_nested_children() {
        const RECORD: &str = r#"{
            "status": "SUCCESS",
            "result": 2,
            "traceback": null,
            "children": [[["00b0d7c2-9c80-4dc3-b5a7-671a099b92ea", null], null]],
            "date_done": "2026-01-01T00:00:00+00:00",
            "task_id": "7b1a0d1e-0000-4000-8000-000000000001"
        }"#;

        let parsed = ResultMessage::from_json(RECORD.as_bytes())
            .expect("a record with children must deserialize");

        assert_eq!(parsed.status, TaskStatus::Success);
        assert_eq!(
            parsed.child_ids(),
            vec![Uuid::parse_str("00b0d7c2-9c80-4dc3-b5a7-671a099b92ea").expect("uuid")]
        );
        assert_eq!(parsed.children[0].parent, None);
        assert!(!parsed.children[0].is_group());

        // And it goes back out in the shape Celery reads.
        let value: serde_json::Value =
            serde_json::from_slice(&parsed.to_json().expect("serialize")).expect("parse");
        assert_eq!(
            value["children"],
            json!([[["00b0d7c2-9c80-4dc3-b5a7-671a099b92ea", null], null]])
        );
    }

    /// A chained task's child carries its parent, recursively, and a group's
    /// child carries its members in the trailing slot.
    #[test]
    fn test_parses_parent_chains_and_group_children() {
        const RECORD: &str = r#"{
            "status": "SUCCESS",
            "task_id": "7b1a0d1e-0000-4000-8000-000000000001",
            "children": [
                [["11111111-1111-4111-8111-111111111111",
                  [["22222222-2222-4222-8222-222222222222", null], null]], null],
                [["33333333-3333-4333-8333-333333333333", null],
                 [[["44444444-4444-4444-8444-444444444444", null], null]]]
            ]
        }"#;

        let parsed = ResultMessage::from_json(RECORD.as_bytes()).expect("deserialize");
        assert_eq!(parsed.children.len(), 2);

        let chained = &parsed.children[0];
        let parent = chained.parent.as_ref().expect("the parent tuple is kept");
        assert_eq!(
            parent.task_id,
            Uuid::parse_str("22222222-2222-4222-8222-222222222222").expect("uuid")
        );
        assert!(!chained.is_group());

        let group = &parsed.children[1];
        let members = group
            .children
            .as_ref()
            .expect("a group carries its members");
        assert_eq!(members.len(), 1);
        assert_eq!(
            members[0].task_id,
            Uuid::parse_str("44444444-4444-4444-8444-444444444444").expect("uuid")
        );
        assert_eq!(
            group.descendant_ids(),
            vec![
                Uuid::parse_str("33333333-3333-4333-8333-333333333333").expect("uuid"),
                Uuid::parse_str("44444444-4444-4444-8444-444444444444").expect("uuid"),
            ]
        );

        // Every node survives the round trip, group-ness included.
        let round_tripped =
            ResultMessage::from_json(&parsed.to_json().expect("serialize")).expect("deserialize");
        assert_eq!(round_tripped.children, parsed.children);
    }

    /// `celery.result.result_from_tuple` also accepts the short
    /// `[id, group_results]` node, and the legacy CeleRS form was a bare id
    /// string. Both must still parse.
    #[test]
    fn test_children_accept_the_short_and_legacy_forms() {
        const RECORD: &str = r#"{
            "status": "SUCCESS",
            "task_id": "7b1a0d1e-0000-4000-8000-000000000001",
            "children": [
                "55555555-5555-4555-8555-555555555555",
                ["66666666-6666-4666-8666-666666666666", null]
            ]
        }"#;

        let parsed = ResultMessage::from_json(RECORD.as_bytes()).expect("deserialize");
        assert_eq!(
            parsed.child_ids(),
            vec![
                Uuid::parse_str("55555555-5555-4555-8555-555555555555").expect("uuid"),
                Uuid::parse_str("66666666-6666-4666-8666-666666666666").expect("uuid"),
            ]
        );
        // Both normalize to the canonical form on the way out.
        let value: serde_json::Value =
            serde_json::from_slice(&parsed.to_json().expect("serialize")).expect("parse");
        assert_eq!(
            value["children"],
            json!([
                [["55555555-5555-4555-8555-555555555555", null], null],
                [["66666666-6666-4666-8666-666666666666", null], null]
            ])
        );
    }

    /// An empty group (`Some(vec![])`) is not the same thing as "not a group"
    /// (`None`), and the wire form keeps them apart.
    #[test]
    fn test_an_empty_group_is_not_a_missing_group() {
        let empty_group = ResultChild::new(Uuid::new_v4()).with_children(Vec::new());
        let not_a_group = ResultChild::new(empty_group.task_id);

        assert_ne!(empty_group, not_a_group);
        assert_eq!(
            serde_json::to_value(&empty_group).expect("serialize")[1],
            json!([])
        );
        assert_eq!(
            serde_json::to_value(&not_a_group).expect("serialize")[1],
            json!(null)
        );
    }

    /// `Backend.current_task_children` returns `None` when a result is stored
    /// outside a task context (a client's `mark_as_failure`, a chord
    /// callback), so `"children": null` is a record a real backend writes.
    #[test]
    fn test_null_children_parse_as_no_children() {
        let parsed = ResultMessage::from_json(
            br#"{"task_id":"7b1a0d1e-0000-4000-8000-000000000001","status":"SUCCESS","children":null}"#,
        )
        .expect("a null children list must deserialize");

        assert!(parsed.children.is_empty());
    }

    /// A malformed child is an error, not a silently dropped one.
    #[test]
    fn test_a_malformed_child_is_rejected() {
        let err = ResultMessage::from_json(
            br#"{"task_id":"7b1a0d1e-0000-4000-8000-000000000001","status":"SUCCESS","children":[["not-a-uuid", null]]}"#,
        )
        .expect_err("a non-UUID child id must be rejected");
        assert!(err.to_string().contains("UUID"), "unexpected error: {err}");
    }

    #[test]
    fn test_result_message_with_retries() {
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Retry).with_retries(5);

        assert_eq!(result.retries, Some(5));
        assert_eq!(result.retry_count(), 5);
    }

    #[test]
    fn test_result_message_retry_count_default() {
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success);

        assert_eq!(result.retries, None);
        assert_eq!(result.retry_count(), 0); // Defaults to 0
    }

    #[test]
    fn test_result_message_with_date_done() {
        let now = chrono::Utc::now();
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success).with_date_done(now);

        assert_eq!(result.date_done, Some(now));
    }

    #[test]
    fn test_result_message_metadata() {
        let mut result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success);

        // Test add_meta (mutable)
        result.add_meta("key1", json!("value1"));
        result.add_meta("key2", json!(42));

        assert_eq!(result.meta_len(), 2);
        assert!(result.has_meta("key1"));
        assert!(result.has_meta("key2"));
        assert!(!result.has_meta("key3"));

        assert_eq!(result.get_meta("key1"), Some(&json!("value1")));
        assert_eq!(result.get_meta("key2"), Some(&json!(42)));
        assert_eq!(result.get_meta("key3"), None);
    }

    #[test]
    fn test_result_message_with_meta_builder() {
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success)
            .with_meta("version", json!("1.0.0"))
            .with_meta("region", json!("us-west-2"));

        assert_eq!(result.meta_len(), 2);
        assert_eq!(result.get_meta("version"), Some(&json!("1.0.0")));
        assert_eq!(result.get_meta("region"), Some(&json!("us-west-2")));
    }

    #[test]
    fn test_result_message_builder_chaining() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let now = chrono::Utc::now();

        let result = ResultMessage::success(task_id, json!({"data": 42}))
            .with_task("my.task")
            .with_worker("worker-1")
            .with_parent(parent_id)
            .with_root(root_id)
            .with_retries(3)
            .with_date_done(now)
            .with_meta("source", json!("api"));

        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Success);
        assert_eq!(result.task, Some("my.task".to_string()));
        assert_eq!(result.worker, Some("worker-1".to_string()));
        assert_eq!(result.parent_id, Some(parent_id));
        assert_eq!(result.root_id, Some(root_id));
        assert_eq!(result.retry_count(), 3);
        assert_eq!(result.date_done, Some(now));
        assert_eq!(result.get_meta("source"), Some(&json!("api")));
    }
}
