//! Structured, append-only audit log for task lifecycle events.
//!
//! This module provides a panic-free audit trail for the major lifecycle
//! transitions of a `CeleRS` task (sent, received, started, succeeded, failed,
//! retried, revoked). Each transition is captured as an [`AuditEntry`] carrying a
//! timestamp, the task identity, the [`AuditEventKind`], an optional actor/worker,
//! an optional before/after state pair, and a free-form metadata map.
//!
//! Two concrete [`AuditSink`] implementations are offered:
//!
//! * [`RingBufferAuditSink`] — an in-memory, bounded ring buffer that evicts the
//!   oldest entry once capacity is reached and supports querying/filtering by task
//!   id, event kind, and time range.
//! * [`JsonlAuditSink`] — an append-only [JSON Lines](https://jsonlines.org/)
//!   file sink that writes one JSON object per line and can reload/parse entries
//!   back, tolerating malformed lines.
//!
//! Consistent with the rest of `celers-metrics`, JSON is rendered and parsed by
//! hand (the crate does not depend on `serde`), timestamps are Unix seconds, and
//! locking never panics on poison.
//!
//! # Examples
//!
//! ```
//! use celers_metrics::{AuditEntry, AuditEventKind, AuditSink, RingBufferAuditSink};
//!
//! let sink = RingBufferAuditSink::new(128);
//! sink.record(
//!     AuditEntry::new("task-1", "send_email", AuditEventKind::Sent)
//!         .with_worker("worker-a")
//!         .with_metadata("queue", "default"),
//! );
//! sink.record(AuditEntry::new("task-1", "send_email", AuditEventKind::Succeeded));
//!
//! let history = sink.query_by_task_id("task-1");
//! assert_eq!(history.len(), 2);
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// The kind of task lifecycle event recorded in the audit log.
///
/// The variants mirror the canonical Celery task states that an external
/// observer can witness as a task moves through the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuditEventKind {
    /// The task was published to a broker / queue by a client.
    Sent,
    /// A worker received the task from the broker.
    Received,
    /// A worker began executing the task body.
    Started,
    /// The task finished successfully.
    Succeeded,
    /// The task finished with a permanent failure.
    Failed,
    /// The task was scheduled for another attempt after a failure.
    Retried,
    /// The task was revoked / cancelled before (or during) execution.
    Revoked,
}

impl AuditEventKind {
    /// Return the lowercase, stable string identifier for this event kind.
    ///
    /// The returned value is what is serialized to JSON and is the value
    /// accepted by [`AuditEventKind::from_str`].
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sent => "sent",
            Self::Received => "received",
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Retried => "retried",
            Self::Revoked => "revoked",
        }
    }

    /// Every event kind, in lifecycle order. Useful for iteration in tests and
    /// for building filter UIs.
    #[must_use]
    pub const fn all() -> [Self; 7] {
        [
            Self::Sent,
            Self::Received,
            Self::Started,
            Self::Succeeded,
            Self::Failed,
            Self::Retried,
            Self::Revoked,
        ]
    }

    /// Whether this event kind represents a terminal outcome of a task
    /// (`Succeeded`, `Failed`, or `Revoked`).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Revoked)
    }
}

impl fmt::Display for AuditEventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when a string cannot be parsed into an [`AuditEventKind`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEventKindError {
    /// The unrecognized input value.
    pub value: String,
}

impl fmt::Display for ParseEventKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown audit event kind: {:?}", self.value)
    }
}

impl std::error::Error for ParseEventKindError {}

impl FromStr for AuditEventKind {
    type Err = ParseEventKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sent" => Ok(Self::Sent),
            "received" => Ok(Self::Received),
            "started" => Ok(Self::Started),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "retried" => Ok(Self::Retried),
            "revoked" => Ok(Self::Revoked),
            other => Err(ParseEventKindError {
                value: other.to_string(),
            }),
        }
    }
}

/// Return the current Unix timestamp in seconds.
///
/// If the system clock is set before the Unix epoch the duration computation
/// fails; rather than panic we fall back to `0`, keeping audit recording
/// infallible.
fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// A single, immutable audit record describing one task lifecycle event.
///
/// Construct with [`AuditEntry::new`] (which stamps the current time) or
/// [`AuditEntry::with_timestamp`] for deterministic tests, then attach optional
/// context with the chained builder methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEntry {
    /// Unix timestamp (seconds) at which the event was recorded.
    pub timestamp: u64,
    /// The task's unique identifier (e.g. a UUID string).
    pub task_id: String,
    /// The registered name of the task (e.g. `app.tasks.send_email`).
    pub task_name: String,
    /// The lifecycle event that occurred.
    pub event: AuditEventKind,
    /// Optional actor or worker that caused or observed the event.
    pub actor: Option<String>,
    /// Optional state the task was in *before* this event.
    pub before_state: Option<String>,
    /// Optional state the task was in *after* this event.
    pub after_state: Option<String>,
    /// Free-form metadata captured alongside the event. A [`BTreeMap`] is used
    /// so serialization is deterministically ordered.
    pub metadata: BTreeMap<String, String>,
}

impl AuditEntry {
    /// Create a new entry stamped with the current Unix time.
    #[must_use]
    pub fn new(
        task_id: impl Into<String>,
        task_name: impl Into<String>,
        event: AuditEventKind,
    ) -> Self {
        Self::with_timestamp(current_unix_seconds(), task_id, task_name, event)
    }

    /// Create a new entry with an explicit timestamp.
    ///
    /// Primarily useful for deterministic tests and for backfilling historical
    /// events from another source.
    #[must_use]
    pub fn with_timestamp(
        timestamp: u64,
        task_id: impl Into<String>,
        task_name: impl Into<String>,
        event: AuditEventKind,
    ) -> Self {
        Self {
            timestamp,
            task_id: task_id.into(),
            task_name: task_name.into(),
            event,
            actor: None,
            before_state: None,
            after_state: None,
            metadata: BTreeMap::new(),
        }
    }

    /// Set the actor responsible for the event (builder style).
    #[must_use]
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Set the worker that observed the event (builder style).
    ///
    /// This is an alias for [`AuditEntry::with_actor`] expressed in worker
    /// terminology for readability at call sites.
    #[must_use]
    pub fn with_worker(self, worker: impl Into<String>) -> Self {
        self.with_actor(worker)
    }

    /// Set the before/after state transition (builder style).
    #[must_use]
    pub fn with_state_transition(
        mut self,
        before: impl Into<String>,
        after: impl Into<String>,
    ) -> Self {
        self.before_state = Some(before.into());
        self.after_state = Some(after.into());
        self
    }

    /// Insert a single metadata key/value pair (builder style).
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Override the timestamp (builder style).
    #[must_use]
    pub const fn at(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Serialize this entry to a single-line JSON object (no trailing newline).
    ///
    /// The output is a compact, dependency-free encoding using the same hand
    /// written approach as the rest of the crate. It round-trips through
    /// [`AuditEntry::from_json_line`].
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{AuditEntry, AuditEventKind};
    ///
    /// let entry = AuditEntry::with_timestamp(42, "t-1", "demo", AuditEventKind::Started);
    /// let line = entry.to_json_line();
    /// assert!(line.starts_with('{'));
    /// assert!(line.contains("\"event\":\"started\""));
    /// let parsed = AuditEntry::from_json_line(&line).expect("round-trip");
    /// assert_eq!(parsed, entry);
    /// ```
    #[must_use]
    pub fn to_json_line(&self) -> String {
        let mut out = String::with_capacity(96);
        out.push('{');

        out.push_str("\"timestamp\":");
        out.push_str(&self.timestamp.to_string());

        out.push_str(",\"task_id\":");
        push_json_string(&mut out, &self.task_id);

        out.push_str(",\"task_name\":");
        push_json_string(&mut out, &self.task_name);

        out.push_str(",\"event\":");
        push_json_string(&mut out, self.event.as_str());

        out.push_str(",\"actor\":");
        push_json_opt_string(&mut out, self.actor.as_deref());

        out.push_str(",\"before_state\":");
        push_json_opt_string(&mut out, self.before_state.as_deref());

        out.push_str(",\"after_state\":");
        push_json_opt_string(&mut out, self.after_state.as_deref());

        out.push_str(",\"metadata\":{");
        for (index, (key, value)) in self.metadata.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            push_json_string(&mut out, key);
            out.push(':');
            push_json_string(&mut out, value);
        }
        out.push('}');

        out.push('}');
        out
    }

    /// Parse a single JSON line produced by [`AuditEntry::to_json_line`].
    ///
    /// Returns an [`AuditParseError`] describing the first problem encountered.
    /// The parser is intentionally strict about the fields it emits but only
    /// requires `task_id`, `task_name`, and `event`; all other fields default
    /// when absent so that forward/backward compatible records still load.
    ///
    /// # Errors
    ///
    /// Returns [`AuditParseError`] when the line is not a JSON object, a required
    /// field is missing, or the `event` value is not a known [`AuditEventKind`].
    pub fn from_json_line(line: &str) -> Result<Self, AuditParseError> {
        let mut parser = JsonParser::new(line);
        let value = parser.parse_value()?;
        parser.skip_whitespace();
        if !parser.is_at_end() {
            return Err(AuditParseError::TrailingData);
        }

        let JsonValue::Object(mut fields) = value else {
            return Err(AuditParseError::NotAnObject);
        };

        let task_id = take_string_field(&mut fields, "task_id")?;
        let task_name = take_string_field(&mut fields, "task_name")?;
        let event_raw = take_string_field(&mut fields, "event")?;
        let event = AuditEventKind::from_str(&event_raw)
            .map_err(|_| AuditParseError::UnknownEvent(event_raw))?;

        let timestamp = match fields.remove("timestamp") {
            Some(JsonValue::Number(n)) => n as u64,
            Some(JsonValue::Null) | None => current_unix_seconds(),
            Some(_) => return Err(AuditParseError::InvalidField("timestamp")),
        };

        let actor = take_opt_string_field(&mut fields, "actor")?;
        let before_state = take_opt_string_field(&mut fields, "before_state")?;
        let after_state = take_opt_string_field(&mut fields, "after_state")?;

        let metadata = match fields.remove("metadata") {
            Some(JsonValue::Object(map)) => {
                let mut out = BTreeMap::new();
                for (key, value) in map {
                    let JsonValue::String(text) = value else {
                        return Err(AuditParseError::InvalidField("metadata"));
                    };
                    out.insert(key, text);
                }
                out
            }
            Some(JsonValue::Null) | None => BTreeMap::new(),
            Some(_) => return Err(AuditParseError::InvalidField("metadata")),
        };

        Ok(Self {
            timestamp,
            task_id,
            task_name,
            event,
            actor,
            before_state,
            after_state,
            metadata,
        })
    }
}

/// Remove and return a required string field, erroring if missing or wrong type.
fn take_string_field(
    fields: &mut BTreeMap<String, JsonValue>,
    name: &'static str,
) -> Result<String, AuditParseError> {
    match fields.remove(name) {
        Some(JsonValue::String(text)) => Ok(text),
        Some(_) => Err(AuditParseError::InvalidField(name)),
        None => Err(AuditParseError::MissingField(name)),
    }
}

/// Remove and return an optional string field, treating absent/null as `None`.
fn take_opt_string_field(
    fields: &mut BTreeMap<String, JsonValue>,
    name: &'static str,
) -> Result<Option<String>, AuditParseError> {
    match fields.remove(name) {
        Some(JsonValue::String(text)) => Ok(Some(text)),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(AuditParseError::InvalidField(name)),
    }
}

/// Append a JSON string literal (with escaping and surrounding quotes) to `out`.
fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Append either a JSON string literal or `null` for an optional value.
fn push_json_opt_string(out: &mut String, value: Option<&str>) {
    match value {
        Some(text) => push_json_string(out, text),
        None => out.push_str("null"),
    }
}

/// Errors that can occur while parsing an [`AuditEntry`] from JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditParseError {
    /// The line was empty or contained only whitespace.
    Empty,
    /// The top-level JSON value was not an object.
    NotAnObject,
    /// A required field was absent.
    MissingField(&'static str),
    /// A field was present but had the wrong JSON type.
    InvalidField(&'static str),
    /// The `event` field held a value that is not a known event kind.
    UnknownEvent(String),
    /// The input was not syntactically valid JSON.
    Syntax(String),
    /// Valid JSON was followed by extra, non-whitespace characters.
    TrailingData,
}

impl fmt::Display for AuditParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("empty audit line"),
            Self::NotAnObject => f.write_str("audit line is not a JSON object"),
            Self::MissingField(name) => write!(f, "missing required field: {name}"),
            Self::InvalidField(name) => write!(f, "field has invalid type: {name}"),
            Self::UnknownEvent(value) => write!(f, "unknown event kind: {value:?}"),
            Self::Syntax(detail) => write!(f, "JSON syntax error: {detail}"),
            Self::TrailingData => f.write_str("unexpected trailing data after JSON value"),
        }
    }
}

impl std::error::Error for AuditParseError {}

/// A reusable, time-range/kind/id filter for querying audit entries.
///
/// All criteria are optional and combined with logical AND. An empty filter
/// (the [`Default`]) matches every entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditQuery {
    /// Match only entries whose `task_id` equals this value.
    pub task_id: Option<String>,
    /// Match only entries whose `event` is one of these kinds (empty = any).
    pub event_kinds: Vec<AuditEventKind>,
    /// Inclusive lower bound on the entry timestamp (Unix seconds).
    pub start_time: Option<u64>,
    /// Inclusive upper bound on the entry timestamp (Unix seconds).
    pub end_time: Option<u64>,
}

impl AuditQuery {
    /// Create an empty query that matches every entry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict to a specific task id (builder style).
    #[must_use]
    pub fn task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    /// Add an event kind to the allowed set (builder style).
    #[must_use]
    pub fn event_kind(mut self, kind: AuditEventKind) -> Self {
        if !self.event_kinds.contains(&kind) {
            self.event_kinds.push(kind);
        }
        self
    }

    /// Set the inclusive lower time bound (builder style).
    #[must_use]
    pub const fn start_time(mut self, start: u64) -> Self {
        self.start_time = Some(start);
        self
    }

    /// Set the inclusive upper time bound (builder style).
    #[must_use]
    pub const fn end_time(mut self, end: u64) -> Self {
        self.end_time = Some(end);
        self
    }

    /// Return `true` if `entry` satisfies every configured criterion.
    #[must_use]
    pub fn matches(&self, entry: &AuditEntry) -> bool {
        if let Some(task_id) = &self.task_id {
            if &entry.task_id != task_id {
                return false;
            }
        }
        if !self.event_kinds.is_empty() && !self.event_kinds.contains(&entry.event) {
            return false;
        }
        if let Some(start) = self.start_time {
            if entry.timestamp < start {
                return false;
            }
        }
        if let Some(end) = self.end_time {
            if entry.timestamp > end {
                return false;
            }
        }
        true
    }
}

/// An append-only sink for [`AuditEntry`] records.
///
/// Implementations must be safe to call from multiple threads and must never
/// panic on a recording error; instead they should record what they can and
/// surface I/O failures through their own fallible methods where appropriate.
pub trait AuditSink: Send + Sync {
    /// Append a single entry to the audit trail.
    fn record(&self, entry: AuditEntry);

    /// Append several entries. The default implementation forwards to
    /// [`AuditSink::record`] but sinks may override it for batching.
    fn record_all(&self, entries: impl IntoIterator<Item = AuditEntry>)
    where
        Self: Sized,
    {
        for entry in entries {
            self.record(entry);
        }
    }

    /// Return the number of entries currently retained by this sink.
    ///
    /// For unbounded/streaming sinks this may reflect only the entries observed
    /// in the current process; consult the concrete type's documentation.
    fn len(&self) -> usize;

    /// Return `true` when [`AuditSink::len`] is zero.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// An in-memory, bounded ring buffer [`AuditSink`].
///
/// New entries are appended to the back; once `capacity` is reached the oldest
/// entry at the front is evicted to make room. All operations take a short
/// internal lock and never panic, even if a previous holder panicked (the poison
/// is recovered).
///
/// # Examples
///
/// ```
/// use celers_metrics::{AuditEntry, AuditEventKind, AuditSink, RingBufferAuditSink};
///
/// let sink = RingBufferAuditSink::new(2);
/// sink.record(AuditEntry::with_timestamp(1, "a", "t", AuditEventKind::Sent));
/// sink.record(AuditEntry::with_timestamp(2, "b", "t", AuditEventKind::Sent));
/// sink.record(AuditEntry::with_timestamp(3, "c", "t", AuditEventKind::Sent));
/// // The first entry was evicted.
/// assert_eq!(sink.len(), 2);
/// assert!(sink.query_by_task_id("a").is_empty());
/// ```
#[derive(Debug)]
pub struct RingBufferAuditSink {
    capacity: usize,
    entries: Mutex<VecDeque<AuditEntry>>,
}

impl RingBufferAuditSink {
    /// Create a ring buffer holding at most `capacity` entries.
    ///
    /// A `capacity` of `0` is promoted to `1` so the buffer always retains at
    /// least the most recent entry and recording remains meaningful.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// The maximum number of entries this buffer retains.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Lock the inner buffer, recovering from a poisoned mutex without panicking.
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<AuditEntry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Return a snapshot copy of all retained entries, oldest first.
    #[must_use]
    pub fn snapshot(&self) -> Vec<AuditEntry> {
        self.lock().iter().cloned().collect()
    }

    /// Remove every retained entry.
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Return all entries matching `query`, oldest first.
    #[must_use]
    pub fn query(&self, query: &AuditQuery) -> Vec<AuditEntry> {
        self.lock()
            .iter()
            .filter(|entry| query.matches(entry))
            .cloned()
            .collect()
    }

    /// Convenience: all entries for a given task id, oldest first.
    #[must_use]
    pub fn query_by_task_id(&self, task_id: &str) -> Vec<AuditEntry> {
        self.lock()
            .iter()
            .filter(|entry| entry.task_id == task_id)
            .cloned()
            .collect()
    }

    /// Convenience: all entries of a given event kind, oldest first.
    #[must_use]
    pub fn query_by_event(&self, kind: AuditEventKind) -> Vec<AuditEntry> {
        self.lock()
            .iter()
            .filter(|entry| entry.event == kind)
            .cloned()
            .collect()
    }

    /// Convenience: all entries with `start <= timestamp <= end`, oldest first.
    #[must_use]
    pub fn query_time_range(&self, start: u64, end: u64) -> Vec<AuditEntry> {
        self.lock()
            .iter()
            .filter(|entry| entry.timestamp >= start && entry.timestamp <= end)
            .cloned()
            .collect()
    }

    /// Count entries matching `query` without cloning them.
    #[must_use]
    pub fn count_matching(&self, query: &AuditQuery) -> usize {
        self.lock()
            .iter()
            .filter(|entry| query.matches(entry))
            .count()
    }

    /// Return the most recent entry, if any.
    #[must_use]
    pub fn latest(&self) -> Option<AuditEntry> {
        self.lock().back().cloned()
    }

    /// Return the oldest retained entry, if any.
    #[must_use]
    pub fn oldest(&self) -> Option<AuditEntry> {
        self.lock().front().cloned()
    }
}

impl AuditSink for RingBufferAuditSink {
    fn record(&self, entry: AuditEntry) {
        let mut entries = self.lock();
        while entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    fn len(&self) -> usize {
        self.lock().len()
    }
}

/// An append-only [JSON Lines](https://jsonlines.org/) file [`AuditSink`].
///
/// Each recorded entry is serialized via [`AuditEntry::to_json_line`] and written
/// as one line (terminated by `\n`) to the configured file, which is opened in
/// append mode. Entries can be reloaded with [`JsonlAuditSink::load_entries`],
/// which tolerates malformed lines.
///
/// Recording failures (e.g. a full disk) are swallowed by the infallible
/// [`AuditSink::record`] method to honour the panic-free contract; use
/// [`JsonlAuditSink::try_record`] when you need to observe I/O errors.
///
/// # Examples
///
/// ```
/// use celers_metrics::{AuditEntry, AuditEventKind, JsonlAuditSink};
///
/// let mut path = std::env::temp_dir();
/// path.push(format!("celers_audit_doctest_{}.jsonl", std::process::id()));
/// let _ = std::fs::remove_file(&path);
///
/// let sink = JsonlAuditSink::new(&path);
/// sink.try_record(&AuditEntry::with_timestamp(1, "t-1", "demo", AuditEventKind::Sent))
///     .expect("write");
///
/// let reloaded = sink.load_entries().expect("read");
/// assert_eq!(reloaded.len(), 1);
/// assert_eq!(reloaded[0].task_id, "t-1");
/// let _ = std::fs::remove_file(&path);
/// ```
#[derive(Debug)]
pub struct JsonlAuditSink {
    path: PathBuf,
    /// Serializes concurrent writers so interleaved lines cannot be produced.
    write_lock: Mutex<()>,
}

/// Outcome of reloading a JSONL audit file, including any lines that failed to
/// parse so callers can report or audit data corruption.
#[derive(Debug, Clone, Default)]
pub struct JsonlLoadReport {
    /// Successfully parsed entries, in file order.
    pub entries: Vec<AuditEntry>,
    /// `(line_number, error)` pairs for each line that could not be parsed.
    /// Blank lines are skipped silently and are not reported here.
    pub errors: Vec<(usize, AuditParseError)>,
}

impl JsonlLoadReport {
    /// `true` when every non-blank line parsed successfully.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }

    /// Number of successfully parsed entries.
    #[must_use]
    pub fn ok_count(&self) -> usize {
        self.entries.len()
    }

    /// Number of malformed lines that were skipped.
    #[must_use]
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }
}

impl JsonlAuditSink {
    /// Create a sink that appends to `path`.
    ///
    /// The file is created lazily on the first successful write; constructing the
    /// sink does not touch the filesystem.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Mutex::new(()),
        }
    }

    /// The path this sink writes to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a single entry, returning any I/O error.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`io::Error`] if the file cannot be opened or the
    /// line cannot be written/flushed.
    pub fn try_record(&self, entry: &AuditEntry) -> io::Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut line = entry.to_json_line();
        line.push('\n');
        file.write_all(line.as_bytes())?;
        file.flush()
    }

    /// Append several entries in one open/flush cycle.
    ///
    /// Writing stops at the first error, which is returned; entries already
    /// written before the failure remain in the file.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`io::Error`] from opening, writing, or flushing.
    pub fn try_record_all<'a, I>(&self, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = &'a AuditEntry>,
    {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut buffer = String::new();
        for entry in entries {
            buffer.clear();
            buffer.push_str(&entry.to_json_line());
            buffer.push('\n');
            file.write_all(buffer.as_bytes())?;
        }
        file.flush()
    }

    /// Reload and parse every valid entry from the file.
    ///
    /// Malformed lines are skipped silently; use [`JsonlAuditSink::load_report`]
    /// to additionally capture parse errors. A missing file is treated as an
    /// empty log (returns `Ok(vec![])`).
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] only for I/O failures other than "not found".
    pub fn load_entries(&self) -> io::Result<Vec<AuditEntry>> {
        Ok(self.load_report()?.entries)
    }

    /// Reload the file, returning both parsed entries and per-line parse errors.
    ///
    /// Blank lines are ignored. A missing file yields an empty, clean report. The
    /// parser is robust to malformed JSON: a bad line never aborts the load and
    /// never panics.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] only for I/O failures other than "not found".
    pub fn load_report(&self) -> io::Result<JsonlLoadReport> {
        let file = match OpenOptions::new().read(true).open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(JsonlLoadReport::default());
            }
            Err(err) => return Err(err),
        };

        let reader = BufReader::new(file);
        let mut report = JsonlLoadReport::default();
        for (index, line) in reader.lines().enumerate() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match AuditEntry::from_json_line(trimmed) {
                Ok(entry) => report.entries.push(entry),
                Err(err) => report.errors.push((index + 1, err)),
            }
        }
        Ok(report)
    }

    /// Load all entries (tolerating malformed lines) and return those matching
    /// `query`.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] only for I/O failures other than "not found".
    pub fn query(&self, query: &AuditQuery) -> io::Result<Vec<AuditEntry>> {
        let entries = self.load_entries()?;
        Ok(entries
            .into_iter()
            .filter(|entry| query.matches(entry))
            .collect())
    }

    /// Truncate the underlying file, discarding all recorded entries.
    ///
    /// A missing file is treated as already empty.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] for filesystem failures other than "not found".
    pub fn clear(&self) -> io::Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
        {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Number of successfully parsed entries currently in the file.
    ///
    /// Returns `0` on any I/O error so the infallible [`AuditSink`] contract is
    /// preserved; use [`JsonlAuditSink::load_report`] when error visibility is
    /// required.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.load_entries()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }
}

impl AuditSink for JsonlAuditSink {
    fn record(&self, entry: AuditEntry) {
        // The trait contract is infallible and panic-free: best-effort write,
        // dropping any I/O error. Callers needing error visibility use
        // `try_record`.
        let _ = self.try_record(&entry);
    }

    fn len(&self) -> usize {
        self.entry_count()
    }
}

// ============================================================================
// Minimal, dependency-free JSON value + parser
//
// The crate deliberately avoids a `serde` dependency, so a small recursive
// descent parser handles the subset of JSON that audit lines use: objects,
// strings, numbers, booleans, and null. It is panic-free and reports syntax
// errors as `AuditParseError`.
// ============================================================================

/// A parsed JSON value sufficient for decoding audit entries.
#[derive(Debug, Clone, PartialEq)]
enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

/// A small recursive-descent JSON parser over a `&str`.
struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
    source: &'a str,
}

impl<'a> JsonParser<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            bytes: source.as_bytes(),
            pos: 0,
            source,
        }
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.bytes.get(self.pos).copied();
        if byte.is_some() {
            self.pos += 1;
        }
        byte
    }

    fn skip_whitespace(&mut self) {
        while let Some(byte) = self.peek() {
            if byte == b' ' || byte == b'\t' || byte == b'\n' || byte == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, expected: u8) -> Result<(), AuditParseError> {
        match self.bump() {
            Some(byte) if byte == expected => Ok(()),
            Some(byte) => Err(AuditParseError::Syntax(format!(
                "expected {:?}, found {:?}",
                expected as char, byte as char
            ))),
            None => Err(AuditParseError::Syntax(format!(
                "expected {:?}, found end of input",
                expected as char
            ))),
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, AuditParseError> {
        self.skip_whitespace();
        match self.peek() {
            None => Err(AuditParseError::Empty),
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(JsonValue::String),
            Some(b't' | b'f') => self.parse_bool(),
            Some(b'n') => self.parse_null(),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            Some(other) => Err(AuditParseError::Syntax(format!(
                "unexpected character {:?}",
                other as char
            ))),
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, AuditParseError> {
        self.expect(b'{')?;
        let mut map = BTreeMap::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(JsonValue::Object(map));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(AuditParseError::Syntax("expected string key".to_string()));
            }
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect(b':')?;
            let value = self.parse_value()?;
            map.insert(key, value);
            self.skip_whitespace();
            match self.bump() {
                Some(b',') => {}
                Some(b'}') => break,
                Some(other) => {
                    return Err(AuditParseError::Syntax(format!(
                        "expected ',' or '}}' in object, found {:?}",
                        other as char
                    )));
                }
                None => {
                    return Err(AuditParseError::Syntax("unterminated object".to_string()));
                }
            }
        }
        Ok(JsonValue::Object(map))
    }

    fn parse_array(&mut self) -> Result<JsonValue, AuditParseError> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_whitespace();
            match self.bump() {
                Some(b',') => {}
                Some(b']') => break,
                Some(other) => {
                    return Err(AuditParseError::Syntax(format!(
                        "expected ',' or ']' in array, found {:?}",
                        other as char
                    )));
                }
                None => {
                    return Err(AuditParseError::Syntax("unterminated array".to_string()));
                }
            }
        }
        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, AuditParseError> {
        self.expect(b'"')?;
        let mut result = String::new();
        loop {
            match self.bump() {
                None => {
                    return Err(AuditParseError::Syntax("unterminated string".to_string()));
                }
                Some(b'"') => break,
                Some(b'\\') => {
                    let escaped = self
                        .bump()
                        .ok_or_else(|| AuditParseError::Syntax("dangling escape".to_string()))?;
                    match escaped {
                        b'"' => result.push('"'),
                        b'\\' => result.push('\\'),
                        b'/' => result.push('/'),
                        b'n' => result.push('\n'),
                        b'r' => result.push('\r'),
                        b't' => result.push('\t'),
                        b'b' => result.push('\u{08}'),
                        b'f' => result.push('\u{0c}'),
                        b'u' => {
                            let code = self.parse_unicode_escape()?;
                            result.push(code);
                        }
                        other => {
                            return Err(AuditParseError::Syntax(format!(
                                "invalid escape \\{}",
                                other as char
                            )));
                        }
                    }
                }
                Some(byte) if byte < 0x80 => result.push(byte as char),
                Some(_) => {
                    // Multi-byte UTF-8: decode the full sequence from the source
                    // string starting at the byte we just consumed.
                    let start = self.pos - 1;
                    let ch = self.decode_utf8_at(start)?;
                    // Advance past the remaining continuation bytes.
                    self.pos = start + ch.len_utf8();
                    result.push(ch);
                }
            }
        }
        Ok(result)
    }

    /// Decode a single UTF-8 scalar value beginning at byte offset `start`.
    fn decode_utf8_at(&self, start: usize) -> Result<char, AuditParseError> {
        self.source[start..]
            .chars()
            .next()
            .ok_or_else(|| AuditParseError::Syntax("invalid UTF-8 in string".to_string()))
    }

    fn parse_unicode_escape(&mut self) -> Result<char, AuditParseError> {
        let high = self.read_hex4()?;
        // Handle surrogate pairs for characters outside the BMP.
        if (0xD800..=0xDBFF).contains(&high) {
            if self.bump() != Some(b'\\') || self.bump() != Some(b'u') {
                return Err(AuditParseError::Syntax(
                    "expected low surrogate after high surrogate".to_string(),
                ));
            }
            let low = self.read_hex4()?;
            if !(0xDC00..=0xDFFF).contains(&low) {
                return Err(AuditParseError::Syntax("invalid low surrogate".to_string()));
            }
            let combined =
                0x1_0000 + ((u32::from(high) - 0xD800) << 10) + (u32::from(low) - 0xDC00);
            char::from_u32(combined)
                .ok_or_else(|| AuditParseError::Syntax("invalid surrogate pair".to_string()))
        } else if (0xDC00..=0xDFFF).contains(&high) {
            Err(AuditParseError::Syntax(
                "unexpected low surrogate".to_string(),
            ))
        } else {
            char::from_u32(u32::from(high))
                .ok_or_else(|| AuditParseError::Syntax("invalid unicode escape".to_string()))
        }
    }

    fn read_hex4(&mut self) -> Result<u16, AuditParseError> {
        let mut value: u16 = 0;
        for _ in 0..4 {
            let byte = self
                .bump()
                .ok_or_else(|| AuditParseError::Syntax("short unicode escape".to_string()))?;
            let digit = match byte {
                b'0'..=b'9' => u16::from(byte - b'0'),
                b'a'..=b'f' => u16::from(byte - b'a' + 10),
                b'A'..=b'F' => u16::from(byte - b'A' + 10),
                other => {
                    return Err(AuditParseError::Syntax(format!(
                        "invalid hex digit {:?}",
                        other as char
                    )));
                }
            };
            value = value * 16 + digit;
        }
        Ok(value)
    }

    fn parse_bool(&mut self) -> Result<JsonValue, AuditParseError> {
        if self.source[self.pos..].starts_with("true") {
            self.pos += 4;
            Ok(JsonValue::Bool(true))
        } else if self.source[self.pos..].starts_with("false") {
            self.pos += 5;
            Ok(JsonValue::Bool(false))
        } else {
            Err(AuditParseError::Syntax(
                "invalid boolean literal".to_string(),
            ))
        }
    }

    fn parse_null(&mut self) -> Result<JsonValue, AuditParseError> {
        if self.source[self.pos..].starts_with("null") {
            self.pos += 4;
            Ok(JsonValue::Null)
        } else {
            Err(AuditParseError::Syntax("invalid null literal".to_string()))
        }
    }

    fn parse_number(&mut self) -> Result<JsonValue, AuditParseError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(byte) = self.peek() {
            if byte.is_ascii_digit()
                || byte == b'.'
                || byte == b'e'
                || byte == b'E'
                || byte == b'+'
                || byte == b'-'
            {
                self.pos += 1;
            } else {
                break;
            }
        }
        let slice = &self.source[start..self.pos];
        slice
            .parse::<f64>()
            .map(JsonValue::Number)
            .map_err(|_| AuditParseError::Syntax(format!("invalid number {slice:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    /// Build a deterministic entry with the given timestamp/id/event.
    fn entry(ts: u64, id: &str, event: AuditEventKind) -> AuditEntry {
        AuditEntry::with_timestamp(ts, id, "demo.task", event)
    }

    /// Allocate a unique temp path under the OS temp dir for file-based tests.
    fn temp_path(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_audit_{tag}_{}_{nanos}.jsonl",
            std::process::id()
        ));
        path
    }

    #[test]
    fn event_kind_string_round_trip() {
        for kind in AuditEventKind::all() {
            let text = kind.as_str();
            let parsed = AuditEventKind::from_str(text).expect("known kind parses");
            assert_eq!(parsed, kind);
            assert_eq!(format!("{kind}"), text);
        }
    }

    #[test]
    fn event_kind_unknown_is_error() {
        let err = AuditEventKind::from_str("exploded").unwrap_err();
        assert_eq!(err.value, "exploded");
        assert!(format!("{err}").contains("exploded"));
    }

    #[test]
    fn event_kind_terminal_classification() {
        assert!(AuditEventKind::Succeeded.is_terminal());
        assert!(AuditEventKind::Failed.is_terminal());
        assert!(AuditEventKind::Revoked.is_terminal());
        assert!(!AuditEventKind::Sent.is_terminal());
        assert!(!AuditEventKind::Started.is_terminal());
    }

    #[test]
    fn entry_builders_set_fields() {
        let e = AuditEntry::new("task-9", "demo.task", AuditEventKind::Started)
            .with_worker("worker-7")
            .with_state_transition("received", "started")
            .with_metadata("queue", "high")
            .with_metadata("attempt", "1")
            .at(123);
        assert_eq!(e.timestamp, 123);
        assert_eq!(e.task_id, "task-9");
        assert_eq!(e.actor.as_deref(), Some("worker-7"));
        assert_eq!(e.before_state.as_deref(), Some("received"));
        assert_eq!(e.after_state.as_deref(), Some("started"));
        assert_eq!(e.metadata.get("queue").map(String::as_str), Some("high"));
        assert_eq!(e.metadata.get("attempt").map(String::as_str), Some("1"));
    }

    #[test]
    fn json_line_round_trip_full() {
        let original = AuditEntry::with_timestamp(987, "id-x", "pkg.task", AuditEventKind::Failed)
            .with_actor("worker-α")
            .with_state_transition("started", "failed")
            .with_metadata("error", "boom: \"quoted\"\n\ttabbed")
            .with_metadata("emoji", "🚀");
        let line = original.to_json_line();
        assert!(!line.contains('\n')); // newlines escaped, single line
        let parsed = AuditEntry::from_json_line(&line).expect("round-trip");
        assert_eq!(parsed, original);
    }

    #[test]
    fn json_line_round_trip_minimal() {
        let original = entry(1, "min", AuditEventKind::Sent);
        let line = original.to_json_line();
        let parsed = AuditEntry::from_json_line(&line).expect("round-trip");
        assert_eq!(parsed, original);
        assert!(parsed.actor.is_none());
        assert!(parsed.metadata.is_empty());
    }

    #[test]
    fn json_parse_missing_required_field() {
        let line = r#"{"task_id":"a","event":"sent"}"#; // no task_name
        let err = AuditEntry::from_json_line(line).unwrap_err();
        assert_eq!(err, AuditParseError::MissingField("task_name"));
    }

    #[test]
    fn json_parse_unknown_event() {
        let line = r#"{"task_id":"a","task_name":"t","event":"melted"}"#;
        let err = AuditEntry::from_json_line(line).unwrap_err();
        assert_eq!(err, AuditParseError::UnknownEvent("melted".to_string()));
    }

    #[test]
    fn json_parse_not_an_object() {
        let err = AuditEntry::from_json_line("[1,2,3]").unwrap_err();
        assert_eq!(err, AuditParseError::NotAnObject);
    }

    #[test]
    fn json_parse_trailing_data_rejected() {
        let line = r#"{"task_id":"a","task_name":"t","event":"sent"} extra"#;
        let err = AuditEntry::from_json_line(line).unwrap_err();
        assert_eq!(err, AuditParseError::TrailingData);
    }

    #[test]
    fn json_parse_garbage_is_syntax_error() {
        let err = AuditEntry::from_json_line("{not json").unwrap_err();
        assert!(matches!(err, AuditParseError::Syntax(_)));
    }

    #[test]
    fn json_parse_default_timestamp_when_absent() {
        let line = r#"{"task_id":"a","task_name":"t","event":"sent"}"#;
        let parsed = AuditEntry::from_json_line(line).expect("parse");
        // Falls back to current time, which is well after the epoch.
        assert!(parsed.timestamp > 0);
    }

    #[test]
    fn query_matches_combination() {
        let e = AuditEntry::with_timestamp(100, "t-1", "demo", AuditEventKind::Started);
        let q = AuditQuery::new()
            .task_id("t-1")
            .event_kind(AuditEventKind::Started)
            .start_time(50)
            .end_time(150);
        assert!(q.matches(&e));

        assert!(!AuditQuery::new().task_id("other").matches(&e));
        assert!(!AuditQuery::new()
            .event_kind(AuditEventKind::Failed)
            .matches(&e));
        assert!(!AuditQuery::new().start_time(101).matches(&e));
        assert!(!AuditQuery::new().end_time(99).matches(&e));
        assert!(AuditQuery::new().matches(&e)); // empty matches all
    }

    #[test]
    fn ring_buffer_records_and_queries_by_id() {
        let sink = RingBufferAuditSink::new(16);
        sink.record(entry(1, "a", AuditEventKind::Sent));
        sink.record(entry(2, "b", AuditEventKind::Sent));
        sink.record(entry(3, "a", AuditEventKind::Succeeded));

        let a = sink.query_by_task_id("a");
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].event, AuditEventKind::Sent);
        assert_eq!(a[1].event, AuditEventKind::Succeeded);
        assert_eq!(sink.query_by_task_id("b").len(), 1);
        assert_eq!(sink.query_by_task_id("missing").len(), 0);
    }

    #[test]
    fn ring_buffer_queries_by_event_and_range() {
        let sink = RingBufferAuditSink::new(16);
        sink.record(entry(10, "a", AuditEventKind::Sent));
        sink.record(entry(20, "b", AuditEventKind::Failed));
        sink.record(entry(30, "c", AuditEventKind::Failed));
        sink.record(entry(40, "d", AuditEventKind::Succeeded));

        assert_eq!(sink.query_by_event(AuditEventKind::Failed).len(), 2);
        let ranged = sink.query_time_range(20, 30);
        assert_eq!(ranged.len(), 2);
        assert_eq!(ranged[0].timestamp, 20);
        assert_eq!(ranged[1].timestamp, 30);

        let q = AuditQuery::new()
            .event_kind(AuditEventKind::Failed)
            .start_time(25);
        assert_eq!(sink.query(&q).len(), 1);
        assert_eq!(sink.count_matching(&q), 1);
    }

    #[test]
    fn ring_buffer_evicts_oldest_at_capacity() {
        let sink = RingBufferAuditSink::new(3);
        for i in 1..=5 {
            sink.record(entry(i, &format!("task-{i}"), AuditEventKind::Sent));
        }
        assert_eq!(sink.len(), 3);
        // Oldest two (ts 1,2) evicted; 3,4,5 remain.
        assert!(sink.query_by_task_id("task-1").is_empty());
        assert!(sink.query_by_task_id("task-2").is_empty());
        assert_eq!(sink.oldest().map(|e| e.timestamp), Some(3));
        assert_eq!(sink.latest().map(|e| e.timestamp), Some(5));
        let snapshot = sink.snapshot();
        assert_eq!(
            snapshot.iter().map(|e| e.timestamp).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    #[test]
    fn ring_buffer_zero_capacity_promoted_to_one() {
        let sink = RingBufferAuditSink::new(0);
        assert_eq!(sink.capacity(), 1);
        sink.record(entry(1, "a", AuditEventKind::Sent));
        sink.record(entry(2, "b", AuditEventKind::Sent));
        assert_eq!(sink.len(), 1);
        assert_eq!(sink.latest().map(|e| e.task_id), Some("b".to_string()));
    }

    #[test]
    fn ring_buffer_clear_and_empty() {
        let sink = RingBufferAuditSink::new(4);
        assert!(sink.is_empty());
        sink.record(entry(1, "a", AuditEventKind::Sent));
        assert!(!sink.is_empty());
        sink.clear();
        assert!(sink.is_empty());
        assert_eq!(sink.len(), 0);
    }

    #[test]
    fn ring_buffer_record_all_via_trait() {
        let sink = RingBufferAuditSink::new(8);
        let batch = vec![
            entry(1, "a", AuditEventKind::Sent),
            entry(2, "a", AuditEventKind::Received),
            entry(3, "a", AuditEventKind::Started),
        ];
        sink.record_all(batch);
        assert_eq!(sink.len(), 3);
    }

    #[test]
    fn ring_buffer_is_thread_safe() {
        let sink = Arc::new(RingBufferAuditSink::new(10_000));
        let mut handles = Vec::new();
        for t in 0u64..8 {
            let sink = Arc::clone(&sink);
            handles.push(thread::spawn(move || {
                for i in 0u64..100 {
                    sink.record(entry(
                        t * 1000 + i,
                        &format!("t{t}-{i}"),
                        AuditEventKind::Sent,
                    ));
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread joins");
        }
        assert_eq!(sink.len(), 800);
    }

    #[test]
    fn jsonl_round_trip_write_then_read() {
        let path = temp_path("round_trip");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlAuditSink::new(&path);

        let entries = vec![
            entry(1, "a", AuditEventKind::Sent).with_metadata("k", "v"),
            entry(2, "a", AuditEventKind::Started).with_worker("w-1"),
            entry(3, "a", AuditEventKind::Succeeded),
        ];
        for e in &entries {
            sink.try_record(e).expect("write line");
        }

        let reloaded = sink.load_entries().expect("reload");
        assert_eq!(reloaded, entries);
        assert_eq!(sink.entry_count(), 3);
        assert_eq!(sink.len(), 3);
        assert!(!sink.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_append_accumulates_across_sinks() {
        let path = temp_path("append");
        let _ = std::fs::remove_file(&path);

        {
            let sink = JsonlAuditSink::new(&path);
            sink.try_record(&entry(1, "a", AuditEventKind::Sent))
                .expect("write");
        }
        {
            // A fresh sink pointed at the same path must append, not overwrite.
            let sink = JsonlAuditSink::new(&path);
            sink.try_record(&entry(2, "b", AuditEventKind::Sent))
                .expect("write");
            let all = sink.load_entries().expect("reload");
            assert_eq!(all.len(), 2);
            assert_eq!(all[0].task_id, "a");
            assert_eq!(all[1].task_id, "b");
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_record_all_batch() {
        let path = temp_path("batch");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlAuditSink::new(&path);
        let batch = vec![
            entry(1, "a", AuditEventKind::Sent),
            entry(2, "b", AuditEventKind::Received),
        ];
        sink.try_record_all(batch.iter()).expect("batch write");
        assert_eq!(sink.load_entries().expect("reload"), batch);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_tolerates_malformed_lines() {
        let path = temp_path("malformed");
        let _ = std::fs::remove_file(&path);

        let good1 = entry(1, "a", AuditEventKind::Sent).to_json_line();
        let good2 = entry(2, "b", AuditEventKind::Failed).to_json_line();
        let contents = format!(
            "{good1}\n\
             this is not json\n\
             \n\
             {{\"partial\": true\n\
             {{\"task_id\":\"c\",\"task_name\":\"t\",\"event\":\"banana\"}}\n\
             {good2}\n"
        );
        std::fs::write(&path, contents).expect("seed file");

        let report = sink_load_report(&path);
        assert_eq!(report.ok_count(), 2);
        assert!(report.error_count() >= 2);
        assert!(!report.is_clean());
        assert_eq!(report.entries[0].task_id, "a");
        assert_eq!(report.entries[1].task_id, "b");

        // load_entries should silently skip the bad lines and return the 2 good.
        let entries = JsonlAuditSink::new(&path).load_entries().expect("reload");
        assert_eq!(entries.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    /// Helper that loads a report for `path` (keeps the test body readable).
    fn sink_load_report(path: &Path) -> JsonlLoadReport {
        JsonlAuditSink::new(path)
            .load_report()
            .expect("load report")
    }

    #[test]
    fn jsonl_missing_file_is_empty_clean() {
        let path = temp_path("absent");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlAuditSink::new(&path);
        assert_eq!(sink.load_entries().expect("empty"), Vec::new());
        assert!(sink.load_report().expect("empty").is_clean());
        assert_eq!(sink.len(), 0);
        assert!(sink.is_empty());
        assert_eq!(sink.entry_count(), 0);
    }

    #[test]
    fn jsonl_query_filters_loaded_entries() {
        let path = temp_path("query");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlAuditSink::new(&path);
        sink.try_record_all(
            [
                entry(10, "a", AuditEventKind::Sent),
                entry(20, "a", AuditEventKind::Failed),
                entry(30, "b", AuditEventKind::Failed),
            ]
            .iter(),
        )
        .expect("write");

        let by_id = sink
            .query(&AuditQuery::new().task_id("a"))
            .expect("query id");
        assert_eq!(by_id.len(), 2);

        let by_kind_range = sink
            .query(
                &AuditQuery::new()
                    .event_kind(AuditEventKind::Failed)
                    .start_time(25),
            )
            .expect("query kind+range");
        assert_eq!(by_kind_range.len(), 1);
        assert_eq!(by_kind_range[0].task_id, "b");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_clear_truncates_file() {
        let path = temp_path("clear");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlAuditSink::new(&path);
        sink.try_record(&entry(1, "a", AuditEventKind::Sent))
            .expect("write");
        assert_eq!(sink.entry_count(), 1);
        sink.clear().expect("clear");
        assert_eq!(sink.entry_count(), 0);
        // Clearing an already-missing file is a no-op.
        let _ = std::fs::remove_file(&path);
        sink.clear().expect("clear missing");
    }

    #[test]
    fn jsonl_record_via_trait_is_infallible() {
        let path = temp_path("trait");
        let _ = std::fs::remove_file(&path);
        let sink = JsonlAuditSink::new(&path);
        // The infallible trait method must not panic and must persist the entry.
        AuditSink::record(&sink, entry(1, "a", AuditEventKind::Sent));
        assert_eq!(sink.load_entries().expect("reload").len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn audit_sink_trait_object_usable() {
        // Confirm both sinks are object-safe behind a trait object.
        let sinks: Vec<Box<dyn AuditSink>> = vec![Box::new(RingBufferAuditSink::new(4))];
        for sink in &sinks {
            sink.record(entry(1, "a", AuditEventKind::Sent));
            assert_eq!(sink.len(), 1);
            assert!(!sink.is_empty());
        }
    }

    #[test]
    fn json_string_escaping_handles_control_chars() {
        let mut out = String::new();
        push_json_string(&mut out, "a\u{0001}b");
        assert!(out.contains("\\u0001"));
        // And it parses back to the original character.
        let entry = entry(1, "a\u{0001}b", AuditEventKind::Sent);
        let parsed = AuditEntry::from_json_line(&entry.to_json_line()).expect("round-trip");
        assert_eq!(parsed.task_id, "a\u{0001}b");
    }
}
