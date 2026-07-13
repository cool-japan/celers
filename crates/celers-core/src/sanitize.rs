//! Task argument sanitization.
//!
//! Task arguments and keyword arguments frequently originate from untrusted
//! callers (HTTP handlers, message brokers, user input). Before such a payload
//! is logged, persisted, or executed it is prudent to:
//!
//! * **bound its size** — reject pathologically large strings/blobs and
//!   excessively long argument lists (a cheap denial-of-service vector);
//! * **strip control characters** from strings so they cannot corrupt logs,
//!   terminals, or downstream parsers (log injection / ANSI escape attacks);
//! * **reject disallowed value kinds** — e.g. forbid raw binary blobs or
//!   floating-point arguments in contexts that only expect scalars;
//! * **redact secret-looking keys** — values stored under keys such as
//!   `password`, `token`, `secret`, or `api_key` should never be retained in
//!   plaintext within stored task metadata.
//!
//! The work is driven by a [`SanitizerConfig`] so each deployment can tune the
//! policy, and the [`Sanitizer`] returns a [`SanitizeReport`] describing every
//! action it took (useful for audit logging and tests).
//!
//! All of this operates over a small, JSON-like value model — [`TaskValue`] —
//! which is shared by the signature and PII modules so the three security
//! features compose over a single representation.
//!
//! # Example
//!
//! ```rust
//! use celers_core::sanitize::{Sanitizer, SanitizerConfig, TaskValue};
//!
//! let sanitizer = Sanitizer::new(SanitizerConfig::default());
//!
//! let mut kwargs = vec![
//!     ("username".to_string(), TaskValue::from("alice")),
//!     ("password".to_string(), TaskValue::from("hunter2")),
//! ];
//! let report = sanitizer.sanitize_kwargs(&mut kwargs).unwrap();
//!
//! // The secret value is redacted in place.
//! assert_eq!(kwargs[1].1, TaskValue::from("[REDACTED]"));
//! assert_eq!(report.redacted_keys, 1);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

/// A JSON-like value used to represent a single task argument.
///
/// This intentionally mirrors the shape of a serialized argument (the kind of
/// thing you would get from `serde_json::Value`) while keeping integer and
/// byte information that JSON would lose. It is the common currency of the
/// security modules: signatures canonicalize it, the sanitizer cleans it, and
/// the PII detector scans the strings inside it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TaskValue {
    /// Absence of a value (`null`).
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Int(i64),
    /// Unsigned 64-bit integer (for values exceeding `i64::MAX`).
    UInt(u64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes (binary blob).
    Bytes(Vec<u8>),
    /// Ordered list of values.
    Array(Vec<TaskValue>),
    /// Ordered key/value map. Kept as a `Vec` of pairs so insertion order is
    /// preserved for display; canonicalization and lookups sort by key.
    Object(Vec<(String, TaskValue)>),
}

impl TaskValue {
    /// Human-readable name of this value's kind.
    #[must_use]
    pub const fn kind(&self) -> ValueKind {
        match self {
            TaskValue::Null => ValueKind::Null,
            TaskValue::Bool(_) => ValueKind::Bool,
            TaskValue::Int(_) | TaskValue::UInt(_) => ValueKind::Integer,
            TaskValue::Float(_) => ValueKind::Float,
            TaskValue::String(_) => ValueKind::String,
            TaskValue::Bytes(_) => ValueKind::Bytes,
            TaskValue::Array(_) => ValueKind::Array,
            TaskValue::Object(_) => ValueKind::Object,
        }
    }

    /// Borrow the inner string, if this is a [`TaskValue::String`].
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            TaskValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Approximate in-memory "weight" of this value in bytes, used for size
    /// limiting. Containers sum their children plus a small per-element
    /// overhead so deeply nested structures are accounted for.
    #[must_use]
    pub fn approx_size(&self) -> usize {
        match self {
            TaskValue::Null => 1,
            TaskValue::Bool(_) => 1,
            TaskValue::Int(_) | TaskValue::UInt(_) | TaskValue::Float(_) => 8,
            TaskValue::String(s) => s.len(),
            TaskValue::Bytes(b) => b.len(),
            TaskValue::Array(items) => items.iter().map(|v| v.approx_size() + 1).sum::<usize>() + 1,
            TaskValue::Object(entries) => {
                entries
                    .iter()
                    .map(|(k, v)| k.len() + v.approx_size() + 1)
                    .sum::<usize>()
                    + 1
            }
        }
    }

    /// Maximum container nesting depth (a scalar has depth 1).
    #[must_use]
    pub fn depth(&self) -> usize {
        match self {
            TaskValue::Array(items) => 1 + items.iter().map(TaskValue::depth).max().unwrap_or(0),
            TaskValue::Object(entries) => {
                1 + entries.iter().map(|(_, v)| v.depth()).max().unwrap_or(0)
            }
            _ => 1,
        }
    }
}

impl From<bool> for TaskValue {
    fn from(v: bool) -> Self {
        TaskValue::Bool(v)
    }
}
impl From<i64> for TaskValue {
    fn from(v: i64) -> Self {
        TaskValue::Int(v)
    }
}
impl From<i32> for TaskValue {
    fn from(v: i32) -> Self {
        TaskValue::Int(i64::from(v))
    }
}
impl From<u64> for TaskValue {
    fn from(v: u64) -> Self {
        TaskValue::UInt(v)
    }
}
impl From<f64> for TaskValue {
    fn from(v: f64) -> Self {
        TaskValue::Float(v)
    }
}
impl From<&str> for TaskValue {
    fn from(v: &str) -> Self {
        TaskValue::String(v.to_string())
    }
}
impl From<String> for TaskValue {
    fn from(v: String) -> Self {
        TaskValue::String(v)
    }
}
impl From<Vec<u8>> for TaskValue {
    fn from(v: Vec<u8>) -> Self {
        TaskValue::Bytes(v)
    }
}

impl From<serde_json::Value> for TaskValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => TaskValue::Null,
            serde_json::Value::Bool(b) => TaskValue::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    TaskValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    TaskValue::UInt(u)
                } else if let Some(f) = n.as_f64() {
                    TaskValue::Float(f)
                } else {
                    // Unreachable in practice, but stay total.
                    TaskValue::Null
                }
            }
            serde_json::Value::String(s) => TaskValue::String(s),
            serde_json::Value::Array(items) => {
                TaskValue::Array(items.into_iter().map(TaskValue::from).collect())
            }
            serde_json::Value::Object(map) => TaskValue::Object(
                map.into_iter()
                    .map(|(k, v)| (k, TaskValue::from(v)))
                    .collect(),
            ),
        }
    }
}

/// The discriminant of a [`TaskValue`], independent of its contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ValueKind {
    /// `null`.
    Null,
    /// Boolean.
    Bool,
    /// Any integer (signed or unsigned).
    Integer,
    /// Floating point.
    Float,
    /// UTF-8 string.
    String,
    /// Binary blob.
    Bytes,
    /// List.
    Array,
    /// Map.
    Object,
}

impl fmt::Display for ValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ValueKind::Null => "null",
            ValueKind::Bool => "bool",
            ValueKind::Integer => "integer",
            ValueKind::Float => "float",
            ValueKind::String => "string",
            ValueKind::Bytes => "bytes",
            ValueKind::Array => "array",
            ValueKind::Object => "object",
        };
        f.write_str(name)
    }
}

/// Error returned when a value violates a hard sanitizer limit.
///
/// "Soft" actions (stripping control characters, redacting secrets, truncating
/// when configured to do so) do not error — they are recorded in the
/// [`SanitizeReport`]. Errors are reserved for policy violations the caller
/// asked to be treated as fatal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SanitizeError {
    /// A string or blob exceeded `max_string_bytes` and truncation was
    /// disabled.
    ValueTooLarge {
        /// Observed size in bytes.
        size: usize,
        /// Configured limit in bytes.
        limit: usize,
    },
    /// The total number of positional + keyword arguments exceeded
    /// `max_arg_count`.
    TooManyArgs {
        /// Observed argument count.
        count: usize,
        /// Configured limit.
        limit: usize,
    },
    /// Container nesting exceeded `max_depth`.
    TooDeep {
        /// Observed depth.
        depth: usize,
        /// Configured limit.
        limit: usize,
    },
    /// A value of a disallowed kind was encountered.
    DisallowedKind(ValueKind),
    /// A key was longer than `max_key_bytes`.
    KeyTooLong {
        /// Observed key length in bytes.
        size: usize,
        /// Configured limit.
        limit: usize,
    },
}

impl fmt::Display for SanitizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SanitizeError::ValueTooLarge { size, limit } => {
                write!(f, "value too large: {size} bytes exceeds limit of {limit}")
            }
            SanitizeError::TooManyArgs { count, limit } => {
                write!(f, "too many arguments: {count} exceeds limit of {limit}")
            }
            SanitizeError::TooDeep { depth, limit } => {
                write!(
                    f,
                    "value nested too deeply: {depth} exceeds limit of {limit}"
                )
            }
            SanitizeError::DisallowedKind(kind) => {
                write!(f, "disallowed argument kind: {kind}")
            }
            SanitizeError::KeyTooLong { size, limit } => {
                write!(f, "key too long: {size} bytes exceeds limit of {limit}")
            }
        }
    }
}

impl std::error::Error for SanitizeError {}

impl From<SanitizeError> for crate::CelersError {
    fn from(err: SanitizeError) -> Self {
        crate::CelersError::Other(format!("sanitize error: {err}"))
    }
}

/// What to do when a string exceeds `max_string_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OversizeAction {
    /// Return a [`SanitizeError::ValueTooLarge`].
    Reject,
    /// Truncate the string/blob to the limit and record it in the report.
    Truncate,
}

/// Configuration controlling how a [`Sanitizer`] processes arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizerConfig {
    /// Maximum number of positional + keyword arguments allowed.
    pub max_arg_count: usize,

    /// Maximum size, in bytes, of any individual string or byte blob.
    pub max_string_bytes: usize,

    /// Maximum length, in bytes, of any keyword-argument key.
    pub max_key_bytes: usize,

    /// Maximum container nesting depth.
    pub max_depth: usize,

    /// What to do with strings/blobs exceeding `max_string_bytes`.
    pub oversize_action: OversizeAction,

    /// Whether to strip ASCII/Unicode control characters from strings.
    pub strip_control_chars: bool,

    /// Value kinds that are rejected outright. Empty means "allow all".
    pub disallowed_kinds: BTreeSet<ValueKind>,

    /// Lower-cased substrings that mark a key as secret; matching keys have
    /// their values replaced with [`SanitizerConfig::redaction_placeholder`].
    pub secret_key_markers: BTreeSet<String>,

    /// Placeholder value substituted for redacted secrets.
    pub redaction_placeholder: String,
}

impl Default for SanitizerConfig {
    fn default() -> Self {
        let mut disallowed = BTreeSet::new();
        // Binary blobs are rejected by default: most task arguments should be
        // structured, and large opaque blobs are a common abuse vector.
        disallowed.insert(ValueKind::Bytes);

        let markers = [
            "password",
            "passwd",
            "secret",
            "token",
            "api_key",
            "apikey",
            "access_key",
            "private",
            "credential",
            "auth",
            "session",
            "cookie",
        ];
        let secret_key_markers = markers.iter().map(|s| (*s).to_string()).collect();

        Self {
            max_arg_count: 64,
            max_string_bytes: 64 * 1024,
            max_key_bytes: 256,
            max_depth: 16,
            oversize_action: OversizeAction::Reject,
            strip_control_chars: true,
            disallowed_kinds: disallowed,
            secret_key_markers,
            redaction_placeholder: "[REDACTED]".to_string(),
        }
    }
}

impl SanitizerConfig {
    /// A permissive configuration: high limits, no disallowed kinds, truncate
    /// rather than reject, but still strip control characters and redact
    /// secrets (these are cheap and almost always desirable).
    #[must_use]
    pub fn permissive() -> Self {
        Self {
            max_arg_count: 4096,
            max_string_bytes: 8 * 1024 * 1024,
            max_key_bytes: 4096,
            max_depth: 64,
            oversize_action: OversizeAction::Truncate,
            strip_control_chars: true,
            disallowed_kinds: BTreeSet::new(),
            ..Self::default()
        }
    }

    /// Set the maximum argument count.
    #[must_use]
    pub const fn with_max_arg_count(mut self, n: usize) -> Self {
        self.max_arg_count = n;
        self
    }

    /// Set the maximum per-string size in bytes.
    #[must_use]
    pub const fn with_max_string_bytes(mut self, n: usize) -> Self {
        self.max_string_bytes = n;
        self
    }

    /// Set the maximum container nesting depth.
    #[must_use]
    pub const fn with_max_depth(mut self, n: usize) -> Self {
        self.max_depth = n;
        self
    }

    /// Set the oversize action.
    #[must_use]
    pub const fn with_oversize_action(mut self, action: OversizeAction) -> Self {
        self.oversize_action = action;
        self
    }

    /// Enable or disable control-character stripping.
    #[must_use]
    pub const fn with_strip_control_chars(mut self, strip: bool) -> Self {
        self.strip_control_chars = strip;
        self
    }

    /// Replace the set of disallowed value kinds.
    #[must_use]
    pub fn with_disallowed_kinds(mut self, kinds: impl IntoIterator<Item = ValueKind>) -> Self {
        self.disallowed_kinds = kinds.into_iter().collect();
        self
    }

    /// Add a single secret-key marker (matched case-insensitively as a
    /// substring of the key).
    #[must_use]
    pub fn with_secret_marker(mut self, marker: impl Into<String>) -> Self {
        self.secret_key_markers.insert(marker.into().to_lowercase());
        self
    }

    /// Replace the redaction placeholder.
    #[must_use]
    pub fn with_redaction_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.redaction_placeholder = placeholder.into();
        self
    }

    /// Whether `key` looks like a secret according to the configured markers.
    #[must_use]
    pub fn is_secret_key(&self, key: &str) -> bool {
        let lowered = key.to_lowercase();
        self.secret_key_markers
            .iter()
            .any(|marker| lowered.contains(marker.as_str()))
    }
}

/// Summary of the actions a [`Sanitizer`] performed on a payload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SanitizeReport {
    /// Number of strings that had at least one control character removed.
    pub strings_stripped: usize,
    /// Total number of control characters removed across all strings.
    pub control_chars_removed: usize,
    /// Number of strings/blobs truncated to the size limit.
    pub values_truncated: usize,
    /// Number of secret-looking keyword values redacted.
    pub redacted_keys: usize,
}

impl SanitizeReport {
    /// `true` if the sanitizer modified the payload in any way.
    #[must_use]
    pub const fn made_changes(&self) -> bool {
        self.strings_stripped > 0
            || self.values_truncated > 0
            || self.redacted_keys > 0
            || self.control_chars_removed > 0
    }

    /// Merge another report into this one.
    pub fn merge(&mut self, other: &SanitizeReport) {
        self.strings_stripped += other.strings_stripped;
        self.control_chars_removed += other.control_chars_removed;
        self.values_truncated += other.values_truncated;
        self.redacted_keys += other.redacted_keys;
    }
}

/// Configurable sanitizer over task arguments and keyword arguments.
#[derive(Debug, Clone)]
pub struct Sanitizer {
    config: SanitizerConfig,
}

impl Default for Sanitizer {
    fn default() -> Self {
        Self::new(SanitizerConfig::default())
    }
}

impl Sanitizer {
    /// Create a sanitizer with the given configuration.
    #[must_use]
    pub fn new(config: SanitizerConfig) -> Self {
        Self { config }
    }

    /// Borrow the active configuration.
    #[must_use]
    pub const fn config(&self) -> &SanitizerConfig {
        &self.config
    }

    /// Sanitize a list of positional arguments in place.
    ///
    /// # Errors
    ///
    /// Returns a [`SanitizeError`] if a hard limit is violated (too many
    /// arguments, disallowed kind, oversize value when configured to reject,
    /// or excessive nesting).
    pub fn sanitize_args(&self, args: &mut [TaskValue]) -> Result<SanitizeReport, SanitizeError> {
        self.check_arg_count(args.len(), 0)?;
        let mut report = SanitizeReport::default();
        for value in args.iter_mut() {
            self.sanitize_value(value, 1, &mut report)?;
        }
        Ok(report)
    }

    /// Sanitize a list of keyword arguments in place, redacting secret keys.
    ///
    /// # Errors
    ///
    /// Returns a [`SanitizeError`] as for [`Sanitizer::sanitize_args`], plus
    /// [`SanitizeError::KeyTooLong`] for over-long keys.
    pub fn sanitize_kwargs(
        &self,
        kwargs: &mut [(String, TaskValue)],
    ) -> Result<SanitizeReport, SanitizeError> {
        self.check_arg_count(kwargs.len(), 0)?;
        let mut report = SanitizeReport::default();
        for (key, value) in kwargs.iter_mut() {
            if key.len() > self.config.max_key_bytes {
                return Err(SanitizeError::KeyTooLong {
                    size: key.len(),
                    limit: self.config.max_key_bytes,
                });
            }
            if self.config.is_secret_key(key) {
                *value = TaskValue::String(self.config.redaction_placeholder.clone());
                report.redacted_keys += 1;
                // Redacted values are not scanned/stripped further.
                continue;
            }
            self.sanitize_value(value, 1, &mut report)?;
        }
        Ok(report)
    }

    /// Sanitize both positional and keyword arguments together, enforcing the
    /// `max_arg_count` limit against their *combined* length.
    ///
    /// # Errors
    ///
    /// Returns a [`SanitizeError`] if any hard limit is violated.
    pub fn sanitize_call(
        &self,
        args: &mut [TaskValue],
        kwargs: &mut [(String, TaskValue)],
    ) -> Result<SanitizeReport, SanitizeError> {
        let total = args.len().saturating_add(kwargs.len());
        if total > self.config.max_arg_count {
            return Err(SanitizeError::TooManyArgs {
                count: total,
                limit: self.config.max_arg_count,
            });
        }

        let mut report = SanitizeReport::default();
        for value in args.iter_mut() {
            self.sanitize_value(value, 1, &mut report)?;
        }
        for (key, value) in kwargs.iter_mut() {
            if key.len() > self.config.max_key_bytes {
                return Err(SanitizeError::KeyTooLong {
                    size: key.len(),
                    limit: self.config.max_key_bytes,
                });
            }
            if self.config.is_secret_key(key) {
                *value = TaskValue::String(self.config.redaction_placeholder.clone());
                report.redacted_keys += 1;
                continue;
            }
            self.sanitize_value(value, 1, &mut report)?;
        }
        Ok(report)
    }

    /// Recursively sanitize a single value at the given nesting `depth`.
    fn sanitize_value(
        &self,
        value: &mut TaskValue,
        depth: usize,
        report: &mut SanitizeReport,
    ) -> Result<(), SanitizeError> {
        if depth > self.config.max_depth {
            return Err(SanitizeError::TooDeep {
                depth,
                limit: self.config.max_depth,
            });
        }

        let kind = value.kind();
        if self.config.disallowed_kinds.contains(&kind) {
            return Err(SanitizeError::DisallowedKind(kind));
        }

        match value {
            TaskValue::String(s) => self.sanitize_string(s, report)?,
            TaskValue::Bytes(b) => {
                if b.len() > self.config.max_string_bytes {
                    match self.config.oversize_action {
                        OversizeAction::Reject => {
                            return Err(SanitizeError::ValueTooLarge {
                                size: b.len(),
                                limit: self.config.max_string_bytes,
                            });
                        }
                        OversizeAction::Truncate => {
                            b.truncate(self.config.max_string_bytes);
                            report.values_truncated += 1;
                        }
                    }
                }
            }
            TaskValue::Array(items) => {
                for item in items.iter_mut() {
                    self.sanitize_value(item, depth + 1, report)?;
                }
            }
            TaskValue::Object(entries) => {
                for (key, item) in entries.iter_mut() {
                    if key.len() > self.config.max_key_bytes {
                        return Err(SanitizeError::KeyTooLong {
                            size: key.len(),
                            limit: self.config.max_key_bytes,
                        });
                    }
                    if self.config.is_secret_key(key) {
                        *item = TaskValue::String(self.config.redaction_placeholder.clone());
                        report.redacted_keys += 1;
                        continue;
                    }
                    self.sanitize_value(item, depth + 1, report)?;
                }
            }
            // Scalars (other than strings/bytes handled above) need no work.
            TaskValue::Null
            | TaskValue::Bool(_)
            | TaskValue::Int(_)
            | TaskValue::UInt(_)
            | TaskValue::Float(_) => {}
        }

        Ok(())
    }

    /// Strip control characters from and size-limit a single string.
    fn sanitize_string(
        &self,
        s: &mut String,
        report: &mut SanitizeReport,
    ) -> Result<(), SanitizeError> {
        if self.config.strip_control_chars {
            let removed = strip_control_chars(s);
            if removed > 0 {
                report.strings_stripped += 1;
                report.control_chars_removed += removed;
            }
        }

        if s.len() > self.config.max_string_bytes {
            match self.config.oversize_action {
                OversizeAction::Reject => {
                    return Err(SanitizeError::ValueTooLarge {
                        size: s.len(),
                        limit: self.config.max_string_bytes,
                    });
                }
                OversizeAction::Truncate => {
                    truncate_str_bytes(s, self.config.max_string_bytes);
                    report.values_truncated += 1;
                }
            }
        }

        Ok(())
    }

    /// Enforce `max_arg_count` against `count` (plus an `extra` offset used
    /// when combining lists).
    fn check_arg_count(&self, count: usize, extra: usize) -> Result<(), SanitizeError> {
        let total = count.saturating_add(extra);
        if total > self.config.max_arg_count {
            return Err(SanitizeError::TooManyArgs {
                count: total,
                limit: self.config.max_arg_count,
            });
        }
        Ok(())
    }
}

/// Remove control characters from a string in place, returning how many were
/// removed.
///
/// "Control character" means any [`char::is_control`] codepoint **except**
/// the common whitespace characters tab (`\t`), newline (`\n`), and carriage
/// return (`\r`), which are preserved because they are legitimate in most
/// textual arguments. This neutralizes ANSI escape sequences, NUL bytes, and
/// other terminal/log-injection vectors.
#[must_use]
pub fn strip_control_chars(s: &mut String) -> usize {
    let mut removed = 0usize;
    let needs_work = s
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r'));
    if !needs_work {
        return 0;
    }
    let cleaned: String = s
        .chars()
        .filter(|c| {
            let keep = !c.is_control() || matches!(c, '\t' | '\n' | '\r');
            if !keep {
                removed += 1;
            }
            keep
        })
        .collect();
    *s = cleaned;
    removed
}

/// Truncate a string to at most `max_bytes` bytes without splitting a UTF-8
/// codepoint.
fn truncate_str_bytes(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_and_size() {
        assert_eq!(TaskValue::from(1_i64).kind(), ValueKind::Integer);
        assert_eq!(TaskValue::from(1.5_f64).kind(), ValueKind::Float);
        assert_eq!(TaskValue::from("hi").kind(), ValueKind::String);
        assert_eq!(TaskValue::from("hello").approx_size(), 5);
        assert!(TaskValue::Array(vec![TaskValue::from("ab")]).approx_size() >= 3);
    }

    #[test]
    fn depth_calculation() {
        let nested = TaskValue::Array(vec![TaskValue::Array(vec![TaskValue::from(1_i64)])]);
        assert_eq!(nested.depth(), 3);
        assert_eq!(TaskValue::from(1_i64).depth(), 1);
    }

    #[test]
    fn from_serde_json() {
        let v: serde_json::Value = serde_json::json!({
            "a": 1,
            "b": [true, "x", null],
            "c": 2.5
        });
        let tv = TaskValue::from(v);
        match tv {
            TaskValue::Object(entries) => {
                assert_eq!(entries.len(), 3);
            }
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn strip_control_chars_removes_escapes_keeps_whitespace() {
        let mut s = "hello\x1b[31mworld\x00\tok\nline".to_string();
        let removed = strip_control_chars(&mut s);
        // ESC, NUL removed; tab and newline kept.
        assert_eq!(removed, 2);
        assert_eq!(s, "hello[31mworld\tok\nline");
    }

    #[test]
    fn strip_control_chars_noop_on_clean() {
        let mut s = "perfectly clean\tstring\n".to_string();
        assert_eq!(strip_control_chars(&mut s), 0);
        assert_eq!(s, "perfectly clean\tstring\n");
    }

    #[test]
    fn sanitize_strips_strings_in_args() {
        let sanitizer = Sanitizer::default();
        let mut args = vec![TaskValue::from("a\x07b"), TaskValue::from(5_i64)];
        let report = sanitizer.sanitize_args(&mut args).unwrap();
        assert_eq!(args[0], TaskValue::from("ab"));
        assert_eq!(report.strings_stripped, 1);
        assert_eq!(report.control_chars_removed, 1);
    }

    #[test]
    fn sanitize_redacts_secret_kwargs() {
        let sanitizer = Sanitizer::default();
        let mut kwargs = vec![
            ("username".to_string(), TaskValue::from("alice")),
            ("password".to_string(), TaskValue::from("hunter2")),
            ("API_KEY".to_string(), TaskValue::from("sk-123")),
            ("auth_token".to_string(), TaskValue::from("xyz")),
        ];
        let report = sanitizer.sanitize_kwargs(&mut kwargs).unwrap();
        assert_eq!(kwargs[0].1, TaskValue::from("alice"));
        assert_eq!(kwargs[1].1, TaskValue::from("[REDACTED]"));
        assert_eq!(kwargs[2].1, TaskValue::from("[REDACTED]"));
        assert_eq!(kwargs[3].1, TaskValue::from("[REDACTED]"));
        assert_eq!(report.redacted_keys, 3);
    }

    #[test]
    fn sanitize_redacts_nested_secret_keys() {
        let sanitizer = Sanitizer::default();
        let mut args = vec![TaskValue::Object(vec![
            ("host".to_string(), TaskValue::from("db")),
            ("secret".to_string(), TaskValue::from("top")),
        ])];
        let report = sanitizer.sanitize_args(&mut args).unwrap();
        assert_eq!(report.redacted_keys, 1);
        if let TaskValue::Object(entries) = &args[0] {
            assert_eq!(entries[1].1, TaskValue::from("[REDACTED]"));
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn sanitize_rejects_too_many_args() {
        let config = SanitizerConfig::default().with_max_arg_count(2);
        let sanitizer = Sanitizer::new(config);
        let mut args = vec![
            TaskValue::from(1_i64),
            TaskValue::from(2_i64),
            TaskValue::from(3_i64),
        ];
        assert_eq!(
            sanitizer.sanitize_args(&mut args),
            Err(SanitizeError::TooManyArgs { count: 3, limit: 2 })
        );
    }

    #[test]
    fn sanitize_combined_arg_count() {
        let config = SanitizerConfig::default().with_max_arg_count(3);
        let sanitizer = Sanitizer::new(config);
        let mut args = vec![TaskValue::from(1_i64), TaskValue::from(2_i64)];
        let mut kwargs = vec![
            ("a".to_string(), TaskValue::from(1_i64)),
            ("b".to_string(), TaskValue::from(2_i64)),
        ];
        // 2 + 2 = 4 > 3
        assert!(matches!(
            sanitizer.sanitize_call(&mut args, &mut kwargs),
            Err(SanitizeError::TooManyArgs { count: 4, limit: 3 })
        ));
    }

    #[test]
    fn sanitize_rejects_oversize_string() {
        let config = SanitizerConfig::default().with_max_string_bytes(4);
        let sanitizer = Sanitizer::new(config);
        let mut args = vec![TaskValue::from("toolong")];
        assert_eq!(
            sanitizer.sanitize_args(&mut args),
            Err(SanitizeError::ValueTooLarge { size: 7, limit: 4 })
        );
    }

    #[test]
    fn sanitize_truncates_oversize_string() {
        let config = SanitizerConfig::default()
            .with_max_string_bytes(4)
            .with_oversize_action(OversizeAction::Truncate);
        let sanitizer = Sanitizer::new(config);
        let mut args = vec![TaskValue::from("toolong")];
        let report = sanitizer.sanitize_args(&mut args).unwrap();
        assert_eq!(args[0], TaskValue::from("tool"));
        assert_eq!(report.values_truncated, 1);
    }

    #[test]
    fn truncate_respects_utf8_boundary() {
        let config = SanitizerConfig::default()
            .with_max_string_bytes(3)
            .with_oversize_action(OversizeAction::Truncate);
        let sanitizer = Sanitizer::new(config);
        // "é" is 2 bytes; "aé" is 3 bytes; limit 3 keeps both, limit 2 keeps "a".
        let mut args = vec![TaskValue::from("aéb")];
        sanitizer.sanitize_args(&mut args).unwrap();
        // Must remain valid UTF-8 and not exceed the limit.
        if let TaskValue::String(s) = &args[0] {
            assert!(s.len() <= 3);
            assert_eq!(s, "aé");
        } else {
            panic!("expected string");
        }
    }

    #[test]
    fn sanitize_rejects_disallowed_bytes_by_default() {
        let sanitizer = Sanitizer::default();
        let mut args = vec![TaskValue::Bytes(vec![1, 2, 3])];
        assert_eq!(
            sanitizer.sanitize_args(&mut args),
            Err(SanitizeError::DisallowedKind(ValueKind::Bytes))
        );
    }

    #[test]
    fn sanitize_allows_bytes_when_permitted() {
        let config = SanitizerConfig::default().with_disallowed_kinds([]);
        let sanitizer = Sanitizer::new(config);
        let mut args = vec![TaskValue::Bytes(vec![1, 2, 3])];
        assert!(sanitizer.sanitize_args(&mut args).is_ok());
    }

    #[test]
    fn sanitize_rejects_too_deep() {
        let config = SanitizerConfig::default().with_max_depth(2);
        let sanitizer = Sanitizer::new(config);
        // depth 3: Array -> Array -> Int
        let mut args = vec![TaskValue::Array(vec![TaskValue::Array(vec![
            TaskValue::from(1_i64),
        ])])];
        assert!(matches!(
            sanitizer.sanitize_args(&mut args),
            Err(SanitizeError::TooDeep { .. })
        ));
    }

    #[test]
    fn sanitize_rejects_long_key() {
        let config = SanitizerConfig::default();
        let sanitizer = Sanitizer::new(config);
        let long_key = "k".repeat(1000);
        let mut kwargs = vec![(long_key, TaskValue::from(1_i64))];
        assert!(matches!(
            sanitizer.sanitize_kwargs(&mut kwargs),
            Err(SanitizeError::KeyTooLong { .. })
        ));
    }

    #[test]
    fn report_made_changes_and_merge() {
        let mut a = SanitizeReport::default();
        assert!(!a.made_changes());
        a.redacted_keys = 1;
        assert!(a.made_changes());
        let b = SanitizeReport {
            strings_stripped: 2,
            control_chars_removed: 5,
            values_truncated: 1,
            redacted_keys: 3,
        };
        a.merge(&b);
        assert_eq!(a.redacted_keys, 4);
        assert_eq!(a.control_chars_removed, 5);
        assert_eq!(a.values_truncated, 1);
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = SanitizerConfig::default().with_max_arg_count(7);
        let json = serde_json::to_string(&config).expect("serialize");
        let restored: SanitizerConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.max_arg_count, 7);
        assert!(restored.disallowed_kinds.contains(&ValueKind::Bytes));
    }

    #[test]
    fn permissive_truncates_and_allows_bytes() {
        let sanitizer = Sanitizer::new(SanitizerConfig::permissive());
        let mut args = vec![TaskValue::Bytes(vec![1, 2, 3])];
        assert!(sanitizer.sanitize_args(&mut args).is_ok());
    }
}
