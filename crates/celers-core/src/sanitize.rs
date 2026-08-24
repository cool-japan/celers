//! Task argument sanitization.
//!
//! # What is automatic, and what is opt-in
//!
//! **Nothing here runs automatically.** A [`Sanitizer`] cleans the arguments
//! you hand it and nothing else; CeleRS does not sanitize a task's payload on
//! enqueue, on dequeue, or before execution, and a task always executes the
//! exact bytes its producer sent.
//!
//! | Where | Sanitized? | How to turn it on |
//! |---|---|---|
//! | The payload a task executes | **never** — by design | not available: rewriting it would change what the caller asked for, and would invalidate the message's signature |
//! | The `inspect active` payload preview, and the worker's `debug!` rendering of arguments | only when configured | set `WorkerConfig::payload_hygiene` (see [`crate::task_security::PayloadHygiene`]) |
//! | Dead-letter entries | **no** | not available: a DLQ entry is replayable, so its payload is an executing payload |
//! | Result-backend records | nothing to sanitize | CeleRS stores no task arguments in any result backend |
//! | Your own logging / persistence | only when you call it | call [`Sanitizer::sanitize_call`] on your copy |
//!
//! The `security_wiring` example in the `celers` crate shows the whole wiring.
//!
//! # Why sanitize a copy at all
//!
//! Task arguments frequently originate from untrusted callers (HTTP handlers,
//! message brokers, user input). Before such a payload is *logged or persisted*
//! it is prudent to:
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
//! Note that the size and kind limits are *errors*, not redactions:
//! [`Sanitizer::sanitize_call`] returns [`SanitizeError`] for a payload that
//! violates one, and [`SanitizerConfig::default`] disallows
//! [`ValueKind::Bytes`] outright. A caller using this for logging must treat
//! that as "render a placeholder", never as "reject the task" — which is what
//! [`crate::task_security::PayloadHygiene`] does.
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
    /// The aggregate approximate weight of the payload exceeded
    /// `max_total_bytes`.
    PayloadTooLarge {
        /// Observed aggregate size in bytes.
        size: usize,
        /// Configured limit in bytes.
        limit: usize,
    },
    /// An array or object held more elements than `max_container_len`.
    ContainerTooLong {
        /// Observed element count.
        len: usize,
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
            SanitizeError::PayloadTooLarge { size, limit } => {
                write!(
                    f,
                    "payload too large: {size} bytes exceeds limit of {limit}"
                )
            }
            SanitizeError::ContainerTooLong { len, limit } => {
                write!(
                    f,
                    "container too long: {len} elements exceeds limit of {limit}"
                )
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

    /// Maximum aggregate approximate weight, in bytes, of a whole payload
    /// (see [`TaskValue::approx_size`]).
    ///
    /// Per-string, per-key and per-depth limits alone cannot stop an array of
    /// ten million small integers, which is well within `max_depth` and has no
    /// oversized string in it.
    #[serde(default = "default_max_total_bytes")]
    pub max_total_bytes: usize,

    /// Maximum number of elements in any single array or object.
    #[serde(default = "default_max_container_len")]
    pub max_container_len: usize,

    /// Lower-cased markers that make a key secret. A marker matches when its
    /// own word sequence appears as consecutive whole words of the key, where
    /// words are split on `_`, `-`, `.`, `/`, `:`, whitespace and camel-case
    /// boundaries.
    ///
    /// Whole-word matching is what keeps ordinary arguments intact: `auth` and
    /// `auth_token` are secret, `author` and `authorized_by` are not.
    pub secret_key_markers: BTreeSet<String>,

    /// Lower-cased nouns that make a key secret when a key word *ends* with
    /// them, even with no separator: `authtoken`, `mytoken`, `apipassword`.
    ///
    /// Whole-word matching alone would let those glued spellings through, which
    /// would be a loosening of a security-relevant default. Matching only at the
    /// *end* of a word is what keeps `tokenizer` and `author` intact.
    #[serde(default = "default_secret_key_suffixes")]
    pub secret_key_suffixes: BTreeSet<String>,

    /// Lower-cased nouns that make a key secret when a key word *starts* with
    /// them: `secretkey`, `passwordhash`, `credentialstore`.
    ///
    /// Deliberately narrower than [`Self::secret_key_suffixes`]: `token`,
    /// `auth`, `session` and `private` are excluded because ordinary words
    /// extend them (`tokenizer`, `author`, `session_count`, `privateer`).
    #[serde(default = "default_secret_key_prefixes")]
    pub secret_key_prefixes: BTreeSet<String>,

    /// Lower-cased markers matched as raw substrings of the key.
    ///
    /// Empty by default. This is the escape hatch for deployments that want the
    /// blunter historical behaviour (`token` matching `tokenizer`) for a
    /// specific set of markers.
    #[serde(default)]
    pub secret_key_substrings: BTreeSet<String>,

    /// Lower-cased keys that are never redacted, whatever the markers say.
    ///
    /// Defaults to a handful of metric-style keys that share a word with a
    /// marker but carry no secret (`token_count`, `session_count`, ...).
    #[serde(default)]
    pub allowed_keys: BTreeSet<String>,

    /// Placeholder value substituted for redacted secrets.
    pub redaction_placeholder: String,
}

/// Serde default for [`SanitizerConfig::max_total_bytes`].
const fn default_max_total_bytes() -> usize {
    4 * 1024 * 1024
}

/// Serde default for [`SanitizerConfig::max_container_len`].
const fn default_max_container_len() -> usize {
    10_000
}

/// Serde/`Default` value for [`SanitizerConfig::secret_key_suffixes`].
fn default_secret_key_suffixes() -> BTreeSet<String> {
    [
        "password",
        "passwd",
        "secret",
        "token",
        "credential",
        "credentials",
        "cookie",
        "apikey",
        "accesskey",
        "privatekey",
        "sessionid",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect()
}

/// Serde/`Default` value for [`SanitizerConfig::secret_key_prefixes`].
fn default_secret_key_prefixes() -> BTreeSet<String> {
    [
        "password",
        "passwd",
        "secret",
        "credential",
        "credentials",
        "apikey",
        "accesskey",
        "privatekey",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect()
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

        // Keys that share a word with a marker but are plainly metrics, not
        // secrets. Deployments can extend or replace this set.
        let allowed = [
            "token_count",
            "token_counts",
            "tokens_used",
            "session_count",
            "session_counts",
            "auth_count",
            "auth_attempts",
        ];
        let allowed_keys = allowed.iter().map(|s| (*s).to_string()).collect();

        Self {
            max_arg_count: 64,
            max_string_bytes: 64 * 1024,
            max_key_bytes: 256,
            max_depth: 16,
            max_total_bytes: 4 * 1024 * 1024,
            max_container_len: 10_000,
            oversize_action: OversizeAction::Reject,
            strip_control_chars: true,
            disallowed_kinds: disallowed,
            secret_key_markers,
            secret_key_suffixes: default_secret_key_suffixes(),
            secret_key_prefixes: default_secret_key_prefixes(),
            secret_key_substrings: BTreeSet::new(),
            allowed_keys,
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
            max_total_bytes: 64 * 1024 * 1024,
            max_container_len: 1_000_000,
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

    /// Set the maximum aggregate payload weight in bytes.
    #[must_use]
    pub const fn with_max_total_bytes(mut self, n: usize) -> Self {
        self.max_total_bytes = n;
        self
    }

    /// Set the maximum number of elements in any single container.
    #[must_use]
    pub const fn with_max_container_len(mut self, n: usize) -> Self {
        self.max_container_len = n;
        self
    }

    /// Add a single secret-key marker (matched case-insensitively against whole
    /// words of the key).
    #[must_use]
    pub fn with_secret_marker(mut self, marker: impl Into<String>) -> Self {
        self.secret_key_markers.insert(marker.into().to_lowercase());
        self
    }

    /// Add a noun that makes a key secret when a key word ends with it.
    #[must_use]
    pub fn with_secret_suffix(mut self, marker: impl Into<String>) -> Self {
        self.secret_key_suffixes
            .insert(marker.into().to_lowercase());
        self
    }

    /// Add a noun that makes a key secret when a key word starts with it.
    #[must_use]
    pub fn with_secret_prefix(mut self, marker: impl Into<String>) -> Self {
        self.secret_key_prefixes
            .insert(marker.into().to_lowercase());
        self
    }

    /// Add a marker matched as a raw case-insensitive substring of the key.
    #[must_use]
    pub fn with_secret_substring(mut self, marker: impl Into<String>) -> Self {
        self.secret_key_substrings
            .insert(marker.into().to_lowercase());
        self
    }

    /// Add a key that is never redacted, whatever the markers say.
    #[must_use]
    pub fn with_allowed_key(mut self, key: impl Into<String>) -> Self {
        self.allowed_keys.insert(key.into().to_lowercase());
        self
    }

    /// Replace the redaction placeholder.
    #[must_use]
    pub fn with_redaction_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.redaction_placeholder = placeholder.into();
        self
    }

    /// Whether `key` looks like a secret according to the configured markers.
    ///
    /// Markers are matched against **whole words** of the key rather than raw
    /// substrings. Substring matching redacted ordinary arguments — `author`
    /// contains `auth`, `tokenizer` contains `token` — and because redaction is
    /// destructive and in place, a task on the execute path received
    /// `[REDACTED]` instead of its real argument. `secret_key_substrings` keeps
    /// the old behaviour available where it is genuinely wanted, and
    /// `allowed_keys` exempts specific keys outright.
    #[must_use]
    pub fn is_secret_key(&self, key: &str) -> bool {
        let lowered = key.to_lowercase();

        if self.allowed_keys.contains(&lowered) {
            return false;
        }

        if self
            .secret_key_substrings
            .iter()
            .any(|marker| lowered.contains(marker.as_str()))
        {
            return true;
        }

        let words = key_words(key);
        if self
            .secret_key_markers
            .iter()
            .any(|marker| contains_word_sequence(&words, &key_words(marker)))
        {
            return true;
        }

        // Glued spellings with no separator: `authtoken`, `secretkey`. Without
        // these, whole-word matching would be a net loosening of the default
        // policy compared with the old substring behaviour.
        words.iter().any(|word| {
            self.secret_key_suffixes
                .iter()
                .any(|marker| word.ends_with(marker.as_str()))
                || self
                    .secret_key_prefixes
                    .iter()
                    .any(|marker| word.starts_with(marker.as_str()))
        })
    }
}

/// Split a key into lower-cased words on separator characters and camel-case
/// boundaries.
///
/// `api_key`, `apiKey`, `API-Key` and `api.key` all yield `["api", "key"]`.
fn key_words(key: &str) -> Vec<String> {
    let mut words: Vec<String> = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = key.chars().collect();

    for (index, &ch) in chars.iter().enumerate() {
        if !ch.is_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }

        if ch.is_uppercase() && !current.is_empty() {
            let previous = chars[index - 1];
            let next_is_lower = chars.get(index + 1).is_some_and(|c| c.is_lowercase());
            // `fooBar` -> foo|Bar, `HTTPServer` -> HTTP|Server.
            if previous.is_lowercase() || previous.is_numeric() || next_is_lower {
                words.push(std::mem::take(&mut current));
            }
        }

        current.extend(ch.to_lowercase());
    }

    if !current.is_empty() {
        words.push(current);
    }

    words
}

/// Whether `needle` appears as a contiguous run of words inside `haystack`.
fn contains_word_sequence(haystack: &[String], needle: &[String]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
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
        let mut total = 0usize;
        for value in args.iter_mut() {
            self.accumulate_size(value, &mut total)?;
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
        let mut total = 0usize;
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
            self.accumulate_size(value, &mut total)?;
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
        // The aggregate weight is tracked across the whole call, not per value.
        let mut total = 0usize;
        for value in args.iter_mut() {
            self.accumulate_size(value, &mut total)?;
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
            self.accumulate_size(value, &mut total)?;
            self.sanitize_value(value, 1, &mut report)?;
        }
        Ok(report)
    }

    /// Add `value`'s approximate weight to the running total, failing once the
    /// configured aggregate limit is exceeded.
    fn accumulate_size(&self, value: &TaskValue, total: &mut usize) -> Result<(), SanitizeError> {
        *total = total.saturating_add(value.approx_size());
        if *total > self.config.max_total_bytes {
            return Err(SanitizeError::PayloadTooLarge {
                size: *total,
                limit: self.config.max_total_bytes,
            });
        }
        Ok(())
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
                self.check_container_len(items.len())?;
                for item in items.iter_mut() {
                    self.sanitize_value(item, depth + 1, report)?;
                }
            }
            TaskValue::Object(entries) => {
                self.check_container_len(entries.len())?;
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

    /// Enforce `max_container_len` against a single array/object.
    fn check_container_len(&self, len: usize) -> Result<(), SanitizeError> {
        if len > self.config.max_container_len {
            return Err(SanitizeError::ContainerTooLong {
                len,
                limit: self.config.max_container_len,
            });
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

/// Whether `c` is a Unicode format/separator codepoint that must be stripped
/// alongside the C0/C1 control characters.
///
/// [`char::is_control`] only covers General_Category `Cc`
/// (`U+0000`–`U+001F`, `U+007F`–`U+009F`). The characters below are category
/// `Cf` (or `Zl`/`Zp`) and therefore passed straight through, yet they are the
/// classic log-spoofing vectors:
///
/// * `U+202A`–`U+202E` and `U+2066`–`U+2069` — bidirectional overrides and
///   isolates ("Trojan Source"): they visually reorder a log line;
/// * `U+200B`–`U+200F` — zero-width space/joiners and LTR/RTL marks: invisible
///   characters that hide or reorder text;
/// * `U+2028`/`U+2029` — line and paragraph separators: they inject a line
///   break into line-oriented logs;
/// * `U+FEFF` — zero-width no-break space (BOM) appearing mid-string.
#[must_use]
pub const fn is_unicode_format_char(c: char) -> bool {
    matches!(
        c,
        '\u{200B}'..='\u{200F}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2066}'..='\u{2069}'
            | '\u{FEFF}'
    )
}

/// Whether `c` must be stripped by [`strip_control_chars`].
#[inline]
fn is_strippable(c: char) -> bool {
    if matches!(c, '\t' | '\n' | '\r') {
        return false;
    }
    c.is_control() || is_unicode_format_char(c)
}

/// Remove control and Unicode format characters from a string in place,
/// returning how many were removed.
///
/// Stripped codepoints are any [`char::is_control`] character plus the
/// bidi/format characters listed by [`is_unicode_format_char`], **except**
/// the common whitespace characters tab (`\t`), newline (`\n`), and carriage
/// return (`\r`), which are preserved because they are legitimate in most
/// textual arguments. This neutralizes ANSI escape sequences, NUL bytes,
/// bidirectional-override log spoofing, and other terminal/log-injection
/// vectors.
#[must_use]
pub fn strip_control_chars(s: &mut String) -> usize {
    let mut removed = 0usize;
    if !s.chars().any(is_strippable) {
        return 0;
    }
    let cleaned: String = s
        .chars()
        .filter(|&c| {
            let keep = !is_strippable(c);
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

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// Regression: `is_secret_key` matched markers as raw substrings, so
    /// ordinary arguments were destructively replaced with `[REDACTED]`.
    #[test]
    fn secret_key_detection_matches_whole_words_only() {
        let config = SanitizerConfig::default();

        // Genuine secrets still match.
        for key in [
            "password",
            "api_key",
            "apiKey",
            "API-KEY",
            "auth",
            "auth_token",
            "authToken",
            "x.session.cookie",
            "access_key_id",
            "user_secret",
        ] {
            assert!(
                config.is_secret_key(key),
                "{key} should be treated as secret"
            );
        }

        // Glued spellings with no separator must still be caught: whole-word
        // matching alone would be a net loosening of the default policy.
        for key in [
            "authtoken",
            "mytoken",
            "usertoken",
            "apitoken",
            "secretkey",
            "passwordhash",
            "credentialstore",
            "sessionid",
            "accesskeyid",
        ] {
            assert!(
                config.is_secret_key(key),
                "{key} should be treated as secret"
            );
        }

        // Ordinary arguments must survive untouched.
        for key in [
            "author",
            "authority",
            "authorized_by",
            "authorization_note",
            "tokenizer",
            "token_count",
            "session_count",
            "privateer",
            "username",
            "sort_key",
            "keyword",
            "monkey_patch",
        ] {
            assert!(
                !config.is_secret_key(key),
                "{key} must not be treated as secret"
            );
        }
    }

    /// Guards the security direction of the whole-word rewrite: every key the
    /// old substring matcher redacted for a *good* reason must still be redacted.
    #[test]
    fn secret_key_detection_did_not_loosen_for_real_secrets() {
        let config = SanitizerConfig::default();
        for key in [
            "password",
            "user_password",
            "passwordHash",
            "db_passwd",
            "secret",
            "client_secret",
            "secretKey",
            "api_key",
            "API_KEY",
            "apikey",
            "access_key",
            "accessKeyId",
            "auth",
            "auth_header",
            "authToken",
            "bearer_token",
            "refresh_token",
            "session",
            "session_id",
            "sessionId",
            "cookie",
            "set_cookie",
            "credential",
            "aws_credentials",
            "private",
            "private_key",
            "privateKey",
        ] {
            assert!(
                config.is_secret_key(key),
                "{key} must still be treated as secret"
            );
        }
    }

    #[test]
    fn secret_affix_sets_are_configurable() {
        // A word ending in a suffix marker is secret...
        let config = SanitizerConfig::default();
        assert!(config.is_secret_key("xyztoken"));
        // ...but the bare marker rule does not fire on a longer prefix.
        assert!(!config.is_secret_key("tokenxyz"));

        let with_prefix = SanitizerConfig::default().with_secret_prefix("token");
        assert!(with_prefix.is_secret_key("tokenxyz"));

        let with_suffix = SanitizerConfig::default().with_secret_suffix("pin");
        assert!(with_suffix.is_secret_key("userpin"));
        assert!(!SanitizerConfig::default().is_secret_key("userpin"));
    }

    #[test]
    fn ordinary_kwargs_are_not_redacted() {
        let sanitizer = Sanitizer::default();
        let mut kwargs = vec![
            ("author".to_string(), TaskValue::from("alice")),
            ("token_count".to_string(), TaskValue::from(1234_i64)),
            ("session_count".to_string(), TaskValue::from(7_i64)),
            ("tokenizer".to_string(), TaskValue::from("bpe")),
            ("api_key".to_string(), TaskValue::from("sk-live-1")),
        ];
        let report = sanitizer
            .sanitize_kwargs(&mut kwargs)
            .expect("sanitize should succeed");

        assert_eq!(report.redacted_keys, 1);
        assert_eq!(kwargs[0].1, TaskValue::from("alice"));
        assert_eq!(kwargs[1].1, TaskValue::from(1234_i64));
        assert_eq!(kwargs[2].1, TaskValue::from(7_i64));
        assert_eq!(kwargs[3].1, TaskValue::from("bpe"));
        assert_eq!(kwargs[4].1, TaskValue::from("[REDACTED]"));
    }

    #[test]
    fn substring_markers_and_allowlist_are_configurable() {
        // Opt back in to the blunt behaviour for a specific marker.
        let blunt = SanitizerConfig::default().with_secret_substring("token");
        assert!(blunt.is_secret_key("tokenizer"));

        // And exempt a key outright.
        let exempt = SanitizerConfig::default().with_allowed_key("api_key");
        assert!(!exempt.is_secret_key("api_key"));
        assert!(exempt.is_secret_key("password"));
    }

    #[test]
    fn key_words_splits_on_separators_and_camel_case() {
        assert_eq!(key_words("api_key"), vec!["api", "key"]);
        assert_eq!(key_words("apiKey"), vec!["api", "key"]);
        assert_eq!(key_words("API-Key"), vec!["api", "key"]);
        assert_eq!(key_words("HTTPServer"), vec!["http", "server"]);
        assert_eq!(key_words("author"), vec!["author"]);
        assert_eq!(
            key_words("x.session.cookie"),
            vec!["x", "session", "cookie"]
        );
        assert!(key_words("").is_empty());
    }

    /// Regression: `char::is_control` covers only category `Cc`, so the
    /// bidirectional overrides, zero-width characters and line/paragraph
    /// separators used for log spoofing passed straight through.
    #[test]
    fn unicode_format_characters_are_stripped() {
        let mut s =
            "user\u{202E}drowssap\u{202C} logged\u{2028}in\u{200B}now\u{2069}\u{FEFF}".to_string();
        let removed = strip_control_chars(&mut s);
        assert_eq!(removed, 6, "stripped {s:?}");
        assert_eq!(s, "userdrowssap loggedinnow");

        // Legitimate whitespace is preserved.
        let mut keep = "a\tb\nc\rd".to_string();
        assert_eq!(strip_control_chars(&mut keep), 0);
        assert_eq!(keep, "a\tb\nc\rd");

        // And the classic C0 vectors still go.
        let mut ansi = "before\u{1b}[31mred\u{0}after".to_string();
        assert_eq!(strip_control_chars(&mut ansi), 2);
        assert_eq!(ansi, "before[31mredafter");
    }

    #[test]
    fn bidi_override_in_a_task_argument_is_neutralized() {
        let sanitizer = Sanitizer::default();
        let mut args = vec![TaskValue::from("safe\u{202E}evil")];
        let report = sanitizer
            .sanitize_args(&mut args)
            .expect("sanitize should succeed");
        assert_eq!(report.control_chars_removed, 1);
        assert_eq!(args[0], TaskValue::from("safeevil"));
    }

    /// Regression: only per-string and per-depth limits were enforced, so an
    /// array of millions of small integers passed sanitization untouched.
    #[test]
    fn aggregate_payload_size_is_bounded() {
        let config = SanitizerConfig::default().with_max_total_bytes(1024);
        let sanitizer = Sanitizer::new(config);

        let big = TaskValue::Array((0..1000).map(TaskValue::Int).collect());
        let mut args = vec![big];
        let err = sanitizer
            .sanitize_args(&mut args)
            .expect_err("an oversized payload must be rejected");
        assert!(
            matches!(err, SanitizeError::PayloadTooLarge { .. }),
            "{err}"
        );

        // The limit is cumulative across a whole call, not per value.
        let mut args = vec![TaskValue::from("x".repeat(600))];
        let mut kwargs = vec![("k".to_string(), TaskValue::from("y".repeat(600)))];
        let err = sanitizer
            .sanitize_call(&mut args, &mut kwargs)
            .expect_err("cumulative weight must be enforced");
        assert!(
            matches!(err, SanitizeError::PayloadTooLarge { .. }),
            "{err}"
        );
    }

    #[test]
    fn container_length_is_bounded() {
        let config = SanitizerConfig::default()
            .with_max_container_len(4)
            .with_max_total_bytes(usize::MAX);
        let sanitizer = Sanitizer::new(config);

        let mut ok = vec![TaskValue::Array((0..4).map(TaskValue::Int).collect())];
        assert!(sanitizer.sanitize_args(&mut ok).is_ok());

        let mut too_long = vec![TaskValue::Array((0..5).map(TaskValue::Int).collect())];
        let err = sanitizer
            .sanitize_args(&mut too_long)
            .expect_err("an over-long array must be rejected");
        assert!(
            matches!(err, SanitizeError::ContainerTooLong { len: 5, limit: 4 }),
            "{err}"
        );

        // Nested containers are checked too.
        let mut nested = vec![TaskValue::Object(vec![(
            "inner".to_string(),
            TaskValue::Array((0..9).map(TaskValue::Int).collect()),
        )])];
        assert!(matches!(
            sanitizer.sanitize_args(&mut nested),
            Err(SanitizeError::ContainerTooLong { len: 9, limit: 4 })
        ));
    }
}
