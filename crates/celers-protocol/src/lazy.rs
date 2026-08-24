//! Lazy deserialization for improved performance
//!
//! This module provides lazy deserialization capabilities where message bodies
//! and other large data structures are only deserialized when actually accessed.
//! This is particularly useful for:
//! - Large message bodies that may not always be needed
//! - Message routing/filtering scenarios where only headers are needed
//! - High-throughput scenarios where deserialization overhead matters
//!
//! # Examples
//!
//! ```
//! use celers_protocol::lazy::LazyMessage;
//! use uuid::Uuid;
//!
//! let task_id = Uuid::new_v4();
//! let json = format!(r#"{{"headers":{{"task":"tasks.add","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"e30=","content-type":"application/json","content-encoding":"utf-8"}}"#, task_id);
//!
//! // Parse only the headers initially
//! let msg = LazyMessage::from_json(json.as_bytes()).unwrap();
//!
//! // Headers are immediately available
//! assert_eq!(msg.task_name(), "tasks.add");
//!
//! // Body is only deserialized when accessed
//! let body = msg.body().unwrap();
//! ```

use serde::Deserialize;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Error type for lazy deserialization
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LazyError {
    /// Deserialization failed
    DeserializationFailed(String),
    /// Invalid JSON
    InvalidJson(String),
    /// Body not available
    BodyNotAvailable,
}

impl std::fmt::Display for LazyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LazyError::DeserializationFailed(msg) => write!(f, "Deserialization failed: {}", msg),
            LazyError::InvalidJson(msg) => write!(f, "Invalid JSON: {}", msg),
            LazyError::BodyNotAvailable => write!(f, "Message body not available"),
        }
    }
}

impl std::error::Error for LazyError {}

/// Lazy-deserialized message body
///
/// The body is stored in raw form and only deserialized when accessed.
/// Uses interior mutability with `RwLock` to cache the deserialized value.
#[derive(Debug, Clone)]
pub struct LazyBody {
    /// Raw body bytes
    raw: Vec<u8>,
    /// Cached deserialized body
    cached: Arc<RwLock<Option<Vec<u8>>>>,
}

impl LazyBody {
    /// Create a new lazy body from raw bytes
    pub fn new(raw: Vec<u8>) -> Self {
        Self {
            raw,
            cached: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the raw, still base64-*encoded* bytes without decoding them.
    ///
    /// This returns the wire-format text bytes exactly as received (e.g.
    /// `b"dGVzdA=="`), **not** the decoded body content. For the decoded
    /// body, use [`LazyBody::deserialize`]; for its length without
    /// allocating, use [`LazyBody::decoded_size`].
    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Get the size, in bytes, of the raw base64-*encoded* representation.
    ///
    /// This is the length of the still-encoded wire text (about 4/3 the
    /// decoded size), **not** the size of the decoded body. For a size that
    /// matches [`crate::Message::body_size`] on the same wire input, use
    /// [`LazyBody::decoded_size`].
    #[inline]
    pub fn encoded_size(&self) -> usize {
        self.raw.len()
    }

    /// Compute the exact size, in bytes, of the base64-*decoded* body.
    ///
    /// This mirrors the length [`LazyBody::deserialize`] would produce, but
    /// is computed arithmetically from the encoded length so it never
    /// allocates or decodes the body, preserving the "lazy" contract of
    /// this type.
    pub fn decoded_size(&self) -> usize {
        decoded_base64_len(&self.raw)
    }

    /// Deserialize the body (base64 decode)
    pub fn deserialize(&self) -> Result<Vec<u8>, LazyError> {
        // Check cache first
        {
            let cached = self.cached.read().unwrap_or_else(|e| e.into_inner());
            if let Some(body) = cached.as_ref() {
                return Ok(body.clone());
            }
        }

        // Deserialize and cache
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &self.raw)
            .map_err(|e| LazyError::DeserializationFailed(e.to_string()))?;

        let mut cached = self.cached.write().unwrap_or_else(|e| e.into_inner());
        *cached = Some(decoded.clone());

        Ok(decoded)
    }

    /// Check if body is already deserialized
    pub fn is_cached(&self) -> bool {
        self.cached
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }
}

/// Compute the exact decoded length of a standard (padded) base64 byte
/// string without allocating or decoding it.
///
/// Per RFC 4648 §4, every 4 encoded bytes decode to 3 bytes, minus one byte
/// for each trailing `=` padding character (at most two). This is exact for
/// well-formed, padded base64 input; for malformed input it degrades
/// gracefully (never panics, never overflows) rather than producing a
/// guaranteed-correct value, matching [`LazyBody::deserialize`]'s own
/// eventual decode error in that case.
fn decoded_base64_len(raw: &[u8]) -> usize {
    let len = raw.len();
    if len == 0 {
        return 0;
    }
    let full_len = (len / 4) * 3;
    let padding = match (raw.get(len.wrapping_sub(1)), raw.get(len.wrapping_sub(2))) {
        (Some(b'='), Some(b'=')) => 2,
        (Some(b'='), _) => 1,
        _ => 0,
    };
    full_len.saturating_sub(padding)
}

/// Lazy-deserialized message
///
/// Headers and properties are eagerly deserialized (they're small),
/// but the body is lazily deserialized only when accessed.
#[derive(Debug, Clone)]
pub struct LazyMessage {
    /// Message headers (eagerly deserialized)
    pub headers: crate::MessageHeaders,

    /// Message properties (eagerly deserialized)
    pub properties: crate::MessageProperties,

    /// Lazy body
    body: LazyBody,

    /// Content type
    pub content_type: String,

    /// Content encoding
    pub content_encoding: String,
}

impl LazyMessage {
    /// Create a lazy message from JSON bytes
    ///
    /// This performs minimal deserialization - only headers and properties
    /// are parsed. The body remains in raw form until accessed.
    pub fn from_json(data: &[u8]) -> Result<Self, LazyError> {
        #[derive(Deserialize)]
        struct LazyMessageHelper {
            headers: crate::MessageHeaders,
            properties: crate::MessageProperties,
            #[serde(with = "serde_bytes_helper")]
            body: Vec<u8>,
            #[serde(rename = "content-type")]
            content_type: String,
            #[serde(rename = "content-encoding")]
            content_encoding: String,
        }

        let helper: LazyMessageHelper =
            serde_json::from_slice(data).map_err(|e| LazyError::InvalidJson(e.to_string()))?;

        Ok(Self {
            headers: helper.headers,
            properties: helper.properties,
            body: LazyBody::new(helper.body),
            content_type: helper.content_type,
            content_encoding: helper.content_encoding,
        })
    }

    /// Get the task ID
    pub fn task_id(&self) -> Uuid {
        self.headers.id
    }

    /// Get the task name
    pub fn task_name(&self) -> &str {
        &self.headers.task
    }

    /// Get the exact size, in bytes, of the *decoded* body without
    /// deserializing it.
    ///
    /// This matches [`crate::Message::body_size`] for the same wire input
    /// (computed arithmetically from the base64-encoded length, so it never
    /// allocates or decodes the body). For the size of the still-encoded
    /// wire representation instead, see
    /// [`LazyMessage::encoded_body_size`].
    pub fn body_size(&self) -> usize {
        self.body.decoded_size()
    }

    /// Get the size, in bytes, of the raw base64-*encoded* body without
    /// decoding it. This is the wire-format length, not the decoded body
    /// length - see [`LazyMessage::body_size`] for that.
    pub fn encoded_body_size(&self) -> usize {
        self.body.encoded_size()
    }

    /// Check if body is already deserialized
    pub fn is_body_cached(&self) -> bool {
        self.body.is_cached()
    }

    /// Get the raw, still base64-*encoded* body bytes without decoding
    /// them (e.g. `b"dGVzdA=="`, not `b"test"`). For the decoded bytes, use
    /// [`LazyMessage::body`].
    pub fn encoded_body(&self) -> &[u8] {
        self.body.raw_bytes()
    }

    /// Get deserialized body (triggers deserialization if not cached)
    pub fn body(&self) -> Result<Vec<u8>, LazyError> {
        self.body.deserialize()
    }

    /// Check if message has ETA
    pub fn has_eta(&self) -> bool {
        self.headers.eta.is_some()
    }

    /// Check if message has expiration
    pub fn has_expires(&self) -> bool {
        self.headers.expires.is_some()
    }

    /// Check if message has parent
    pub fn has_parent(&self) -> bool {
        self.headers.parent_id.is_some()
    }

    /// Check if message has root
    pub fn has_root(&self) -> bool {
        self.headers.root_id.is_some()
    }

    /// Check if message has group
    pub fn has_group(&self) -> bool {
        self.headers.group.is_some()
    }

    /// Convert to fully deserialized message
    pub fn into_message(self) -> Result<crate::Message, LazyError> {
        Ok(crate::Message {
            headers: self.headers,
            properties: self.properties,
            body: self.body.deserialize()?,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
        })
    }
}

// Helper for deserializing body as raw bytes
mod serde_bytes_helper {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(s.into_bytes())
    }
}

/// Lazy task arguments
///
/// Defers parsing of task arguments until they're actually needed.
#[derive(Debug, Clone)]
pub struct LazyTaskArgs {
    /// Raw JSON bytes
    raw: Vec<u8>,
    /// Cached parsed arguments
    cached: Arc<RwLock<Option<crate::TaskArgs>>>,
}

impl LazyTaskArgs {
    /// Create lazy task arguments from raw JSON
    pub fn new(raw: Vec<u8>) -> Self {
        Self {
            raw,
            cached: Arc::new(RwLock::new(None)),
        }
    }

    /// Get raw bytes
    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Parse task arguments (cached)
    pub fn parse(&self) -> Result<crate::TaskArgs, LazyError> {
        // Check cache
        {
            let cached = self.cached.read().unwrap_or_else(|e| e.into_inner());
            if let Some(args) = cached.as_ref() {
                return Ok(args.clone());
            }
        }

        // Parse and cache
        let args: crate::TaskArgs = serde_json::from_slice(&self.raw)
            .map_err(|e| LazyError::DeserializationFailed(e.to_string()))?;

        let mut cached = self.cached.write().unwrap_or_else(|e| e.into_inner());
        *cached = Some(args.clone());

        Ok(args)
    }

    /// Check if arguments are cached
    pub fn is_cached(&self) -> bool {
        self.cached
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy_body() {
        let raw = b"dGVzdCBkYXRh"; // base64 "test data" (12 encoded bytes -> 9 decoded)
        let body = LazyBody::new(raw.to_vec());

        assert_eq!(body.encoded_size(), raw.len());
        assert_eq!(body.decoded_size(), b"test data".len());
        assert!(!body.is_cached());

        let decoded = body.deserialize().unwrap();
        assert_eq!(decoded, b"test data");
        assert_eq!(body.decoded_size(), decoded.len());
        assert!(body.is_cached());
    }

    #[test]
    fn test_lazy_body_decoded_size_matches_real_decode_for_various_padding() {
        // No padding: "AAAA" (4 chars) -> 3 bytes.
        assert_eq!(LazyBody::new(b"AAAA".to_vec()).decoded_size(), 3);
        // One '=' padding char: "AAA=" (4 chars) -> 2 bytes.
        assert_eq!(LazyBody::new(b"AAA=".to_vec()).decoded_size(), 2);
        // Two '=' padding chars: "AA==" (4 chars) -> 1 byte.
        assert_eq!(LazyBody::new(b"AA==".to_vec()).decoded_size(), 1);
        // Empty body.
        assert_eq!(LazyBody::new(Vec::new()).decoded_size(), 0);

        // Cross-check decoded_size() against an actual decode for a longer,
        // realistic payload.
        let raw = b"SGVsbG8sIFdvcmxkIQ==".to_vec(); // "Hello, World!"
        let body = LazyBody::new(raw);
        let decoded = body.deserialize().unwrap();
        assert_eq!(body.decoded_size(), decoded.len());
    }

    #[test]
    fn test_lazy_body_survives_poisoned_cache_lock() {
        // Regression: a panic while holding a `LazyBody`'s internal cache
        // `RwLock` used to poison it, after which every subsequent
        // `deserialize()`/`is_cached()` call would itself panic via
        // `.expect("lock should not be poisoned")`. The lock must be
        // recovered instead (same fix as `MessagePool`, see `pool.rs`).
        let body = LazyBody::new(b"dGVzdA==".to_vec()); // base64 "test"
        let cached = body.cached.clone();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = cached.write().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(cached.is_poisoned());

        // Neither of these may panic even though the lock is poisoned.
        assert!(!body.is_cached());
        let decoded = body.deserialize().unwrap();
        assert_eq!(decoded, b"test");
        assert!(body.is_cached());
    }

    #[test]
    fn test_lazy_message_from_json() {
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.add","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"e30=","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );

        let msg = LazyMessage::from_json(json.as_bytes()).unwrap();

        assert_eq!(msg.task_name(), "tasks.add");
        assert_eq!(msg.task_id(), task_id);
        assert!(!msg.is_body_cached());

        let _body = msg.body().unwrap();
        assert!(msg.is_body_cached());
    }

    #[test]
    fn test_lazy_message_predicates() {
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.test","id":"{}","lang":"rust","eta":"2024-12-31T23:59:59Z"}},"properties":{{"delivery_mode":2}},"body":"e30=","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );

        let msg = LazyMessage::from_json(json.as_bytes()).unwrap();

        assert!(msg.has_eta());
        assert!(!msg.has_expires());
        assert!(!msg.has_parent());
    }

    #[test]
    fn test_lazy_message_body_size() {
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.test","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"dGVzdA==","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );

        let msg = LazyMessage::from_json(json.as_bytes()).unwrap();

        // Regression: `body_size()` must report the *decoded* body size
        // ("dGVzdA==" decodes to "test", 4 bytes), not the base64-encoded
        // wire text length (8 bytes).
        assert_eq!(msg.body_size(), 4);
        assert_eq!(msg.encoded_body_size(), 8);
    }

    #[test]
    fn test_lazy_message_body_size_matches_owned_message_body_size() {
        // Regression: for identical wire input, `LazyMessage::body_size()`
        // must agree with `Message::body_size()` (previously they disagreed
        // because `LazyMessage` reported the base64-encoded length).
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.test","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"SGVsbG8sIFdvcmxkIQ==","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );

        let lazy = LazyMessage::from_json(json.as_bytes()).unwrap();
        let owned: crate::Message = serde_json::from_str(&json).unwrap();

        assert_eq!(lazy.body_size(), owned.body_size());
        assert_eq!(lazy.body().unwrap(), owned.body);
    }

    #[test]
    fn test_lazy_message_encoded_body_is_still_encoded_text() {
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.test","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"dGVzdA==","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );

        let msg = LazyMessage::from_json(json.as_bytes()).unwrap();

        // encoded_body() is the raw wire text (undecoded)...
        assert_eq!(msg.encoded_body(), b"dGVzdA==");
        // ...while body() is the real, decoded content.
        assert_eq!(msg.body().unwrap(), b"test");
    }

    #[test]
    fn test_lazy_task_args() {
        let json = r#"{"args":[1,2,3],"kwargs":{"key":"value"}}"#;
        let lazy_args = LazyTaskArgs::new(json.as_bytes().to_vec());

        assert!(!lazy_args.is_cached());

        let args = lazy_args.parse().unwrap();
        assert_eq!(args.args.len(), 3);
        assert_eq!(args.kwargs.get("key").unwrap(), "value");

        assert!(lazy_args.is_cached());
    }

    #[test]
    fn test_lazy_task_args_survives_poisoned_cache_lock() {
        // Same regression as `test_lazy_body_survives_poisoned_cache_lock`,
        // for `LazyTaskArgs`'s independent cache lock.
        let json = r#"{"args":[1,2],"kwargs":{}}"#;
        let lazy_args = LazyTaskArgs::new(json.as_bytes().to_vec());
        let cached = lazy_args.cached.clone();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = cached.write().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(cached.is_poisoned());

        assert!(!lazy_args.is_cached());
        let parsed = lazy_args.parse().unwrap();
        assert_eq!(parsed.args.len(), 2);
        assert!(lazy_args.is_cached());
    }

    #[test]
    fn test_lazy_error_display() {
        let err = LazyError::DeserializationFailed("test error".to_string());
        assert_eq!(err.to_string(), "Deserialization failed: test error");

        let err = LazyError::InvalidJson("bad json".to_string());
        assert_eq!(err.to_string(), "Invalid JSON: bad json");

        let err = LazyError::BodyNotAvailable;
        assert_eq!(err.to_string(), "Message body not available");
    }

    #[test]
    fn test_lazy_message_into_message() {
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.test","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"dGVzdA==","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );

        let lazy_msg = LazyMessage::from_json(json.as_bytes()).unwrap();
        let msg = lazy_msg.into_message().unwrap();

        assert_eq!(msg.headers.task, "tasks.test");
        assert_eq!(msg.headers.id, task_id);
    }
}
