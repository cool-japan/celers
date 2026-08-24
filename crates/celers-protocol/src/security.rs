//! Security utilities for protocol handling
//!
//! This module provides security-related utilities including content-type
//! whitelisting, message validation, and safety checks.
//!
//! # Content-Type Whitelist
//!
//! By default, only safe serialization formats are allowed. Pickle is
//! explicitly blocked due to security concerns (arbitrary code execution).
//!
//! # Example
//!
//! ```
//! use celers_protocol::security::{ContentTypeWhitelist, SecurityPolicy};
//!
//! let policy = SecurityPolicy::strict();
//! assert!(policy.is_content_type_allowed("application/json"));
//! assert!(!policy.is_content_type_allowed("application/x-python-pickle"));
//! ```

use std::collections::HashSet;

/// Content types that carry a Python `pickle` payload.
///
/// Deserializing any of these executes arbitrary code inside the consumer, so
/// every built-in whitelist blocks all of them unconditionally.
///
/// `application/x-python-serialize` is the spelling **kombu registers for the
/// `pickle` serializer** (`kombu.serialization.register_pickle`), i.e. the one
/// that actually appears on the wire when a Python producer is configured with
/// `task_serializer = 'pickle'`. The remaining spellings are emitted by
/// third-party producers and older kombu releases and are blocked for the same
/// reason.
pub const UNSAFE_CONTENT_TYPES: &[&str] = &[
    // kombu's registered pickle content type -- the one seen on the wire.
    "application/x-python-serialize",
    "application/x-python-pickle",
    "application/python-pickle",
    "application/x-pickle",
];

/// Build the set of always-blocked content types.
fn unsafe_content_type_set() -> HashSet<String> {
    UNSAFE_CONTENT_TYPES
        .iter()
        .map(|ct| (*ct).to_string())
        .collect()
}

/// Content-type whitelist for allowed serialization formats
#[derive(Debug, Clone)]
pub struct ContentTypeWhitelist {
    /// Allowed content types
    allowed: HashSet<String>,
    /// Blocked content types (takes precedence)
    blocked: HashSet<String>,
}

impl Default for ContentTypeWhitelist {
    fn default() -> Self {
        Self::safe()
    }
}

impl ContentTypeWhitelist {
    /// Create a new empty whitelist
    pub fn new() -> Self {
        Self {
            allowed: HashSet::new(),
            blocked: HashSet::new(),
        }
    }

    /// Create a whitelist with safe defaults (JSON, MessagePack)
    ///
    /// `application/octet-stream` is allowed because it is the content type of
    /// this crate's own `binary` serializer (see [`crate::ContentType::Binary`]);
    /// the payload is an opaque byte string that is never *executed*, unlike a
    /// pickle payload. Every content type in [`UNSAFE_CONTENT_TYPES`] is blocked,
    /// and blocking takes precedence over allowing.
    pub fn safe() -> Self {
        let mut allowed = HashSet::new();
        allowed.insert("application/json".to_string());
        allowed.insert("application/x-msgpack".to_string());
        allowed.insert("application/octet-stream".to_string());

        // Block pickle - security risk (arbitrary code execution).
        Self {
            allowed,
            blocked: unsafe_content_type_set(),
        }
    }

    /// Create a permissive whitelist (allows all except blocked)
    ///
    /// The allow list is empty, so anything that is not explicitly blocked is
    /// accepted -- which makes the completeness of [`UNSAFE_CONTENT_TYPES`]
    /// load-bearing here: a pickle content type missing from that list would
    /// sail straight through.
    pub fn permissive() -> Self {
        Self {
            allowed: HashSet::new(), // Empty means check blocked list only
            blocked: unsafe_content_type_set(),
        }
    }

    /// Create a strict whitelist (JSON only)
    pub fn strict() -> Self {
        let mut allowed = HashSet::new();
        allowed.insert("application/json".to_string());

        Self {
            allowed,
            blocked: unsafe_content_type_set(),
        }
    }

    /// Allow a content type
    #[must_use]
    pub fn allow(mut self, content_type: impl Into<String>) -> Self {
        let ct = content_type.into();
        self.allowed.insert(ct.clone());
        self.blocked.remove(&ct);
        self
    }

    /// Block a content type
    #[must_use]
    pub fn block(mut self, content_type: impl Into<String>) -> Self {
        let ct = content_type.into();
        self.blocked.insert(ct.clone());
        self.allowed.remove(&ct);
        self
    }

    /// Check if a content type is allowed
    pub fn is_allowed(&self, content_type: &str) -> bool {
        // Normalize content type (lowercase, strip parameters)
        let normalized = normalize_content_type(content_type);

        // Blocked takes precedence
        if self.blocked.contains(&normalized) {
            return false;
        }

        // If allowed list is empty, allow anything not blocked
        if self.allowed.is_empty() {
            return true;
        }

        // Check allowed list
        self.allowed.contains(&normalized)
    }

    /// Get all allowed content types
    #[inline]
    pub fn allowed_types(&self) -> Vec<&str> {
        self.allowed.iter().map(|s| s.as_str()).collect()
    }

    /// Get all blocked content types
    #[inline]
    pub fn blocked_types(&self) -> Vec<&str> {
        self.blocked.iter().map(|s| s.as_str()).collect()
    }
}

/// Normalize a content type string
fn normalize_content_type(content_type: &str) -> String {
    // Extract main type (before any parameters like charset)
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_lowercase()
}

/// Security validation error
#[derive(Debug, Clone)]
pub enum SecurityError {
    /// Content type is not allowed
    ContentTypeBlocked(String),
    /// Message size exceeds limit
    MessageTooLarge { size: usize, limit: usize },
    /// Task name contains invalid characters
    InvalidTaskName(String),
    /// Potential injection detected
    PotentialInjection(String),
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityError::ContentTypeBlocked(ct) => {
                write!(f, "Content type '{}' is not allowed", ct)
            }
            SecurityError::MessageTooLarge { size, limit } => {
                write!(
                    f,
                    "Message size {} bytes exceeds limit of {} bytes",
                    size, limit
                )
            }
            SecurityError::InvalidTaskName(name) => {
                write!(f, "Invalid task name: {}", name)
            }
            SecurityError::PotentialInjection(desc) => {
                write!(f, "Potential injection detected: {}", desc)
            }
        }
    }
}

impl std::error::Error for SecurityError {}

/// Security policy for message handling
#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    /// Content type whitelist
    pub content_types: ContentTypeWhitelist,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Maximum task name length
    pub max_task_name_length: usize,
    /// Allowed task name pattern (regex-like)
    pub task_name_pattern: Option<String>,
    /// Enable strict validation
    pub strict_validation: bool,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self::standard()
    }
}

impl SecurityPolicy {
    /// Create a standard security policy
    pub fn standard() -> Self {
        Self {
            content_types: ContentTypeWhitelist::safe(),
            max_message_size: 10 * 1024 * 1024, // 10 MB
            max_task_name_length: 256,
            task_name_pattern: None,
            strict_validation: false,
        }
    }

    /// Create a strict security policy
    pub fn strict() -> Self {
        Self {
            content_types: ContentTypeWhitelist::strict(),
            max_message_size: 1024 * 1024, // 1 MB
            max_task_name_length: 128,
            task_name_pattern: Some(r"^[a-zA-Z_][a-zA-Z0-9_.]*$".to_string()),
            strict_validation: true,
        }
    }

    /// Create a permissive security policy
    pub fn permissive() -> Self {
        Self {
            content_types: ContentTypeWhitelist::permissive(),
            max_message_size: 100 * 1024 * 1024, // 100 MB
            max_task_name_length: 512,
            task_name_pattern: None,
            strict_validation: false,
        }
    }

    /// Check if a content type is allowed
    pub fn is_content_type_allowed(&self, content_type: &str) -> bool {
        self.content_types.is_allowed(content_type)
    }

    /// Validate content type
    pub fn validate_content_type(&self, content_type: &str) -> Result<(), SecurityError> {
        if self.content_types.is_allowed(content_type) {
            Ok(())
        } else {
            Err(SecurityError::ContentTypeBlocked(content_type.to_string()))
        }
    }

    /// Validate message size
    pub fn validate_message_size(&self, size: usize) -> Result<(), SecurityError> {
        if size <= self.max_message_size {
            Ok(())
        } else {
            Err(SecurityError::MessageTooLarge {
                size,
                limit: self.max_message_size,
            })
        }
    }

    /// Validate task name
    pub fn validate_task_name(&self, name: &str) -> Result<(), SecurityError> {
        // Check length
        if name.len() > self.max_task_name_length {
            return Err(SecurityError::InvalidTaskName(format!(
                "Task name too long: {} > {}",
                name.len(),
                self.max_task_name_length
            )));
        }

        // Check for empty name
        if name.is_empty() {
            return Err(SecurityError::InvalidTaskName(
                "Task name cannot be empty".to_string(),
            ));
        }

        // Check for null bytes
        if name.contains('\0') {
            return Err(SecurityError::PotentialInjection(
                "Task name contains null bytes".to_string(),
            ));
        }

        // In strict mode, validate pattern
        if self.strict_validation {
            // Simple pattern check: must start with letter/underscore,
            // contain only alphanumeric, underscore, or dot
            let is_valid = name.chars().enumerate().all(|(i, c)| {
                if i == 0 {
                    c.is_ascii_alphabetic() || c == '_'
                } else {
                    c.is_ascii_alphanumeric() || c == '_' || c == '.'
                }
            });

            if !is_valid {
                return Err(SecurityError::InvalidTaskName(format!(
                    "Task name '{}' contains invalid characters",
                    name
                )));
            }
        }

        Ok(())
    }

    /// Validate a complete message
    pub fn validate_message(
        &self,
        content_type: &str,
        body_size: usize,
        task_name: &str,
    ) -> Result<(), SecurityError> {
        self.validate_content_type(content_type)?;
        self.validate_message_size(body_size)?;
        self.validate_task_name(task_name)?;
        Ok(())
    }

    /// Set maximum message size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Set maximum task name length
    pub fn with_max_task_name_length(mut self, length: usize) -> Self {
        self.max_task_name_length = length;
        self
    }

    /// Enable strict validation
    pub fn with_strict_validation(mut self, strict: bool) -> Self {
        self.strict_validation = strict;
        self
    }

    /// Set content type whitelist
    pub fn with_content_types(mut self, whitelist: ContentTypeWhitelist) -> Self {
        self.content_types = whitelist;
        self
    }
}

/// Check if a content type is known to be unsafe
pub fn is_unsafe_content_type(content_type: &str) -> bool {
    let normalized = normalize_content_type(content_type);
    UNSAFE_CONTENT_TYPES.contains(&normalized.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_type_whitelist_safe() {
        let whitelist = ContentTypeWhitelist::safe();
        assert!(whitelist.is_allowed("application/json"));
        assert!(whitelist.is_allowed("application/x-msgpack"));
        assert!(!whitelist.is_allowed("application/x-python-pickle"));
    }

    #[test]
    fn test_content_type_whitelist_strict() {
        let whitelist = ContentTypeWhitelist::strict();
        assert!(whitelist.is_allowed("application/json"));
        assert!(!whitelist.is_allowed("application/x-msgpack"));
    }

    #[test]
    fn test_content_type_whitelist_permissive() {
        let whitelist = ContentTypeWhitelist::permissive();
        assert!(whitelist.is_allowed("application/json"));
        assert!(whitelist.is_allowed("application/x-msgpack"));
        assert!(whitelist.is_allowed("text/plain"));
        assert!(!whitelist.is_allowed("application/x-python-pickle"));
    }

    #[test]
    fn test_content_type_normalization() {
        let whitelist = ContentTypeWhitelist::safe();
        assert!(whitelist.is_allowed("application/json; charset=utf-8"));
        assert!(whitelist.is_allowed("APPLICATION/JSON"));
    }

    #[test]
    fn test_content_type_whitelist_allow_block() {
        let whitelist = ContentTypeWhitelist::new()
            .allow("text/plain")
            .block("text/html");

        assert!(whitelist.is_allowed("text/plain"));
        assert!(!whitelist.is_allowed("text/html"));
        assert!(!whitelist.is_allowed("application/json"));
    }

    #[test]
    fn test_security_policy_standard() {
        let policy = SecurityPolicy::standard();
        assert!(policy.is_content_type_allowed("application/json"));
        assert!(!policy.is_content_type_allowed("application/x-python-pickle"));
    }

    #[test]
    fn test_security_policy_strict() {
        let policy = SecurityPolicy::strict();
        assert!(policy.is_content_type_allowed("application/json"));
        assert!(!policy.is_content_type_allowed("application/x-msgpack"));
    }

    #[test]
    fn test_validate_message_size() {
        let policy = SecurityPolicy::standard().with_max_message_size(100);
        assert!(policy.validate_message_size(50).is_ok());
        assert!(policy.validate_message_size(100).is_ok());
        assert!(policy.validate_message_size(101).is_err());
    }

    #[test]
    fn test_validate_task_name() {
        let policy = SecurityPolicy::standard();
        assert!(policy.validate_task_name("tasks.add").is_ok());
        assert!(policy.validate_task_name("my_task").is_ok());
        assert!(policy.validate_task_name("").is_err());
    }

    #[test]
    fn test_validate_task_name_strict() {
        let policy = SecurityPolicy::strict();
        assert!(policy.validate_task_name("tasks.add").is_ok());
        assert!(policy.validate_task_name("_private_task").is_ok());
        assert!(policy.validate_task_name("123_invalid").is_err());
        assert!(policy.validate_task_name("task-with-dash").is_err());
    }

    #[test]
    fn test_validate_task_name_null_bytes() {
        let policy = SecurityPolicy::standard();
        assert!(policy.validate_task_name("task\0name").is_err());
    }

    #[test]
    fn test_validate_task_name_length() {
        let policy = SecurityPolicy::standard().with_max_task_name_length(10);
        assert!(policy.validate_task_name("short").is_ok());
        assert!(policy.validate_task_name("this_is_too_long").is_err());
    }

    #[test]
    fn test_validate_message() {
        let policy = SecurityPolicy::standard();
        assert!(policy
            .validate_message("application/json", 1000, "tasks.add")
            .is_ok());
    }

    #[test]
    fn test_is_unsafe_content_type() {
        assert!(is_unsafe_content_type("application/x-python-pickle"));
        assert!(is_unsafe_content_type("application/python-pickle"));
        assert!(!is_unsafe_content_type("application/json"));
    }

    /// Regression: `application/x-python-serialize` is the content type kombu
    /// registers for the `pickle` serializer, so it is the spelling that
    /// actually arrives on the wire from a Python producer configured with
    /// `task_serializer = 'pickle'`. It used to be missing from the whitelists
    /// (while `is_unsafe_content_type` already listed it), which let a pickle
    /// payload through `permissive()` -- whose empty allow list accepts
    /// anything that is not explicitly blocked.
    #[test]
    fn test_python_serialize_is_blocked_by_every_whitelist() {
        const KOMBU_PICKLE: &str = "application/x-python-serialize";

        assert!(!ContentTypeWhitelist::safe().is_allowed(KOMBU_PICKLE));
        assert!(!ContentTypeWhitelist::permissive().is_allowed(KOMBU_PICKLE));
        assert!(!ContentTypeWhitelist::strict().is_allowed(KOMBU_PICKLE));
        assert!(!ContentTypeWhitelist::default().is_allowed(KOMBU_PICKLE));

        assert!(!SecurityPolicy::standard().is_content_type_allowed(KOMBU_PICKLE));
        assert!(!SecurityPolicy::permissive().is_content_type_allowed(KOMBU_PICKLE));
        assert!(!SecurityPolicy::strict().is_content_type_allowed(KOMBU_PICKLE));

        // Content-type parameters and casing must not defeat the block.
        assert!(!ContentTypeWhitelist::permissive()
            .is_allowed("APPLICATION/X-PYTHON-SERIALIZE; charset=binary"));
    }

    /// The whitelists and `is_unsafe_content_type` must agree: both are driven
    /// by the single `UNSAFE_CONTENT_TYPES` list.
    #[test]
    fn test_unsafe_content_types_are_consistent_everywhere() {
        for content_type in UNSAFE_CONTENT_TYPES {
            assert!(
                is_unsafe_content_type(content_type),
                "{content_type} must be reported unsafe"
            );
            for whitelist in [
                ContentTypeWhitelist::safe(),
                ContentTypeWhitelist::permissive(),
                ContentTypeWhitelist::strict(),
            ] {
                assert!(
                    !whitelist.is_allowed(content_type),
                    "{content_type} must never be allowed"
                );
            }
        }

        // JSON stays allowed by the non-strict whitelists.
        assert!(ContentTypeWhitelist::safe().is_allowed("application/json"));
        assert!(ContentTypeWhitelist::permissive().is_allowed("application/json"));
    }

    #[test]
    fn test_security_error_display() {
        let err = SecurityError::ContentTypeBlocked("pickle".to_string());
        assert!(err.to_string().contains("pickle"));

        let err = SecurityError::MessageTooLarge {
            size: 100,
            limit: 50,
        };
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("50"));

        let err = SecurityError::InvalidTaskName("bad".to_string());
        assert!(err.to_string().contains("bad"));

        let err = SecurityError::PotentialInjection("null".to_string());
        assert!(err.to_string().contains("null"));
    }

    #[test]
    fn test_allowed_blocked_types() {
        let whitelist = ContentTypeWhitelist::safe();
        let allowed = whitelist.allowed_types();
        let blocked = whitelist.blocked_types();

        assert!(allowed.contains(&"application/json"));
        assert!(blocked.contains(&"application/x-python-pickle"));
    }
}
