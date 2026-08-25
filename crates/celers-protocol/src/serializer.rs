//! Pluggable serialization framework
//!
//! This module provides a unified [`Serializer`] trait for different serialization
//! formats (JSON, MessagePack, etc.) with automatic content type detection.
//!
//! # Example
//!
//! ```
//! use celers_protocol::serializer::{Serializer, JsonSerializer};
//!
//! let serializer = JsonSerializer;
//! let data = vec![1, 2, 3];
//! let bytes = serializer.serialize(&data).unwrap();
//! let decoded: Vec<i32> = serializer.deserialize(&bytes).unwrap();
//! assert_eq!(data, decoded);
//! ```

use crate::{ContentEncoding, ContentType};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

/// Error type for serialization operations
#[derive(Debug)]
pub enum SerializerError {
    /// Serialization failed
    Serialize(String),
    /// Deserialization failed
    Deserialize(String),
    /// Unsupported content type
    UnsupportedContentType(String),
    /// Compression error
    Compression(String),
}

impl fmt::Display for SerializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerializerError::Serialize(msg) => write!(f, "Serialization error: {}", msg),
            SerializerError::Deserialize(msg) => write!(f, "Deserialization error: {}", msg),
            SerializerError::UnsupportedContentType(ct) => {
                write!(f, "Unsupported content type: {}", ct)
            }
            SerializerError::Compression(msg) => write!(f, "Compression error: {}", msg),
        }
    }
}

impl std::error::Error for SerializerError {}

/// Result type for serialization operations
pub type SerializerResult<T> = Result<T, SerializerError>;

/// Trait for pluggable serialization formats
///
/// Implementors of this trait can serialize and deserialize data to/from bytes,
/// providing their content type and encoding information.
pub trait Serializer: Send + Sync {
    /// Returns the content type for this serializer
    fn content_type(&self) -> ContentType;

    /// Returns the content encoding for this serializer
    fn content_encoding(&self) -> ContentEncoding;

    /// Serialize a value to bytes
    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>>;

    /// Deserialize bytes to a value
    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T>;

    /// Returns the name of this serializer
    fn name(&self) -> &'static str;
}

/// JSON serializer implementation
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    #[inline]
    fn content_type(&self) -> ContentType {
        ContentType::Json
    }

    #[inline]
    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Utf8
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        serde_json::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        serde_json::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    #[inline]
    fn name(&self) -> &'static str {
        "json"
    }
}

/// MessagePack serializer implementation
#[cfg(feature = "msgpack")]
#[derive(Debug, Clone, Copy, Default)]
pub struct MessagePackSerializer;

#[cfg(feature = "msgpack")]
impl Serializer for MessagePackSerializer {
    #[inline]
    fn content_type(&self) -> ContentType {
        ContentType::MessagePack
    }

    #[inline]
    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Binary
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        rmp_serde::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        rmp_serde::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    #[inline]
    fn name(&self) -> &'static str {
        "msgpack"
    }
}

/// YAML serializer implementation
#[cfg(feature = "yaml")]
#[derive(Debug, Clone, Copy, Default)]
pub struct YamlSerializer;

#[cfg(feature = "yaml")]
impl Serializer for YamlSerializer {
    #[inline]
    fn content_type(&self) -> ContentType {
        ContentType::Custom("application/x-yaml".to_string())
    }

    #[inline]
    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Utf8
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        serde_yaml_ng::to_string(value)
            .map(|s| s.into_bytes())
            .map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        serde_yaml_ng::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    #[inline]
    fn name(&self) -> &'static str {
        "yaml"
    }
}

/// BSON serializer implementation
#[cfg(feature = "bson-format")]
#[derive(Debug, Clone, Copy, Default)]
pub struct BsonSerializer;

#[cfg(feature = "bson-format")]
impl Serializer for BsonSerializer {
    #[inline]
    fn content_type(&self) -> ContentType {
        ContentType::Custom("application/bson".to_string())
    }

    #[inline]
    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Binary
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        bson::serialize_to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        bson::deserialize_from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    #[inline]
    fn name(&self) -> &'static str {
        "bson"
    }
}

/// Protobuf serializer implementation
///
/// Note: This is a generic Protobuf serializer using prost.
/// For proper Protobuf support, you should use prost code generation
/// to create message-specific serializers.
#[cfg(feature = "protobuf")]
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtobufSerializer;

#[cfg(feature = "protobuf")]
impl ProtobufSerializer {
    /// Serialize a prost::Message to bytes
    pub fn serialize_message<T: prost::Message>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        let mut buf = Vec::new();
        value
            .encode(&mut buf)
            .map_err(|e| SerializerError::Serialize(e.to_string()))?;
        Ok(buf)
    }

    /// Deserialize bytes to a prost::Message
    pub fn deserialize_message<T: prost::Message + Default>(
        &self,
        bytes: &[u8],
    ) -> SerializerResult<T> {
        T::decode(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    /// Get content type for Protobuf
    #[inline]
    pub fn content_type(&self) -> ContentType {
        ContentType::Custom("application/protobuf".to_string())
    }

    /// Get content encoding for Protobuf
    #[inline]
    pub fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Binary
    }

    /// Get the name
    #[inline]
    pub fn name(&self) -> &'static str {
        "protobuf"
    }
}

/// Serializer type enum for dynamic dispatch without dyn trait issues
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum SerializerType {
    /// JSON serializer
    #[default]
    Json,
    /// MessagePack serializer
    #[cfg(feature = "msgpack")]
    MessagePack,
    /// YAML serializer
    #[cfg(feature = "yaml")]
    Yaml,
    /// BSON serializer
    #[cfg(feature = "bson-format")]
    Bson,
    /// Protobuf serializer
    #[cfg(feature = "protobuf")]
    Protobuf,
}

impl SerializerType {
    /// Get a serializer type by content type string
    pub fn from_content_type(content_type: &str) -> SerializerResult<Self> {
        match content_type {
            "application/json" => Ok(SerializerType::Json),
            #[cfg(feature = "msgpack")]
            "application/x-msgpack" => Ok(SerializerType::MessagePack),
            #[cfg(feature = "yaml")]
            "application/x-yaml" | "application/yaml" | "text/yaml" => Ok(SerializerType::Yaml),
            #[cfg(feature = "bson-format")]
            "application/bson" => Ok(SerializerType::Bson),
            #[cfg(feature = "protobuf")]
            "application/protobuf" | "application/x-protobuf" => Ok(SerializerType::Protobuf),
            _ => Err(SerializerError::UnsupportedContentType(
                content_type.to_string(),
            )),
        }
    }

    /// Serialize a value to bytes
    pub fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        match self {
            SerializerType::Json => JsonSerializer.serialize(value),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.serialize(value),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.serialize(value),
            #[cfg(feature = "bson-format")]
            SerializerType::Bson => BsonSerializer.serialize(value),
            #[cfg(feature = "protobuf")]
            SerializerType::Protobuf => Err(SerializerError::Serialize(
                "Protobuf requires prost::Message; use ProtobufSerializer::serialize_message instead".to_string(),
            )),
        }
    }

    /// Deserialize bytes to a value
    pub fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        match self {
            SerializerType::Json => JsonSerializer.deserialize(bytes),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.deserialize(bytes),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.deserialize(bytes),
            #[cfg(feature = "bson-format")]
            SerializerType::Bson => BsonSerializer.deserialize(bytes),
            #[cfg(feature = "protobuf")]
            SerializerType::Protobuf => Err(SerializerError::Deserialize(
                "Protobuf requires prost::Message; use ProtobufSerializer::deserialize_message instead".to_string(),
            )),
        }
    }

    /// Get the content type
    #[inline]
    pub fn content_type(&self) -> ContentType {
        match self {
            SerializerType::Json => JsonSerializer.content_type(),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.content_type(),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.content_type(),
            #[cfg(feature = "bson-format")]
            SerializerType::Bson => BsonSerializer.content_type(),
            #[cfg(feature = "protobuf")]
            SerializerType::Protobuf => ProtobufSerializer.content_type(),
        }
    }

    /// Get the content encoding
    #[inline]
    pub fn content_encoding(&self) -> ContentEncoding {
        match self {
            SerializerType::Json => JsonSerializer.content_encoding(),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.content_encoding(),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.content_encoding(),
            #[cfg(feature = "bson-format")]
            SerializerType::Bson => BsonSerializer.content_encoding(),
            #[cfg(feature = "protobuf")]
            SerializerType::Protobuf => ProtobufSerializer.content_encoding(),
        }
    }

    /// Get the name
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            SerializerType::Json => JsonSerializer.name(),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.name(),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.name(),
            #[cfg(feature = "bson-format")]
            SerializerType::Bson => BsonSerializer.name(),
            #[cfg(feature = "protobuf")]
            SerializerType::Protobuf => ProtobufSerializer.name(),
        }
    }
}

impl fmt::Display for SerializerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<&str> for SerializerType {
    type Error = SerializerError;

    fn try_from(content_type: &str) -> Result<Self, Self::Error> {
        Self::from_content_type(content_type)
    }
}

/// Get a serializer type by content type string
///
/// Returns the appropriate serializer type for the given content type string,
/// or an error if the content type is not supported.
///
/// # Supported Content Types
///
/// - `application/json` - JSON serializer
/// - `application/x-msgpack` - MessagePack serializer (requires `msgpack` feature)
/// - `application/x-yaml` - YAML serializer (requires `yaml` feature)
/// - `application/bson` - BSON serializer (requires `bson-format` feature)
pub fn get_serializer(content_type: &str) -> SerializerResult<SerializerType> {
    SerializerType::from_content_type(content_type)
}

/// Registry of available serializers
pub struct SerializerRegistry {
    default: SerializerType,
}

impl Default for SerializerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializerRegistry {
    /// Create a new serializer registry with JSON as default
    pub fn new() -> Self {
        Self {
            default: SerializerType::Json,
        }
    }

    /// Get the default serializer type
    #[inline]
    pub fn default_serializer(&self) -> SerializerType {
        self.default
    }

    /// Get a serializer by content type
    pub fn get(&self, content_type: &str) -> SerializerResult<SerializerType> {
        get_serializer(content_type)
    }

    /// List all available serializers
    pub fn available() -> Vec<&'static str> {
        vec![
            "application/json",
            #[cfg(feature = "msgpack")]
            "application/x-msgpack",
            #[cfg(feature = "yaml")]
            "application/x-yaml",
            #[cfg(feature = "bson-format")]
            "application/bson",
            #[cfg(feature = "protobuf")]
            "application/protobuf",
        ]
    }

    /// Detect serialization format from raw bytes using magic numbers,
    /// structural heuristics, and - where a first byte alone would be
    /// ambiguous - a real trial parse.
    ///
    /// - JSON: after optional leading whitespace, the data must both start
    ///   with `{`/`[` *and* parse as a complete, valid JSON value. A
    ///   first-byte-only check would misclassify a single-byte MessagePack
    ///   positive fixint payload as JSON, since `0x7b`/`0x5b` (the fixints
    ///   for 123/91) are also the ASCII codes for `{`/`[`.
    /// - YAML: starts with `---` document marker
    /// - BSON: 4-byte LE size header matching data length, trailing `0x00`
    /// - MessagePack: structural type markers that can never begin JSON or
    ///   plain ASCII text - fixmap/fixarray/fixstr (`0x80..=0xbf`) and
    ///   nil/bool/bin/ext/float/int/str8-32/array16-32/map16-32
    ///   (`0xc0..=0xdf`) - checked first as a cheap fast path, then (for the
    ///   ambiguous fixint ranges `0x00..=0x7f` / `0xe0..=0xff`, which
    ///   overlap with plain ASCII text and are deliberately excluded from
    ///   the fast path) a full, consumption-verified MessagePack parse:
    ///   classified as MessagePack only if the *entire* buffer decodes as
    ///   one self-describing value, not merely because the first byte falls
    ///   in a fixint range.
    /// - Protobuf: valid wire-type and field-number in the first tag byte
    ///   (weak heuristic; tried last since it has no way to verify a match)
    ///
    /// Returns `None` if the format cannot be determined. Like the rest of
    /// this heuristic, callers should treat this as a hint for e.g.
    /// diagnostics or default-format selection, not as a substitute for an
    /// explicit, trusted content-type when deserializing untrusted input.
    pub fn detect_format(data: &[u8]) -> Option<SerializerType> {
        if data.is_empty() {
            return None;
        }

        // JSON: gate on a cheap first-byte check, then confirm with a real,
        // full-buffer parse (`serde_json` already rejects incomplete input
        // and trailing garbage, and tolerates surrounding whitespace on its
        // own - see the module tests - so no manual trimming is needed for
        // the parse itself).
        let trimmed = data.iter().position(|&b| !b.is_ascii_whitespace());
        if let Some(pos) = trimmed {
            if (data[pos] == b'{' || data[pos] == b'[')
                && serde_json::from_slice::<serde::de::IgnoredAny>(data).is_ok()
            {
                return Some(SerializerType::Json);
            }
        }

        // YAML: starts with "---" document marker
        #[cfg(feature = "yaml")]
        if data.starts_with(b"---") {
            return Some(SerializerType::Yaml);
        }

        // BSON: starts with 4-byte LE document size, ends with 0x00
        #[cfg(feature = "bson-format")]
        if data.len() >= 5 {
            let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if size == data.len() && data[data.len() - 1] == 0x00 {
                return Some(SerializerType::Bson);
            }
        }

        // MessagePack (structural fast path): fixmap (0x80-0x8f), fixarray
        // (0x90-0x9f), fixstr (0xa0-0xbf), and the nil/bool/bin/ext/float/
        // int/str8-32/array16-32/map16-32 markers (0xc0-0xdf). None of
        // these bytes can begin valid JSON or plain ASCII/UTF-8 text, so
        // there is no ambiguity left to resolve with a real parse here.
        #[cfg(feature = "msgpack")]
        if matches!(data[0], 0x80..=0xdf) {
            return Some(SerializerType::MessagePack);
        }

        // MessagePack (fixint fallback): the positive/negative fixint
        // ranges (0x00-0x7f / 0xe0-0xff) are deliberately excluded from the
        // structural check above because they overlap with plain ASCII
        // text and UTF-8 lead bytes - blindly matching them would turn this
        // heuristic into a near-blanket MessagePack match. Instead, such a
        // byte is classified as MessagePack only if the whole buffer is a
        // single, fully-consumed, self-describing MessagePack value.
        #[cfg(feature = "msgpack")]
        {
            let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(data));
            let parsed: Result<serde::de::IgnoredAny, _> =
                serde::de::Deserialize::deserialize(&mut de);
            if parsed.is_ok() && de.position() as usize == data.len() {
                return Some(SerializerType::MessagePack);
            }
        }

        // Protobuf: harder to detect, use heuristic
        // Field tag format: (field_number << 3) | wire_type
        // Wire type 0-5, field number > 0
        // Require at least 2 bytes (tag + value) and the first byte must not be
        // plain ASCII whitespace or printable text (to avoid false positives).
        // Tried last: unlike the checks above, this heuristic cannot verify
        // its guess against the rest of the buffer.
        #[cfg(feature = "protobuf")]
        if data.len() >= 2 {
            let first = data[0];
            let wire_type = first & 0x07;
            let field_number = first >> 3;
            if wire_type <= 5
                && field_number > 0
                && field_number < 100
                && !first.is_ascii_whitespace()
                && !first.is_ascii_alphanumeric()
                && first != b'_'
                && first != b'-'
            {
                return Some(SerializerType::Protobuf);
            }
        }

        None
    }

    /// Negotiate the best serialization format between local and remote capabilities.
    ///
    /// Iterates through `local_preferred` in order and returns the first type
    /// that also appears in `remote_supported`. Returns `None` if there is no
    /// overlap between the two sets.
    pub fn negotiate(
        local_preferred: &[SerializerType],
        remote_supported: &[SerializerType],
    ) -> Option<SerializerType> {
        local_preferred
            .iter()
            .find(|s| remote_supported.contains(s))
            .copied()
    }

    /// Get all available serializer types based on enabled features.
    ///
    /// JSON is always included. Additional types are added when their
    /// corresponding Cargo features are enabled.
    pub fn available_types() -> Vec<SerializerType> {
        #[allow(unused_mut)]
        let mut types = vec![SerializerType::Json]; // always available

        #[cfg(feature = "msgpack")]
        types.push(SerializerType::MessagePack);

        #[cfg(feature = "yaml")]
        types.push(SerializerType::Yaml);

        #[cfg(feature = "bson-format")]
        types.push(SerializerType::Bson);

        #[cfg(feature = "protobuf")]
        types.push(SerializerType::Protobuf);

        types
    }
}

/// Object-safe trait for user-provided (custom) serializers.
///
/// The built-in [`Serializer`] trait uses generic methods
/// (`serialize<T: Serialize>`) and therefore cannot be turned into a trait
/// object (`dyn Serializer`). To support a *runtime* registry of user-supplied
/// serializers, this trait operates on the universal serde intermediate
/// [`serde_json::Value`] instead, which keeps it object-safe so implementations
/// can be stored as `Box<dyn CustomSerializer>`.
///
/// Implementors convert a [`serde_json::Value`] to/from bytes using whatever
/// encoding they wish (a domain-specific text format, a third-party binary
/// codec, an encrypted envelope, etc.) and advertise their content type and
/// name so the [`CustomSerializerRegistry`] can dispatch by either.
///
/// # Note on Pickle
///
/// A Python-`pickle` serializer is *intentionally* not provided here, and
/// callers are strongly discouraged from implementing one: unpickling executes
/// arbitrary code embedded in the payload and is a remote-code-execution risk.
/// Use a safe format (JSON, MessagePack, YAML, ...) instead.
///
/// # Example
///
/// ```
/// use celers_protocol::serializer::{CustomSerializer, CustomSerializerRegistry, SerializerError};
/// use serde_json::Value;
///
/// /// A trivial "uppercase JSON" serializer for demonstration.
/// struct UpperJson;
///
/// impl CustomSerializer for UpperJson {
///     fn name(&self) -> &str {
///         "upper-json"
///     }
///
///     fn content_type(&self) -> &str {
///         "application/x-upper-json"
///     }
///
///     fn serialize_value(&self, value: &Value) -> Result<Vec<u8>, SerializerError> {
///         let s = serde_json::to_string(value)
///             .map_err(|e| SerializerError::Serialize(e.to_string()))?;
///         Ok(s.to_uppercase().into_bytes())
///     }
///
///     fn deserialize_value(&self, bytes: &[u8]) -> Result<Value, SerializerError> {
///         let s = std::str::from_utf8(bytes)
///             .map_err(|e| SerializerError::Deserialize(e.to_string()))?;
///         serde_json::from_str(&s.to_lowercase())
///             .map_err(|e| SerializerError::Deserialize(e.to_string()))
///     }
/// }
///
/// let mut registry = CustomSerializerRegistry::new();
/// registry.register(Box::new(UpperJson));
///
/// let value = serde_json::json!({"ok": true});
/// let bytes = registry
///     .serialize_by_name("upper-json", &value)
///     .expect("serializer registered");
/// let decoded: Value = registry
///     .deserialize_by_content_type("application/x-upper-json", &bytes)
///     .expect("serializer registered");
/// assert_eq!(value, decoded);
/// ```
pub trait CustomSerializer: Send + Sync {
    /// Unique name of this serializer (used for name-based dispatch).
    fn name(&self) -> &str;

    /// Content type advertised by this serializer (used for content-type
    /// dispatch and auto-detection wiring).
    fn content_type(&self) -> &str;

    /// Content encoding produced by this serializer.
    ///
    /// Defaults to [`ContentEncoding::Binary`] because custom formats are
    /// frequently binary. Text-based implementations should override this to
    /// return [`ContentEncoding::Utf8`].
    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Binary
    }

    /// Serialize a [`serde_json::Value`] to bytes.
    fn serialize_value(&self, value: &serde_json::Value) -> SerializerResult<Vec<u8>>;

    /// Deserialize bytes into a [`serde_json::Value`].
    fn deserialize_value(&self, bytes: &[u8]) -> SerializerResult<serde_json::Value>;
}

/// Registry of user-provided [`CustomSerializer`] implementations.
///
/// Serializers are stored as trait objects and can be looked up by either their
/// [`CustomSerializer::name`] or [`CustomSerializer::content_type`]. This
/// complements [`SerializerType`] (which handles the built-in, statically-known
/// formats) by allowing applications to plug in additional formats at runtime
/// without modifying this crate.
///
/// Registering a serializer whose name or content type collides with an
/// existing entry replaces that mapping (last registration wins), which makes
/// it possible to override a previously installed serializer.
#[derive(Default)]
pub struct CustomSerializerRegistry {
    by_name: std::collections::HashMap<String, std::sync::Arc<dyn CustomSerializer>>,
    by_content_type: std::collections::HashMap<String, std::sync::Arc<dyn CustomSerializer>>,
}

impl CustomSerializerRegistry {
    /// Create a new, empty custom serializer registry.
    pub fn new() -> Self {
        Self {
            by_name: std::collections::HashMap::new(),
            by_content_type: std::collections::HashMap::new(),
        }
    }

    /// Register a custom serializer.
    ///
    /// The serializer is indexed by both its [`CustomSerializer::name`] and its
    /// [`CustomSerializer::content_type`]. Any existing entry under either key
    /// is replaced.
    pub fn register(&mut self, serializer: Box<dyn CustomSerializer>) {
        let serializer: std::sync::Arc<dyn CustomSerializer> = serializer.into();
        let name = serializer.name().to_string();
        let content_type = serializer.content_type().to_string();
        self.by_name
            .insert(name, std::sync::Arc::clone(&serializer));
        self.by_content_type.insert(content_type, serializer);
    }

    /// Remove a serializer by name.
    ///
    /// Also removes the corresponding content-type mapping. Returns `true` if a
    /// serializer was found and removed.
    pub fn unregister(&mut self, name: &str) -> bool {
        if let Some(serializer) = self.by_name.remove(name) {
            let content_type = serializer.content_type().to_string();
            // Only drop the content-type mapping if it still points at the same
            // serializer (a later registration under that content type wins).
            if let Some(existing) = self.by_content_type.get(&content_type) {
                if std::sync::Arc::ptr_eq(existing, &serializer) {
                    self.by_content_type.remove(&content_type);
                }
            }
            true
        } else {
            false
        }
    }

    /// Look up a registered serializer by name.
    #[inline]
    pub fn get_by_name(&self, name: &str) -> Option<&dyn CustomSerializer> {
        self.by_name.get(name).map(|s| s.as_ref())
    }

    /// Look up a registered serializer by content type.
    #[inline]
    pub fn get_by_content_type(&self, content_type: &str) -> Option<&dyn CustomSerializer> {
        self.by_content_type.get(content_type).map(|s| s.as_ref())
    }

    /// Returns `true` if a serializer with the given name is registered.
    #[inline]
    pub fn contains_name(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Returns `true` if a serializer for the given content type is registered.
    #[inline]
    pub fn contains_content_type(&self, content_type: &str) -> bool {
        self.by_content_type.contains_key(content_type)
    }

    /// Number of registered serializers (counted by unique name).
    #[inline]
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Returns `true` if no serializers are registered.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// List the names of all registered serializers.
    pub fn names(&self) -> Vec<&str> {
        self.by_name.keys().map(String::as_str).collect()
    }

    /// List the content types of all registered serializers.
    pub fn content_types(&self) -> Vec<&str> {
        self.by_content_type.keys().map(String::as_str).collect()
    }

    /// Serialize a value using the serializer registered under `name`.
    ///
    /// The value is first converted to a [`serde_json::Value`] (the universal
    /// serde intermediate) and then handed to the custom serializer. Returns an
    /// [`SerializerError::UnsupportedContentType`] if no serializer is
    /// registered under that name.
    pub fn serialize_by_name<T: Serialize>(
        &self,
        name: &str,
        value: &T,
    ) -> SerializerResult<Vec<u8>> {
        let serializer = self
            .get_by_name(name)
            .ok_or_else(|| SerializerError::UnsupportedContentType(name.to_string()))?;
        let value =
            serde_json::to_value(value).map_err(|e| SerializerError::Serialize(e.to_string()))?;
        serializer.serialize_value(&value)
    }

    /// Serialize a value using the serializer registered for `content_type`.
    pub fn serialize_by_content_type<T: Serialize>(
        &self,
        content_type: &str,
        value: &T,
    ) -> SerializerResult<Vec<u8>> {
        let serializer = self
            .get_by_content_type(content_type)
            .ok_or_else(|| SerializerError::UnsupportedContentType(content_type.to_string()))?;
        let value =
            serde_json::to_value(value).map_err(|e| SerializerError::Serialize(e.to_string()))?;
        serializer.serialize_value(&value)
    }

    /// Deserialize bytes using the serializer registered under `name`.
    ///
    /// The custom serializer produces a [`serde_json::Value`] which is then
    /// converted into the requested type `T`.
    pub fn deserialize_by_name<T: DeserializeOwned>(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> SerializerResult<T> {
        let serializer = self
            .get_by_name(name)
            .ok_or_else(|| SerializerError::UnsupportedContentType(name.to_string()))?;
        let value = serializer.deserialize_value(bytes)?;
        serde_json::from_value(value).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    /// Deserialize bytes using the serializer registered for `content_type`.
    pub fn deserialize_by_content_type<T: DeserializeOwned>(
        &self,
        content_type: &str,
        bytes: &[u8],
    ) -> SerializerResult<T> {
        let serializer = self
            .get_by_content_type(content_type)
            .ok_or_else(|| SerializerError::UnsupportedContentType(content_type.to_string()))?;
        let value = serializer.deserialize_value(bytes)?;
        serde_json::from_value(value).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_json_serializer_round_trip() {
        let serializer = JsonSerializer;
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let bytes = serializer.serialize(&data).unwrap();
        let decoded: TestData = serializer.deserialize(&bytes).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_json_serializer_content_type() {
        let serializer = JsonSerializer;
        assert_eq!(serializer.content_type(), ContentType::Json);
        assert_eq!(serializer.content_encoding(), ContentEncoding::Utf8);
        assert_eq!(serializer.name(), "json");
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_msgpack_serializer_round_trip() {
        let serializer = MessagePackSerializer;
        let data = TestData {
            name: "msgpack_test".to_string(),
            value: 100,
        };

        let bytes = serializer.serialize(&data).unwrap();
        let decoded: TestData = serializer.deserialize(&bytes).unwrap();

        assert_eq!(data, decoded);
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_msgpack_serializer_content_type() {
        let serializer = MessagePackSerializer;
        assert_eq!(serializer.content_type(), ContentType::MessagePack);
        assert_eq!(serializer.content_encoding(), ContentEncoding::Binary);
        assert_eq!(serializer.name(), "msgpack");
    }

    #[test]
    fn test_get_serializer_json() {
        let serializer = get_serializer("application/json").unwrap();
        assert_eq!(serializer.name(), "json");
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_get_serializer_msgpack() {
        let serializer = get_serializer("application/x-msgpack").unwrap();
        assert_eq!(serializer.name(), "msgpack");
    }

    #[test]
    fn test_get_serializer_unsupported() {
        let result = get_serializer("application/unsupported");
        assert!(result.is_err());
        match result {
            Err(SerializerError::UnsupportedContentType(ct)) => {
                assert_eq!(ct, "application/unsupported");
            }
            _ => panic!("Expected UnsupportedContentType error"),
        }
    }

    #[test]
    fn test_serializer_registry() {
        let registry = SerializerRegistry::new();
        assert_eq!(registry.default_serializer().name(), "json");

        let json = registry.get("application/json").unwrap();
        assert_eq!(json.name(), "json");
    }

    #[test]
    fn test_serializer_registry_available() {
        let available = SerializerRegistry::available();
        assert!(available.contains(&"application/json"));
    }

    #[test]
    fn test_serializer_error_display() {
        let err = SerializerError::Serialize("test error".to_string());
        assert_eq!(err.to_string(), "Serialization error: test error");

        let err = SerializerError::Deserialize("parse failed".to_string());
        assert_eq!(err.to_string(), "Deserialization error: parse failed");

        let err = SerializerError::UnsupportedContentType("text/plain".to_string());
        assert_eq!(err.to_string(), "Unsupported content type: text/plain");

        let err = SerializerError::Compression("gzip failed".to_string());
        assert_eq!(err.to_string(), "Compression error: gzip failed");
    }

    #[cfg(feature = "bson-format")]
    #[test]
    fn test_bson_serializer_round_trip() {
        let serializer = BsonSerializer;
        let data = TestData {
            name: "bson_test".to_string(),
            value: 200,
        };

        let bytes = serializer.serialize(&data).unwrap();
        let decoded: TestData = serializer.deserialize(&bytes).unwrap();

        assert_eq!(data, decoded);
    }

    #[cfg(feature = "bson-format")]
    #[test]
    fn test_bson_serializer_content_type() {
        let serializer = BsonSerializer;
        assert_eq!(
            serializer.content_type(),
            ContentType::Custom("application/bson".to_string())
        );
        assert_eq!(serializer.content_encoding(), ContentEncoding::Binary);
        assert_eq!(serializer.name(), "bson");
    }

    #[cfg(feature = "bson-format")]
    #[test]
    fn test_get_serializer_bson() {
        let serializer = get_serializer("application/bson").unwrap();
        assert_eq!(serializer.name(), "bson");
    }

    #[test]
    fn test_serializer_type_equality() {
        let json1 = SerializerType::Json;
        let json2 = SerializerType::Json;
        assert_eq!(json1, json2);

        #[cfg(feature = "msgpack")]
        {
            let msgpack = SerializerType::MessagePack;
            assert_ne!(json1, msgpack);
        }
    }

    #[test]
    fn test_serializer_type_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(SerializerType::Json);
        set.insert(SerializerType::Json); // Duplicate

        #[cfg(feature = "msgpack")]
        set.insert(SerializerType::MessagePack);

        #[cfg(feature = "msgpack")]
        assert_eq!(set.len(), 2);

        #[cfg(not(feature = "msgpack"))]
        assert_eq!(set.len(), 1);

        assert!(set.contains(&SerializerType::Json));
    }

    #[test]
    fn test_serializer_type_display() {
        assert_eq!(SerializerType::Json.to_string(), "json");

        #[cfg(feature = "msgpack")]
        assert_eq!(SerializerType::MessagePack.to_string(), "msgpack");

        #[cfg(feature = "yaml")]
        assert_eq!(SerializerType::Yaml.to_string(), "yaml");

        #[cfg(feature = "bson-format")]
        assert_eq!(SerializerType::Bson.to_string(), "bson");
    }

    #[test]
    fn test_serializer_type_try_from() {
        use std::convert::TryFrom;

        let json = SerializerType::try_from("application/json").unwrap();
        assert_eq!(json, SerializerType::Json);

        #[cfg(feature = "msgpack")]
        {
            let msgpack = SerializerType::try_from("application/x-msgpack").unwrap();
            assert_eq!(msgpack, SerializerType::MessagePack);
        }

        #[cfg(feature = "yaml")]
        {
            let yaml = SerializerType::try_from("application/yaml").unwrap();
            assert_eq!(yaml, SerializerType::Yaml);
        }

        // Test error case
        let result = SerializerType::try_from("application/unsupported");
        assert!(result.is_err());
    }

    #[test]
    fn test_serializer_type_default() {
        let default_type = SerializerType::default();
        assert_eq!(default_type, SerializerType::Json);
    }

    #[test]
    fn test_serializer_type_copy() {
        let json = SerializerType::Json;
        let json_copy = json; // Copy trait
        let _json_original = json; // Can still use original

        assert_eq!(json_copy, SerializerType::Json);
    }

    // ---- Format detection tests ----

    #[test]
    fn test_detect_format_empty_data() {
        assert!(SerializerRegistry::detect_format(&[]).is_none());
    }

    #[test]
    fn test_detect_format_json_object() {
        let data = br#"{"key": "value"}"#;
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Json)
        );
    }

    #[test]
    fn test_detect_format_json_array() {
        let data = b"[1,2,3]";
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Json)
        );
    }

    #[test]
    fn test_detect_format_json_with_leading_whitespace() {
        let data = b"  \t\n  {\"key\": \"value\"}";
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Json)
        );
    }

    #[test]
    fn test_detect_format_json_array_with_whitespace() {
        let data = b"   [1, 2, 3]";
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Json)
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_detect_format_msgpack() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert("key", "value");
        let bytes = rmp_serde::to_vec(&map).expect("msgpack serialization failed");
        assert_eq!(
            SerializerRegistry::detect_format(&bytes),
            Some(SerializerType::MessagePack)
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_detect_format_msgpack_fixmap() {
        // fixmap with 1 entry: 0x81
        let data: &[u8] = &[
            0x81, 0xa3, b'k', b'e', b'y', 0xa5, b'v', b'a', b'l', b'u', b'e',
        ];
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::MessagePack)
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_detect_format_msgpack_fixstr_top_level() {
        // Regression: fixstr (0xa0-0xbf) was previously omitted from the
        // MessagePack detection range, so a top-level MessagePack string
        // was never detected.
        let data: &[u8] = &[0xa5, b'h', b'e', b'l', b'l', b'o']; // fixstr "hello"
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::MessagePack)
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_detect_format_msgpack_single_positive_fixint_not_misdetected_as_json() {
        // Regression: 0x7b is both the ASCII code for '{' and a valid
        // MessagePack positive fixint (123). A lone byte 0x7b is *not*
        // valid JSON (an unterminated object), so it must be classified as
        // MessagePack, not JSON.
        let data: &[u8] = &[0x7b];
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::MessagePack)
        );

        // Same story for 0x5b ('[' / fixint 91).
        let data: &[u8] = &[0x5b];
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::MessagePack)
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_detect_format_msgpack_single_negative_fixint() {
        // 0xff is a valid MessagePack negative fixint (-1), previously
        // never detected since the fixint ranges were omitted entirely.
        let data: &[u8] = &[0xff];
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::MessagePack)
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_detect_format_msgpack_fixint_fallback_does_not_misdetect_plain_text() {
        // The fixint fallback must not turn into a blanket MessagePack
        // match for ordinary multi-byte ASCII text: only the first byte
        // would parse as a (single-byte) fixint, leaving the rest of the
        // buffer unconsumed, so the full-consumption check must reject it.
        let data = b"hello world, this is plain ascii text";
        assert_ne!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::MessagePack)
        );
    }

    #[test]
    fn test_detect_format_json_still_wins_for_real_json_starting_with_brace_or_bracket() {
        // Full JSON objects/arrays must still be classified as JSON, not
        // swept up by the new MessagePack fixint fallback (this holds
        // regardless of which optional features are enabled, since a real
        // JSON document is never also a single, fully-consumed MessagePack
        // value).
        assert_eq!(
            SerializerRegistry::detect_format(br#"{"key": "value"}"#),
            Some(SerializerType::Json)
        );
        assert_eq!(
            SerializerRegistry::detect_format(b"[1,2,3]"),
            Some(SerializerType::Json)
        );
    }

    #[cfg(feature = "bson-format")]
    #[test]
    fn test_detect_format_bson() {
        let data = TestData {
            name: "bson_detect".to_string(),
            value: 42,
        };
        let bytes = bson::serialize_to_vec(&data).expect("bson serialization failed");
        assert_eq!(
            SerializerRegistry::detect_format(&bytes),
            Some(SerializerType::Bson)
        );
    }

    #[cfg(feature = "bson-format")]
    #[test]
    fn test_detect_format_bson_not_matching_size() {
        // 4-byte LE size says 100, but actual length is 5 -> not BSON
        let data: &[u8] = &[100, 0, 0, 0, 0x00];
        assert_ne!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Bson)
        );
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_detect_format_yaml() {
        let data = b"---\nkey: value\n";
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Yaml)
        );
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_detect_format_yaml_minimal() {
        let data = b"---";
        assert_eq!(
            SerializerRegistry::detect_format(data),
            Some(SerializerType::Yaml)
        );
    }

    #[test]
    fn test_detect_format_unknown_binary() {
        // Random binary that doesn't match any known pattern
        let data: &[u8] = &[0x00, 0x00, 0x00];
        // With all features, BSON check: size=0 != len=3, msgpack: 0x00 not in range,
        // protobuf: field_number=0 (invalid). Should be None.
        assert!(SerializerRegistry::detect_format(data).is_none());
    }

    #[test]
    fn test_detect_format_whitespace_only() {
        let data = b"   \t\n  ";
        // No non-whitespace byte found, trimmed position is None
        assert!(SerializerRegistry::detect_format(data).is_none());
    }

    // ---- Negotiation tests ----

    #[test]
    fn test_negotiate_overlap() {
        let local = [SerializerType::Json];
        let remote = [SerializerType::Json];
        assert_eq!(
            SerializerRegistry::negotiate(&local, &remote),
            Some(SerializerType::Json)
        );
    }

    #[test]
    fn test_negotiate_prefers_local_order() {
        #[cfg(feature = "msgpack")]
        {
            let local = [SerializerType::MessagePack, SerializerType::Json];
            let remote = [SerializerType::Json, SerializerType::MessagePack];
            assert_eq!(
                SerializerRegistry::negotiate(&local, &remote),
                Some(SerializerType::MessagePack)
            );
        }
    }

    #[test]
    fn test_negotiate_disjoint() {
        // With only Json on one side and nothing on the other
        let local = [SerializerType::Json];
        let remote: &[SerializerType] = &[];
        assert!(SerializerRegistry::negotiate(&local, remote).is_none());
    }

    #[test]
    fn test_negotiate_empty_local() {
        let local: &[SerializerType] = &[];
        let remote = [SerializerType::Json];
        assert!(SerializerRegistry::negotiate(local, &remote).is_none());
    }

    #[test]
    fn test_negotiate_both_empty() {
        let local: &[SerializerType] = &[];
        let remote: &[SerializerType] = &[];
        assert!(SerializerRegistry::negotiate(local, remote).is_none());
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_negotiate_partial_overlap() {
        let local = [SerializerType::MessagePack, SerializerType::Json];
        let remote = [SerializerType::Json];
        assert_eq!(
            SerializerRegistry::negotiate(&local, &remote),
            Some(SerializerType::Json)
        );
    }

    // ---- available_types tests ----

    #[test]
    fn test_available_types_always_has_json() {
        let types = SerializerRegistry::available_types();
        assert!(types.contains(&SerializerType::Json));
        // JSON should be the first element
        assert_eq!(types[0], SerializerType::Json);
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_available_types_has_msgpack() {
        let types = SerializerRegistry::available_types();
        assert!(types.contains(&SerializerType::MessagePack));
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_available_types_has_yaml() {
        let types = SerializerRegistry::available_types();
        assert!(types.contains(&SerializerType::Yaml));
    }

    #[cfg(feature = "bson-format")]
    #[test]
    fn test_available_types_has_bson() {
        let types = SerializerRegistry::available_types();
        assert!(types.contains(&SerializerType::Bson));
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn test_available_types_has_protobuf() {
        let types = SerializerRegistry::available_types();
        assert!(types.contains(&SerializerType::Protobuf));
    }

    #[test]
    fn test_available_types_count() {
        let types = SerializerRegistry::available_types();
        #[allow(unused_mut)]
        let mut expected = 1; // Json always; incremented below if optional serializers are enabled

        #[cfg(feature = "msgpack")]
        {
            expected += 1;
        }
        #[cfg(feature = "yaml")]
        {
            expected += 1;
        }
        #[cfg(feature = "bson-format")]
        {
            expected += 1;
        }
        #[cfg(feature = "protobuf")]
        {
            expected += 1;
        }

        assert_eq!(types.len(), expected);
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn test_serializer_type_protobuf_name() {
        assert_eq!(SerializerType::Protobuf.name(), "protobuf");
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn test_serializer_type_protobuf_content_type() {
        assert_eq!(
            SerializerType::Protobuf.content_type(),
            ContentType::Custom("application/protobuf".to_string())
        );
    }

    // ---- YAML serializer tests ----

    #[cfg(feature = "yaml")]
    #[test]
    fn test_yaml_serializer_round_trip() {
        let serializer = YamlSerializer;
        let data = TestData {
            name: "yaml_test".to_string(),
            value: 314,
        };

        let bytes = serializer.serialize(&data).unwrap();
        // YAML is a text format: the encoded payload must be valid UTF-8.
        let text = std::str::from_utf8(&bytes).expect("YAML output should be UTF-8");
        assert!(text.contains("yaml_test"));
        assert!(text.contains("314"));

        let decoded: TestData = serializer.deserialize(&bytes).unwrap();
        assert_eq!(data, decoded);
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_yaml_serializer_content_type() {
        let serializer = YamlSerializer;
        assert_eq!(
            serializer.content_type(),
            ContentType::Custom("application/x-yaml".to_string())
        );
        assert_eq!(serializer.content_encoding(), ContentEncoding::Utf8);
        assert_eq!(serializer.name(), "yaml");
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_yaml_serializer_value_round_trip() {
        // Round-trip a representative dynamic serde value (a message-like map).
        let serializer = YamlSerializer;
        let value = serde_json::json!({
            "task": "tasks.add",
            "args": [2, 3],
            "kwargs": {"factor": 10},
            "retries": 0,
            "persistent": true,
        });

        let bytes = serializer.serialize(&value).unwrap();
        let decoded: serde_json::Value = serializer.deserialize(&bytes).unwrap();
        assert_eq!(value, decoded);
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_yaml_content_type_aliases_detect() {
        // Both the primary and alias content types must resolve to YAML.
        for ct in ["application/x-yaml", "application/yaml", "text/yaml"] {
            let serializer = get_serializer(ct).unwrap();
            assert_eq!(serializer.name(), "yaml", "content type {ct}");
        }
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn test_yaml_serializer_type_dispatch_round_trip() {
        // Exercise the SerializerType dispatch arm (not just the concrete type).
        let data = TestData {
            name: "dispatch".to_string(),
            value: 7,
        };
        let bytes = SerializerType::Yaml.serialize(&data).unwrap();
        let decoded: TestData = SerializerType::Yaml.deserialize(&bytes).unwrap();
        assert_eq!(data, decoded);
        assert_eq!(
            SerializerType::Yaml.content_encoding(),
            ContentEncoding::Utf8
        );
    }

    // ---- Custom serializer registry tests ----

    /// A demonstration custom serializer that wraps JSON bytes with a fixed
    /// magic prefix. Round-trips through `serde_json::Value`.
    struct PrefixedJson;

    const PREFIX: &[u8] = b"PJSN:";

    impl CustomSerializer for PrefixedJson {
        fn name(&self) -> &str {
            "prefixed-json"
        }

        fn content_type(&self) -> &str {
            "application/x-prefixed-json"
        }

        fn content_encoding(&self) -> ContentEncoding {
            ContentEncoding::Utf8
        }

        fn serialize_value(&self, value: &serde_json::Value) -> SerializerResult<Vec<u8>> {
            let json =
                serde_json::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))?;
            let mut out = Vec::with_capacity(PREFIX.len() + json.len());
            out.extend_from_slice(PREFIX);
            out.extend_from_slice(&json);
            Ok(out)
        }

        fn deserialize_value(&self, bytes: &[u8]) -> SerializerResult<serde_json::Value> {
            let payload = bytes.strip_prefix(PREFIX).ok_or_else(|| {
                SerializerError::Deserialize("missing PJSN: magic prefix".to_string())
            })?;
            serde_json::from_slice(payload).map_err(|e| SerializerError::Deserialize(e.to_string()))
        }
    }

    #[test]
    fn test_custom_serializer_default_encoding() {
        // A serializer that does not override content_encoding should default to
        // Binary.
        struct BinaryCustom;
        impl CustomSerializer for BinaryCustom {
            fn name(&self) -> &str {
                "binary-custom"
            }
            fn content_type(&self) -> &str {
                "application/x-binary-custom"
            }
            fn serialize_value(&self, value: &serde_json::Value) -> SerializerResult<Vec<u8>> {
                serde_json::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
            }
            fn deserialize_value(&self, bytes: &[u8]) -> SerializerResult<serde_json::Value> {
                serde_json::from_slice(bytes)
                    .map_err(|e| SerializerError::Deserialize(e.to_string()))
            }
        }
        assert_eq!(BinaryCustom.content_encoding(), ContentEncoding::Binary);
    }

    #[test]
    fn test_custom_registry_empty() {
        let registry = CustomSerializerRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(!registry.contains_name("prefixed-json"));
        assert!(!registry.contains_content_type("application/x-prefixed-json"));
        assert!(registry.get_by_name("prefixed-json").is_none());
        assert!(registry
            .get_by_content_type("application/x-prefixed-json")
            .is_none());
    }

    #[test]
    fn test_custom_registry_register_and_lookup() {
        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
        assert!(registry.contains_name("prefixed-json"));
        assert!(registry.contains_content_type("application/x-prefixed-json"));

        let by_name = registry.get_by_name("prefixed-json").unwrap();
        assert_eq!(by_name.content_type(), "application/x-prefixed-json");
        assert_eq!(by_name.content_encoding(), ContentEncoding::Utf8);

        let by_ct = registry
            .get_by_content_type("application/x-prefixed-json")
            .unwrap();
        assert_eq!(by_ct.name(), "prefixed-json");
    }

    #[test]
    fn test_custom_registry_round_trip_by_name() {
        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));

        let data = TestData {
            name: "custom".to_string(),
            value: 99,
        };

        let bytes = registry.serialize_by_name("prefixed-json", &data).unwrap();
        assert!(bytes.starts_with(PREFIX));

        let decoded: TestData = registry
            .deserialize_by_name("prefixed-json", &bytes)
            .unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_custom_registry_round_trip_by_content_type() {
        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));

        // Serialize a dynamic serde Value and recover it via content-type dispatch.
        let value = serde_json::json!({"a": 1, "b": [true, null, "x"]});

        let bytes = registry
            .serialize_by_content_type("application/x-prefixed-json", &value)
            .unwrap();
        let decoded: serde_json::Value = registry
            .deserialize_by_content_type("application/x-prefixed-json", &bytes)
            .unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_custom_registry_cross_dispatch_equivalent() {
        // Serializing by name and by content type must yield identical bytes.
        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));

        let value = serde_json::json!({"k": "v"});
        let by_name = registry.serialize_by_name("prefixed-json", &value).unwrap();
        let by_ct = registry
            .serialize_by_content_type("application/x-prefixed-json", &value)
            .unwrap();
        assert_eq!(by_name, by_ct);
    }

    #[test]
    fn test_custom_registry_unknown_returns_error() {
        let registry = CustomSerializerRegistry::new();
        let value = serde_json::json!({"x": 1});

        let err = registry.serialize_by_name("nope", &value).unwrap_err();
        assert!(matches!(err, SerializerError::UnsupportedContentType(ct) if ct == "nope"));

        let err = registry
            .serialize_by_content_type("application/x-nope", &value)
            .unwrap_err();
        assert!(
            matches!(err, SerializerError::UnsupportedContentType(ct) if ct == "application/x-nope")
        );

        let err = registry
            .deserialize_by_name::<serde_json::Value>("nope", b"data")
            .unwrap_err();
        assert!(matches!(err, SerializerError::UnsupportedContentType(_)));
    }

    #[test]
    fn test_custom_registry_deserialize_propagates_error() {
        // The custom serializer rejects payloads without the magic prefix; that
        // error must surface from the registry as a Deserialize error.
        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));

        let err = registry
            .deserialize_by_name::<TestData>("prefixed-json", b"not-prefixed")
            .unwrap_err();
        assert!(matches!(err, SerializerError::Deserialize(_)));
    }

    #[test]
    fn test_custom_registry_unregister() {
        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));
        assert!(registry.contains_name("prefixed-json"));
        assert!(registry.contains_content_type("application/x-prefixed-json"));

        assert!(registry.unregister("prefixed-json"));
        assert!(!registry.contains_name("prefixed-json"));
        // The content-type mapping is dropped alongside the name mapping.
        assert!(!registry.contains_content_type("application/x-prefixed-json"));
        assert!(registry.is_empty());

        // Unregistering a missing serializer reports false.
        assert!(!registry.unregister("prefixed-json"));
    }

    #[test]
    fn test_custom_registry_re_register_overrides() {
        // A second serializer sharing the same name replaces the first.
        struct PrefixedJsonV2;
        impl CustomSerializer for PrefixedJsonV2 {
            fn name(&self) -> &str {
                "prefixed-json"
            }
            fn content_type(&self) -> &str {
                "application/x-prefixed-json"
            }
            fn serialize_value(&self, _value: &serde_json::Value) -> SerializerResult<Vec<u8>> {
                Ok(b"V2".to_vec())
            }
            fn deserialize_value(&self, _bytes: &[u8]) -> SerializerResult<serde_json::Value> {
                Ok(serde_json::json!("v2"))
            }
        }

        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));
        registry.register(Box::new(PrefixedJsonV2));

        // Still exactly one entry under that name.
        assert_eq!(registry.len(), 1);
        let bytes = registry
            .serialize_by_name("prefixed-json", &serde_json::json!({}))
            .unwrap();
        assert_eq!(bytes, b"V2");
    }

    #[test]
    fn test_custom_registry_names_and_content_types() {
        struct Other;
        impl CustomSerializer for Other {
            fn name(&self) -> &str {
                "other"
            }
            fn content_type(&self) -> &str {
                "application/x-other"
            }
            fn serialize_value(&self, value: &serde_json::Value) -> SerializerResult<Vec<u8>> {
                serde_json::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
            }
            fn deserialize_value(&self, bytes: &[u8]) -> SerializerResult<serde_json::Value> {
                serde_json::from_slice(bytes)
                    .map_err(|e| SerializerError::Deserialize(e.to_string()))
            }
        }

        let mut registry = CustomSerializerRegistry::new();
        registry.register(Box::new(PrefixedJson));
        registry.register(Box::new(Other));

        let mut names = registry.names();
        names.sort_unstable();
        assert_eq!(names, vec!["other", "prefixed-json"]);

        let mut cts = registry.content_types();
        cts.sort_unstable();
        assert_eq!(
            cts,
            vec!["application/x-other", "application/x-prefixed-json"]
        );
    }

    #[test]
    fn test_custom_registry_is_object_safe_and_default() {
        // Confirm the trait is object-safe (stored as Box<dyn ...>) and that the
        // registry has a working Default impl.
        let registry = CustomSerializerRegistry::default();
        assert!(registry.is_empty());
        let _boxed: Box<dyn CustomSerializer> = Box::new(PrefixedJson);
    }
}
