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
    fn content_type(&self) -> ContentType {
        ContentType::Json
    }

    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Utf8
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        serde_json::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        serde_json::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

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
    fn content_type(&self) -> ContentType {
        ContentType::MessagePack
    }

    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Binary
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        rmp_serde::to_vec(value).map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        rmp_serde::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

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
    fn content_type(&self) -> ContentType {
        ContentType::Custom("application/x-yaml".to_string())
    }

    fn content_encoding(&self) -> ContentEncoding {
        ContentEncoding::Utf8
    }

    fn serialize<T: Serialize>(&self, value: &T) -> SerializerResult<Vec<u8>> {
        serde_yaml::to_string(value)
            .map(|s| s.into_bytes())
            .map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        serde_yaml::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
    }

    fn name(&self) -> &'static str {
        "yaml"
    }
}

/// Serializer type enum for dynamic dispatch without dyn trait issues
#[derive(Debug, Clone, Copy)]
pub enum SerializerType {
    /// JSON serializer
    Json,
    /// MessagePack serializer
    #[cfg(feature = "msgpack")]
    MessagePack,
    /// YAML serializer
    #[cfg(feature = "yaml")]
    Yaml,
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
        }
    }

    /// Get the content type
    pub fn content_type(&self) -> ContentType {
        match self {
            SerializerType::Json => JsonSerializer.content_type(),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.content_type(),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.content_type(),
        }
    }

    /// Get the content encoding
    pub fn content_encoding(&self) -> ContentEncoding {
        match self {
            SerializerType::Json => JsonSerializer.content_encoding(),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.content_encoding(),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.content_encoding(),
        }
    }

    /// Get the name
    pub fn name(&self) -> &'static str {
        match self {
            SerializerType::Json => JsonSerializer.name(),
            #[cfg(feature = "msgpack")]
            SerializerType::MessagePack => MessagePackSerializer.name(),
            #[cfg(feature = "yaml")]
            SerializerType::Yaml => YamlSerializer.name(),
        }
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
        ]
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
}
