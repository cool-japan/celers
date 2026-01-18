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
        serde_yaml::to_string(value)
            .map(|s| s.into_bytes())
            .map_err(|e| SerializerError::Serialize(e.to_string()))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> SerializerResult<T> {
        serde_yaml::from_slice(bytes).map_err(|e| SerializerError::Deserialize(e.to_string()))
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
}
