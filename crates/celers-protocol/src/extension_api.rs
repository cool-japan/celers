//! Custom protocol extensions API
//!
//! This module provides a flexible extension system for adding custom fields
//! and behaviors to Celery protocol messages without breaking compatibility.
//!
//! # Examples
//!
//! ```
//! use celers_protocol::extension_api::{Extension, ExtensionRegistry, ExtensionValue};
//! use serde_json::json;
//!
//! // Define a custom extension
//! struct TelemetryExtension;
//!
//! impl Extension for TelemetryExtension {
//!     fn name(&self) -> &str {
//!         "telemetry"
//!     }
//!
//!     fn validate(&self, value: &ExtensionValue) -> Result<(), String> {
//!         // Custom validation logic
//!         if let ExtensionValue::Object(map) = value {
//!             if !map.contains_key("trace_id") {
//!                 return Err("Missing trace_id".to_string());
//!             }
//!         }
//!         Ok(())
//!     }
//! }
//!
//! // Register extension
//! let mut registry = ExtensionRegistry::new();
//! registry.register(Box::new(TelemetryExtension));
//!
//! // Use extension
//! let value = ExtensionValue::Object(
//!     vec![("trace_id".to_string(), json!("abc123"))]
//!         .into_iter()
//!         .collect()
//! );
//! registry.validate("telemetry", &value).unwrap();
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Extension value types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ExtensionValue {
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// Array of values
    Array(Vec<serde_json::Value>),
    /// Object/map of values
    Object(HashMap<String, serde_json::Value>),
    /// Null value
    Null,
}

/// Extension trait for custom protocol extensions
pub trait Extension: Send + Sync {
    /// Extension name (must be unique)
    fn name(&self) -> &str;

    /// Validate extension value
    fn validate(&self, value: &ExtensionValue) -> Result<(), String>;

    /// Transform extension value (optional)
    fn transform(&self, value: ExtensionValue) -> Result<ExtensionValue, String> {
        Ok(value)
    }

    /// Check if extension is compatible with protocol version
    fn is_compatible(&self, _version: crate::ProtocolVersion) -> bool {
        true // Compatible by default
    }
}

/// Registry for managing custom extensions
#[derive(Default)]
pub struct ExtensionRegistry {
    extensions: HashMap<String, Box<dyn Extension>>,
}

impl ExtensionRegistry {
    /// Create a new extension registry
    pub fn new() -> Self {
        Self {
            extensions: HashMap::new(),
        }
    }

    /// Register a new extension
    pub fn register(&mut self, extension: Box<dyn Extension>) -> Result<(), String> {
        let name = extension.name().to_string();

        if self.extensions.contains_key(&name) {
            return Err(format!("Extension '{}' already registered", name));
        }

        self.extensions.insert(name, extension);
        Ok(())
    }

    /// Unregister an extension
    pub fn unregister(&mut self, name: &str) -> bool {
        self.extensions.remove(name).is_some()
    }

    /// Get an extension by name
    pub fn get(&self, name: &str) -> Option<&dyn Extension> {
        self.extensions.get(name).map(|b| b.as_ref())
    }

    /// Check if an extension is registered
    #[inline]
    pub fn has(&self, name: &str) -> bool {
        self.extensions.contains_key(name)
    }

    /// List all registered extension names
    #[inline]
    pub fn list(&self) -> Vec<&str> {
        self.extensions.keys().map(|s| s.as_str()).collect()
    }

    /// Validate an extension value
    pub fn validate(&self, name: &str, value: &ExtensionValue) -> Result<(), String> {
        match self.get(name) {
            Some(ext) => ext.validate(value),
            None => Err(format!("Extension '{}' not registered", name)),
        }
    }

    /// Transform an extension value
    pub fn transform(&self, name: &str, value: ExtensionValue) -> Result<ExtensionValue, String> {
        match self.get(name) {
            Some(ext) => ext.transform(value),
            None => Err(format!("Extension '{}' not registered", name)),
        }
    }

    /// Validate all extensions in a map
    pub fn validate_all(&self, extensions: &HashMap<String, ExtensionValue>) -> Result<(), String> {
        for (name, value) in extensions {
            self.validate(name, value)?;
        }
        Ok(())
    }
}

/// Message with custom extensions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedMessage {
    /// Base message
    #[serde(flatten)]
    pub message: crate::Message,

    /// Custom extensions
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub extensions: HashMap<String, ExtensionValue>,
}

impl ExtendedMessage {
    /// Create an extended message
    pub fn new(message: crate::Message) -> Self {
        Self {
            message,
            extensions: HashMap::new(),
        }
    }

    /// Add an extension
    pub fn with_extension(mut self, name: String, value: ExtensionValue) -> Self {
        self.extensions.insert(name, value);
        self
    }

    /// Get an extension value
    pub fn get_extension(&self, name: &str) -> Option<&ExtensionValue> {
        self.extensions.get(name)
    }

    /// Remove an extension
    pub fn remove_extension(&mut self, name: &str) -> Option<ExtensionValue> {
        self.extensions.remove(name)
    }

    /// Validate all extensions
    pub fn validate_extensions(&self, registry: &ExtensionRegistry) -> Result<(), String> {
        registry.validate_all(&self.extensions)
    }
}

// Built-in extensions

/// Telemetry/tracing extension
pub struct TelemetryExtension;

impl Extension for TelemetryExtension {
    fn name(&self) -> &str {
        "telemetry"
    }

    fn validate(&self, value: &ExtensionValue) -> Result<(), String> {
        match value {
            ExtensionValue::Object(map) => {
                if !map.contains_key("trace_id") && !map.contains_key("span_id") {
                    return Err("Telemetry extension requires 'trace_id' or 'span_id'".to_string());
                }
                Ok(())
            }
            _ => Err("Telemetry extension must be an object".to_string()),
        }
    }
}

/// Metrics collection extension
pub struct MetricsExtension;

impl Extension for MetricsExtension {
    fn name(&self) -> &str {
        "metrics"
    }

    fn validate(&self, value: &ExtensionValue) -> Result<(), String> {
        match value {
            ExtensionValue::Object(_) | ExtensionValue::Array(_) => Ok(()),
            _ => Err("Metrics extension must be an object or array".to_string()),
        }
    }
}

/// Custom routing extension
pub struct RoutingExtension;

impl Extension for RoutingExtension {
    fn name(&self) -> &str {
        "routing"
    }

    fn validate(&self, value: &ExtensionValue) -> Result<(), String> {
        match value {
            ExtensionValue::Object(map) => {
                if let Some(priority) = map.get("priority") {
                    if let Some(p) = priority.as_i64() {
                        if !(0..=9).contains(&p) {
                            return Err("Routing priority must be 0-9".to_string());
                        }
                    }
                }
                Ok(())
            }
            _ => Err("Routing extension must be an object".to_string()),
        }
    }
}

/// Create a registry with built-in extensions
pub fn create_default_registry() -> ExtensionRegistry {
    let mut registry = ExtensionRegistry::new();
    registry
        .register(Box::new(TelemetryExtension))
        .expect("Failed to register TelemetryExtension");
    registry
        .register(Box::new(MetricsExtension))
        .expect("Failed to register MetricsExtension");
    registry
        .register(Box::new(RoutingExtension))
        .expect("Failed to register RoutingExtension");
    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn test_extension_registry() {
        let mut registry = ExtensionRegistry::new();

        struct TestExt;
        impl Extension for TestExt {
            fn name(&self) -> &str {
                "test"
            }
            fn validate(&self, _value: &ExtensionValue) -> Result<(), String> {
                Ok(())
            }
        }

        assert!(registry.register(Box::new(TestExt)).is_ok());
        assert!(registry.has("test"));
        assert_eq!(registry.list(), vec!["test"]);
    }

    #[test]
    fn test_duplicate_registration() {
        let mut registry = ExtensionRegistry::new();

        struct TestExt;
        impl Extension for TestExt {
            fn name(&self) -> &str {
                "test"
            }
            fn validate(&self, _value: &ExtensionValue) -> Result<(), String> {
                Ok(())
            }
        }

        assert!(registry.register(Box::new(TestExt)).is_ok());
        assert!(registry.register(Box::new(TestExt)).is_err());
    }

    #[test]
    fn test_extension_validation() {
        let registry = create_default_registry();

        let telemetry = ExtensionValue::Object(
            vec![("trace_id".to_string(), json!("abc123"))]
                .into_iter()
                .collect(),
        );

        assert!(registry.validate("telemetry", &telemetry).is_ok());
    }

    #[test]
    fn test_invalid_telemetry() {
        let registry = create_default_registry();

        let invalid = ExtensionValue::Object(HashMap::new());
        assert!(registry.validate("telemetry", &invalid).is_err());
    }

    #[test]
    fn test_extended_message() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&crate::TaskArgs::new()).unwrap();
        let msg = crate::Message::new("tasks.test".to_string(), task_id, body);

        let ext_msg = ExtendedMessage::new(msg).with_extension(
            "telemetry".to_string(),
            ExtensionValue::Object(
                vec![("trace_id".to_string(), json!("xyz789"))]
                    .into_iter()
                    .collect(),
            ),
        );

        assert!(ext_msg.get_extension("telemetry").is_some());
    }

    #[test]
    fn test_extended_message_validation() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&crate::TaskArgs::new()).unwrap();
        let msg = crate::Message::new("tasks.test".to_string(), task_id, body);

        let ext_msg = ExtendedMessage::new(msg).with_extension(
            "telemetry".to_string(),
            ExtensionValue::Object(
                vec![("trace_id".to_string(), json!("abc123"))]
                    .into_iter()
                    .collect(),
            ),
        );

        let registry = create_default_registry();
        assert!(ext_msg.validate_extensions(&registry).is_ok());
    }

    #[test]
    fn test_unregister_extension() {
        let mut registry = ExtensionRegistry::new();

        struct TestExt;
        impl Extension for TestExt {
            fn name(&self) -> &str {
                "test"
            }
            fn validate(&self, _value: &ExtensionValue) -> Result<(), String> {
                Ok(())
            }
        }

        registry.register(Box::new(TestExt)).unwrap();
        assert!(registry.has("test"));

        assert!(registry.unregister("test"));
        assert!(!registry.has("test"));
    }

    #[test]
    fn test_routing_extension_validation() {
        let registry = create_default_registry();

        let valid_routing = ExtensionValue::Object(
            vec![("priority".to_string(), json!(5))]
                .into_iter()
                .collect(),
        );
        assert!(registry.validate("routing", &valid_routing).is_ok());

        let invalid_routing = ExtensionValue::Object(
            vec![("priority".to_string(), json!(10))]
                .into_iter()
                .collect(),
        );
        assert!(registry.validate("routing", &invalid_routing).is_err());
    }

    #[test]
    fn test_extension_value_serialization() {
        let value = ExtensionValue::Object(
            vec![("key".to_string(), json!("value"))]
                .into_iter()
                .collect(),
        );

        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized: ExtensionValue = serde_json::from_str(&serialized).unwrap();

        assert_eq!(value, deserialized);
    }
}
