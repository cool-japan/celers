//! Worker metadata and version tracking
//!
//! This module provides metadata tracking for workers, including version information,
//! build metadata, and deployment context. This is useful for:
//!
//! - Deployment tracking and rollback
//! - A/B testing different worker versions
//! - Debugging production issues
//! - Monitoring worker fleet composition
//!
//! # Examples
//!
//! ```
//! use celers_worker::WorkerMetadata;
//!
//! // Create metadata for a worker
//! let metadata = WorkerMetadata::builder()
//!     .version("1.2.3")
//!     .build_info("git-abc123")
//!     .environment("production")
//!     .region("us-east-1")
//!     .add_label("team", "platform")
//!     .add_label("service", "task-processor")
//!     .build();
//!
//! assert_eq!(metadata.version(), "1.2.3");
//! assert_eq!(metadata.environment(), Some("production"));
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Worker metadata including version and deployment information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetadata {
    /// Worker version (e.g., "1.2.3", "v2.0.0-beta.1")
    version: String,

    /// Build information (e.g., git commit hash, build timestamp)
    #[serde(skip_serializing_if = "Option::is_none")]
    build_info: Option<String>,

    /// Deployment environment (e.g., "production", "staging", "development")
    #[serde(skip_serializing_if = "Option::is_none")]
    environment: Option<String>,

    /// Region or data center (e.g., "us-east-1", "eu-west-1")
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,

    /// Arbitrary key-value labels for custom metadata
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    labels: HashMap<String, String>,

    /// Timestamp when this metadata was created (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
}

impl WorkerMetadata {
    /// Create a new WorkerMetadata with the specified version
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            build_info: None,
            environment: None,
            region: None,
            labels: HashMap::new(),
            created_at: Some(chrono::Utc::now().to_rfc3339()),
        }
    }

    /// Create a builder for WorkerMetadata
    pub fn builder() -> WorkerMetadataBuilder {
        WorkerMetadataBuilder::new()
    }

    /// Get the worker version
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the build information
    pub fn build_info(&self) -> Option<&str> {
        self.build_info.as_deref()
    }

    /// Get the deployment environment
    pub fn environment(&self) -> Option<&str> {
        self.environment.as_deref()
    }

    /// Get the region
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// Get a label by key
    pub fn get_label(&self, key: &str) -> Option<&str> {
        self.labels.get(key).map(|s| s.as_str())
    }

    /// Get all labels
    pub fn labels(&self) -> &HashMap<String, String> {
        &self.labels
    }

    /// Get the creation timestamp
    pub fn created_at(&self) -> Option<&str> {
        self.created_at.as_deref()
    }

    /// Check if this metadata has a specific label
    pub fn has_label(&self, key: &str) -> bool {
        self.labels.contains_key(key)
    }

    /// Check if this metadata matches a specific version
    pub fn is_version(&self, version: &str) -> bool {
        self.version == version
    }

    /// Check if this metadata is for a specific environment
    pub fn is_environment(&self, env: &str) -> bool {
        self.environment.as_deref() == Some(env)
    }

    /// Convert to a JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Convert to a pretty JSON string
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Parse from a JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

impl Default for WorkerMetadata {
    fn default() -> Self {
        Self::new(env!("CARGO_PKG_VERSION"))
    }
}

impl std::fmt::Display for WorkerMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Worker[v{}]", self.version)?;
        if let Some(env) = &self.environment {
            write!(f, " env={}", env)?;
        }
        if let Some(region) = &self.region {
            write!(f, " region={}", region)?;
        }
        if let Some(build) = &self.build_info {
            write!(f, " build={}", build)?;
        }
        Ok(())
    }
}

/// Builder for WorkerMetadata
#[derive(Debug, Default)]
pub struct WorkerMetadataBuilder {
    version: Option<String>,
    build_info: Option<String>,
    environment: Option<String>,
    region: Option<String>,
    labels: HashMap<String, String>,
}

impl WorkerMetadataBuilder {
    /// Create a new WorkerMetadataBuilder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the worker version
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Set the build information
    pub fn build_info(mut self, build_info: impl Into<String>) -> Self {
        self.build_info = Some(build_info.into());
        self
    }

    /// Set the deployment environment
    pub fn environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Set the region
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Add a custom label
    pub fn add_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Set all labels at once
    pub fn labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    /// Build the WorkerMetadata
    ///
    /// If no version is set, uses the package version from Cargo.toml
    pub fn build(self) -> WorkerMetadata {
        WorkerMetadata {
            version: self
                .version
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
            build_info: self.build_info,
            environment: self.environment,
            region: self.region,
            labels: self.labels,
            created_at: Some(chrono::Utc::now().to_rfc3339()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_metadata_new() {
        let metadata = WorkerMetadata::new("1.0.0");
        assert_eq!(metadata.version(), "1.0.0");
        assert!(metadata.build_info().is_none());
        assert!(metadata.environment().is_none());
        assert!(metadata.region().is_none());
        assert!(metadata.labels().is_empty());
        assert!(metadata.created_at().is_some());
    }

    #[test]
    fn test_worker_metadata_default() {
        let metadata = WorkerMetadata::default();
        assert_eq!(metadata.version(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_worker_metadata_builder() {
        let metadata = WorkerMetadata::builder()
            .version("2.0.0")
            .build_info("git-abc123")
            .environment("production")
            .region("us-west-2")
            .add_label("team", "platform")
            .add_label("service", "worker")
            .build();

        assert_eq!(metadata.version(), "2.0.0");
        assert_eq!(metadata.build_info(), Some("git-abc123"));
        assert_eq!(metadata.environment(), Some("production"));
        assert_eq!(metadata.region(), Some("us-west-2"));
        assert_eq!(metadata.get_label("team"), Some("platform"));
        assert_eq!(metadata.get_label("service"), Some("worker"));
        assert!(metadata.has_label("team"));
        assert!(!metadata.has_label("nonexistent"));
    }

    #[test]
    fn test_worker_metadata_predicates() {
        let metadata = WorkerMetadata::builder()
            .version("1.5.0")
            .environment("staging")
            .build();

        assert!(metadata.is_version("1.5.0"));
        assert!(!metadata.is_version("2.0.0"));
        assert!(metadata.is_environment("staging"));
        assert!(!metadata.is_environment("production"));
    }

    #[test]
    fn test_worker_metadata_display() {
        let metadata = WorkerMetadata::builder()
            .version("3.0.0")
            .environment("production")
            .region("eu-central-1")
            .build_info("build-456")
            .build();

        let display = format!("{}", metadata);
        assert!(display.contains("v3.0.0"));
        assert!(display.contains("production"));
        assert!(display.contains("eu-central-1"));
        assert!(display.contains("build-456"));
    }

    #[test]
    fn test_worker_metadata_json_serialization() {
        let metadata = WorkerMetadata::builder()
            .version("1.0.0")
            .environment("test")
            .add_label("key", "value")
            .build();

        let json = metadata.to_json().unwrap();
        assert!(json.contains("1.0.0"));
        assert!(json.contains("test"));

        let deserialized = WorkerMetadata::from_json(&json).unwrap();
        assert_eq!(deserialized.version(), "1.0.0");
        assert_eq!(deserialized.environment(), Some("test"));
        assert_eq!(deserialized.get_label("key"), Some("value"));
    }

    #[test]
    fn test_worker_metadata_builder_minimal() {
        let metadata = WorkerMetadata::builder().build();
        // Should use package version from Cargo.toml
        assert_eq!(metadata.version(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_worker_metadata_labels() {
        let mut labels = HashMap::new();
        labels.insert("k1".to_string(), "v1".to_string());
        labels.insert("k2".to_string(), "v2".to_string());

        let metadata = WorkerMetadata::builder()
            .version("1.0.0")
            .labels(labels.clone())
            .build();

        assert_eq!(metadata.labels(), &labels);
        assert_eq!(metadata.get_label("k1"), Some("v1"));
        assert_eq!(metadata.get_label("k2"), Some("v2"));
    }
}
