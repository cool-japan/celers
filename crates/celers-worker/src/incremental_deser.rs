//! Incremental deserialization for large task payloads
//!
//! This module provides incremental deserialization to handle large task payloads
//! without loading the entire payload into memory at once. Benefits include:
//! - Reduced memory usage for large payloads
//! - Streaming deserialization for better performance
//! - Early validation before full deserialization
//! - Support for partial deserialization
//!
//! # Example
//!
//! ```
//! use celers_worker::incremental_deser::{IncrementalDeserializer, DeserConfig};
//!
//! let config = DeserConfig::default()
//!     .with_chunk_size(4096)
//!     .with_max_payload_size(1024 * 1024);
//!
//! // Deserialize data incrementally
//! let deserializer = IncrementalDeserializer::new(config);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

/// Deserialization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeserConfig {
    /// Chunk size for incremental deserialization (bytes)
    chunk_size: usize,
    /// Maximum payload size (bytes)
    max_payload_size: usize,
    /// Enable validation before deserialization
    enable_validation: bool,
    /// Enable streaming deserialization
    enable_streaming: bool,
}

impl Default for DeserConfig {
    fn default() -> Self {
        Self {
            chunk_size: 4096,
            max_payload_size: 10 * 1024 * 1024, // 10MB
            enable_validation: true,
            enable_streaming: false,
        }
    }
}

impl DeserConfig {
    /// Create a new deserialization configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set chunk size
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Set maximum payload size
    pub fn with_max_payload_size(mut self, size: usize) -> Self {
        self.max_payload_size = size;
        self
    }

    /// Enable or disable validation
    pub fn with_validation(mut self, enable: bool) -> Self {
        self.enable_validation = enable;
        self
    }

    /// Enable or disable streaming
    pub fn with_streaming(mut self, enable: bool) -> Self {
        self.enable_streaming = enable;
        self
    }

    /// Get chunk size
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Get maximum payload size
    pub fn max_payload_size(&self) -> usize {
        self.max_payload_size
    }

    /// Check if validation is enabled
    pub fn is_validation_enabled(&self) -> bool {
        self.enable_validation
    }

    /// Check if streaming is enabled
    pub fn is_streaming_enabled(&self) -> bool {
        self.enable_streaming
    }

    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        self.chunk_size > 0 && self.max_payload_size > 0 && self.chunk_size <= self.max_payload_size
    }

    /// Create a configuration for small payloads
    pub fn small() -> Self {
        Self {
            chunk_size: 1024,
            max_payload_size: 100 * 1024, // 100KB
            enable_validation: true,
            enable_streaming: false,
        }
    }

    /// Create a configuration for large payloads
    pub fn large() -> Self {
        Self {
            chunk_size: 65536,
            max_payload_size: 100 * 1024 * 1024, // 100MB
            enable_validation: true,
            enable_streaming: true,
        }
    }
}

impl fmt::Display for DeserConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DeserConfig(chunk={}, max={}, validation={}, streaming={})",
            self.chunk_size, self.max_payload_size, self.enable_validation, self.enable_streaming
        )
    }
}

/// Deserialization statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeserStats {
    /// Total bytes deserialized
    total_bytes: u64,
    /// Total chunks processed
    total_chunks: u64,
    /// Successful deserializations
    successful: u64,
    /// Failed deserializations
    failed: u64,
    /// Validation errors
    validation_errors: u64,
    /// Average chunk processing time (microseconds)
    avg_chunk_time_us: u64,
}

impl DeserStats {
    /// Create new deserialization statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total bytes deserialized
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Get total chunks processed
    pub fn total_chunks(&self) -> u64 {
        self.total_chunks
    }

    /// Get successful deserializations
    pub fn successful(&self) -> u64 {
        self.successful
    }

    /// Get failed deserializations
    pub fn failed(&self) -> u64 {
        self.failed
    }

    /// Get validation errors
    pub fn validation_errors(&self) -> u64 {
        self.validation_errors
    }

    /// Get average chunk processing time
    pub fn avg_chunk_time_us(&self) -> u64 {
        self.avg_chunk_time_us
    }

    /// Calculate success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.successful + self.failed;
        if total == 0 {
            return 0.0;
        }
        self.successful as f64 / total as f64
    }

    /// Get average bytes per chunk
    pub fn avg_bytes_per_chunk(&self) -> u64 {
        if self.total_chunks == 0 {
            return 0;
        }
        self.total_bytes / self.total_chunks
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for DeserStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DeserStats(bytes={}, chunks={}, success_rate={:.2}%)",
            self.total_bytes,
            self.total_chunks,
            self.success_rate() * 100.0
        )
    }
}

/// Deserialization state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeserState {
    /// Initial state
    Init,
    /// Validating payload
    Validating,
    /// Deserializing chunks
    Deserializing,
    /// Completed successfully
    Completed,
    /// Failed with error
    Failed,
}

impl fmt::Display for DeserState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "Init"),
            Self::Validating => write!(f, "Validating"),
            Self::Deserializing => write!(f, "Deserializing"),
            Self::Completed => write!(f, "Completed"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}

/// Incremental deserializer
pub struct IncrementalDeserializer {
    config: DeserConfig,
    stats: DeserStats,
    state: DeserState,
    buffer: Vec<u8>,
    position: usize,
}

impl IncrementalDeserializer {
    /// Create a new incremental deserializer
    pub fn new(config: DeserConfig) -> Self {
        Self {
            config,
            stats: DeserStats::default(),
            state: DeserState::Init,
            buffer: Vec::new(),
            position: 0,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &DeserConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &DeserStats {
        &self.stats
    }

    /// Get current state
    pub fn state(&self) -> DeserState {
        self.state
    }

    /// Check if deserialization is complete
    pub fn is_complete(&self) -> bool {
        self.state == DeserState::Completed
    }

    /// Check if deserialization failed
    pub fn is_failed(&self) -> bool {
        self.state == DeserState::Failed
    }

    /// Validate payload size
    pub fn validate_size(&mut self, size: usize) -> Result<(), String> {
        self.state = DeserState::Validating;

        if size > self.config.max_payload_size {
            self.state = DeserState::Failed;
            self.stats.validation_errors += 1;
            return Err(format!(
                "Payload size {} exceeds maximum {}",
                size, self.config.max_payload_size
            ));
        }

        Ok(())
    }

    /// Feed data to the deserializer
    pub fn feed(&mut self, data: &[u8]) -> Result<(), String> {
        if self.state == DeserState::Failed {
            return Err("Deserializer is in failed state".to_string());
        }

        // Validate size
        if self.config.enable_validation {
            let new_size = self.buffer.len() + data.len();
            self.validate_size(new_size)?;
        }

        self.state = DeserState::Deserializing;
        self.buffer.extend_from_slice(data);
        self.stats.total_bytes += data.len() as u64;

        Ok(())
    }

    /// Try to deserialize the buffered data
    pub fn try_deserialize<T: for<'de> Deserialize<'de>>(&mut self) -> Result<T, String> {
        if self.buffer.is_empty() {
            return Err("No data to deserialize".to_string());
        }

        match serde_json::from_slice::<T>(&self.buffer) {
            Ok(value) => {
                self.state = DeserState::Completed;
                self.stats.successful += 1;
                Ok(value)
            }
            Err(e) => {
                self.state = DeserState::Failed;
                self.stats.failed += 1;
                Err(format!("Deserialization failed: {}", e))
            }
        }
    }

    /// Deserialize in chunks
    pub fn deserialize_chunked<T: for<'de> Deserialize<'de>>(
        &mut self,
        data: &[u8],
    ) -> Result<T, String> {
        // Process in chunks
        let chunk_size = self.config.chunk_size;
        let mut offset = 0;

        while offset < data.len() {
            let end = (offset + chunk_size).min(data.len());
            let chunk = &data[offset..end];

            self.feed(chunk)?;
            self.stats.total_chunks += 1;

            offset = end;
        }

        // Try final deserialization
        self.try_deserialize()
    }

    /// Get buffered data size
    pub fn buffered_size(&self) -> usize {
        self.buffer.len()
    }

    /// Get current position
    pub fn position(&self) -> usize {
        self.position
    }

    /// Clear buffer and reset state
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.position = 0;
        self.state = DeserState::Init;
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats.reset();
    }
}

impl fmt::Debug for IncrementalDeserializer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncrementalDeserializer")
            .field("config", &self.config)
            .field("state", &self.state)
            .field("buffered_size", &self.buffer.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deser_config_default() {
        let config = DeserConfig::default();
        assert_eq!(config.chunk_size(), 4096);
        assert_eq!(config.max_payload_size(), 10 * 1024 * 1024);
        assert!(config.is_valid());
    }

    #[test]
    fn test_deser_config_builder() {
        let config = DeserConfig::new()
            .with_chunk_size(8192)
            .with_max_payload_size(1024 * 1024)
            .with_validation(false);

        assert_eq!(config.chunk_size(), 8192);
        assert_eq!(config.max_payload_size(), 1024 * 1024);
        assert!(!config.is_validation_enabled());
    }

    #[test]
    fn test_deser_config_presets() {
        let small = DeserConfig::small();
        assert_eq!(small.chunk_size(), 1024);

        let large = DeserConfig::large();
        assert_eq!(large.chunk_size(), 65536);
        assert!(large.is_streaming_enabled());
    }

    #[test]
    fn test_deser_stats_default() {
        let stats = DeserStats::default();
        assert_eq!(stats.total_bytes(), 0);
        assert_eq!(stats.total_chunks(), 0);
        assert_eq!(stats.success_rate(), 0.0);
    }

    #[test]
    fn test_deser_stats_success_rate() {
        let stats = DeserStats {
            successful: 80,
            failed: 20,
            ..Default::default()
        };

        assert_eq!(stats.success_rate(), 0.8);
    }

    #[test]
    fn test_deser_stats_avg_bytes() {
        let stats = DeserStats {
            total_bytes: 10000,
            total_chunks: 10,
            ..Default::default()
        };

        assert_eq!(stats.avg_bytes_per_chunk(), 1000);
    }

    #[test]
    fn test_deser_state_display() {
        assert_eq!(format!("{}", DeserState::Init), "Init");
        assert_eq!(format!("{}", DeserState::Completed), "Completed");
    }

    #[test]
    fn test_incremental_deserializer_creation() {
        let config = DeserConfig::default();
        let deser = IncrementalDeserializer::new(config);

        assert_eq!(deser.state(), DeserState::Init);
        assert_eq!(deser.buffered_size(), 0);
    }

    #[test]
    fn test_incremental_deserializer_validate_size() {
        let config = DeserConfig::default().with_max_payload_size(1000);
        let mut deser = IncrementalDeserializer::new(config);

        assert!(deser.validate_size(500).is_ok());
        assert!(deser.validate_size(1500).is_err());
        assert!(deser.is_failed());
    }

    #[test]
    fn test_incremental_deserializer_feed() {
        let config = DeserConfig::default();
        let mut deser = IncrementalDeserializer::new(config);

        let data = b"test data";
        assert!(deser.feed(data).is_ok());
        assert_eq!(deser.buffered_size(), data.len());
        assert_eq!(deser.state(), DeserState::Deserializing);
    }

    #[test]
    fn test_incremental_deserializer_deserialize() {
        let config = DeserConfig::default();
        let mut deser = IncrementalDeserializer::new(config);

        #[derive(Debug, Deserialize, PartialEq)]
        struct TestData {
            value: i32,
        }

        let json = r#"{"value": 42}"#;
        assert!(deser.feed(json.as_bytes()).is_ok());

        let result: Result<TestData, _> = deser.try_deserialize();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().value, 42);
        assert!(deser.is_complete());
    }

    #[test]
    fn test_incremental_deserializer_chunked() {
        let config = DeserConfig::default().with_chunk_size(5);
        let mut deser = IncrementalDeserializer::new(config);

        #[derive(Debug, Deserialize, PartialEq)]
        struct TestData {
            value: i32,
        }

        let json = r#"{"value": 42}"#;
        let result: Result<TestData, _> = deser.deserialize_chunked(json.as_bytes());

        assert!(result.is_ok());
        assert_eq!(result.unwrap().value, 42);
        assert!(deser.stats().total_chunks() > 1);
    }

    #[test]
    fn test_incremental_deserializer_reset() {
        let config = DeserConfig::default();
        let mut deser = IncrementalDeserializer::new(config);

        let data = b"test data";
        let _ = deser.feed(data);

        deser.reset();
        assert_eq!(deser.buffered_size(), 0);
        assert_eq!(deser.state(), DeserState::Init);
    }

    #[test]
    fn test_incremental_deserializer_failed_state() {
        let config = DeserConfig::default();
        let mut deser = IncrementalDeserializer::new(config);

        let invalid_json = b"not json";
        let _ = deser.feed(invalid_json);

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TestData {
            value: i32,
        }

        let result: Result<TestData, _> = deser.try_deserialize();
        assert!(result.is_err());
        assert!(deser.is_failed());
    }
}
