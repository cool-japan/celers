//! Crash dump generation for debugging worker failures
//!
//! This module provides mechanisms to capture and save crash dumps when
//! worker failures occur, enabling post-mortem debugging and analysis.
//!
//! # Features
//!
//! - Automatic crash dump generation on failures
//! - Configurable dump content (stack traces, task state, system info)
//! - Multiple output formats (JSON, plain text)
//! - Dump size limits and rotation
//! - Filtering by error severity
//!
//! # Example
//!
//! ```
//! use celers_worker::{CrashDumpManager, CrashDumpConfig, CrashDump};
//!
//! # async fn example() {
//! let config = CrashDumpConfig::new()
//!     .with_max_dumps(10)
//!     .with_auto_rotate(true);
//!
//! let mut manager = CrashDumpManager::new(config);
//!
//! // Record a crash
//! let dump = CrashDump::new("task-123".to_string(), "RuntimeError".to_string())
//!     .with_message("Task execution failed")
//!     .with_stack_trace(vec!["frame1".to_string(), "frame2".to_string()]);
//!
//! manager.record_crash(dump).await;
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::info;

/// Crash dump severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CrashSeverity {
    /// Low severity - recoverable errors
    Low,
    /// Medium severity - non-critical failures
    Medium,
    /// High severity - critical failures
    High,
    /// Fatal severity - unrecoverable failures
    Fatal,
}

impl CrashSeverity {
    /// Check if severity is critical or fatal
    pub fn is_critical(&self) -> bool {
        matches!(self, CrashSeverity::High | CrashSeverity::Fatal)
    }
}

/// Crash dump data
#[derive(Clone, Debug)]
pub struct CrashDump {
    /// Task ID that crashed
    pub task_id: String,
    /// Error type
    pub error_type: String,
    /// Error message
    pub message: String,
    /// When the crash occurred
    pub timestamp: SystemTime,
    /// Stack trace
    pub stack_trace: Vec<String>,
    /// Task state at crash time
    pub task_state: HashMap<String, String>,
    /// System information
    pub system_info: HashMap<String, String>,
    /// Crash severity
    pub severity: CrashSeverity,
    /// Worker ID
    pub worker_id: Option<String>,
}

impl CrashDump {
    /// Create a new crash dump
    pub fn new(task_id: String, error_type: String) -> Self {
        Self {
            task_id,
            error_type,
            message: String::new(),
            timestamp: SystemTime::now(),
            stack_trace: Vec::new(),
            task_state: HashMap::new(),
            system_info: HashMap::new(),
            severity: CrashSeverity::Medium,
            worker_id: None,
        }
    }

    /// Set error message
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    /// Set stack trace
    pub fn with_stack_trace(mut self, stack_trace: Vec<String>) -> Self {
        self.stack_trace = stack_trace;
        self
    }

    /// Add task state
    pub fn with_task_state(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.task_state.insert(key.into(), value.into());
        self
    }

    /// Add system info
    pub fn with_system_info(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.system_info.insert(key.into(), value.into());
        self
    }

    /// Set severity
    pub fn with_severity(mut self, severity: CrashSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Set worker ID
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    /// Generate a summary string
    pub fn summary(&self) -> String {
        format!(
            "Crash: {} - {} (severity: {:?}, task: {})",
            self.error_type, self.message, self.severity, self.task_id
        )
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> String {
        // Simple JSON serialization
        let mut json = String::from("{");
        json.push_str(&format!("\"task_id\":\"{}\",", self.task_id));
        json.push_str(&format!("\"error_type\":\"{}\",", self.error_type));
        json.push_str(&format!("\"message\":\"{}\",", self.message));
        json.push_str(&format!("\"severity\":\"{:?}\",", self.severity));

        if let Some(worker_id) = &self.worker_id {
            json.push_str(&format!("\"worker_id\":\"{}\",", worker_id));
        }

        json.push_str(&format!(
            "\"stack_trace_frames\":{}",
            self.stack_trace.len()
        ));
        json.push('}');
        json
    }
}

/// Crash dump configuration
#[derive(Clone)]
pub struct CrashDumpConfig {
    /// Enable crash dump generation
    pub enabled: bool,
    /// Maximum number of dumps to keep
    pub max_dumps: usize,
    /// Auto-rotate old dumps
    pub auto_rotate: bool,
    /// Minimum severity to record
    pub min_severity: CrashSeverity,
    /// Include stack traces
    pub include_stack_trace: bool,
    /// Include task state
    pub include_task_state: bool,
    /// Include system info
    pub include_system_info: bool,
}

impl CrashDumpConfig {
    /// Create a new crash dump configuration
    pub fn new() -> Self {
        Self {
            enabled: true,
            max_dumps: 100,
            auto_rotate: true,
            min_severity: CrashSeverity::Low,
            include_stack_trace: true,
            include_task_state: true,
            include_system_info: true,
        }
    }

    /// Enable or disable crash dumps
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set maximum number of dumps
    pub fn with_max_dumps(mut self, max_dumps: usize) -> Self {
        self.max_dumps = max_dumps;
        self
    }

    /// Enable or disable auto-rotation
    pub fn with_auto_rotate(mut self, auto_rotate: bool) -> Self {
        self.auto_rotate = auto_rotate;
        self
    }

    /// Set minimum severity level
    pub fn with_min_severity(mut self, severity: CrashSeverity) -> Self {
        self.min_severity = severity;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_dumps == 0 {
            return Err("Maximum dumps must be greater than 0".to_string());
        }
        Ok(())
    }
}

impl Default for CrashDumpConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Crash dump statistics
#[derive(Clone, Debug, Default)]
pub struct CrashDumpStats {
    /// Total crashes recorded
    pub total_crashes: usize,
    /// Crashes by severity
    pub by_severity: HashMap<String, usize>,
    /// Crashes by error type
    pub by_error_type: HashMap<String, usize>,
    /// Current number of dumps
    pub current_dumps: usize,
    /// Rotated dumps count
    pub rotated_dumps: usize,
}

/// Crash dump manager
pub struct CrashDumpManager {
    /// Configuration
    config: CrashDumpConfig,
    /// Stored crash dumps
    dumps: Arc<RwLock<Vec<CrashDump>>>,
    /// Statistics
    stats: Arc<RwLock<CrashDumpStats>>,
}

impl CrashDumpManager {
    /// Create a new crash dump manager
    pub fn new(config: CrashDumpConfig) -> Self {
        Self {
            config,
            dumps: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(CrashDumpStats::default())),
        }
    }

    /// Record a crash dump
    pub async fn record_crash(&self, dump: CrashDump) {
        if !self.config.enabled {
            return;
        }

        // Check severity filter
        if dump.severity < self.config.min_severity {
            return;
        }

        let mut dumps = self.dumps.write().await;
        let mut stats = self.stats.write().await;

        // Add new dump
        dumps.push(dump.clone());

        // Update statistics
        stats.total_crashes += 1;
        *stats
            .by_severity
            .entry(format!("{:?}", dump.severity))
            .or_insert(0) += 1;
        *stats
            .by_error_type
            .entry(dump.error_type.clone())
            .or_insert(0) += 1;

        // Auto-rotate if needed
        if self.config.auto_rotate && dumps.len() > self.config.max_dumps {
            let to_remove = dumps.len() - self.config.max_dumps;
            dumps.drain(0..to_remove);
            stats.rotated_dumps += to_remove;
        }

        stats.current_dumps = dumps.len();

        info!("Recorded crash dump: {}", dump.summary());
    }

    /// Get all crash dumps
    pub async fn get_dumps(&self) -> Vec<CrashDump> {
        self.dumps.read().await.clone()
    }

    /// Get dumps by severity
    pub async fn get_dumps_by_severity(&self, severity: CrashSeverity) -> Vec<CrashDump> {
        let dumps = self.dumps.read().await;
        dumps
            .iter()
            .filter(|d| d.severity == severity)
            .cloned()
            .collect()
    }

    /// Get dumps by error type
    pub async fn get_dumps_by_error_type(&self, error_type: &str) -> Vec<CrashDump> {
        let dumps = self.dumps.read().await;
        dumps
            .iter()
            .filter(|d| d.error_type == error_type)
            .cloned()
            .collect()
    }

    /// Get crash count
    pub async fn crash_count(&self) -> usize {
        self.dumps.read().await.len()
    }

    /// Get critical crash count
    pub async fn critical_crash_count(&self) -> usize {
        let dumps = self.dumps.read().await;
        dumps.iter().filter(|d| d.severity.is_critical()).count()
    }

    /// Clear all dumps
    pub async fn clear_all(&self) {
        let mut dumps = self.dumps.write().await;
        dumps.clear();

        let mut stats = self.stats.write().await;
        stats.current_dumps = 0;

        info!("Cleared all crash dumps");
    }

    /// Get crash statistics
    pub async fn get_stats(&self) -> CrashDumpStats {
        self.stats.read().await.clone()
    }

    /// Get recent crashes (last N)
    pub async fn get_recent_crashes(&self, count: usize) -> Vec<CrashDump> {
        let dumps = self.dumps.read().await;
        let start = dumps.len().saturating_sub(count);
        dumps[start..].to_vec()
    }

    /// Export dumps to JSON
    pub async fn export_json(&self) -> String {
        let dumps = self.dumps.read().await;
        let mut json = String::from("[");

        for (i, dump) in dumps.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&dump.to_json());
        }

        json.push(']');
        json
    }

    /// Get unique error types
    pub async fn get_error_types(&self) -> Vec<String> {
        let stats = self.stats.read().await;
        stats.by_error_type.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crash_dump_creation() {
        let dump = CrashDump::new("task-1".to_string(), "RuntimeError".to_string());
        assert_eq!(dump.task_id, "task-1");
        assert_eq!(dump.error_type, "RuntimeError");
        assert_eq!(dump.severity, CrashSeverity::Medium);
    }

    #[test]
    fn test_crash_dump_builder() {
        let dump = CrashDump::new("task-1".to_string(), "Error".to_string())
            .with_message("Test error")
            .with_severity(CrashSeverity::High)
            .with_worker_id("worker-1")
            .with_task_state("retry_count", "3")
            .with_system_info("cpu", "50%");

        assert_eq!(dump.message, "Test error");
        assert_eq!(dump.severity, CrashSeverity::High);
        assert_eq!(dump.worker_id, Some("worker-1".to_string()));
        assert!(dump.task_state.contains_key("retry_count"));
        assert!(dump.system_info.contains_key("cpu"));
    }

    #[test]
    fn test_crash_severity() {
        assert!(!CrashSeverity::Low.is_critical());
        assert!(!CrashSeverity::Medium.is_critical());
        assert!(CrashSeverity::High.is_critical());
        assert!(CrashSeverity::Fatal.is_critical());
    }

    #[test]
    fn test_crash_dump_summary() {
        let dump = CrashDump::new("task-1".to_string(), "Error".to_string()).with_message("Test");

        let summary = dump.summary();
        assert!(summary.contains("Error"));
        assert!(summary.contains("Test"));
        assert!(summary.contains("task-1"));
    }

    #[test]
    fn test_crash_dump_config_default() {
        let config = CrashDumpConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_dumps, 100);
        assert!(config.auto_rotate);
    }

    #[test]
    fn test_crash_dump_config_builder() {
        let config = CrashDumpConfig::new()
            .enabled(false)
            .with_max_dumps(50)
            .with_auto_rotate(false)
            .with_min_severity(CrashSeverity::High);

        assert!(!config.enabled);
        assert_eq!(config.max_dumps, 50);
        assert!(!config.auto_rotate);
        assert_eq!(config.min_severity, CrashSeverity::High);
    }

    #[test]
    fn test_crash_dump_config_validation() {
        let invalid_config = CrashDumpConfig::new().with_max_dumps(0);
        assert!(invalid_config.validate().is_err());

        let valid_config = CrashDumpConfig::new();
        assert!(valid_config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_crash_dump_manager() {
        let config = CrashDumpConfig::default();
        let manager = CrashDumpManager::new(config);

        let dump = CrashDump::new("task-1".to_string(), "Error".to_string());
        manager.record_crash(dump).await;

        assert_eq!(manager.crash_count().await, 1);
    }

    #[tokio::test]
    async fn test_crash_dump_manager_filtering() {
        let config = CrashDumpConfig::new().with_min_severity(CrashSeverity::High);
        let manager = CrashDumpManager::new(config);

        // This should not be recorded (severity too low)
        let dump1 = CrashDump::new("task-1".to_string(), "Error".to_string())
            .with_severity(CrashSeverity::Low);
        manager.record_crash(dump1).await;

        // This should be recorded
        let dump2 = CrashDump::new("task-2".to_string(), "Error".to_string())
            .with_severity(CrashSeverity::High);
        manager.record_crash(dump2).await;

        assert_eq!(manager.crash_count().await, 1);
    }

    #[tokio::test]
    async fn test_crash_dump_auto_rotate() {
        let config = CrashDumpConfig::new()
            .with_max_dumps(3)
            .with_auto_rotate(true);
        let manager = CrashDumpManager::new(config);

        // Add 5 dumps
        for i in 0..5 {
            let dump = CrashDump::new(format!("task-{}", i), "Error".to_string());
            manager.record_crash(dump).await;
        }

        // Should only keep last 3
        assert_eq!(manager.crash_count().await, 3);
    }

    #[tokio::test]
    async fn test_crash_dump_by_severity() {
        let config = CrashDumpConfig::default();
        let manager = CrashDumpManager::new(config);

        manager
            .record_crash(
                CrashDump::new("task-1".to_string(), "Error".to_string())
                    .with_severity(CrashSeverity::High),
            )
            .await;

        manager
            .record_crash(
                CrashDump::new("task-2".to_string(), "Error".to_string())
                    .with_severity(CrashSeverity::Low),
            )
            .await;

        let high_severity = manager.get_dumps_by_severity(CrashSeverity::High).await;
        assert_eq!(high_severity.len(), 1);
    }

    #[tokio::test]
    async fn test_crash_dump_stats() {
        let config = CrashDumpConfig::default();
        let manager = CrashDumpManager::new(config);

        manager
            .record_crash(CrashDump::new(
                "task-1".to_string(),
                "RuntimeError".to_string(),
            ))
            .await;

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_crashes, 1);
        assert_eq!(stats.current_dumps, 1);
    }

    #[tokio::test]
    async fn test_crash_dump_clear() {
        let config = CrashDumpConfig::default();
        let manager = CrashDumpManager::new(config);

        manager
            .record_crash(CrashDump::new("task-1".to_string(), "Error".to_string()))
            .await;

        assert_eq!(manager.crash_count().await, 1);

        manager.clear_all().await;
        assert_eq!(manager.crash_count().await, 0);
    }

    #[tokio::test]
    async fn test_recent_crashes() {
        let config = CrashDumpConfig::default();
        let manager = CrashDumpManager::new(config);

        for i in 0..5 {
            manager
                .record_crash(CrashDump::new(format!("task-{}", i), "Error".to_string()))
                .await;
        }

        let recent = manager.get_recent_crashes(2).await;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].task_id, "task-3");
        assert_eq!(recent[1].task_id, "task-4");
    }

    #[tokio::test]
    async fn test_critical_crash_count() {
        let config = CrashDumpConfig::default();
        let manager = CrashDumpManager::new(config);

        manager
            .record_crash(
                CrashDump::new("task-1".to_string(), "Error".to_string())
                    .with_severity(CrashSeverity::High),
            )
            .await;

        manager
            .record_crash(
                CrashDump::new("task-2".to_string(), "Error".to_string())
                    .with_severity(CrashSeverity::Low),
            )
            .await;

        assert_eq!(manager.critical_crash_count().await, 1);
    }
}
