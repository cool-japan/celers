//! Structured Logging Enhancements
//!
//! Provides advanced structured logging capabilities:
//! - Contextual logging with task IDs and correlation
//! - Log correlation across distributed systems
//! - Performance impact analysis from logs
//! - Structured log formatting (JSON, key-value)
//! - Log sampling and filtering
//!
//! # Example
//!
//! ```rust
//! use celers_broker_redis::structured_logging::{LogContext, StructuredLogger};
//!
//! # fn example() {
//! let mut logger = StructuredLogger::new("my_service");
//!
//! // Create a context for a task
//! let mut context = LogContext::new("task_123")
//!     .with_correlation_id("req_456")
//!     .with_metadata("user_id", "user_789");
//!
//! // Log with context
//! logger.info(&context, "Processing task", &[("retry", "1")]);
//! logger.warn(&context, "Slow operation detected", &[("duration_ms", "5000")]);
//! logger.error(&context, "Task failed", &[("error", "TimeoutError")]);
//!
//! // Analyze performance from logs
//! let analysis = logger.analyze_performance();
//! println!("Slow operations: {}", analysis.slow_operation_count);
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Structured logger with context support
#[derive(Debug, Clone)]
pub struct StructuredLogger {
    service_name: String,
    logs: Arc<RwLock<Vec<LogEntry>>>,
    config: LogConfig,
}

/// Log entry with structured data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp (Unix milliseconds)
    pub timestamp: i64,
    /// Log level
    pub level: StructuredLogLevel,
    /// Service name
    pub service: String,
    /// Task ID (if applicable)
    pub task_id: Option<String>,
    /// Correlation ID for distributed tracing
    pub correlation_id: Option<String>,
    /// Log message
    pub message: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Duration in milliseconds (if applicable)
    pub duration_ms: Option<u64>,
}

/// Log context for correlation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogContext {
    /// Task ID
    pub task_id: String,
    /// Correlation ID for distributed tracing
    pub correlation_id: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Start time for duration tracking
    #[serde(skip)]
    pub start_time: Option<Instant>,
}

/// Structured log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StructuredLogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warning level
    Warn,
    /// Error level
    Error,
}

/// Log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Minimum log level to record
    pub min_level: StructuredLogLevel,
    /// Enable JSON formatting
    pub json_format: bool,
    /// Maximum log entries to keep in memory
    pub max_entries: usize,
    /// Sample rate (0.0-1.0, where 1.0 = all logs)
    pub sample_rate: f64,
    /// Enable performance analysis
    pub enable_performance_analysis: bool,
}

/// Performance analysis from logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAnalysis {
    /// Total log entries analyzed
    pub total_entries: usize,
    /// Number of error logs
    pub error_count: usize,
    /// Number of warning logs
    pub warning_count: usize,
    /// Slow operations (> 1 second)
    pub slow_operation_count: usize,
    /// Average operation duration
    pub avg_duration_ms: f64,
    /// P95 duration
    pub p95_duration_ms: f64,
    /// P99 duration
    pub p99_duration_ms: f64,
    /// Most common errors
    pub top_errors: Vec<(String, usize)>,
    /// Task IDs with most errors
    pub problematic_tasks: Vec<(String, usize)>,
}

/// Log correlation analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationAnalysis {
    /// Correlation ID
    pub correlation_id: String,
    /// Number of log entries
    pub entry_count: usize,
    /// Involved task IDs
    pub task_ids: Vec<String>,
    /// Total duration
    pub total_duration_ms: u64,
    /// Error count
    pub error_count: usize,
    /// Timeline of events
    pub timeline: Vec<(i64, String)>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            min_level: StructuredLogLevel::Info,
            json_format: false,
            max_entries: 10000,
            sample_rate: 1.0,
            enable_performance_analysis: true,
        }
    }
}

impl LogContext {
    /// Create a new log context
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
            correlation_id: None,
            metadata: HashMap::new(),
            start_time: None,
        }
    }

    /// Set correlation ID
    pub fn with_correlation_id(mut self, correlation_id: &str) -> Self {
        self.correlation_id = Some(correlation_id.to_string());
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Start duration tracking
    pub fn start_timer(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Get elapsed duration
    pub fn elapsed_ms(&self) -> Option<u64> {
        self.start_time
            .map(|start| start.elapsed().as_millis() as u64)
    }
}

impl StructuredLogger {
    /// Create a new structured logger
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            logs: Arc::new(RwLock::new(Vec::new())),
            config: LogConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(service_name: &str, config: LogConfig) -> Self {
        Self {
            service_name: service_name.to_string(),
            logs: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Log a trace message
    pub fn trace(&self, context: &LogContext, message: &str, metadata: &[(&str, &str)]) {
        self.log(StructuredLogLevel::Trace, context, message, metadata);
    }

    /// Log a debug message
    pub fn debug_log(&self, context: &LogContext, message: &str, metadata: &[(&str, &str)]) {
        self.log(StructuredLogLevel::Debug, context, message, metadata);
    }

    /// Log an info message
    pub fn info(&self, context: &LogContext, message: &str, metadata: &[(&str, &str)]) {
        self.log(StructuredLogLevel::Info, context, message, metadata);
    }

    /// Log a warning message
    pub fn warn(&self, context: &LogContext, message: &str, metadata: &[(&str, &str)]) {
        self.log(StructuredLogLevel::Warn, context, message, metadata);
    }

    /// Log an error message
    pub fn error(&self, context: &LogContext, message: &str, metadata: &[(&str, &str)]) {
        self.log(StructuredLogLevel::Error, context, message, metadata);
    }

    /// Internal log method
    fn log(
        &self,
        level: StructuredLogLevel,
        context: &LogContext,
        message: &str,
        metadata: &[(&str, &str)],
    ) {
        if level < self.config.min_level {
            return;
        }

        // Sample logs if configured
        if self.config.sample_rate < 1.0 {
            if rand::random::<f64>() > self.config.sample_rate {
                return;
            }
        }

        let mut entry_metadata = context.metadata.clone();
        for (k, v) in metadata {
            entry_metadata.insert(k.to_string(), v.to_string());
        }

        let entry = LogEntry {
            timestamp: chrono::Utc::now().timestamp_millis(),
            level,
            service: self.service_name.clone(),
            task_id: Some(context.task_id.clone()),
            correlation_id: context.correlation_id.clone(),
            message: message.to_string(),
            metadata: entry_metadata,
            duration_ms: context.elapsed_ms(),
        };

        // Store entry
        if let Ok(mut logs) = self.logs.write() {
            logs.push(entry.clone());

            // Limit memory usage
            let len = logs.len();
            if len > self.config.max_entries {
                logs.drain(0..len - self.config.max_entries);
            }
        }

        // Emit to tracing backend
        match level {
            StructuredLogLevel::Trace => debug!(
                task_id = %context.task_id,
                correlation_id = ?context.correlation_id,
                "{}", message
            ),
            StructuredLogLevel::Debug => debug!(
                task_id = %context.task_id,
                correlation_id = ?context.correlation_id,
                "{}", message
            ),
            StructuredLogLevel::Info => info!(
                task_id = %context.task_id,
                correlation_id = ?context.correlation_id,
                "{}", message
            ),
            StructuredLogLevel::Warn => warn!(
                task_id = %context.task_id,
                correlation_id = ?context.correlation_id,
                "{}", message
            ),
            StructuredLogLevel::Error => error!(
                task_id = %context.task_id,
                correlation_id = ?context.correlation_id,
                "{}", message
            ),
        }
    }

    /// Analyze performance from logs
    pub fn analyze_performance(&self) -> PerformanceAnalysis {
        let logs = self.logs.read().unwrap();
        let total_entries = logs.len();

        let mut error_count = 0;
        let mut warning_count = 0;
        let mut slow_operation_count = 0;
        let mut durations = Vec::new();
        let mut error_messages: HashMap<String, usize> = HashMap::new();
        let mut task_errors: HashMap<String, usize> = HashMap::new();

        for entry in logs.iter() {
            if entry.level == StructuredLogLevel::Error {
                error_count += 1;
                *error_messages.entry(entry.message.clone()).or_insert(0) += 1;
                if let Some(task_id) = &entry.task_id {
                    *task_errors.entry(task_id.clone()).or_insert(0) += 1;
                }
            }

            if entry.level == StructuredLogLevel::Warn {
                warning_count += 1;
            }

            if let Some(duration) = entry.duration_ms {
                durations.push(duration);
                if duration > 1000 {
                    slow_operation_count += 1;
                }
            }
        }

        // Calculate percentiles
        durations.sort();
        let p95_duration_ms = if !durations.is_empty() {
            durations[(durations.len() * 95 / 100).min(durations.len() - 1)] as f64
        } else {
            0.0
        };

        let p99_duration_ms = if !durations.is_empty() {
            durations[(durations.len() * 99 / 100).min(durations.len() - 1)] as f64
        } else {
            0.0
        };

        let avg_duration_ms = if !durations.is_empty() {
            durations.iter().sum::<u64>() as f64 / durations.len() as f64
        } else {
            0.0
        };

        // Top errors
        let mut top_errors: Vec<_> = error_messages.into_iter().collect();
        top_errors.sort_by(|a, b| b.1.cmp(&a.1));
        top_errors.truncate(10);

        // Problematic tasks
        let mut problematic_tasks: Vec<_> = task_errors.into_iter().collect();
        problematic_tasks.sort_by(|a, b| b.1.cmp(&a.1));
        problematic_tasks.truncate(10);

        PerformanceAnalysis {
            total_entries,
            error_count,
            warning_count,
            slow_operation_count,
            avg_duration_ms,
            p95_duration_ms,
            p99_duration_ms,
            top_errors,
            problematic_tasks,
        }
    }

    /// Analyze logs by correlation ID
    pub fn analyze_correlation(&self, correlation_id: &str) -> Option<CorrelationAnalysis> {
        let logs = self.logs.read().unwrap();
        let mut entries: Vec<_> = logs
            .iter()
            .filter(|e| e.correlation_id.as_ref() == Some(&correlation_id.to_string()))
            .collect();

        if entries.is_empty() {
            return None;
        }

        entries.sort_by_key(|e| e.timestamp);

        let entry_count = entries.len();
        let mut task_ids: Vec<String> = entries.iter().filter_map(|e| e.task_id.clone()).collect();
        task_ids.dedup();

        let total_duration_ms = entries.iter().filter_map(|e| e.duration_ms).sum();

        let error_count = entries
            .iter()
            .filter(|e| e.level == StructuredLogLevel::Error)
            .count();

        let timeline: Vec<(i64, String)> = entries
            .iter()
            .map(|e| (e.timestamp, e.message.clone()))
            .collect();

        Some(CorrelationAnalysis {
            correlation_id: correlation_id.to_string(),
            entry_count,
            task_ids,
            total_duration_ms,
            error_count,
            timeline,
        })
    }

    /// Get all log entries
    pub fn get_logs(&self) -> Vec<LogEntry> {
        self.logs.read().unwrap().clone()
    }

    /// Clear all log entries
    pub fn clear(&self) {
        if let Ok(mut logs) = self.logs.write() {
            logs.clear();
        }
    }

    /// Export logs as JSON
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        let logs = self.logs.read().unwrap();
        serde_json::to_string_pretty(&*logs)
    }

    /// Get log count by level
    pub fn count_by_level(&self, level: StructuredLogLevel) -> usize {
        self.logs
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.level == level)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_log_context_creation() {
        let context = LogContext::new("task_123")
            .with_correlation_id("req_456")
            .with_metadata("user_id", "user_789");

        assert_eq!(context.task_id, "task_123");
        assert_eq!(context.correlation_id, Some("req_456".to_string()));
        assert_eq!(
            context.metadata.get("user_id"),
            Some(&"user_789".to_string())
        );
    }

    #[test]
    fn test_log_context_timer() {
        let mut context = LogContext::new("task_123");
        context.start_timer();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = context.elapsed_ms();
        assert!(elapsed.is_some());
        assert!(elapsed.unwrap() >= 10);
    }

    #[test]
    fn test_structured_logger() {
        let logger = StructuredLogger::new("test_service");
        let context = LogContext::new("task_123");

        logger.info(&context, "Test message", &[("key", "value")]);
        logger.warn(&context, "Warning message", &[]);
        logger.error(&context, "Error message", &[("error", "TestError")]);

        assert_eq!(logger.get_logs().len(), 3);
        assert_eq!(logger.count_by_level(StructuredLogLevel::Info), 1);
        assert_eq!(logger.count_by_level(StructuredLogLevel::Warn), 1);
        assert_eq!(logger.count_by_level(StructuredLogLevel::Error), 1);
    }

    #[test]
    fn test_performance_analysis() {
        let logger = StructuredLogger::new("test_service");
        let mut context = LogContext::new("task_123");

        context.start_timer();
        logger.info(&context, "Operation", &[]);

        let mut slow_context = LogContext::new("task_456");
        slow_context.start_timer();
        std::thread::sleep(Duration::from_millis(10));
        logger.warn(&slow_context, "Slow operation", &[]);
        logger.error(&slow_context, "Error occurred", &[]);

        let analysis = logger.analyze_performance();
        assert!(analysis.total_entries > 0);
        assert_eq!(analysis.error_count, 1);
        assert_eq!(analysis.warning_count, 1);
    }

    #[test]
    fn test_correlation_analysis() {
        let logger = StructuredLogger::new("test_service");
        let context1 = LogContext::new("task_123").with_correlation_id("corr_1");
        let context2 = LogContext::new("task_456").with_correlation_id("corr_1");

        logger.info(&context1, "Step 1", &[]);
        logger.info(&context2, "Step 2", &[]);

        let analysis = logger.analyze_correlation("corr_1");
        assert!(analysis.is_some());

        let analysis = analysis.unwrap();
        assert_eq!(analysis.entry_count, 2);
        assert_eq!(analysis.task_ids.len(), 2);
    }

    #[test]
    fn test_log_export_json() {
        let logger = StructuredLogger::new("test_service");
        let context = LogContext::new("task_123");

        logger.info(&context, "Test message", &[]);

        let json = logger.export_json();
        assert!(json.is_ok());
        assert!(json.unwrap().contains("Test message"));
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(StructuredLogLevel::Trace < StructuredLogLevel::Debug);
        assert!(StructuredLogLevel::Debug < StructuredLogLevel::Info);
        assert!(StructuredLogLevel::Info < StructuredLogLevel::Warn);
        assert!(StructuredLogLevel::Warn < StructuredLogLevel::Error);
    }
}
