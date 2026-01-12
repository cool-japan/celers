//! Telemetry and observability hooks for Redis backend
//!
//! This module provides hooks and callbacks for integrating with external
//! observability systems like OpenTelemetry, Prometheus, or custom monitoring.

use crate::BackendError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Operation type for telemetry tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Store result operation
    Store,
    /// Get result operation
    Get,
    /// Delete result operation
    Delete,
    /// Batch store operation
    BatchStore,
    /// Batch get operation
    BatchGet,
    /// Batch delete operation
    BatchDelete,
    /// Chord increment operation
    ChordIncrement,
    /// Health check operation
    HealthCheck,
    /// Query operation
    Query,
    /// Transaction operation
    Transaction,
}

impl OperationType {
    /// Get the operation name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::Store => "store",
            OperationType::Get => "get",
            OperationType::Delete => "delete",
            OperationType::BatchStore => "batch_store",
            OperationType::BatchGet => "batch_get",
            OperationType::BatchDelete => "batch_delete",
            OperationType::ChordIncrement => "chord_increment",
            OperationType::HealthCheck => "health_check",
            OperationType::Query => "query",
            OperationType::Transaction => "transaction",
        }
    }
}

/// Operation context passed to telemetry hooks
#[derive(Debug, Clone)]
pub struct OperationContext {
    /// Type of operation
    pub operation: OperationType,
    /// Task ID (if applicable)
    pub task_id: Option<Uuid>,
    /// Number of items in batch operation (if applicable)
    pub batch_size: Option<usize>,
    /// Operation start time
    pub start_time: Instant,
    /// Custom metadata
    pub metadata: Vec<(String, String)>,
}

impl OperationContext {
    /// Create a new operation context
    pub fn new(operation: OperationType) -> Self {
        Self {
            operation,
            task_id: None,
            batch_size: None,
            start_time: Instant::now(),
            metadata: Vec::new(),
        }
    }

    /// Set the task ID
    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Add custom metadata
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.push((key, value));
        self
    }

    /// Get the elapsed time since operation start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Operation result for telemetry
#[derive(Debug, Clone)]
pub struct OperationResult {
    /// Operation context
    pub context: OperationContext,
    /// Operation duration
    pub duration: Duration,
    /// Whether the operation succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Data size in bytes (if applicable)
    pub data_size: Option<usize>,
}

impl OperationResult {
    /// Create a successful operation result
    pub fn success(context: OperationContext) -> Self {
        let duration = context.elapsed();
        Self {
            context,
            duration,
            success: true,
            error: None,
            data_size: None,
        }
    }

    /// Create a failed operation result
    pub fn failure(context: OperationContext, error: &BackendError) -> Self {
        let duration = context.elapsed();
        Self {
            context,
            duration,
            success: false,
            error: Some(error.to_string()),
            data_size: None,
        }
    }

    /// Set the data size
    pub fn with_data_size(mut self, size: usize) -> Self {
        self.data_size = Some(size);
        self
    }
}

/// Telemetry hook trait for custom observability integration
pub trait TelemetryHook: Send + Sync {
    /// Called before an operation starts
    fn before_operation(&self, _context: &OperationContext) {}

    /// Called after an operation completes
    fn after_operation(&self, _result: &OperationResult) {}

    /// Called when an error occurs
    fn on_error(&self, _operation: OperationType, _error: &BackendError) {}

    /// Called for custom events
    fn on_event(&self, _event_name: &str, _data: &[(String, String)]) {}
}

/// No-op telemetry hook (default)
#[derive(Debug, Clone, Default)]
pub struct NoOpHook;

impl TelemetryHook for NoOpHook {}

/// Logging telemetry hook that outputs to tracing
#[derive(Debug, Clone, Default)]
pub struct LoggingHook {
    /// Whether to log successful operations
    pub log_success: bool,
    /// Minimum duration to log (filters out fast operations)
    pub min_duration_ms: Option<f64>,
}

impl LoggingHook {
    /// Create a new logging hook
    pub fn new() -> Self {
        Self {
            log_success: true,
            min_duration_ms: None,
        }
    }

    /// Only log operations that take longer than the specified duration
    pub fn with_min_duration(mut self, min_ms: f64) -> Self {
        self.min_duration_ms = Some(min_ms);
        self
    }

    /// Only log errors, not successful operations
    pub fn errors_only(mut self) -> Self {
        self.log_success = false;
        self
    }
}

impl TelemetryHook for LoggingHook {
    fn before_operation(&self, context: &OperationContext) {
        if let Some(task_id) = context.task_id {
            tracing::debug!(
                operation = context.operation.as_str(),
                task_id = %task_id,
                "Starting operation"
            );
        } else if let Some(batch_size) = context.batch_size {
            tracing::debug!(
                operation = context.operation.as_str(),
                batch_size = batch_size,
                "Starting batch operation"
            );
        } else {
            tracing::debug!(operation = context.operation.as_str(), "Starting operation");
        }
    }

    fn after_operation(&self, result: &OperationResult) {
        let duration_ms = result.duration.as_secs_f64() * 1000.0;

        // Filter by minimum duration
        if let Some(min_ms) = self.min_duration_ms {
            if duration_ms < min_ms {
                return;
            }
        }

        if result.success && self.log_success {
            tracing::info!(
                operation = result.context.operation.as_str(),
                duration_ms = %format!("{:.2}", duration_ms),
                success = true,
                "Operation completed"
            );
        } else if !result.success {
            tracing::error!(
                operation = result.context.operation.as_str(),
                duration_ms = %format!("{:.2}", duration_ms),
                error = result.error.as_deref().unwrap_or("unknown"),
                "Operation failed"
            );
        }
    }

    fn on_error(&self, operation: OperationType, error: &BackendError) {
        tracing::error!(
            operation = operation.as_str(),
            error = %error,
            "Backend error occurred"
        );
    }

    fn on_event(&self, event_name: &str, data: &[(String, String)]) {
        tracing::info!(
            event = event_name,
            data = ?data,
            "Custom event"
        );
    }
}

/// Metrics telemetry hook that tracks operation statistics
#[derive(Debug, Default)]
pub struct MetricsHook {
    /// Total operation count by type
    operation_counts: std::sync::Mutex<std::collections::HashMap<&'static str, u64>>,
    /// Total error count by type
    error_counts: std::sync::Mutex<std::collections::HashMap<&'static str, u64>>,
    /// Total duration by operation type
    duration_totals: std::sync::Mutex<std::collections::HashMap<&'static str, Duration>>,
}

impl MetricsHook {
    /// Create a new metrics hook
    pub fn new() -> Self {
        Self::default()
    }

    /// Get operation count for a specific operation type
    pub fn operation_count(&self, operation: OperationType) -> u64 {
        self.operation_counts
            .lock()
            .unwrap()
            .get(operation.as_str())
            .copied()
            .unwrap_or(0)
    }

    /// Get error count for a specific operation type
    pub fn error_count(&self, operation: OperationType) -> u64 {
        self.error_counts
            .lock()
            .unwrap()
            .get(operation.as_str())
            .copied()
            .unwrap_or(0)
    }

    /// Get total duration for a specific operation type
    pub fn total_duration(&self, operation: OperationType) -> Duration {
        self.duration_totals
            .lock()
            .unwrap()
            .get(operation.as_str())
            .copied()
            .unwrap_or(Duration::ZERO)
    }

    /// Get average duration for a specific operation type
    pub fn average_duration(&self, operation: OperationType) -> Option<Duration> {
        let count = self.operation_count(operation);
        if count == 0 {
            return None;
        }
        let total = self.total_duration(operation);
        Some(total / count as u32)
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.operation_counts.lock().unwrap().clear();
        self.error_counts.lock().unwrap().clear();
        self.duration_totals.lock().unwrap().clear();
    }
}

impl TelemetryHook for MetricsHook {
    fn after_operation(&self, result: &OperationResult) {
        let op_name = result.context.operation.as_str();

        // Update operation count
        let mut counts = self.operation_counts.lock().unwrap();
        *counts.entry(op_name).or_insert(0) += 1;
        drop(counts);

        // Update duration total
        let mut durations = self.duration_totals.lock().unwrap();
        let entry = durations.entry(op_name).or_insert(Duration::ZERO);
        *entry += result.duration;
        drop(durations);

        // Update error count if failed
        if !result.success {
            let mut errors = self.error_counts.lock().unwrap();
            *errors.entry(op_name).or_insert(0) += 1;
        }
    }

    fn on_error(&self, operation: OperationType, _error: &BackendError) {
        let mut errors = self.error_counts.lock().unwrap();
        *errors.entry(operation.as_str()).or_insert(0) += 1;
    }
}

/// Combined telemetry hook that calls multiple hooks
#[derive(Default)]
pub struct MultiHook {
    hooks: Vec<Arc<dyn TelemetryHook>>,
}

impl MultiHook {
    /// Create a new multi-hook
    pub fn new() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Add a hook
    pub fn add_hook(mut self, hook: Arc<dyn TelemetryHook>) -> Self {
        self.hooks.push(hook);
        self
    }

    /// Get the number of registered hooks
    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    /// Check if there are no hooks
    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

impl TelemetryHook for MultiHook {
    fn before_operation(&self, context: &OperationContext) {
        for hook in &self.hooks {
            hook.before_operation(context);
        }
    }

    fn after_operation(&self, result: &OperationResult) {
        for hook in &self.hooks {
            hook.after_operation(result);
        }
    }

    fn on_error(&self, operation: OperationType, error: &BackendError) {
        for hook in &self.hooks {
            hook.on_error(operation, error);
        }
    }

    fn on_event(&self, event_name: &str, data: &[(String, String)]) {
        for hook in &self.hooks {
            hook.on_event(event_name, data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_type_as_str() {
        assert_eq!(OperationType::Store.as_str(), "store");
        assert_eq!(OperationType::Get.as_str(), "get");
        assert_eq!(OperationType::BatchStore.as_str(), "batch_store");
    }

    #[test]
    fn test_operation_context() {
        let ctx = OperationContext::new(OperationType::Store)
            .with_task_id(Uuid::new_v4())
            .with_batch_size(10)
            .with_metadata("key".to_string(), "value".to_string());

        assert_eq!(ctx.operation, OperationType::Store);
        assert!(ctx.task_id.is_some());
        assert_eq!(ctx.batch_size, Some(10));
        assert_eq!(ctx.metadata.len(), 1);
    }

    #[test]
    fn test_operation_result_success() {
        let ctx = OperationContext::new(OperationType::Get);
        let result = OperationResult::success(ctx).with_data_size(1024);

        assert!(result.success);
        assert!(result.error.is_none());
        assert_eq!(result.data_size, Some(1024));
    }

    #[test]
    fn test_operation_result_failure() {
        let ctx = OperationContext::new(OperationType::Store);
        let error = BackendError::Connection("test error".to_string());
        let result = OperationResult::failure(ctx, &error);

        assert!(!result.success);
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("test error"));
    }

    #[test]
    fn test_logging_hook_creation() {
        let hook = LoggingHook::new().with_min_duration(10.0).errors_only();

        assert!(!hook.log_success);
        assert_eq!(hook.min_duration_ms, Some(10.0));
    }

    #[test]
    fn test_metrics_hook() {
        let hook = MetricsHook::new();
        let ctx = OperationContext::new(OperationType::Store);
        let result = OperationResult::success(ctx);

        hook.after_operation(&result);

        assert_eq!(hook.operation_count(OperationType::Store), 1);
        assert_eq!(hook.error_count(OperationType::Store), 0);
        assert!(hook.average_duration(OperationType::Store).is_some());
    }

    #[test]
    fn test_metrics_hook_reset() {
        let hook = MetricsHook::new();
        let ctx = OperationContext::new(OperationType::Get);
        let result = OperationResult::success(ctx);

        hook.after_operation(&result);
        assert_eq!(hook.operation_count(OperationType::Get), 1);

        hook.reset();
        assert_eq!(hook.operation_count(OperationType::Get), 0);
    }

    #[test]
    fn test_multi_hook() {
        let hook1 = Arc::new(MetricsHook::new());
        let hook2 = Arc::new(LoggingHook::new());

        let multi = MultiHook::new().add_hook(hook1.clone()).add_hook(hook2);

        assert_eq!(multi.len(), 2);
        assert!(!multi.is_empty());

        let ctx = OperationContext::new(OperationType::Delete);
        let result = OperationResult::success(ctx);

        multi.after_operation(&result);

        assert_eq!(hook1.operation_count(OperationType::Delete), 1);
    }

    #[test]
    fn test_multi_hook_empty() {
        let multi = MultiHook::new();
        assert!(multi.is_empty());
        assert_eq!(multi.len(), 0);
    }
}
