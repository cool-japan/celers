//! PostgreSQL broker utility functions
//!
//! This module provides utility functions for PostgreSQL broker operations,
//! including batch optimization, memory estimation, and performance tuning.
//!
//! # Examples
//!
//! ```
//! use celers_broker_postgres::utilities::*;
//!
//! // Calculate optimal batch size
//! let batch_size = calculate_optimal_postgres_batch_size(1000, 1024, 100);
//! println!("Recommended batch size: {}", batch_size);
//!
//! // Estimate memory usage
//! let memory = estimate_postgres_queue_memory(1000, 1024);
//! println!("Estimated memory: {} bytes", memory);
//!
//! // Calculate optimal pool size
//! let pool_size = calculate_optimal_postgres_pool_size(100, 50);
//! println!("Recommended pool size: {}", pool_size);
//! ```

use std::collections::HashMap;

/// Calculate optimal batch size for PostgreSQL operations
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `avg_message_size` - Average message size in bytes
/// * `target_latency_ms` - Target latency in milliseconds
///
/// # Returns
///
/// Recommended batch size
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_optimal_postgres_batch_size;
///
/// let batch_size = calculate_optimal_postgres_batch_size(1000, 1024, 100);
/// assert!(batch_size > 0);
/// assert!(batch_size <= 1000);
/// ```
pub fn calculate_optimal_postgres_batch_size(
    queue_size: usize,
    avg_message_size: usize,
    target_latency_ms: u64,
) -> usize {
    // PostgreSQL optimal batch size formula:
    // - Smaller batches for large messages
    // - Larger batches for small messages
    // - Consider transaction overhead

    let base_batch_size = if avg_message_size > 100_000 {
        // Large messages (> 100KB): small batches
        10
    } else if avg_message_size > 10_000 {
        // Medium messages (10-100KB): medium batches
        50
    } else {
        // Small messages (< 10KB): large batches
        100
    };

    // Adjust for latency requirements
    let latency_factor = if target_latency_ms < 50 {
        0.5
    } else if target_latency_ms < 100 {
        1.0
    } else {
        1.5
    };

    let batch_size = (base_batch_size as f64 * latency_factor) as usize;

    // Never exceed queue size or reasonable max (1000)
    batch_size.min(queue_size).clamp(1, 1000)
}

/// Estimate PostgreSQL memory usage for queue
///
/// # Arguments
///
/// * `queue_size` - Number of messages in queue
/// * `avg_message_size` - Average message size in bytes
///
/// # Returns
///
/// Estimated memory usage in bytes
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::estimate_postgres_queue_memory;
///
/// let memory = estimate_postgres_queue_memory(1000, 1024);
/// assert!(memory > 0);
/// ```
pub fn estimate_postgres_queue_memory(queue_size: usize, avg_message_size: usize) -> usize {
    // PostgreSQL overhead per row (approximate)
    // - Row header: ~23 bytes
    // - Alignment padding: ~8 bytes
    // - Index overhead: ~32 bytes per index
    // - Page header overhead: ~24 bytes per 8KB page
    let overhead_per_row = 23 + 8 + (32 * 3) + 3; // 3 indexes, amortized page overhead

    queue_size * (avg_message_size + overhead_per_row)
}

/// Calculate optimal number of PostgreSQL connections for pool
///
/// # Arguments
///
/// * `expected_concurrency` - Expected concurrent operations
/// * `avg_operation_duration_ms` - Average operation duration in ms
///
/// # Returns
///
/// Recommended pool size
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_optimal_postgres_pool_size;
///
/// let pool_size = calculate_optimal_postgres_pool_size(100, 50);
/// assert!(pool_size > 0);
/// ```
pub fn calculate_optimal_postgres_pool_size(
    expected_concurrency: usize,
    avg_operation_duration_ms: u64,
) -> usize {
    // Rule of thumb: Pool size should handle expected concurrency
    // with some buffer for spikes
    // PostgreSQL has connection overhead, so don't over-provision

    let base_size = expected_concurrency;

    // Add buffer based on operation duration
    let buffer = if avg_operation_duration_ms > 100 {
        (expected_concurrency as f64 * 0.5) as usize
    } else if avg_operation_duration_ms > 50 {
        (expected_concurrency as f64 * 0.3) as usize
    } else {
        (expected_concurrency as f64 * 0.2) as usize
    };

    // PostgreSQL max_connections default is 100
    // Keep pool size reasonable
    (base_size + buffer).clamp(5, 200)
}

/// Estimate time to drain queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `processing_rate` - Processing rate (messages per second)
///
/// # Returns
///
/// Estimated drain time in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::estimate_postgres_queue_drain_time;
///
/// let drain_time = estimate_postgres_queue_drain_time(1000, 50.0);
/// assert_eq!(drain_time, 20.0);
/// ```
pub fn estimate_postgres_queue_drain_time(queue_size: usize, processing_rate: f64) -> f64 {
    if processing_rate > 0.0 {
        queue_size as f64 / processing_rate
    } else {
        f64::INFINITY
    }
}

/// Suggest PostgreSQL query optimization strategy
///
/// # Arguments
///
/// * `operation_count` - Number of operations
/// * `operation_type` - Type of operation ("read" or "write")
///
/// # Returns
///
/// Optimization strategy as string
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_query_strategy;
///
/// let strategy = suggest_postgres_query_strategy(100, "write");
/// assert!(!strategy.is_empty());
/// ```
pub fn suggest_postgres_query_strategy(operation_count: usize, operation_type: &str) -> String {
    if operation_count < 10 {
        "Execute operations individually - transaction overhead minimal".to_string()
    } else if operation_count < 100 {
        format!(
            "Use single transaction with {} {} operations",
            operation_count, operation_type
        )
    } else {
        let chunk_size = if operation_type == "write" { 500 } else { 1000 };
        format!(
            "Use chunked transactions of {} operations each (total: {} chunks) with COPY for bulk inserts",
            chunk_size,
            operation_count.div_ceil(chunk_size)
        )
    }
}

/// Suggest PostgreSQL VACUUM strategy
///
/// # Arguments
///
/// * `table_bloat_percent` - Table bloat percentage (0-100)
/// * `table_size_mb` - Table size in megabytes
///
/// # Returns
///
/// Recommended VACUUM strategy
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_vacuum_strategy;
///
/// let strategy = suggest_postgres_vacuum_strategy(25.0, 100.0);
/// assert!(strategy.contains("VACUUM"));
/// ```
pub fn suggest_postgres_vacuum_strategy(table_bloat_percent: f64, table_size_mb: f64) -> String {
    if table_bloat_percent > 50.0 {
        "VACUUM FULL recommended - high bloat detected (will lock table)".to_string()
    } else if table_bloat_percent > 20.0 && table_size_mb > 1000.0 {
        "VACUUM (ANALYZE) recommended - moderate bloat on large table".to_string()
    } else if table_bloat_percent > 10.0 {
        "VACUUM recommended - low to moderate bloat".to_string()
    } else {
        "ANALYZE only - bloat is acceptable, update statistics".to_string()
    }
}

/// Suggest PostgreSQL index strategy
///
/// # Arguments
///
/// * `index_scan_count` - Number of index scans
/// * `seq_scan_count` - Number of sequential scans
/// * `table_rows` - Number of rows in table
///
/// # Returns
///
/// Index recommendation
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_index_strategy;
///
/// let recommendation = suggest_postgres_index_strategy(100, 10000, 1000000);
/// assert!(!recommendation.is_empty());
/// ```
pub fn suggest_postgres_index_strategy(
    index_scan_count: u64,
    seq_scan_count: u64,
    table_rows: usize,
) -> String {
    let total_scans = index_scan_count + seq_scan_count;
    if total_scans == 0 {
        return "No query activity detected".to_string();
    }

    let seq_scan_ratio = seq_scan_count as f64 / total_scans as f64;

    if seq_scan_ratio > 0.5 && table_rows > 100_000 {
        "High sequential scan ratio on large table - consider adding indexes".to_string()
    } else if seq_scan_ratio > 0.2 && table_rows > 1_000_000 {
        "Moderate sequential scan ratio - review query patterns and consider selective indexes"
            .to_string()
    } else if index_scan_count > 0 && seq_scan_ratio < 0.1 {
        "Good index usage - indexes are effective".to_string()
    } else {
        "Balanced scan pattern - current indexes appear adequate".to_string()
    }
}

/// Analyze PostgreSQL query performance
///
/// # Arguments
///
/// * `query_latencies` - Map of query type to latency in ms
///
/// # Returns
///
/// Performance analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::analyze_postgres_query_performance;
/// use std::collections::HashMap;
///
/// let mut latencies = HashMap::new();
/// latencies.insert("enqueue".to_string(), 5.0);
/// latencies.insert("dequeue".to_string(), 10.0);
/// latencies.insert("ack".to_string(), 3.0);
///
/// let analysis = analyze_postgres_query_performance(&latencies);
/// assert!(analysis.contains_key("slowest_query"));
/// ```
pub fn analyze_postgres_query_performance(
    query_latencies: &HashMap<String, f64>,
) -> HashMap<String, String> {
    let mut analysis = HashMap::new();

    if query_latencies.is_empty() {
        analysis.insert("status".to_string(), "no_data".to_string());
        return analysis;
    }

    // Find slowest query
    let (slowest_query, max_latency) = query_latencies
        .iter()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .unwrap();

    analysis.insert("slowest_query".to_string(), slowest_query.clone());
    analysis.insert("max_latency_ms".to_string(), format!("{:.2}", max_latency));

    // Calculate average
    let avg_latency: f64 = query_latencies.values().sum::<f64>() / query_latencies.len() as f64;
    analysis.insert("avg_latency_ms".to_string(), format!("{:.2}", avg_latency));

    // Performance status
    let status = if avg_latency < 5.0 {
        "excellent"
    } else if avg_latency < 10.0 {
        "good"
    } else if avg_latency < 20.0 {
        "acceptable"
    } else {
        "poor"
    };
    analysis.insert("overall_status".to_string(), status.to_string());

    analysis
}

/// Suggest PostgreSQL autovacuum tuning
///
/// # Arguments
///
/// * `throughput_msg_per_sec` - Message throughput
/// * `table_update_rate` - Table update rate (updates per second)
///
/// # Returns
///
/// Recommended autovacuum configuration
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_autovacuum_tuning;
///
/// let config = suggest_postgres_autovacuum_tuning(500.0, 1000.0);
/// assert!(config.contains("autovacuum"));
/// ```
pub fn suggest_postgres_autovacuum_tuning(
    throughput_msg_per_sec: f64,
    table_update_rate: f64,
) -> String {
    if table_update_rate > 1000.0 {
        "High update rate: autovacuum_naptime=10s, autovacuum_vacuum_scale_factor=0.05 (aggressive)"
            .to_string()
    } else if table_update_rate > 500.0 {
        "Moderate update rate: autovacuum_naptime=20s, autovacuum_vacuum_scale_factor=0.1 (balanced)".to_string()
    } else if throughput_msg_per_sec > 100.0 {
        "Low update rate, high throughput: autovacuum_naptime=30s, autovacuum_vacuum_scale_factor=0.15 (standard)".to_string()
    } else {
        "Low activity: autovacuum_naptime=60s, autovacuum_vacuum_scale_factor=0.2 (conservative)"
            .to_string()
    }
}

/// Calculate optimal PostgreSQL timeout values
///
/// # Arguments
///
/// * `avg_operation_ms` - Average operation duration in ms
/// * `p99_operation_ms` - 99th percentile operation duration in ms
///
/// # Returns
///
/// (connection_timeout, statement_timeout) in milliseconds
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_timeout_values;
///
/// let (conn_timeout, stmt_timeout) = calculate_postgres_timeout_values(50.0, 200.0);
/// assert!(conn_timeout > 0);
/// assert!(stmt_timeout > 0);
/// ```
pub fn calculate_postgres_timeout_values(
    avg_operation_ms: f64,
    p99_operation_ms: f64,
) -> (u64, u64) {
    // Connection timeout: 3x average operation time, min 2000ms
    let connection_timeout = ((avg_operation_ms * 3.0) as u64).max(2000);

    // Statement timeout: 2x p99 latency, min 10000ms
    let statement_timeout = ((p99_operation_ms * 2.0) as u64).max(10000);

    (connection_timeout, statement_timeout)
}

/// Suggest PostgreSQL work_mem setting
///
/// # Arguments
///
/// * `avg_sort_size_mb` - Average sort operation size in MB
/// * `concurrent_workers` - Expected concurrent workers
/// * `total_ram_gb` - Total available RAM in GB
///
/// # Returns
///
/// Recommended work_mem in MB
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_work_mem;
///
/// let work_mem = suggest_postgres_work_mem(10.0, 20, 16.0);
/// assert!(work_mem > 0);
/// ```
pub fn suggest_postgres_work_mem(
    avg_sort_size_mb: f64,
    concurrent_workers: usize,
    total_ram_gb: f64,
) -> usize {
    // Rule of thumb: work_mem = (Total RAM * 0.25) / max_concurrent_connections
    // But also consider average sort size

    let ram_based = ((total_ram_gb * 1024.0 * 0.25) / concurrent_workers as f64) as usize;
    let sort_based = (avg_sort_size_mb * 1.5) as usize;

    // Use the larger of the two, but cap at reasonable limits
    ram_based.max(sort_based).clamp(4, 256)
}

/// Estimate PostgreSQL shared_buffers recommendation
///
/// # Arguments
///
/// * `total_ram_gb` - Total available RAM in GB
/// * `database_size_gb` - Total database size in GB
///
/// # Returns
///
/// Recommended shared_buffers in MB
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_shared_buffers;
///
/// let shared_buffers = suggest_postgres_shared_buffers(32.0, 10.0);
/// assert!(shared_buffers > 0);
/// ```
pub fn suggest_postgres_shared_buffers(total_ram_gb: f64, database_size_gb: f64) -> usize {
    // Rule of thumb:
    // - 25% of RAM for dedicated server
    // - Up to 40% for very large RAM
    // - Consider database size

    let ram_based = if total_ram_gb >= 64.0 {
        (total_ram_gb * 1024.0 * 0.35) as usize
    } else if total_ram_gb >= 32.0 {
        (total_ram_gb * 1024.0 * 0.30) as usize
    } else {
        (total_ram_gb * 1024.0 * 0.25) as usize
    };

    let db_based = (database_size_gb * 1024.0 * 0.5) as usize;

    // Use the smaller of the two, capped at reasonable limits
    ram_based.min(db_based).clamp(128, 16384)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_batch_size() {
        let batch_size = calculate_optimal_postgres_batch_size(1000, 1024, 100);
        assert!(batch_size > 0);
        assert!(batch_size <= 1000);
    }

    #[test]
    fn test_calculate_optimal_batch_size_large_messages() {
        let batch_size = calculate_optimal_postgres_batch_size(1000, 200_000, 100);
        assert!(batch_size <= 20);
    }

    #[test]
    fn test_estimate_queue_memory() {
        let memory = estimate_postgres_queue_memory(1000, 1024);
        assert!(memory > 1024 * 1000);
    }

    #[test]
    fn test_calculate_optimal_pool_size() {
        let pool_size = calculate_optimal_postgres_pool_size(100, 50);
        assert!(pool_size >= 100);
        assert!(pool_size <= 200);
    }

    #[test]
    fn test_estimate_drain_time() {
        let drain_time = estimate_postgres_queue_drain_time(1000, 50.0);
        assert_eq!(drain_time, 20.0);
    }

    #[test]
    fn test_estimate_drain_time_zero_rate() {
        let drain_time = estimate_postgres_queue_drain_time(1000, 0.0);
        assert!(drain_time.is_infinite());
    }

    #[test]
    fn test_suggest_query_strategy() {
        let strategy = suggest_postgres_query_strategy(5, "write");
        assert!(strategy.contains("individually"));

        let strategy = suggest_postgres_query_strategy(50, "write");
        assert!(strategy.contains("single transaction"));

        let strategy = suggest_postgres_query_strategy(1000, "write");
        assert!(strategy.contains("chunked"));
    }

    #[test]
    fn test_suggest_vacuum_strategy() {
        let strategy = suggest_postgres_vacuum_strategy(60.0, 100.0);
        assert!(strategy.contains("VACUUM FULL"));

        let strategy = suggest_postgres_vacuum_strategy(25.0, 1500.0);
        assert!(strategy.contains("VACUUM (ANALYZE)"));

        let strategy = suggest_postgres_vacuum_strategy(5.0, 100.0);
        assert!(strategy.contains("ANALYZE"));
    }

    #[test]
    fn test_suggest_index_strategy() {
        let recommendation = suggest_postgres_index_strategy(100, 10000, 1000000);
        assert!(recommendation.contains("sequential scan"));

        let recommendation = suggest_postgres_index_strategy(10000, 100, 1000000);
        assert!(recommendation.contains("Good index usage"));
    }

    #[test]
    fn test_analyze_query_performance() {
        let mut latencies = HashMap::new();
        latencies.insert("enqueue".to_string(), 5.0);
        latencies.insert("dequeue".to_string(), 15.0);
        latencies.insert("ack".to_string(), 3.0);

        let analysis = analyze_postgres_query_performance(&latencies);
        assert_eq!(analysis.get("slowest_query"), Some(&"dequeue".to_string()));
        assert!(analysis.contains_key("avg_latency_ms"));
        assert!(analysis.contains_key("overall_status"));
    }

    #[test]
    fn test_suggest_autovacuum_tuning() {
        let config = suggest_postgres_autovacuum_tuning(500.0, 1500.0);
        assert!(config.contains("autovacuum"));

        let config = suggest_postgres_autovacuum_tuning(50.0, 50.0);
        assert!(config.contains("autovacuum"));
    }

    #[test]
    fn test_calculate_timeout_values() {
        let (conn_timeout, stmt_timeout) = calculate_postgres_timeout_values(50.0, 200.0);
        assert!(conn_timeout >= 2000);
        assert!(stmt_timeout >= 10000);
    }

    #[test]
    fn test_suggest_work_mem() {
        let work_mem = suggest_postgres_work_mem(10.0, 20, 16.0);
        assert!(work_mem >= 4);
        assert!(work_mem <= 256);
    }

    #[test]
    fn test_suggest_shared_buffers() {
        let shared_buffers = suggest_postgres_shared_buffers(32.0, 10.0);
        assert!(shared_buffers >= 128);
        assert!(shared_buffers <= 16384);
    }

    #[test]
    fn test_calculate_replication_lag_severity() {
        let severity = calculate_postgres_replication_lag_severity(30.0);
        assert_eq!(severity, "moderate");

        let severity = calculate_postgres_replication_lag_severity(200.0);
        assert_eq!(severity, "critical");
    }

    #[test]
    fn test_estimate_migration_time() {
        let time = estimate_postgres_migration_time(1_000_000, 10_000);
        assert!(time > 0.0);
    }

    #[test]
    fn test_calculate_wal_generation_rate() {
        let rate = calculate_postgres_wal_generation_rate(1024 * 1024 * 100, 3600);
        assert!(rate > 0.0);
    }

    #[test]
    fn test_suggest_maintenance_window() {
        let window = suggest_postgres_maintenance_window(100_000, 50.0);
        assert!(window.duration_minutes > 0);
        assert!(!window.recommendations.is_empty());
    }

    #[test]
    fn test_calculate_index_rebuild_priority() {
        let priority = calculate_postgres_index_rebuild_priority(1000, 0.4, 72.0);
        assert!(priority.priority_score >= 0.0 && priority.priority_score <= 1.0);
    }

    #[test]
    fn test_compression_recommendation_large_json() {
        let rec = calculate_compression_recommendation(10240, "json", 1000);
        assert!(rec.should_compress);
        assert_eq!(rec.estimated_ratio, 0.3);
        assert_eq!(rec.recommended_algorithm, "gzip");
    }

    #[test]
    fn test_compression_recommendation_small_payload() {
        let rec = calculate_compression_recommendation(512, "json", 1000);
        assert!(!rec.should_compress);
        assert!(rec.reason.contains("too small"));
    }

    #[test]
    fn test_compression_recommendation_binary() {
        let rec = calculate_compression_recommendation(10240, "binary", 1000);
        assert!(!rec.should_compress);
        assert_eq!(rec.estimated_ratio, 0.9);
    }

    #[test]
    fn test_query_performance_regression_no_regression() {
        let analysis = detect_query_performance_regression("test_query", 100.0, 100.0, 20.0);
        assert!(!analysis.is_regression);
        assert_eq!(analysis.severity, "none");
        assert_eq!(analysis.regression_percent, 0.0);
    }

    #[test]
    fn test_query_performance_regression_minor() {
        let analysis = detect_query_performance_regression("test_query", 130.0, 100.0, 20.0);
        assert!(analysis.is_regression);
        assert_eq!(analysis.severity, "minor");
        assert_eq!(analysis.regression_percent, 30.0);
    }

    #[test]
    fn test_query_performance_regression_severe() {
        let analysis = detect_query_performance_regression("test_query", 250.0, 100.0, 20.0);
        assert!(analysis.is_regression);
        assert_eq!(analysis.severity, "severe");
        assert!(analysis.regression_percent > 100.0);
    }

    #[test]
    fn test_task_execution_metrics_normal() {
        let metrics = calculate_task_execution_metrics("normal_task", 500.0, 5_000_000, 10);
        assert_eq!(metrics.avg_cpu_time_ms, 50.0);
        assert_eq!(metrics.avg_memory_bytes, 500_000);
        assert!(!metrics.is_resource_intensive);
    }

    #[test]
    fn test_task_execution_metrics_intensive() {
        let metrics = calculate_task_execution_metrics("heavy_task", 50000.0, 500_000_000, 10);
        assert_eq!(metrics.avg_cpu_time_ms, 5000.0);
        assert_eq!(metrics.avg_memory_bytes, 50_000_000);
        assert!(metrics.is_resource_intensive);
        assert!(!metrics.suggestions.is_empty());
    }

    #[test]
    fn test_task_execution_metrics_no_executions() {
        let metrics = calculate_task_execution_metrics("new_task", 0.0, 0, 0);
        assert_eq!(metrics.avg_cpu_time_ms, 0.0);
        assert_eq!(metrics.total_executions, 0);
    }

    #[test]
    fn test_dlq_retry_policy_default() {
        let policy = DlqRetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_delay_secs, 60);
        assert_eq!(policy.max_delay_secs, 3600);
        assert!(policy.use_jitter);
    }

    #[test]
    fn test_calculate_dlq_retry_delay_initial() {
        let policy = DlqRetryPolicy::default();
        let delay = calculate_dlq_retry_delay(0, &policy);
        assert_eq!(delay, 60);
    }

    #[test]
    fn test_calculate_dlq_retry_delay_capped() {
        let policy = DlqRetryPolicy::default();
        let delay = calculate_dlq_retry_delay(10, &policy);
        assert!(delay <= 3600);
    }

    #[test]
    fn test_calculate_dlq_retry_delay_max_retries() {
        let policy = DlqRetryPolicy::default();
        let delay = calculate_dlq_retry_delay(5, &policy);
        assert_eq!(delay, 0);
    }

    #[test]
    fn test_suggest_dlq_retry_policy_fast_task() {
        let policy = suggest_dlq_retry_policy("fast_task", 100.0, 0.01);
        assert_eq!(policy.initial_delay_secs, 30);
        assert_eq!(policy.max_retries, 3);
    }

    #[test]
    fn test_suggest_dlq_retry_policy_slow_task() {
        let policy = suggest_dlq_retry_policy("slow_task", 15000.0, 0.1);
        assert_eq!(policy.initial_delay_secs, 300);
        assert_eq!(policy.max_delay_secs, 7200);
    }

    #[test]
    fn test_suggest_dlq_retry_policy_high_failure_rate() {
        let policy = suggest_dlq_retry_policy("unreliable_task", 1000.0, 0.6);
        assert_eq!(policy.max_retries, 1);
    }

    #[test]
    fn test_analyze_connection_pool_healthy() {
        let analytics = analyze_connection_pool_advanced(20, 12, 8, 1000, 950, 10.0, 3600);
        assert_eq!(analytics.health_status, "healthy");
        assert!((analytics.utilization - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_analyze_connection_pool_stressed() {
        let analytics = analyze_connection_pool_advanced(20, 19, 1, 1000, 950, 150.0, 3600);
        assert_eq!(analytics.health_status, "stressed");
        assert!(analytics.utilization > 0.9);
        assert!(!analytics.warnings.is_empty());
    }

    #[test]
    fn test_analyze_connection_pool_leak_detection() {
        let analytics = analyze_connection_pool_advanced(20, 15, 5, 1000, 950, 10.0, 3600);
        assert!(analytics.warnings.iter().any(|w| w.contains("leak")));
    }

    #[test]
    fn test_forecast_task_processing_rate_stable() {
        let rates = vec![100.0, 105.0, 95.0, 100.0, 98.0];
        let forecast = forecast_task_processing_rate(&rates, 5, 100.0);
        assert_eq!(forecast.trend, "stable");
        assert!(forecast.confidence > 0.0);
    }

    #[test]
    fn test_forecast_task_processing_rate_increasing() {
        let rates = vec![100.0, 110.0, 120.0, 130.0, 140.0];
        let forecast = forecast_task_processing_rate(&rates, 5, 100.0);
        assert_eq!(forecast.trend, "increasing");
        assert!(forecast.forecasted_rate_24h > forecast.current_rate);
    }

    #[test]
    fn test_forecast_task_processing_rate_empty() {
        let rates: Vec<f64> = vec![];
        let forecast = forecast_task_processing_rate(&rates, 5, 100.0);
        assert_eq!(forecast.current_rate, 0.0);
        assert_eq!(forecast.trend, "unknown");
    }

    #[test]
    fn test_calculate_database_health_score_excellent() {
        let health = calculate_database_health_score(0.95, 50.0, 0.60, 0.05, 0.90, 12.0);
        assert_eq!(health.health_grade, "A");
        assert!(health.overall_score > 0.9);
    }

    #[test]
    fn test_calculate_database_health_score_poor() {
        let health = calculate_database_health_score(0.80, 600.0, 0.95, 0.25, 0.50, 200.0);
        assert!(health.overall_score < 0.6);
        assert!(!health.critical_issues.is_empty());
    }

    #[test]
    fn test_calculate_database_health_score_warnings() {
        let health = calculate_database_health_score(0.88, 150.0, 0.80, 0.12, 0.75, 50.0);
        assert!(!health.warnings.is_empty());
        assert!(!health.recommendations.is_empty());
    }

    #[test]
    fn test_analyze_batch_optimization_optimal() {
        let analysis = analyze_batch_optimization(25, 1024, 100.0, 512, 5.0);
        assert!(analysis.efficiency_score > 0.9);
        assert_eq!(analysis.recommended_batch_size, 25);
    }

    #[test]
    fn test_analyze_batch_optimization_too_small() {
        let analysis = analyze_batch_optimization(5, 1024, 100.0, 512, 15.0);
        assert!(analysis.recommended_batch_size > 5);
        assert!(analysis.throughput_improvement_percent > 0.0);
    }

    #[test]
    fn test_analyze_batch_optimization_high_latency() {
        let analysis = analyze_batch_optimization(10, 1024, 100.0, 512, 25.0);
        assert_eq!(analysis.network_impact, "high - batching critical");
        assert!(analysis.recommended_batch_size > 10);
    }

    #[test]
    fn test_analyze_batch_optimization_memory_constrained() {
        let analysis = analyze_batch_optimization(100, 100_000, 100.0, 5, 5.0);
        // With 5MB available and 100KB per task, recommended batch is limited by memory
        assert!(analysis.recommended_batch_size < 100);
        // Check that memory is considered in the recommendations
        assert!(analysis.efficiency_score < 1.0);
    }
}

/// Calculate replication lag severity
///
/// Determines the severity level of replication lag in seconds.
///
/// # Arguments
///
/// * `lag_seconds` - Replication lag in seconds
///
/// # Returns
///
/// Severity level as string: "low", "moderate", "high", "critical"
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_replication_lag_severity;
///
/// let severity = calculate_postgres_replication_lag_severity(120.0);
/// println!("Lag severity: {}", severity);
/// ```
pub fn calculate_postgres_replication_lag_severity(lag_seconds: f64) -> String {
    if lag_seconds > 180.0 {
        "critical".to_string()
    } else if lag_seconds > 60.0 {
        "high".to_string()
    } else if lag_seconds > 10.0 {
        "moderate".to_string()
    } else {
        "low".to_string()
    }
}

/// Estimate migration time for table data
///
/// Provides a rough estimate of how long a data migration might take.
///
/// # Arguments
///
/// * `row_count` - Number of rows to migrate
/// * `rows_per_second` - Expected migration throughput
///
/// # Returns
///
/// Estimated time in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::estimate_postgres_migration_time;
///
/// let time_secs = estimate_postgres_migration_time(1_000_000, 5_000);
/// println!("Estimated migration time: {:.1} minutes", time_secs / 60.0);
/// ```
pub fn estimate_postgres_migration_time(row_count: u64, rows_per_second: u64) -> f64 {
    if rows_per_second == 0 {
        return 0.0;
    }
    row_count as f64 / rows_per_second as f64
}

/// Calculate WAL generation rate
///
/// Computes the rate at which Write-Ahead Log files are being generated.
///
/// # Arguments
///
/// * `wal_bytes_generated` - Bytes of WAL generated
/// * `time_period_secs` - Time period in seconds
///
/// # Returns
///
/// WAL generation rate in bytes per second
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_wal_generation_rate;
///
/// // 100 MB of WAL in 1 hour
/// let rate = calculate_postgres_wal_generation_rate(104_857_600, 3600);
/// println!("WAL rate: {:.2} MB/sec", rate / 1_048_576.0);
/// ```
pub fn calculate_postgres_wal_generation_rate(
    wal_bytes_generated: u64,
    time_period_secs: u64,
) -> f64 {
    if time_period_secs == 0 {
        return 0.0;
    }
    wal_bytes_generated as f64 / time_period_secs as f64
}

/// Maintenance window recommendation
#[derive(Debug, Clone)]
pub struct MaintenanceWindow {
    /// Recommended duration in minutes
    pub duration_minutes: u64,
    /// Recommended time of day (24h format)
    pub recommended_hour: u8,
    /// Operations to perform
    pub operations: Vec<String>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Suggest optimal maintenance window
///
/// Recommends an appropriate maintenance window based on table size and load.
///
/// # Arguments
///
/// * `table_rows` - Number of rows in table
/// * `daily_growth_percent` - Daily growth rate as percentage
///
/// # Returns
///
/// Maintenance window recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_maintenance_window;
///
/// let window = suggest_postgres_maintenance_window(5_000_000, 10.0);
/// println!("Recommended window: {} minutes", window.duration_minutes);
/// println!("Recommended hour: {}:00", window.recommended_hour);
/// ```
pub fn suggest_postgres_maintenance_window(
    table_rows: u64,
    daily_growth_percent: f64,
) -> MaintenanceWindow {
    // Estimate maintenance time based on table size
    // Rough heuristic: 1M rows ~= 1 minute for VACUUM + ANALYZE
    let base_duration = (table_rows as f64 / 1_000_000.0 * 5.0).max(5.0).ceil() as u64;

    // Add buffer time for high-growth tables
    let growth_buffer = if daily_growth_percent > 20.0 {
        base_duration / 2
    } else if daily_growth_percent > 10.0 {
        base_duration / 4
    } else {
        0
    };

    let duration_minutes = base_duration + growth_buffer;

    // Recommend early morning hours (2-4 AM) for low traffic
    let recommended_hour = 3;

    let mut operations = vec![
        "VACUUM ANALYZE".to_string(),
        "REINDEX if needed".to_string(),
    ];

    if daily_growth_percent > 15.0 {
        operations.push("Check for bloat".to_string());
        operations.push("Consider partitioning strategy".to_string());
    }

    let mut recommendations = vec![
        format!(
            "Schedule maintenance during low-traffic hours ({}:00)",
            recommended_hour
        ),
        format!(
            "Allocate {} minutes for maintenance window",
            duration_minutes
        ),
    ];

    if table_rows > 10_000_000 {
        recommendations.push("Consider incremental maintenance for large tables".to_string());
        recommendations.push("Monitor autovacuum effectiveness".to_string());
    }

    if daily_growth_percent > 20.0 {
        recommendations.push("High growth rate - consider more frequent maintenance".to_string());
    }

    MaintenanceWindow {
        duration_minutes,
        recommended_hour,
        operations,
        recommendations,
    }
}

/// Index rebuild priority
#[derive(Debug, Clone)]
pub struct IndexRebuildPriority {
    /// Priority score (0.0 = low, 1.0 = high)
    pub priority_score: f64,
    /// Priority level
    pub priority_level: String,
    /// Estimated rebuild time in minutes
    pub estimated_rebuild_minutes: u64,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Calculate index rebuild priority
///
/// Determines how urgently an index needs to be rebuilt based on bloat and usage.
///
/// # Arguments
///
/// * `index_scans` - Number of index scans
/// * `bloat_ratio` - Index bloat ratio (0.0 to 1.0)
/// * `hours_since_last_reindex` - Hours since last REINDEX
///
/// # Returns
///
/// Index rebuild priority analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_index_rebuild_priority;
///
/// let priority = calculate_postgres_index_rebuild_priority(50_000, 0.35, 168.0);
/// println!("Priority: {} ({:.2})", priority.priority_level, priority.priority_score);
/// ```
pub fn calculate_postgres_index_rebuild_priority(
    index_scans: u64,
    bloat_ratio: f64,
    hours_since_last_reindex: f64,
) -> IndexRebuildPriority {
    // Calculate priority based on usage and bloat
    let usage_score = (index_scans as f64 / 100_000.0).min(1.0);
    let bloat_score = bloat_ratio;
    let time_score = (hours_since_last_reindex / 720.0).min(1.0); // 720 hours = 30 days

    let priority_score = (usage_score * 0.4 + bloat_score * 0.5 + time_score * 0.1).min(1.0);

    let priority_level = if priority_score > 0.8 {
        "critical".to_string()
    } else if priority_score > 0.6 {
        "high".to_string()
    } else if priority_score > 0.4 {
        "medium".to_string()
    } else {
        "low".to_string()
    };

    // Estimate rebuild time (heuristic)
    let estimated_rebuild_minutes = ((index_scans as f64 / 10_000.0) * 2.0).max(1.0).ceil() as u64;

    let mut recommendations = Vec::new();

    match priority_level.as_str() {
        "critical" => {
            recommendations.push("URGENT: Index needs immediate rebuild".to_string());
            recommendations
                .push("Schedule REINDEX CONCURRENTLY during maintenance window".to_string());
            recommendations.push("Monitor query performance before and after".to_string());
        }
        "high" => {
            recommendations.push("Index should be rebuilt soon".to_string());
            recommendations.push("Use REINDEX CONCURRENTLY to avoid locking".to_string());
        }
        "medium" => {
            recommendations.push("Consider rebuilding during next maintenance window".to_string());
        }
        _ => {
            recommendations.push("Index rebuild is not urgent".to_string());
        }
    }

    if bloat_ratio > 0.4 {
        recommendations.push(format!(
            "High bloat detected ({:.0}%) - rebuild recommended",
            bloat_ratio * 100.0
        ));
    }

    if index_scans > 1_000_000 {
        recommendations
            .push("Heavily used index - minimize downtime with REINDEX CONCURRENTLY".to_string());
    }

    IndexRebuildPriority {
        priority_score,
        priority_level,
        estimated_rebuild_minutes,
        recommendations,
    }
}

/// Query cost estimation result
#[derive(Debug, Clone)]
pub struct QueryCostEstimate {
    /// Estimated rows to be scanned
    pub estimated_rows: u64,
    /// Estimated cost units
    pub estimated_cost: f64,
    /// Cost classification
    pub cost_level: String,
    /// Whether query needs optimization
    pub needs_optimization: bool,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

/// Estimate PostgreSQL query execution cost
///
/// Provides cost estimation and optimization recommendations for queries
/// based on row counts, selectivity, and access patterns.
///
/// # Arguments
///
/// * `table_rows` - Total rows in the table
/// * `selectivity` - Estimated selectivity (0.0 to 1.0)
/// * `has_index` - Whether an index is available
/// * `is_sequential_scan` - Whether query will use sequential scan
///
/// # Returns
///
/// Query cost estimate with optimization recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::estimate_postgres_query_cost;
///
/// // Large table with poor selectivity and no index
/// let cost = estimate_postgres_query_cost(1_000_000, 0.5, false, true);
/// println!("Estimated cost: {:.2}", cost.estimated_cost);
/// println!("Needs optimization: {}", cost.needs_optimization);
/// assert!(cost.needs_optimization);
/// ```
pub fn estimate_postgres_query_cost(
    table_rows: u64,
    selectivity: f64,
    has_index: bool,
    is_sequential_scan: bool,
) -> QueryCostEstimate {
    let selectivity = selectivity.clamp(0.0, 1.0);
    let estimated_rows = (table_rows as f64 * selectivity) as u64;

    // PostgreSQL cost model (simplified):
    // - Sequential scan: cost = rows * seq_page_cost (default 1.0)
    // - Index scan: cost = rows * random_page_cost (default 4.0) * selectivity
    let estimated_cost = if is_sequential_scan {
        table_rows as f64 * 1.0 // seq_page_cost
    } else if has_index {
        table_rows as f64 * 4.0 * selectivity // random_page_cost
    } else {
        table_rows as f64 * 1.0
    };

    let cost_level = if estimated_cost > 100_000.0 {
        "very_high".to_string()
    } else if estimated_cost > 10_000.0 {
        "high".to_string()
    } else if estimated_cost > 1_000.0 {
        "medium".to_string()
    } else {
        "low".to_string()
    };

    let needs_optimization = (is_sequential_scan && table_rows > 10_000 && selectivity < 0.1)
        || (!has_index && table_rows > 100_000)
        || estimated_cost > 10_000.0;

    let mut recommendations = Vec::new();

    if needs_optimization {
        if is_sequential_scan && selectivity < 0.1 && table_rows > 10_000 {
            recommendations
                .push("Add index: Sequential scan with low selectivity on large table".to_string());
            recommendations.push(format!(
                "Scanning {} rows to return {} rows ({:.1}% selectivity)",
                table_rows,
                estimated_rows,
                selectivity * 100.0
            ));
        }

        if !has_index && table_rows > 100_000 {
            recommendations.push("Create index for large table queries".to_string());
        }

        if estimated_cost > 100_000.0 {
            recommendations.push(format!(
                "Very expensive query (cost: {:.0}) - urgent optimization needed",
                estimated_cost
            ));
            recommendations.push("Consider table partitioning or materialized views".to_string());
        } else if estimated_cost > 10_000.0 {
            recommendations.push(format!(
                "Expensive query (cost: {:.0}) - optimization recommended",
                estimated_cost
            ));
        }
    } else {
        recommendations.push("Query cost is acceptable".to_string());
        if has_index && !is_sequential_scan {
            recommendations.push("Index is being used effectively".to_string());
        }
    }

    QueryCostEstimate {
        estimated_rows,
        estimated_cost,
        cost_level,
        needs_optimization,
        recommendations,
    }
}

/// Table partitioning recommendation
#[derive(Debug, Clone)]
pub struct PartitioningRecommendation {
    /// Whether partitioning is recommended
    pub should_partition: bool,
    /// Recommended partitioning strategy
    pub strategy: String,
    /// Estimated partition size
    pub recommended_partition_size_mb: u64,
    /// Reasoning for recommendation
    pub reasoning: Vec<String>,
}

/// Suggest table partitioning strategy
///
/// Analyzes table characteristics and recommends partitioning strategy
/// for large or rapidly growing tables.
///
/// # Arguments
///
/// * `table_size_mb` - Current table size in MB
/// * `row_count` - Total number of rows
/// * `growth_rate_mb_per_day` - Daily growth rate in MB
/// * `has_time_column` - Whether table has a timestamp column
///
/// # Returns
///
/// Partitioning recommendation with strategy
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_postgres_table_partitioning;
///
/// // Large, rapidly growing table with timestamps
/// let recommendation = suggest_postgres_table_partitioning(50_000, 10_000_000, 500.0, true);
/// println!("Should partition: {}", recommendation.should_partition);
/// println!("Strategy: {}", recommendation.strategy);
/// assert!(recommendation.should_partition);
/// ```
pub fn suggest_postgres_table_partitioning(
    table_size_mb: u64,
    row_count: u64,
    growth_rate_mb_per_day: f64,
    has_time_column: bool,
) -> PartitioningRecommendation {
    let should_partition = table_size_mb > 10_000 // >10GB
        || (table_size_mb > 5_000 && growth_rate_mb_per_day > 100.0)
        || row_count > 100_000_000; // >100M rows

    let strategy = if should_partition && has_time_column {
        "range_by_time".to_string()
    } else if should_partition && row_count > 100_000_000 {
        "hash_by_id".to_string()
    } else if should_partition {
        "list_by_category".to_string()
    } else {
        "no_partitioning".to_string()
    };

    // Aim for partitions of 1-5GB each
    let recommended_partition_size_mb = if table_size_mb > 50_000 {
        5_000 // 5GB partitions for very large tables
    } else if table_size_mb > 20_000 {
        2_000 // 2GB partitions
    } else {
        1_000 // 1GB partitions
    };

    let mut reasoning = Vec::new();

    if should_partition {
        reasoning.push(format!(
            "Table size ({} MB) exceeds partitioning threshold",
            table_size_mb
        ));

        match strategy.as_str() {
            "range_by_time" => {
                reasoning.push("Recommend RANGE partitioning by timestamp".to_string());
                reasoning.push("Enables efficient data archiving and query pruning".to_string());
                if growth_rate_mb_per_day > 100.0 {
                    let days_per_partition =
                        recommended_partition_size_mb as f64 / growth_rate_mb_per_day;
                    reasoning.push(format!(
                        "Suggested partition interval: {:.0} days",
                        days_per_partition
                    ));
                }
            }
            "hash_by_id" => {
                reasoning
                    .push("Recommend HASH partitioning by ID for load distribution".to_string());
                reasoning.push("Enables parallel query execution across partitions".to_string());
                let partition_count = (table_size_mb / recommended_partition_size_mb).max(4);
                reasoning.push(format!("Suggested partition count: {}", partition_count));
            }
            "list_by_category" => {
                reasoning.push("Recommend LIST partitioning by category/status".to_string());
                reasoning.push("Enables partition pruning for filtered queries".to_string());
            }
            _ => {}
        }

        reasoning.push(format!(
            "Target partition size: {} MB",
            recommended_partition_size_mb
        ));
    } else {
        reasoning.push("Table size does not require partitioning yet".to_string());
        if table_size_mb > 5_000 {
            reasoning.push("Monitor growth - may need partitioning soon".to_string());
        }
    }

    PartitioningRecommendation {
        should_partition,
        strategy,
        recommended_partition_size_mb,
        reasoning,
    }
}

/// Parallel query configuration
#[derive(Debug, Clone)]
pub struct ParallelQueryConfig {
    /// Recommended max_parallel_workers_per_gather
    pub max_parallel_workers_per_gather: i32,
    /// Recommended max_parallel_workers
    pub max_parallel_workers: i32,
    /// Recommended parallel_tuple_cost
    pub parallel_tuple_cost: f64,
    /// Configuration reasoning
    pub recommendations: Vec<String>,
}

/// Calculate optimal parallel query settings
///
/// Determines optimal PostgreSQL parallel query configuration based on
/// hardware resources and workload characteristics.
///
/// # Arguments
///
/// * `cpu_cores` - Number of CPU cores available
/// * `concurrent_connections` - Expected concurrent connections
/// * `avg_query_rows` - Average rows returned per query
/// * `workload_type` - Type of workload ("oltp", "olap", "mixed")
///
/// # Returns
///
/// Parallel query configuration recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_parallel_query_config;
///
/// let config = calculate_postgres_parallel_query_config(16, 50, 100_000, "olap");
/// println!("Max parallel workers per gather: {}", config.max_parallel_workers_per_gather);
/// println!("Max parallel workers: {}", config.max_parallel_workers);
/// ```
pub fn calculate_postgres_parallel_query_config(
    cpu_cores: i32,
    concurrent_connections: i32,
    avg_query_rows: u64,
    workload_type: &str,
) -> ParallelQueryConfig {
    // OLTP: minimize parallelism (fast, small queries)
    // OLAP: maximize parallelism (complex, large queries)
    // Mixed: balanced approach

    let max_parallel_workers_per_gather = match workload_type {
        "oltp" => 2.min(cpu_cores / 4),   // Conservative for OLTP
        "olap" => (cpu_cores / 2).min(8), // Aggressive for OLAP
        _ => (cpu_cores / 3).min(4),      // Balanced for mixed
    };

    let max_parallel_workers = match workload_type {
        "oltp" => (cpu_cores / 2).max(4),
        "olap" => cpu_cores.min(16),
        _ => (cpu_cores * 2 / 3).max(8),
    };

    // Adjust parallel_tuple_cost based on query size
    // Higher cost = less likely to use parallelism
    let parallel_tuple_cost = if avg_query_rows > 1_000_000 {
        0.01 // Encourage parallelism for large result sets
    } else if avg_query_rows > 100_000 {
        0.05 // Default
    } else {
        0.1 // Discourage parallelism for small queries
    };

    let mut recommendations = Vec::new();

    recommendations.push(format!(
        "Workload type: {} - optimized for {}",
        workload_type,
        match workload_type {
            "oltp" => "transaction throughput",
            "olap" => "analytical query performance",
            _ => "mixed workload",
        }
    ));

    if concurrent_connections > cpu_cores * 2 {
        recommendations.push(
            "High connection count - conservative parallelism to avoid contention".to_string(),
        );
    }

    recommendations.push(format!(
        "Set max_parallel_workers_per_gather = {}",
        max_parallel_workers_per_gather
    ));
    recommendations.push(format!(
        "Set max_parallel_workers = {}",
        max_parallel_workers
    ));
    recommendations.push(format!(
        "Set parallel_tuple_cost = {:.3}",
        parallel_tuple_cost
    ));

    if workload_type == "olap" {
        recommendations
            .push("Consider also setting: max_parallel_maintenance_workers = 4".to_string());
        recommendations
            .push("Consider: min_parallel_table_scan_size = 8MB for smaller tables".to_string());
    }

    ParallelQueryConfig {
        max_parallel_workers_per_gather,
        max_parallel_workers,
        parallel_tuple_cost,
        recommendations,
    }
}

/// Memory tuning recommendation
#[derive(Debug, Clone)]
pub struct MemoryTuningRecommendation {
    /// Recommended work_mem (MB)
    pub work_mem_mb: i32,
    /// Recommended maintenance_work_mem (MB)
    pub maintenance_work_mem_mb: i32,
    /// Recommended effective_cache_size (MB)
    pub effective_cache_size_mb: i32,
    /// Tuning recommendations
    pub recommendations: Vec<String>,
}

/// Calculate PostgreSQL memory tuning recommendations
///
/// Provides memory allocation recommendations across PostgreSQL memory
/// parameters based on available RAM and workload characteristics.
///
/// # Arguments
///
/// * `total_ram_mb` - Total system RAM in MB
/// * `shared_buffers_mb` - Configured shared_buffers in MB
/// * `max_connections` - Maximum number of connections
/// * `workload_type` - Type of workload ("oltp", "olap", "mixed")
///
/// # Returns
///
/// Memory tuning recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_memory_tuning;
///
/// let tuning = calculate_postgres_memory_tuning(16_384, 4_096, 100, "mixed");
/// println!("Recommended work_mem: {} MB", tuning.work_mem_mb);
/// println!("Recommended maintenance_work_mem: {} MB", tuning.maintenance_work_mem_mb);
/// ```
pub fn calculate_postgres_memory_tuning(
    total_ram_mb: i32,
    shared_buffers_mb: i32,
    max_connections: i32,
    workload_type: &str,
) -> MemoryTuningRecommendation {
    // Calculate available RAM after shared_buffers and OS overhead
    let os_overhead = (total_ram_mb as f64 * 0.15) as i32; // 15% for OS
    let available_for_work = total_ram_mb - shared_buffers_mb - os_overhead;

    // work_mem: memory for sorts and hash tables per operation
    let work_mem_mb = match workload_type {
        "oltp" => {
            // OLTP: smaller work_mem, more connections
            (available_for_work / (max_connections * 4)).clamp(4, 64)
        }
        "olap" => {
            // OLAP: larger work_mem, fewer concurrent operations
            (available_for_work / (max_connections / 2)).clamp(64, 512)
        }
        _ => {
            // Mixed: balanced
            (available_for_work / (max_connections * 2)).clamp(16, 128)
        }
    };

    // maintenance_work_mem: for VACUUM, CREATE INDEX, etc.
    let maintenance_work_mem_mb = match workload_type {
        "olap" => (total_ram_mb / 8).clamp(256, 2048), // Higher for OLAP
        _ => (total_ram_mb / 16).clamp(64, 1024),
    };

    // effective_cache_size: hint to planner about OS cache
    let effective_cache_size_mb = ((total_ram_mb as f64 * 0.75) as i32).max(shared_buffers_mb * 2);

    let mut recommendations = Vec::new();

    recommendations.push(format!("Total RAM: {} MB", total_ram_mb));
    recommendations.push(format!("Shared buffers: {} MB", shared_buffers_mb));
    recommendations.push(format!("Available for work: {} MB", available_for_work));

    recommendations.push(format!("Set work_mem = {}MB", work_mem_mb));
    recommendations.push(format!(
        "Set maintenance_work_mem = {}MB",
        maintenance_work_mem_mb
    ));
    recommendations.push(format!(
        "Set effective_cache_size = {}MB",
        effective_cache_size_mb
    ));

    if work_mem_mb < 16 {
        recommendations
            .push("WARNING: work_mem is low - may cause disk spills during sorts".to_string());
    }

    if max_connections > 200 && work_mem_mb > 32 {
        recommendations.push("WARNING: High connections * high work_mem may cause OOM".to_string());
        recommendations.push(format!(
            "Potential memory usage: {} MB",
            max_connections * work_mem_mb * 4
        ));
    }

    if workload_type == "olap" {
        recommendations.push(
            "OLAP workload: Consider temporary memory increase for complex queries".to_string(),
        );
        recommendations.push("Use SET work_mem = '256MB' for specific heavy queries".to_string());
    }

    MemoryTuningRecommendation {
        work_mem_mb,
        maintenance_work_mem_mb,
        effective_cache_size_mb,
        recommendations,
    }
}

/// Disk I/O optimization recommendation
#[derive(Debug, Clone)]
pub struct DiskIoRecommendation {
    /// Recommended random_page_cost
    pub random_page_cost: f64,
    /// Recommended seq_page_cost
    pub seq_page_cost: f64,
    /// Recommended effective_io_concurrency
    pub effective_io_concurrency: i32,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

/// Calculate disk I/O optimization settings
///
/// Provides PostgreSQL I/O parameter recommendations based on storage type
/// and performance characteristics.
///
/// # Arguments
///
/// * `storage_type` - Type of storage ("ssd", "nvme", "hdd", "raid")
/// * `iops` - Available IOPS (approximate)
/// * `is_cloud` - Whether running on cloud infrastructure
///
/// # Returns
///
/// Disk I/O optimization recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_postgres_disk_io_config;
///
/// let config = calculate_postgres_disk_io_config("nvme", 100_000, false);
/// println!("Random page cost: {:.2}", config.random_page_cost);
/// println!("Effective IO concurrency: {}", config.effective_io_concurrency);
/// ```
pub fn calculate_postgres_disk_io_config(
    storage_type: &str,
    iops: u32,
    is_cloud: bool,
) -> DiskIoRecommendation {
    let (random_page_cost, seq_page_cost, effective_io_concurrency) = match storage_type {
        "nvme" => (1.1, 1.0, 200), // NVMe: very low latency
        "ssd" => (1.5, 1.0, 100),  // SSD: low latency
        "raid" => (2.0, 1.0, 50),  // RAID: moderate improvement
        "hdd" => (4.0, 1.0, 2),    // HDD: default, high seek penalty
        _ => (4.0, 1.0, 1),        // Unknown: conservative
    };

    // Adjust for cloud environments (often have variable I/O)
    let (random_page_cost, effective_io_concurrency) = if is_cloud {
        (random_page_cost * 1.2, effective_io_concurrency / 2)
    } else {
        (random_page_cost, effective_io_concurrency)
    };

    // Adjust based on IOPS if provided
    let effective_io_concurrency = if iops > 50_000 {
        effective_io_concurrency.max(200)
    } else if iops > 10_000 {
        effective_io_concurrency.max(100)
    } else if iops > 1_000 {
        effective_io_concurrency.max(50)
    } else {
        effective_io_concurrency.min(10)
    };

    let mut recommendations = Vec::new();

    recommendations.push(format!("Storage type: {}", storage_type));
    recommendations.push(format!("Approximate IOPS: {}", iops));

    recommendations.push(format!("Set random_page_cost = {:.1}", random_page_cost));
    recommendations.push(format!("Set seq_page_cost = {:.1}", seq_page_cost));
    recommendations.push(format!(
        "Set effective_io_concurrency = {}",
        effective_io_concurrency
    ));

    match storage_type {
        "nvme" | "ssd" => {
            recommendations
                .push("SSD detected: Optimized for low-latency random access".to_string());
            recommendations.push("Lower random_page_cost encourages index usage".to_string());
        }
        "hdd" => {
            recommendations
                .push("HDD detected: Sequential scans preferred for large result sets".to_string());
            recommendations.push("Consider upgrading to SSD for better performance".to_string());
        }
        _ => {}
    }

    if is_cloud {
        recommendations
            .push("Cloud environment: Settings adjusted for I/O variability".to_string());
        recommendations.push("Monitor I/O performance and adjust if needed".to_string());
    }

    if effective_io_concurrency > 100 {
        recommendations.push("High I/O concurrency enables parallel bitmap heap scans".to_string());
    }

    DiskIoRecommendation {
        random_page_cost,
        seq_page_cost,
        effective_io_concurrency,
        recommendations,
    }
}

/// PostgreSQL configuration validation result
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigValidationResult {
    /// Overall validation status
    pub is_optimal: bool,
    /// Configuration warnings
    pub warnings: Vec<String>,
    /// Configuration errors (critical issues)
    pub errors: Vec<String>,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
    /// Validation score (0.0 = poor, 1.0 = excellent)
    pub score: f64,
}

/// Validate PostgreSQL configuration for CeleRS workloads
///
/// Checks if PostgreSQL is optimally configured for queue workloads.
/// Returns validation result with warnings, errors, and recommendations.
///
/// # Arguments
///
/// * `shared_buffers_mb` - shared_buffers setting in MB
/// * `work_mem_mb` - work_mem setting in MB
/// * `max_connections` - max_connections setting
/// * `autovacuum_enabled` - Whether autovacuum is enabled
/// * `effective_cache_size_mb` - effective_cache_size in MB
/// * `total_ram_gb` - Total system RAM in GB
///
/// # Example
///
/// ```
/// use celers_broker_postgres::utilities::validate_postgres_config;
///
/// let validation = validate_postgres_config(
///     256,    // shared_buffers: 256MB
///     16,     // work_mem: 16MB
///     100,    // max_connections: 100
///     true,   // autovacuum enabled
///     1024,   // effective_cache_size: 1GB
///     8.0,    // total RAM: 8GB
/// );
///
/// if !validation.is_optimal {
///     for warning in &validation.warnings {
///         println!("Warning: {}", warning);
///     }
///     for error in &validation.errors {
///         println!("Error: {}", error);
///     }
/// }
///
/// println!("Configuration score: {:.2}/1.0", validation.score);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn validate_postgres_config(
    shared_buffers_mb: usize,
    work_mem_mb: usize,
    max_connections: usize,
    autovacuum_enabled: bool,
    effective_cache_size_mb: usize,
    total_ram_gb: f64,
) -> ConfigValidationResult {
    let mut warnings = Vec::new();
    let mut errors = Vec::new();
    let mut recommendations = Vec::new();
    let mut score: f64 = 1.0;

    let total_ram_mb = (total_ram_gb * 1024.0) as usize;

    // Check shared_buffers
    let optimal_shared_buffers = (total_ram_mb as f64 * 0.25) as usize;
    if shared_buffers_mb < optimal_shared_buffers / 2 {
        errors.push(format!(
            "shared_buffers ({} MB) is too low. Recommended: {} MB (25% of RAM)",
            shared_buffers_mb, optimal_shared_buffers
        ));
        score -= 0.2;
    } else if shared_buffers_mb < optimal_shared_buffers {
        warnings.push(format!(
            "shared_buffers ({} MB) is below optimal. Consider increasing to {} MB",
            shared_buffers_mb, optimal_shared_buffers
        ));
        score -= 0.1;
    } else if shared_buffers_mb > total_ram_mb / 2 {
        warnings.push(format!(
            "shared_buffers ({} MB) is too high (>50% RAM). May cause memory pressure",
            shared_buffers_mb
        ));
        score -= 0.1;
    }

    // Check work_mem
    let max_work_mem_total = total_ram_mb / max_connections;
    if work_mem_mb * max_connections > total_ram_mb {
        errors.push(format!(
            "work_mem ({} MB) * max_connections ({}) exceeds RAM. Risk of OOM!",
            work_mem_mb, max_connections
        ));
        score -= 0.3;
    } else if work_mem_mb < 4 {
        warnings.push(format!(
            "work_mem ({} MB) is very low. May cause excessive disk sorting",
            work_mem_mb
        ));
        score -= 0.1;
    } else if work_mem_mb > max_work_mem_total {
        warnings.push(format!(
            "work_mem ({} MB) is high. With {} connections, could use {}+ MB RAM",
            work_mem_mb,
            max_connections,
            work_mem_mb * max_connections
        ));
        score -= 0.05;
    }

    // Check max_connections
    if max_connections < 20 {
        warnings.push(format!(
            "max_connections ({}) is very low. May limit worker parallelism",
            max_connections
        ));
        score -= 0.1;
    } else if max_connections > 500 {
        warnings.push(format!(
            "max_connections ({}) is very high. Consider using connection pooling (PgBouncer)",
            max_connections
        ));
        score -= 0.1;
    }

    // Check autovacuum
    if !autovacuum_enabled {
        errors.push(
            "autovacuum is disabled! Queue tables WILL bloat. Enable autovacuum immediately"
                .to_string(),
        );
        score -= 0.4;
    } else {
        recommendations.push(
            "Autovacuum is enabled. Good! Consider aggressive settings for queue tables"
                .to_string(),
        );
    }

    // Check effective_cache_size
    let optimal_cache_size = (total_ram_mb as f64 * 0.75) as usize;
    if effective_cache_size_mb < optimal_cache_size / 2 {
        warnings.push(format!(
            "effective_cache_size ({} MB) is too low. Recommended: {} MB (75% of RAM)",
            effective_cache_size_mb, optimal_cache_size
        ));
        score -= 0.1;
    } else if effective_cache_size_mb > total_ram_mb {
        warnings.push(format!(
            "effective_cache_size ({} MB) exceeds total RAM ({} MB)",
            effective_cache_size_mb, total_ram_mb
        ));
        score -= 0.05;
    }

    // Additional recommendations for queue workloads
    recommendations.push("For queue tables, set autovacuum_vacuum_scale_factor = 0.01".to_string());
    recommendations
        .push("Set checkpoint_completion_target = 0.9 for write-heavy workloads".to_string());
    recommendations.push(
        "Enable synchronous_commit = off for better write throughput (if durability allows)"
            .to_string(),
    );
    recommendations.push("Consider wal_buffers = 16MB for high write workloads".to_string());

    score = score.clamp(0.0, 1.0);

    ConfigValidationResult {
        is_optimal: score >= 0.8 && errors.is_empty(),
        warnings,
        errors,
        recommendations,
        score,
    }
}

/// Query execution plan analysis result
#[derive(Debug, Clone)]
pub struct QueryPlanAnalysis {
    /// Estimated query cost
    pub estimated_cost: f64,
    /// Estimated rows returned
    pub estimated_rows: u64,
    /// Whether query uses index scan
    pub uses_index_scan: bool,
    /// Whether query uses sequential scan
    pub uses_seq_scan: bool,
    /// Whether query could benefit from an index
    pub needs_index: bool,
    /// Performance issues identified
    pub issues: Vec<String>,
    /// Optimization suggestions
    pub suggestions: Vec<String>,
}

/// Analyze a query execution plan for performance issues
///
/// Parses EXPLAIN output (simplified) to identify common performance problems.
/// This is a heuristic-based analysis, not a full EXPLAIN parser.
///
/// # Arguments
///
/// * `table_rows` - Number of rows in the main table
/// * `where_selectivity` - Estimated selectivity of WHERE clause (0.0 to 1.0)
/// * `has_index_on_filter` - Whether an index exists for filter columns
/// * `joins_count` - Number of joins in the query
///
/// # Example
///
/// ```
/// use celers_broker_postgres::utilities::analyze_query_plan;
///
/// let analysis = analyze_query_plan(
///     1_000_000,  // 1M rows in table
///     0.01,       // WHERE clause returns 1% of rows
///     false,      // No index on filter column
///     0,          // No joins
/// );
///
/// if analysis.needs_index {
///     println!("Query would benefit from an index!");
/// }
///
/// for issue in &analysis.issues {
///     println!("Issue: {}", issue);
/// }
/// ```
#[allow(clippy::too_many_arguments)]
pub fn analyze_query_plan(
    table_rows: u64,
    where_selectivity: f64,
    has_index_on_filter: bool,
    joins_count: usize,
) -> QueryPlanAnalysis {
    let mut issues = Vec::new();
    let mut suggestions = Vec::new();

    let expected_result_rows = (table_rows as f64 * where_selectivity) as u64;

    // Determine if index scan or seq scan would be used
    let uses_index_scan = has_index_on_filter && where_selectivity < 0.05;
    let uses_seq_scan = !uses_index_scan;

    // Estimate cost (simplified PostgreSQL cost model)
    let seq_scan_cost = table_rows as f64 * 1.0; // seq_page_cost = 1.0
    let index_scan_cost = if has_index_on_filter {
        expected_result_rows as f64 * 4.0 // random_page_cost = 4.0 for HDD
    } else {
        f64::MAX
    };

    let estimated_cost = if uses_index_scan {
        index_scan_cost
    } else {
        seq_scan_cost
    };

    // Check for performance issues
    let mut needs_index = false;

    if uses_seq_scan && table_rows > 100_000 && where_selectivity < 0.1 {
        issues.push(format!(
            "Sequential scan on large table ({} rows) with selective filter ({:.1}%)",
            table_rows,
            where_selectivity * 100.0
        ));
        needs_index = true;
        suggestions.push(format!(
            "Create an index on filter columns. Expected speedup: {:.0}x",
            seq_scan_cost / index_scan_cost.max(1.0)
        ));
    }

    if !has_index_on_filter && table_rows > 10_000 && where_selectivity < 0.5 {
        suggestions.push("Consider adding an index for better query performance".to_string());
        needs_index = true;
    }

    if joins_count > 3 {
        issues.push(format!(
            "Complex query with {} joins. May benefit from optimization",
            joins_count
        ));
        suggestions.push("Review join order and consider indexes on join columns".to_string());
        suggestions.push("Use EXPLAIN ANALYZE to verify join strategy".to_string());
    }

    if expected_result_rows > 100_000 {
        suggestions.push(format!(
            "Large result set ({} rows). Consider pagination or filtering",
            expected_result_rows
        ));
    }

    if where_selectivity > 0.5 && has_index_on_filter {
        suggestions.push(
            "Index may not be beneficial for low selectivity. Sequential scan might be faster"
                .to_string(),
        );
    }

    QueryPlanAnalysis {
        estimated_cost,
        estimated_rows: expected_result_rows,
        uses_index_scan,
        uses_seq_scan,
        needs_index,
        issues,
        suggestions,
    }
}

/// Compression recommendation for task results
#[derive(Debug, Clone, PartialEq)]
pub struct CompressionRecommendation {
    /// Whether compression is recommended
    pub should_compress: bool,
    /// Estimated compression ratio (0.0-1.0, lower is better)
    pub estimated_ratio: f64,
    /// Estimated size after compression in bytes
    pub estimated_compressed_size: usize,
    /// Compression algorithm recommendation
    pub recommended_algorithm: String,
    /// Reason for recommendation
    pub reason: String,
}

/// Calculate compression recommendation for task results
///
/// # Arguments
///
/// * `payload_size` - Size of the payload in bytes
/// * `payload_type` - Type of payload ("json", "binary", "text", etc.)
/// * `frequency` - How often this task type is executed (tasks/hour)
///
/// # Returns
///
/// Compression recommendation with estimated savings
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_compression_recommendation;
///
/// let rec = calculate_compression_recommendation(10240, "json", 1000);
/// assert!(rec.estimated_ratio > 0.0 && rec.estimated_ratio <= 1.0);
/// if rec.should_compress {
///     println!("Compression recommended: {}", rec.reason);
/// }
/// ```
pub fn calculate_compression_recommendation(
    payload_size: usize,
    payload_type: &str,
    frequency: usize,
) -> CompressionRecommendation {
    // Compression is generally beneficial for payloads > 1KB
    let size_threshold = 1024;

    // Estimate compression ratio based on payload type
    let estimated_ratio = match payload_type.to_lowercase().as_str() {
        "json" | "xml" | "text" => 0.3, // Text data compresses well (70% reduction)
        "binary" | "image" | "video" => 0.9, // Already compressed
        "protobuf" | "msgpack" => 0.7,  // Binary formats, some compression possible
        _ => 0.5,                       // Unknown, assume moderate compression
    };

    let estimated_compressed_size = (payload_size as f64 * estimated_ratio) as usize;
    let savings_bytes = payload_size - estimated_compressed_size;
    let daily_savings = savings_bytes * frequency * 24;

    let should_compress = payload_size > size_threshold && estimated_ratio < 0.8;

    let recommended_algorithm = if payload_size > 100_000 {
        "zstd".to_string() // Best compression for large payloads
    } else if payload_type == "json" || payload_type == "text" {
        "gzip".to_string() // Good balance for text
    } else {
        "lz4".to_string() // Fast compression for smaller payloads
    };

    let reason = if !should_compress && payload_size <= size_threshold {
        format!(
            "Payload too small ({} bytes), compression overhead not worth it",
            payload_size
        )
    } else if !should_compress && estimated_ratio >= 0.8 {
        format!(
            "Payload type '{}' doesn't compress well ({}% reduction)",
            payload_type,
            (1.0 - estimated_ratio) * 100.0
        )
    } else {
        format!(
            "Compression recommended: save ~{} bytes/task (~{} MB/day at {} tasks/hour)",
            savings_bytes,
            daily_savings / 1_000_000,
            frequency
        )
    };

    CompressionRecommendation {
        should_compress,
        estimated_ratio,
        estimated_compressed_size,
        recommended_algorithm,
        reason,
    }
}

/// Query performance regression analysis
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPerformanceRegression {
    /// Query identifier or pattern
    pub query_id: String,
    /// Current execution time in milliseconds
    pub current_time_ms: f64,
    /// Baseline execution time in milliseconds
    pub baseline_time_ms: f64,
    /// Regression percentage (positive means slower)
    pub regression_percent: f64,
    /// Whether this is a significant regression
    pub is_regression: bool,
    /// Severity level (none, minor, moderate, severe)
    pub severity: String,
    /// Recommended actions
    pub recommendations: Vec<String>,
}

/// Detect query performance regression
///
/// # Arguments
///
/// * `query_id` - Identifier for the query
/// * `current_time_ms` - Current average execution time
/// * `baseline_time_ms` - Baseline (historical) execution time
/// * `threshold_percent` - Regression threshold percentage (e.g., 20.0 for 20%)
///
/// # Returns
///
/// Performance regression analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::detect_query_performance_regression;
///
/// let analysis = detect_query_performance_regression(
///     "dequeue_batch",
///     150.0,  // Current: 150ms
///     100.0,  // Baseline: 100ms
///     20.0    // 20% threshold
/// );
///
/// assert!(analysis.is_regression);
/// assert_eq!(analysis.severity, "moderate");
/// assert!(analysis.regression_percent > 20.0);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn detect_query_performance_regression(
    query_id: &str,
    current_time_ms: f64,
    baseline_time_ms: f64,
    threshold_percent: f64,
) -> QueryPerformanceRegression {
    let regression_percent = if baseline_time_ms > 0.0 {
        ((current_time_ms - baseline_time_ms) / baseline_time_ms) * 100.0
    } else {
        0.0
    };

    let is_regression = regression_percent > threshold_percent;

    let severity = if regression_percent < threshold_percent {
        "none".to_string()
    } else if regression_percent < 50.0 {
        "minor".to_string()
    } else if regression_percent < 100.0 {
        "moderate".to_string()
    } else {
        "severe".to_string()
    };

    let mut recommendations = Vec::new();

    if is_regression {
        recommendations.push(format!(
            "Query '{}' is {}% slower than baseline ({:.1}ms vs {:.1}ms)",
            query_id, regression_percent as i32, current_time_ms, baseline_time_ms
        ));

        if regression_percent > 50.0 {
            recommendations.push("Run ANALYZE on affected tables".to_string());
            recommendations.push("Check for missing or bloated indexes".to_string());
        }

        if regression_percent > 100.0 {
            recommendations.push("Consider VACUUM FULL on affected tables".to_string());
            recommendations.push("Review recent schema changes".to_string());
            recommendations.push("Check for lock contention or concurrent queries".to_string());
        }

        recommendations.push("Run EXPLAIN ANALYZE to identify bottlenecks".to_string());
    }

    QueryPerformanceRegression {
        query_id: query_id.to_string(),
        current_time_ms,
        baseline_time_ms,
        regression_percent,
        is_regression,
        severity,
        recommendations,
    }
}

/// Task execution metrics
#[derive(Debug, Clone, PartialEq)]
pub struct TaskExecutionMetrics {
    /// Task name
    pub task_name: String,
    /// Average CPU time in milliseconds
    pub avg_cpu_time_ms: f64,
    /// Average memory usage in bytes
    pub avg_memory_bytes: usize,
    /// Total executions
    pub total_executions: usize,
    /// Memory usage per execution
    pub memory_per_execution: f64,
    /// Whether this task is resource-intensive
    pub is_resource_intensive: bool,
    /// Optimization suggestions
    pub suggestions: Vec<String>,
}

/// Calculate task execution metrics and resource usage
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `total_cpu_time_ms` - Total CPU time across all executions
/// * `total_memory_bytes` - Total memory used across all executions
/// * `execution_count` - Number of executions
///
/// # Returns
///
/// Task execution metrics with optimization suggestions
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_task_execution_metrics;
///
/// let metrics = calculate_task_execution_metrics(
///     "heavy_task",
///     5000.0,      // 5 seconds total CPU
///     100_000_000, // 100MB total memory
///     10           // 10 executions
/// );
///
/// assert_eq!(metrics.avg_cpu_time_ms, 500.0);
/// assert_eq!(metrics.avg_memory_bytes, 10_000_000);
/// if metrics.is_resource_intensive {
///     println!("Task needs optimization: {:?}", metrics.suggestions);
/// }
/// ```
pub fn calculate_task_execution_metrics(
    task_name: &str,
    total_cpu_time_ms: f64,
    total_memory_bytes: usize,
    execution_count: usize,
) -> TaskExecutionMetrics {
    if execution_count == 0 {
        return TaskExecutionMetrics {
            task_name: task_name.to_string(),
            avg_cpu_time_ms: 0.0,
            avg_memory_bytes: 0,
            total_executions: 0,
            memory_per_execution: 0.0,
            is_resource_intensive: false,
            suggestions: vec!["No executions yet".to_string()],
        };
    }

    let avg_cpu_time_ms = total_cpu_time_ms / execution_count as f64;
    let avg_memory_bytes = total_memory_bytes / execution_count;
    let memory_per_execution = avg_memory_bytes as f64;

    // Consider a task resource-intensive if it uses > 100ms CPU or > 10MB memory
    let is_resource_intensive = avg_cpu_time_ms > 100.0 || avg_memory_bytes > 10_000_000;

    let mut suggestions = Vec::new();

    if avg_cpu_time_ms > 1000.0 {
        suggestions.push(format!(
            "High CPU usage ({:.1}ms avg). Consider optimizing algorithms or splitting task",
            avg_cpu_time_ms
        ));
    }

    if avg_memory_bytes > 100_000_000 {
        suggestions.push(format!(
            "High memory usage ({} MB avg). Consider streaming or batch processing",
            avg_memory_bytes / 1_000_000
        ));
    }

    if avg_cpu_time_ms > 100.0 && avg_cpu_time_ms <= 1000.0 {
        suggestions.push("Moderate CPU usage. Monitor for further increases".to_string());
    }

    if avg_memory_bytes > 10_000_000 && avg_memory_bytes <= 100_000_000 {
        suggestions.push("Moderate memory usage. Consider memory pooling".to_string());
    }

    if !is_resource_intensive {
        suggestions.push("Task resource usage is within normal limits".to_string());
    }

    TaskExecutionMetrics {
        task_name: task_name.to_string(),
        avg_cpu_time_ms,
        avg_memory_bytes,
        total_executions: execution_count,
        memory_per_execution,
        is_resource_intensive,
        suggestions,
    }
}

/// DLQ retry policy configuration
#[derive(Debug, Clone, PartialEq)]
pub struct DlqRetryPolicy {
    /// Maximum number of retry attempts
    pub max_retries: usize,
    /// Initial delay in seconds
    pub initial_delay_secs: u64,
    /// Maximum delay in seconds
    pub max_delay_secs: u64,
    /// Backoff multiplier (e.g., 2.0 for exponential)
    pub backoff_multiplier: f64,
    /// Whether to add jitter to prevent thundering herd
    pub use_jitter: bool,
    /// Task types this policy applies to (empty = all)
    pub task_types: Vec<String>,
}

impl Default for DlqRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_secs: 60,
            max_delay_secs: 3600,
            backoff_multiplier: 2.0,
            use_jitter: true,
            task_types: Vec::new(),
        }
    }
}

/// Calculate next retry delay for DLQ task
///
/// # Arguments
///
/// * `retry_count` - Current retry attempt (0-indexed)
/// * `policy` - DLQ retry policy configuration
///
/// # Returns
///
/// Delay in seconds until next retry
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::{calculate_dlq_retry_delay, DlqRetryPolicy};
///
/// let policy = DlqRetryPolicy::default();
///
/// let delay0 = calculate_dlq_retry_delay(0, &policy);
/// assert_eq!(delay0, 60); // Initial delay
///
/// let delay1 = calculate_dlq_retry_delay(1, &policy);
/// assert!(delay1 >= 60); // Exponential backoff with jitter (at least initial delay)
///
/// let delay3 = calculate_dlq_retry_delay(10, &policy);
/// assert!(delay3 <= 3600); // Capped at max_delay
/// ```
pub fn calculate_dlq_retry_delay(retry_count: usize, policy: &DlqRetryPolicy) -> u64 {
    if retry_count >= policy.max_retries {
        return 0; // No more retries
    }

    // Calculate exponential backoff
    let base_delay =
        policy.initial_delay_secs as f64 * policy.backoff_multiplier.powi(retry_count as i32);

    // Cap at maximum delay
    let capped_delay = base_delay.min(policy.max_delay_secs as f64);

    // Add jitter if enabled (±25% random variation)
    let final_delay = if policy.use_jitter {
        // Simulate jitter with deterministic calculation based on retry count
        let jitter_factor = 1.0 + ((retry_count % 5) as f64 / 10.0 - 0.25);
        (capped_delay * jitter_factor).max(policy.initial_delay_secs as f64)
    } else {
        capped_delay
    };

    final_delay as u64
}

/// Suggest DLQ retry policy based on task characteristics
///
/// # Arguments
///
/// * `task_type` - Type of task
/// * `avg_execution_time_ms` - Average execution time
/// * `failure_rate` - Historical failure rate (0.0-1.0)
///
/// # Returns
///
/// Recommended DLQ retry policy
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::suggest_dlq_retry_policy;
///
/// // Fast task with low failure rate
/// let policy1 = suggest_dlq_retry_policy("quick_task", 100.0, 0.01);
/// assert_eq!(policy1.initial_delay_secs, 30);
///
/// // Slow task with high failure rate
/// let policy2 = suggest_dlq_retry_policy("slow_task", 5000.0, 0.5);
/// assert_eq!(policy2.initial_delay_secs, 300);
/// assert!(policy2.max_retries <= 3);
/// ```
pub fn suggest_dlq_retry_policy(
    task_type: &str,
    avg_execution_time_ms: f64,
    failure_rate: f64,
) -> DlqRetryPolicy {
    // Adjust retry count based on failure rate
    let max_retries = if failure_rate > 0.5 {
        1 // High failure rate = few retries
    } else if failure_rate > 0.2 {
        2
    } else {
        3 // Low failure rate = more retries
    };

    // Adjust initial delay based on execution time
    let initial_delay_secs = if avg_execution_time_ms < 1000.0 {
        30 // Fast tasks = shorter delay
    } else if avg_execution_time_ms < 5000.0 {
        60
    } else {
        300 // Slow tasks = longer delay
    };

    // Adjust max delay based on task characteristics
    let max_delay_secs = if avg_execution_time_ms < 1000.0 {
        1800 // 30 minutes for fast tasks
    } else {
        7200 // 2 hours for slow tasks
    };

    DlqRetryPolicy {
        max_retries,
        initial_delay_secs,
        max_delay_secs,
        backoff_multiplier: 2.0,
        use_jitter: true,
        task_types: vec![task_type.to_string()],
    }
}

/// Connection pool advanced analytics
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionPoolAnalytics {
    /// Pool utilization percentage (0.0-1.0)
    pub utilization: f64,
    /// Average connection lifetime in seconds
    pub avg_connection_lifetime_secs: f64,
    /// Connection acquisition wait time in milliseconds
    pub avg_acquisition_wait_ms: f64,
    /// Connection churn rate (connections/minute)
    pub churn_rate: f64,
    /// Pool health status
    pub health_status: String,
    /// Warnings and issues
    pub warnings: Vec<String>,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

/// Analyze connection pool performance and health
///
/// Provides deep insights into connection pool behavior including utilization,
/// connection lifetime, acquisition wait times, and churn rate.
///
/// # Arguments
///
/// * `pool_size` - Current pool size
/// * `active_connections` - Number of active connections
/// * `idle_connections` - Number of idle connections
/// * `total_acquisitions` - Total connection acquisitions
/// * `total_releases` - Total connection releases
/// * `avg_acquisition_wait_ms` - Average wait time for acquiring connection
/// * `uptime_secs` - Pool uptime in seconds
///
/// # Returns
///
/// Advanced connection pool analytics
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::analyze_connection_pool_advanced;
///
/// let analytics = analyze_connection_pool_advanced(
///     20,     // Pool size
///     15,     // Active connections
///     5,      // Idle connections
///     10000,  // Total acquisitions
///     9500,   // Total releases
///     5.0,    // Avg acquisition wait (ms)
///     3600    // Uptime (1 hour)
/// );
///
/// assert!(analytics.utilization > 0.0);
/// assert_eq!(analytics.health_status, "healthy");
/// ```
#[allow(clippy::too_many_arguments)]
pub fn analyze_connection_pool_advanced(
    pool_size: usize,
    active_connections: usize,
    _idle_connections: usize,
    total_acquisitions: usize,
    total_releases: usize,
    avg_acquisition_wait_ms: f64,
    uptime_secs: u64,
) -> ConnectionPoolAnalytics {
    let utilization = if pool_size > 0 {
        active_connections as f64 / pool_size as f64
    } else {
        0.0
    };

    let avg_connection_lifetime_secs = if total_releases > 0 {
        uptime_secs as f64 / total_releases as f64
    } else {
        0.0
    };

    let churn_rate = if uptime_secs > 0 {
        (total_acquisitions as f64 / uptime_secs as f64) * 60.0
    } else {
        0.0
    };

    let mut warnings = Vec::new();
    let mut recommendations = Vec::new();

    // Health status determination
    let health_status = if utilization > 0.9 {
        warnings.push("Pool utilization very high (>90%)".to_string());
        recommendations.push("Increase pool size to handle peak load".to_string());
        "stressed".to_string()
    } else if utilization > 0.75 {
        warnings.push("Pool utilization high (>75%)".to_string());
        recommendations.push("Monitor for potential bottlenecks".to_string());
        "moderate".to_string()
    } else if utilization < 0.2 && pool_size > 5 {
        warnings.push("Pool underutilized (<20%)".to_string());
        recommendations.push("Consider reducing pool size to save resources".to_string());
        "underutilized".to_string()
    } else {
        "healthy".to_string()
    };

    if avg_acquisition_wait_ms > 100.0 {
        warnings.push(format!(
            "High connection acquisition wait time ({:.1}ms)",
            avg_acquisition_wait_ms
        ));
        recommendations.push("Increase pool size or optimize query performance".to_string());
    }

    if churn_rate > 100.0 {
        warnings.push(format!(
            "High connection churn rate ({:.1} conn/min)",
            churn_rate
        ));
        recommendations
            .push("Consider connection pooling strategies or longer lifetimes".to_string());
    }

    if total_acquisitions > total_releases + 10 {
        warnings.push(format!(
            "Potential connection leak detected ({} unreleased)",
            total_acquisitions - total_releases
        ));
        recommendations.push("Check for unclosed connections in application code".to_string());
    }

    ConnectionPoolAnalytics {
        utilization,
        avg_connection_lifetime_secs,
        avg_acquisition_wait_ms,
        churn_rate,
        health_status,
        warnings,
        recommendations,
    }
}

/// Task processing rate forecast
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingRateForecast {
    /// Current processing rate (tasks/hour)
    pub current_rate: f64,
    /// Forecasted rate for next hour
    pub forecasted_rate_1h: f64,
    /// Forecasted rate for next 6 hours
    pub forecasted_rate_6h: f64,
    /// Forecasted rate for next 24 hours
    pub forecasted_rate_24h: f64,
    /// Trend direction (increasing, stable, decreasing)
    pub trend: String,
    /// Confidence level (0.0-1.0)
    pub confidence: f64,
    /// Recommended worker count adjustments
    pub worker_recommendations: Vec<String>,
}

/// Forecast task processing rate based on historical data
///
/// Uses simple linear regression to predict future processing rates and
/// provides worker scaling recommendations.
///
/// # Arguments
///
/// * `recent_rates` - Recent processing rates (tasks/hour) in chronological order
/// * `current_worker_count` - Current number of workers
/// * `target_processing_time_ms` - Target processing time per task
///
/// # Returns
///
/// Processing rate forecast with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::forecast_task_processing_rate;
///
/// let recent_rates = vec![100.0, 110.0, 105.0, 115.0, 120.0];
/// let forecast = forecast_task_processing_rate(&recent_rates, 5, 100.0);
///
/// assert!(forecast.current_rate > 0.0);
/// assert!(forecast.confidence > 0.0 && forecast.confidence <= 1.0);
/// println!("Trend: {}", forecast.trend);
/// ```
pub fn forecast_task_processing_rate(
    recent_rates: &[f64],
    current_worker_count: usize,
    _target_processing_time_ms: f64,
) -> ProcessingRateForecast {
    if recent_rates.is_empty() {
        return ProcessingRateForecast {
            current_rate: 0.0,
            forecasted_rate_1h: 0.0,
            forecasted_rate_6h: 0.0,
            forecasted_rate_24h: 0.0,
            trend: "unknown".to_string(),
            confidence: 0.0,
            worker_recommendations: vec!["Insufficient data for forecast".to_string()],
        };
    }

    let current_rate = recent_rates[recent_rates.len() - 1];

    // Simple linear regression for trend
    let n = recent_rates.len() as f64;
    let avg_rate: f64 = recent_rates.iter().sum::<f64>() / n;

    let slope = if recent_rates.len() >= 2 {
        let x_values: Vec<f64> = (0..recent_rates.len()).map(|i| i as f64).collect();
        let x_mean = x_values.iter().sum::<f64>() / n;

        let numerator: f64 = x_values
            .iter()
            .zip(recent_rates.iter())
            .map(|(x, y)| (x - x_mean) * (y - avg_rate))
            .sum();

        let denominator: f64 = x_values.iter().map(|x| (x - x_mean).powi(2)).sum();

        if denominator > 0.0 {
            numerator / denominator
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Forecast future rates using linear trend
    let forecasted_rate_1h = (current_rate + slope).max(0.0);
    let forecasted_rate_6h = (current_rate + slope * 6.0).max(0.0);
    let forecasted_rate_24h = (current_rate + slope * 24.0).max(0.0);

    // Determine trend
    let trend = if slope > 5.0 {
        "increasing".to_string()
    } else if slope < -5.0 {
        "decreasing".to_string()
    } else {
        "stable".to_string()
    };

    // Calculate confidence based on variance
    let variance: f64 = recent_rates
        .iter()
        .map(|r| (r - avg_rate).powi(2))
        .sum::<f64>()
        / n;
    let std_dev = variance.sqrt();
    let coefficient_of_variation = if avg_rate > 0.0 {
        std_dev / avg_rate
    } else {
        1.0
    };

    let confidence = (1.0 - coefficient_of_variation.min(1.0)).max(0.0);

    // Worker recommendations
    let mut worker_recommendations = Vec::new();

    if trend == "increasing" && forecasted_rate_24h > current_rate * 1.5 {
        let recommended_workers =
            ((forecasted_rate_24h / current_rate) * current_worker_count as f64).ceil() as usize;
        worker_recommendations.push(format!(
            "Increasing load detected. Consider scaling to {} workers within 24h",
            recommended_workers
        ));
    } else if trend == "decreasing" && forecasted_rate_24h < current_rate * 0.5 {
        let recommended_workers =
            ((forecasted_rate_24h / current_rate) * current_worker_count as f64).ceil() as usize;
        worker_recommendations.push(format!(
            "Decreasing load detected. Can scale down to {} workers",
            recommended_workers.max(1)
        ));
    } else {
        worker_recommendations.push(format!(
            "Current worker count ({}) appropriate for stable load",
            current_worker_count
        ));
    }

    if confidence < 0.5 {
        worker_recommendations
            .push("Low confidence forecast due to high variance. Monitor closely".to_string());
    }

    ProcessingRateForecast {
        current_rate,
        forecasted_rate_1h,
        forecasted_rate_6h,
        forecasted_rate_24h,
        trend,
        confidence,
        worker_recommendations,
    }
}

/// Database overall health score
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseHealthScore {
    /// Overall health score (0.0-1.0, higher is better)
    pub overall_score: f64,
    /// Connection pool score
    pub connection_pool_score: f64,
    /// Query performance score
    pub query_performance_score: f64,
    /// Index health score
    pub index_health_score: f64,
    /// Maintenance score
    pub maintenance_score: f64,
    /// Health grade (A, B, C, D, F)
    pub health_grade: String,
    /// Critical issues
    pub critical_issues: Vec<String>,
    /// Warnings
    pub warnings: Vec<String>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Calculate overall database health score
///
/// Provides a comprehensive assessment of database health across multiple
/// dimensions: connection pool, query performance, indexes, and maintenance.
///
/// # Arguments
///
/// * `cache_hit_ratio` - Buffer cache hit ratio (0.0-1.0)
/// * `avg_query_time_ms` - Average query execution time
/// * `connection_utilization` - Connection pool utilization (0.0-1.0)
/// * `dead_tuple_ratio` - Ratio of dead tuples (0.0-1.0)
/// * `index_usage_ratio` - Ratio of queries using indexes (0.0-1.0)
/// * `last_vacuum_hours_ago` - Hours since last VACUUM
///
/// # Returns
///
/// Comprehensive database health assessment
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::calculate_database_health_score;
///
/// let health = calculate_database_health_score(
///     0.95,  // 95% cache hit ratio
///     50.0,  // 50ms avg query time
///     0.60,  // 60% pool utilization
///     0.05,  // 5% dead tuples
///     0.90,  // 90% index usage
///     12.0   // 12 hours since VACUUM
/// );
///
/// assert!(health.overall_score > 0.0 && health.overall_score <= 1.0);
/// assert!(health.health_grade.len() == 1);
/// println!("Database health: {} ({})", health.health_grade, health.overall_score);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn calculate_database_health_score(
    cache_hit_ratio: f64,
    avg_query_time_ms: f64,
    connection_utilization: f64,
    dead_tuple_ratio: f64,
    index_usage_ratio: f64,
    last_vacuum_hours_ago: f64,
) -> DatabaseHealthScore {
    // Calculate individual component scores

    // Connection pool score (optimal: 50-75% utilization)
    let connection_pool_score = if (0.5..=0.75).contains(&connection_utilization) {
        1.0
    } else if connection_utilization < 0.5 {
        connection_utilization / 0.5
    } else {
        1.0 - ((connection_utilization - 0.75) / 0.25).min(1.0)
    };

    // Query performance score (optimal: <100ms avg)
    let query_performance_score = if avg_query_time_ms < 100.0 {
        1.0
    } else if avg_query_time_ms < 500.0 {
        1.0 - ((avg_query_time_ms - 100.0) / 400.0) * 0.5
    } else {
        0.5 - ((avg_query_time_ms - 500.0) / 1000.0).min(0.5)
    };

    // Index health score
    let index_health_score = (cache_hit_ratio + index_usage_ratio) / 2.0;

    // Maintenance score (based on dead tuples and vacuum frequency)
    let vacuum_score = if last_vacuum_hours_ago < 24.0 {
        1.0
    } else if last_vacuum_hours_ago < 168.0 {
        1.0 - ((last_vacuum_hours_ago - 24.0) / 144.0) * 0.5
    } else {
        0.5
    };

    let dead_tuple_score = 1.0 - dead_tuple_ratio;
    let maintenance_score = (vacuum_score + dead_tuple_score) / 2.0;

    // Weighted overall score
    let overall_score = (connection_pool_score * 0.2)
        + (query_performance_score * 0.3)
        + (index_health_score * 0.3)
        + (maintenance_score * 0.2);

    // Health grade
    let health_grade = if overall_score >= 0.9 {
        "A".to_string()
    } else if overall_score >= 0.8 {
        "B".to_string()
    } else if overall_score >= 0.7 {
        "C".to_string()
    } else if overall_score >= 0.6 {
        "D".to_string()
    } else {
        "F".to_string()
    };

    // Identify issues and recommendations
    let mut critical_issues = Vec::new();
    let mut warnings = Vec::new();
    let mut recommendations = Vec::new();

    if cache_hit_ratio < 0.9 {
        critical_issues.push(format!(
            "Low cache hit ratio ({:.1}%). Target: >90%",
            cache_hit_ratio * 100.0
        ));
        recommendations.push("Increase shared_buffers".to_string());
    }

    if avg_query_time_ms > 500.0 {
        critical_issues.push(format!(
            "Slow average query time ({:.1}ms). Target: <100ms",
            avg_query_time_ms
        ));
        recommendations.push("Review slow queries with EXPLAIN ANALYZE".to_string());
        recommendations.push("Check for missing indexes".to_string());
    } else if avg_query_time_ms > 100.0 {
        warnings.push(format!(
            "Moderate query time ({:.1}ms). Target: <100ms",
            avg_query_time_ms
        ));
    }

    if connection_utilization > 0.9 {
        critical_issues.push(format!(
            "Connection pool nearly exhausted ({:.1}%)",
            connection_utilization * 100.0
        ));
        recommendations.push("Increase max_connections or pool size".to_string());
    } else if connection_utilization > 0.75 {
        warnings.push("High connection pool utilization".to_string());
    }

    if dead_tuple_ratio > 0.2 {
        critical_issues.push(format!(
            "High dead tuple ratio ({:.1}%)",
            dead_tuple_ratio * 100.0
        ));
        recommendations.push("Run VACUUM FULL immediately".to_string());
    } else if dead_tuple_ratio > 0.1 {
        warnings.push("Moderate dead tuple accumulation".to_string());
        recommendations.push("Schedule VACUUM soon".to_string());
    }

    if index_usage_ratio < 0.7 {
        warnings.push(format!(
            "Low index usage ({:.1}%). Many sequential scans detected",
            index_usage_ratio * 100.0
        ));
        recommendations.push("Review query patterns and add indexes".to_string());
    }

    if last_vacuum_hours_ago > 168.0 {
        critical_issues.push(format!(
            "VACUUM not run in {} days",
            (last_vacuum_hours_ago / 24.0) as i32
        ));
        recommendations.push("Enable autovacuum or schedule regular VACUUM".to_string());
    } else if last_vacuum_hours_ago > 48.0 {
        warnings.push("VACUUM should be run more frequently".to_string());
    }

    DatabaseHealthScore {
        overall_score,
        connection_pool_score,
        query_performance_score,
        index_health_score,
        maintenance_score,
        health_grade,
        critical_issues,
        warnings,
        recommendations,
    }
}

/// Batch optimization analysis
#[derive(Debug, Clone, PartialEq)]
pub struct BatchOptimizationAnalysis {
    /// Recommended batch size
    pub recommended_batch_size: usize,
    /// Current efficiency score (0.0-1.0)
    pub efficiency_score: f64,
    /// Expected throughput improvement percentage
    pub throughput_improvement_percent: f64,
    /// Expected latency reduction percentage
    pub latency_reduction_percent: f64,
    /// Memory impact assessment
    pub memory_impact: String,
    /// Network impact assessment
    pub network_impact: String,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Analyze and recommend optimal batch size for operations
///
/// Considers network latency, memory constraints, and throughput requirements
/// to determine the optimal batch size for queue operations.
///
/// # Arguments
///
/// * `current_batch_size` - Current batch size being used
/// * `avg_task_size_bytes` - Average size of a single task
/// * `tasks_per_second` - Current task processing rate
/// * `available_memory_mb` - Available memory for batching
/// * `network_latency_ms` - Network latency to database
///
/// # Returns
///
/// Batch optimization analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::utilities::analyze_batch_optimization;
///
/// let analysis = analyze_batch_optimization(
///     10,      // Current batch size
///     1024,    // 1KB per task
///     100.0,   // 100 tasks/sec
///     512,     // 512MB available memory
///     5.0      // 5ms network latency
/// );
///
/// assert!(analysis.recommended_batch_size > 0);
/// assert!(analysis.efficiency_score >= 0.0 && analysis.efficiency_score <= 1.0);
/// println!("Recommended batch size: {}", analysis.recommended_batch_size);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn analyze_batch_optimization(
    current_batch_size: usize,
    avg_task_size_bytes: usize,
    tasks_per_second: f64,
    available_memory_mb: usize,
    network_latency_ms: f64,
) -> BatchOptimizationAnalysis {
    // Calculate optimal batch size based on network latency and throughput
    let latency_optimal_batch = if network_latency_ms > 10.0 {
        ((network_latency_ms / 10.0) * 50.0) as usize
    } else {
        25
    };

    // Calculate memory-constrained batch size
    let available_memory_bytes = available_memory_mb * 1024 * 1024;
    let memory_optimal_batch = (available_memory_bytes / (avg_task_size_bytes * 10)).max(1);

    // Calculate throughput-optimal batch size
    let throughput_optimal_batch = if tasks_per_second > 100.0 {
        100
    } else if tasks_per_second > 10.0 {
        50
    } else {
        10
    };

    // Recommended batch size is the minimum of constraints
    let recommended_batch_size = latency_optimal_batch
        .min(memory_optimal_batch)
        .min(throughput_optimal_batch)
        .clamp(1, 1000);

    // Calculate efficiency score
    let size_ratio = current_batch_size as f64 / recommended_batch_size as f64;
    let efficiency_score = if size_ratio > 1.0 {
        (1.0 / size_ratio).max(0.0)
    } else {
        size_ratio
    }
    .min(1.0);

    // Calculate expected improvements
    let throughput_improvement_percent = if current_batch_size < recommended_batch_size {
        ((recommended_batch_size as f64 / current_batch_size as f64) - 1.0) * 50.0
    } else {
        0.0
    };

    let latency_reduction_percent = if current_batch_size < recommended_batch_size {
        (1.0 - (current_batch_size as f64 / recommended_batch_size as f64)) * 30.0
    } else {
        0.0
    };

    // Assess impacts
    let memory_impact = if recommended_batch_size * avg_task_size_bytes > available_memory_bytes / 2
    {
        "high".to_string()
    } else if recommended_batch_size * avg_task_size_bytes > available_memory_bytes / 4 {
        "moderate".to_string()
    } else {
        "low".to_string()
    };

    let network_impact = if network_latency_ms > 20.0 {
        "high - batching critical".to_string()
    } else if network_latency_ms > 5.0 {
        "moderate - batching beneficial".to_string()
    } else {
        "low - batching optional".to_string()
    };

    // Generate recommendations
    let mut recommendations = Vec::new();

    if current_batch_size < recommended_batch_size / 2 {
        recommendations.push(format!(
            "Increase batch size from {} to {} for better throughput",
            current_batch_size, recommended_batch_size
        ));
    } else if current_batch_size > recommended_batch_size * 2 {
        recommendations.push(format!(
            "Decrease batch size from {} to {} to reduce memory pressure",
            current_batch_size, recommended_batch_size
        ));
    } else {
        recommendations.push("Current batch size is near optimal".to_string());
    }

    if memory_impact == "high" {
        recommendations.push("Monitor memory usage closely with current batch size".to_string());
    }

    if network_latency_ms > 10.0 {
        recommendations.push(
            "High network latency detected. Batching is critical for performance".to_string(),
        );
    }

    if tasks_per_second > 200.0 && recommended_batch_size < 100 {
        recommendations
            .push("High task volume. Consider larger batches or horizontal scaling".to_string());
    }

    BatchOptimizationAnalysis {
        recommended_batch_size,
        efficiency_score,
        throughput_improvement_percent,
        latency_reduction_percent,
        memory_impact,
        network_impact,
        recommendations,
    }
}
