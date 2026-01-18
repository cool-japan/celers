//! MySQL broker utility functions
//!
//! This module provides utility functions for MySQL broker operations,
//! including batch optimization, memory estimation, and performance tuning.
//!
//! # Examples
//!
//! ```
//! use celers_broker_sql::utilities::*;
//!
//! // Calculate optimal batch size
//! let batch_size = calculate_optimal_mysql_batch_size(1000, 1024, 100);
//! println!("Recommended batch size: {}", batch_size);
//!
//! // Estimate memory usage
//! let memory = estimate_mysql_queue_memory(1000, 1024);
//! println!("Estimated memory: {} bytes", memory);
//!
//! // Calculate optimal pool size
//! let pool_size = calculate_optimal_mysql_pool_size(100, 50);
//! println!("Recommended pool size: {}", pool_size);
//! ```

use std::collections::HashMap;

/// Calculate optimal batch size for MySQL operations
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
/// use celers_broker_sql::utilities::calculate_optimal_mysql_batch_size;
///
/// let batch_size = calculate_optimal_mysql_batch_size(1000, 1024, 100);
/// assert!(batch_size > 0);
/// assert!(batch_size <= 1000);
/// ```
pub fn calculate_optimal_mysql_batch_size(
    queue_size: usize,
    avg_message_size: usize,
    target_latency_ms: u64,
) -> usize {
    // MySQL optimal batch size formula:
    // - Smaller batches for large messages (MEDIUMBLOB considerations)
    // - Larger batches for small messages
    // - Consider transaction overhead and max_allowed_packet

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

/// Estimate MySQL memory usage for queue
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
/// use celers_broker_sql::utilities::estimate_mysql_queue_memory;
///
/// let memory = estimate_mysql_queue_memory(1000, 1024);
/// assert!(memory > 0);
/// ```
pub fn estimate_mysql_queue_memory(queue_size: usize, avg_message_size: usize) -> usize {
    // MySQL InnoDB overhead per row (approximate)
    // - Row header: ~20 bytes
    // - CHAR(36) for UUID: 36 bytes
    // - Timestamps (4x): ~16 bytes
    // - Index overhead: ~40 bytes per index (4 indexes)
    // - Page overhead: ~16KB page, amortized
    let overhead_per_row = 20 + 36 + 16 + (40 * 4) + 2; // Amortized page overhead

    queue_size * (avg_message_size + overhead_per_row)
}

/// Calculate optimal number of MySQL connections for pool
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
/// use celers_broker_sql::utilities::calculate_optimal_mysql_pool_size;
///
/// let pool_size = calculate_optimal_mysql_pool_size(100, 50);
/// assert!(pool_size > 0);
/// ```
pub fn calculate_optimal_mysql_pool_size(
    expected_concurrency: usize,
    avg_operation_duration_ms: u64,
) -> usize {
    // Rule of thumb: Pool size should handle expected concurrency
    // with some buffer for spikes
    // MySQL has connection overhead, but less than PostgreSQL

    let base_size = expected_concurrency;

    // Add buffer based on operation duration
    let buffer = if avg_operation_duration_ms > 100 {
        (expected_concurrency as f64 * 0.5) as usize
    } else if avg_operation_duration_ms > 50 {
        (expected_concurrency as f64 * 0.3) as usize
    } else {
        (expected_concurrency as f64 * 0.2) as usize
    };

    // MySQL max_connections default is 151
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
/// use celers_broker_sql::utilities::estimate_mysql_queue_drain_time;
///
/// let drain_time = estimate_mysql_queue_drain_time(1000, 50.0);
/// assert_eq!(drain_time, 20.0);
/// ```
pub fn estimate_mysql_queue_drain_time(queue_size: usize, processing_rate: f64) -> f64 {
    if processing_rate > 0.0 {
        queue_size as f64 / processing_rate
    } else {
        f64::INFINITY
    }
}

/// Suggest MySQL query optimization strategy
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
/// use celers_broker_sql::utilities::suggest_mysql_query_strategy;
///
/// let strategy = suggest_mysql_query_strategy(100, "write");
/// assert!(!strategy.is_empty());
/// ```
pub fn suggest_mysql_query_strategy(operation_count: usize, operation_type: &str) -> String {
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
            "Use chunked transactions of {} operations each (total: {} chunks) with bulk INSERT for better performance",
            chunk_size,
            operation_count.div_ceil(chunk_size)
        )
    }
}

/// Suggest MySQL OPTIMIZE TABLE strategy
///
/// # Arguments
///
/// * `table_fragmentation_percent` - Table fragmentation percentage (0-100)
/// * `table_size_mb` - Table size in megabytes
///
/// # Returns
///
/// Recommended OPTIMIZE strategy
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::suggest_mysql_optimize_strategy;
///
/// let strategy = suggest_mysql_optimize_strategy(25.0, 100.0);
/// assert!(strategy.contains("OPTIMIZE"));
/// ```
pub fn suggest_mysql_optimize_strategy(
    table_fragmentation_percent: f64,
    table_size_mb: f64,
) -> String {
    if table_fragmentation_percent > 50.0 {
        "OPTIMIZE TABLE recommended - high fragmentation detected (will lock table)".to_string()
    } else if table_fragmentation_percent > 20.0 && table_size_mb > 1000.0 {
        "OPTIMIZE TABLE recommended - moderate fragmentation on large table (schedule during off-peak)".to_string()
    } else if table_fragmentation_percent > 10.0 {
        "OPTIMIZE TABLE recommended - low to moderate fragmentation".to_string()
    } else {
        "ANALYZE TABLE only - fragmentation is acceptable, update statistics".to_string()
    }
}

/// Suggest MySQL index strategy
///
/// # Arguments
///
/// * `index_scan_count` - Number of index scans
/// * `full_scan_count` - Number of full table scans
/// * `table_rows` - Number of rows in table
///
/// # Returns
///
/// Index recommendation
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::suggest_mysql_index_strategy;
///
/// let recommendation = suggest_mysql_index_strategy(100, 10000, 1000000);
/// assert!(!recommendation.is_empty());
/// ```
pub fn suggest_mysql_index_strategy(
    index_scan_count: u64,
    full_scan_count: u64,
    table_rows: usize,
) -> String {
    let total_scans = index_scan_count + full_scan_count;
    if total_scans == 0 {
        return "No query activity detected".to_string();
    }

    let full_scan_ratio = full_scan_count as f64 / total_scans as f64;

    if full_scan_ratio > 0.5 && table_rows > 100_000 {
        "High full table scan ratio on large table - consider adding indexes".to_string()
    } else if full_scan_ratio > 0.2 && table_rows > 1_000_000 {
        "Moderate full table scan ratio - review query patterns and consider selective indexes"
            .to_string()
    } else if index_scan_count > 0 && full_scan_ratio < 0.1 {
        "Good index usage - indexes are effective".to_string()
    } else {
        "Balanced scan pattern - current indexes appear adequate".to_string()
    }
}

/// Analyze MySQL query performance
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
/// use celers_broker_sql::utilities::analyze_mysql_query_performance;
/// use std::collections::HashMap;
///
/// let mut latencies = HashMap::new();
/// latencies.insert("enqueue".to_string(), 5.0);
/// latencies.insert("dequeue".to_string(), 10.0);
/// latencies.insert("ack".to_string(), 3.0);
///
/// let analysis = analyze_mysql_query_performance(&latencies);
/// assert!(analysis.contains_key("slowest_query"));
/// ```
pub fn analyze_mysql_query_performance(
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
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .expect("collection validated to be non-empty");

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

/// Suggest MySQL InnoDB buffer pool tuning
///
/// # Arguments
///
/// * `throughput_msg_per_sec` - Message throughput
/// * `table_size_gb` - Total table size in GB
///
/// # Returns
///
/// Recommended InnoDB buffer pool configuration
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::suggest_mysql_innodb_tuning;
///
/// let config = suggest_mysql_innodb_tuning(500.0, 10.0);
/// assert!(config.contains("innodb_buffer_pool_size"));
/// ```
pub fn suggest_mysql_innodb_tuning(throughput_msg_per_sec: f64, table_size_gb: f64) -> String {
    if table_size_gb > 50.0 && throughput_msg_per_sec > 1000.0 {
        "High load: innodb_buffer_pool_size=70% of RAM, innodb_flush_log_at_trx_commit=2 (performance mode)".to_string()
    } else if table_size_gb > 10.0 && throughput_msg_per_sec > 500.0 {
        "Moderate load: innodb_buffer_pool_size=60% of RAM, innodb_flush_log_at_trx_commit=1 (balanced)".to_string()
    } else if throughput_msg_per_sec > 100.0 {
        "Standard load: innodb_buffer_pool_size=50% of RAM, innodb_flush_log_at_trx_commit=1 (standard)".to_string()
    } else {
        "Low load: innodb_buffer_pool_size=40% of RAM, innodb_flush_log_at_trx_commit=1 (conservative)".to_string()
    }
}

/// Calculate optimal MySQL timeout values
///
/// # Arguments
///
/// * `avg_operation_ms` - Average operation duration in ms
/// * `p99_operation_ms` - 99th percentile operation duration in ms
///
/// # Returns
///
/// (connect_timeout, wait_timeout) in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::calculate_mysql_timeout_values;
///
/// let (conn_timeout, wait_timeout) = calculate_mysql_timeout_values(50.0, 200.0);
/// assert!(conn_timeout > 0);
/// assert!(wait_timeout > 0);
/// ```
pub fn calculate_mysql_timeout_values(avg_operation_ms: f64, p99_operation_ms: f64) -> (u64, u64) {
    // Connection timeout: 3x average operation time, min 5 seconds
    let connect_timeout = ((avg_operation_ms * 3.0 / 1000.0) as u64).max(5);

    // Wait timeout: 2x p99 latency, min 60 seconds, max 28800 (8 hours)
    let wait_timeout = ((p99_operation_ms * 2.0 / 1000.0) as u64).clamp(60, 28800);

    (connect_timeout, wait_timeout)
}

/// Suggest MySQL sort_buffer_size setting
///
/// # Arguments
///
/// * `avg_sort_size_mb` - Average sort operation size in MB
/// * `concurrent_workers` - Expected concurrent workers
/// * `total_ram_gb` - Total available RAM in GB
///
/// # Returns
///
/// Recommended sort_buffer_size in MB
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::suggest_mysql_sort_buffer_size;
///
/// let sort_buffer = suggest_mysql_sort_buffer_size(10.0, 20, 16.0);
/// assert!(sort_buffer > 0);
/// ```
pub fn suggest_mysql_sort_buffer_size(
    avg_sort_size_mb: f64,
    concurrent_workers: usize,
    total_ram_gb: f64,
) -> usize {
    // Rule of thumb: sort_buffer_size should accommodate average sort
    // but not consume too much memory per connection

    let ram_based = ((total_ram_gb * 1024.0 * 0.1) / concurrent_workers as f64) as usize;
    let sort_based = (avg_sort_size_mb * 1.2) as usize;

    // Use the larger of the two, but cap at reasonable limits
    // MySQL default is 256KB, max recommended is ~16MB per connection
    ram_based.max(sort_based).clamp(1, 16)
}

/// Estimate MySQL InnoDB buffer pool recommendation
///
/// # Arguments
///
/// * `total_ram_gb` - Total available RAM in GB
/// * `database_size_gb` - Total database size in GB
///
/// # Returns
///
/// Recommended InnoDB buffer pool size in MB
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::suggest_mysql_innodb_buffer_pool_size;
///
/// let buffer_pool = suggest_mysql_innodb_buffer_pool_size(32.0, 10.0);
/// assert!(buffer_pool > 0);
/// ```
pub fn suggest_mysql_innodb_buffer_pool_size(total_ram_gb: f64, database_size_gb: f64) -> usize {
    // Rule of thumb for dedicated MySQL server:
    // - 70-80% of RAM for large databases
    // - 50-70% of RAM for medium databases
    // - Consider database size

    let ram_based = if total_ram_gb >= 64.0 {
        (total_ram_gb * 1024.0 * 0.75) as usize
    } else if total_ram_gb >= 32.0 {
        (total_ram_gb * 1024.0 * 0.70) as usize
    } else if total_ram_gb >= 16.0 {
        (total_ram_gb * 1024.0 * 0.60) as usize
    } else {
        (total_ram_gb * 1024.0 * 0.50) as usize
    };

    let db_based = (database_size_gb * 1024.0 * 1.2) as usize;

    // Use the smaller of the two, capped at reasonable limits
    ram_based.min(db_based).clamp(128, 65536)
}

/// Suggest MySQL max_allowed_packet setting
///
/// # Arguments
///
/// * `max_message_size_mb` - Maximum expected message size in MB
///
/// # Returns
///
/// Recommended max_allowed_packet in MB
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::suggest_mysql_max_allowed_packet;
///
/// let max_packet = suggest_mysql_max_allowed_packet(5.0);
/// assert!(max_packet > 0);
/// ```
pub fn suggest_mysql_max_allowed_packet(max_message_size_mb: f64) -> usize {
    // max_allowed_packet should be larger than max message size
    // Add 50% buffer for overhead
    let recommended = (max_message_size_mb * 1.5) as usize;

    // MySQL default is 64MB, min 1MB, max 1GB
    recommended.clamp(1, 1024)
}

/// Query pattern analysis result
#[derive(Debug, Clone)]
pub struct QueryPatternAnalysis {
    /// Query type (SELECT, INSERT, UPDATE, DELETE)
    pub query_type: String,
    /// Execution count
    pub execution_count: u64,
    /// Average execution time (ms)
    pub avg_execution_time_ms: f64,
    /// P95 execution time (ms)
    pub p95_execution_time_ms: f64,
    /// Rows examined per execution
    pub avg_rows_examined: f64,
    /// Rows returned per execution
    pub avg_rows_returned: f64,
    /// Optimization recommendation
    pub recommendation: String,
}

/// Connection pool health metrics
#[derive(Debug, Clone)]
pub struct ConnectionPoolHealth {
    /// Total connections in pool
    pub total_connections: usize,
    /// Active connections
    pub active_connections: usize,
    /// Idle connections
    pub idle_connections: usize,
    /// Pool utilization percentage
    pub utilization_percent: f64,
    /// Average connection wait time (ms)
    pub avg_wait_time_ms: f64,
    /// Connection failures count
    pub connection_failures: u64,
    /// Health status
    pub health_status: PoolHealthStatus,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Pool health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolHealthStatus {
    /// Healthy (< 70% utilization, low wait times)
    Healthy,
    /// Warning (70-90% utilization, moderate wait times)
    Warning,
    /// Critical (> 90% utilization, high wait times)
    Critical,
}

/// Index effectiveness metrics
#[derive(Debug, Clone)]
pub struct IndexEffectiveness {
    /// Index name
    pub index_name: String,
    /// Table name
    pub table_name: String,
    /// Index scans count
    pub index_scans: u64,
    /// Full table scans that could use this index
    pub potential_usage: u64,
    /// Effectiveness score (0-100)
    pub effectiveness_score: f64,
    /// Recommendation
    pub recommendation: String,
}

/// Table bloat analysis
#[derive(Debug, Clone)]
pub struct TableBloatAnalysis {
    /// Table name
    pub table_name: String,
    /// Total table size (MB)
    pub total_size_mb: f64,
    /// Data size (MB)
    pub data_size_mb: f64,
    /// Index size (MB)
    pub index_size_mb: f64,
    /// Estimated bloat (MB)
    pub bloat_mb: f64,
    /// Bloat percentage
    pub bloat_percent: f64,
    /// Recommendation
    pub recommendation: String,
}

/// Replication lag metrics
#[derive(Debug, Clone)]
pub struct ReplicationLag {
    /// Replica server ID
    pub replica_id: String,
    /// Lag in seconds
    pub lag_seconds: f64,
    /// Replica status
    pub status: ReplicaStatus,
    /// Recommendation
    pub recommendation: String,
}

/// Replica status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaStatus {
    /// Healthy (< 1 second lag)
    Healthy,
    /// Warning (1-5 seconds lag)
    Warning,
    /// Critical (> 5 seconds lag)
    Critical,
    /// Error (replication stopped)
    Error,
}

/// Analyze query execution pattern
///
/// # Arguments
///
/// * `query_type` - Type of query (SELECT, INSERT, UPDATE, DELETE)
/// * `execution_count` - Number of times query executed
/// * `execution_times_ms` - Slice of execution times in milliseconds
/// * `rows_examined` - Slice of rows examined per execution
/// * `rows_returned` - Slice of rows returned per execution
///
/// # Returns
///
/// Query pattern analysis with optimization recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::analyze_query_pattern;
///
/// let times = vec![10.0, 15.0, 12.0, 20.0, 11.0];
/// let examined = vec![1000.0, 1200.0, 1100.0, 1500.0, 1000.0];
/// let returned = vec![10.0, 12.0, 11.0, 15.0, 10.0];
///
/// let analysis = analyze_query_pattern(
///     "SELECT",
///     5,
///     &times,
///     &examined,
///     &returned
/// );
/// assert_eq!(analysis.query_type, "SELECT");
/// assert_eq!(analysis.execution_count, 5);
/// ```
pub fn analyze_query_pattern(
    query_type: &str,
    execution_count: u64,
    execution_times_ms: &[f64],
    rows_examined: &[f64],
    rows_returned: &[f64],
) -> QueryPatternAnalysis {
    let avg_execution_time_ms = if !execution_times_ms.is_empty() {
        execution_times_ms.iter().sum::<f64>() / execution_times_ms.len() as f64
    } else {
        0.0
    };

    let mut sorted_times = execution_times_ms.to_vec();
    sorted_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p95_execution_time_ms = if !sorted_times.is_empty() {
        let index = ((95.0 / 100.0) * (sorted_times.len() - 1) as f64).round() as usize;
        sorted_times[index.min(sorted_times.len() - 1)]
    } else {
        0.0
    };

    let avg_rows_examined = if !rows_examined.is_empty() {
        rows_examined.iter().sum::<f64>() / rows_examined.len() as f64
    } else {
        0.0
    };

    let avg_rows_returned = if !rows_returned.is_empty() {
        rows_returned.iter().sum::<f64>() / rows_returned.len() as f64
    } else {
        0.0
    };

    // Calculate selectivity ratio
    let selectivity = if avg_rows_examined > 0.0 {
        avg_rows_returned / avg_rows_examined
    } else {
        1.0
    };

    let recommendation = if selectivity < 0.01 && avg_rows_examined > 10000.0 {
        "Poor selectivity: query examines many rows but returns few. Consider adding or optimizing indexes.".to_string()
    } else if avg_execution_time_ms > 1000.0 {
        "Slow query detected (>1s). Review query plan and consider optimization.".to_string()
    } else if p95_execution_time_ms > avg_execution_time_ms * 5.0 {
        "High variance in execution times. Investigate outliers and consider query cache."
            .to_string()
    } else if execution_count > 1000 && avg_execution_time_ms > 100.0 {
        "Frequently executed slow query. Prime candidate for optimization.".to_string()
    } else {
        "Query performance acceptable.".to_string()
    };

    QueryPatternAnalysis {
        query_type: query_type.to_string(),
        execution_count,
        avg_execution_time_ms,
        p95_execution_time_ms,
        avg_rows_examined,
        avg_rows_returned,
        recommendation,
    }
}

/// Analyze connection pool health
///
/// # Arguments
///
/// * `total_connections` - Total connections in pool
/// * `active_connections` - Currently active connections
/// * `avg_wait_time_ms` - Average connection acquisition wait time
/// * `connection_failures` - Number of connection failures
///
/// # Returns
///
/// Connection pool health analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::analyze_connection_pool_health;
///
/// let health = analyze_connection_pool_health(20, 15, 50.0, 5);
/// assert_eq!(health.total_connections, 20);
/// assert_eq!(health.active_connections, 15);
/// ```
pub fn analyze_connection_pool_health(
    total_connections: usize,
    active_connections: usize,
    avg_wait_time_ms: f64,
    connection_failures: u64,
) -> ConnectionPoolHealth {
    let idle_connections = total_connections.saturating_sub(active_connections);
    let utilization_percent = if total_connections > 0 {
        (active_connections as f64 / total_connections as f64) * 100.0
    } else {
        0.0
    };

    let health_status = if utilization_percent > 90.0 || avg_wait_time_ms > 100.0 {
        PoolHealthStatus::Critical
    } else if utilization_percent > 70.0 || avg_wait_time_ms > 50.0 {
        PoolHealthStatus::Warning
    } else {
        PoolHealthStatus::Healthy
    };

    let mut recommendations = Vec::new();

    if utilization_percent > 90.0 {
        recommendations.push(
            "Pool utilization is very high (>90%). Consider increasing pool size.".to_string(),
        );
    }

    if avg_wait_time_ms > 100.0 {
        recommendations.push(
            "High connection wait times (>100ms). Increase pool size or optimize query performance.".to_string()
        );
    }

    if connection_failures > 0 {
        recommendations.push(format!(
            "Connection failures detected ({}). Check network stability and MySQL max_connections.",
            connection_failures
        ));
    }

    if utilization_percent < 30.0 && total_connections > 10 {
        recommendations.push(
            "Low pool utilization (<30%). Consider reducing pool size to conserve resources."
                .to_string(),
        );
    }

    if recommendations.is_empty() {
        recommendations.push("Connection pool is healthy.".to_string());
    }

    ConnectionPoolHealth {
        total_connections,
        active_connections,
        idle_connections,
        utilization_percent,
        avg_wait_time_ms,
        connection_failures,
        health_status,
        recommendations,
    }
}

/// Analyze index effectiveness
///
/// # Arguments
///
/// * `index_name` - Name of the index
/// * `table_name` - Name of the table
/// * `index_scans` - Number of index scans
/// * `full_table_scans` - Number of full table scans
///
/// # Returns
///
/// Index effectiveness analysis
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::analyze_index_effectiveness;
///
/// let analysis = analyze_index_effectiveness(
///     "idx_tasks_state",
///     "celers_tasks",
///     10000,
///     100
/// );
/// assert_eq!(analysis.index_name, "idx_tasks_state");
/// assert!(analysis.effectiveness_score > 90.0);
/// ```
pub fn analyze_index_effectiveness(
    index_name: &str,
    table_name: &str,
    index_scans: u64,
    full_table_scans: u64,
) -> IndexEffectiveness {
    let total_scans = index_scans + full_table_scans;
    let effectiveness_score = if total_scans > 0 {
        (index_scans as f64 / total_scans as f64) * 100.0
    } else {
        0.0
    };

    let potential_usage = full_table_scans;

    let recommendation = if effectiveness_score > 90.0 {
        "Index is highly effective and well-utilized.".to_string()
    } else if effectiveness_score > 70.0 {
        "Index is moderately effective. Review query patterns for optimization opportunities."
            .to_string()
    } else if effectiveness_score > 50.0 {
        "Index has low effectiveness. Consider reviewing index design or query patterns."
            .to_string()
    } else if index_scans == 0 && full_table_scans > 1000 {
        "Index is not being used despite many table scans. Consider dropping or redesigning."
            .to_string()
    } else {
        "Index effectiveness is very low. Review if this index is needed.".to_string()
    };

    IndexEffectiveness {
        index_name: index_name.to_string(),
        table_name: table_name.to_string(),
        index_scans,
        potential_usage,
        effectiveness_score,
        recommendation,
    }
}

/// Analyze table bloat
///
/// # Arguments
///
/// * `table_name` - Name of the table
/// * `total_size_mb` - Total table size in MB
/// * `row_count` - Number of rows in table
/// * `avg_row_length_bytes` - Average row length in bytes
///
/// # Returns
///
/// Table bloat analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::analyze_table_bloat;
///
/// let analysis = analyze_table_bloat("celers_tasks", 1000.0, 500000, 2048);
/// assert_eq!(analysis.table_name, "celers_tasks");
/// assert!(analysis.total_size_mb > 0.0);
/// ```
pub fn analyze_table_bloat(
    table_name: &str,
    total_size_mb: f64,
    row_count: u64,
    avg_row_length_bytes: usize,
) -> TableBloatAnalysis {
    // Estimate expected data size
    let expected_data_mb = (row_count as f64 * avg_row_length_bytes as f64) / (1024.0 * 1024.0);

    // InnoDB overhead: ~30% for indexes, page overhead, etc.
    let expected_total_mb = expected_data_mb * 1.3;

    let bloat_mb = (total_size_mb - expected_total_mb).max(0.0);
    let bloat_percent = if expected_total_mb > 0.0 {
        (bloat_mb / expected_total_mb) * 100.0
    } else {
        0.0
    };

    // Rough estimates for data vs index split (InnoDB typically 70/30)
    let data_size_mb = total_size_mb * 0.7;
    let index_size_mb = total_size_mb * 0.3;

    let recommendation = if bloat_percent > 50.0 {
        "Significant bloat detected (>50%). Run OPTIMIZE TABLE to reclaim space.".to_string()
    } else if bloat_percent > 25.0 {
        "Moderate bloat detected (25-50%). Consider running OPTIMIZE TABLE during maintenance window.".to_string()
    } else if bloat_percent > 10.0 {
        "Low bloat detected (10-25%). Monitor and optimize if it increases.".to_string()
    } else {
        "Table bloat is within acceptable range.".to_string()
    };

    TableBloatAnalysis {
        table_name: table_name.to_string(),
        total_size_mb,
        data_size_mb,
        index_size_mb,
        bloat_mb,
        bloat_percent,
        recommendation,
    }
}

/// Analyze replication lag
///
/// # Arguments
///
/// * `replica_id` - Replica server identifier
/// * `lag_seconds` - Replication lag in seconds
/// * `io_thread_running` - Whether IO thread is running
/// * `sql_thread_running` - Whether SQL thread is running
///
/// # Returns
///
/// Replication lag analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sql::utilities::analyze_replication_lag;
///
/// let analysis = analyze_replication_lag("replica-1", 0.5, true, true);
/// assert_eq!(analysis.replica_id, "replica-1");
/// assert_eq!(analysis.lag_seconds, 0.5);
/// ```
pub fn analyze_replication_lag(
    replica_id: &str,
    lag_seconds: f64,
    io_thread_running: bool,
    sql_thread_running: bool,
) -> ReplicationLag {
    let status = if !io_thread_running || !sql_thread_running {
        ReplicaStatus::Error
    } else if lag_seconds > 5.0 {
        ReplicaStatus::Critical
    } else if lag_seconds > 1.0 {
        ReplicaStatus::Warning
    } else {
        ReplicaStatus::Healthy
    };

    let recommendation = match status {
        ReplicaStatus::Error => {
            "Replication threads are not running. Check replica configuration and logs.".to_string()
        }
        ReplicaStatus::Critical => {
            "Replication lag is critical (>5s). Check replica load, network, and binlog position."
                .to_string()
        }
        ReplicaStatus::Warning => {
            "Replication lag is elevated (1-5s). Monitor closely and investigate if it persists."
                .to_string()
        }
        ReplicaStatus::Healthy => "Replication is healthy with minimal lag (<1s).".to_string(),
    };

    ReplicationLag {
        replica_id: replica_id.to_string(),
        lag_seconds,
        status,
        recommendation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_batch_size() {
        let batch_size = calculate_optimal_mysql_batch_size(1000, 1024, 100);
        assert!(batch_size > 0);
        assert!(batch_size <= 1000);
    }

    #[test]
    fn test_calculate_optimal_batch_size_large_messages() {
        let batch_size = calculate_optimal_mysql_batch_size(1000, 200_000, 100);
        assert!(batch_size <= 20);
    }

    #[test]
    fn test_estimate_queue_memory() {
        let memory = estimate_mysql_queue_memory(1000, 1024);
        assert!(memory > 1024 * 1000);
    }

    #[test]
    fn test_calculate_optimal_pool_size() {
        let pool_size = calculate_optimal_mysql_pool_size(100, 50);
        assert!(pool_size >= 100);
        assert!(pool_size <= 200);
    }

    #[test]
    fn test_estimate_drain_time() {
        let drain_time = estimate_mysql_queue_drain_time(1000, 50.0);
        assert_eq!(drain_time, 20.0);
    }

    #[test]
    fn test_estimate_drain_time_zero_rate() {
        let drain_time = estimate_mysql_queue_drain_time(1000, 0.0);
        assert!(drain_time.is_infinite());
    }

    #[test]
    fn test_suggest_query_strategy() {
        let strategy = suggest_mysql_query_strategy(5, "write");
        assert!(strategy.contains("individually"));

        let strategy = suggest_mysql_query_strategy(50, "write");
        assert!(strategy.contains("single transaction"));

        let strategy = suggest_mysql_query_strategy(1000, "write");
        assert!(strategy.contains("chunked"));
    }

    #[test]
    fn test_suggest_optimize_strategy() {
        let strategy = suggest_mysql_optimize_strategy(60.0, 100.0);
        assert!(strategy.contains("OPTIMIZE TABLE"));

        let strategy = suggest_mysql_optimize_strategy(25.0, 1500.0);
        assert!(strategy.contains("OPTIMIZE TABLE"));

        let strategy = suggest_mysql_optimize_strategy(5.0, 100.0);
        assert!(strategy.contains("ANALYZE"));
    }

    #[test]
    fn test_suggest_index_strategy() {
        let recommendation = suggest_mysql_index_strategy(100, 10000, 1000000);
        assert!(recommendation.contains("full table scan"));

        let recommendation = suggest_mysql_index_strategy(10000, 100, 1000000);
        assert!(recommendation.contains("Good index usage"));
    }

    #[test]
    fn test_analyze_query_performance() {
        let mut latencies = HashMap::new();
        latencies.insert("enqueue".to_string(), 5.0);
        latencies.insert("dequeue".to_string(), 15.0);
        latencies.insert("ack".to_string(), 3.0);

        let analysis = analyze_mysql_query_performance(&latencies);
        assert_eq!(analysis.get("slowest_query"), Some(&"dequeue".to_string()));
        assert!(analysis.contains_key("avg_latency_ms"));
        assert!(analysis.contains_key("overall_status"));
    }

    #[test]
    fn test_suggest_innodb_tuning() {
        let config = suggest_mysql_innodb_tuning(1500.0, 60.0);
        assert!(config.contains("innodb_buffer_pool_size"));

        let config = suggest_mysql_innodb_tuning(50.0, 5.0);
        assert!(config.contains("innodb_buffer_pool_size"));
    }

    #[test]
    fn test_calculate_timeout_values() {
        let (conn_timeout, wait_timeout) = calculate_mysql_timeout_values(50.0, 200.0);
        assert!(conn_timeout >= 5);
        assert!(wait_timeout >= 60);
    }

    #[test]
    fn test_suggest_sort_buffer_size() {
        let sort_buffer = suggest_mysql_sort_buffer_size(10.0, 20, 16.0);
        assert!(sort_buffer >= 1);
        assert!(sort_buffer <= 16);
    }

    #[test]
    fn test_suggest_innodb_buffer_pool_size() {
        let buffer_pool = suggest_mysql_innodb_buffer_pool_size(32.0, 10.0);
        assert!(buffer_pool >= 128);
        assert!(buffer_pool <= 65536);
    }

    #[test]
    fn test_suggest_max_allowed_packet() {
        let max_packet = suggest_mysql_max_allowed_packet(5.0);
        assert!(max_packet >= 1);
        assert!(max_packet <= 1024);
    }

    #[test]
    fn test_analyze_query_pattern_good() {
        let times = vec![10.0, 15.0, 12.0, 20.0, 11.0];
        let examined = vec![100.0, 120.0, 110.0, 150.0, 100.0];
        let returned = vec![10.0, 12.0, 11.0, 15.0, 10.0];

        let analysis = analyze_query_pattern("SELECT", 5, &times, &examined, &returned);
        assert_eq!(analysis.query_type, "SELECT");
        assert_eq!(analysis.execution_count, 5);
        assert!(analysis.avg_execution_time_ms > 0.0);
        assert!(analysis.recommendation.contains("acceptable"));
    }

    #[test]
    fn test_analyze_query_pattern_slow() {
        let times = vec![1500.0, 1600.0, 1700.0]; // Slow queries > 1s
        let examined = vec![1000.0, 1200.0, 1100.0];
        let returned = vec![10.0, 12.0, 11.0];

        let analysis = analyze_query_pattern("SELECT", 3, &times, &examined, &returned);
        assert!(analysis.avg_execution_time_ms > 1000.0);
        assert!(analysis.recommendation.contains("Slow query"));
    }

    #[test]
    fn test_analyze_query_pattern_poor_selectivity() {
        let times = vec![100.0, 120.0, 110.0];
        let examined = vec![100000.0, 120000.0, 110000.0]; // Many rows examined
        let returned = vec![10.0, 12.0, 11.0]; // Few returned

        let analysis = analyze_query_pattern("SELECT", 3, &times, &examined, &returned);
        assert!(analysis.recommendation.contains("selectivity"));
    }

    #[test]
    fn test_analyze_connection_pool_health_healthy() {
        let health = analyze_connection_pool_health(20, 10, 20.0, 0);
        assert_eq!(health.total_connections, 20);
        assert_eq!(health.active_connections, 10);
        assert_eq!(health.idle_connections, 10);
        assert_eq!(health.utilization_percent, 50.0);
        assert_eq!(health.health_status, PoolHealthStatus::Healthy);
    }

    #[test]
    fn test_analyze_connection_pool_health_critical() {
        let health = analyze_connection_pool_health(20, 19, 150.0, 5);
        assert!(health.utilization_percent > 90.0);
        assert_eq!(health.health_status, PoolHealthStatus::Critical);
        assert!(!health.recommendations.is_empty());
    }

    #[test]
    fn test_analyze_connection_pool_health_warning() {
        let health = analyze_connection_pool_health(20, 15, 60.0, 0);
        assert_eq!(health.utilization_percent, 75.0);
        assert_eq!(health.health_status, PoolHealthStatus::Warning);
    }

    #[test]
    fn test_analyze_index_effectiveness_high() {
        let analysis = analyze_index_effectiveness("idx_tasks_state", "celers_tasks", 10000, 100);
        assert_eq!(analysis.index_name, "idx_tasks_state");
        assert_eq!(analysis.table_name, "celers_tasks");
        assert!(analysis.effectiveness_score > 90.0);
        assert!(analysis.recommendation.contains("effective"));
    }

    #[test]
    fn test_analyze_index_effectiveness_low() {
        let analysis = analyze_index_effectiveness("idx_unused", "celers_tasks", 0, 5000);
        assert_eq!(analysis.effectiveness_score, 0.0);
        assert!(analysis.recommendation.contains("not being used"));
    }

    #[test]
    fn test_analyze_table_bloat_low() {
        let analysis = analyze_table_bloat("celers_tasks", 1000.0, 500000, 2048);
        assert_eq!(analysis.table_name, "celers_tasks");
        assert!(analysis.total_size_mb > 0.0);
        assert!(analysis.bloat_percent >= 0.0);
    }

    #[test]
    fn test_analyze_table_bloat_high() {
        // Small row count but large size = high bloat
        let analysis = analyze_table_bloat("bloated_table", 5000.0, 10000, 1024);
        assert!(analysis.bloat_mb > 0.0);
        assert!(analysis.bloat_percent > 0.0);
    }

    #[test]
    fn test_analyze_replication_lag_healthy() {
        let analysis = analyze_replication_lag("replica-1", 0.5, true, true);
        assert_eq!(analysis.replica_id, "replica-1");
        assert_eq!(analysis.lag_seconds, 0.5);
        assert_eq!(analysis.status, ReplicaStatus::Healthy);
    }

    #[test]
    fn test_analyze_replication_lag_warning() {
        let analysis = analyze_replication_lag("replica-2", 3.0, true, true);
        assert_eq!(analysis.status, ReplicaStatus::Warning);
        assert!(analysis.recommendation.contains("elevated"));
    }

    #[test]
    fn test_analyze_replication_lag_critical() {
        let analysis = analyze_replication_lag("replica-3", 10.0, true, true);
        assert_eq!(analysis.status, ReplicaStatus::Critical);
        assert!(analysis.recommendation.contains("critical"));
    }

    #[test]
    fn test_analyze_replication_lag_error() {
        let analysis = analyze_replication_lag("replica-4", 0.0, false, false);
        assert_eq!(analysis.status, ReplicaStatus::Error);
        assert!(analysis.recommendation.contains("not running"));
    }
}
