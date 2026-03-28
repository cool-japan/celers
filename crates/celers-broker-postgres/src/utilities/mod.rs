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

pub mod diagnostics;
pub mod performance;

// Re-export all public items from submodules
pub use diagnostics::*;
pub use performance::*;

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
