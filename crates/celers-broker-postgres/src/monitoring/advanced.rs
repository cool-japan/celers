//! Advanced PostgreSQL monitoring and analytics
//!
//! This module provides advanced monitoring utilities for PostgreSQL-specific
//! internals including lock contention, table bloat, checkpoint performance,
//! VACUUM urgency, connection pool efficiency, cache hit ratios, table statistics
//! staleness, long-running query detection, autovacuum effectiveness, sequential
//! scan detection, index bloat analysis, and connection state analysis.

use serde::{Deserialize, Serialize};

/// Lock contention severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockContentionSeverity {
    /// Minimal or no lock contention
    Low,
    /// Moderate lock contention
    Medium,
    /// High lock contention requiring attention
    High,
    /// Critical lock contention requiring immediate action
    Critical,
}

/// Lock contention analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockContentionAnalysis {
    /// Number of active locks
    pub active_locks: usize,
    /// Number of locks waiting
    pub waiting_locks: usize,
    /// Number of deadlocks detected
    pub deadlocks_detected: usize,
    /// Lock contention ratio
    pub contention_ratio: f64,
    /// Severity level
    pub severity: LockContentionSeverity,
    /// Recommendations for addressing lock contention
    pub recommendations: Vec<String>,
}

/// Analyze PostgreSQL lock contention
///
/// Analyzes the current lock situation and provides recommendations
/// for addressing lock contention issues.
///
/// # Arguments
///
/// * `active_locks` - Number of currently active locks
/// * `waiting_locks` - Number of queries waiting for locks
/// * `deadlocks_detected` - Number of deadlocks detected
///
/// # Returns
///
/// Lock contention analysis with severity and recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::analyze_postgres_lock_contention;
///
/// let analysis = analyze_postgres_lock_contention(10, 50, 2);
/// println!("Lock severity: {:?}", analysis.severity);
/// println!("Waiting locks: {}", analysis.waiting_locks);
/// for rec in &analysis.recommendations {
///     println!("- {}", rec);
/// }
/// ```
pub fn analyze_postgres_lock_contention(
    active_locks: usize,
    waiting_locks: usize,
    deadlocks_detected: usize,
) -> LockContentionAnalysis {
    let contention_ratio = if active_locks > 0 {
        waiting_locks as f64 / active_locks as f64
    } else {
        0.0
    };

    let severity = if deadlocks_detected > 5 || contention_ratio > 5.0 {
        LockContentionSeverity::Critical
    } else if deadlocks_detected > 2 || contention_ratio > 2.0 {
        LockContentionSeverity::High
    } else if contention_ratio > 0.5 {
        LockContentionSeverity::Medium
    } else {
        LockContentionSeverity::Low
    };

    let mut recommendations = Vec::new();

    match severity {
        LockContentionSeverity::Critical => {
            recommendations
                .push("URGENT: Investigate and resolve deadlocks immediately".to_string());
            recommendations.push("Consider reducing transaction duration".to_string());
            recommendations.push("Review lock acquisition order in application code".to_string());
            recommendations.push("Enable deadlock logging: log_lock_waits = on".to_string());
        }
        LockContentionSeverity::High => {
            recommendations.push("High lock contention detected".to_string());
            recommendations.push("Consider using advisory locks for coordination".to_string());
            recommendations.push("Review and optimize long-running transactions".to_string());
            recommendations.push("Enable lock monitoring: log_lock_waits = on".to_string());
        }
        LockContentionSeverity::Medium => {
            recommendations.push("Moderate lock contention observed".to_string());
            recommendations.push("Monitor lock wait times".to_string());
            recommendations.push("Consider statement timeout configuration".to_string());
        }
        LockContentionSeverity::Low => {
            recommendations.push("Lock contention is within acceptable limits".to_string());
        }
    }

    LockContentionAnalysis {
        active_locks,
        waiting_locks,
        deadlocks_detected,
        contention_ratio,
        severity,
        recommendations,
    }
}

/// Bloat severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BloatSeverity {
    /// Minimal bloat, no action needed
    Low,
    /// Moderate bloat, consider VACUUM
    Medium,
    /// High bloat, VACUUM recommended
    High,
    /// Critical bloat, immediate VACUUM FULL may be needed
    Critical,
}

/// Table bloat analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBloatAnalysis {
    /// Total table size in bytes
    pub total_bytes: u64,
    /// Live data size in bytes
    pub live_bytes: u64,
    /// Bloat (dead tuples) in bytes
    pub bloat_bytes: u64,
    /// Bloat ratio (0.0 to 1.0)
    pub bloat_ratio: f64,
    /// Severity level
    pub severity: BloatSeverity,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Calculate PostgreSQL table bloat ratio
///
/// Analyzes table bloat and provides recommendations for VACUUM operations.
///
/// # Arguments
///
/// * `total_bytes` - Total table size in bytes
/// * `live_bytes` - Live data size in bytes
///
/// # Returns
///
/// Table bloat analysis with severity and recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::calculate_postgres_table_bloat_ratio;
///
/// let bloat = calculate_postgres_table_bloat_ratio(1_000_000, 600_000);
/// println!("Bloat ratio: {:.2}%", bloat.bloat_ratio * 100.0);
/// println!("Severity: {:?}", bloat.severity);
/// ```
pub fn calculate_postgres_table_bloat_ratio(
    total_bytes: u64,
    live_bytes: u64,
) -> TableBloatAnalysis {
    let bloat_bytes = total_bytes.saturating_sub(live_bytes);
    let bloat_ratio = if total_bytes > 0 {
        bloat_bytes as f64 / total_bytes as f64
    } else {
        0.0
    };

    let severity = if bloat_ratio > 0.5 {
        BloatSeverity::Critical
    } else if bloat_ratio > 0.3 {
        BloatSeverity::High
    } else if bloat_ratio > 0.15 {
        BloatSeverity::Medium
    } else {
        BloatSeverity::Low
    };

    let mut recommendations = Vec::new();

    match severity {
        BloatSeverity::Critical => {
            recommendations.push("CRITICAL: Table has >50% bloat".to_string());
            recommendations.push("Consider VACUUM FULL during maintenance window".to_string());
            recommendations.push("Review autovacuum settings".to_string());
            recommendations.push("Consider table partitioning for large tables".to_string());
        }
        BloatSeverity::High => {
            recommendations.push("High bloat detected (>30%)".to_string());
            recommendations.push("Run VACUUM manually or tune autovacuum".to_string());
            recommendations.push("Monitor bloat growth trends".to_string());
        }
        BloatSeverity::Medium => {
            recommendations.push("Moderate bloat (15-30%)".to_string());
            recommendations.push("Autovacuum should handle this automatically".to_string());
            recommendations.push("Monitor if bloat continues to increase".to_string());
        }
        BloatSeverity::Low => {
            recommendations.push("Bloat is within acceptable limits (<15%)".to_string());
        }
    }

    TableBloatAnalysis {
        total_bytes,
        live_bytes,
        bloat_bytes,
        bloat_ratio,
        severity,
        recommendations,
    }
}

/// Checkpoint performance analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointPerformanceAnalysis {
    /// Average time between checkpoints (seconds)
    pub checkpoint_interval_secs: f64,
    /// Average checkpoint duration (seconds)
    pub avg_checkpoint_duration_secs: f64,
    /// Checkpoints per hour
    pub checkpoints_per_hour: f64,
    /// Percentage of time spent checkpointing
    pub checkpoint_time_percent: f64,
    /// Performance status
    pub status: String,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Analyze PostgreSQL checkpoint performance
///
/// Analyzes checkpoint behavior and provides tuning recommendations.
///
/// # Arguments
///
/// * `checkpoint_interval_secs` - Time between checkpoints
/// * `avg_checkpoint_duration_secs` - Average checkpoint duration
/// * `writes_per_checkpoint` - Buffers written per checkpoint
/// * `checkpoint_completion_target` - checkpoint_completion_target setting
///
/// # Returns
///
/// Checkpoint performance analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::analyze_postgres_checkpoint_performance;
///
/// let analysis = analyze_postgres_checkpoint_performance(300.0, 45.0, 5000, 0.9);
/// println!("Checkpoint status: {}", analysis.status);
/// println!("Time spent checkpointing: {:.1}%", analysis.checkpoint_time_percent);
/// ```
pub fn analyze_postgres_checkpoint_performance(
    checkpoint_interval_secs: f64,
    avg_checkpoint_duration_secs: f64,
    writes_per_checkpoint: usize,
    checkpoint_completion_target: f64,
) -> CheckpointPerformanceAnalysis {
    let checkpoints_per_hour = 3600.0 / checkpoint_interval_secs;
    let checkpoint_time_percent = (avg_checkpoint_duration_secs / checkpoint_interval_secs) * 100.0;

    let status = if checkpoint_time_percent > 50.0 {
        "critical".to_string()
    } else if checkpoint_time_percent > 30.0 {
        "warning".to_string()
    } else if checkpoint_time_percent > 15.0 {
        "moderate".to_string()
    } else {
        "good".to_string()
    };

    let mut recommendations = Vec::new();

    if checkpoint_time_percent > 30.0 {
        recommendations.push("High checkpoint overhead detected (>30%)".to_string());
        recommendations.push("Consider increasing shared_buffers if memory allows".to_string());
        recommendations.push(
            "Consider increasing checkpoint_timeout (current checkpoints are frequent)".to_string(),
        );
    }

    if avg_checkpoint_duration_secs > 60.0 {
        recommendations.push("Checkpoints are taking >60 seconds".to_string());
        recommendations.push(
            "Consider increasing max_wal_size to allow more time between checkpoints".to_string(),
        );
    }

    if writes_per_checkpoint > 50000 {
        recommendations.push("High number of writes per checkpoint".to_string());
        recommendations.push(
            "Consider increasing checkpoint_timeout to spread writes over longer period"
                .to_string(),
        );
    }

    if checkpoint_completion_target < 0.7 {
        recommendations.push(format!(
            "checkpoint_completion_target is low ({:.1})",
            checkpoint_completion_target
        ));
        recommendations
            .push("Consider increasing to 0.9 to spread I/O over checkpoint interval".to_string());
    }

    if status == "good" {
        recommendations.push("Checkpoint performance is within optimal range".to_string());
    }

    CheckpointPerformanceAnalysis {
        checkpoint_interval_secs,
        avg_checkpoint_duration_secs,
        checkpoints_per_hour,
        checkpoint_time_percent,
        status,
        recommendations,
    }
}

/// VACUUM urgency analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumUrgencyAnalysis {
    /// Urgency score (0.0 = not urgent, 1.0 = very urgent)
    pub urgency_score: f64,
    /// Recommended action
    pub action: String,
    /// Estimated VACUUM duration in seconds
    pub estimated_time_secs: f64,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Estimate PostgreSQL VACUUM urgency
///
/// Calculates how urgently a table needs VACUUM based on multiple factors.
///
/// # Arguments
///
/// * `dead_tuples` - Number of dead tuples
/// * `live_tuples` - Number of live tuples
/// * `bloat_ratio` - Current bloat ratio (0.0 to 1.0)
/// * `hours_since_last_vacuum` - Hours since last VACUUM
///
/// # Returns
///
/// VACUUM urgency analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::estimate_postgres_vacuum_urgency;
///
/// let urgency = estimate_postgres_vacuum_urgency(500_000, 1_000_000, 0.3, 48.0);
/// println!("Urgency score: {:.2}", urgency.urgency_score);
/// println!("Action: {}", urgency.action);
/// ```
pub fn estimate_postgres_vacuum_urgency(
    dead_tuples: u64,
    live_tuples: u64,
    bloat_ratio: f64,
    hours_since_last_vacuum: f64,
) -> VacuumUrgencyAnalysis {
    let total_tuples = dead_tuples + live_tuples;
    let dead_ratio = if total_tuples > 0 {
        dead_tuples as f64 / total_tuples as f64
    } else {
        0.0
    };

    // Calculate urgency based on multiple factors
    let bloat_score = bloat_ratio;
    let dead_score = dead_ratio;
    let time_score = (hours_since_last_vacuum / 168.0).min(1.0); // 168 hours = 1 week

    let urgency_score = (bloat_score * 0.4 + dead_score * 0.4 + time_score * 0.2).min(1.0);

    let action = if urgency_score > 0.8 {
        "Run VACUUM immediately".to_string()
    } else if urgency_score > 0.6 {
        "Schedule VACUUM soon".to_string()
    } else if urgency_score > 0.4 {
        "VACUUM recommended within 24 hours".to_string()
    } else if urgency_score > 0.2 {
        "VACUUM can wait, monitor".to_string()
    } else {
        "VACUUM not urgent".to_string()
    };

    // Estimate VACUUM time (rough heuristic: ~1000 tuples/sec)
    let estimated_time_secs = (total_tuples as f64 / 1000.0).max(1.0);

    let mut recommendations = Vec::new();

    if urgency_score > 0.7 {
        recommendations.push("High urgency - table needs immediate attention".to_string());
        if bloat_ratio > 0.4 {
            recommendations.push("Consider VACUUM FULL during maintenance window".to_string());
        } else {
            recommendations.push("Regular VACUUM should be sufficient".to_string());
        }
    }

    if hours_since_last_vacuum > 72.0 {
        recommendations
            .push("Table has not been vacuumed in >3 days - check autovacuum settings".to_string());
    }

    if dead_tuples > 1_000_000 {
        recommendations.push(format!(
            "Large number of dead tuples ({}) - may impact query performance",
            dead_tuples
        ));
    }

    VacuumUrgencyAnalysis {
        urgency_score,
        action,
        estimated_time_secs,
        recommendations,
    }
}

/// Connection pool efficiency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolEfficiency {
    /// Total connections in pool
    pub total_connections: usize,
    /// Currently active connections
    pub active_connections: usize,
    /// Currently idle connections
    pub idle_connections: usize,
    /// Total queries executed
    pub total_queries: u64,
    /// Queries per connection
    pub queries_per_connection: f64,
    /// Pool utilization percentage
    pub utilization_percent: f64,
    /// Efficiency status
    pub status: String,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Calculate PostgreSQL connection pool efficiency
///
/// Analyzes connection pool usage patterns and provides optimization recommendations.
///
/// # Arguments
///
/// * `total_connections` - Total connections in pool
/// * `active_connections` - Currently active connections
/// * `idle_connections` - Currently idle connections
/// * `total_queries` - Total queries executed
///
/// # Returns
///
/// Connection pool efficiency analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::calculate_postgres_connection_pool_efficiency;
///
/// let efficiency = calculate_postgres_connection_pool_efficiency(50, 45, 5, 10_000);
/// println!("Pool utilization: {:.1}%", efficiency.utilization_percent);
/// println!("Status: {}", efficiency.status);
/// ```
pub fn calculate_postgres_connection_pool_efficiency(
    total_connections: usize,
    active_connections: usize,
    idle_connections: usize,
    total_queries: u64,
) -> ConnectionPoolEfficiency {
    let utilization_percent = if total_connections > 0 {
        (active_connections as f64 / total_connections as f64) * 100.0
    } else {
        0.0
    };

    let queries_per_connection = if total_connections > 0 {
        total_queries as f64 / total_connections as f64
    } else {
        0.0
    };

    let status = if utilization_percent > 90.0 {
        "critical".to_string()
    } else if utilization_percent > 75.0 {
        "high".to_string()
    } else if utilization_percent > 50.0 {
        "good".to_string()
    } else if utilization_percent > 20.0 {
        "moderate".to_string()
    } else {
        "low".to_string()
    };

    let mut recommendations = Vec::new();

    match status.as_str() {
        "critical" => {
            recommendations.push("CRITICAL: Pool utilization >90%".to_string());
            recommendations.push("Increase max_connections in pool configuration".to_string());
            recommendations.push("Monitor for connection exhaustion".to_string());
            recommendations
                .push("Consider connection pooling at application level (PgBouncer)".to_string());
        }
        "high" => {
            recommendations.push("High pool utilization (75-90%)".to_string());
            recommendations.push("Consider increasing pool size".to_string());
            recommendations.push("Monitor connection wait times".to_string());
        }
        "low" => {
            recommendations.push("Low pool utilization (<20%)".to_string());
            recommendations.push("Consider reducing pool size to conserve resources".to_string());
            recommendations.push(format!(
                "Current pool size ({}) may be oversized",
                total_connections
            ));
        }
        _ => {
            recommendations.push("Pool utilization is within optimal range".to_string());
        }
    }

    if idle_connections as f64 > total_connections as f64 * 0.5 {
        recommendations.push(format!(
            "High number of idle connections ({}) - consider reducing pool size",
            idle_connections
        ));
    }

    ConnectionPoolEfficiency {
        total_connections,
        active_connections,
        idle_connections,
        total_queries,
        queries_per_connection,
        utilization_percent,
        status,
        recommendations,
    }
}

/// Cache hit ratio analysis for buffer cache effectiveness
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheHitRatioAnalysis {
    /// Total number of buffer hits (from cache)
    pub heap_blks_hit: i64,
    /// Total number of buffer reads (from disk)
    pub heap_blks_read: i64,
    /// Overall cache hit ratio (0.0 to 100.0)
    pub hit_ratio_percent: f64,
    /// Cache performance status
    pub status: String,
    /// Recommendations for cache optimization
    pub recommendations: Vec<String>,
}

/// Analyze PostgreSQL buffer cache hit ratio
///
/// Analyzes the effectiveness of PostgreSQL's shared_buffers cache by calculating
/// the ratio of cache hits to total reads. A high hit ratio (>95%) indicates good
/// cache performance, while low ratios suggest increasing shared_buffers.
///
/// # Arguments
///
/// * `heap_blks_hit` - Number of blocks read from cache
/// * `heap_blks_read` - Number of blocks read from disk
///
/// # Returns
///
/// Cache hit ratio analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::analyze_postgres_cache_hit_ratio;
///
/// let analysis = analyze_postgres_cache_hit_ratio(950_000, 50_000);
/// println!("Cache hit ratio: {:.2}%", analysis.hit_ratio_percent);
/// println!("Status: {}", analysis.status);
/// assert!(analysis.hit_ratio_percent > 90.0);
/// ```
pub fn analyze_postgres_cache_hit_ratio(
    heap_blks_hit: i64,
    heap_blks_read: i64,
) -> CacheHitRatioAnalysis {
    let total_reads = heap_blks_hit + heap_blks_read;
    let hit_ratio_percent = if total_reads > 0 {
        (heap_blks_hit as f64 / total_reads as f64) * 100.0
    } else {
        0.0
    };

    let status = if hit_ratio_percent >= 99.0 {
        "excellent".to_string()
    } else if hit_ratio_percent >= 95.0 {
        "good".to_string()
    } else if hit_ratio_percent >= 90.0 {
        "acceptable".to_string()
    } else if hit_ratio_percent >= 80.0 {
        "poor".to_string()
    } else {
        "critical".to_string()
    };

    let mut recommendations = Vec::new();

    match status.as_str() {
        "critical" => {
            recommendations.push("CRITICAL: Cache hit ratio <80%".to_string());
            recommendations
                .push("Immediately increase shared_buffers (recommend 25% of RAM)".to_string());
            recommendations
                .push("Check for table bloat and excessive sequential scans".to_string());
            recommendations.push("Consider adding indexes to reduce disk I/O".to_string());
            recommendations.push(format!(
                "Current disk reads: {} blocks - very high",
                heap_blks_read
            ));
        }
        "poor" => {
            recommendations.push("Low cache hit ratio (80-90%)".to_string());
            recommendations
                .push("Increase shared_buffers to improve cache performance".to_string());
            recommendations
                .push("Analyze query patterns for optimization opportunities".to_string());
        }
        "acceptable" => {
            recommendations.push("Cache hit ratio is acceptable (90-95%)".to_string());
            recommendations.push("Consider moderate increase in shared_buffers".to_string());
        }
        "good" => {
            recommendations.push("Cache hit ratio is good (95-99%)".to_string());
            recommendations.push("shared_buffers is well-tuned".to_string());
        }
        "excellent" => {
            recommendations.push("Excellent cache hit ratio (>99%)".to_string());
            recommendations.push("shared_buffers is optimally configured".to_string());
        }
        _ => {}
    }

    CacheHitRatioAnalysis {
        heap_blks_hit,
        heap_blks_read,
        hit_ratio_percent,
        status,
        recommendations,
    }
}

/// Table statistics staleness information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatsStaleness {
    /// Table name
    pub table_name: String,
    /// Last ANALYZE timestamp
    pub last_analyze: Option<String>,
    /// Last autovacuum timestamp
    pub last_autovacuum: Option<String>,
    /// Number of live rows
    pub n_live_tup: i64,
    /// Number of dead rows
    pub n_dead_tup: i64,
    /// Staleness severity
    pub severity: String,
    /// Whether ANALYZE is needed
    pub needs_analyze: bool,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Detect table statistics staleness
///
/// Checks if table statistics are stale and need ANALYZE to ensure the query
/// planner has accurate information for optimal execution plans.
///
/// # Arguments
///
/// * `table_name` - Name of the table
/// * `last_analyze_days_ago` - Days since last ANALYZE (None if never)
/// * `n_live_tup` - Number of live tuples
/// * `n_dead_tup` - Number of dead tuples
/// * `modifications_since_analyze` - Rows modified since last ANALYZE
///
/// # Returns
///
/// Table statistics staleness analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::detect_postgres_table_stats_staleness;
///
/// let staleness = detect_postgres_table_stats_staleness(
///     "celers_tasks",
///     Some(10.0),
///     100_000,
///     5_000,
///     25_000
/// );
/// println!("Needs ANALYZE: {}", staleness.needs_analyze);
/// println!("Severity: {}", staleness.severity);
/// ```
pub fn detect_postgres_table_stats_staleness(
    table_name: &str,
    last_analyze_days_ago: Option<f64>,
    n_live_tup: i64,
    n_dead_tup: i64,
    modifications_since_analyze: i64,
) -> TableStatsStaleness {
    let dead_ratio = if n_live_tup > 0 {
        (n_dead_tup as f64 / n_live_tup as f64) * 100.0
    } else {
        0.0
    };

    let modification_ratio = if n_live_tup > 0 {
        (modifications_since_analyze as f64 / n_live_tup as f64) * 100.0
    } else {
        0.0
    };

    let needs_analyze = match last_analyze_days_ago {
        None => true,                                 // Never analyzed
        Some(days) if days > 7.0 => true,             // Stale (>7 days)
        Some(_) if modification_ratio > 20.0 => true, // >20% modifications
        Some(_) if dead_ratio > 10.0 => true,         // >10% dead tuples
        _ => false,
    };

    let severity = if last_analyze_days_ago.is_none() || last_analyze_days_ago.unwrap_or(0.0) > 30.0
    {
        "critical".to_string()
    } else if modification_ratio > 50.0 || dead_ratio > 20.0 {
        "high".to_string()
    } else if modification_ratio > 20.0 || dead_ratio > 10.0 {
        "medium".to_string()
    } else {
        "low".to_string()
    };

    let mut recommendations = Vec::new();

    if needs_analyze {
        match severity.as_str() {
            "critical" => {
                recommendations.push("CRITICAL: Table statistics are critically stale".to_string());
                recommendations.push(format!("Run ANALYZE {} immediately", table_name));
                recommendations.push("Query planner may be using outdated statistics".to_string());
            }
            "high" => {
                recommendations.push(format!("High staleness: Run ANALYZE {} soon", table_name));
                recommendations.push(format!(
                    "Modifications since ANALYZE: {} ({:.1}%)",
                    modifications_since_analyze, modification_ratio
                ));
            }
            "medium" => {
                recommendations.push(format!(
                    "Moderate staleness: Schedule ANALYZE {}",
                    table_name
                ));
            }
            _ => {}
        }
    } else {
        recommendations.push("Table statistics are fresh".to_string());
    }

    if dead_ratio > 10.0 {
        recommendations.push(format!(
            "High dead tuple ratio ({:.1}%) - consider VACUUM",
            dead_ratio
        ));
    }

    TableStatsStaleness {
        table_name: table_name.to_string(),
        last_analyze: last_analyze_days_ago.map(|d| format!("{:.1} days ago", d)),
        last_autovacuum: None,
        n_live_tup,
        n_dead_tup,
        severity,
        needs_analyze,
        recommendations,
    }
}

/// Long-running query information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LongRunningQueryInfo {
    /// Query PID
    pub pid: i32,
    /// Query runtime in seconds
    pub runtime_seconds: f64,
    /// Query state (active, idle in transaction, etc.)
    pub state: String,
    /// Query text (truncated)
    pub query: String,
    /// Whether query is blocking others
    pub is_blocking: bool,
    /// Severity level
    pub severity: String,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Detect long-running queries
///
/// Identifies queries that have been running longer than expected, which may
/// indicate inefficient queries, missing indexes, or blocking issues.
///
/// # Arguments
///
/// * `pid` - Process ID of the query
/// * `runtime_seconds` - How long the query has been running
/// * `state` - Query state (active, idle in transaction, etc.)
/// * `query` - Query text
/// * `is_blocking` - Whether the query is blocking other queries
///
/// # Returns
///
/// Long-running query analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::detect_postgres_long_running_query;
///
/// let query_info = detect_postgres_long_running_query(
///     12345,
///     300.0,
///     "active",
///     "SELECT * FROM celers_tasks WHERE ...",
///     false
/// );
/// println!("Severity: {}", query_info.severity);
/// println!("Runtime: {:.1}s", query_info.runtime_seconds);
/// ```
pub fn detect_postgres_long_running_query(
    pid: i32,
    runtime_seconds: f64,
    state: &str,
    query: &str,
    is_blocking: bool,
) -> LongRunningQueryInfo {
    let severity = if is_blocking && runtime_seconds > 60.0 {
        "critical".to_string()
    } else if runtime_seconds > 600.0 {
        // 10 minutes
        "critical".to_string()
    } else if runtime_seconds > 300.0 {
        // 5 minutes
        "high".to_string()
    } else if runtime_seconds > 60.0 {
        // 1 minute
        "medium".to_string()
    } else {
        "low".to_string()
    };

    let mut recommendations = Vec::new();

    match severity.as_str() {
        "critical" => {
            if is_blocking {
                recommendations.push("CRITICAL: Blocking query detected!".to_string());
                recommendations.push(format!("Query PID {} is blocking other queries", pid));
                recommendations
                    .push("Consider terminating with pg_terminate_backend()".to_string());
            } else {
                recommendations.push(format!(
                    "CRITICAL: Query running for {:.1} seconds",
                    runtime_seconds
                ));
                recommendations.push("Investigate query plan with EXPLAIN ANALYZE".to_string());
                recommendations.push("Check for missing indexes or table bloat".to_string());
            }
        }
        "high" => {
            recommendations.push(format!("Long-running query: {:.1}s", runtime_seconds));
            recommendations.push("Review query for optimization opportunities".to_string());
        }
        "medium" => {
            recommendations.push(format!("Moderate runtime: {:.1}s", runtime_seconds));
            recommendations.push("Monitor query performance".to_string());
        }
        _ => {}
    }

    if state == "idle in transaction" {
        recommendations.push("WARNING: Query is 'idle in transaction'".to_string());
        recommendations
            .push("This may be holding locks - check application connection handling".to_string());
    }

    // Truncate query for display
    let query_truncated = if query.len() > 100 {
        format!("{}...", &query[..97])
    } else {
        query.to_string()
    };

    LongRunningQueryInfo {
        pid,
        runtime_seconds,
        state: state.to_string(),
        query: query_truncated,
        is_blocking,
        severity,
        recommendations,
    }
}

/// Autovacuum effectiveness analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutovacuumEffectiveness {
    /// Table name
    pub table_name: String,
    /// Number of autovacuum runs
    pub autovacuum_count: i64,
    /// Number of dead tuples
    pub n_dead_tup: i64,
    /// Dead tuple ratio (percent)
    pub dead_ratio_percent: f64,
    /// Time since last autovacuum (seconds)
    pub seconds_since_last_autovacuum: Option<f64>,
    /// Effectiveness status
    pub status: String,
    /// Whether manual intervention is needed
    pub needs_manual_vacuum: bool,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Monitor autovacuum effectiveness
///
/// Analyzes whether autovacuum is keeping up with dead tuple accumulation
/// and provides recommendations for manual VACUUM or autovacuum tuning.
///
/// # Arguments
///
/// * `table_name` - Name of the table
/// * `autovacuum_count` - Number of autovacuum runs on this table
/// * `n_dead_tup` - Current number of dead tuples
/// * `n_live_tup` - Number of live tuples
/// * `seconds_since_last_autovacuum` - Time since last autovacuum (None if never)
///
/// # Returns
///
/// Autovacuum effectiveness analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::monitor_postgres_autovacuum_effectiveness;
///
/// let effectiveness = monitor_postgres_autovacuum_effectiveness(
///     "celers_tasks",
///     25,
///     50_000,
///     1_000_000,
///     Some(3600.0)
/// );
/// println!("Status: {}", effectiveness.status);
/// println!("Needs manual VACUUM: {}", effectiveness.needs_manual_vacuum);
/// ```
pub fn monitor_postgres_autovacuum_effectiveness(
    table_name: &str,
    autovacuum_count: i64,
    n_dead_tup: i64,
    n_live_tup: i64,
    seconds_since_last_autovacuum: Option<f64>,
) -> AutovacuumEffectiveness {
    let dead_ratio_percent = if n_live_tup > 0 {
        (n_dead_tup as f64 / n_live_tup as f64) * 100.0
    } else {
        0.0
    };

    let needs_manual_vacuum = (seconds_since_last_autovacuum.is_none() && n_dead_tup > 10_000)
        || dead_ratio_percent > 20.0
        || seconds_since_last_autovacuum
            .map(|secs| secs > 86400.0 && dead_ratio_percent > 10.0)
            .unwrap_or(false);

    let status = if dead_ratio_percent > 30.0 {
        "critical".to_string()
    } else if dead_ratio_percent > 20.0 {
        "poor".to_string()
    } else if dead_ratio_percent > 10.0 {
        "acceptable".to_string()
    } else {
        "good".to_string()
    };

    let mut recommendations = Vec::new();

    match status.as_str() {
        "critical" => {
            recommendations.push("CRITICAL: Autovacuum is not keeping up!".to_string());
            recommendations.push(format!(
                "Dead tuple ratio: {:.1}% - extremely high",
                dead_ratio_percent
            ));
            recommendations.push(format!("Run VACUUM {} immediately", table_name));
            recommendations.push("Consider tuning autovacuum_vacuum_scale_factor".to_string());
        }
        "poor" => {
            recommendations.push("Autovacuum effectiveness is poor".to_string());
            recommendations.push(format!(
                "Dead tuple ratio: {:.1}% - needs attention",
                dead_ratio_percent
            ));
            if let Some(secs) = seconds_since_last_autovacuum {
                recommendations.push(format!("Last autovacuum: {:.1} hours ago", secs / 3600.0));
            }
        }
        "acceptable" => {
            recommendations.push("Autovacuum is working but could be improved".to_string());
        }
        "good" => {
            recommendations.push("Autovacuum is effective".to_string());
        }
        _ => {}
    }

    if autovacuum_count == 0 && n_dead_tup > 0 {
        recommendations.push("WARNING: No autovacuum runs detected".to_string());
        recommendations.push("Check autovacuum configuration (autovacuum = on)".to_string());
    }

    AutovacuumEffectiveness {
        table_name: table_name.to_string(),
        autovacuum_count,
        n_dead_tup,
        dead_ratio_percent,
        seconds_since_last_autovacuum,
        status,
        needs_manual_vacuum,
        recommendations,
    }
}

/// Sequential scan detection analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequentialScanAnalysis {
    /// Table name
    pub table_name: String,
    /// Number of sequential scans
    pub seq_scan: i64,
    /// Number of index scans
    pub idx_scan: i64,
    /// Rows returned by sequential scans
    pub seq_tup_read: i64,
    /// Table size in rows
    pub n_live_tup: i64,
    /// Sequential scan ratio (percent)
    pub seq_scan_ratio: f64,
    /// Severity level
    pub severity: String,
    /// Whether indexes are needed
    pub needs_index: bool,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Detect excessive sequential scans
///
/// Identifies tables that are frequently scanned sequentially instead of
/// using indexes, which can cause performance issues on large tables.
///
/// # Arguments
///
/// * `table_name` - Name of the table
/// * `seq_scan` - Number of sequential scans
/// * `idx_scan` - Number of index scans
/// * `seq_tup_read` - Tuples read via sequential scans
/// * `n_live_tup` - Number of live tuples in table
///
/// # Returns
///
/// Sequential scan analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::detect_postgres_sequential_scans;
///
/// let analysis = detect_postgres_sequential_scans(
///     "celers_tasks",
///     1000,
///     500,
///     5_000_000,
///     1_000_000
/// );
/// println!("Sequential scan ratio: {:.1}%", analysis.seq_scan_ratio);
/// println!("Needs index: {}", analysis.needs_index);
/// ```
pub fn detect_postgres_sequential_scans(
    table_name: &str,
    seq_scan: i64,
    idx_scan: i64,
    seq_tup_read: i64,
    n_live_tup: i64,
) -> SequentialScanAnalysis {
    let total_scans = seq_scan + idx_scan;
    let seq_scan_ratio = if total_scans > 0 {
        (seq_scan as f64 / total_scans as f64) * 100.0
    } else {
        0.0
    };

    let avg_rows_per_seq_scan = if seq_scan > 0 {
        seq_tup_read / seq_scan
    } else {
        0
    };

    // Large tables with high seq scan ratios need indexes
    let needs_index = n_live_tup > 10_000 && seq_scan_ratio > 50.0 && seq_scan > 100;

    let severity = if needs_index && seq_scan_ratio > 90.0 {
        "critical".to_string()
    } else if needs_index && seq_scan_ratio > 70.0 {
        "high".to_string()
    } else if seq_scan_ratio > 50.0 && n_live_tup > 10_000 {
        "medium".to_string()
    } else {
        "low".to_string()
    };

    let mut recommendations = Vec::new();

    match severity.as_str() {
        "critical" => {
            recommendations.push("CRITICAL: Excessive sequential scans!".to_string());
            recommendations.push(format!(
                "{:.1}% of scans are sequential on large table ({} rows)",
                seq_scan_ratio, n_live_tup
            ));
            recommendations.push(format!("Add indexes to {} immediately", table_name));
            recommendations.push("Use EXPLAIN ANALYZE to identify missing indexes".to_string());
        }
        "high" => {
            recommendations.push("High sequential scan ratio detected".to_string());
            recommendations.push(format!(
                "Sequential scans: {} ({:.1}%)",
                seq_scan, seq_scan_ratio
            ));
            recommendations.push("Consider adding indexes for common queries".to_string());
        }
        "medium" => {
            recommendations.push("Moderate sequential scan activity".to_string());
            recommendations.push("Review query patterns for optimization".to_string());
        }
        _ => {
            if idx_scan > seq_scan * 2 {
                recommendations.push("Good index usage - indexes are effective".to_string());
            }
        }
    }

    if avg_rows_per_seq_scan > n_live_tup / 2 && n_live_tup > 10_000 {
        recommendations.push(format!(
            "Sequential scans reading {} rows on average - very inefficient",
            avg_rows_per_seq_scan
        ));
    }

    SequentialScanAnalysis {
        table_name: table_name.to_string(),
        seq_scan,
        idx_scan,
        seq_tup_read,
        n_live_tup,
        seq_scan_ratio,
        severity,
        needs_index,
        recommendations,
    }
}

/// Index bloat analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBloatAnalysis {
    /// Index name
    pub index_name: String,
    /// Table name
    pub table_name: String,
    /// Index size in bytes
    pub index_size_bytes: i64,
    /// Estimated bloat in bytes
    pub bloat_bytes: i64,
    /// Bloat ratio (percent)
    pub bloat_ratio_percent: f64,
    /// Bloat severity
    pub severity: String,
    /// Whether REINDEX is recommended
    pub needs_reindex: bool,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Calculate index bloat ratio
///
/// Estimates index bloat based on actual vs. expected index size and provides
/// recommendations for REINDEX operations.
///
/// # Arguments
///
/// * `index_name` - Name of the index
/// * `table_name` - Name of the table
/// * `index_size_bytes` - Current index size
/// * `expected_size_bytes` - Expected index size without bloat
///
/// # Returns
///
/// Index bloat analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::calculate_postgres_index_bloat;
///
/// let bloat = calculate_postgres_index_bloat(
///     "idx_tasks_state_priority",
///     "celers_tasks",
///     50_000_000,
///     30_000_000
/// );
/// println!("Bloat ratio: {:.1}%", bloat.bloat_ratio_percent);
/// println!("Needs REINDEX: {}", bloat.needs_reindex);
/// ```
pub fn calculate_postgres_index_bloat(
    index_name: &str,
    table_name: &str,
    index_size_bytes: i64,
    expected_size_bytes: i64,
) -> IndexBloatAnalysis {
    let bloat_bytes = (index_size_bytes - expected_size_bytes).max(0);
    let bloat_ratio_percent = if expected_size_bytes > 0 {
        (bloat_bytes as f64 / expected_size_bytes as f64) * 100.0
    } else {
        0.0
    };

    let needs_reindex = bloat_ratio_percent > 30.0 && bloat_bytes > 10_000_000; // >30% and >10MB

    let severity = if bloat_ratio_percent > 50.0 {
        "critical".to_string()
    } else if bloat_ratio_percent > 30.0 {
        "high".to_string()
    } else if bloat_ratio_percent > 20.0 {
        "medium".to_string()
    } else {
        "low".to_string()
    };

    let mut recommendations = Vec::new();

    match severity.as_str() {
        "critical" => {
            recommendations.push("CRITICAL: Excessive index bloat!".to_string());
            recommendations.push(format!(
                "Index bloat: {:.1}% ({} MB wasted)",
                bloat_ratio_percent,
                bloat_bytes / 1_000_000
            ));
            recommendations.push(format!(
                "Run REINDEX INDEX CONCURRENTLY {} immediately",
                index_name
            ));
            recommendations.push("Use CONCURRENTLY to avoid blocking queries".to_string());
        }
        "high" => {
            recommendations.push("High index bloat detected".to_string());
            recommendations.push(format!("Bloat: {:.1}%", bloat_ratio_percent));
            recommendations.push(format!(
                "Schedule REINDEX INDEX CONCURRENTLY {}",
                index_name
            ));
        }
        "medium" => {
            recommendations.push("Moderate index bloat".to_string());
            recommendations
                .push("Monitor and consider REINDEX during maintenance window".to_string());
        }
        _ => {
            recommendations.push("Index bloat is within acceptable limits".to_string());
        }
    }

    if index_size_bytes > 100_000_000 {
        // >100MB
        recommendations.push(format!(
            "Large index ({} MB) - REINDEX will take time",
            index_size_bytes / 1_000_000
        ));
    }

    IndexBloatAnalysis {
        index_name: index_name.to_string(),
        table_name: table_name.to_string(),
        index_size_bytes,
        bloat_bytes,
        bloat_ratio_percent,
        severity,
        needs_reindex,
        recommendations,
    }
}

/// Connection state breakdown analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStateAnalysis {
    /// Active connections
    pub active: i32,
    /// Idle connections
    pub idle: i32,
    /// Idle in transaction connections (potentially problematic)
    pub idle_in_transaction: i32,
    /// Idle in transaction (aborted)
    pub idle_in_transaction_aborted: i32,
    /// Total connections
    pub total: i32,
    /// Health status
    pub status: String,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Analyze connection states
///
/// Provides detailed breakdown of connection states, identifying problematic
/// states like "idle in transaction" that may be holding locks.
///
/// # Arguments
///
/// * `active` - Number of active connections
/// * `idle` - Number of idle connections
/// * `idle_in_transaction` - Connections idle in transaction
/// * `idle_in_transaction_aborted` - Connections idle in aborted transaction
///
/// # Returns
///
/// Connection state analysis
///
/// # Examples
///
/// ```
/// use celers_broker_postgres::monitoring::analyze_postgres_connection_states;
///
/// let analysis = analyze_postgres_connection_states(10, 5, 2, 0);
/// println!("Status: {}", analysis.status);
/// println!("Idle in transaction: {}", analysis.idle_in_transaction);
/// ```
pub fn analyze_postgres_connection_states(
    active: i32,
    idle: i32,
    idle_in_transaction: i32,
    idle_in_transaction_aborted: i32,
) -> ConnectionStateAnalysis {
    let total = active + idle + idle_in_transaction + idle_in_transaction_aborted;

    let idle_in_tx_ratio = if total > 0 {
        (idle_in_transaction as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let status = if idle_in_transaction_aborted > 0 || idle_in_tx_ratio > 20.0 {
        "critical".to_string()
    } else if idle_in_tx_ratio > 10.0 {
        "warning".to_string()
    } else if idle_in_transaction > 0 {
        "acceptable".to_string()
    } else {
        "good".to_string()
    };

    let mut recommendations = Vec::new();

    match status.as_str() {
        "critical" => {
            if idle_in_transaction_aborted > 0 {
                recommendations.push("CRITICAL: Aborted transactions detected!".to_string());
                recommendations.push(format!(
                    "{} connections in aborted transaction state",
                    idle_in_transaction_aborted
                ));
                recommendations.push("These connections may be holding locks".to_string());
            }
            if idle_in_tx_ratio > 20.0 {
                recommendations.push(format!(
                    "CRITICAL: {:.1}% of connections idle in transaction",
                    idle_in_tx_ratio
                ));
                recommendations.push("Check application connection handling".to_string());
                recommendations.push("Set idle_in_transaction_session_timeout".to_string());
            }
        }
        "warning" => {
            recommendations.push(format!(
                "High idle in transaction ratio: {:.1}%",
                idle_in_tx_ratio
            ));
            recommendations.push("Review application transaction management".to_string());
        }
        "acceptable" => {
            recommendations.push("Some idle in transaction connections".to_string());
            recommendations.push("Monitor for long-running transactions".to_string());
        }
        "good" => {
            recommendations.push("Connection states are healthy".to_string());
        }
        _ => {}
    }

    if idle as f64 > total as f64 * 0.7 {
        recommendations.push(format!(
            "High idle connection ratio ({:.1}%)",
            (idle as f64 / total as f64) * 100.0
        ));
        recommendations.push("Consider reducing connection pool size".to_string());
    }

    ConnectionStateAnalysis {
        active,
        idle,
        idle_in_transaction,
        idle_in_transaction_aborted,
        total,
        status,
        recommendations,
    }
}
