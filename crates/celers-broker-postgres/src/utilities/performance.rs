//! PostgreSQL performance analytics and health utilities
//!
//! This module provides advanced performance analytics for PostgreSQL broker operations,
//! including connection pool analysis, processing rate forecasting, database health scoring,
//! and batch optimization analysis.

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
