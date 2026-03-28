//! PostgreSQL diagnostics and configuration utilities
//!
//! This module provides diagnostic functions for PostgreSQL broker operations,
//! including query cost estimation, partitioning recommendations, parallel query
//! configuration, memory tuning, disk I/O optimization, configuration validation,
//! query plan analysis, compression recommendations, performance regression detection,
//! task execution metrics, and DLQ retry policies.

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

    // Add jitter if enabled (+-25% random variation)
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
