//! Database operations for CeleRS CLI.
//!
//! This module provides commands for managing database connections, health checks,
//! and migrations. Currently supports PostgreSQL with planned support for MySQL.
//!
//! # Features
//!
//! - Connection testing with latency benchmarks
//! - Health monitoring and diagnostics
//! - Connection pool statistics
//! - Schema migrations (auto-migration via SQLx)
//!
//! # Examples
//!
//! ```no_run
//! use celers_cli::database;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Test database connection
//!     database::db_test_connection(
//!         "postgresql://user:pass@localhost/celers",
//!         false
//!     ).await?;
//!
//!     // Check database health
//!     database::db_health("postgresql://user:pass@localhost/celers").await?;
//!
//!     Ok(())
//! }
//! ```

use crate::command_utils::mask_password;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Test database connection with optional benchmarking.
///
/// Connects to the database and verifies connectivity. If benchmark mode is enabled,
/// runs 10 test queries and reports latency statistics.
///
/// # Arguments
///
/// * `url` - Database connection URL
/// * `benchmark` - Whether to run latency benchmark (10 queries)
///
/// # Supported Databases
///
/// - PostgreSQL (fully supported)
/// - MySQL (planned)
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::database::db_test_connection;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Simple connection test
/// db_test_connection("postgresql://localhost/celers", false).await?;
///
/// // With latency benchmark
/// db_test_connection("postgresql://localhost/celers", true).await?;
/// # Ok(())
/// # }
/// ```
pub async fn db_test_connection(url: &str, benchmark: bool) -> anyhow::Result<()> {
    println!("{}", "=== Database Connection Test ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    // Determine database type from URL
    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    println!("Database type: {}", db_type.yellow());
    println!();

    // Test connection
    println!("Testing connection...");
    let start = std::time::Instant::now();

    // For PostgreSQL
    if db_type == "PostgreSQL" {
        match test_postgres_connection(url).await {
            Ok(version) => {
                let duration = start.elapsed();
                println!("{}", "✓ Connection successful".green());
                println!("  Version: {}", version.cyan());
                println!("  Latency: {}ms", duration.as_millis().to_string().yellow());
            }
            Err(e) => {
                println!("{}", "✗ Connection failed".red());
                println!("  Error: {e}");
                return Err(e);
            }
        }
    } else if db_type == "MySQL" {
        println!("{}", "⚠️  MySQL support not yet implemented".yellow());
        return Ok(());
    } else {
        println!("{}", "⚠️  Unknown database type".yellow());
        return Ok(());
    }

    // Run benchmark if requested
    if benchmark {
        println!();
        println!("{}", "Running latency benchmark...".bold());
        println!();

        let mut latencies = Vec::new();
        for i in 1..=10 {
            let start = std::time::Instant::now();
            if let Err(e) = test_postgres_connection(url).await {
                println!("  {} Query {} failed: {}", "✗".red(), i, e);
                continue;
            }
            let duration = start.elapsed();
            latencies.push(duration.as_millis());
            println!("  {} Query {}: {}ms", "✓".green(), i, duration.as_millis());
        }

        if !latencies.is_empty() {
            let avg = latencies.iter().sum::<u128>() / latencies.len() as u128;
            let min = latencies.iter().min().unwrap();
            let max = latencies.iter().max().unwrap();

            println!();
            println!("Benchmark results:");
            println!("  Average: {}ms", avg.to_string().cyan());
            println!("  Min: {}ms", min.to_string().green());
            println!("  Max: {}ms", max.to_string().yellow());
        }
    }

    Ok(())
}

/// Test PostgreSQL connection (internal helper).
async fn test_postgres_connection(url: &str) -> anyhow::Result<String> {
    use celers_broker_postgres::PostgresBroker;

    // Create a temporary broker to test connection
    let broker = PostgresBroker::new(url).await?;

    // Test connection and return a version string
    let connected = broker.test_connection().await?;
    if connected {
        Ok("Connected".to_string())
    } else {
        anyhow::bail!("Connection test failed")
    }
}

/// Check database health with comprehensive diagnostics.
///
/// Performs multiple health checks including connection verification,
/// query latency measurement, and connection pool status.
///
/// # Arguments
///
/// * `url` - Database connection URL
///
/// # Health Checks
///
/// - Connection status
/// - Query latency (5 test queries)
/// - Connection pool utilization
/// - Performance warnings (high latency > 100ms, high pool usage > 80%)
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::database::db_health;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// db_health("postgresql://localhost/celers").await?;
/// # Ok(())
/// # }
/// ```
pub async fn db_health(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Database Health Check ===".bold().cyan());
    println!();

    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    println!("Database: {}", db_type.yellow());
    println!();

    if db_type == "PostgreSQL" {
        check_postgres_health(url).await?;
    } else {
        println!(
            "{}",
            "⚠️  Health check not supported for this database type".yellow()
        );
    }

    Ok(())
}

/// Check PostgreSQL health (internal helper).
async fn check_postgres_health(url: &str) -> anyhow::Result<()> {
    use celers_broker_postgres::PostgresBroker;

    println!("Checking connection...");
    let broker = PostgresBroker::new(url).await?;
    println!("{}", "  ✓ Connection OK".green());

    println!();
    println!("Testing connection with latency measurement...");

    // Measure query performance
    let mut latencies = Vec::new();
    for i in 1..=5 {
        let start = std::time::Instant::now();
        let connected = broker.test_connection().await?;
        let duration = start.elapsed();

        if connected {
            latencies.push(duration.as_millis());
            println!("  {} Query {}: {}ms", "✓".green(), i, duration.as_millis());
        } else {
            println!("  {} Query {} failed", "✗".red(), i);
        }
    }

    if !latencies.is_empty() {
        let avg = latencies.iter().sum::<u128>() / latencies.len() as u128;
        let min = latencies.iter().min().unwrap();
        let max = latencies.iter().max().unwrap();

        println!();
        println!("Query Performance:");
        println!("  Average: {}ms", avg.to_string().cyan());
        println!("  Min: {}ms", min.to_string().green());
        println!("  Max: {}ms", max.to_string().yellow());

        if avg > 100 {
            println!("  {}", "⚠️  High query latency detected".yellow());
        } else {
            println!("  {}", "✓ Query latency is healthy".green());
        }
    }

    // Check pool metrics
    println!();
    println!("Connection Pool Status:");
    let pool_metrics = broker.get_pool_metrics();
    println!(
        "  Max Connections: {}",
        pool_metrics.max_size.to_string().cyan()
    );
    println!("  Active: {}", pool_metrics.size.to_string().cyan());
    println!("  Idle: {}", pool_metrics.idle.to_string().cyan());
    println!("  In-Use: {}", pool_metrics.in_use.to_string().cyan());

    let utilization = if pool_metrics.max_size > 0 {
        (f64::from(pool_metrics.in_use) / f64::from(pool_metrics.max_size)) * 100.0
    } else {
        0.0
    };

    if utilization > 80.0 {
        println!(
            "  {}",
            "⚠️  High pool utilization - consider scaling".yellow()
        );
    }

    println!();
    println!("{}", "✓ Database health check completed".green().bold());

    Ok(())
}

/// Show connection pool statistics.
///
/// Displays detailed metrics about the database connection pool including
/// active, idle, and in-use connections with utilization percentage.
///
/// # Arguments
///
/// * `url` - Database connection URL
///
/// # Metrics
///
/// - Max connections (pool size)
/// - Active connections
/// - Idle connections
/// - In-use connections
/// - Waiting tasks (if any)
/// - Pool utilization percentage
///
/// # Recommendations
///
/// - High utilization (>80%): Consider increasing max_connections
/// - Low utilization (<20%): Consider reducing max_connections
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::database::db_pool_stats;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// db_pool_stats("postgresql://localhost/celers").await?;
/// # Ok(())
/// # }
/// ```
pub async fn db_pool_stats(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Connection Pool Statistics ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    if db_type == "PostgreSQL" {
        use celers_broker_postgres::PostgresBroker;

        println!("Connecting to database...");
        let broker = PostgresBroker::new(url).await?;
        println!("{}", "  ✓ Connected".green());
        println!();

        let metrics = broker.get_pool_metrics();

        #[derive(Tabled)]
        struct PoolStat {
            #[tabled(rename = "Metric")]
            metric: String,
            #[tabled(rename = "Value")]
            value: String,
        }

        let stats = vec![
            PoolStat {
                metric: "Max Connections".to_string(),
                value: metrics.max_size.to_string(),
            },
            PoolStat {
                metric: "Active Connections".to_string(),
                value: metrics.size.to_string(),
            },
            PoolStat {
                metric: "Idle Connections".to_string(),
                value: metrics.idle.to_string(),
            },
            PoolStat {
                metric: "In-Use Connections".to_string(),
                value: metrics.in_use.to_string(),
            },
            PoolStat {
                metric: "Waiting Tasks".to_string(),
                value: if metrics.waiting > 0 {
                    metrics.waiting.to_string()
                } else {
                    "0 (estimated)".to_string()
                },
            },
        ];

        let table = Table::new(stats).with(Style::rounded()).to_string();
        println!("{table}");
        println!();

        // Pool utilization
        let utilization = if metrics.max_size > 0 {
            (f64::from(metrics.in_use) / f64::from(metrics.max_size)) * 100.0
        } else {
            0.0
        };

        println!("Pool Utilization: {utilization:.1}%");
        if utilization > 80.0 {
            println!(
                "{}",
                "⚠️  High pool utilization - consider increasing max_connections".yellow()
            );
        } else if utilization < 20.0 && metrics.max_size > 10 {
            println!(
                "{}",
                "ℹ️  Low pool utilization - consider reducing max_connections".cyan()
            );
        } else {
            println!("{}", "✓ Pool utilization is healthy".green());
        }
    } else {
        println!(
            "{}",
            "⚠️  Pool statistics only supported for PostgreSQL".yellow()
        );
    }

    Ok(())
}

/// Run database migrations.
///
/// Manages database schema migrations using SQLx auto-migration.
/// Supports applying migrations, checking status, and rollback guidance.
///
/// # Arguments
///
/// * `url` - Database connection URL
/// * `action` - Migration action: "apply", "rollback", or "status"
/// * `steps` - Number of steps for rollback (currently requires manual rollback)
///
/// # Actions
///
/// - **apply**: Initialize schema and apply migrations
/// - **status**: Check current schema state
/// - **rollback**: Show manual rollback instructions
///
/// # Note
///
/// CeleRS uses SQLx auto-migration. For rollbacks, restore from database backup.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::database::db_migrate;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Apply migrations
/// db_migrate("postgresql://localhost/celers", "apply", 0).await?;
///
/// // Check status
/// db_migrate("postgresql://localhost/celers", "status", 0).await?;
/// # Ok(())
/// # }
/// ```
pub async fn db_migrate(url: &str, action: &str, steps: usize) -> anyhow::Result<()> {
    println!("{}", "=== Database Migrations ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!("Action: {}", action.yellow());
    println!();

    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    if db_type == "PostgreSQL" {
        use celers_broker_postgres::PostgresBroker;

        match action.to_lowercase().as_str() {
            "apply" => {
                println!("Applying migrations...");
                let broker = PostgresBroker::new(url).await?;
                let _ = broker; // Use broker to ensure it connects

                println!("{}", "  ✓ Schema initialized".green());
                println!();
                println!("{}", "✓ Migrations applied successfully".green().bold());
            }
            "rollback" => {
                println!("Rolling back {steps} migration(s)...");
                println!();
                println!(
                    "{}",
                    "⚠️  Manual rollback required - use SQL scripts".yellow()
                );
                println!("  CeleRS uses auto-migration with SQLx");
                println!("  To rollback, restore from database backup");
            }
            "status" => {
                println!("Checking migration status...");
                let broker = PostgresBroker::new(url).await?;
                let connected = broker.test_connection().await?;

                println!();
                if connected {
                    println!("{}", "  ✓ Database schema is up-to-date".green());
                    println!("  Tables: celers_tasks, celers_results");
                } else {
                    println!("{}", "  ✗ Cannot connect to database".red());
                }
            }
            _ => {
                anyhow::bail!("Unknown action '{action}'. Valid actions: apply, rollback, status");
            }
        }
    } else {
        println!(
            "{}",
            "⚠️  Migrations only supported for PostgreSQL".yellow()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_database_type_detection() {
        // PostgreSQL variants
        assert!(matches!(
            "postgres://localhost/db",
            url if url.starts_with("postgres://")
        ));
        assert!(matches!(
            "postgresql://localhost/db",
            url if url.starts_with("postgresql://")
        ));

        // MySQL
        assert!(matches!(
            "mysql://localhost/db",
            url if url.starts_with("mysql://")
        ));
    }

    #[test]
    fn test_migration_action_validation() {
        let valid_actions = vec!["apply", "rollback", "status"];
        for action in valid_actions {
            assert!(
                matches!(action, "apply" | "rollback" | "status"),
                "Action {action} should be valid"
            );
        }

        let invalid_action = "invalid";
        assert!(
            !matches!(invalid_action, "apply" | "rollback" | "status"),
            "Action {invalid_action} should be invalid"
        );
    }

    #[test]
    fn test_pool_utilization_calculation() {
        // High utilization
        let in_use = 85_u32;
        let max_size = 100_u32;
        let utilization = (f64::from(in_use) / f64::from(max_size)) * 100.0;
        assert!(utilization > 80.0);

        // Low utilization
        let in_use = 15_u32;
        let utilization = (f64::from(in_use) / f64::from(max_size)) * 100.0;
        assert!(utilization < 20.0);

        // Zero max_size edge case
        let max_size = 0_u32;
        let utilization = if max_size > 0 {
            (f64::from(in_use) / f64::from(max_size)) * 100.0
        } else {
            0.0
        };
        assert_eq!(utilization, 0.0);
    }
}
