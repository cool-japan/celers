//! Database operations command implementations.

use super::utils::mask_password;
use crate::row_ext::RowExt;
use crate::tls_mode::{mysql_tls_mode_for_url, pg_tls_mode_for_url};
use celers_core::Broker;
use colored::Colorize;
use oxisql_core::Connection;
use oxisql_mysql::MyConnection;
use oxisql_postgres::PgConnection;

/// Test database connection
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

    let start = std::time::Instant::now();

    // Test connection based on database type
    match db_type {
        "PostgreSQL" => {
            let tls_mode = pg_tls_mode_for_url(url)?;
            let conn = PgConnection::connect(url, tls_mode).await?;
            let elapsed = start.elapsed();
            println!(
                "{}",
                format!("✓ Connected successfully in {elapsed:?}").green()
            );

            // Get database version
            let rows = conn.query("SELECT version()", &[]).await?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("SELECT version() returned no rows"))?;
            let version: String = row.col("version")?;
            println!("  {} {}", "Version:".cyan(), version);

            // Get current database name
            let rows = conn.query("SELECT current_database()", &[]).await?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("SELECT current_database() returned no rows"))?;
            let database: String = row.col("current_database")?;
            println!("  {} {}", "Database:".cyan(), database);

            if benchmark {
                println!();
                println!("{}", "Running benchmark...".cyan().bold());
                let mut times = Vec::new();
                for _ in 0..10 {
                    let query_start = std::time::Instant::now();
                    let rows = conn.query("SELECT 1", &[]).await?;
                    rows.into_iter()
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("SELECT 1 returned no rows"))?;
                    times.push(query_start.elapsed());
                }

                let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / times.len() as u128;
                let min = times.iter().map(|t| t.as_micros()).min().unwrap_or(0);
                let max = times.iter().map(|t| t.as_micros()).max().unwrap_or(0);

                println!("  {} {avg}µs", "Avg query time:".yellow());
                println!("  {} {min}µs", "Min query time:".yellow());
                println!("  {} {max}µs", "Max query time:".yellow());
            }
        }
        "MySQL" => {
            let tls_mode = mysql_tls_mode_for_url(url)?;
            let conn = MyConnection::connect(url, tls_mode).await?;
            let elapsed = start.elapsed();
            println!(
                "{}",
                format!("✓ Connected successfully in {elapsed:?}").green()
            );

            let rows = conn.query("SELECT VERSION()", &[]).await?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("SELECT VERSION() returned no rows"))?;
            let version: String = row.col("VERSION()")?;
            println!("  {} {}", "Version:".cyan(), version);

            if benchmark {
                println!();
                println!("{}", "Running benchmark...".cyan().bold());
                let mut times = Vec::new();
                for _ in 0..10 {
                    let query_start = std::time::Instant::now();
                    let rows = conn.query("SELECT 1", &[]).await?;
                    rows.into_iter()
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("SELECT 1 returned no rows"))?;
                    times.push(query_start.elapsed());
                }

                let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / times.len() as u128;
                let min = times.iter().map(|t| t.as_micros()).min().unwrap_or(0);
                let max = times.iter().map(|t| t.as_micros()).max().unwrap_or(0);

                println!("  {} {avg}µs", "Avg query time:".yellow());
                println!("  {} {min}µs", "Min query time:".yellow());
                println!("  {} {max}µs", "Max query time:".yellow());
            }
        }
        _ => {
            println!(
                "{}",
                "✗ Unsupported database type. Use PostgreSQL or MySQL URL."
                    .red()
                    .bold()
            );
            anyhow::bail!("unsupported database type");
        }
    }

    Ok(())
}

/// Database health check
pub async fn db_health(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Database Health Check ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let tls_mode = pg_tls_mode_for_url(url)?;
        let conn = PgConnection::connect(url, tls_mode).await?;

        // Check connection count
        let rows = conn
            .query(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
                &[],
            )
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| {
            anyhow::anyhow!("SELECT count(*) FROM pg_stat_activity ... returned no rows")
        })?;
        let active_connections: i64 = row.col("count")?;
        println!("  {} {}", "Active connections:".cyan(), active_connections);

        // Check database size
        let rows = conn
            .query(
                "SELECT pg_size_pretty(pg_database_size(current_database()))",
                &[],
            )
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| {
            anyhow::anyhow!("SELECT pg_size_pretty(pg_database_size(...)) returned no rows")
        })?;
        let database_size: String = row.col("pg_size_pretty")?;
        println!("  {} {}", "Database size:".cyan(), database_size);

        // Check uptime
        let uptime: String = match conn
            .query("SELECT now() - pg_postmaster_start_time()::text", &[])
            .await
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.col_idx::<String>(0).ok())
        {
            Some(v) => v,
            None => "N/A".to_string(),
        };
        println!("  {} {}", "Uptime:".cyan(), uptime);

        // Check for locks
        let rows = conn
            .query("SELECT count(*) FROM pg_locks WHERE NOT granted", &[])
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("SELECT count(*) FROM pg_locks ... returned no rows"))?;
        let waiting_locks: i64 = row.col("count")?;
        if waiting_locks > 0 {
            println!(
                "  {} {} waiting locks",
                "⚠".yellow(),
                waiting_locks.to_string().yellow()
            );
        } else {
            println!("  {} No waiting locks", "✓".green());
        }

        println!();
        println!("{}", "✓ Database health check complete".green().bold());
    } else if url.starts_with("mysql://") {
        let tls_mode = mysql_tls_mode_for_url(url)?;
        let conn = MyConnection::connect(url, tls_mode).await?;

        let rows = conn.query("SELECT VERSION()", &[]).await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("SELECT VERSION() returned no rows"))?;
        let version: String = row.col("VERSION()")?;
        println!("  {} {}", "Version:".cyan(), version);

        println!();
        println!("{}", "✓ Database health check complete".green().bold());
    } else {
        println!("{}", "✗ Unsupported database URL format".red().bold());
        anyhow::bail!("unsupported database URL format");
    }

    Ok(())
}

/// Database pool statistics
pub async fn db_pool_stats(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Database Pool Statistics ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        // NOTE: `oxisql_postgres::PgConnection` is a single mutex-serialized
        // connection (`Arc<Mutex<tokio_postgres::Client>>`), not a real
        // multi-connection pool like the previous SQL toolkit's
        // `PgPoolOptions` — there is no `oxisql-pool` wiring in this crate.
        // `PgConnection` is `Clone` (the clone shares the same underlying
        // connection), which lets the concurrent-query throughput shape of
        // this probe survive the migration, but the "Max connections" /
        // "Current size" pool metrics the old toolkit exposed have no
        // equivalent here and are intentionally dropped rather than
        // fabricated.
        let tls_mode = pg_tls_mode_for_url(url)?;
        let conn = PgConnection::connect(url, tls_mode).await?;

        // Test concurrent query performance
        println!("{}", "Concurrent Query Test:".cyan().bold());

        let start = std::time::Instant::now();
        let mut handles = vec![];
        for _ in 0..10 {
            let conn = conn.clone();
            handles.push(tokio::spawn(async move {
                let rows = conn.query("SELECT 1", &[]).await.ok()?;
                rows.into_iter().next()?;
                Some(())
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        let elapsed = start.elapsed();
        println!("  {} {elapsed:?}", "10 concurrent queries:".yellow());
        println!("  {} {:?}", "Avg per query:".yellow(), elapsed / 10);
    } else {
        println!(
            "{}",
            "✗ Pool statistics only available for PostgreSQL".red()
        );
        anyhow::bail!("pool statistics only available for PostgreSQL");
    }

    Ok(())
}

/// Database migration management
pub async fn db_migrate(url: &str, action: &str, steps: usize) -> anyhow::Result<()> {
    println!("{}", "=== Database Migration ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!("Action: {}", action.yellow());
    println!();

    match action {
        "status" => {
            println!("{}", "Migration Status:".cyan().bold());
            println!();
            println!(
                "{}",
                "ℹ Migration status requires SQLx migrations directory".cyan()
            );
            println!("  Ensure ./migrations directory exists with migration files");
        }
        "up" => {
            println!(
                "{}",
                format!("Running {steps} pending migrations...").cyan()
            );
            println!();
            println!(
                "{}",
                "ℹ Running migrations requires SQLx migrations directory".cyan()
            );
            println!("  Use: sqlx migrate run");
        }
        "down" => {
            println!("{}", format!("Rolling back {steps} migrations...").cyan());
            println!();
            println!(
                "{}",
                "ℹ Rolling back migrations requires SQLx migrations directory".cyan()
            );
            println!("  Use: sqlx migrate revert");
        }
        _ => {
            println!("{}", format!("✗ Unknown migration action: {action}").red());
            println!("  Available actions: status, up, down");
            anyhow::bail!("unknown migration action: {action}");
        }
    }

    Ok(())
}

/// Run live dashboard
pub async fn run_dashboard(broker_url: &str, queue: &str, refresh_secs: u64) -> anyhow::Result<()> {
    println!("{}", "=== CeleRS Live Dashboard ===".bold().green());
    println!(
        "{}",
        format!("Refreshing every {refresh_secs} seconds (Ctrl+C to stop)").dimmed()
    );
    println!();

    let broker = celers_broker_redis::RedisBroker::new(broker_url, queue)?;

    loop {
        // Clear screen
        print!("\x1B[2J\x1B[1;1H");

        println!("{}", "╔══════════════════════════════════════╗".cyan());
        println!(
            "{}",
            "║      CeleRS Live Dashboard           ║".cyan().bold()
        );
        println!("{}", "╚══════════════════════════════════════╝".cyan());
        println!();

        let now = chrono::Utc::now();
        println!(
            "{}",
            format!("Last updated: {}", now.format("%Y-%m-%d %H:%M:%S")).dimmed()
        );
        println!();

        // Queue metrics
        let queue_size = broker.queue_size().await.unwrap_or(0);
        let dlq_size = broker.dlq_size().await.unwrap_or(0);

        println!("{}", "Queue Status:".cyan().bold());
        println!("  {} {}", "Pending:".yellow(), queue_size);
        println!("  {} {}", "DLQ:".yellow(), dlq_size);
        println!();

        // Worker metrics
        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;

        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await
            .unwrap_or_default();

        println!("{}", "Workers:".cyan().bold());
        println!("  {} {}", "Active:".yellow(), worker_keys.len());
        println!();

        // Memory
        if let Ok(info) = redis::cmd("INFO")
            .arg("memory")
            .query_async::<String>(&mut conn)
            .await
        {
            for line in info.lines() {
                if line.starts_with("used_memory_human:") {
                    let memory = line.split(':').nth(1).unwrap_or("N/A");
                    println!("{}", "Redis Memory:".cyan().bold());
                    println!("  {} {}", "Used:".yellow(), memory);
                    break;
                }
            }
        }

        println!();

        // Health status
        if dlq_size > 0 {
            println!("{}", format!("⚠ {dlq_size} tasks in DLQ").yellow().bold());
        }
        if worker_keys.is_empty() && queue_size > 0 {
            println!("{}", "⚠ No workers available!".red().bold());
        }
        if dlq_size == 0 && (worker_keys.is_empty() || queue_size == 0) {
            println!("{}", "✓ System healthy".green().bold());
        }

        println!();
        println!(
            "{}",
            format!("Press Ctrl+C to exit | Refresh: {refresh_secs}s").dimmed()
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(refresh_secs)).await;
    }
}
