//! Database operations command implementations.

use super::utils::mask_password;
use celers_core::Broker;
use colored::Colorize;

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
            let pool = sqlx::postgres::PgPool::connect(url).await?;
            let elapsed = start.elapsed();
            println!(
                "{}",
                format!("✓ Connected successfully in {elapsed:?}").green()
            );

            // Get database version
            let row: (String,) = sqlx::query_as("SELECT version()").fetch_one(&pool).await?;
            println!("  {} {}", "Version:".cyan(), row.0);

            // Get current database name
            let row: (String,) = sqlx::query_as("SELECT current_database()")
                .fetch_one(&pool)
                .await?;
            println!("  {} {}", "Database:".cyan(), row.0);

            if benchmark {
                println!();
                println!("{}", "Running benchmark...".cyan().bold());
                let mut times = Vec::new();
                for _ in 0..10 {
                    let query_start = std::time::Instant::now();
                    let _: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await?;
                    times.push(query_start.elapsed());
                }

                let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / times.len() as u128;
                let min = times.iter().map(|t| t.as_micros()).min().unwrap_or(0);
                let max = times.iter().map(|t| t.as_micros()).max().unwrap_or(0);

                println!("  {} {avg}µs", "Avg query time:".yellow());
                println!("  {} {min}µs", "Min query time:".yellow());
                println!("  {} {max}µs", "Max query time:".yellow());
            }

            pool.close().await;
        }
        "MySQL" => {
            let pool = sqlx::mysql::MySqlPool::connect(url).await?;
            let elapsed = start.elapsed();
            println!(
                "{}",
                format!("✓ Connected successfully in {elapsed:?}").green()
            );

            let row: (String,) = sqlx::query_as("SELECT VERSION()").fetch_one(&pool).await?;
            println!("  {} {}", "Version:".cyan(), row.0);

            if benchmark {
                println!();
                println!("{}", "Running benchmark...".cyan().bold());
                let mut times = Vec::new();
                for _ in 0..10 {
                    let query_start = std::time::Instant::now();
                    let _: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await?;
                    times.push(query_start.elapsed());
                }

                let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / times.len() as u128;
                let min = times.iter().map(|t| t.as_micros()).min().unwrap_or(0);
                let max = times.iter().map(|t| t.as_micros()).max().unwrap_or(0);

                println!("  {} {avg}µs", "Avg query time:".yellow());
                println!("  {} {min}µs", "Min query time:".yellow());
                println!("  {} {max}µs", "Max query time:".yellow());
            }

            pool.close().await;
        }
        _ => {
            println!(
                "{}",
                "✗ Unsupported database type. Use PostgreSQL or MySQL URL."
                    .red()
                    .bold()
            );
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
        let pool = sqlx::postgres::PgPool::connect(url).await?;

        // Check connection count
        let row: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
        )
        .fetch_one(&pool)
        .await?;
        println!("  {} {}", "Active connections:".cyan(), row.0);

        // Check database size
        let row: (String,) =
            sqlx::query_as("SELECT pg_size_pretty(pg_database_size(current_database()))")
                .fetch_one(&pool)
                .await?;
        println!("  {} {}", "Database size:".cyan(), row.0);

        // Check uptime
        let row: (String,) = sqlx::query_as("SELECT now() - pg_postmaster_start_time()::text")
            .fetch_one(&pool)
            .await
            .unwrap_or(("N/A".to_string(),));
        println!("  {} {}", "Uptime:".cyan(), row.0);

        // Check for locks
        let row: (i64,) = sqlx::query_as("SELECT count(*) FROM pg_locks WHERE NOT granted")
            .fetch_one(&pool)
            .await?;
        if row.0 > 0 {
            println!(
                "  {} {} waiting locks",
                "⚠".yellow(),
                row.0.to_string().yellow()
            );
        } else {
            println!("  {} No waiting locks", "✓".green());
        }

        println!();
        println!("{}", "✓ Database health check complete".green().bold());

        pool.close().await;
    } else if url.starts_with("mysql://") {
        let pool = sqlx::mysql::MySqlPool::connect(url).await?;

        let row: (String,) = sqlx::query_as("SELECT VERSION()").fetch_one(&pool).await?;
        println!("  {} {}", "Version:".cyan(), row.0);

        println!();
        println!("{}", "✓ Database health check complete".green().bold());

        pool.close().await;
    } else {
        println!("{}", "✗ Unsupported database URL format".red().bold());
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
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;

        println!("Pool Configuration:");
        println!("  {} {}", "Max connections:".cyan(), 5);
        println!("  {} {}", "Current size:".cyan(), pool.size());

        // Test pool performance
        println!();
        println!("{}", "Pool Performance Test:".cyan().bold());

        let start = std::time::Instant::now();
        let mut handles = vec![];
        for _ in 0..10 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let _: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await.ok()?;
                Some(())
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        let elapsed = start.elapsed();
        println!("  {} {elapsed:?}", "10 concurrent queries:".yellow());
        println!("  {} {:?}", "Avg per query:".yellow(), elapsed / 10);

        pool.close().await;
    } else {
        println!(
            "{}",
            "✗ Pool statistics only available for PostgreSQL".red()
        );
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
