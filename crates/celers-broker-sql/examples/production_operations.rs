//! Production Operations Toolkit Example
//!
//! Demonstrates the production operations utilities for disaster recovery,
//! performance testing, deployment validation, and query optimization.
//!
//! # Usage
//!
//! ```bash
//! # Set database URL
//! export DATABASE_URL="mysql://root:password@localhost/celers_dev"
//!
//! # Run the example
//! cargo run --example production_operations
//! ```

use celers_broker_sql::MysqlBroker;
use celers_core::{Broker, SerializedTask};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== CeleRS MySQL Broker: Production Operations Toolkit ===\n");

    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers_dev".to_string());

    // Create broker
    println!("✓ Connecting to MySQL broker...");
    let broker = MysqlBroker::new(&database_url).await?;

    // Run migrations
    println!("✓ Running migrations...");
    broker.migrate().await?;

    println!("\n--- Demo 1: Migration Verification ---");
    println!("  Verifying database schema integrity...");

    match broker.verify_migrations().await {
        Ok(report) => {
            println!("  ✓ Migration verification complete:");
            println!("    - Applied migrations: {}", report.applied_count);
            println!("    - Missing migrations: {}", report.missing_count);
            println!("    - Schema valid: {}", report.schema_valid);
            println!("    - Complete: {}", report.is_complete);

            if !report.applied_migrations.is_empty() {
                println!("\n  Applied migrations:");
                for migration in &report.applied_migrations {
                    println!("    ✓ {}", migration);
                }
            }

            if !report.missing_migrations.is_empty() {
                println!("\n  ⚠ Missing migrations:");
                for migration in &report.missing_migrations {
                    println!("    ✗ {}", migration);
                }
            }

            if report.is_complete {
                println!("\n  ✓ All migrations applied successfully!");
            } else {
                println!("\n  ⚠ Some migrations are missing. Run broker.migrate() to apply them.");
            }
        }
        Err(e) => {
            println!("  ✗ Migration verification failed: {}", e);
        }
    }

    println!("\n  💡 Use Case: Run this check in CI/CD pipelines before deployment");
    println!("     to ensure database schema is up-to-date.");

    println!("\n--- Demo 2: Load Generation for Performance Testing ---");
    println!("  Generating synthetic load for benchmarking...");

    // Generate 100 test tasks with 1KB payload and random priorities
    let task_ids = broker
        .generate_load(100, "load_test", 1024, Some((1, 10)))
        .await?;

    println!("  ✓ Generated {} test tasks", task_ids.len());

    // Get queue statistics
    let stats = broker.get_statistics().await?;
    println!("  ✓ Current queue size: {} pending tasks", stats.pending);

    println!("\n  💡 Use Cases:");
    println!("     - Load testing before production deployment");
    println!("     - Capacity planning for peak traffic");
    println!("     - Stress testing connection pool and database");
    println!("     - Benchmarking different configurations");

    println!("\n--- Demo 3: Query Performance Profiling ---");
    println!("  Analyzing query performance (requires performance_schema)...");

    // Profile queries taking more than 1ms
    match broker.profile_query_performance(1.0, 10).await {
        Ok(profiles) => {
            if profiles.is_empty() {
                println!("  ℹ No slow queries found (all queries < 1ms)");
                println!("  💡 Try lowering the threshold or generating more load");
            } else {
                println!("  ✓ Found {} slow queries:\n", profiles.len());

                for (i, profile) in profiles.iter().enumerate().take(5) {
                    println!("  Query #{}: ", i + 1);
                    println!("    Digest: {}", profile.query_digest);
                    println!("    Executions: {}", profile.execution_count);
                    println!("    Avg time: {:.2}ms", profile.avg_execution_time_ms);
                    println!("    Rows examined: {}", profile.total_rows_examined);
                    println!("    Rows sent: {}", profile.total_rows_sent);

                    if profile.needs_optimization {
                        println!("    ⚠ NEEDS OPTIMIZATION");
                        if profile.no_index_used_count > 0 {
                            println!(
                                "      - No index used: {} times",
                                profile.no_index_used_count
                            );
                        }
                        if profile.no_good_index_used_count > 0 {
                            println!(
                                "      - Suboptimal index: {} times",
                                profile.no_good_index_used_count
                            );
                        }
                    } else {
                        println!("    ✓ Query is optimized");
                    }
                    println!();
                }
            }
        }
        Err(e) => {
            println!("  ℹ Performance profiling unavailable: {}", e);
            println!("  💡 Enable performance_schema in MySQL configuration:");
            println!("     SET GLOBAL performance_schema = ON;");
        }
    }

    println!("  💡 Use Cases:");
    println!("     - Identify and optimize slow queries in production");
    println!("     - Find missing or unused indexes");
    println!("     - Optimize table access patterns");
    println!("     - Performance troubleshooting");

    println!("\n--- Demo 4: Simulating Failed Tasks ---");
    println!("  Creating tasks that will fail and move to DLQ...");

    // Create some tasks
    for i in 0..10 {
        let task = SerializedTask::new(format!("payment_processing_{}", i % 3), vec![1, 2, 3, 4]);
        broker.enqueue(task).await?;
    }

    println!("  ✓ Enqueued 10 tasks");

    // Simulate failures by dequeuing and rejecting
    println!("  Simulating task failures (rejecting without requeue)...");
    let mut failed_count = 0;
    for _ in 0..5 {
        if let Some(msg) = broker.dequeue().await? {
            // Reject without requeue to simulate permanent failure
            broker
                .reject(&msg.task.metadata.id, msg.receipt_handle.as_deref(), false)
                .await?;
            failed_count += 1;
        }
    }
    println!("  ✓ Simulated {} task failures", failed_count);

    // Wait a bit for tasks to reach max retries and move to DLQ
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check DLQ
    let stats = broker.get_statistics().await?;
    println!("  ✓ DLQ size: {} tasks", stats.dlq);

    println!("\n--- Demo 5: Batch Replay from DLQ ---");
    println!("  Recovering failed tasks from Dead Letter Queue...");

    if stats.dlq > 0 {
        // Replay all payment processing tasks that failed with 1+ retries
        let replayed = broker
            .replay_dlq_batch(Some("payment"), Some(1), 100)
            .await?;

        println!("  ✓ Replayed {} tasks from DLQ", replayed);

        // Check queue size after replay
        let stats_after = broker.get_statistics().await?;
        println!(
            "  ✓ Queue status after replay: {} pending, {} DLQ",
            stats_after.pending, stats_after.dlq
        );

        println!("\n  💡 Use Cases:");
        println!("     - Recover from systematic failures after bug fix");
        println!("     - Replay failed payment transactions");
        println!("     - Bulk retry of failed notification deliveries");
        println!("     - Disaster recovery operations");
    } else {
        println!("  ℹ No tasks in DLQ to replay");
        println!("  💡 In production, this feature is critical for disaster recovery:");
        println!("     - Replay all failed tasks: replay_dlq_batch(None, None, 1000)");
        println!("     - Replay specific tasks: replay_dlq_batch(Some(\"payment\"), Some(3), 100)");
    }

    println!("\n--- Demo 6: Connection Health Monitoring ---");
    println!("  Checking connection pool health...");

    match broker.check_connection_health().await {
        Ok(true) => {
            println!("  ✓ Connection pool is healthy");
        }
        Ok(false) => {
            println!("  ⚠ Connection pool is degraded (high utilization or slow queries)");
        }
        Err(e) => {
            println!("  ✗ Connection pool has critical issues: {}", e);
        }
    }

    // Get pool health metrics
    let pool_health = broker.get_pool_health().await?;
    println!("  Connection pool metrics:");
    println!("    - Total connections: {}", pool_health.total_connections);
    println!(
        "    - Active connections: {}",
        pool_health.active_connections
    );
    println!("    - Idle connections: {}", pool_health.idle_connections);
    println!(
        "    - Utilization: {:.1}%",
        pool_health.pool_utilization_percent
    );

    println!("\n  💡 Use Cases:");
    println!("     - Health check endpoints for load balancers");
    println!("     - Auto-scaling decisions");
    println!("     - Proactive alerting before issues occur");
    println!("     - Capacity planning");

    println!("\n--- Demo 7: Cleanup ---");
    println!("  Cleaning up test data...");

    // Purge all pending test tasks
    let purged = broker
        .delete_tasks_by_criteria(None, Duration::from_secs(0))
        .await?;
    println!("  ✓ Cleaned up {} test tasks", purged);

    println!("\n=== Production Operations Toolkit Demo Complete ===\n");

    println!("📚 Key Takeaways:");
    println!();
    println!("1. Migration Verification");
    println!("   → verify_migrations() - Validate schema in CI/CD");
    println!();
    println!("2. Load Generation");
    println!("   → generate_load() - Benchmark and capacity planning");
    println!();
    println!("3. Query Profiling");
    println!("   → profile_query_performance() - Find slow queries");
    println!();
    println!("4. DLQ Batch Replay");
    println!("   → replay_dlq_batch() - Disaster recovery");
    println!();
    println!("5. Connection Health");
    println!("   → check_connection_health() - Proactive monitoring");
    println!();
    println!("💡 These utilities are essential for production operations:");
    println!("   - Deployment safety and validation");
    println!("   - Performance testing and optimization");
    println!("   - Disaster recovery and task replay");
    println!("   - Proactive health monitoring");
    println!("   - Capacity planning and scaling");

    Ok(())
}
