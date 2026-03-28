//! Connection health, resilience, and metrics update methods

use celers_core::{CelersError, Result};
use std::time::Duration;

use crate::types::{BatchSizeRecommendation, DetailedHealthStatus};
use crate::PostgresBroker;

#[cfg(feature = "metrics")]
use celers_metrics::{
    DLQ_SIZE, POSTGRES_POOL_IDLE, POSTGRES_POOL_IN_USE, POSTGRES_POOL_MAX_SIZE, POSTGRES_POOL_SIZE,
    PROCESSING_QUEUE_SIZE, QUEUE_SIZE,
};

// Connection Health & Resilience Methods
impl PostgresBroker {
    /// Perform detailed health check with diagnostics and recommendations
    ///
    /// This provides a comprehensive health assessment including connection status,
    /// query performance, pool metrics, and actionable recommendations.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let health = broker.check_health_detailed().await?;
    /// if !health.healthy {
    ///     eprintln!("Health check failed!");
    ///     for warning in &health.warnings {
    ///         eprintln!("Warning: {}", warning);
    ///     }
    /// }
    /// for rec in &health.recommendations {
    ///     println!("Recommendation: {}", rec);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_health_detailed(&self) -> Result<DetailedHealthStatus> {
        let start = std::time::Instant::now();

        // Test connection and get version
        let version_result = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(&self.pool)
            .await;

        let (connection_ok, database_version) = match version_result {
            Ok(v) => (true, v),
            Err(e) => (false, format!("Connection failed: {}", e)),
        };

        let query_latency_ms = start.elapsed().as_millis() as i64;

        // Get metrics even if connection failed (from cached pool state)
        let pool_metrics = self.get_pool_metrics();

        // Try to get queue stats (may fail if connection is down)
        let queue_stats = self.get_statistics().await.unwrap_or_default();

        // Generate warnings and recommendations
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();

        // Check connection health
        if !connection_ok {
            warnings.push("Database connection failed".to_string());
            recommendations
                .push("Check database availability and network connectivity".to_string());
        } else if query_latency_ms > 1000 {
            warnings.push(format!("High query latency: {}ms", query_latency_ms));
            recommendations
                .push("Database may be overloaded or network latency is high".to_string());
        }

        // Check pool utilization
        let utilization = if pool_metrics.max_size > 0 {
            (pool_metrics.in_use as f64 / pool_metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        if utilization > 90.0 {
            warnings.push(format!("Critical pool utilization: {:.1}%", utilization));
            recommendations.push(
                "Increase max_connections immediately to prevent service degradation".to_string(),
            );
        } else if utilization > 80.0 {
            warnings.push(format!("High pool utilization: {:.1}%", utilization));
            recommendations.push("Consider increasing max_connections".to_string());
        }

        // Check idle connections
        if pool_metrics.idle == 0 && pool_metrics.in_use > 0 {
            warnings.push("No idle connections available".to_string());
            recommendations.push("Pool exhaustion detected - increase max_connections".to_string());
        }

        // Check queue health
        if queue_stats.processing > queue_stats.pending * 5 {
            warnings.push("Many tasks stuck in processing state".to_string());
            recommendations
                .push("Run recover_stuck_tasks() to recover abandoned tasks".to_string());
        }

        if queue_stats.dlq > 1000 {
            warnings.push(format!("Large DLQ size: {} tasks", queue_stats.dlq));
            recommendations.push("Review and address failed tasks in DLQ".to_string());
        }

        // Overall health determination
        let healthy = connection_ok && utilization < 95.0 && query_latency_ms < 5000;

        Ok(DetailedHealthStatus {
            healthy,
            connection_ok,
            query_latency_ms,
            pool_metrics,
            queue_stats,
            database_version,
            warnings,
            recommendations,
        })
    }

    /// Test connection with automatic retry on transient failures
    ///
    /// Attempts to connect to the database with exponential backoff retry logic.
    /// Useful for startup health checks and connection validation.
    ///
    /// # Arguments
    /// * `max_retries` - Maximum number of retry attempts
    /// * `initial_delay_ms` - Initial delay between retries in milliseconds
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Test with 5 retries, starting with 100ms delay
    /// match broker.test_connection_with_retry(5, 100).await {
    ///     Ok(_) => println!("Connection successful"),
    ///     Err(e) => eprintln!("Connection failed after retries: {}", e),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn test_connection_with_retry(
        &self,
        max_retries: u32,
        initial_delay_ms: u64,
    ) -> Result<String> {
        let mut last_error = None;

        for attempt in 0..=max_retries {
            match sqlx::query_scalar::<_, String>("SELECT version()")
                .fetch_one(&self.pool)
                .await
            {
                Ok(version) => {
                    if attempt > 0 {
                        tracing::info!("Connection successful after {} attempt(s)", attempt + 1);
                    }
                    return Ok(version);
                }
                Err(e) => {
                    last_error = Some(e);

                    if attempt < max_retries {
                        let delay_ms = initial_delay_ms * 2_u64.pow(attempt);
                        tracing::warn!(
                            "Connection attempt {} failed, retrying in {}ms: {}",
                            attempt + 1,
                            delay_ms,
                            last_error.as_ref().expect("error just set in Err branch")
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        Err(CelersError::Other(format!(
            "Connection failed after {} retries: {}",
            max_retries,
            last_error.expect("error occurred during retries")
        )))
    }

    /// Warm up the connection pool by pre-establishing connections
    ///
    /// Creates connections up to the specified count to avoid cold start latency.
    /// Useful for reducing latency on first requests after application startup.
    ///
    /// # Arguments
    ///
    /// * `target_connections` - Number of connections to pre-establish (capped at max_size)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Warm up 10 connections on startup
    /// let warmed = broker.warmup_connection_pool(10).await?;
    /// println!("Warmed up {} connections", warmed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn warmup_connection_pool(&self, target_connections: u32) -> Result<u32> {
        let pool_metrics = self.get_pool_metrics();
        let max_warmup = target_connections.min(pool_metrics.max_size);

        if max_warmup == 0 {
            return Ok(0);
        }

        tracing::info!(
            target_connections,
            max_warmup,
            current_size = pool_metrics.size,
            "Starting connection pool warmup"
        );

        let mut warmed = 0u32;
        let mut tasks = Vec::new();

        for i in 0..max_warmup {
            let pool = self.pool.clone();
            let task = tokio::spawn(async move {
                match sqlx::query_scalar::<_, i32>("SELECT 1")
                    .fetch_one(&pool)
                    .await
                {
                    Ok(_) => {
                        tracing::debug!(connection = i + 1, "Connection warmed up");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::warn!(connection = i + 1, error = %e, "Failed to warm up connection");
                        Err(e)
                    }
                }
            });
            tasks.push(task);
        }

        // Wait for all warmup tasks to complete
        for task in tasks {
            match task.await {
                Ok(Ok(())) => warmed += 1,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Warmup connection failed");
                }
                Err(e) => {
                    tracing::error!(error = %e, "Warmup task panicked");
                }
            }
        }

        tracing::info!(
            warmed,
            target = max_warmup,
            "Connection pool warmup complete"
        );

        Ok(warmed)
    }

    /// Get recommended batch sizes based on current workload and pool configuration
    ///
    /// Analyzes the current queue state and connection pool to recommend
    /// optimal batch sizes for different operations.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let recommendation = broker.get_recommended_batch_size().await?;
    /// println!("Recommended enqueue batch size: {}", recommendation.enqueue_batch_size);
    /// println!("Reasoning: {}", recommendation.reasoning);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_recommended_batch_size(&self) -> Result<BatchSizeRecommendation> {
        let stats = self.get_statistics().await?;
        let pool_metrics = self.get_pool_metrics();

        // Calculate pool pressure
        let pool_utilization = if pool_metrics.max_size > 0 {
            pool_metrics.in_use as f64 / pool_metrics.max_size as f64
        } else {
            0.0
        };

        // Base batch sizes on workload
        let queue_depth = stats.pending + stats.processing;

        let (enqueue_batch_size, dequeue_batch_size, ack_batch_size, reasoning) =
            if pool_utilization > 0.8 {
                // High pool pressure - use smaller batches to avoid connection exhaustion
                (
                    50,
                    10,
                    50,
                    format!(
                        "High pool utilization ({:.1}%) - using smaller batches to reduce connection time. \
                        Consider increasing max_connections from {} to {}.",
                        pool_utilization * 100.0,
                        pool_metrics.max_size,
                        pool_metrics.max_size * 2
                    )
                )
            } else if queue_depth > 10000 {
                // High queue depth - use larger batches for throughput
                (
                    500,
                    50,
                    200,
                    format!(
                        "High queue depth ({} tasks) - using larger batches for maximum throughput. \
                        Pool utilization is healthy at {:.1}%.",
                        queue_depth,
                        pool_utilization * 100.0
                    )
                )
            } else if queue_depth > 1000 {
                // Medium queue depth - balanced approach
                (
                    200,
                    25,
                    100,
                    format!(
                        "Medium queue depth ({} tasks) - balanced batch sizes. \
                        Pool utilization: {:.1}%.",
                        queue_depth,
                        pool_utilization * 100.0
                    ),
                )
            } else {
                // Low queue depth - smaller batches for latency
                (
                    100,
                    10,
                    50,
                    format!(
                        "Low queue depth ({} tasks) - using smaller batches for lower latency. \
                        Pool utilization: {:.1}%.",
                        queue_depth,
                        pool_utilization * 100.0
                    ),
                )
            };

        Ok(BatchSizeRecommendation {
            enqueue_batch_size,
            dequeue_batch_size,
            ack_batch_size,
            reasoning,
        })
    }

    /// Detect potential connection leaks
    ///
    /// Analyzes connection pool state over time to identify potential leaks.
    /// Returns a report of suspicious patterns.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let report = broker.detect_connection_leaks().await?;
    /// println!("{}", report);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn detect_connection_leaks(&self) -> Result<String> {
        let initial_metrics = self.get_pool_metrics();

        // Wait a bit and check again
        tokio::time::sleep(Duration::from_secs(5)).await;

        let final_metrics = self.get_pool_metrics();

        let mut report = String::from("Connection Leak Detection Report:\n\n");

        report.push_str("Initial state:\n");
        report.push_str(&format!("  - In use: {}\n", initial_metrics.in_use));
        report.push_str(&format!("  - Idle: {}\n", initial_metrics.idle));
        report.push_str(&format!("  - Total: {}\n\n", initial_metrics.size));

        report.push_str("After 5 seconds:\n");
        report.push_str(&format!("  - In use: {}\n", final_metrics.in_use));
        report.push_str(&format!("  - Idle: {}\n", final_metrics.idle));
        report.push_str(&format!("  - Total: {}\n\n", final_metrics.size));

        // Analyze changes
        if final_metrics.in_use > initial_metrics.in_use {
            report.push_str("⚠️  WARNING: Connections in use increased without being released.\n");
            report.push_str("   This may indicate a connection leak.\n");
            report.push_str(
                "   Ensure all database operations use proper error handling and cleanup.\n\n",
            );
        } else if final_metrics.in_use == initial_metrics.in_use && final_metrics.in_use > 0 {
            report.push_str("⚠️  INFO: Connections remain in use without change.\n");
            report.push_str("   This is normal during active workload but monitor for growth.\n\n");
        } else {
            report.push_str("✓ No obvious connection leaks detected.\n\n");
        }

        // Check for high sustained usage
        let utilization = if final_metrics.max_size > 0 {
            (final_metrics.in_use as f64 / final_metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        if utilization > 80.0 {
            report.push_str(&format!(
                "⚠️  HIGH UTILIZATION: {:.1}% of connections in use.\n",
                utilization
            ));
            report.push_str(
                "   Monitor for connection exhaustion and consider increasing pool size.\n",
            );
        }

        Ok(report)
    }
}

impl PostgresBroker {
    /// Update Prometheus metrics gauges for queue sizes and connection pool
    ///
    /// This should be called periodically (e.g., every few seconds) to keep
    /// metrics up to date. Not part of the Broker trait, but useful for monitoring.
    #[cfg(feature = "metrics")]
    pub async fn update_metrics(&self) -> Result<()> {
        // Get pending tasks count
        let pending_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending'")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Get processing tasks count
        let processing_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = 'processing'")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to get processing count: {}", e))
                })?;

        // Get DLQ count
        let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_dead_letter_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;

        // Update queue gauges
        QUEUE_SIZE.set(pending_count as f64);
        PROCESSING_QUEUE_SIZE.set(processing_count as f64);
        DLQ_SIZE.set(dlq_count as f64);

        // Update connection pool metrics
        let pool_metrics = self.get_pool_metrics();
        POSTGRES_POOL_MAX_SIZE.set(pool_metrics.max_size as f64);
        POSTGRES_POOL_SIZE.set(pool_metrics.size as f64);
        POSTGRES_POOL_IDLE.set(pool_metrics.idle as f64);
        POSTGRES_POOL_IN_USE.set(pool_metrics.in_use as f64);

        Ok(())
    }
}
