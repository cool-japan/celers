//! Worker Pool Example for CeleRS MySQL Broker
//!
//! This example demonstrates a production-ready worker pool implementation
//! with proper error handling, graceful shutdown, and health monitoring.
//!
//! Run with:
//! ```bash
//! export DATABASE_URL="mysql://root:password@localhost/celers_dev"
//! cargo run --example worker_pool
//! ```

use celers_broker_sql::{MysqlBroker, PoolConfig};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Example task types
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailTask {
    to: String,
    subject: String,
    body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingTask {
    id: u64,
    data: Vec<u8>,
}

/// Worker pool configuration
#[derive(Clone)]
struct WorkerConfig {
    worker_id: String,
    max_concurrent_tasks: usize,
    poll_interval: Duration,
    retry_delay: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
            max_concurrent_tasks: 10,
            poll_interval: Duration::from_millis(100),
            retry_delay: Duration::from_secs(5),
        }
    }
}

/// Task processor trait
#[async_trait::async_trait]
trait TaskProcessor: Send + Sync {
    async fn process(&self, task_name: &str, payload: &[u8]) -> Result<(), String>;
}

/// Email task processor implementation
struct EmailProcessor;

#[async_trait::async_trait]
impl TaskProcessor for EmailProcessor {
    async fn process(&self, task_name: &str, payload: &[u8]) -> Result<(), String> {
        if task_name == "send_email" {
            let email: EmailTask = serde_json::from_slice(payload)
                .map_err(|e| format!("Failed to deserialize email task: {}", e))?;

            info!("Sending email to: {}, subject: {}", email.to, email.subject);

            // Simulate email sending
            sleep(Duration::from_millis(500)).await;

            info!("Email sent successfully to: {}", email.to);
            Ok(())
        } else {
            Err(format!("Unknown task type: {}", task_name))
        }
    }
}

/// Data processing task processor
struct DataProcessor;

#[async_trait::async_trait]
impl TaskProcessor for DataProcessor {
    async fn process(&self, task_name: &str, payload: &[u8]) -> Result<(), String> {
        if task_name == "process_data" {
            let task: ProcessingTask = serde_json::from_slice(payload)
                .map_err(|e| format!("Failed to deserialize processing task: {}", e))?;

            info!(
                "Processing data for ID: {}, size: {} bytes",
                task.id,
                task.data.len()
            );

            // Simulate data processing
            sleep(Duration::from_millis(1000)).await;

            info!("Data processing completed for ID: {}", task.id);
            Ok(())
        } else {
            Err(format!("Unknown task type: {}", task_name))
        }
    }
}

/// Composite task processor that delegates to specific processors
struct CompositeProcessor {
    processors: Vec<Box<dyn TaskProcessor>>,
}

impl CompositeProcessor {
    fn new() -> Self {
        Self {
            processors: vec![Box::new(EmailProcessor), Box::new(DataProcessor)],
        }
    }
}

#[async_trait::async_trait]
impl TaskProcessor for CompositeProcessor {
    async fn process(&self, task_name: &str, payload: &[u8]) -> Result<(), String> {
        for processor in &self.processors {
            match processor.process(task_name, payload).await {
                Ok(()) => return Ok(()),
                Err(_) => continue,
            }
        }
        Err(format!("No processor found for task: {}", task_name))
    }
}

/// Worker pool that processes tasks from the broker
struct WorkerPool {
    broker: Arc<MysqlBroker>,
    processor: Arc<dyn TaskProcessor>,
    config: WorkerConfig,
    shutdown: Arc<tokio::sync::Notify>,
}

impl WorkerPool {
    fn new(broker: MysqlBroker, processor: Arc<dyn TaskProcessor>, config: WorkerConfig) -> Self {
        Self {
            broker: Arc::new(broker),
            processor,
            config,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Start the worker pool
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting worker pool: {}", self.config.worker_id);
        info!("Max concurrent tasks: {}", self.config.max_concurrent_tasks);

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let mut tasks = Vec::new();

        loop {
            // Check for shutdown signal
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("Shutdown signal received, waiting for tasks to complete...");
                    break;
                }
                _ = sleep(self.config.poll_interval) => {
                    // Continue to process tasks
                }
            }

            // Try to acquire permit for task processing
            if let Ok(permit) = semaphore.clone().try_acquire_owned() {
                match self.broker.dequeue().await {
                    Ok(Some(msg)) => {
                        let task_id = msg.task_id();
                        let task_name = msg.task_name().to_string();
                        let payload = msg.task.payload.clone();
                        let receipt_handle = msg.receipt_handle.clone();

                        info!("Dequeued task: {} ({})", task_id, task_name);

                        let broker = self.broker.clone();
                        let processor = self.processor.clone();
                        let worker_id = self.config.worker_id.clone();

                        // Spawn task to process in background
                        let task = tokio::spawn(async move {
                            let _permit = permit; // Hold permit until task completes

                            // Set worker ID on task
                            if let Err(e) = broker.set_worker_id(&task_id, &worker_id).await {
                                warn!("Failed to set worker ID: {}", e);
                            }

                            // Process the task
                            match processor.process(&task_name, &payload).await {
                                Ok(()) => {
                                    info!("Task completed successfully: {}", task_id);
                                    if let Err(e) =
                                        broker.ack(&task_id, receipt_handle.as_deref()).await
                                    {
                                        error!("Failed to acknowledge task {}: {}", task_id, e);
                                    }
                                }
                                Err(e) => {
                                    error!("Task failed: {} - {}", task_id, e);

                                    // Update error message
                                    if let Err(e) = broker.update_error_message(&task_id, &e).await
                                    {
                                        warn!("Failed to update error message: {}", e);
                                    }

                                    // Reject and requeue for retry
                                    if let Err(e) = broker
                                        .reject(&task_id, receipt_handle.as_deref(), true)
                                        .await
                                    {
                                        error!("Failed to reject task {}: {}", task_id, e);
                                    }
                                }
                            }
                        });

                        tasks.push(task);
                    }
                    Ok(None) => {
                        // No tasks available, wait before next poll
                        sleep(self.config.retry_delay).await;
                    }
                    Err(e) => {
                        error!("Error dequeueing task: {}", e);
                        sleep(self.config.retry_delay).await;
                    }
                }
            } else {
                // All workers busy, wait before checking again
                sleep(Duration::from_millis(50)).await;
            }

            // Clean up completed tasks
            tasks.retain(|task| !task.is_finished());
        }

        // Wait for all tasks to complete
        for task in tasks {
            let _ = task.await;
        }

        info!("Worker pool stopped gracefully");
        Ok(())
    }

    /// Signal shutdown to the worker pool
    fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

/// Health check task that monitors broker health
async fn health_check_task(broker: Arc<MysqlBroker>) {
    loop {
        match broker.check_health().await {
            Ok(health) => {
                if health.healthy {
                    info!(
                        "Health check: OK (MySQL {}), pool: {}/{}, tasks: {} pending, {} processing",
                        health.database_version,
                        health.idle_connections,
                        health.connection_pool_size,
                        health.pending_tasks,
                        health.processing_tasks
                    );
                } else {
                    warn!("Health check: UNHEALTHY");
                }
            }
            Err(e) => {
                error!("Health check failed: {}", e);
            }
        }

        // Get queue statistics
        match broker.get_statistics().await {
            Ok(stats) => {
                info!(
                    "Queue stats - pending: {}, processing: {}, completed: {}, failed: {}, dlq: {}",
                    stats.pending, stats.processing, stats.completed, stats.failed, stats.dlq
                );
            }
            Err(e) => {
                warn!("Failed to get statistics: {}", e);
            }
        }

        sleep(Duration::from_secs(30)).await;
    }
}

/// Maintenance task that cleans up old data
async fn maintenance_task(broker: Arc<MysqlBroker>) {
    loop {
        // Wait 1 hour between maintenance runs
        sleep(Duration::from_secs(3600)).await;

        info!("Running maintenance tasks...");

        // Archive completed tasks older than 7 days
        match broker
            .archive_completed_tasks(Duration::from_secs(7 * 24 * 3600))
            .await
        {
            Ok(count) => info!("Archived {} completed tasks", count),
            Err(e) => error!("Failed to archive tasks: {}", e),
        }

        // Recover stuck tasks (stuck for more than 1 hour)
        match broker.recover_stuck_tasks(Duration::from_secs(3600)).await {
            Ok(count) => {
                if count > 0 {
                    warn!("Recovered {} stuck tasks", count);
                }
            }
            Err(e) => error!("Failed to recover stuck tasks: {}", e),
        }

        // Optimize tables for better performance
        if let Err(e) = broker.optimize_tables().await {
            warn!("Failed to optimize tables: {}", e);
        } else {
            info!("Tables optimized successfully");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    info!("Starting CeleRS Worker Pool Example");

    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers_dev".to_string());

    // Configure connection pool for optimal performance
    let pool_config = PoolConfig {
        max_connections: 20,
        min_connections: 5,
        acquire_timeout_secs: 30,
        max_lifetime_secs: Some(1800),
        idle_timeout_secs: Some(600),
    };

    // Create broker
    info!(
        "Connecting to MySQL: {}",
        database_url.split('@').next_back().unwrap_or("unknown")
    );
    let broker = MysqlBroker::with_config(&database_url, "default", pool_config).await?;

    // Run migrations
    info!("Running migrations...");
    broker.migrate().await?;

    // Create worker pool
    let worker_config = WorkerConfig {
        worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
        max_concurrent_tasks: 10,
        poll_interval: Duration::from_millis(100),
        retry_delay: Duration::from_secs(5),
    };

    let processor = Arc::new(CompositeProcessor::new());
    let worker_pool = WorkerPool::new(broker, processor, worker_config);

    // Spawn health check task
    let broker_arc = worker_pool.broker.clone();
    tokio::spawn(health_check_task(broker_arc));

    // Spawn maintenance task
    let broker_arc = worker_pool.broker.clone();
    tokio::spawn(maintenance_task(broker_arc));

    // Spawn worker pool
    let worker_handle = {
        let pool = worker_pool.clone();
        tokio::spawn(async move {
            if let Err(e) = pool.run().await {
                error!("Worker pool error: {}", e);
            }
        })
    };

    // Wait for shutdown signal
    info!("Worker pool started. Press Ctrl+C to shutdown...");
    signal::ctrl_c().await?;
    info!("Shutdown signal received");

    // Gracefully shutdown worker pool
    worker_pool.shutdown();
    worker_handle.await?;

    info!("Worker pool shut down successfully");
    Ok(())
}

// Helper trait to make WorkerPool cloneable
impl Clone for WorkerPool {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            processor: self.processor.clone(),
            config: self.config.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}
