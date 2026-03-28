//! PostgreSQL broker implementation for CeleRS
//!
//! This broker uses PostgreSQL with `FOR UPDATE SKIP LOCKED` for reliable,
//! distributed task queue processing. It supports:
//! - Priority queues
//! - Dead Letter Queue (DLQ) for permanently failed tasks
//! - Delayed task execution (enqueue_at, enqueue_after)
//! - **Task chaining for sequential execution**
//! - **DAG-based workflows with stage dependencies**
//! - **Task deduplication with idempotency keys**
//! - **Real-time LISTEN/NOTIFY for event-driven workers**
//! - Prometheus metrics (optional `metrics` feature)
//! - Batch enqueue/dequeue/ack operations
//! - Transaction safety
//! - Distributed workers without contention
//! - Queue pause/resume functionality
//! - DLQ inspection and requeue
//! - Task status inspection
//! - Database health checks
//! - Automatic task archiving
//!
//! # Quick Start
//!
//! ```no_run
//! use celers_broker_postgres::PostgresBroker;
//! use celers_core::{Broker, SerializedTask};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create broker and run migrations
//! let broker = PostgresBroker::new("postgres://user:pass@localhost/mydb").await?;
//! broker.migrate().await?;
//!
//! // Enqueue a task
//! let task = SerializedTask::new("my_task".to_string(), vec![1, 2, 3]);
//! let task_id = broker.enqueue(task).await?;
//!
//! // Dequeue and process
//! if let Some(msg) = broker.dequeue().await? {
//!     // Process task...
//!     broker.ack(&msg.task.metadata.id, msg.receipt_handle.as_deref()).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Delayed Execution
//!
//! Schedule tasks for future execution:
//!
//! ```no_run
//! use celers_broker_postgres::PostgresBroker;
//! use celers_core::{Broker, SerializedTask};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let broker = PostgresBroker::new("postgres://localhost/db").await?;
//! let task = SerializedTask::new("delayed_task".to_string(), vec![]);
//!
//! // Schedule for specific timestamp (Unix seconds)
//! let execute_at = 1735689600; // Some future timestamp
//! broker.enqueue_at(task.clone(), execute_at).await?;
//!
//! // Or schedule after a delay (seconds)
//! broker.enqueue_after(task, 300).await?; // 5 minutes from now
//! # Ok(())
//! # }
//! ```
//!
//! # Batch Operations
//!
//! Process multiple tasks efficiently:
//!
//! ```no_run
//! use celers_broker_postgres::PostgresBroker;
//! use celers_core::{Broker, SerializedTask};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let broker = PostgresBroker::new("postgres://localhost/db").await?;
//! // Batch enqueue
//! let tasks = vec![
//!     SerializedTask::new("task1".to_string(), vec![1]),
//!     SerializedTask::new("task2".to_string(), vec![2]),
//! ];
//! broker.enqueue_batch(tasks).await?;
//!
//! // Batch dequeue
//! let messages = broker.dequeue_batch(10).await?;
//!
//! // Batch acknowledge
//! let acks: Vec<_> = messages.iter()
//!     .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
//!     .collect();
//! broker.ack_batch(&acks).await?;
//! # Ok(())
//! # }
//! ```

// Core modules
mod broker_core;
mod broker_trait;
pub mod types;

// Feature modules
mod advanced_ops;
mod analytics;
mod convenience;
mod db_monitoring;
mod deduplication;
mod dlq;
mod health;
mod notifications;
mod partitions;
mod query_optimization;
mod queue_ops;
mod results;
mod scheduling;
mod workflows;

// Pre-existing public modules
pub mod monitoring;
pub mod utilities;

// Re-export all public types
pub use broker_core::PostgresBroker;
pub use notifications::TaskNotificationListener;
pub use types::*;
pub use workflows::TenantBroker;

#[cfg(test)]
mod tests;
