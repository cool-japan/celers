//! AWS SQS broker implementation for CeleRS
//!
//! This crate provides AWS SQS support for cloud-native deployments.
//!
//! # Features
//!
//! - Long polling for efficiency (up to 20 seconds)
//! - Visibility timeout handling
//! - Dead Letter Queue (DLQ) integration
//! - FIFO queue support with message deduplication
//! - Server-side encryption (SSE) with optional KMS
//! - IAM role authentication
//! - Batch operations for throughput (publish_batch, consume_batch, ack_batch)
//! - Priority queue support (via message attributes)
//! - Cost optimization through batch API calls (10x reduction)
//! - Queue monitoring and statistics
//! - CloudWatch metrics integration
//! - Adaptive polling strategies
//! - Parallel message processing
//! - **Circuit breaker pattern** for AWS API resilience ✨ NEW
//! - **Real-time cost tracking** with detailed breakdown ✨ NEW
//! - **Advanced batch optimizer** with dynamic sizing ✨ NEW
//! - **Distributed tracing** with correlation IDs and trace context ✨ NEW
//! - **Quota/budget management** for cost control ✨ NEW
//! - **Multi-queue routing** for message distribution ✨ NEW
//! - **Performance profiling** with latency tracking ✨ NEW
//! - **Message deduplication** utilities for preventing duplicate processing ✨ NEW
//! - **Advanced DLQ analytics** with error pattern detection and retry recommendations ✨ NEW
//! - **Observability hooks** for custom metrics and logging integration ✨ NEW
//! - **Backpressure management** for preventing system overload ✨ NEW
//! - **Poison message detection** for isolating repeatedly failing messages ✨ NEW
//! - **Cost alert system** with configurable thresholds and callbacks ✨ NEW
//! - **Lambda integration helpers** for AWS Lambda SQS event processing ✨ NEW
//!
//! # Quick Start
//!
//! ```ignore
//! use celers_broker_sqs::SqsBroker;
//! use celers_kombu::{Transport, Producer, Consumer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut broker = SqsBroker::new("my-queue-name").await?;
//! broker.connect().await?;
//!
//! // Publish a message
//! let message = Message::new("tasks.add");
//! broker.publish("my-queue", message).await?;
//!
//! // Consume messages
//! let envelope = broker.consume("my-queue", std::time::Duration::from_secs(20)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # FIFO Queue Example
//!
//! ```ignore
//! use celers_broker_sqs::{SqsBroker, FifoConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create FIFO queue with content-based deduplication
//! let fifo_config = FifoConfig::new()
//!     .with_content_based_deduplication(true)
//!     .with_high_throughput(true);
//!
//! let mut broker = SqsBroker::new("my-queue.fifo")
//!     .await?
//!     .with_fifo(fifo_config);
//!
//! // For FIFO queues, publish with message group ID
//! broker.publish_fifo("my-queue.fifo", message, "group-1", None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Batch Operations (10x Cost Reduction)
//!
//! ```ignore
//! use celers_broker_sqs::SqsBroker;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut broker = SqsBroker::new("my-queue").await?;
//! broker.connect().await?;
//!
//! // Batch publish (up to 10 messages)
//! let messages = vec![
//!     Message::new("task.1"),
//!     Message::new("task.2"),
//!     Message::new("task.3"),
//! ];
//! broker.publish_batch("my-queue", messages).await?;
//!
//! // Batch consume (up to 10 messages)
//! let envelopes = broker.consume_batch("my-queue", 10, Duration::from_secs(20)).await?;
//!
//! // Batch acknowledge
//! let tags: Vec<String> = envelopes.iter().map(|e| e.delivery_tag.clone()).collect();
//! broker.ack_batch(&tags).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # AWS IAM Policy Requirements
//!
//! Minimum IAM permissions needed for production use:
//!
//! ```json
//! {
//!   "Version": "2012-10-17",
//!   "Statement": [
//!     {
//!       "Effect": "Allow",
//!       "Action": [
//!         "sqs:SendMessage",
//!         "sqs:SendMessageBatch",
//!         "sqs:ReceiveMessage",
//!         "sqs:DeleteMessage",
//!         "sqs:DeleteMessageBatch",
//!         "sqs:ChangeMessageVisibility",
//!         "sqs:GetQueueUrl",
//!         "sqs:GetQueueAttributes"
//!       ],
//!       "Resource": "arn:aws:sqs:*:*:celers-*"
//!     }
//!   ]
//! }
//! ```
//!
//! For queue management operations, add:
//! - `sqs:CreateQueue`
//! - `sqs:DeleteQueue`
//! - `sqs:PurgeQueue`
//! - `sqs:ListQueues`
//!
//! For CloudWatch metrics, add:
//! ```json
//! {
//!   "Effect": "Allow",
//!   "Action": ["cloudwatch:PutMetricData"],
//!   "Resource": "*",
//!   "Condition": {
//!     "StringEquals": {"cloudwatch:namespace": "CeleRS/SQS"}
//!   }
//! }
//! ```
//!
//! # Cost Optimization
//!
//! AWS SQS charges per API request. Optimize costs by:
//!
//! 1. **Use batch operations** (10x cost reduction)
//! 2. **Enable long polling** (reduces empty receives)
//! 3. **Use adaptive polling** (adjusts wait time based on activity)
//!
//! Example with cost optimization:
//!
//! ```ignore
//! use celers_broker_sqs::{SqsBroker, AdaptivePollingConfig, PollingStrategy};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
//!     .with_min_wait_time(1)
//!     .with_max_wait_time(20);
//!
//! let mut broker = SqsBroker::new("my-queue")
//!     .await?
//!     .with_wait_time(20)           // Enable long polling
//!     .with_max_messages(10)        // Receive up to 10 messages at once
//!     .with_adaptive_polling(adaptive_config);
//!
//! broker.connect().await?;
//!
//! // This configuration can reduce costs by 90% for high-volume workloads
//! # Ok(())
//! # }
//! ```
//!
//! **Cost Example**: Processing 10M messages/month
//! - Without optimization: ~$10.80/month
//! - With batching + long polling: ~$0.80/month (92.6% savings!)
//!
//! # Authentication
//!
//! The broker uses AWS SDK's credential chain in this order:
//! 1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
//! 2. IAM role (recommended for EC2/ECS/Lambda)
//! 3. AWS credentials file (`~/.aws/credentials`)
//! 4. ECS container credentials
//! 5. EC2 instance metadata
//!
//! **Recommendation**: Use IAM roles in production for enhanced security.

// Feature modules
pub mod auto_tuner;
pub mod backpressure;
pub mod batch_optimizer;
pub mod celery_compat;
pub mod circuit_breaker;
pub mod cost_alerts;
pub mod cost_tracker;
pub mod dedup;
pub mod dlq_analytics;
pub mod hooks;
pub mod lambda_helpers;
pub mod metrics_aggregator;
pub mod monitoring;
pub mod optimization;
pub mod poison_detector;
pub mod profiler;
pub mod quota_manager;
pub mod replay;
pub mod router;
pub mod sla_monitor;
pub mod tracing_util;
pub mod utilities;
pub mod workload_presets;

// Core modules
pub mod broker_core;
pub mod broker_ops;
pub mod types;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

// Re-exports
pub use broker_core::SqsBroker;
pub use types::*;
