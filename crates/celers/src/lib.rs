//! # CeleRS - Celery-Compatible Distributed Task Queue for Rust
//!
//! **CeleRS** is a production-ready, Celery-compatible distributed task queue library
//! providing binary-level protocol compatibility with Python Celery while delivering
//! superior performance, type safety, and reliability.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use celers::prelude::*;
//!
//! #[derive(Serialize, Deserialize, Debug)]
//! struct AddArgs {
//!     x: i32,
//!     y: i32,
//! }
//!
//! #[celers::task]
//! async fn add(args: AddArgs) -> Result<i32, Box<dyn std::error::Error>> {
//!     Ok(args.x + args.y)
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Start worker (implementation depends on enabled features)
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! - **Type-Safe**: Compile-time guarantees for task signatures
//! - **Celery-Compatible**: Binary protocol compatibility with Python Celery
//! - **High Performance**: 10x throughput compared to Python Celery
//! - **Multiple Brokers**: Redis, PostgreSQL, MySQL, RabbitMQ (AMQP), AWS SQS
//! - **Multiple Backends**: Redis, PostgreSQL/MySQL (Database), gRPC
//! - **Workflow Primitives**: Chain, Group, Chord, Map, Starmap
//! - **Observability**: Prometheus metrics, distributed tracing
//! - **Production Features**: Batch operations, persistent scheduler, progress tracking
//!
//! ## Architecture
//!
//! CeleRS follows a layered architecture:
//!
//! ```text
//! Application Layer (Your Tasks)
//!        ↓
//! Runtime Layer (celers-worker, celers-canvas)
//!        ↓
//! Messaging Layer (celers-kombu, celers-broker-*)
//!        ↓
//! Protocol Layer (celers-protocol)
//! ```

// Re-export core types
pub use celers_core::{
    ActiveTaskInfo, AsyncResult, Broker, BrokerStats, CompositeEventEmitter, ControlCommand,
    ControlResponse, DeliveryInfo, Event, EventEmitter, GlobPattern, InMemoryEventEmitter,
    InspectCommand, InspectResponse, LogLevel, LoggingEventEmitter, NoOpEventEmitter,
    PatternMatcher, PoolStats, QueueStats, RateLimitConfig, RateLimiter, RegexPattern, RequestInfo,
    ReservedTaskInfo, ResultStore, RouteResult, RouteRule, Router, RouterBuilder, RoutingConfig,
    ScheduledTaskInfo, SerializedTask, SlidingWindow, TaskEvent, TaskEventBuilder, TaskRateLimiter,
    TaskResultValue, TaskState, TokenBucket, WorkerConf, WorkerEvent, WorkerEventBuilder,
    WorkerRateLimiter, WorkerReport, WorkerStats,
};

// Re-export protocol types
pub use celers_protocol::{
    ContentEncoding, ContentType, Message, MessageHeaders, MessageProperties, ProtocolVersion,
    TaskArgs,
};

// Re-export kombu types
pub use celers_kombu::{
    BrokerError, Consumer, Envelope, Producer, QueueConfig, QueueMode, Result, Transport,
};

// Re-export worker types
pub use celers_worker::{Worker, WorkerConfig};

// Re-export canvas types
pub use celers_canvas::{
    Chain, Chord, Chunks, Group, Map, Signature, Starmap, TaskOptions, XMap, XStarmap,
};

// Re-export macros
pub use celers_macros::{task, Task};

// Optional broker re-exports
#[cfg(feature = "redis")]
pub use celers_broker_redis::RedisBroker;

#[cfg(feature = "postgres")]
pub use celers_broker_postgres::PostgresBroker;

#[cfg(feature = "mysql")]
pub use celers_broker_sql::MysqlBroker;

#[cfg(feature = "amqp")]
pub use celers_broker_amqp::AmqpBroker;

#[cfg(feature = "sqs")]
pub use celers_broker_sqs::SqsBroker;

// Optional backend re-exports
#[cfg(feature = "backend-redis")]
pub use celers_backend_redis::{
    event_transport::{RedisEventConfig, RedisEventEmitter, RedisEventReceiver},
    ChordState, RedisResultBackend, ResultBackend, TaskMeta, TaskResult,
};

#[cfg(feature = "backend-db")]
pub use celers_backend_db::{MysqlResultBackend, PostgresResultBackend};

#[cfg(feature = "backend-rpc")]
pub use celers_backend_rpc::GrpcResultBackend;

// Optional beat re-exports
#[cfg(feature = "beat")]
pub use celers_beat::{BeatScheduler, Schedule, ScheduledTask};

// Optional metrics re-exports
#[cfg(feature = "metrics")]
pub use celers_metrics::{gather_metrics, reset_metrics};

/// Prelude module for common imports
pub mod prelude {
    // Core types
    pub use crate::AsyncResult;
    pub use crate::Broker;
    pub use crate::CompositeEventEmitter;
    pub use crate::ControlCommand;
    pub use crate::ControlResponse;
    pub use crate::Event;
    pub use crate::EventEmitter;
    pub use crate::InMemoryEventEmitter;
    pub use crate::InspectCommand;
    pub use crate::InspectResponse;
    pub use crate::LogLevel;
    pub use crate::LoggingEventEmitter;
    pub use crate::NoOpEventEmitter;
    pub use crate::ResultStore;
    pub use crate::SerializedTask;
    pub use crate::TaskEvent;
    pub use crate::TaskEventBuilder;
    pub use crate::TaskResultValue;
    pub use crate::TaskState;
    pub use crate::WorkerEvent;
    pub use crate::WorkerEventBuilder;
    pub use crate::WorkerStats;

    // Rate limiting types
    pub use crate::RateLimitConfig;
    pub use crate::RateLimiter;
    pub use crate::SlidingWindow;
    pub use crate::TaskRateLimiter;
    pub use crate::TokenBucket;
    pub use crate::WorkerRateLimiter;

    // Task routing types
    pub use crate::GlobPattern;
    pub use crate::PatternMatcher;
    pub use crate::RegexPattern;
    pub use crate::RouteResult;
    pub use crate::RouteRule;
    pub use crate::Router;
    pub use crate::RouterBuilder;
    pub use crate::RoutingConfig;

    // Worker types
    pub use crate::Worker;
    pub use crate::WorkerConfig;
    pub use celers_worker::WorkerConfigBuilder;

    // Broker helper functions
    pub use crate::broker_helper::{create_broker, create_broker_from_env, BrokerConfigError};

    // Broker implementations
    #[cfg(feature = "redis")]
    pub use crate::RedisBroker;

    #[cfg(feature = "postgres")]
    pub use crate::PostgresBroker;

    #[cfg(feature = "mysql")]
    pub use crate::MysqlBroker;

    #[cfg(feature = "amqp")]
    pub use crate::AmqpBroker;

    #[cfg(feature = "sqs")]
    pub use crate::SqsBroker;

    // Backend implementations
    #[cfg(feature = "backend-redis")]
    pub use crate::{
        ChordState, RedisEventConfig, RedisEventEmitter, RedisEventReceiver, RedisResultBackend,
        ResultBackend, TaskMeta, TaskResult,
    };

    #[cfg(feature = "backend-db")]
    pub use crate::{MysqlResultBackend, PostgresResultBackend};

    #[cfg(feature = "backend-rpc")]
    pub use crate::GrpcResultBackend;

    // Beat scheduler
    #[cfg(feature = "beat")]
    pub use crate::{BeatScheduler, Schedule, ScheduledTask};

    // Metrics
    #[cfg(feature = "metrics")]
    pub use crate::{gather_metrics, reset_metrics};

    // Macros (task attribute and Task derive)
    pub use celers_macros::{task, Task};

    // Canvas primitives
    pub use crate::{
        Chain, Chord, Chunks, Group, Map, Signature, Starmap, TaskOptions, XMap, XStarmap,
    };

    // Error types
    pub use crate::BrokerError;

    // Common external crates
    pub use async_trait::async_trait;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json;
    pub use serde_json::json;
    pub use tokio;
    pub use uuid::Uuid;
}

/// Error types re-exported from celers-kombu
pub mod error {
    pub use celers_kombu::BrokerError;
}

/// Protocol types for advanced usage
pub mod protocol {
    pub use celers_protocol::*;
}

/// Canvas workflow types
pub mod canvas {
    pub use celers_canvas::*;
}

/// Worker runtime types
pub mod worker {
    pub use celers_worker::*;
}

/// Rate limiting types
pub mod rate_limit {
    pub use celers_core::rate_limit::*;
}

/// Task routing types
pub mod router {
    pub use celers_core::router::*;
}

/// Broker selection helpers
pub mod broker_helper {
    use std::env;

    /// Broker configuration error
    #[derive(Debug, thiserror::Error)]
    pub enum BrokerConfigError {
        #[error("Missing environment variable: {0}")]
        MissingEnvVar(String),

        #[error("Unsupported broker type: {0}")]
        UnsupportedBrokerType(String),

        #[error("Feature not enabled: {0}")]
        FeatureNotEnabled(String),

        #[error("Broker creation failed: {0}")]
        CreationFailed(String),
    }

    /// Create a broker from environment variables
    ///
    /// Environment variables:
    /// - `CELERS_BROKER_TYPE`: Type of broker (redis, postgres, mysql, amqp, sqs)
    /// - `CELERS_BROKER_URL`: Connection URL for the broker
    /// - `CELERS_BROKER_QUEUE`: Queue name (default: "celers")
    ///
    /// # Example
    ///
    /// ```bash
    /// export CELERS_BROKER_TYPE=redis
    /// export CELERS_BROKER_URL=redis://localhost:6379
    /// export CELERS_BROKER_QUEUE=my_queue
    /// ```
    ///
    /// ```rust,ignore
    /// use celers::broker_helper::create_broker_from_env;
    ///
    /// let broker = create_broker_from_env().await?;
    /// ```
    pub async fn create_broker_from_env() -> Result<Box<dyn crate::Broker>, BrokerConfigError> {
        let broker_type = env::var("CELERS_BROKER_TYPE")
            .map_err(|_| BrokerConfigError::MissingEnvVar("CELERS_BROKER_TYPE".to_string()))?;

        let broker_url = env::var("CELERS_BROKER_URL")
            .map_err(|_| BrokerConfigError::MissingEnvVar("CELERS_BROKER_URL".to_string()))?;

        let queue_name = env::var("CELERS_BROKER_QUEUE").unwrap_or_else(|_| "celers".to_string());

        create_broker(&broker_type, &broker_url, &queue_name).await
    }

    /// Create a broker with explicit configuration
    ///
    /// # Arguments
    ///
    /// * `broker_type` - Type of broker: "redis", "postgres", "mysql", "amqp", "sqs"
    /// * `broker_url` - Connection URL for the broker
    /// * `queue_name` - Name of the queue to use
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::broker_helper::create_broker;
    ///
    /// let broker = create_broker("redis", "redis://localhost:6379", "my_queue").await?;
    /// ```
    pub async fn create_broker(
        broker_type: &str,
        broker_url: &str,
        queue_name: &str,
    ) -> Result<Box<dyn crate::Broker>, BrokerConfigError> {
        match broker_type.to_lowercase().as_str() {
            #[cfg(feature = "redis")]
            "redis" => {
                use crate::RedisBroker;

                RedisBroker::new(broker_url, queue_name)
                    .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                    .map_err(|e| BrokerConfigError::CreationFailed(e.to_string()))
            }

            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => {
                use crate::PostgresBroker;

                PostgresBroker::with_queue(broker_url, queue_name)
                    .await
                    .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                    .map_err(|e| BrokerConfigError::CreationFailed(e.to_string()))
            }

            #[cfg(feature = "mysql")]
            "mysql" => {
                use crate::MysqlBroker;

                MysqlBroker::with_queue(broker_url, queue_name)
                    .await
                    .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                    .map_err(|e| BrokerConfigError::CreationFailed(e.to_string()))
            }

            _ => {
                // Check if it's a known type but feature not enabled
                #[cfg(not(feature = "redis"))]
                if broker_type.to_lowercase() == "redis" {
                    return Err(BrokerConfigError::FeatureNotEnabled(
                        "redis feature not enabled".to_string(),
                    ));
                }

                #[cfg(not(feature = "postgres"))]
                if broker_type.to_lowercase() == "postgres"
                    || broker_type.to_lowercase() == "postgresql"
                {
                    return Err(BrokerConfigError::FeatureNotEnabled(
                        "postgres feature not enabled".to_string(),
                    ));
                }

                #[cfg(not(feature = "mysql"))]
                if broker_type.to_lowercase() == "mysql" {
                    return Err(BrokerConfigError::FeatureNotEnabled(
                        "mysql feature not enabled".to_string(),
                    ));
                }

                // AMQP and SQS use celers-kombu Transport trait, not Broker trait
                if broker_type.to_lowercase() == "amqp" || broker_type.to_lowercase() == "rabbitmq"
                {
                    return Err(BrokerConfigError::UnsupportedBrokerType(
                        "AMQP brokers use Transport trait (celers-kombu), use AmqpBroker directly"
                            .to_string(),
                    ));
                }

                if broker_type.to_lowercase() == "sqs" {
                    return Err(BrokerConfigError::UnsupportedBrokerType(
                        "SQS brokers use Transport trait (celers-kombu), use SqsBroker directly"
                            .to_string(),
                    ));
                }

                Err(BrokerConfigError::UnsupportedBrokerType(
                    broker_type.to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_facade_exports() {
        // Verify main types are exported
        let _: Option<Box<dyn Broker>> = None;
    }
}
