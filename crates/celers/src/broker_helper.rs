use std::env;

/// Broker configuration error with helpful suggestions
#[derive(Debug, thiserror::Error)]
pub enum BrokerConfigError {
    /// Missing required environment variable
    #[error("Missing environment variable: {0}\n\nSuggestion: Set the environment variable before running:\n  export {0}=<value>")]
    MissingEnvVar(String),

    /// Unsupported broker type requested
    #[error("Unsupported broker type: {broker_type}\n\nSupported types: redis, postgres, mysql, amqp, sqs\nNote: {note}")]
    UnsupportedBrokerType {
        /// The broker type that was requested
        broker_type: String,
        /// Additional note about the error
        note: String,
    },

    /// Required feature not enabled in Cargo.toml
    #[error("Feature not enabled: {feature}\n\nTo enable this feature, add it to your Cargo.toml:\n  celers = {{ version = \"0.1\", features = [\"{feature}\"] }}\n\nAvailable features: redis, postgres, mysql, amqp, sqs, backend-redis, backend-db, backend-rpc")]
    FeatureNotEnabled {
        /// The feature name that needs to be enabled
        feature: String,
    },

    /// Broker creation failed with detailed error information
    #[error("Broker creation failed: {message}\n\nPossible causes:\n{suggestions}")]
    CreationFailed {
        /// Error message from the broker creation attempt
        message: String,
        /// Suggestions for resolving the error
        suggestions: String,
    },
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
                .map_err(|e| BrokerConfigError::CreationFailed {
                    message: e.to_string(),
                    suggestions: "- Check that Redis server is running\n  - Verify the connection URL format: redis://host:port\n  - Ensure network connectivity to Redis server".to_string(),
                })
        }

        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => {
            use crate::PostgresBroker;

            PostgresBroker::with_queue(broker_url, queue_name)
                .await
                .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                .map_err(|e| BrokerConfigError::CreationFailed {
                    message: e.to_string(),
                    suggestions: "- Check that PostgreSQL server is running\n  - Verify the connection URL format: postgres://user:pass@host:port/db\n  - Ensure database exists and user has permissions".to_string(),
                })
        }

        #[cfg(feature = "mysql")]
        "mysql" => {
            use crate::MysqlBroker;

            MysqlBroker::with_queue(broker_url, queue_name)
                .await
                .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                .map_err(|e| BrokerConfigError::CreationFailed {
                    message: e.to_string(),
                    suggestions: "- Check that MySQL server is running\n  - Verify the connection URL format: mysql://user:pass@host:port/db\n  - Ensure database exists and user has permissions".to_string(),
                })
        }

        _ => {
            // Check if it's a known type but feature not enabled
            #[cfg(not(feature = "redis"))]
            if broker_type.to_lowercase() == "redis" {
                return Err(BrokerConfigError::FeatureNotEnabled {
                    feature: "redis".to_string(),
                });
            }

            #[cfg(not(feature = "postgres"))]
            if broker_type.to_lowercase() == "postgres"
                || broker_type.to_lowercase() == "postgresql"
            {
                return Err(BrokerConfigError::FeatureNotEnabled {
                    feature: "postgres".to_string(),
                });
            }

            #[cfg(not(feature = "mysql"))]
            if broker_type.to_lowercase() == "mysql" {
                return Err(BrokerConfigError::FeatureNotEnabled {
                    feature: "mysql".to_string(),
                });
            }

            // AMQP and SQS use celers-kombu Transport trait, not Broker trait
            if broker_type.to_lowercase() == "amqp" || broker_type.to_lowercase() == "rabbitmq" {
                return Err(BrokerConfigError::UnsupportedBrokerType {
                    broker_type: broker_type.to_string(),
                    note: "AMQP brokers use the Transport trait. Import and use AmqpBroker directly from celers::AmqpBroker".to_string(),
                });
            }

            if broker_type.to_lowercase() == "sqs" {
                return Err(BrokerConfigError::UnsupportedBrokerType {
                    broker_type: broker_type.to_string(),
                    note: "SQS brokers use the Transport trait. Import and use SqsBroker directly from celers::SqsBroker".to_string(),
                });
            }

            Err(BrokerConfigError::UnsupportedBrokerType {
                broker_type: broker_type.to_string(),
                note: "Check the broker type name for typos".to_string(),
            })
        }
    }
}
