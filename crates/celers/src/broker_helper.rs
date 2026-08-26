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

        #[cfg(feature = "amqp")]
        "amqp" | "rabbitmq" => {
            use crate::AmqpBroker;

            // `AmqpBroker` speaks celers-kombu's transport traits
            // (publish/consume), not `crate::Broker` (enqueue/dequeue).
            // `into_core_broker` is the bridge: it wraps the transport in a
            // `KombuBrokerAdapter`, which *does* implement `crate::Broker`, so
            // a `celers_worker::Worker` built from this can consume RabbitMQ
            // directly instead of the caller having to know about the
            // transport/task-queue split at all.
            AmqpBroker::new(broker_url, queue_name)
                .await
                .map(|b| Box::new(b.into_core_broker(queue_name)) as Box<dyn crate::Broker>)
                .map_err(|e| BrokerConfigError::CreationFailed {
                    message: e.to_string(),
                    suggestions: "- Check that RabbitMQ server is running\n  - Verify the connection URL format: amqp://user:pass@host:port/vhost\n  - Ensure network connectivity to the RabbitMQ server".to_string(),
                })
        }

        #[cfg(feature = "sqs")]
        "sqs" => {
            use crate::SqsBroker;

            // `SqsBroker` has no connection URL of its own (the AWS SDK reads
            // its endpoint/credentials from the environment), so `broker_url`
            // is intentionally unused here. In a build with only the `sqs`
            // broker feature on, this arm is the *only* one compiled, so
            // without this explicit discard the parameter is unused across
            // the whole function and `-D warnings` rejects the build.
            let _ = broker_url;

            // `with_max_messages(10)` matches the crate's own quickstart: it
            // turns on prefetching, which is where SQS's batch-dequeue cost
            // reduction comes from.
            SqsBroker::new(queue_name)
                .await
                .map(|b| {
                    Box::new(b.with_max_messages(10).into_core_broker(queue_name))
                        as Box<dyn crate::Broker>
                })
                .map_err(|e| BrokerConfigError::CreationFailed {
                    message: e.to_string(),
                    suggestions: "- Check AWS credentials are configured (environment, profile, or IAM role)\n  - Verify the queue name is valid (a FIFO queue name must end with \".fifo\")\n  - Ensure network connectivity to the AWS SQS endpoint".to_string(),
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

            #[cfg(not(feature = "amqp"))]
            if broker_type.to_lowercase() == "amqp" || broker_type.to_lowercase() == "rabbitmq" {
                return Err(BrokerConfigError::FeatureNotEnabled {
                    feature: "amqp".to_string(),
                });
            }

            #[cfg(not(feature = "sqs"))]
            if broker_type.to_lowercase() == "sqs" {
                return Err(BrokerConfigError::FeatureNotEnabled {
                    feature: "sqs".to_string(),
                });
            }

            Err(BrokerConfigError::UnsupportedBrokerType {
                broker_type: broker_type.to_string(),
                note: "Check the broker type name for typos".to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`Result::expect_err`] needs `T: Debug`, and `T` here is
    /// `Box<dyn crate::Broker>` — a trait object with no `Debug` impl. This is
    /// the same unwrap without that bound.
    ///
    /// Used by the two feature-*off* tests below; cfg-matched to them so it is
    /// never dead code (with both `amqp` and `sqs` on, neither test exists).
    #[cfg(not(all(feature = "amqp", feature = "sqs")))]
    #[track_caller]
    fn expect_err(
        result: Result<Box<dyn crate::Broker>, BrokerConfigError>,
        msg: &str,
    ) -> BrokerConfigError {
        match result {
            Ok(_) => panic!("{msg}"),
            Err(e) => e,
        }
    }

    // ------------------------------------------------------------------
    // Feature-off: an honest "enable the feature" error, not a claim that
    // AMQP/SQS are structurally unsupported.
    //
    // Before this module's `create_broker` fix, an "amqp"/"rabbitmq"/"sqs"
    // request *always* fell through to the generic `_` arm and returned
    // `UnsupportedBrokerType` — even in a build that had the feature on,
    // because the feature-gated match arms above did not exist yet. These
    // two tests pin the case that is still reachable once they do: the
    // feature really is off, and the error says so.
    // ------------------------------------------------------------------

    #[cfg(not(feature = "amqp"))]
    #[tokio::test]
    async fn amqp_without_the_feature_reports_feature_not_enabled() {
        for broker_type in ["amqp", "rabbitmq", "AMQP", "RabbitMQ"] {
            let result = create_broker(broker_type, "amqp://localhost:5672", "q").await;
            let err = expect_err(result, "the amqp feature is off in this build");
            assert!(
                matches!(&err, BrokerConfigError::FeatureNotEnabled { feature } if feature == "amqp"),
                "{broker_type} should report FeatureNotEnabled(\"amqp\"), got {err:?}"
            );
        }
    }

    #[cfg(not(feature = "sqs"))]
    #[tokio::test]
    async fn sqs_without_the_feature_reports_feature_not_enabled() {
        let result = create_broker("sqs", "unused", "q").await;
        let err = expect_err(result, "the sqs feature is off in this build");
        assert!(
            matches!(&err, BrokerConfigError::FeatureNotEnabled { feature } if feature == "sqs"),
            "sqs should report FeatureNotEnabled(\"sqs\"), got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // Feature-on dispatch shape. No live broker is reachable in this suite,
    // so each test below is chosen specifically so that reaching it proves
    // the right *arm* ran without needing any network I/O to succeed.
    //
    // Both `SqsBroker::new` and `AmqpBroker::new` (with the default,
    // pool-disabled `AmqpConfig`) construct their struct with no connection
    // opened — confirmed empirically: an `AmqpBroker::new` against
    // `amqp://127.0.0.1:1`, a port nothing listens on, still returns `Ok`.
    // Neither transport connects until something first calls
    // `Transport::connect` (directly, or lazily via the adapter's first real
    // operation), so this proves *dispatch* — the "amqp"/"sqs" arms exist and
    // build the advertised `KombuBrokerAdapter` — without proving the
    // resulting broker can actually reach a server. Before this module's fix,
    // both calls below returned `UnsupportedBrokerType` unconditionally; the
    // full round trip against a real broker is `live_amqp` below.
    // ------------------------------------------------------------------

    #[cfg(feature = "sqs")]
    #[tokio::test]
    async fn sqs_dispatches_to_a_real_core_broker_with_no_network_call() {
        let broker = create_broker("sqs", "unused-for-sqs", "kombu-dispatch-test").await;
        assert!(
            broker.is_ok(),
            "the sqs arm must construct a broker with no AWS endpoint reachable"
        );
    }

    #[cfg(feature = "amqp")]
    #[tokio::test]
    async fn amqp_dispatches_to_a_real_core_broker_with_no_network_call() {
        for broker_type in ["amqp", "rabbitmq"] {
            let broker = create_broker(broker_type, "amqp://127.0.0.1:1", "q").await;
            assert!(
                broker.is_ok(),
                "{broker_type}: construction is lazy, so a port nothing listens on must still dispatch to a broker instead of UnsupportedBrokerType"
            );
        }
    }

    /// End-to-end proof against a real RabbitMQ: `create_broker("amqp", ..)`
    /// really does hand back a working, worker-usable `crate::Broker`.
    ///
    /// Gated on `CELERS_TEST_AMQP_URL` (docker compose `rabbitmq`), matching
    /// `celers_broker_amqp::core_broker`'s own live suite: skips visibly
    /// rather than silently doing nothing under plain `cargo test`.
    #[cfg(feature = "amqp")]
    mod live_amqp {
        use super::*;
        use celers_kombu::{Broker as KombuTransportBroker, Transport};
        use uuid::Uuid;

        #[track_caller]
        fn integration_url() -> Option<String> {
            match std::env::var("CELERS_TEST_AMQP_URL") {
                Ok(url) if !url.is_empty() => Some(url),
                _ => {
                    let location = std::panic::Location::caller();
                    eprintln!("SKIPPED: {location} (set CELERS_TEST_AMQP_URL to run)");
                    None
                }
            }
        }

        /// Declares nothing itself: `create_broker` is the thing under test,
        /// so the queue it declares (via lazy connect) must not be pre-empted
        /// by a helper declaring it first.
        async fn drop_queue(url: &str, queue: &str) {
            use crate::AmqpBroker;

            let Ok(mut transport) = AmqpBroker::new(url, queue).await else {
                return;
            };
            if transport.connect().await.is_ok() {
                let _ = transport.delete_queue(queue).await;
                let _ = transport.disconnect().await;
            }
        }

        #[tokio::test]
        async fn create_broker_amqp_reaches_a_real_rabbitmq_end_to_end() {
            let Some(url) = integration_url() else {
                return;
            };
            let queue = format!("celers-broker-helper-amqp-{}", Uuid::new_v4().simple());

            let broker = create_broker("amqp", &url, &queue)
                .await
                .expect("create_broker(\"amqp\", ..) against a live server");

            let task = crate::SerializedTask::new("broker_helper.amqp_e2e".to_string(), vec![7]);
            let task_id = broker.enqueue(task).await.expect("enqueue");
            let message = broker
                .dequeue()
                .await
                .expect("dequeue")
                .expect("the message just enqueued is available");
            assert_eq!(message.task.metadata.id, task_id);
            broker
                .ack(&task_id, message.receipt_handle.as_deref())
                .await
                .expect("ack");

            drop_queue(&url, &queue).await;
        }
    }
}
