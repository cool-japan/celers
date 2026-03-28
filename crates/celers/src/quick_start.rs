/// Quick Redis broker setup with sensible defaults
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::redis_broker;
///
/// let broker = redis_broker("localhost:6379", "celery")?;
/// ```
#[cfg(feature = "redis")]
pub fn redis_broker(
    url: &str,
    queue: &str,
) -> std::result::Result<crate::RedisBroker, celers_core::error::CelersError> {
    let full_url = if url.starts_with("redis://") {
        url.to_string()
    } else {
        format!("redis://{}", url)
    };
    crate::RedisBroker::new(&full_url, queue)
}

/// Quick PostgreSQL broker setup
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::postgres_broker;
///
/// let broker = postgres_broker(
///     "postgresql://user:pass@localhost/db",
///     "celery"
/// ).await?;
/// ```
#[cfg(feature = "postgres")]
pub async fn postgres_broker(
    url: &str,
    queue: &str,
) -> std::result::Result<crate::PostgresBroker, celers_core::error::CelersError> {
    crate::PostgresBroker::with_queue(url, queue).await
}

/// Quick MySQL broker setup
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::mysql_broker;
///
/// let broker = mysql_broker(
///     "mysql://user:pass@localhost/db",
///     "celery"
/// ).await?;
/// ```
#[cfg(feature = "mysql")]
pub async fn mysql_broker(
    url: &str,
    queue: &str,
) -> std::result::Result<crate::MysqlBroker, celers_core::error::CelersError> {
    crate::MysqlBroker::with_queue(url, queue).await
}

/// Quick AMQP (RabbitMQ) broker setup
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::amqp_broker;
///
/// let broker = amqp_broker(
///     "amqp://guest:guest@localhost:5672",
///     "celery"
/// ).await?;
/// ```
#[cfg(feature = "amqp")]
pub async fn amqp_broker(
    url: &str,
    queue: &str,
) -> std::result::Result<crate::AmqpBroker, celers_core::error::CelersError> {
    crate::AmqpBroker::new(url, queue)
        .await
        .map_err(|e| celers_core::error::CelersError::Broker(e.to_string()))
}

/// Quick SQS broker setup
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::sqs_broker;
///
/// let broker = sqs_broker("celery").await?;
/// ```
#[cfg(feature = "sqs")]
pub async fn sqs_broker(
    queue: &str,
) -> std::result::Result<crate::SqsBroker, celers_core::error::CelersError> {
    crate::SqsBroker::new(queue)
        .await
        .map_err(|e| celers_core::error::CelersError::Broker(e.to_string()))
}

/// Build a WorkerConfig with sensible defaults
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::{redis_broker, default_worker_config};
///
/// let config = default_worker_config()?;
/// // Use config to create worker
/// ```
pub fn default_worker_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new()
        .concurrency(num_cpus::get())
        .build()
}

/// Build a WorkerConfig with custom concurrency
///
/// # Example
/// ```rust,ignore
/// use celers::quick_start::worker_config_with_concurrency;
///
/// let config = worker_config_with_concurrency(8)?;
/// ```
pub fn worker_config_with_concurrency(
    concurrency: usize,
) -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new().concurrency(concurrency).build()
}
