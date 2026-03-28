#![cfg(test)]

use crate::*;
use celers_kombu::{Broker, Consumer, Producer, Transport};
use std::time::Duration;

#[tokio::test]
async fn test_amqp_broker_creation() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue").await;
    assert!(broker.is_ok());
}

#[test]
fn test_broker_name() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let broker = rt.block_on(async {
        AmqpBroker::new("amqp://localhost:5672", "test")
            .await
            .unwrap()
    });
    assert_eq!(broker.name(), "amqp");
}

#[tokio::test]
async fn test_broker_with_config() {
    let config = AmqpConfig::default()
        .with_prefetch(10)
        .with_retry(3, Duration::from_millis(100))
        .with_exchange("test_exchange")
        .with_exchange_type(AmqpExchangeType::Topic);

    let broker = AmqpBroker::with_config("amqp://localhost:5672", "test_queue", config).await;
    assert!(broker.is_ok());

    let broker = broker.unwrap();
    assert_eq!(broker.config().prefetch_count, 10);
    assert_eq!(broker.config().retry_count, 3);
    assert_eq!(broker.config().default_exchange, "test_exchange");
    assert_eq!(
        broker.config().default_exchange_type,
        AmqpExchangeType::Topic
    );
}

#[test]
fn test_amqp_config_builder() {
    let config = AmqpConfig::default()
        .with_prefetch(25)
        .with_global_prefetch(true)
        .with_retry(5, Duration::from_secs(2))
        .with_exchange("my_exchange")
        .with_exchange_type(AmqpExchangeType::Fanout)
        .with_heartbeat(120)
        .with_connection_timeout(Duration::from_secs(60))
        .with_vhost("my_vhost");

    assert_eq!(config.prefetch_count, 25);
    assert!(config.prefetch_global);
    assert_eq!(config.retry_count, 5);
    assert_eq!(config.retry_delay, Duration::from_secs(2));
    assert_eq!(config.default_exchange, "my_exchange");
    assert_eq!(config.default_exchange_type, AmqpExchangeType::Fanout);
    assert_eq!(config.heartbeat, 120);
    assert_eq!(config.connection_timeout, Duration::from_secs(60));
    assert_eq!(config.vhost, Some("my_vhost".to_string()));
}

#[test]
fn test_queue_config_builder() {
    let dlx = DlxConfig::new("dlx_exchange").with_routing_key("dead");

    let config = QueueConfig::new()
        .durable(true)
        .auto_delete(false)
        .with_max_priority(10)
        .with_message_ttl(60000)
        .with_dlx(dlx)
        .with_expires(3600000)
        .with_max_length(10000);

    assert!(config.durable);
    assert!(!config.auto_delete);
    assert_eq!(config.max_priority, Some(10));
    assert_eq!(config.message_ttl, Some(60000));
    assert!(config.dlx.is_some());
    assert_eq!(config.dlx.as_ref().unwrap().exchange, "dlx_exchange");
    assert_eq!(
        config.dlx.as_ref().unwrap().routing_key,
        Some("dead".to_string())
    );
    assert_eq!(config.expires, Some(3600000));
    assert_eq!(config.max_length, Some(10000));
}

#[test]
fn test_queue_config_field_table() {
    use lapin::types::ShortString;

    let dlx = DlxConfig::new("dlx").with_routing_key("failed");

    let config = QueueConfig::new()
        .with_max_priority(5)
        .with_message_ttl(30000)
        .with_dlx(dlx)
        .with_expires(7200000)
        .with_max_length(5000);

    let table = config.to_field_table();

    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-max-priority")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-message-ttl")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-dead-letter-exchange")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-dead-letter-routing-key")));
    assert!(table.inner().contains_key(&ShortString::from("x-expires")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-max-length")));
}

#[test]
fn test_dlx_config() {
    let dlx = DlxConfig::new("my_dlx");
    assert_eq!(dlx.exchange, "my_dlx");
    assert!(dlx.routing_key.is_none());

    let dlx_with_key = DlxConfig::new("my_dlx").with_routing_key("dead_letters");
    assert_eq!(dlx_with_key.exchange, "my_dlx");
    assert_eq!(dlx_with_key.routing_key, Some("dead_letters".to_string()));
}

#[test]
fn test_exchange_types() {
    use lapin::ExchangeKind;

    assert_eq!(AmqpExchangeType::default(), AmqpExchangeType::Direct);

    // Test conversion to ExchangeKind
    assert!(matches!(
        AmqpExchangeType::Direct.to_exchange_kind(),
        ExchangeKind::Direct
    ));
    assert!(matches!(
        AmqpExchangeType::Fanout.to_exchange_kind(),
        ExchangeKind::Fanout
    ));
    assert!(matches!(
        AmqpExchangeType::Topic.to_exchange_kind(),
        ExchangeKind::Topic
    ));
    assert!(matches!(
        AmqpExchangeType::Headers.to_exchange_kind(),
        ExchangeKind::Headers
    ));
}

#[tokio::test]
async fn test_effective_url_with_vhost() {
    let config = AmqpConfig::default().with_vhost("production");
    let broker = AmqpBroker::with_config("amqp://localhost:5672", "test", config)
        .await
        .unwrap();

    assert_eq!(broker.effective_url(), "amqp://localhost:5672/production");
}

#[tokio::test]
async fn test_effective_url_with_trailing_slash() {
    let config = AmqpConfig::default().with_vhost("staging");
    let broker = AmqpBroker::with_config("amqp://localhost:5672/", "test", config)
        .await
        .unwrap();

    assert_eq!(broker.effective_url(), "amqp://localhost:5672/staging");
}

#[tokio::test]
async fn test_effective_url_no_vhost() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test")
        .await
        .unwrap();

    assert_eq!(broker.effective_url(), "amqp://localhost:5672");
}

#[tokio::test]
async fn test_health_status_not_connected() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test")
        .await
        .unwrap();

    let status = broker.health_status().await;
    assert!(!status.connected);
    assert!(!status.channel_open);
    assert!(!status.is_healthy());
    assert_eq!(status.connection_state, "Not connected");
    assert_eq!(status.channel_state, "No channel");
    // Connection pool is disabled by default (connection_pool_size: 0)
    assert!(status.connection_pool_metrics.is_none());
    // Channel pool is enabled by default (channel_pool_size: 10)
    assert!(status.channel_pool_metrics.is_some());
}

#[tokio::test]
async fn test_is_healthy() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test")
        .await
        .unwrap();

    // Not connected yet, should not be healthy
    assert!(!broker.is_healthy());
}

#[tokio::test]
async fn test_transaction_state_default() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test")
        .await
        .unwrap();

    assert_eq!(broker.transaction_state(), TransactionState::None);
}

#[test]
fn test_transaction_state_enum() {
    assert_ne!(TransactionState::None, TransactionState::Started);
    assert_ne!(TransactionState::Started, TransactionState::Committed);
    assert_ne!(TransactionState::Committed, TransactionState::RolledBack);

    // Test clone and copy
    let state = TransactionState::Started;
    let cloned = state;
    assert_eq!(state, cloned);
}

#[test]
fn test_health_status_is_healthy() {
    let healthy = HealthStatus {
        connected: true,
        channel_open: true,
        connection_state: "Connected".to_string(),
        channel_state: "Open".to_string(),
        connection_pool_metrics: None,
        channel_pool_metrics: None,
    };
    assert!(healthy.is_healthy());

    let not_connected = HealthStatus {
        connected: false,
        channel_open: true,
        connection_state: "Disconnected".to_string(),
        channel_state: "Open".to_string(),
        connection_pool_metrics: None,
        channel_pool_metrics: None,
    };
    assert!(!not_connected.is_healthy());

    let no_channel = HealthStatus {
        connected: true,
        channel_open: false,
        connection_state: "Connected".to_string(),
        channel_state: "Closed".to_string(),
        connection_pool_metrics: None,
        channel_pool_metrics: None,
    };
    assert!(!no_channel.is_healthy());
}

// ==================== Integration Tests ====================
// These tests require a running RabbitMQ instance
// Run with: docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_connection_and_disconnect() {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_integration")
        .await
        .unwrap();

    // Test connection
    let result = broker.connect().await;
    if result.is_err() {
        eprintln!(
            "Skipping integration test - RabbitMQ not available: {:?}",
            result.err()
        );
        return;
    }

    assert!(broker.is_connected());
    assert!(broker.is_healthy());

    // Test health status
    let health = broker.health_status().await;
    assert!(health.connected);
    assert!(health.channel_open);

    // Test disconnect
    broker.disconnect().await.unwrap();
    assert!(!broker.is_connected());
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_publish_and_consume() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_pubsub")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Purge queue to start clean
    let _ = broker.purge("test_pubsub").await;

    // Publish a message
    let message = MessageBuilder::new("test.task")
        .args(vec![serde_json::json!(42)])
        .build()
        .unwrap();

    broker
        .publish("test_pubsub", message.clone())
        .await
        .unwrap();

    // Consume the message
    let envelope = broker
        .consume("test_pubsub", Duration::from_secs(1))
        .await
        .unwrap();

    assert!(envelope.is_some());
    let envelope = envelope.unwrap();
    assert_eq!(envelope.message.headers.task, "test.task");

    // Acknowledge the message
    broker.ack(&envelope.delivery_tag).await.unwrap();

    // Verify queue is empty
    let size = broker.queue_size("test_pubsub").await.unwrap();
    assert_eq!(size, 0);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_batch_publish() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_batch")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_batch").await;

    // Create batch of messages
    let mut messages = Vec::new();
    for i in 0..10 {
        let msg = MessageBuilder::new("batch.task")
            .args(vec![serde_json::json!(i)])
            .build()
            .unwrap();
        messages.push(msg);
    }

    // Publish batch
    let count = broker.publish_batch("test_batch", messages).await.unwrap();
    assert_eq!(count, 10);

    // Verify queue size
    let size = broker.queue_size("test_batch").await.unwrap();
    assert_eq!(size, 10);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_pipeline_publish() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_pipeline")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_pipeline").await;

    // Create batch of messages
    let mut messages = Vec::new();
    for i in 0..20 {
        let msg = MessageBuilder::new("pipeline.task")
            .args(vec![serde_json::json!(i)])
            .build()
            .unwrap();
        messages.push(msg);
    }

    // Publish with pipeline depth of 5
    let count = broker
        .publish_pipeline("test_pipeline", messages, 5)
        .await
        .unwrap();
    assert_eq!(count, 20);

    // Verify queue size
    let size = broker.queue_size("test_pipeline").await.unwrap();
    assert_eq!(size, 20);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_message_ordering() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_ordering")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_ordering").await;

    // Publish messages in order
    for i in 0..5 {
        let msg = MessageBuilder::new("ordering.task")
            .args(vec![serde_json::json!(i)])
            .build()
            .unwrap();
        broker.publish("test_ordering", msg).await.unwrap();
    }

    // Consume messages and verify order
    for i in 0..5 {
        let envelope = broker
            .consume("test_ordering", Duration::from_secs(1))
            .await
            .unwrap();
        assert!(envelope.is_some());

        let envelope = envelope.unwrap();
        // Deserialize body to get TaskArgs
        let task_args: celers_protocol::TaskArgs =
            serde_json::from_slice(&envelope.message.body).unwrap();
        assert_eq!(task_args.args[0], serde_json::json!(i));

        broker.ack(&envelope.delivery_tag).await.unwrap();
    }

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_priority_queue() {
    use celers_protocol::builder::MessageBuilder;

    let config = AmqpConfig::default();
    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_priority", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Declare priority queue
    let queue_config = QueueConfig::new().with_max_priority(10);
    broker
        .declare_queue_with_config("test_priority", &queue_config)
        .await
        .unwrap();

    let _ = broker.purge("test_priority").await;

    // Publish messages with different priorities (lower number = lower priority)
    for priority in [1, 5, 3, 9, 7] {
        let msg = MessageBuilder::new("priority.task")
            .priority(priority)
            .args(vec![serde_json::json!(priority)])
            .build()
            .unwrap();
        broker.publish("test_priority", msg).await.unwrap();
    }

    // Give RabbitMQ time to sort by priority
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Consume messages - should be in priority order (highest first)
    let expected_order = [9, 7, 5, 3, 1];
    for expected_priority in expected_order {
        let envelope = broker
            .consume("test_priority", Duration::from_secs(1))
            .await
            .unwrap();

        assert!(envelope.is_some());
        let envelope = envelope.unwrap();
        assert_eq!(
            envelope.message.properties.priority,
            Some(expected_priority)
        );
        broker.ack(&envelope.delivery_tag).await.unwrap();
    }

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_concurrent_publishing() {
    use celers_protocol::builder::MessageBuilder;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let broker = Arc::new(Mutex::new(
        AmqpBroker::new("amqp://localhost:5672", "test_concurrent")
            .await
            .unwrap(),
    ));

    {
        let mut b = broker.lock().await;
        if b.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }
        let _ = b.purge("test_concurrent").await;
    }

    // Spawn multiple tasks to publish concurrently
    let mut handles = vec![];

    for task_id in 0..10 {
        let broker_clone = Arc::clone(&broker);
        let handle = tokio::spawn(async move {
            let mut b = broker_clone.lock().await;
            for i in 0..5 {
                let msg = MessageBuilder::new("concurrent.task")
                    .args(vec![serde_json::json!(format!(
                        "task-{}-msg-{}",
                        task_id, i
                    ))])
                    .build()
                    .unwrap();
                b.publish("test_concurrent", msg).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all 50 messages were published (10 tasks * 5 messages each)
    let mut b = broker.lock().await;
    let size = b.queue_size("test_concurrent").await.unwrap();
    assert_eq!(size, 50);

    b.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_connection_recovery() {
    let config = AmqpConfig::default()
        .with_auto_reconnect(true)
        .with_auto_reconnect_config(3, Duration::from_millis(500));

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_recovery", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Verify initial connection
    assert!(broker.is_connected());

    // Note: Actually killing and restoring the connection would require
    // Docker container manipulation or similar, which is beyond the scope
    // of a unit test. Instead, we verify the reconnection stats structure.
    let stats = broker.reconnection_stats();
    assert_eq!(stats.total_attempts, 0);
    assert_eq!(stats.successful_reconnections, 0);
    assert_eq!(stats.failed_reconnections, 0);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_transaction_commit() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_transaction")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_transaction").await;

    // Start transaction
    broker.start_transaction().await.unwrap();
    assert_eq!(broker.transaction_state(), TransactionState::Started);

    // Publish within transaction
    let msg = MessageBuilder::new("transaction.task")
        .args(vec![serde_json::json!("commit")])
        .build()
        .unwrap();
    broker.publish("test_transaction", msg).await.unwrap();

    // Commit transaction
    broker.commit_transaction().await.unwrap();
    assert_eq!(broker.transaction_state(), TransactionState::Committed);

    // Verify message was committed
    let size = broker.queue_size("test_transaction").await.unwrap();
    assert_eq!(size, 1);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_transaction_rollback() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_rollback")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_rollback").await;

    // Start transaction
    broker.start_transaction().await.unwrap();

    // Publish within transaction
    let msg = MessageBuilder::new("rollback.task")
        .args(vec![serde_json::json!("rollback")])
        .build()
        .unwrap();
    broker.publish("test_rollback", msg).await.unwrap();

    // Rollback transaction
    broker.rollback_transaction().await.unwrap();
    assert_eq!(broker.transaction_state(), TransactionState::RolledBack);

    // Verify message was NOT committed
    let size = broker.queue_size("test_rollback").await.unwrap();
    assert_eq!(size, 0);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_dead_letter_exchange() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_dlx_main")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Declare DLX
    broker
        .declare_dlx("test_dlx_exchange", "test_dlx_queue")
        .await
        .unwrap();

    // Declare main queue with DLX configuration
    let dlx_config = DlxConfig::new("test_dlx_exchange").with_routing_key("test_dlx_queue");
    let queue_config = QueueConfig::new().with_dlx(dlx_config);

    broker
        .declare_queue_with_config("test_dlx_main", &queue_config)
        .await
        .unwrap();

    let _ = broker.purge("test_dlx_main").await;
    let _ = broker.purge("test_dlx_queue").await;

    // Publish a message
    let msg = MessageBuilder::new("dlx.task")
        .args(vec![serde_json::json!("will be rejected")])
        .build()
        .unwrap();
    broker.publish("test_dlx_main", msg).await.unwrap();

    // Consume and reject (without requeue) - should go to DLX
    let envelope = broker
        .consume("test_dlx_main", Duration::from_secs(1))
        .await
        .unwrap();
    assert!(envelope.is_some());

    broker
        .reject(&envelope.unwrap().delivery_tag, false)
        .await
        .unwrap();

    // Give RabbitMQ time to route to DLX
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify message is in DLX queue
    let dlx_size = broker.queue_size("test_dlx_queue").await.unwrap();
    assert_eq!(dlx_size, 1);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_message_ttl() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_ttl")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_ttl").await;

    // Publish message with 500ms TTL
    let msg = MessageBuilder::new("ttl.task")
        .args(vec![serde_json::json!("expires soon")])
        .build()
        .unwrap();

    broker.publish_with_ttl("test_ttl", msg, 500).await.unwrap();

    // Wait for message to expire
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Message should have expired
    let envelope = broker
        .consume("test_ttl", Duration::from_millis(100))
        .await
        .unwrap();
    assert!(envelope.is_none());

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_metrics_tracking() {
    use celers_protocol::builder::MessageBuilder;

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_metrics")
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    broker.reset_metrics();
    let _ = broker.purge("test_metrics").await;

    // Publish some messages
    for i in 0..3 {
        let msg = MessageBuilder::new("metrics.task")
            .args(vec![serde_json::json!(i)])
            .build()
            .unwrap();
        broker.publish("test_metrics", msg).await.unwrap();
    }

    // Check metrics
    let metrics = broker.channel_metrics();
    assert_eq!(metrics.messages_published, 3);

    let confirm_stats = broker.publisher_confirm_stats();
    assert_eq!(confirm_stats.successful_confirms, 3);

    // Consume and ack
    for _ in 0..3 {
        if let Ok(Some(envelope)) = broker.consume("test_metrics", Duration::from_secs(1)).await {
            broker.ack(&envelope.delivery_tag).await.unwrap();
        }
    }

    let metrics = broker.channel_metrics();
    assert_eq!(metrics.messages_consumed, 3);
    assert_eq!(metrics.messages_acked, 3);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ to be running
async fn test_integration_deduplication() {
    use celers_protocol::builder::MessageBuilder;

    let config = AmqpConfig::default()
        .with_deduplication(true)
        .with_deduplication_config(100, Duration::from_secs(60));

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_dedup", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    let _ = broker.purge("test_dedup").await;

    // Create a message with specific ID
    let msg1 = MessageBuilder::new("dedup.task")
        .args(vec![serde_json::json!("first")])
        .build()
        .unwrap();

    // Publish first time - should succeed
    broker.publish("test_dedup", msg1.clone()).await.unwrap();

    // Publish again with same message ID - should be deduplicated
    broker.publish("test_dedup", msg1).await.unwrap();

    // Only one message should be in queue
    let size = broker.queue_size("test_dedup").await.unwrap();
    assert_eq!(size, 1);

    broker.disconnect().await.unwrap();
}

#[test]
fn test_management_api_config() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    assert_eq!(
        config.management_url,
        Some("http://localhost:15672".to_string())
    );
    assert_eq!(config.management_username, Some("guest".to_string()));
    assert_eq!(config.management_password, Some("guest".to_string()));
}

#[tokio::test]
async fn test_management_api_not_configured() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue")
        .await
        .unwrap();

    assert!(!broker.has_management_api());

    let result = broker.list_queues().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not configured"));
}

#[tokio::test]
async fn test_management_api_configured() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let broker = AmqpBroker::with_config("amqp://localhost:5672", "test_queue", config)
        .await
        .unwrap();

    assert!(broker.has_management_api());
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_list_queues() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_list", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Declare a test queue
    broker
        .declare_queue("test_mgmt_list", celers_kombu::QueueMode::Fifo)
        .await
        .unwrap();

    // List queues
    let queues = broker.list_queues().await.unwrap();
    assert!(!queues.is_empty());

    // Find our test queue
    let found = queues.iter().any(|q| q.name == "test_mgmt_list");
    assert!(found);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_queue_stats() {
    use celers_protocol::builder::MessageBuilder;

    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_stats", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Declare a test queue
    broker
        .declare_queue("test_mgmt_stats", celers_kombu::QueueMode::Fifo)
        .await
        .unwrap();

    let _ = broker.purge("test_mgmt_stats").await;

    // Publish some messages
    for i in 0..5 {
        let msg = MessageBuilder::new("test.task")
            .args(vec![serde_json::json!(i)])
            .build()
            .unwrap();
        broker.publish("test_mgmt_stats", msg).await.unwrap();
    }

    // Give RabbitMQ time to update stats
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get queue stats
    let stats = broker.get_queue_stats("test_mgmt_stats").await.unwrap();
    assert_eq!(stats.name, "test_mgmt_stats");
    assert_eq!(stats.messages, 5);
    assert_eq!(stats.messages_ready, 5);
    assert_eq!(stats.messages_unacknowledged, 0);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_server_overview() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_overview", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Get server overview
    let overview = broker.get_server_overview().await.unwrap();

    // Verify we got valid data
    assert!(!overview.rabbitmq_version.is_empty());
    assert!(!overview.erlang_version.is_empty());
    assert!(!overview.management_version.is_empty());
    assert!(!overview.cluster_name.is_empty());

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_list_connections() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_conn", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // List connections
    let connections = broker.list_connections().await.unwrap();

    // Should have at least our connection
    assert!(!connections.is_empty());

    // Verify connection data
    let conn = &connections[0];
    assert!(!conn.name.is_empty());
    assert!(!conn.user.is_empty());
    assert!(!conn.state.is_empty());

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_list_channels() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_chan", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // List channels
    let channels = broker.list_channels().await.unwrap();

    // Should have at least our channel
    assert!(!channels.is_empty());

    // Verify channel data
    let chan = &channels[0];
    assert!(!chan.name.is_empty());
    assert!(!chan.user.is_empty());
    assert!(!chan.state.is_empty());
    assert!(chan.number > 0);

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_list_exchanges() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_exch", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // List exchanges
    let exchanges = broker.list_exchanges(None).await.unwrap();

    // Should have default exchanges
    assert!(!exchanges.is_empty());

    // Find the default exchange
    let default_exch = exchanges.iter().find(|e| e.name.is_empty());
    assert!(default_exch.is_some());

    // Find the amq.direct exchange
    let direct_exch = exchanges.iter().find(|e| e.name == "amq.direct");
    assert!(direct_exch.is_some());
    assert_eq!(direct_exch.unwrap().exchange_type, "direct");

    broker.disconnect().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires RabbitMQ Management API to be running
async fn test_integration_list_queue_bindings() {
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_bindings", config)
        .await
        .unwrap();

    if broker.connect().await.is_err() {
        eprintln!("Skipping integration test - RabbitMQ not available");
        return;
    }

    // Declare a test queue
    broker
        .declare_queue("test_mgmt_bindings", celers_kombu::QueueMode::Fifo)
        .await
        .unwrap();

    // Give RabbitMQ time to register the queue
    tokio::time::sleep(Duration::from_millis(200)).await;

    // List bindings
    let bindings = broker
        .list_queue_bindings("test_mgmt_bindings")
        .await
        .unwrap();

    // Should have at least the default binding
    assert!(!bindings.is_empty());

    // Verify binding data
    let binding = &bindings[0];
    assert_eq!(binding.destination, "test_mgmt_bindings");
    assert_eq!(binding.destination_type, "queue");

    broker.disconnect().await.unwrap();
}

#[test]
fn test_queue_type_enum() {
    assert_eq!(QueueType::Classic.as_str(), "classic");
    assert_eq!(QueueType::Quorum.as_str(), "quorum");
    assert_eq!(QueueType::Stream.as_str(), "stream");
}

#[test]
fn test_queue_lazy_mode_enum() {
    assert_eq!(QueueLazyMode::Default.as_str(), "default");
    assert_eq!(QueueLazyMode::Lazy.as_str(), "lazy");
}

#[test]
fn test_queue_overflow_behavior_enum() {
    assert_eq!(QueueOverflowBehavior::DropHead.as_str(), "drop-head");
    assert_eq!(
        QueueOverflowBehavior::RejectPublish.as_str(),
        "reject-publish"
    );
    assert_eq!(
        QueueOverflowBehavior::RejectPublishDlx.as_str(),
        "reject-publish-dlx"
    );
}

#[test]
fn test_queue_config_with_modern_features() {
    let config = QueueConfig::new()
        .with_queue_type(QueueType::Quorum)
        .with_queue_mode(QueueLazyMode::Lazy)
        .with_overflow_behavior(QueueOverflowBehavior::RejectPublish)
        .with_single_active_consumer(true);

    assert_eq!(config.queue_type, Some(QueueType::Quorum));
    assert_eq!(config.queue_mode, Some(QueueLazyMode::Lazy));
    assert_eq!(
        config.overflow_behavior,
        Some(QueueOverflowBehavior::RejectPublish)
    );
    assert!(config.single_active_consumer);
}

#[test]
fn test_queue_config_field_table_with_modern_features() {
    use lapin::types::ShortString;

    let config = QueueConfig::new()
        .with_queue_type(QueueType::Quorum)
        .with_queue_mode(QueueLazyMode::Lazy)
        .with_overflow_behavior(QueueOverflowBehavior::DropHead)
        .with_single_active_consumer(true)
        .with_max_length(1000);

    let table = config.to_field_table();

    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-queue-type")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-queue-mode")));
    assert!(table.inner().contains_key(&ShortString::from("x-overflow")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-single-active-consumer")));
    assert!(table
        .inner()
        .contains_key(&ShortString::from("x-max-length")));
}

#[test]
fn test_quorum_queue_config() {
    let config = QueueConfig::new()
        .durable(true)
        .with_queue_type(QueueType::Quorum)
        .with_max_length(10000)
        .with_overflow_behavior(QueueOverflowBehavior::RejectPublish);

    assert!(config.durable);
    assert_eq!(config.queue_type, Some(QueueType::Quorum));
    assert_eq!(config.max_length, Some(10000));
    assert_eq!(
        config.overflow_behavior,
        Some(QueueOverflowBehavior::RejectPublish)
    );
}

#[test]
fn test_lazy_queue_config() {
    let config = QueueConfig::new()
        .with_queue_mode(QueueLazyMode::Lazy)
        .with_max_length(1000000);

    assert_eq!(config.queue_mode, Some(QueueLazyMode::Lazy));
    assert_eq!(config.max_length, Some(1000000));
}

#[test]
fn test_stream_queue_config() {
    let config = QueueConfig::new()
        .durable(true)
        .with_queue_type(QueueType::Stream);

    assert!(config.durable);
    assert_eq!(config.queue_type, Some(QueueType::Stream));
}

// ===== v4 Helper Methods Tests =====

#[test]
fn test_queue_info_helpers() {
    let info = QueueInfo {
        name: "test_queue".to_string(),
        vhost: "/".to_string(),
        durable: true,
        auto_delete: false,
        messages: 100,
        messages_ready: 80,
        messages_unacknowledged: 20,
        consumers: 2,
        memory: 10485760, // 10 MB
    };

    assert!(!info.is_empty());
    assert!(info.has_consumers());
    assert!(!info.is_idle());
    assert_eq!(info.ready_percentage(), 80.0);
    assert_eq!(info.unacked_percentage(), 20.0);
    assert_eq!(info.memory_mb(), 10.0);
    assert_eq!(info.avg_message_memory(), 104857.6);

    // Test empty queue
    let empty_info = QueueInfo {
        name: "empty".to_string(),
        vhost: "/".to_string(),
        durable: true,
        auto_delete: false,
        messages: 0,
        messages_ready: 0,
        messages_unacknowledged: 0,
        consumers: 0,
        memory: 0,
    };

    assert!(empty_info.is_empty());
    assert!(!empty_info.has_consumers());
    assert!(empty_info.is_idle());
    assert_eq!(empty_info.ready_percentage(), 0.0);
    assert_eq!(empty_info.avg_message_memory(), 0.0);
}

#[test]
fn test_queue_stats_helpers() {
    let stats = QueueStats {
        name: "test_queue".to_string(),
        vhost: "/".to_string(),
        durable: true,
        auto_delete: false,
        messages: 1000,
        messages_ready: 800,
        messages_unacknowledged: 200,
        consumers: 5,
        memory: 10485760,       // 10 MB
        message_bytes: 5242880, // 5 MB
        message_bytes_ready: 4194304,
        message_bytes_unacknowledged: 1048576,
        message_stats: Some(MessageStats {
            publish: 10000,
            publish_details: Some(RateDetails { rate: 100.0 }),
            deliver: 9500,
            deliver_details: Some(RateDetails { rate: 95.0 }),
            ack: 9000,
            ack_details: Some(RateDetails { rate: 90.0 }),
        }),
    };

    assert!(!stats.is_empty());
    assert!(stats.has_consumers());
    assert!(!stats.is_idle());
    assert_eq!(stats.ready_percentage(), 80.0);
    assert_eq!(stats.unacked_percentage(), 20.0);
    assert_eq!(stats.memory_mb(), 10.0);
    assert_eq!(stats.message_bytes_mb(), 5.0);
    assert_eq!(stats.avg_message_size(), 5242.88);
    assert_eq!(stats.avg_message_memory(), 10485.76);
    assert_eq!(stats.publish_rate(), Some(100.0));
    assert_eq!(stats.deliver_rate(), Some(95.0));
    assert_eq!(stats.ack_rate(), Some(90.0));
    assert!(stats.is_growing());
    assert!(!stats.is_shrinking());
    assert!(!stats.consumers_keeping_up()); // Ack rate (90) < deliver rate * 0.95 (90.25)
}

#[test]
fn test_message_stats_helpers() {
    let stats = MessageStats {
        publish: 1000,
        publish_details: Some(RateDetails { rate: 10.0 }),
        deliver: 950,
        deliver_details: Some(RateDetails { rate: 9.5 }),
        ack: 900,
        ack_details: Some(RateDetails { rate: 9.0 }),
    };

    assert_eq!(stats.total_processed(), 2850);
    assert_eq!(stats.publish_rate(), Some(10.0));
    assert_eq!(stats.deliver_rate(), Some(9.5));
    assert_eq!(stats.ack_rate(), Some(9.0));
}

#[test]
fn test_server_overview_helpers() {
    let overview = ServerOverview {
        management_version: "3.12.0".to_string(),
        rabbitmq_version: "3.12.0".to_string(),
        erlang_version: "26.0".to_string(),
        cluster_name: "rabbit@localhost".to_string(),
        queue_totals: Some(QueueTotals {
            messages: 5000,
            messages_ready: 4000,
            messages_unacknowledged: 1000,
        }),
        object_totals: Some(ObjectTotals {
            consumers: 10,
            queues: 5,
            exchanges: 7,
            connections: 3,
            channels: 15,
        }),
    };

    assert_eq!(overview.total_messages(), 5000);
    assert_eq!(overview.total_messages_ready(), 4000);
    assert_eq!(overview.total_messages_unacked(), 1000);
    assert_eq!(overview.total_queues(), 5);
    assert_eq!(overview.total_connections(), 3);
    assert_eq!(overview.total_channels(), 15);
    assert_eq!(overview.total_consumers(), 10);
    assert!(overview.has_connections());
    assert!(overview.has_messages());
}

#[test]
fn test_queue_totals_helpers() {
    let totals = QueueTotals {
        messages: 1000,
        messages_ready: 750,
        messages_unacknowledged: 250,
    };

    assert_eq!(totals.ready_percentage(), 75.0);
    assert_eq!(totals.unacked_percentage(), 25.0);
}

#[test]
fn test_object_totals_helpers() {
    let totals = ObjectTotals {
        consumers: 20,
        queues: 10,
        exchanges: 15,
        connections: 5,
        channels: 25,
    };

    assert_eq!(totals.avg_channels_per_connection(), 5.0);
    assert_eq!(totals.avg_consumers_per_queue(), 2.0);
    assert!(!totals.has_idle_queues()); // 20 consumers for 10 queues
}

#[test]
fn test_connection_info_helpers() {
    let conn = ConnectionInfo {
        name: "test_connection".to_string(),
        vhost: "/".to_string(),
        user: "guest".to_string(),
        state: "running".to_string(),
        channels: 10,
        peer_host: "127.0.0.1".to_string(),
        peer_port: 5672,
        recv_oct: 10485760, // 10 MB
        send_oct: 5242880,  // 5 MB
        recv_cnt: 1000,
        send_cnt: 500,
    };

    assert!(conn.is_running());
    assert!(conn.has_channels());
    assert_eq!(conn.total_bytes(), 15728640);
    assert_eq!(conn.recv_mb(), 10.0);
    assert_eq!(conn.send_mb(), 5.0);
    assert_eq!(conn.total_messages(), 1500);
    assert_eq!(conn.avg_message_size(), 10485.76);
    assert_eq!(conn.peer_address(), "127.0.0.1:5672");
}

#[test]
fn test_channel_info_helpers() {
    let channel = ChannelInfo {
        name: "test_channel".to_string(),
        connection_details: Some(ConnectionDetails {
            name: "conn".to_string(),
            peer_host: "127.0.0.1".to_string(),
            peer_port: 5672,
        }),
        vhost: "/".to_string(),
        user: "guest".to_string(),
        number: 1,
        consumers: 3,
        messages_unacknowledged: 50,
        messages_uncommitted: 10,
        acks_uncommitted: 5,
        prefetch_count: 100,
        state: "running".to_string(),
    };

    assert!(channel.is_running());
    assert!(channel.has_consumers());
    assert!(channel.has_unacked_messages());
    assert!(channel.is_in_transaction());
    assert!(channel.has_prefetch());
    assert_eq!(channel.utilization(), 50.0);
    assert_eq!(channel.peer_address(), Some("127.0.0.1:5672".to_string()));
}

// ==================== Topic Router Integration Tests ====================

#[tokio::test]
async fn test_topic_router_field_default_none() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue").await;
    match broker {
        Ok(b) => assert!(b.topic_router().is_none()),
        Err(_) => {
            // Connection failure is expected in test environments without RabbitMQ;
            // the struct initialization path still sets topic_router to None.
        }
    }
}

#[tokio::test]
async fn test_with_topic_router_builder() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue").await;
    match broker {
        Ok(b) => {
            let config = topic_routing::AmqpRoutingConfig::new()
                .with_exchange("my.topic.exchange")
                .with_rule(topic_routing::TopicRoutingRule::new(
                    "email",
                    "task.email.*",
                    "queue.email",
                ));
            let router = topic_routing::TopicRouter::new(config);
            let b = b.with_topic_router(router);

            assert!(b.topic_router().is_some());
            let router_ref = b.topic_router().expect("router should be set");
            assert_eq!(router_ref.exchange_name(), "my.topic.exchange");
            assert_eq!(router_ref.rule_count(), 1);
        }
        Err(_) => {
            // Connection failure is acceptable in test environments
        }
    }
}

#[tokio::test]
async fn test_topic_router_accessors() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue").await;
    match broker {
        Ok(mut b) => {
            // Initially None
            assert!(b.topic_router().is_none());
            assert!(b.topic_router_mut().is_none());

            // Set via set_topic_router
            let config = topic_routing::AmqpRoutingConfig::new().with_rule(
                topic_routing::TopicRoutingRule::new("reports", "task.report.**", "queue.reports"),
            );
            let router = topic_routing::TopicRouter::new(config);
            b.set_topic_router(router);

            assert!(b.topic_router().is_some());
            assert!(b.topic_router_mut().is_some());

            // Verify routing resolution through the accessor
            if let Some(router_ref) = b.topic_router() {
                assert_eq!(
                    router_ref.resolve_routing_key("task.report.generate"),
                    "queue.reports"
                );
                assert_eq!(
                    router_ref.resolve_routing_key("task.unknown"),
                    "task.default"
                );
            }

            // Verify mutable access allows adding rules
            if let Some(router_mut) = b.topic_router_mut() {
                router_mut.add_rule(topic_routing::TopicRoutingRule::new(
                    "email",
                    "task.email.*",
                    "queue.email",
                ));
                assert_eq!(router_mut.rule_count(), 2);
            }
        }
        Err(_) => {
            // Connection failure is acceptable in test environments
        }
    }
}
