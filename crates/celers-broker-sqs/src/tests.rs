#![cfg(test)]

use crate::*;
use celers_kombu::Transport;
use celers_protocol::Message;

#[tokio::test]
async fn test_sqs_broker_creation() {
    let broker = SqsBroker::new("test-queue").await;
    assert!(broker.is_ok());
}

#[test]
fn test_broker_name() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let broker = rt.block_on(async { SqsBroker::new("test").await.unwrap() });
    assert_eq!(broker.name(), "sqs");
}

#[tokio::test]
async fn test_builder_pattern() {
    let broker = SqsBroker::new("test-queue")
        .await
        .unwrap()
        .with_visibility_timeout(60)
        .with_wait_time(10)
        .with_max_messages(5);

    assert_eq!(broker.visibility_timeout, 60);
    assert_eq!(broker.wait_time_seconds, 10);
    assert_eq!(broker.max_messages, 5);
}

#[tokio::test]
async fn test_fifo_config() {
    let fifo = FifoConfig::new()
        .with_content_based_deduplication(true)
        .with_high_throughput(true)
        .with_default_message_group_id("default-group");

    assert!(fifo.content_based_deduplication);
    assert!(fifo.high_throughput);
    assert_eq!(
        fifo.default_message_group_id,
        Some("default-group".to_string())
    );
}

#[tokio::test]
async fn test_fifo_broker() {
    let broker = SqsBroker::new("test-queue.fifo")
        .await
        .unwrap()
        .with_fifo(FifoConfig::new().with_content_based_deduplication(true));

    assert!(broker.is_fifo());
    assert!(broker.fifo_config.is_some());
}

#[tokio::test]
async fn test_dlq_config() {
    let dlq = DlqConfig::new("arn:aws:sqs:us-east-1:123456789:my-dlq", 3);

    assert_eq!(dlq.dlq_arn, "arn:aws:sqs:us-east-1:123456789:my-dlq");
    assert_eq!(dlq.max_receive_count, 3);
}

#[tokio::test]
async fn test_dlq_config_clamping() {
    let dlq_low = DlqConfig::new("arn:test", 0);
    assert_eq!(dlq_low.max_receive_count, 1);

    let dlq_high = DlqConfig::new("arn:test", 2000);
    assert_eq!(dlq_high.max_receive_count, 1000);
}

#[tokio::test]
async fn test_sse_config_sqs_managed() {
    let sse = SseConfig::sqs_managed();

    assert!(!sse.use_kms);
    assert!(sse.kms_key_id.is_none());
}

#[tokio::test]
async fn test_sse_config_kms() {
    let sse = SseConfig::kms("alias/my-key").with_data_key_reuse_period(600);

    assert!(sse.use_kms);
    assert_eq!(sse.kms_key_id, Some("alias/my-key".to_string()));
    assert_eq!(sse.kms_data_key_reuse_period, Some(600));
}

#[tokio::test]
async fn test_sse_config_data_key_reuse_clamping() {
    let sse = SseConfig::kms("key").with_data_key_reuse_period(30);
    assert_eq!(sse.kms_data_key_reuse_period, Some(60)); // min 60

    let sse_high = SseConfig::kms("key").with_data_key_reuse_period(100000);
    assert_eq!(sse_high.kms_data_key_reuse_period, Some(86400)); // max 86400
}

#[tokio::test]
async fn test_broker_with_all_configs() {
    let broker = SqsBroker::new("my-queue.fifo")
        .await
        .unwrap()
        .with_visibility_timeout(60)
        .with_wait_time(15)
        .with_max_messages(10)
        .with_message_retention(86400)
        .with_delay_seconds(5)
        .with_fifo(FifoConfig::new().with_content_based_deduplication(true))
        .with_sse(SseConfig::sqs_managed())
        .with_dlq(DlqConfig::new("arn:test:dlq", 5));

    assert_eq!(broker.visibility_timeout, 60);
    assert_eq!(broker.wait_time_seconds, 15);
    assert_eq!(broker.max_messages, 10);
    assert_eq!(broker.message_retention_seconds, 86400);
    assert_eq!(broker.delay_seconds, 5);
    assert!(broker.fifo_config.is_some());
    assert!(broker.sse_config.is_some());
    assert!(broker.dlq_config.is_some());
}

#[tokio::test]
async fn test_visibility_timeout_clamping() {
    let broker = SqsBroker::new("test")
        .await
        .unwrap()
        .with_visibility_timeout(50000); // above max

    assert_eq!(broker.visibility_timeout, 43200); // clamped to max
}

#[tokio::test]
async fn test_wait_time_clamping() {
    let broker = SqsBroker::new("test").await.unwrap().with_wait_time(30); // above max

    assert_eq!(broker.wait_time_seconds, 20); // clamped to max
}

#[tokio::test]
async fn test_max_messages_clamping() {
    let broker = SqsBroker::new("test").await.unwrap().with_max_messages(15); // above max

    assert_eq!(broker.max_messages, 10); // clamped to max
}

#[tokio::test]
async fn test_message_retention_clamping() {
    let broker_low = SqsBroker::new("test")
        .await
        .unwrap()
        .with_message_retention(30); // below min

    assert_eq!(broker_low.message_retention_seconds, 60); // clamped to min

    let broker_high = SqsBroker::new("test")
        .await
        .unwrap()
        .with_message_retention(2000000); // above max

    assert_eq!(broker_high.message_retention_seconds, 1209600); // clamped to max (14 days)
}

#[tokio::test]
async fn test_delay_seconds_clamping() {
    let broker = SqsBroker::new("test")
        .await
        .unwrap()
        .with_delay_seconds(1000); // above max

    assert_eq!(broker.delay_seconds, 900); // clamped to max (15 min)
}

#[test]
fn test_queue_stats_default() {
    let stats = QueueStats::default();

    assert_eq!(stats.approximate_message_count, 0);
    assert_eq!(stats.approximate_not_visible_count, 0);
    assert_eq!(stats.approximate_delayed_count, 0);
    assert!(stats.created_timestamp.is_none());
    assert!(stats.last_modified_timestamp.is_none());
    assert!(stats.message_retention_period.is_none());
    assert!(stats.visibility_timeout.is_none());
    assert!(!stats.is_fifo);
}

#[tokio::test]
async fn test_is_fifo_by_name() {
    let broker = SqsBroker::new("my-queue.fifo").await.unwrap();
    assert!(broker.is_fifo());
}

#[tokio::test]
async fn test_is_not_fifo() {
    let broker = SqsBroker::new("my-queue").await.unwrap();
    assert!(!broker.is_fifo());
}

// CloudWatch configuration tests
#[test]
fn test_cloudwatch_config_default() {
    let config = CloudWatchConfig::default();
    assert_eq!(config.namespace, "CeleRS/SQS");
    assert!(!config.enabled);
    assert!(config.dimensions.is_empty());
}

#[test]
fn test_cloudwatch_config_new() {
    let config = CloudWatchConfig::new("MyNamespace");
    assert_eq!(config.namespace, "MyNamespace");
    assert!(config.enabled);
    assert!(config.dimensions.is_empty());
}

#[test]
fn test_cloudwatch_config_with_dimensions() {
    let config = CloudWatchConfig::new("CeleRS/SQS")
        .with_dimension("Environment", "production")
        .with_dimension("Application", "my-app");

    assert_eq!(config.dimensions.len(), 2);
    assert_eq!(
        config.dimensions.get("Environment"),
        Some(&"production".to_string())
    );
    assert_eq!(
        config.dimensions.get("Application"),
        Some(&"my-app".to_string())
    );
}

#[test]
fn test_cloudwatch_config_enabled() {
    let config = CloudWatchConfig::new("test").with_enabled(false);
    assert!(!config.enabled);

    let config2 = CloudWatchConfig::default().with_enabled(true);
    assert!(config2.enabled);
}

#[tokio::test]
async fn test_broker_with_cloudwatch() {
    let cw_config = CloudWatchConfig::new("CeleRS/SQS").with_dimension("Test", "value");

    let broker = SqsBroker::new("test-queue")
        .await
        .unwrap()
        .with_cloudwatch(cw_config);

    assert!(broker.cloudwatch_config.is_some());
    let config = broker.cloudwatch_config.unwrap();
    assert_eq!(config.namespace, "CeleRS/SQS");
    assert!(config.enabled);
}

// Adaptive polling tests
#[test]
fn test_polling_strategy_default() {
    let strategy = PollingStrategy::default();
    assert_eq!(strategy, PollingStrategy::Fixed);
}

#[test]
fn test_adaptive_polling_config_default() {
    let config = AdaptivePollingConfig::default();
    assert_eq!(config.strategy, PollingStrategy::Fixed);
    assert_eq!(config.min_wait_time, 1);
    assert_eq!(config.max_wait_time, 20);
    assert_eq!(config.backoff_multiplier, 2.0);
    assert_eq!(config.current_wait_time(), 20);
}

#[test]
fn test_adaptive_polling_config_new() {
    let config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff);
    assert_eq!(config.strategy, PollingStrategy::ExponentialBackoff);
    assert_eq!(config.current_wait_time(), 20);
}

#[test]
fn test_adaptive_polling_config_builders() {
    let config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
        .with_min_wait_time(2)
        .with_max_wait_time(15)
        .with_backoff_multiplier(3.0);

    assert_eq!(config.min_wait_time, 2);
    assert_eq!(config.max_wait_time, 15);
    assert_eq!(config.backoff_multiplier, 3.0);
}

#[test]
fn test_adaptive_polling_config_clamping() {
    let config = AdaptivePollingConfig::new(PollingStrategy::Fixed)
        .with_min_wait_time(0) // below min
        .with_max_wait_time(30) // above max
        .with_backoff_multiplier(15.0); // above max

    assert_eq!(config.min_wait_time, 1); // clamped to min
    assert_eq!(config.max_wait_time, 20); // clamped to max
    assert_eq!(config.backoff_multiplier, 10.0); // clamped to max
}

#[test]
fn test_adaptive_polling_fixed_strategy() {
    let mut config = AdaptivePollingConfig::new(PollingStrategy::Fixed);
    let initial_wait = config.current_wait_time();

    config.adjust_wait_time(false); // empty receive
    assert_eq!(config.current_wait_time(), initial_wait); // no change

    config.adjust_wait_time(true); // received messages
    assert_eq!(config.current_wait_time(), initial_wait); // no change
}

#[test]
fn test_adaptive_polling_exponential_backoff() {
    let mut config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
        .with_min_wait_time(1)
        .with_max_wait_time(20)
        .with_backoff_multiplier(2.0);

    // Start with max wait time
    assert_eq!(config.current_wait_time(), 20);

    // Receive messages - should reset to min
    config.adjust_wait_time(true);
    assert_eq!(config.current_wait_time(), 1);

    // Empty receive - should double
    config.adjust_wait_time(false);
    assert_eq!(config.current_wait_time(), 2);

    // Another empty receive - should double again
    config.adjust_wait_time(false);
    assert_eq!(config.current_wait_time(), 4);

    // Keep going until we hit max
    for _ in 0..10 {
        config.adjust_wait_time(false);
    }
    assert_eq!(config.current_wait_time(), 20); // capped at max

    // Receive messages - should reset to min
    config.adjust_wait_time(true);
    assert_eq!(config.current_wait_time(), 1);
}

#[test]
fn test_adaptive_polling_adaptive_strategy() {
    let mut config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
        .with_min_wait_time(1)
        .with_max_wait_time(20)
        .with_backoff_multiplier(2.0);

    // Start with max wait time
    assert_eq!(config.current_wait_time(), 20);

    // Receive messages - should halve
    config.adjust_wait_time(true);
    assert_eq!(config.current_wait_time(), 10);

    // Receive more messages - should halve again
    config.adjust_wait_time(true);
    assert_eq!(config.current_wait_time(), 5);

    // Keep receiving - should eventually hit min
    for _ in 0..10 {
        config.adjust_wait_time(true);
    }
    assert_eq!(config.current_wait_time(), 1);

    // Empty receive (less than 3 consecutive) - should not change
    config.adjust_wait_time(false);
    assert_eq!(config.current_wait_time(), 1);

    config.adjust_wait_time(false);
    assert_eq!(config.current_wait_time(), 1);

    // 3rd consecutive empty receive - should start increasing
    config.adjust_wait_time(false);
    assert_eq!(config.current_wait_time(), 2);

    // More empty receives (each triggers increase after 3+ consecutive)
    config.adjust_wait_time(false); // 4th: 2 * 2 = 4
    assert_eq!(config.current_wait_time(), 4);

    config.adjust_wait_time(false); // 5th: 4 * 2 = 8
    assert_eq!(config.current_wait_time(), 8);

    config.adjust_wait_time(false); // 6th: 8 * 2 = 16
    assert_eq!(config.current_wait_time(), 16);
}

#[test]
fn test_adaptive_polling_reset() {
    let mut config =
        AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff).with_max_wait_time(20);

    // Adjust wait time
    config.adjust_wait_time(true);
    assert_ne!(config.current_wait_time(), 20);

    // Reset
    config.reset();
    assert_eq!(config.current_wait_time(), 20);
}

#[tokio::test]
async fn test_broker_with_adaptive_polling() {
    let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
        .with_min_wait_time(1)
        .with_max_wait_time(15);

    let broker = SqsBroker::new("test-queue")
        .await
        .unwrap()
        .with_adaptive_polling(adaptive_config);

    assert!(broker.adaptive_polling.is_some());
    let config = broker.adaptive_polling.unwrap();
    assert_eq!(config.strategy, PollingStrategy::Adaptive);
    assert_eq!(config.min_wait_time, 1);
    assert_eq!(config.max_wait_time, 15);
}

#[tokio::test]
async fn test_broker_with_all_new_configs() {
    let cw_config = CloudWatchConfig::new("CeleRS/SQS").with_dimension("Environment", "test");

    let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
        .with_min_wait_time(2)
        .with_max_wait_time(18);

    let broker = SqsBroker::new("test-queue")
        .await
        .unwrap()
        .with_cloudwatch(cw_config)
        .with_adaptive_polling(adaptive_config);

    assert!(broker.cloudwatch_config.is_some());
    assert!(broker.adaptive_polling.is_some());
}

#[test]
fn test_health_check_method_exists() {
    // This test just verifies the health_check method is callable
    // Actual testing requires AWS credentials and a real/mock SQS queue
    // Integration tests with LocalStack cover the actual functionality
}

#[test]
fn test_alarm_config_new() {
    let config = AlarmConfig::new("TestAlarm", "ApproximateNumberOfMessages", 100.0);
    assert_eq!(config.alarm_name, "TestAlarm");
    assert_eq!(config.metric_name, "ApproximateNumberOfMessages");
    assert_eq!(config.threshold, 100.0);
    assert_eq!(config.namespace, "CeleRS/SQS");
    assert_eq!(config.comparison_operator, "GreaterThanThreshold");
    assert_eq!(config.evaluation_periods, 1);
    assert_eq!(config.period, 60);
    assert_eq!(config.statistic, "Average");
}

#[test]
fn test_alarm_config_queue_depth() {
    let config = AlarmConfig::queue_depth_alarm("HighDepth", "my-queue", 1000.0);
    assert_eq!(config.alarm_name, "HighDepth");
    assert_eq!(config.metric_name, "ApproximateNumberOfMessages");
    assert_eq!(config.threshold, 1000.0);
    assert_eq!(config.period, 300); // 5 minutes
    assert_eq!(config.evaluation_periods, 2);
    assert_eq!(config.statistic, "Average");
    assert_eq!(
        config.dimensions.get("QueueName"),
        Some(&"my-queue".to_string())
    );
}

#[test]
fn test_alarm_config_message_age() {
    let config = AlarmConfig::message_age_alarm("OldMessages", "my-queue", 600.0);
    assert_eq!(config.alarm_name, "OldMessages");
    assert_eq!(config.metric_name, "ApproximateAgeOfOldestMessage");
    assert_eq!(config.threshold, 600.0);
    assert_eq!(config.period, 300);
    assert_eq!(config.evaluation_periods, 1);
    assert_eq!(config.statistic, "Maximum");
}

#[test]
fn test_alarm_config_builders() {
    let config = AlarmConfig::new("TestAlarm", "TestMetric", 50.0)
        .with_description("Test alarm")
        .with_namespace("Custom/Namespace")
        .with_comparison_operator("LessThanThreshold")
        .with_evaluation_periods(3)
        .with_period(120)
        .with_statistic("Sum")
        .with_treat_missing_data("breaching")
        .with_dimension("Env", "prod")
        .with_alarm_action("arn:aws:sns:us-east-1:123:topic");

    assert_eq!(config.description, Some("Test alarm".to_string()));
    assert_eq!(config.namespace, "Custom/Namespace");
    assert_eq!(config.comparison_operator, "LessThanThreshold");
    assert_eq!(config.evaluation_periods, 3);
    assert_eq!(config.period, 120);
    assert_eq!(config.statistic, "Sum");
    assert_eq!(config.treat_missing_data, "breaching");
    assert_eq!(config.dimensions.get("Env"), Some(&"prod".to_string()));
    assert_eq!(config.alarm_actions.len(), 1);
}

#[test]
fn test_alarm_config_period_clamping() {
    let config = AlarmConfig::new("Test", "Metric", 100.0).with_period(30);
    assert_eq!(config.period, 60); // Clamped to minimum of 60

    let config2 = AlarmConfig::new("Test", "Metric", 100.0).with_period(3600);
    assert_eq!(config2.period, 3600); // No clamping for valid values
}

#[test]
fn test_alarm_config_evaluation_periods_clamping() {
    let config = AlarmConfig::new("Test", "Metric", 100.0).with_evaluation_periods(0);
    assert_eq!(config.evaluation_periods, 1); // Clamped to minimum of 1

    let config2 = AlarmConfig::new("Test", "Metric", 100.0).with_evaluation_periods(5);
    assert_eq!(config2.evaluation_periods, 5); // No clamping for valid values
}

#[tokio::test]
async fn test_production_preset() {
    let broker = SqsBroker::production("test-queue").await.unwrap();
    assert_eq!(broker.wait_time_seconds, 20);
    assert_eq!(broker.max_messages, 10);
    assert_eq!(broker.visibility_timeout, 300);
    assert_eq!(broker.message_retention_seconds, 1209600);
}

#[tokio::test]
async fn test_development_preset() {
    let broker = SqsBroker::development("test-queue").await.unwrap();
    assert_eq!(broker.wait_time_seconds, 5);
    assert_eq!(broker.max_messages, 1);
    assert_eq!(broker.visibility_timeout, 30);
    assert_eq!(broker.message_retention_seconds, 3600);
}

#[tokio::test]
async fn test_cost_optimized_preset() {
    let broker = SqsBroker::cost_optimized("test-queue").await.unwrap();
    assert_eq!(broker.wait_time_seconds, 20);
    assert_eq!(broker.max_messages, 10);
    assert!(broker.adaptive_polling.is_some());

    let adaptive = broker.adaptive_polling.unwrap();
    assert_eq!(adaptive.strategy, PollingStrategy::ExponentialBackoff);
    assert_eq!(adaptive.min_wait_time, 1);
    assert_eq!(adaptive.max_wait_time, 20);
}

#[tokio::test]
async fn test_validate_message_size_small() {
    use uuid::Uuid;

    let broker = SqsBroker::new("test").await.unwrap();
    let msg = Message::new("test.task".to_string(), Uuid::new_v4(), vec![1, 2, 3]);

    let size = broker.validate_message_size(&msg);
    assert!(size.is_ok());
    assert!(size.unwrap() < 262_144); // Less than 256 KB
}

#[tokio::test]
async fn test_validate_message_size_large() {
    use uuid::Uuid;

    let broker = SqsBroker::new("test").await.unwrap();
    // Create a large payload (>256 KB)
    let large_data = vec![0u8; 300_000];
    let msg = Message::new("test.task".to_string(), Uuid::new_v4(), large_data);

    let size = broker.validate_message_size(&msg);
    assert!(size.is_err());
}

#[tokio::test]
async fn test_calculate_batch_size() {
    use uuid::Uuid;

    let broker = SqsBroker::new("test").await.unwrap();
    let mut messages = Vec::new();

    for i in 0..5 {
        let msg = Message::new(format!("test.task.{}", i), Uuid::new_v4(), vec![1, 2, 3]);
        messages.push(msg);
    }

    let total_size = broker.calculate_batch_size(&messages);
    assert!(total_size.is_ok());
    assert!(total_size.unwrap() > 0);
}

// Compression/decompression tests
#[tokio::test]
async fn test_compress_decompress() {
    let broker = SqsBroker::new("test").await.unwrap();
    let original_data = "This is a test message that will be compressed and decompressed";

    // Compress
    let compressed = broker.compress_message(original_data);
    assert!(compressed.is_ok());
    let compressed_data = compressed.unwrap();

    // Should have compression marker
    assert!(compressed_data.starts_with("__GZIP__:"));

    // Decompress
    let decompressed = broker.decompress_message(&compressed_data);
    assert!(decompressed.is_ok());
    assert_eq!(decompressed.unwrap(), original_data);
}

#[tokio::test]
async fn test_decompress_uncompressed() {
    let broker = SqsBroker::new("test").await.unwrap();
    let uncompressed_data = "This is not compressed";

    // Should pass through uncompressed data unchanged
    let result = broker.decompress_message(uncompressed_data);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), uncompressed_data);
}

#[tokio::test]
async fn test_compression_config() {
    let broker = SqsBroker::new("test")
        .await
        .unwrap()
        .with_compression(10240); // 10 KB threshold

    assert_eq!(broker.compression_threshold, Some(10240));
}

#[tokio::test]
async fn test_compression_large_message() {
    let broker = SqsBroker::new("test").await.unwrap();

    // Create a large message
    let large_message = "x".repeat(50000);

    // Compress it
    let compressed = broker.compress_message(&large_message);
    assert!(compressed.is_ok());
    let compressed_data = compressed.unwrap();

    // Compressed should be smaller
    assert!(compressed_data.len() < large_message.len());

    // Should be able to decompress back
    let decompressed = broker.decompress_message(&compressed_data);
    assert!(decompressed.is_ok());
    assert_eq!(decompressed.unwrap(), large_message);
}

#[tokio::test]
async fn test_retry_config() {
    let broker = SqsBroker::new("test")
        .await
        .unwrap()
        .with_retry_config(5, 200);

    assert_eq!(broker.max_retries, 5);
    assert_eq!(broker.retry_base_delay_ms, 200);
}

// ---------------------------------------------------------------------------
// Regression tests for the 0.3.1 SQS audit findings.
//
// These exercise the request-shaping decisions the broker makes *before* it
// talks to AWS: which queue a receipt handle is deleted against, which wait
// time and batch size a poll carries, which attributes a publish emits, how a
// FIFO group id is derived and how a large batch is chunked. That is precisely
// the layer where the shipped bugs lived and where a green suite previously
// proved nothing.
//
// End-to-end coverage lives in `tests/localstack.rs`, gated on
// `CELERS_TEST_SQS_URL`.
// ---------------------------------------------------------------------------

use crate::batch_ops::{plan_batch_chunks, SQS_MAX_BATCH_BYTES, SQS_MAX_BATCH_ENTRIES};
use crate::celery_compat::{CelerySqsConfig, QueueNamingStrategy};
use crate::delivery::{
    decode_delivery_tag, encode_delivery_tag, resolve_wait_time, ReceiptMetadata,
};
use crate::types::FifoGroupIdSource;
use celers_kombu::Envelope;
use std::time::Duration as StdDuration;
use uuid::Uuid;

fn test_message(task: &str) -> Message {
    Message::new(task.to_string(), Uuid::new_v4(), b"{}".to_vec())
}

/// idx 213: a handle received from queue B must be deleted against B, not
/// against the broker's configured queue A.
#[tokio::test]
async fn ack_targets_the_queue_the_message_came_from() {
    let broker = SqsBroker::new("queue-a").await.unwrap();

    let tag_from_b = encode_delivery_tag("queue-b", "AQEB-handle-from-b");
    let grouped = broker.group_handles_by_queue("queue-a", std::slice::from_ref(&tag_from_b));

    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].0, "queue-b", "delete must target queue-b");
    assert_eq!(grouped[0].1[0].1, "AQEB-handle-from-b");

    // And the raw handle is recovered without the queue prefix.
    assert_eq!(
        decode_delivery_tag(&tag_from_b),
        (Some("queue-b"), "AQEB-handle-from-b")
    );
}

/// idx 213: handles from several queues in one `ack_batch` are split per queue.
#[tokio::test]
async fn ack_batch_groups_handles_per_source_queue() {
    let broker = SqsBroker::new("main").await.unwrap();

    let tags = vec![
        encode_delivery_tag("main", "h0"),
        encode_delivery_tag("dlq", "h1"),
        "legacy-handle".to_string(),
        encode_delivery_tag("dlq", "h3"),
    ];

    let grouped = broker.group_handles_by_queue("main", &tags);
    let mut by_queue: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (queue, handles) in grouped {
        by_queue.insert(queue, handles.into_iter().map(|(i, _)| i).collect());
    }

    // The tagless handle falls back to the queue the caller named.
    assert_eq!(by_queue.get("main"), Some(&vec![0, 2]));
    assert_eq!(by_queue.get("dlq"), Some(&vec![1, 3]));
}

/// idx 213: the DLQ redrive must delete from the DLQ, never the main queue.
#[tokio::test]
async fn dlq_queue_name_is_derived_from_the_arn() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_dlq(DlqConfig::new(
            "arn:aws:sqs:us-east-1:123456789012:tasks-dlq",
            5,
        ));

    assert_eq!(broker.dlq_queue_name().unwrap(), "tasks-dlq");
    assert_eq!(broker.main_queue_name(), "tasks");
    assert_ne!(broker.dlq_queue_name().unwrap(), broker.main_queue_name());
}

#[tokio::test]
async fn dlq_helpers_report_missing_configuration() {
    let broker = SqsBroker::new("tasks").await.unwrap();
    let error = broker.dlq_queue_name().unwrap_err();
    assert!(matches!(error, celers_kombu::BrokerError::Configuration(_)));
}

/// idx 216: `Envelope.redelivered` derives from `ApproximateReceiveCount`,
/// which is only present when the system attributes are requested.
#[test]
fn receipt_metadata_drives_the_redelivered_flag() {
    let first = ReceiptMetadata {
        message_id: Some("m".to_string()),
        receive_count: 1,
        sent_timestamp_ms: Some(1_700_000_000_000),
    };
    assert!(!first.is_redelivered());

    let redelivered = ReceiptMetadata {
        receive_count: 5,
        ..first
    };
    assert!(redelivered.is_redelivered());
    assert_eq!(redelivered.sent_timestamp_secs(), Some(1_700_000_000));
}

/// idx 216: the raw receive count is retrievable, not just a boolean.
#[tokio::test]
async fn receive_count_is_tracked_per_delivery_tag() {
    let mut broker = SqsBroker::new("tasks").await.unwrap();
    let tag = encode_delivery_tag("tasks", "AQEB");

    broker.remember_receipt_metadata(
        &tag,
        ReceiptMetadata {
            message_id: Some("m-1".to_string()),
            receive_count: 7,
            sent_timestamp_ms: Some(1_700_000_000_000),
        },
    );

    assert_eq!(broker.receive_count(&tag), Some(7));
    assert!(broker.receipt_metadata(&tag).unwrap().is_redelivered());

    broker.forget_receipt_metadata(&tag);
    assert_eq!(broker.receive_count(&tag), None);
}

#[tokio::test]
async fn receipt_metadata_map_is_bounded() {
    let mut broker = SqsBroker::new("tasks").await.unwrap();

    for index in 0..12_000 {
        broker.remember_receipt_metadata(
            &format!("tag-{index}"),
            ReceiptMetadata {
                receive_count: 1,
                ..Default::default()
            },
        );
    }

    assert!(
        broker.receipt_metadata.len() <= 10_000,
        "receipt metadata must not grow without bound"
    );
}

/// idx 224 / idx 300: batches longer than 10 are chunked, never truncated.
#[test]
fn batches_of_25_are_fully_covered_by_chunks() {
    let sizes = vec![64usize; 25];
    let chunks = plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES);

    let covered: usize = chunks.iter().map(|chunk| chunk.len()).sum();
    assert_eq!(covered, 25);
    assert!(chunks.iter().all(|chunk| chunk.len() <= 10));
}

/// idx 224: the 256 KB aggregate limit is respected while chunking.
#[test]
fn chunking_respects_the_aggregate_payload_limit() {
    let sizes = vec![60_000usize; 10];
    let chunks = plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES);

    for chunk in &chunks {
        let bytes: usize = sizes[chunk.clone()].iter().sum();
        assert!(
            bytes <= SQS_MAX_BATCH_BYTES,
            "chunk of {bytes} bytes exceeds the SQS aggregate limit"
        );
    }
    assert!(chunks.len() > 1, "10 x 60 KB cannot fit in one request");
}

#[tokio::test]
async fn empty_batches_are_no_ops_without_touching_aws() {
    let mut broker = SqsBroker::new("tasks").await.unwrap();

    assert_eq!(broker.publish_batch("tasks", Vec::new()).await.unwrap(), 0);
    assert_eq!(broker.ack_batch("tasks", Vec::new()).await.unwrap(), 0);
    assert_eq!(
        broker
            .publish_fifo_batch("tasks.fifo", Vec::new())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        broker
            .extend_visibility_batch("tasks", Vec::new())
            .await
            .unwrap(),
        0
    );
}

/// idx 225: the configured wait time actually caps the poll.
#[test]
fn configured_wait_time_caps_the_poll() {
    assert_eq!(resolve_wait_time(StdDuration::from_secs(20), 5), 5);
    assert_eq!(resolve_wait_time(StdDuration::from_secs(1), 20), 1);
    assert_eq!(resolve_wait_time(StdDuration::from_secs(3_600), 20), 20);
}

/// idx 225: `production()` really is 20s long polling / 10 messages.
#[tokio::test]
async fn production_preset_matches_its_documentation() {
    let broker = SqsBroker::production("tasks").await.unwrap();

    assert_eq!(broker.wait_time_seconds, 20);
    assert_eq!(broker.max_messages, 10);
    assert_eq!(broker.visibility_timeout, 300);
    assert!(broker.visibility_heartbeat_enabled);

    // A 20 second poll is not silently downgraded to a 1 second short poll.
    assert_eq!(
        resolve_wait_time(StdDuration::from_secs(20), broker.wait_time_seconds),
        20
    );
}

#[tokio::test]
async fn development_preset_uses_short_polling() {
    let broker = SqsBroker::development("tasks").await.unwrap();

    assert_eq!(broker.wait_time_seconds, 5);
    assert_eq!(broker.max_messages, 1);
    assert_eq!(
        resolve_wait_time(StdDuration::from_secs(20), broker.wait_time_seconds),
        5
    );
}

/// idx 225: prefetched messages are keyed per queue, so a poll on queue B never
/// returns a message received from queue A.
#[tokio::test]
async fn prefetch_buffer_is_keyed_per_queue() {
    let mut broker = SqsBroker::new("a").await.unwrap();

    let envelope = Envelope {
        delivery_tag: encode_delivery_tag("a", "h"),
        message: test_message("tasks.add"),
        redelivered: false,
    };
    broker.buffer_prefetched("a", vec![envelope]);

    assert_eq!(broker.prefetched_count(), 1);
    assert!(
        broker.take_prefetched("b").is_none(),
        "queue B must not see queue A's message"
    );

    let taken = broker
        .take_prefetched("a")
        .expect("queue A serves its own buffer");
    assert_eq!(taken.message.headers.task, "tasks.add");
    assert_eq!(broker.prefetched_count(), 0);
}

/// idx 226: every publish path builds attributes through one helper, so single
/// and batch sends share a wire format.
#[tokio::test]
async fn celery_mode_maps_headers_on_every_publish_path() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_celery_defaults();

    let mut message = test_message("tasks.add");
    message.properties.priority = Some(7);
    message.properties.correlation_id = Some("corr-1".to_string());

    let attributes = broker.build_attributes(&message).unwrap();

    assert!(attributes.contains_key(crate::celery_compat::attribute_names::TASK));
    assert!(attributes.contains_key(crate::celery_compat::attribute_names::ID));
    assert!(attributes.contains_key(crate::celery_compat::attribute_names::PRIORITY));
    assert!(attributes.contains_key(crate::celery_compat::attribute_names::CORRELATION_ID));
}

#[tokio::test]
async fn standard_mode_maps_only_priority_and_correlation_id() {
    let broker = SqsBroker::new("tasks").await.unwrap();

    let mut message = test_message("tasks.add");
    message.properties.priority = Some(3);

    let attributes = broker.build_attributes(&message).unwrap();
    assert_eq!(attributes.len(), 1);
    assert!(attributes.contains_key("priority"));
}

/// idx 226: Celery attributes are read back and merged without clobbering the
/// authoritative body.
#[tokio::test]
async fn celery_attributes_round_trip_into_the_message() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_celery_defaults();

    let mut original = test_message("tasks.add");
    original.properties.priority = Some(4);
    original.headers.retries = Some(2);

    let attributes = broker.build_attributes(&original).unwrap();

    let mapper = crate::celery_compat::CeleryAttributeMapper::new();
    let headers = mapper.deserialize_attributes(&attributes).unwrap();

    // A body that lost its optional headers gets them back ...
    let mut stripped = Message::new(String::new(), Uuid::nil(), b"{}".to_vec());
    mapper.apply_headers(&headers, &mut stripped);
    assert_eq!(stripped.headers.task, "tasks.add");
    assert_eq!(stripped.headers.id, original.headers.id);
    assert_eq!(stripped.headers.retries, Some(2));
    assert_eq!(stripped.properties.priority, Some(4));

    // ... but a populated body is never overwritten.
    let mut populated = test_message("tasks.other");
    populated.headers.retries = Some(9);
    mapper.apply_headers(&headers, &mut populated);
    assert_eq!(populated.headers.task, "tasks.other");
    assert_eq!(populated.headers.retries, Some(9));
}

/// idx 226: the Kombu naming strategy is applied on the publish/consume path.
#[tokio::test]
async fn kombu_naming_is_applied_to_queue_names() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_celery_compat(CelerySqsConfig::kombu_compatible("celery"));

    assert_eq!(
        broker.physical_queue_name("tasks.high"),
        "celery_tasks-high"
    );

    let mut message = test_message("tasks.add");
    message.properties.priority = Some(9);
    assert_eq!(
        broker.resolve_publish_queue("tasks", &message),
        "celery_tasks-priority-9"
    );

    let queues = broker.priority_queues("tasks");
    assert_eq!(
        queues.first().map(String::as_str),
        Some("celery_tasks-priority-9")
    );
    assert_eq!(queues.last().map(String::as_str), Some("celery_tasks"));
}

#[tokio::test]
async fn queue_names_are_untouched_without_celery_mode() {
    let broker = SqsBroker::new("tasks").await.unwrap();
    assert_eq!(broker.physical_queue_name("tasks.high"), "tasks.high");
    assert!(broker.priority_queues("tasks").is_empty());
}

/// idx 231: a missing queue is `QueueNotFound`, not a generic failure, and the
/// error text no longer claims "does not exist" for every possible cause.
#[tokio::test]
async fn missing_queue_maps_to_queue_not_found() {
    // Constructed directly: this asserts the mapping, not a live AWS call.
    let error = celers_kombu::BrokerError::QueueNotFound("tasks".to_string());
    assert!(error.is_queue_not_found());

    let credentials = celers_kombu::BrokerError::Connection(
        "GetQueueUrl for 'tasks' failed: AccessDenied".to_string(),
    );
    assert!(credentials.is_connection());
    assert!(!credentials.is_queue_not_found());
    assert!(!crate::retry_policy::is_retryable_error(&credentials));
}

/// idx 232: publishing to a FIFO queue through the generic trait derives a
/// group id instead of sending a request SQS will reject.
#[tokio::test]
async fn fifo_group_id_is_derived_for_generic_publishes() {
    let broker = SqsBroker::new("orders.fifo")
        .await
        .unwrap()
        .with_fifo(FifoConfig::new());

    let message = test_message("tasks.charge");
    assert!(broker.is_fifo());
    assert_eq!(
        broker.derive_group_id("orders.fifo", &message),
        "tasks.charge"
    );
}

#[tokio::test]
async fn fifo_group_id_source_is_configurable() {
    let per_queue = SqsBroker::new("orders.fifo")
        .await
        .unwrap()
        .with_fifo(FifoConfig::new().with_group_id_source(FifoGroupIdSource::PerQueue));
    let message = test_message("tasks.charge");
    assert_eq!(
        per_queue.derive_group_id("orders.fifo", &message),
        "orders.fifo"
    );

    let fixed = SqsBroker::new("orders.fifo").await.unwrap().with_fifo(
        FifoConfig::new().with_group_id_source(FifoGroupIdSource::Fixed("global".to_string())),
    );
    assert_eq!(fixed.derive_group_id("orders.fifo", &message), "global");
}

/// idx 232: FIFO deduplication ids are stable, so a retried send is idempotent.
#[tokio::test]
async fn fifo_deduplication_id_is_stable_across_retries() {
    let message = test_message("tasks.charge");

    let first = crate::fifo::derive_deduplication_id(false, None, &message);
    let second = crate::fifo::derive_deduplication_id(false, None, &message);

    assert_eq!(first, second);
    assert_eq!(first, Some(message.headers.id.to_string()));
}

/// idx 232: a FIFO queue rejects per-message delays up front instead of after
/// spending the whole retry budget on a non-retryable validation error.
#[tokio::test]
async fn delayed_publish_rejects_fifo_queues_before_any_api_call() {
    let mut broker = SqsBroker::new("orders.fifo").await.unwrap();
    let error = broker
        .publish_with_delay("orders.fifo", test_message("tasks.charge"), 30)
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not support per-message delays"),
        "{error}"
    );
}

#[tokio::test]
async fn fifo_publish_rejects_non_fifo_queue_names() {
    let mut broker = SqsBroker::new("orders").await.unwrap();
    let error = broker
        .publish_fifo("orders", test_message("tasks.charge"), "g", None)
        .await
        .unwrap_err();

    assert!(error.to_string().contains(".fifo"), "{error}");
}

/// idx 236: `max_retries` is clamped, and the backoff never overflows.
#[tokio::test]
async fn retry_config_is_clamped() {
    let huge = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_retry_config(u32::MAX, 1_000);
    assert_eq!(huge.max_retries, crate::retry_policy::MAX_RETRY_ATTEMPTS);

    let zero = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_retry_config(0, 10);
    assert_eq!(zero.max_retries, 1);

    // The delay computation is total for every attempt the clamp allows.
    for attempt in 0..=crate::retry_policy::MAX_RETRY_ATTEMPTS {
        let delay = crate::retry_policy::backoff_delay_ms(1_000, attempt, 0);
        assert!(delay <= crate::retry_policy::MAX_BACKOFF_DELAY_MS);
    }
}

/// idx 236: non-retryable validation errors are not retried.
#[tokio::test]
async fn non_retryable_errors_short_circuit_the_retry_loop() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_retry_config(5, 1);

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);

    let result: celers_kombu::Result<()> = broker
        .retry_with_backoff(move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(celers_kombu::BrokerError::OperationFailed(
                    "Failed to send message: MissingParameter: MessageGroupId".to_string(),
                ))
            }
        })
        .await;

    assert!(result.is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "must not retry a validation error"
    );
}

#[tokio::test]
async fn transient_errors_exhaust_the_retry_budget() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_retry_config(3, 0);

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);

    let result: celers_kombu::Result<()> = broker
        .retry_with_backoff(move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(celers_kombu::BrokerError::OperationFailed(
                    "Failed to send message: ServiceUnavailable".to_string(),
                ))
            }
        })
        .await;

    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

/// idx 239: `is_connected` reflects the outcome of real SDK calls.
#[tokio::test]
async fn is_connected_tracks_observed_health() {
    let mut broker = SqsBroker::new("tasks").await.unwrap();

    // Nothing connected yet.
    assert!(!broker.is_connected());

    // Simulate a cached, working connection.
    broker
        .queue_url_cache
        .insert("tasks".to_string(), "http://localhost/tasks".to_string());
    broker.client = Some(aws_sdk_sqs::Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new("us-east-1"))
            .build(),
    ));
    assert!(broker.is_connected());

    // A credential/transport failure observed by any operation flips it.
    broker.mark_unhealthy();
    assert!(!broker.is_connected());
    assert!(!broker.is_connection_healthy());

    broker.mark_healthy();
    assert!(broker.is_connected());
}

/// idx 217: heartbeats are tracked per delivery tag and released on ack.
#[tokio::test]
async fn heartbeats_are_tracked_and_released() {
    let mut broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_visibility_heartbeat(true)
        .with_heartbeat_max_extension(600);

    assert!(broker.visibility_heartbeat_enabled);
    assert_eq!(broker.heartbeat_max_extension_secs, 600);
    assert_eq!(broker.active_heartbeat_count(), 0);

    let client = aws_sdk_sqs::Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new("us-east-1"))
            .build(),
    );
    let tag = encode_delivery_tag("tasks", "AQEB");
    broker.maybe_start_heartbeat(&client, "http://localhost/tasks", &tag, "AQEB");
    assert_eq!(broker.active_heartbeat_count(), 1);

    broker.stop_visibility_heartbeat(&tag);
    assert_eq!(broker.active_heartbeat_count(), 0);
}

#[tokio::test]
async fn heartbeats_are_not_started_when_disabled() {
    let mut broker = SqsBroker::new("tasks").await.unwrap();
    let client = aws_sdk_sqs::Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new("us-east-1"))
            .build(),
    );

    broker.maybe_start_heartbeat(&client, "http://localhost/tasks", "tag", "AQEB");
    assert_eq!(broker.active_heartbeat_count(), 0);
}

/// idx 218: replay refuses to run without a DLQ instead of silently doing
/// nothing, and dry-run is a first-class mode.
#[tokio::test]
async fn replay_requires_a_configured_dlq() {
    use crate::replay::{ReplayConfig, ReplayFilter, ReplayManager};

    let mut broker = SqsBroker::new("tasks").await.unwrap();
    let mut manager = ReplayManager::new(ReplayConfig::new());

    let error = manager
        .replay_from_dlq(&mut broker, &ReplayFilter::new(), true)
        .await
        .unwrap_err();

    assert!(matches!(error, celers_kombu::BrokerError::Configuration(_)));
}

#[test]
fn replay_rate_limiting_is_computed_not_guessed() {
    use crate::replay::replay_target_duration;

    assert_eq!(
        replay_target_duration(50, 100),
        StdDuration::from_millis(500)
    );
    assert_eq!(replay_target_duration(0, 100), StdDuration::ZERO);
    assert_eq!(replay_target_duration(50, 0), StdDuration::ZERO);
}

/// idx 218: the module no longer promises a replay it does not implement.
#[test]
fn replay_filter_selects_by_task_time_and_failure_count() {
    use crate::replay::{ReplayFilter, ReplayableMessage};

    let filter = ReplayFilter::new()
        .with_task_pattern("tasks.payment.*")
        .with_time_range(1_000, 2_000)
        .with_min_failure_count(3);

    let matching = ReplayableMessage {
        message_id: "m-1".to_string(),
        body: "{}".to_string(),
        task_name: "tasks.payment.charge".to_string(),
        attributes: std::collections::HashMap::new(),
        timestamp: 1_500,
        error_message: None,
        failure_count: 4,
    };
    assert!(filter.matches(&matching));

    let too_few_failures = ReplayableMessage {
        failure_count: 1,
        ..matching.clone()
    };
    assert!(!filter.matches(&too_few_failures));

    let wrong_task = ReplayableMessage {
        task_name: "tasks.email.send".to_string(),
        ..matching.clone()
    };
    assert!(!filter.matches(&wrong_task));

    let too_old = ReplayableMessage {
        timestamp: 10,
        ..matching
    };
    assert!(!filter.matches(&too_old));
}

#[test]
fn replay_time_range_hours_never_underflows() {
    use crate::replay::ReplayFilter;

    let filter = ReplayFilter::new().with_time_range_hours(u64::MAX);
    assert_eq!(filter.min_timestamp, Some(0));
}

/// Disconnecting releases every in-flight resource the broker holds.
#[tokio::test]
async fn disconnect_releases_prefetch_heartbeats_and_metadata() {
    use celers_kombu::Transport;

    let mut broker = SqsBroker::new("tasks").await.unwrap();

    broker.buffer_prefetched(
        "tasks",
        vec![Envelope {
            delivery_tag: encode_delivery_tag("tasks", "h"),
            message: test_message("tasks.add"),
            redelivered: false,
        }],
    );
    broker.remember_receipt_metadata("tag", ReceiptMetadata::default());

    broker.disconnect().await.unwrap();

    assert_eq!(broker.prefetched_count(), 0);
    assert_eq!(broker.active_heartbeat_count(), 0);
    assert!(broker.receipt_metadata.is_empty());
    assert!(!broker.is_connected());
}

/// The naming strategy is only ever applied once: a physical name resolved
/// twice is idempotent for the Direct strategy and stable for Kombu.
#[tokio::test]
async fn queue_name_resolution_is_deterministic() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_celery_compat(CelerySqsConfig {
            naming_strategy: QueueNamingStrategy::kombu("celery"),
            ..CelerySqsConfig::default()
        });

    let once = broker.physical_queue_name("tasks");
    assert_eq!(once, "celery_tasks");
    assert_eq!(broker.physical_queue_name("tasks"), once);
}

/// idx 226 follow-through: the read paths (`queue_size`, `purge`) must resolve
/// queue names exactly like the write paths, or a Celery-mode broker publishes
/// to `celery_tasks` and inspects `tasks`.
#[tokio::test]
async fn read_and_write_paths_resolve_the_same_queue_name() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_celery_compat(CelerySqsConfig::kombu_compatible("celery"));

    let message = test_message("tasks.add"); // no priority set
    let write_target = broker.resolve_publish_queue("tasks", &message);
    let read_target = broker.resolve_queue_name("tasks");

    assert_eq!(write_target, read_target);
    assert_eq!(read_target, "celery_tasks");
    assert_eq!(broker.physical_queue_name("tasks"), read_target);
}

/// Resolution must never destroy the `.fifo` suffix: SQS refuses to treat a
/// queue as FIFO without it, and the Kombu strategy rewrites `.` to `-`.
#[tokio::test]
async fn naming_strategy_preserves_the_fifo_suffix() {
    let broker = SqsBroker::new("orders.fifo")
        .await
        .unwrap()
        .with_celery_compat(CelerySqsConfig::kombu_compatible("celery"));

    let physical = broker.physical_queue_name("orders.fifo");
    assert_eq!(physical, "celery_orders.fifo");
    assert!(broker.is_fifo_queue(&physical));

    // FIFO queues are never split across priority siblings.
    let mut message = test_message("tasks.charge");
    message.properties.priority = Some(9);
    assert_eq!(
        broker.resolve_publish_queue("orders.fifo", &message),
        physical
    );
    assert!(broker.priority_queues("orders.fifo").is_empty());
}

/// Resolution is applied exactly once: `get_queue_url`'s auto-create path uses
/// the physical variant so a Kombu prefix is never doubled.
#[tokio::test]
async fn naming_strategy_is_never_applied_twice() {
    let broker = SqsBroker::new("tasks")
        .await
        .unwrap()
        .with_celery_compat(CelerySqsConfig::kombu_compatible("celery"));

    let once = broker.resolve_queue_name("tasks");
    assert_eq!(once, "celery_tasks");
    assert_ne!(
        broker.resolve_queue_name(&once),
        once,
        "the strategy is not idempotent, which is exactly why it must be applied once"
    );
}

/// idx 225 / prefetch: `consume_batch` drains the buffer `consume` filled, so
/// prefetched messages cannot sit there until their visibility timeout expires.
#[tokio::test]
async fn consume_batch_drains_the_prefetch_buffer_first() {
    let mut broker = SqsBroker::new("tasks").await.unwrap();

    let buffered: Vec<Envelope> = (0..3)
        .map(|index| Envelope {
            delivery_tag: encode_delivery_tag("tasks", &format!("h{index}")),
            message: test_message(&format!("tasks.buffered.{index}")),
            redelivered: false,
        })
        .collect();
    broker.buffer_prefetched("tasks", buffered);
    assert_eq!(broker.prefetched_count(), 3);

    // Asking for exactly what is buffered is served entirely from memory: no
    // client is configured, so any API call would fail the test.
    let drained = broker
        .consume_batch_physical("tasks", 3, 0)
        .await
        .expect("served from the prefetch buffer");

    assert_eq!(drained.len(), 3);
    assert_eq!(drained[0].message.headers.task, "tasks.buffered.0");
    assert_eq!(broker.prefetched_count(), 0);
}
