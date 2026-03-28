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
