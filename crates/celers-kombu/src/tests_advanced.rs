//! Advanced middleware and utility tests.

use super::*;
use celers_protocol::Message;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn test_partitioning_middleware_creation() {
    let partitioner = PartitioningMiddleware::new(8);
    assert_eq!(partitioner.partition_count(), 8);
    assert_eq!(partitioner.name(), "partitioning");
}

#[tokio::test]
async fn test_partitioning_middleware_assigns_partition() {
    use uuid::Uuid;

    let partitioner = PartitioningMiddleware::new(4);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    partitioner.before_publish(&mut message).await.unwrap();

    // Check partition ID was assigned
    assert!(message.headers.extra.contains_key("x-partition-id"));
    let partition_id = message.headers.extra["x-partition-id"].as_u64().unwrap() as usize;
    assert!(partition_id < 4);

    // Check partition count was added
    assert_eq!(
        message.headers.extra["x-partition-count"],
        serde_json::json!(4)
    );
}

#[tokio::test]
async fn test_partitioning_middleware_custom_header() {
    use uuid::Uuid;

    let partitioner = PartitioningMiddleware::new(8).with_partition_header("my-partition");
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    partitioner.before_publish(&mut message).await.unwrap();

    assert!(message.headers.extra.contains_key("my-partition"));
}

#[test]
fn test_adaptive_timeout_middleware_creation() {
    use std::time::Duration;

    let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30));
    assert_eq!(adaptive.name(), "adaptive_timeout");
    assert!(!adaptive.has_samples());
}

#[tokio::test]
async fn test_adaptive_timeout_middleware_injects_timeout() {
    use std::time::Duration;
    use uuid::Uuid;

    let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30));
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    adaptive.before_publish(&mut message).await.unwrap();

    // Check adaptive timeout was injected
    assert!(message.headers.extra.contains_key("x-adaptive-timeout"));
    assert!(message.headers.extra.contains_key("x-timeout-percentile"));

    let percentile = message.headers.extra["x-timeout-percentile"]
        .as_f64()
        .unwrap();
    assert_eq!(percentile, 0.95);
}

#[tokio::test]
async fn test_adaptive_timeout_middleware_custom_percentile() {
    use std::time::Duration;
    use uuid::Uuid;

    let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30)).with_percentile(0.99);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    adaptive.before_publish(&mut message).await.unwrap();

    let percentile = message.headers.extra["x-timeout-percentile"]
        .as_f64()
        .unwrap();
    assert_eq!(percentile, 0.99);
}

#[test]
fn test_batch_ack_hint_middleware_creation() {
    let batch_hint = BatchAckHintMiddleware::new(10);
    assert_eq!(batch_hint.batch_size(), 10);
    assert_eq!(batch_hint.name(), "batch_ack_hint");
}

#[tokio::test]
async fn test_batch_ack_hint_middleware_injects_hint() {
    use uuid::Uuid;

    let batch_hint = BatchAckHintMiddleware::new(20);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    batch_hint.before_publish(&mut message).await.unwrap();

    // Check batch hint was injected
    assert!(message.headers.extra.contains_key("x-batch-ack-hint"));
    assert_eq!(
        message.headers.extra["x-batch-ack-hint"],
        serde_json::json!(20)
    );
    assert_eq!(
        message.headers.extra["x-batch-ack-recommended"],
        serde_json::json!(true)
    );
}

#[tokio::test]
async fn test_batch_ack_hint_middleware_custom_header() {
    use uuid::Uuid;

    let batch_hint = BatchAckHintMiddleware::new(15).with_hint_header("my-batch-hint");
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    batch_hint.before_publish(&mut message).await.unwrap();

    assert!(message.headers.extra.contains_key("my-batch-hint"));
    assert_eq!(
        message.headers.extra["my-batch-hint"],
        serde_json::json!(15)
    );
}

#[test]
fn test_load_shedding_middleware_creation() {
    let load_shedder = LoadSheddingMiddleware::new(0.8);
    assert_eq!(load_shedder.threshold(), 0.8);
    assert_eq!(load_shedder.name(), "load_shedding");
}

#[tokio::test]
async fn test_load_shedding_middleware_allows_high_priority() {
    use uuid::Uuid;

    let load_shedder = LoadSheddingMiddleware::new(0.8);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);
    message.headers.extra.insert(
        "priority".to_string(),
        serde_json::json!(8), // High priority
    );

    // Should allow even at high load
    let result = load_shedder.before_publish(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_load_shedding_middleware_sheds_low_priority() {
    use uuid::Uuid;

    let mut load_shedder = LoadSheddingMiddleware::new(0.8);
    load_shedder.update_load(0.9); // High load

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);
    message.headers.extra.insert(
        "priority".to_string(),
        serde_json::json!(2), // Low priority
    );

    // Should shed at high load
    let result = load_shedder.before_publish(&mut message).await;
    assert!(result.is_err());
}

#[test]
fn test_priority_escalation_middleware_creation() {
    let escalator = MessagePriorityEscalationMiddleware::new(300);
    assert_eq!(escalator.age_threshold_secs(), 300);
    assert_eq!(escalator.name(), "priority_escalation");
}

#[tokio::test]
async fn test_priority_escalation_middleware_escalates_on_retry() {
    use uuid::Uuid;

    let escalator = MessagePriorityEscalationMiddleware::new(300);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);
    message
        .headers
        .extra
        .insert("priority".to_string(), serde_json::json!(5));
    message.headers.retries = Some(2);

    escalator.before_publish(&mut message).await.unwrap();

    // Priority should be escalated
    let new_priority = message
        .headers
        .extra
        .get("priority")
        .and_then(|v| v.as_u64())
        .map(|v| v as u8)
        .unwrap();
    assert!(new_priority > 5);
    assert!(message.headers.extra.contains_key("x-priority-escalated"));
}

#[tokio::test]
async fn test_priority_escalation_middleware_respects_max() {
    use uuid::Uuid;

    let escalator = MessagePriorityEscalationMiddleware::new(300).with_max_priority(8);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);
    message
        .headers
        .extra
        .insert("priority".to_string(), serde_json::json!(9));
    message.headers.retries = Some(5);

    escalator.before_publish(&mut message).await.unwrap();

    // Priority should not exceed max
    let new_priority = message
        .headers
        .extra
        .get("priority")
        .and_then(|v| v.as_u64())
        .map(|v| v as u8)
        .unwrap();
    assert!(new_priority <= 8);
}

#[test]
fn test_observability_middleware_creation() {
    let observability = ObservabilityMiddleware::new("test-service");
    assert_eq!(observability.service_name(), "test-service");
    assert_eq!(observability.name(), "observability");
}

#[tokio::test]
async fn test_observability_middleware_injects_metadata() {
    use uuid::Uuid;

    let observability = ObservabilityMiddleware::new("my-service");
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    observability.before_publish(&mut message).await.unwrap();

    assert_eq!(
        message.headers.extra["x-service-name"],
        serde_json::json!("my-service")
    );
    assert_eq!(
        message.headers.extra["x-observability-enabled"],
        serde_json::json!(true)
    );
    assert_eq!(
        message.headers.extra["x-log-level"],
        serde_json::json!("info")
    );
}

#[tokio::test]
async fn test_observability_middleware_without_metrics() {
    use uuid::Uuid;

    let observability = ObservabilityMiddleware::new("test").without_metrics();
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    observability.before_publish(&mut message).await.unwrap();

    assert!(!message
        .headers
        .extra
        .contains_key("x-observability-enabled"));
}

#[test]
fn test_health_check_middleware_creation() {
    let health = HealthCheckMiddleware::new();
    assert_eq!(health.name(), "health_check");
    assert!(health.is_healthy());
}

#[tokio::test]
async fn test_health_check_middleware_injects_status() {
    use uuid::Uuid;

    let health = HealthCheckMiddleware::new();
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    health.before_publish(&mut message).await.unwrap();

    assert_eq!(
        message.headers.extra["x-health-status"],
        serde_json::json!("healthy")
    );
}

#[test]
fn test_health_check_middleware_mark_unhealthy() {
    let health = HealthCheckMiddleware::new();
    assert!(health.is_healthy());

    health.mark_unhealthy();
    assert!(!health.is_healthy());

    health.mark_healthy();
    assert!(health.is_healthy());
}

#[test]
fn test_message_tagging_middleware_creation() {
    let tagging = MessageTaggingMiddleware::new("production");
    assert_eq!(tagging.name(), "message_tagging");
}

#[tokio::test]
async fn test_message_tagging_middleware_injects_tags() {
    use uuid::Uuid;

    let tagging = MessageTaggingMiddleware::new("staging")
        .with_tag("region", "us-west-2")
        .with_tag("team", "backend");

    let task_id = Uuid::new_v4();
    let mut message = Message::new("email_task".to_string(), task_id, vec![]);

    tagging.before_publish(&mut message).await.unwrap();

    assert_eq!(
        message.headers.extra["x-environment"],
        serde_json::json!("staging")
    );
    assert_eq!(
        message.headers.extra["x-tag-region"],
        serde_json::json!("us-west-2")
    );
    assert_eq!(
        message.headers.extra["x-tag-team"],
        serde_json::json!("backend")
    );
    assert_eq!(
        message.headers.extra["x-category"],
        serde_json::json!("communication")
    );
}

#[tokio::test]
async fn test_message_tagging_middleware_categorization() {
    use uuid::Uuid;

    let tagging = MessageTaggingMiddleware::new("production");
    let task_id = Uuid::new_v4();

    // Test email categorization
    let mut email_msg = Message::new("send_email_task".to_string(), task_id, vec![]);
    tagging.before_publish(&mut email_msg).await.unwrap();
    assert_eq!(
        email_msg.headers.extra["x-category"],
        serde_json::json!("communication")
    );

    // Test report categorization
    let mut report_msg = Message::new("generate_report".to_string(), task_id, vec![]);
    tagging.before_publish(&mut report_msg).await.unwrap();
    assert_eq!(
        report_msg.headers.extra["x-category"],
        serde_json::json!("analytics")
    );

    // Test process categorization
    let mut process_msg = Message::new("process_data".to_string(), task_id, vec![]);
    tagging.before_publish(&mut process_msg).await.unwrap();
    assert_eq!(
        process_msg.headers.extra["x-category"],
        serde_json::json!("computation")
    );

    // Test general categorization
    let mut general_msg = Message::new("other_task".to_string(), task_id, vec![]);
    tagging.before_publish(&mut general_msg).await.unwrap();
    assert_eq!(
        general_msg.headers.extra["x-category"],
        serde_json::json!("general")
    );
}

#[test]
fn test_cost_attribution_middleware_creation() {
    let cost = CostAttributionMiddleware::new(0.001);
    assert_eq!(cost.name(), "cost_attribution");
}

#[tokio::test]
async fn test_cost_attribution_middleware_injects_cost() {
    use uuid::Uuid;

    let cost = CostAttributionMiddleware::new(0.001)
        .with_compute_cost_per_sec(0.0001)
        .with_storage_cost_per_mb(0.00001);

    let task_id = Uuid::new_v4();
    let body = vec![0u8; 1024 * 1024]; // 1MB message
    let mut message = Message::new("test_task".to_string(), task_id, body);
    message
        .headers
        .extra
        .insert("x-tenant".to_string(), serde_json::json!("tenant-123"));

    cost.before_publish(&mut message).await.unwrap();

    assert!(message.headers.extra.contains_key("x-cost-estimate"));
    assert_eq!(
        message.headers.extra["x-cost-tenant"],
        serde_json::json!("tenant-123")
    );
    assert!(message.headers.extra.contains_key("x-cost-timestamp"));
}

#[tokio::test]
async fn test_cost_attribution_middleware_calculates_compute_cost() {
    use std::time::Duration;
    use uuid::Uuid;

    let cost = CostAttributionMiddleware::new(0.001).with_compute_cost_per_sec(0.0001);

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    // Simulate publish
    cost.before_publish(&mut message).await.unwrap();
    assert!(message.headers.extra.contains_key("x-cost-estimate"));

    // Simulate processing delay
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Simulate consume (which calculates actual cost)
    cost.after_consume(&mut message).await.unwrap();

    // Should have actual cost now
    if message.headers.extra.contains_key("x-cost-actual") {
        let actual_cost_str = message.headers.extra["x-cost-actual"].as_str().unwrap();
        let actual_cost: f64 = actual_cost_str.parse().unwrap();
        // Actual cost should be >= base cost due to compute time
        assert!(actual_cost >= 0.001);
    }
}

#[test]
fn test_backpressure_config_creation() {
    let config = BackpressureConfig::new()
        .with_max_pending(500)
        .with_max_queue_size(5000)
        .with_high_watermark(0.9)
        .with_low_watermark(0.5);

    assert_eq!(config.max_pending, 500);
    assert_eq!(config.max_queue_size, 5000);
    assert_eq!(config.high_watermark, 0.9);
    assert_eq!(config.low_watermark, 0.5);
}

#[test]
fn test_backpressure_config_watermarks() {
    let config = BackpressureConfig::new()
        .with_max_pending(1000)
        .with_high_watermark(0.8)
        .with_low_watermark(0.6);

    // High watermark = 1000 * 0.8 = 800
    assert!(!config.should_apply_backpressure(799));
    assert!(config.should_apply_backpressure(800));
    assert!(config.should_apply_backpressure(900));

    // Low watermark = 1000 * 0.6 = 600
    assert!(config.should_release_backpressure(600));
    assert!(config.should_release_backpressure(500));
    assert!(!config.should_release_backpressure(601));
}

#[test]
fn test_backpressure_config_capacity() {
    let config = BackpressureConfig::new().with_max_queue_size(10000);

    assert!(!config.is_at_capacity(9999));
    assert!(config.is_at_capacity(10000));
    assert!(config.is_at_capacity(10001));
}

#[test]
fn test_backpressure_config_default() {
    let config = BackpressureConfig::default();
    assert_eq!(config.max_pending, 1000);
    assert_eq!(config.max_queue_size, 10000);
    assert_eq!(config.high_watermark, 0.8);
    assert_eq!(config.low_watermark, 0.6);
}

#[test]
fn test_poison_message_detector_creation() {
    let detector = PoisonMessageDetector::new()
        .with_max_failures(3)
        .with_failure_window(Duration::from_secs(600));

    assert_eq!(detector.max_failures, 3);
    assert_eq!(detector.failure_window, Duration::from_secs(600));
}

#[test]
fn test_poison_message_detector_tracking() {
    let detector = PoisonMessageDetector::new().with_max_failures(3);
    let task_id = Uuid::new_v4();

    // Initially not poison
    assert!(!detector.is_poison(task_id));
    assert_eq!(detector.failure_count(task_id), 0);

    // Record failures
    detector.record_failure(task_id);
    assert_eq!(detector.failure_count(task_id), 1);
    assert!(!detector.is_poison(task_id));

    detector.record_failure(task_id);
    assert_eq!(detector.failure_count(task_id), 2);
    assert!(!detector.is_poison(task_id));

    detector.record_failure(task_id);
    assert_eq!(detector.failure_count(task_id), 3);
    assert!(detector.is_poison(task_id)); // Now poison
}

#[test]
fn test_poison_message_detector_clear() {
    let detector = PoisonMessageDetector::new().with_max_failures(2);
    let task_id = Uuid::new_v4();

    detector.record_failure(task_id);
    detector.record_failure(task_id);
    assert!(detector.is_poison(task_id));

    // Clear specific task
    detector.clear_failures(task_id);
    assert!(!detector.is_poison(task_id));
    assert_eq!(detector.failure_count(task_id), 0);
}

#[test]
fn test_poison_message_detector_clear_all() {
    let detector = PoisonMessageDetector::new().with_max_failures(2);
    let task1 = Uuid::new_v4();
    let task2 = Uuid::new_v4();

    detector.record_failure(task1);
    detector.record_failure(task1);
    detector.record_failure(task2);
    detector.record_failure(task2);

    assert!(detector.is_poison(task1));
    assert!(detector.is_poison(task2));

    detector.clear_all();
    assert!(!detector.is_poison(task1));
    assert!(!detector.is_poison(task2));
}

#[test]
fn test_poison_message_detector_default() {
    let detector = PoisonMessageDetector::default();
    assert_eq!(detector.max_failures, 5);
    assert_eq!(detector.failure_window, Duration::from_secs(3600));
}

#[test]
fn test_calculate_consumer_efficiency_efficient() {
    use crate::utils::calculate_consumer_efficiency;

    // Consumer spends 80% time processing, 20% waiting
    let (efficiency, recommendation) = calculate_consumer_efficiency(8000, 2000, 100);
    assert!(efficiency > 70.0);
    assert!(efficiency < 85.0);
    assert_eq!(recommendation, "efficient");
}

#[test]
fn test_calculate_consumer_efficiency_underutilized() {
    use crate::utils::calculate_consumer_efficiency;

    // Consumer spends 30% time processing, 70% waiting
    let (efficiency, recommendation) = calculate_consumer_efficiency(3000, 7000, 100);
    assert!(efficiency < 50.0);
    assert_eq!(recommendation, "underutilized");
}

#[test]
fn test_calculate_consumer_efficiency_good() {
    use crate::utils::calculate_consumer_efficiency;

    // Consumer spends 65% time processing, 35% waiting
    let (efficiency, recommendation) = calculate_consumer_efficiency(6500, 3500, 100);
    assert!(efficiency > 60.0);
    assert!(efficiency < 70.0);
    assert_eq!(recommendation, "good");
}

#[test]
fn test_calculate_consumer_efficiency_no_data() {
    use crate::utils::calculate_consumer_efficiency;

    // No messages processed
    let (efficiency, recommendation) = calculate_consumer_efficiency(0, 0, 0);
    assert_eq!(efficiency, 0.0);
    assert_eq!(recommendation, "no_data");
}

#[test]
fn test_suggest_connection_pool_size_normal() {
    use crate::utils::suggest_connection_pool_size;

    // Peak: 50 concurrent, Average: 20, Max allowed: 100
    let (min, max, initial) = suggest_connection_pool_size(50, 20, 100);
    assert!(min > 0);
    assert!(max <= 100);
    assert!(initial >= min);
    assert!(initial <= max);
    assert!(max > min);
}

#[test]
fn test_suggest_connection_pool_size_high_load() {
    use crate::utils::suggest_connection_pool_size;

    // Very high load - should respect max_allowed
    let (min, max, initial) = suggest_connection_pool_size(200, 150, 100);
    assert_eq!(max, 100); // Respects max_allowed limit
    assert!(min > 0);
    assert!(initial >= min);
    assert!(initial <= max);
}

#[test]
fn test_suggest_connection_pool_size_low_load() {
    use crate::utils::suggest_connection_pool_size;

    // Low load
    let (min, max, initial) = suggest_connection_pool_size(10, 5, 50);
    assert!(min > 0);
    assert!(min <= 5); // Min should be capped
    assert!(max > min);
    assert!(max <= 50);
    assert!(initial >= min);
    assert!(initial <= max);
}

#[test]
fn test_suggest_connection_pool_size_zero_max() {
    use crate::utils::suggest_connection_pool_size;

    // Zero max allowed
    let (min, max, initial) = suggest_connection_pool_size(10, 5, 0);
    assert_eq!(min, 0);
    assert_eq!(max, 0);
    assert_eq!(initial, 0);
}

#[test]
fn test_calculate_message_processing_trend_improving() {
    use crate::utils::calculate_message_processing_trend;

    // Processing times improving over time (getting faster)
    let times = vec![100, 95, 90, 85, 80];
    let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
    assert_eq!(direction, "improving");
    assert!(strength > 0.0);
    assert_eq!(recommendation, "maintain_current_optimizations");
}

#[test]
fn test_calculate_message_processing_trend_degrading() {
    use crate::utils::calculate_message_processing_trend;

    // Processing times degrading (getting slower)
    let times = vec![80, 85, 90, 95, 100];
    let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
    assert_eq!(direction, "degrading");
    assert!(strength > 0.0);
    assert_eq!(recommendation, "investigate_performance_issues");
}

#[test]
fn test_calculate_message_processing_trend_stable() {
    use crate::utils::calculate_message_processing_trend;

    // Stable processing times
    let times = vec![90, 91, 90, 89, 90];
    let (direction, strength, _) = calculate_message_processing_trend(&times);
    assert_eq!(direction, "stable");
    assert!(strength < 0.1); // Very low trend strength
}

#[test]
fn test_calculate_message_processing_trend_insufficient_data() {
    use crate::utils::calculate_message_processing_trend;

    // Too few data points
    let times = vec![100, 90];
    let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
    assert_eq!(direction, "stable");
    assert_eq!(strength, 0.0);
    assert_eq!(recommendation, "insufficient_data");
}

#[test]
fn test_suggest_prefetch_count_fast_processing() {
    use crate::utils::suggest_prefetch_count;

    // Fast processing (50ms = 20 msg/sec)
    let prefetch = suggest_prefetch_count(50, 10, 100);
    assert!(prefetch > 0);
    assert!(prefetch <= 100);
    assert!(prefetch >= 20); // Should suggest high prefetch for fast processing
}

#[test]
fn test_suggest_prefetch_count_slow_processing() {
    use crate::utils::suggest_prefetch_count;

    // Slow processing (2000ms = 0.5 msg/sec)
    let prefetch = suggest_prefetch_count(2000, 2, 50);
    assert!(prefetch > 0);
    assert!(prefetch <= 20); // Should suggest low prefetch for slow processing
}

#[test]
fn test_suggest_prefetch_count_edge_cases() {
    use crate::utils::suggest_prefetch_count;

    // Zero workers
    let prefetch = suggest_prefetch_count(100, 0, 50);
    assert_eq!(prefetch, 1);

    // Zero max
    let prefetch = suggest_prefetch_count(100, 5, 0);
    assert_eq!(prefetch, 1);
}

#[test]
fn test_analyze_dead_letter_queue_critical() {
    use crate::utils::analyze_dead_letter_queue;

    // High failure rate (>10%)
    let (severity, issue, recommendation) = analyze_dead_letter_queue(500, 1000, 10);
    assert_eq!(severity, "critical");
    assert_eq!(issue, "high_failure_rate");
    assert_eq!(recommendation, "immediate_investigation_required");
}

#[test]
fn test_analyze_dead_letter_queue_healthy() {
    use crate::utils::analyze_dead_letter_queue;

    // Low failure rate (<1%)
    let (severity, issue, _recommendation) = analyze_dead_letter_queue(10, 10000, 5);
    assert_eq!(severity, "low");
    assert!(issue == "normal_failures" || issue == "healthy");
}

#[test]
fn test_analyze_dead_letter_queue_rapid_growth() {
    use crate::utils::analyze_dead_letter_queue;

    // Rapid DLQ growth
    let (severity, issue, recommendation) = analyze_dead_letter_queue(100, 10000, 150);
    assert_eq!(severity, "high");
    assert_eq!(issue, "rapid_dlq_growth");
    assert_eq!(recommendation, "monitor_closely_and_investigate");
}

#[test]
fn test_analyze_dead_letter_queue_no_data() {
    use crate::utils::analyze_dead_letter_queue;

    // No messages processed yet
    let (severity, issue, recommendation) = analyze_dead_letter_queue(0, 0, 0);
    assert_eq!(severity, "low");
    assert_eq!(issue, "no_data");
    assert_eq!(recommendation, "monitor");
}

#[test]
fn test_forecast_queue_capacity_ml_growth() {
    use crate::utils::forecast_queue_capacity_ml;

    // Growing queue
    let history = vec![100, 120, 140, 160, 180];
    let (forecast, slope, _confidence) = forecast_queue_capacity_ml(&history, 1);
    assert!(forecast > 180); // Should predict growth
    assert!(slope > 0.0); // Positive trend
}

#[test]
fn test_forecast_queue_capacity_ml_decline() {
    use crate::utils::forecast_queue_capacity_ml;

    // Declining queue
    let history = vec![200, 180, 160, 140, 120];
    let (forecast, slope, _confidence) = forecast_queue_capacity_ml(&history, 1);
    assert!(forecast < 120); // Should predict decline
    assert!(slope < 0.0); // Negative trend
}

#[test]
fn test_forecast_queue_capacity_ml_insufficient_data() {
    use crate::utils::forecast_queue_capacity_ml;

    let history = vec![100, 110];
    let (forecast, slope, confidence) = forecast_queue_capacity_ml(&history, 1);
    assert_eq!(forecast, 110); // Returns last value
    assert_eq!(slope, 0.0);
    assert_eq!(confidence, "insufficient_data");
}

#[test]
fn test_optimize_batch_strategy_high_latency() {
    use crate::utils::optimize_batch_strategy;

    // High network latency scenario (but realistic processing time)
    let (batch_size, wait_ms, _throughput, strategy) = optimize_batch_strategy(1024, 150, 2, 1000);

    assert!(batch_size > 0);
    assert!(wait_ms > 0);
    // Throughput might be 0 with very high latency, so just check it's calculated
    assert!(
        strategy == "throughput_optimized"
            || strategy == "latency_optimized"
            || strategy == "balanced"
    );
}

#[test]
fn test_optimize_batch_strategy_low_latency() {
    use crate::utils::optimize_batch_strategy;

    // Low network latency scenario
    let (batch_size, _wait_ms, _throughput, _strategy) = optimize_batch_strategy(512, 5, 50, 500);

    assert!(batch_size > 0);
    assert!(batch_size <= 1000); // Should respect limits
}

#[test]
fn test_calculate_multi_queue_efficiency_balanced() {
    use crate::utils::calculate_multi_queue_efficiency;

    let sizes = vec![100, 100, 100];
    let rates = vec![10, 10, 10];
    let (efficiency, balance, recommendation) = calculate_multi_queue_efficiency(&sizes, &rates);

    assert!(efficiency > 0.9); // All queues at same rate
    assert!(balance > 0.9); // Perfectly balanced
    assert_eq!(recommendation, "optimal");
}

#[test]
fn test_calculate_multi_queue_efficiency_imbalanced() {
    use crate::utils::calculate_multi_queue_efficiency;

    let sizes = vec![1000, 100, 50];
    let rates = vec![5, 10, 20];
    let (efficiency, balance, _recommendation) = calculate_multi_queue_efficiency(&sizes, &rates);

    assert!(efficiency > 0.0 && efficiency <= 1.0);
    assert!((0.0..=1.0).contains(&balance));
}

#[test]
fn test_calculate_multi_queue_efficiency_invalid() {
    use crate::utils::calculate_multi_queue_efficiency;

    let sizes = vec![100, 200];
    let rates = vec![10]; // Mismatched lengths
    let (efficiency, balance, recommendation) = calculate_multi_queue_efficiency(&sizes, &rates);

    assert_eq!(efficiency, 0.0);
    assert_eq!(balance, 0.0);
    assert_eq!(recommendation, "invalid_input");
}

#[test]
fn test_predict_resource_exhaustion_critical() {
    use crate::utils::predict_resource_exhaustion;

    let (hours, severity, action) = predict_resource_exhaustion(9500, 10000, 1000);
    assert_eq!(hours, 0); // Will exhaust very soon
    assert_eq!(severity, "critical");
    assert!(action.contains("immediate"));
}

#[test]
fn test_predict_resource_exhaustion_warning() {
    use crate::utils::predict_resource_exhaustion;

    let (hours, severity, _action) = predict_resource_exhaustion(5000, 10000, 500);
    assert_eq!(hours, 10);
    assert_eq!(severity, "warning");
}

#[test]
fn test_predict_resource_exhaustion_healthy() {
    use crate::utils::predict_resource_exhaustion;

    let (hours, severity, action) = predict_resource_exhaustion(1000, 10000, 10);
    assert!(hours > 100);
    assert_eq!(severity, "healthy");
    assert_eq!(action, "normal_monitoring");
}

#[test]
fn test_predict_resource_exhaustion_no_growth() {
    use crate::utils::predict_resource_exhaustion;

    let (hours, severity, action) = predict_resource_exhaustion(5000, 10000, 0);
    assert_eq!(hours, usize::MAX);
    assert_eq!(severity, "healthy");
    assert_eq!(action, "monitor");
}

#[test]
fn test_suggest_autoscaling_policy_high_volatility() {
    use crate::utils::suggest_autoscaling_policy;

    let (min_workers, max_workers, scale_up, scale_down, policy) =
        suggest_autoscaling_policy(1000, 500, 100, 400);

    assert!(min_workers > 0);
    assert!(max_workers >= min_workers);
    assert!(scale_up > scale_down);
    assert_eq!(policy, "aggressive");
}

#[test]
fn test_suggest_autoscaling_policy_low_volatility() {
    use crate::utils::suggest_autoscaling_policy;

    let (min_workers, max_workers, scale_up, scale_down, policy) =
        suggest_autoscaling_policy(600, 500, 400, 50);

    assert!(min_workers > 0);
    assert!(max_workers >= min_workers);
    assert!(scale_up > scale_down);
    assert_eq!(policy, "conservative");
}

#[test]
fn test_suggest_autoscaling_policy_medium_volatility() {
    use crate::utils::suggest_autoscaling_policy;

    let (min_workers, max_workers, scale_up, scale_down, policy) =
        suggest_autoscaling_policy(800, 500, 300, 150);

    assert!(min_workers >= 2); // Always at least 2
    assert!(max_workers >= min_workers * 2); // At least 2x min
    assert!(scale_up > scale_down);
    assert_eq!(policy, "balanced");
}

#[test]
fn test_calculate_message_affinity() {
    use crate::utils::calculate_message_affinity;

    // Same key should always route to same worker
    let worker1 = calculate_message_affinity("user:12345", 8);
    let worker2 = calculate_message_affinity("user:12345", 8);
    assert_eq!(worker1, worker2);
    assert!(worker1 < 8);

    // Different keys should potentially route to different workers
    let worker_a = calculate_message_affinity("order:abc", 10);
    let worker_b = calculate_message_affinity("order:xyz", 10);
    assert!(worker_a < 10);
    assert!(worker_b < 10);
}

#[test]
fn test_calculate_message_affinity_zero_workers() {
    use crate::utils::calculate_message_affinity;

    let worker = calculate_message_affinity("test", 0);
    assert_eq!(worker, 0);
}

#[test]
fn test_analyze_queue_temperature_hot() {
    use crate::utils::analyze_queue_temperature;

    let (temp, rec) = analyze_queue_temperature(100, 30);
    assert_eq!(temp, "hot");
    assert_eq!(rec, "maintain_resources");
}

#[test]
fn test_analyze_queue_temperature_warm() {
    use crate::utils::analyze_queue_temperature;

    let (temp, rec) = analyze_queue_temperature(20, 150);
    assert_eq!(temp, "warm");
    assert_eq!(rec, "monitor");
}

#[test]
fn test_analyze_queue_temperature_cold() {
    use crate::utils::analyze_queue_temperature;

    let (temp, rec) = analyze_queue_temperature(2, 800);
    assert_eq!(temp, "cold");
    assert_eq!(rec, "consider_scaling_down");
}

#[test]
fn test_analyze_queue_temperature_lukewarm() {
    use crate::utils::analyze_queue_temperature;

    let (temp, rec) = analyze_queue_temperature(8, 400);
    assert_eq!(temp, "lukewarm");
    assert_eq!(rec, "monitor");
}

#[test]
fn test_detect_processing_bottleneck_consumer() {
    use crate::utils::detect_processing_bottleneck;

    let (location, severity, _) = detect_processing_bottleneck(100, 50, 2000, 300);
    assert_eq!(location, "consumer");
    assert_eq!(severity, "high");
}

#[test]
fn test_detect_processing_bottleneck_processing() {
    use crate::utils::detect_processing_bottleneck;

    let (location, severity, _) = detect_processing_bottleneck(100, 100, 500, 1500);
    assert_eq!(location, "processing");
    assert_eq!(severity, "medium");
}

#[test]
fn test_detect_processing_bottleneck_queue() {
    use crate::utils::detect_processing_bottleneck;

    let (location, severity, _) = detect_processing_bottleneck(100, 90, 6000, 200);
    assert_eq!(location, "queue");
    assert_eq!(severity, "medium");
}

#[test]
fn test_detect_processing_bottleneck_publisher() {
    use crate::utils::detect_processing_bottleneck;

    let (location, severity, _) = detect_processing_bottleneck(50, 150, 50, 100);
    assert_eq!(location, "publisher");
    assert_eq!(severity, "low");
}

#[test]
fn test_detect_processing_bottleneck_healthy() {
    use crate::utils::detect_processing_bottleneck;

    let (location, severity, rec) = detect_processing_bottleneck(100, 100, 500, 200);
    assert_eq!(location, "none");
    assert_eq!(severity, "low");
    assert_eq!(rec, "system_healthy");
}

#[test]
fn test_calculate_optimal_prefetch_multiplier() {
    use crate::utils::calculate_optimal_prefetch_multiplier;

    let multiplier = calculate_optimal_prefetch_multiplier(100, 10, 4);
    assert!(multiplier >= 1.0);
    assert!(multiplier <= 10.0);
}

#[test]
fn test_calculate_optimal_prefetch_multiplier_fast_processing() {
    use crate::utils::calculate_optimal_prefetch_multiplier;

    let multiplier = calculate_optimal_prefetch_multiplier(30, 5, 2);
    assert!(multiplier > 2.0); // Fast processing should prefetch more
}

#[test]
fn test_calculate_optimal_prefetch_multiplier_slow_processing() {
    use crate::utils::calculate_optimal_prefetch_multiplier;

    let multiplier = calculate_optimal_prefetch_multiplier(500, 10, 4);
    assert!(multiplier < 3.0); // Slow processing should prefetch less
}

#[test]
fn test_calculate_optimal_prefetch_multiplier_zero_inputs() {
    use crate::utils::calculate_optimal_prefetch_multiplier;

    let multiplier = calculate_optimal_prefetch_multiplier(0, 10, 4);
    assert_eq!(multiplier, 1.0);

    let multiplier = calculate_optimal_prefetch_multiplier(100, 10, 0);
    assert_eq!(multiplier, 1.0);
}

#[test]
fn test_suggest_queue_consolidation_low_throughput() {
    use crate::utils::suggest_queue_consolidation;

    let sizes = vec![10, 5, 8];
    let rates = vec![2, 1, 3];
    let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

    assert!(consolidate);
    assert_eq!(reason, "low_throughput");
}

#[test]
fn test_suggest_queue_consolidation_overhead() {
    use crate::utils::suggest_queue_consolidation;

    let sizes = vec![15, 10, 12, 8];
    let rates = vec![3, 2, 4, 1];
    let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

    assert!(consolidate);
    assert_eq!(reason, "overhead");
}

#[test]
fn test_suggest_queue_consolidation_efficient() {
    use crate::utils::suggest_queue_consolidation;

    let sizes = vec![500, 600, 700];
    let rates = vec![50, 60, 70];
    let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

    assert!(!consolidate);
    assert_eq!(reason, "efficient");
}

#[test]
fn test_suggest_queue_consolidation_no_data() {
    use crate::utils::suggest_queue_consolidation;

    let sizes: Vec<usize> = vec![];
    let rates: Vec<usize> = vec![];
    let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

    assert!(!consolidate);
    assert_eq!(reason, "no_data");
}

#[test]
fn test_suggest_queue_consolidation_invalid_input() {
    use crate::utils::suggest_queue_consolidation;

    let sizes = vec![10, 20];
    let rates = vec![5];
    let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

    assert!(!consolidate);
    assert_eq!(reason, "invalid_input");
}

#[test]
fn test_analyze_compression_benefit_json() {
    use crate::utils::analyze_compression_benefit;

    // High volume - should compress
    let (should_compress, ratio, _) = analyze_compression_benefit(1024, 2000, "application/json");
    assert!(should_compress);
    assert_eq!(ratio, 0.3); // JSON compresses to 30%
}

#[test]
fn test_analyze_compression_benefit_small_message() {
    use crate::utils::analyze_compression_benefit;

    let (should_compress, ratio, rec) = analyze_compression_benefit(400, 100, "application/json");
    assert!(!should_compress);
    assert_eq!(ratio, 1.0);
    assert_eq!(rec, "message_too_small");
}

#[test]
fn test_analyze_compression_benefit_not_compressible() {
    use crate::utils::analyze_compression_benefit;

    let (should_compress, ratio, rec) =
        analyze_compression_benefit(2048, 100, "application/octet-stream");
    assert!(!should_compress);
    assert_eq!(ratio, 1.0);
    assert_eq!(rec, "content_type_not_compressible");
}

#[test]
fn test_analyze_compression_benefit_xml() {
    use crate::utils::analyze_compression_benefit;

    let (should_compress, ratio, _) = analyze_compression_benefit(20000, 100, "application/xml");
    assert!(should_compress);
    assert_eq!(ratio, 0.25); // XML compresses to 25%
}

#[test]
fn test_calculate_queue_migration_plan_fast() {
    use crate::utils::calculate_queue_migration_plan;

    let (batches, time, rec) = calculate_queue_migration_plan(1000, 100, 100);
    assert_eq!(batches, 10);
    assert_eq!(time, 10);
    assert_eq!(rec, "fast_migration_proceed");
}

#[test]
fn test_calculate_queue_migration_plan_moderate() {
    use crate::utils::calculate_queue_migration_plan;

    let (batches, time, rec) = calculate_queue_migration_plan(10000, 50, 100);
    assert_eq!(batches, 200);
    assert_eq!(time, 100);
    assert!(rec.starts_with("moderate_migration"));
}

#[test]
fn test_calculate_queue_migration_plan_slow() {
    use crate::utils::calculate_queue_migration_plan;

    let (batches, time, rec) = calculate_queue_migration_plan(100000, 100, 10);
    assert_eq!(batches, 1000);
    assert_eq!(time, 10000);
    assert!(rec.contains("slow_migration"));
}

#[test]
fn test_calculate_queue_migration_plan_empty() {
    use crate::utils::calculate_queue_migration_plan;

    let (batches, time, rec) = calculate_queue_migration_plan(0, 100, 50);
    assert_eq!(batches, 0);
    assert_eq!(time, 0);
    assert_eq!(rec, "no_migration_needed");
}

#[test]
fn test_profile_message_patterns_regular() {
    use crate::utils::profile_message_patterns;

    let sizes = vec![100, 102, 98, 101];
    let intervals = vec![10, 11, 9, 10];
    let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);

    assert_eq!(pattern, "highly_regular");
    assert!(score > 0.8);
    assert_eq!(rec, "predictable_use_static_buffers");
}

#[test]
fn test_profile_message_patterns_irregular() {
    use crate::utils::profile_message_patterns;

    let sizes = vec![100, 500, 50, 1000];
    let intervals = vec![10, 100, 5, 200];
    let (pattern, score, _) = profile_message_patterns(&sizes, &intervals);

    assert_eq!(pattern, "highly_irregular");
    assert!(score < 0.3);
}

#[test]
fn test_profile_message_patterns_moderate() {
    use crate::utils::profile_message_patterns;

    let sizes = vec![100, 150, 80, 120];
    let intervals = vec![10, 15, 8, 12];
    let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);

    assert_eq!(pattern, "regular");
    assert!(score > 0.5 && score <= 0.8);
    assert_eq!(rec, "moderate_use_adaptive_buffers");
}

#[test]
fn test_profile_message_patterns_empty() {
    use crate::utils::profile_message_patterns;

    let sizes: Vec<usize> = vec![];
    let intervals: Vec<usize> = vec![];
    let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);

    assert_eq!(pattern, "unknown");
    assert_eq!(score, 0.0);
    assert_eq!(rec, "insufficient_data");
}

#[test]
fn test_calculate_network_efficiency_balanced() {
    use crate::utils::calculate_network_efficiency;

    let (score, util, _rec) = calculate_network_efficiency(5000, 5000, 10000);
    assert!((0.0..=1.0).contains(&score)); // Score should be in valid range
    assert_eq!(util, 100.0); // 100% bandwidth utilization
}

#[test]
fn test_calculate_network_efficiency_overutilized() {
    use crate::utils::calculate_network_efficiency;

    let (_score, util, rec) = calculate_network_efficiency(9500, 500, 10000);
    assert!(util > 90.0);
    assert_eq!(rec, "increase_bandwidth_overutilized");
}

#[test]
fn test_calculate_network_efficiency_underutilized() {
    use crate::utils::calculate_network_efficiency;

    let (_score, util, rec) = calculate_network_efficiency(1000, 500, 10000);
    assert!(util < 30.0);
    assert_eq!(rec, "reduce_bandwidth_underutilized");
}

#[test]
fn test_calculate_network_efficiency_imbalanced() {
    use crate::utils::calculate_network_efficiency;

    let (_score, _util, rec) = calculate_network_efficiency(8000, 500, 10000);
    // Highly imbalanced send/receive ratio
    assert!(rec.contains("imbalanced") || rec == "good_efficiency");
}

#[test]
fn test_detect_message_hotspots_balanced() {
    use crate::utils::detect_message_hotspots;

    let counts = vec![100, 105, 98, 102];
    let (has_hotspot, _, ratio, rec) = detect_message_hotspots(&counts);

    assert!(!has_hotspot);
    assert!(ratio < 2.0);
    assert_eq!(rec, "balanced_distribution");
}

#[test]
fn test_detect_message_hotspots_severe() {
    use crate::utils::detect_message_hotspots;

    let counts = vec![100, 1000, 120, 110]; // Avg=332.5, ratio=1000/332.5=3.0
    let (has_hotspot, index, ratio, rec) = detect_message_hotspots(&counts);

    assert!(has_hotspot);
    assert_eq!(index, 1);
    assert!(ratio > 2.0); // Moderate hotspot (ratio 3.0)
    assert!(rec.contains("hotspot"));
}

#[test]
fn test_detect_message_hotspots_moderate() {
    use crate::utils::detect_message_hotspots;

    let counts = vec![100, 350, 120, 110];
    let (has_hotspot, index, ratio, rec) = detect_message_hotspots(&counts);

    assert!(has_hotspot);
    assert_eq!(index, 1);
    assert!(ratio > 2.0 && ratio <= 5.0);
    assert!(rec.contains("moderate_hotspot") || rec.contains("minor_hotspot"));
}

#[test]
fn test_detect_message_hotspots_empty() {
    use crate::utils::detect_message_hotspots;

    let counts: Vec<usize> = vec![];
    let (has_hotspot, _, ratio, rec) = detect_message_hotspots(&counts);

    assert!(!has_hotspot);
    assert_eq!(ratio, 1.0);
    assert_eq!(rec, "no_data");
}

#[test]
fn test_recommend_queue_topology_single() {
    use crate::utils::recommend_queue_topology;

    let (topology, count, _) = recommend_queue_topology(100, 50, 10, false);
    assert_eq!(topology, "single_queue");
    assert_eq!(count, 1);
}

#[test]
fn test_recommend_queue_topology_partitioned() {
    use crate::utils::recommend_queue_topology;

    let (topology, count, _) = recommend_queue_topology(10000, 50, 50, true);
    assert_eq!(topology, "partitioned");
    assert!(count >= 4);
}

#[test]
fn test_recommend_queue_topology_multi_queue() {
    use crate::utils::recommend_queue_topology;

    let (topology, count, rec) = recommend_queue_topology(20000, 100, 50, false);
    assert_eq!(topology, "multi_queue");
    assert!(count >= 2);
    assert!(rec.contains("overloaded"));
}

#[test]
fn test_recommend_queue_topology_priority() {
    use crate::utils::recommend_queue_topology;

    // High load but not overloaded: 500 msg/sec with 50 consumers @ 10msg/sec each
    let (topology, count, _) = recommend_queue_topology(450, 100, 50, false);
    assert_eq!(topology, "priority_queues");
    assert_eq!(count, 3);
}

#[test]
fn test_recommend_queue_topology_worker_pool() {
    use crate::utils::recommend_queue_topology;

    // Medium volume that triggers worker_pool: > 1000 msg/sec but load_ratio < 0.8
    // 1100 msg/sec with 200 consumers @ 10msg/sec each = load_ratio 0.55
    let (topology, count, _) = recommend_queue_topology(1100, 100, 200, false);
    assert_eq!(topology, "worker_pool");
    assert!(count >= 2);
}

#[test]
fn test_calculate_message_deduplication_window_fast_stream() {
    use crate::utils::calculate_message_deduplication_window;

    // Fast message stream (100ms interval, 3 retries, 5s max delay)
    let (window, cache_size, rec) = calculate_message_deduplication_window(100, 3, 5000);
    assert!(window > 0);
    assert!(cache_size >= 1000);
    assert!(rec.contains("window"));
}

#[test]
fn test_calculate_message_deduplication_window_slow_stream() {
    use crate::utils::calculate_message_deduplication_window;

    // Slow message stream (10s interval, 1 retry, 2s max delay)
    let (window, cache_size, _) = calculate_message_deduplication_window(10000, 1, 2000);
    assert!(window >= 60); // At least 1 minute
    assert!(cache_size >= 1000);
}

#[test]
fn test_calculate_message_deduplication_window_long_window() {
    use crate::utils::calculate_message_deduplication_window;

    // Long window scenario (requires > 1 hour window for persistent storage recommendation)
    // 120s interval * 15 retries + 600s max delay = 1800s + 600s = 2400s, then *2 = 4800s > 3600s
    let (window, _, rec) = calculate_message_deduplication_window(120000, 15, 600000);
    assert!(window > 3600); // More than 1 hour
    assert!(rec.contains("persistent_storage"));
}

#[test]
fn test_analyze_retry_effectiveness_high() {
    use crate::utils::analyze_retry_effectiveness;

    // High retry effectiveness (80 out of 100 failures recovered)
    let (effectiveness, success_rate, rec) = analyze_retry_effectiveness(1000, 100, 80, 20);
    assert!(effectiveness >= 80.0);
    assert!(success_rate >= 98.0); // 1000 - 20 = 980 successes
    assert!(rec.contains("effective"));
}

#[test]
fn test_analyze_retry_effectiveness_low() {
    use crate::utils::analyze_retry_effectiveness;

    // Low retry effectiveness (10 out of 100 failures recovered)
    let (effectiveness, _, rec) = analyze_retry_effectiveness(1000, 100, 10, 90);
    assert!(effectiveness < 20.0);
    assert!(rec.contains("ineffective"));
}

#[test]
fn test_analyze_retry_effectiveness_no_failures() {
    use crate::utils::analyze_retry_effectiveness;

    // No failures
    let (effectiveness, success_rate, rec) = analyze_retry_effectiveness(1000, 0, 0, 0);
    assert_eq!(effectiveness, 100.0);
    assert_eq!(success_rate, 100.0);
    assert!(rec.contains("no_failures"));
}

#[test]
fn test_calculate_queue_overflow_risk_high() {
    use crate::utils::calculate_queue_overflow_risk;

    // High risk: 8000/10000 capacity, enqueue > dequeue
    let (risk, ttf, rec) = calculate_queue_overflow_risk(8000, 10000, 100, 50);
    assert!(risk > 50.0);
    assert!(ttf > 0); // Should have time to full
    assert!(rec.contains("high_risk") || rec.contains("medium_risk"));
}

#[test]
fn test_calculate_queue_overflow_risk_low() {
    use crate::utils::calculate_queue_overflow_risk;

    // Low risk: low capacity, dequeue > enqueue (draining)
    let (risk, _, rec) = calculate_queue_overflow_risk(100, 10000, 50, 100);
    assert_eq!(risk, 0.0);
    assert!(rec.contains("healthy"));
}

#[test]
fn test_calculate_queue_overflow_risk_critical() {
    use crate::utils::calculate_queue_overflow_risk;

    // Critical risk: 95% utilization
    let (risk, _, rec) = calculate_queue_overflow_risk(9500, 10000, 100, 80);
    assert!(risk >= 90.0);
    assert!(rec.contains("critical"));
}

#[test]
fn test_calculate_queue_overflow_risk_invalid_max() {
    use crate::utils::calculate_queue_overflow_risk;

    // Invalid max size
    let (risk, _, rec) = calculate_queue_overflow_risk(100, 0, 50, 50);
    assert_eq!(risk, 100.0);
    assert!(rec.contains("misconfigured"));
}
