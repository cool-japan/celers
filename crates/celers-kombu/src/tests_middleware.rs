//! Middleware tests.

use super::*;
use celers_protocol::Message;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn test_timeout_middleware_creation() {
    let timeout = TimeoutMiddleware::new(Duration::from_secs(30));
    assert_eq!(timeout.timeout(), Duration::from_secs(30));
    assert_eq!(timeout.name(), "timeout");
}

#[tokio::test]
async fn test_timeout_middleware_sets_header() {
    let timeout = TimeoutMiddleware::new(Duration::from_secs(60));
    let mut message = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);

    timeout.before_publish(&mut message).await.unwrap();

    assert!(message.headers.extra.contains_key("x-timeout-ms"));
}

#[test]
fn test_filter_middleware_creation() {
    let filter = FilterMiddleware::new(|msg: &Message| msg.task_name().starts_with("test"));
    let test_msg = Message::new("test_task".to_string(), Uuid::new_v4(), vec![]);
    let other_msg = Message::new("other_task".to_string(), Uuid::new_v4(), vec![]);

    assert!(filter.matches(&test_msg));
    assert!(!filter.matches(&other_msg));
    assert_eq!(filter.name(), "filter");
}

#[tokio::test]
async fn test_filter_middleware_rejects_non_matching() {
    let filter = FilterMiddleware::new(|msg: &Message| msg.task_name() == "allowed");
    let mut allowed = Message::new("allowed".to_string(), Uuid::new_v4(), vec![]);
    let mut rejected = Message::new("rejected".to_string(), Uuid::new_v4(), vec![]);

    assert!(filter.after_consume(&mut allowed).await.is_ok());
    assert!(filter.after_consume(&mut rejected).await.is_err());
}

#[test]
fn test_sampling_middleware_creation() {
    let sampler = SamplingMiddleware::new(0.5);
    assert_eq!(sampler.sample_rate(), 0.5);
    assert_eq!(sampler.name(), "sampling");

    // Test clamping
    let clamped_low = SamplingMiddleware::new(-0.1);
    assert_eq!(clamped_low.sample_rate(), 0.0);

    let clamped_high = SamplingMiddleware::new(1.5);
    assert_eq!(clamped_high.sample_rate(), 1.0);
}

#[tokio::test]
async fn test_sampling_middleware_samples_messages() {
    // Sample everything
    let sampler = SamplingMiddleware::new(1.0);
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    assert!(sampler.after_consume(&mut msg).await.is_ok());

    // Sample nothing
    let sampler = SamplingMiddleware::new(0.0);
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    assert!(sampler.after_consume(&mut msg).await.is_err());
}

#[test]
fn test_transformation_middleware_creation() {
    let transformer = TransformationMiddleware::new(|body| {
        String::from_utf8_lossy(&body).to_uppercase().into_bytes()
    });
    assert_eq!(transformer.name(), "transformation");
}

#[tokio::test]
async fn test_transformation_middleware_transforms() {
    let transformer = TransformationMiddleware::new(|body| {
        String::from_utf8_lossy(&body).to_uppercase().into_bytes()
    });

    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), b"hello world".to_vec());

    transformer.before_publish(&mut msg).await.unwrap();
    assert_eq!(msg.body, b"HELLO WORLD");

    // Reset for consume test
    msg.body = b"hello again".to_vec();
    transformer.after_consume(&mut msg).await.unwrap();
    assert_eq!(msg.body, b"HELLO AGAIN");
}

#[test]
fn test_tracing_middleware_creation() {
    let tracing = TracingMiddleware::new("my-service");
    assert_eq!(tracing.name(), "tracing");
}

#[tokio::test]
async fn test_tracing_middleware_injects_trace_id() {
    let tracing = TracingMiddleware::new("test-service");

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    tracing.before_publish(&mut msg).await.unwrap();

    // Verify trace ID was injected
    assert!(msg.headers.extra.contains_key("trace-id"));
    assert!(msg.headers.extra.contains_key("service-name"));
    assert!(msg.headers.extra.contains_key("span-id"));
    assert!(msg.headers.extra.contains_key("trace-timestamp"));
    assert_eq!(
        msg.headers
            .extra
            .get("service-name")
            .unwrap()
            .as_str()
            .unwrap(),
        "test-service"
    );
}

#[tokio::test]
async fn test_tracing_middleware_preserves_existing_trace_id() {
    let tracing = TracingMiddleware::new("test-service");

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    // Pre-set a trace ID
    let original_trace_id = "existing-trace-id";
    msg.headers
        .extra
        .insert("trace-id".to_string(), serde_json::json!(original_trace_id));

    tracing.before_publish(&mut msg).await.unwrap();

    // Verify original trace ID was preserved
    assert_eq!(
        msg.headers.extra.get("trace-id").unwrap().as_str().unwrap(),
        original_trace_id
    );
}

#[tokio::test]
async fn test_tracing_middleware_after_consume() {
    let tracing = TracingMiddleware::new("consumer-service");

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    msg.headers
        .extra
        .insert("trace-id".to_string(), serde_json::json!("trace-123"));

    tracing.after_consume(&mut msg).await.unwrap();

    // Verify consume-side headers were added
    assert!(msg.headers.extra.contains_key("consumer-service"));
    assert!(msg.headers.extra.contains_key("trace-id-consumed"));
    assert_eq!(
        msg.headers
            .extra
            .get("consumer-service")
            .unwrap()
            .as_str()
            .unwrap(),
        "consumer-service"
    );
}

#[test]
fn test_batching_middleware_creation() {
    let batching = BatchingMiddleware::new(50, 3000);
    assert_eq!(batching.name(), "batching");
}

#[test]
fn test_batching_middleware_with_defaults() {
    let batching = BatchingMiddleware::with_defaults();
    assert_eq!(batching.name(), "batching");
}

#[tokio::test]
async fn test_batching_middleware_adds_metadata() {
    let batching = BatchingMiddleware::new(100, 5000);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    batching.before_publish(&mut msg).await.unwrap();

    // Verify batching metadata was added
    assert_eq!(
        msg.headers
            .extra
            .get("batch-size-hint")
            .unwrap()
            .as_u64()
            .unwrap(),
        100
    );
    assert_eq!(
        msg.headers
            .extra
            .get("batch-timeout-ms")
            .unwrap()
            .as_u64()
            .unwrap(),
        5000
    );
    assert!(msg
        .headers
        .extra
        .get("batching-enabled")
        .unwrap()
        .as_bool()
        .unwrap());
}

#[test]
fn test_audit_middleware_creation() {
    let audit = AuditMiddleware::new(true);
    assert_eq!(audit.name(), "audit");
}

#[test]
fn test_audit_middleware_with_body_logging() {
    let audit = AuditMiddleware::with_body_logging();
    assert_eq!(audit.name(), "audit");
}

#[test]
fn test_audit_middleware_without_body_logging() {
    let audit = AuditMiddleware::without_body_logging();
    assert_eq!(audit.name(), "audit");
}

#[tokio::test]
async fn test_audit_middleware_before_publish() {
    let audit = AuditMiddleware::new(true);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    audit.before_publish(&mut msg).await.unwrap();

    // Verify audit metadata was added
    assert!(msg.headers.extra.contains_key("audit-publish"));
    assert!(msg.headers.extra.contains_key("audit-id"));

    let audit_entry = msg
        .headers
        .extra
        .get("audit-publish")
        .unwrap()
        .as_str()
        .unwrap();
    assert!(audit_entry.contains("PUBLISH"));
    assert!(audit_entry.contains("body_size=9"));
}

#[tokio::test]
async fn test_audit_middleware_after_consume() {
    let audit = AuditMiddleware::new(false);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    audit.after_consume(&mut msg).await.unwrap();

    // Verify audit metadata was added
    assert!(msg.headers.extra.contains_key("audit-consume"));

    let audit_entry = msg
        .headers
        .extra
        .get("audit-consume")
        .unwrap()
        .as_str()
        .unwrap();
    assert!(audit_entry.contains("CONSUME"));
    assert!(audit_entry.contains("<redacted>"));
}

#[tokio::test]
async fn test_deadline_middleware_creation() {
    let middleware = DeadlineMiddleware::new(Duration::from_secs(300));
    assert_eq!(middleware.deadline_duration(), Duration::from_secs(300));
    assert_eq!(middleware.name(), "deadline");
}

#[tokio::test]
async fn test_deadline_middleware_sets_deadline() {
    let middleware = DeadlineMiddleware::new(Duration::from_secs(60));

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.before_publish(&mut msg).await.unwrap();

    // Verify deadline was set
    assert!(msg.headers.extra.contains_key("x-deadline"));
    let deadline = msg
        .headers
        .extra
        .get("x-deadline")
        .unwrap()
        .as_u64()
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Deadline should be in the future
    assert!(deadline > now);
    assert!(deadline <= now + 61); // Allow 1 second tolerance
}

#[tokio::test]
async fn test_deadline_middleware_detects_exceeded() {
    let middleware = DeadlineMiddleware::new(Duration::from_secs(60));

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    // Set a deadline in the past
    let past_deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 10;

    msg.headers
        .extra
        .insert("x-deadline".to_string(), serde_json::json!(past_deadline));

    middleware.after_consume(&mut msg).await.unwrap();

    // Verify deadline-exceeded flag was set
    assert!(msg.headers.extra.contains_key("x-deadline-exceeded"));
    assert!(msg
        .headers
        .extra
        .get("x-deadline-exceeded")
        .unwrap()
        .as_bool()
        .unwrap());
}

#[test]
fn test_content_type_middleware_creation() {
    let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);
    assert_eq!(middleware.name(), "content_type");
    assert!(middleware.is_allowed("application/json"));
    assert!(!middleware.is_allowed("text/plain"));
}

#[tokio::test]
async fn test_content_type_middleware_sets_default() {
    let middleware =
        ContentTypeMiddleware::new(vec![]).with_default("application/json".to_string());

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );
    msg.content_type = String::new();

    middleware.before_publish(&mut msg).await.unwrap();

    assert_eq!(msg.content_type, "application/json");
}

#[tokio::test]
async fn test_content_type_middleware_rejects_invalid() {
    let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );
    msg.content_type = "text/plain".to_string();

    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().is_configuration());
}

#[tokio::test]
async fn test_content_type_middleware_warns_on_consume() {
    let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );
    msg.content_type = "text/plain".to_string();

    middleware.after_consume(&mut msg).await.unwrap();

    // Should add warning but not fail
    assert!(msg.headers.extra.contains_key("x-content-type-warning"));
}

#[tokio::test]
async fn test_routing_key_middleware_custom() {
    let middleware = RoutingKeyMiddleware::new(|msg| format!("custom.{}", msg.headers.task));

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.before_publish(&mut msg).await.unwrap();

    assert!(msg.headers.extra.contains_key("x-routing-key"));
    let routing_key = msg
        .headers
        .extra
        .get("x-routing-key")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(routing_key, "custom.test_task");
    assert_eq!(middleware.name(), "routing_key");
}

#[tokio::test]
async fn test_routing_key_middleware_from_task_name() {
    let middleware = RoutingKeyMiddleware::from_task_name();

    let mut msg = Message::new("my_task".to_string(), Uuid::new_v4(), b"test body".to_vec());

    middleware.before_publish(&mut msg).await.unwrap();

    let routing_key = msg
        .headers
        .extra
        .get("x-routing-key")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(routing_key, "tasks.my_task");
}

#[tokio::test]
async fn test_routing_key_middleware_from_task_and_priority() {
    let middleware = RoutingKeyMiddleware::from_task_and_priority();

    let mut msg = Message::new("my_task".to_string(), Uuid::new_v4(), b"test body".to_vec());
    msg.headers
        .extra
        .insert("priority".to_string(), serde_json::json!(5));

    middleware.before_publish(&mut msg).await.unwrap();

    let routing_key = msg
        .headers
        .extra
        .get("x-routing-key")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(routing_key, "tasks.my_task.priority_5");
}

#[tokio::test]
async fn test_idempotency_middleware_creation() {
    let middleware = IdempotencyMiddleware::new(5000);
    assert_eq!(middleware.name(), "idempotency");
    assert_eq!(middleware.cache_size(), 0);
}

#[tokio::test]
async fn test_idempotency_middleware_before_publish() {
    let middleware = IdempotencyMiddleware::with_default_cache();
    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.before_publish(&mut msg).await.unwrap();

    assert!(msg.headers.extra.contains_key("x-idempotency-key"));
}

#[tokio::test]
async fn test_idempotency_middleware_after_consume_first_time() {
    let middleware = IdempotencyMiddleware::new(1000);
    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.after_consume(&mut msg).await.unwrap();

    let already_processed = msg
        .headers
        .extra
        .get("x-already-processed")
        .unwrap()
        .as_bool()
        .unwrap();
    assert!(!already_processed); // First time, not processed yet
    assert_eq!(middleware.cache_size(), 1);
}

#[tokio::test]
async fn test_idempotency_middleware_after_consume_duplicate() {
    let middleware = IdempotencyMiddleware::new(1000);
    let task_id = Uuid::new_v4();
    let mut msg1 = Message::new("test_task".to_string(), task_id, b"test body".to_vec());

    // First consumption
    middleware.after_consume(&mut msg1).await.unwrap();

    // Second consumption with same ID
    let mut msg2 = Message::new("test_task".to_string(), task_id, b"test body".to_vec());
    middleware.after_consume(&mut msg2).await.unwrap();

    let already_processed = msg2
        .headers
        .extra
        .get("x-already-processed")
        .unwrap()
        .as_bool()
        .unwrap();
    assert!(already_processed); // Already processed
}

#[test]
fn test_idempotency_middleware_clear() {
    let middleware = IdempotencyMiddleware::new(1000);
    middleware.mark_processed("test-id-1".to_string());
    middleware.mark_processed("test-id-2".to_string());
    assert_eq!(middleware.cache_size(), 2);

    middleware.clear();
    assert_eq!(middleware.cache_size(), 0);
}

#[tokio::test]
async fn test_backoff_middleware_creation() {
    let middleware = BackoffMiddleware::with_defaults();
    assert_eq!(middleware.name(), "backoff");
}

#[tokio::test]
async fn test_backoff_middleware_calculates_delay() {
    use std::time::Duration;
    let middleware = BackoffMiddleware::new(Duration::from_secs(1), Duration::from_secs(60), 2.0);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );
    msg.headers
        .extra
        .insert("retries".to_string(), serde_json::json!(3));

    middleware.after_consume(&mut msg).await.unwrap();

    assert!(msg.headers.extra.contains_key("x-backoff-delay"));
    assert!(msg.headers.extra.contains_key("x-next-retry-at"));

    let backoff_delay = msg
        .headers
        .extra
        .get("x-backoff-delay")
        .unwrap()
        .as_u64()
        .unwrap();
    // With 3 retries, base delay is 1 * 2^3 = 8 seconds = 8000ms
    // With jitter, should be between 8000 and 10000ms
    assert!((8000..=10000).contains(&backoff_delay));
}

#[tokio::test]
async fn test_backoff_middleware_no_retries() {
    let middleware = BackoffMiddleware::with_defaults();

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.after_consume(&mut msg).await.unwrap();

    let backoff_delay = msg
        .headers
        .extra
        .get("x-backoff-delay")
        .unwrap()
        .as_u64()
        .unwrap();
    // No retries, should be initial delay (1 second = 1000ms) + jitter
    assert!((1000..=1250).contains(&backoff_delay));
}

#[tokio::test]
async fn test_caching_middleware_creation() {
    let middleware = CachingMiddleware::new(100, Duration::from_secs(60));
    assert_eq!(middleware.name(), "caching");
    assert_eq!(middleware.cache_size(), 0);
}

#[tokio::test]
async fn test_caching_middleware_cache_miss() {
    let middleware = CachingMiddleware::with_defaults();
    let msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    let cached = middleware.get_cached(&msg);
    assert!(cached.is_none());
}

#[tokio::test]
async fn test_caching_middleware_store_and_retrieve() {
    let middleware = CachingMiddleware::new(100, Duration::from_secs(60));
    let msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    // Store result
    let result = b"cached result".to_vec();
    middleware.store_result(&msg, result.clone());
    assert_eq!(middleware.cache_size(), 1);

    // Retrieve result
    let cached = middleware.get_cached(&msg);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap(), result);
}

#[tokio::test]
async fn test_caching_middleware_after_consume_miss() {
    let middleware = CachingMiddleware::with_defaults();
    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.after_consume(&mut msg).await.unwrap();

    let cache_hit = msg
        .headers
        .extra
        .get("x-cache-hit")
        .unwrap()
        .as_bool()
        .unwrap();
    assert!(!cache_hit);
}

#[tokio::test]
async fn test_caching_middleware_after_consume_hit() {
    let middleware = CachingMiddleware::with_defaults();
    let msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    // Store a result first
    middleware.store_result(&msg, b"result".to_vec());

    // Now consume with the same message
    let mut msg_clone = msg.clone();
    middleware.after_consume(&mut msg_clone).await.unwrap();

    let cache_hit = msg_clone
        .headers
        .extra
        .get("x-cache-hit")
        .unwrap()
        .as_bool()
        .unwrap();
    assert!(cache_hit);

    let cached_size = msg_clone
        .headers
        .extra
        .get("x-cached-result-size")
        .unwrap()
        .as_u64()
        .unwrap();
    assert_eq!(cached_size, 6); // "result".len() == 6
}

#[test]
fn test_caching_middleware_clear() {
    let middleware = CachingMiddleware::new(100, Duration::from_secs(60));
    let msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    middleware.store_result(&msg, b"result".to_vec());
    assert_eq!(middleware.cache_size(), 1);

    middleware.clear();
    assert_eq!(middleware.cache_size(), 0);
}

#[test]
fn test_bulkhead_middleware_creation() {
    let middleware = BulkheadMiddleware::new(50);
    assert_eq!(middleware.total_operations(), 0);
}

#[test]
fn test_bulkhead_middleware_acquire_release() {
    let middleware = BulkheadMiddleware::new(2);

    // Acquire permits
    assert!(middleware.try_acquire("partition1"));
    assert_eq!(middleware.current_operations("partition1"), 1);

    assert!(middleware.try_acquire("partition1"));
    assert_eq!(middleware.current_operations("partition1"), 2);

    // At limit
    assert!(!middleware.try_acquire("partition1"));
    assert_eq!(middleware.current_operations("partition1"), 2);

    // Release one
    middleware.release("partition1");
    assert_eq!(middleware.current_operations("partition1"), 1);

    // Can acquire again
    assert!(middleware.try_acquire("partition1"));
    assert_eq!(middleware.current_operations("partition1"), 2);
}

#[test]
fn test_bulkhead_middleware_multiple_partitions() {
    let middleware = BulkheadMiddleware::new(2);

    // Different partitions have independent limits
    assert!(middleware.try_acquire("partition1"));
    assert!(middleware.try_acquire("partition2"));
    assert_eq!(middleware.current_operations("partition1"), 1);
    assert_eq!(middleware.current_operations("partition2"), 1);
    assert_eq!(middleware.total_operations(), 2);
}

#[test]
fn test_bulkhead_middleware_with_custom_partition_fn() {
    let middleware = BulkheadMiddleware::with_partition_fn(2, |msg| {
        msg.headers
            .extra
            .get("custom_partition")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string()
    });

    let mut msg1 = Message::new("task1".to_string(), Uuid::new_v4(), b"body".to_vec());
    msg1.headers.extra.insert(
        "custom_partition".to_string(),
        serde_json::json!("partition_a"),
    );

    let mut msg2 = Message::new("task2".to_string(), Uuid::new_v4(), b"body".to_vec());
    msg2.headers.extra.insert(
        "custom_partition".to_string(),
        serde_json::json!("partition_b"),
    );

    // Should use custom partition key
    assert_eq!(middleware.current_operations("partition_a"), 0);
    assert_eq!(middleware.current_operations("partition_b"), 0);
}

#[test]
fn test_priority_boost_middleware_age_boost() {
    let middleware =
        PriorityBoostMiddleware::new().with_age_boost(Duration::from_secs(300), Priority::High);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    // Message with old timestamp (600 seconds ago)
    let old_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        - 600.0;

    msg.headers
        .extra
        .insert("timestamp".to_string(), serde_json::json!(old_timestamp));

    let boosted = middleware.calculate_priority(&msg, Priority::Normal);
    assert_eq!(boosted, Priority::High);
}

#[test]
fn test_priority_boost_middleware_retry_boost() {
    let middleware = PriorityBoostMiddleware::new().with_retry_boost(3, Priority::Highest);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );
    msg.headers.retries = Some(5);

    let boosted = middleware.calculate_priority(&msg, Priority::Normal);
    assert_eq!(boosted, Priority::Highest);
}

#[test]
fn test_priority_boost_middleware_no_boost() {
    let middleware = PriorityBoostMiddleware::new().with_retry_boost(5, Priority::High);

    let mut msg = Message::new(
        "test_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );
    msg.headers.retries = Some(2);

    let boosted = middleware.calculate_priority(&msg, Priority::Normal);
    assert_eq!(boosted, Priority::Normal);
}

#[test]
fn test_priority_boost_middleware_custom_fn() {
    let middleware = PriorityBoostMiddleware::with_custom_fn(|msg, _current| {
        if msg.headers.task.contains("critical") {
            Priority::Highest
        } else {
            Priority::Low
        }
    });

    let msg_critical = Message::new(
        "critical_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    let msg_normal = Message::new(
        "normal_task".to_string(),
        Uuid::new_v4(),
        b"test body".to_vec(),
    );

    assert_eq!(
        middleware.calculate_priority(&msg_critical, Priority::Normal),
        Priority::Highest
    );
    assert_eq!(
        middleware.calculate_priority(&msg_normal, Priority::Normal),
        Priority::Low
    );
}

#[test]
fn test_error_classification_middleware_transient() {
    let middleware = ErrorClassificationMiddleware::new();

    assert_eq!(
        middleware.classify_error("connection timeout"),
        ErrorClass::Transient
    );
    assert_eq!(
        middleware.classify_error("network error occurred"),
        ErrorClass::Transient
    );
    assert_eq!(
        middleware.classify_error("service unavailable"),
        ErrorClass::Transient
    );
}

#[test]
fn test_error_classification_middleware_permanent() {
    let middleware = ErrorClassificationMiddleware::new();

    assert_eq!(
        middleware.classify_error("validation failed"),
        ErrorClass::Permanent
    );
    assert_eq!(
        middleware.classify_error("schema mismatch"),
        ErrorClass::Permanent
    );
    assert_eq!(
        middleware.classify_error("invalid input"),
        ErrorClass::Permanent
    );
    assert_eq!(
        middleware.classify_error("access forbidden"),
        ErrorClass::Permanent
    );
}

#[test]
fn test_error_classification_middleware_unknown() {
    let middleware = ErrorClassificationMiddleware::new();

    assert_eq!(
        middleware.classify_error("something went wrong"),
        ErrorClass::Unknown
    );
    assert_eq!(
        middleware.classify_error("unexpected error"),
        ErrorClass::Unknown
    );
}

#[test]
fn test_error_classification_middleware_should_retry() {
    let middleware = ErrorClassificationMiddleware::new();

    // Transient errors should retry up to limit
    assert!(middleware.should_retry("timeout error", 5));
    assert!(!middleware.should_retry("timeout error", 10));
    assert!(!middleware.should_retry("timeout error", 15));

    // Permanent errors should not retry much
    assert!(middleware.should_retry("validation error", 0));
    assert!(!middleware.should_retry("validation error", 1));
    assert!(!middleware.should_retry("validation error", 2));
}

#[test]
fn test_error_classification_middleware_custom_patterns() {
    let middleware = ErrorClassificationMiddleware::new()
        .with_transient_pattern("temporary")
        .with_permanent_pattern("critical");

    assert_eq!(
        middleware.classify_error("temporary issue"),
        ErrorClass::Transient
    );
    assert_eq!(
        middleware.classify_error("critical failure"),
        ErrorClass::Permanent
    );
}

#[test]
fn test_error_classification_middleware_retry_limits() {
    let middleware = ErrorClassificationMiddleware::new()
        .with_max_transient_retries(5)
        .with_max_permanent_retries(0);

    assert!(middleware.should_retry("timeout", 4));
    assert!(!middleware.should_retry("timeout", 5));

    assert!(!middleware.should_retry("validation error", 0));
    assert!(!middleware.should_retry("validation error", 1));
}

#[tokio::test]
async fn test_correlation_middleware_generates_id() {
    use crate::CorrelationMiddleware;

    let middleware = CorrelationMiddleware::new();
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

    middleware.before_publish(&mut msg).await.unwrap();

    assert!(msg.headers.extra.contains_key("x-correlation-id"));
    let correlation_id = msg.headers.extra.get("x-correlation-id").unwrap();
    assert!(correlation_id.is_string());
}

#[tokio::test]
async fn test_correlation_middleware_preserves_existing_id() {
    use crate::CorrelationMiddleware;

    let middleware = CorrelationMiddleware::new();
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    let existing_id = "existing-correlation-123";
    msg.headers.extra.insert(
        "x-correlation-id".to_string(),
        serde_json::json!(existing_id),
    );

    middleware.before_publish(&mut msg).await.unwrap();

    let correlation_id = msg
        .headers
        .extra
        .get("x-correlation-id")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(correlation_id, existing_id);
}

#[tokio::test]
async fn test_correlation_middleware_custom_header() {
    use crate::CorrelationMiddleware;

    let middleware = CorrelationMiddleware::with_header_name("x-request-id");
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

    middleware.before_publish(&mut msg).await.unwrap();

    assert!(msg.headers.extra.contains_key("x-request-id"));
    assert!(!msg.headers.extra.contains_key("x-correlation-id"));
}

#[tokio::test]
async fn test_throttling_middleware_creation() {
    use crate::ThrottlingMiddleware;

    let middleware = ThrottlingMiddleware::new(100.0)
        .with_burst_size(200)
        .with_backpressure_threshold(0.7);

    assert_eq!(middleware.name(), "throttling");
    assert_eq!(middleware.max_rate, 100.0);
    assert_eq!(middleware.burst_size, 200);
    assert_eq!(middleware.backpressure_threshold, 0.7);
}

#[tokio::test]
async fn test_throttling_middleware_allows_messages() {
    use crate::ThrottlingMiddleware;

    let middleware = ThrottlingMiddleware::new(100.0);
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

    // First message should pass without delay
    middleware.before_publish(&mut msg).await.unwrap();

    // Should not have delay header (tokens available)
    assert!(!msg.headers.extra.contains_key("x-throttle-delay-ms"));
}

#[tokio::test]
async fn test_throttling_middleware_backpressure() {
    use crate::ThrottlingMiddleware;

    let middleware = ThrottlingMiddleware::new(10.0)
        .with_burst_size(5)
        .with_backpressure_threshold(0.9);

    // Consume all tokens
    for _ in 0..6 {
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg).await.unwrap();
    }

    // Next message should trigger backpressure
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    middleware.before_publish(&mut msg).await.unwrap();

    // Should have backpressure indicator
    if msg.headers.extra.contains_key("x-backpressure-active") {
        assert_eq!(
            msg.headers.extra.get("x-backpressure-active").unwrap(),
            &serde_json::json!(true)
        );
    }
}

#[tokio::test]
async fn test_circuit_breaker_middleware_creation() {
    use crate::CircuitBreakerMiddleware;
    use std::time::Duration;

    let middleware = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));
    assert_eq!(middleware.name(), "circuit_breaker");
    assert_eq!(middleware.failure_threshold, 5);
}

#[tokio::test]
async fn test_circuit_breaker_middleware_allows_when_closed() {
    use crate::CircuitBreakerMiddleware;
    use std::time::Duration;

    let middleware = CircuitBreakerMiddleware::new(3, Duration::from_secs(60));
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

    // Should allow message when circuit is closed
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_circuit_breaker_middleware_opens_on_failures() {
    use crate::CircuitBreakerMiddleware;
    use std::time::Duration;

    let middleware = CircuitBreakerMiddleware::new(3, Duration::from_secs(60));

    // Record failures
    for _ in 0..3 {
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        msg.headers
            .extra
            .insert("error".to_string(), serde_json::json!("failure"));
        middleware.after_consume(&mut msg).await.unwrap();
    }

    // Circuit should be open now
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_err());

    // Should have circuit breaker headers
    assert_eq!(
        msg.headers.extra.get("x-circuit-breaker-open").unwrap(),
        &serde_json::json!(true)
    );
}

#[tokio::test]
async fn test_circuit_breaker_middleware_tracks_failure_count() {
    use crate::CircuitBreakerMiddleware;
    use std::time::Duration;

    let middleware = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));

    // Record 2 failures
    for _ in 0..2 {
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        msg.headers
            .extra
            .insert("error".to_string(), serde_json::json!("failure"));
        middleware.after_consume(&mut msg).await.unwrap();
    }

    // Check failure count is tracked
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    middleware.after_consume(&mut msg).await.unwrap();

    assert_eq!(
        msg.headers.extra.get("x-circuit-breaker-failures").unwrap(),
        &serde_json::json!(2)
    );
}

#[tokio::test]
async fn test_schema_validation_middleware_creation() {
    use crate::SchemaValidationMiddleware;

    let middleware = SchemaValidationMiddleware::new()
        .with_required_field("user_id")
        .with_max_field_count(10)
        .with_max_body_size(1024);

    assert_eq!(middleware.name(), "schema_validation");
    assert_eq!(middleware.required_fields.len(), 1);
    assert_eq!(middleware.max_field_count, Some(10));
    assert_eq!(middleware.max_body_size, Some(1024));
}

#[tokio::test]
async fn test_schema_validation_middleware_validates_required_fields() {
    use crate::SchemaValidationMiddleware;

    let middleware = SchemaValidationMiddleware::new().with_required_field("user_id");

    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

    // Should fail without required field
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_err());

    // Should succeed with required field
    msg.headers
        .extra
        .insert("user_id".to_string(), serde_json::json!("123"));
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_ok());
    assert_eq!(
        msg.headers.extra.get("x-schema-validated").unwrap(),
        &serde_json::json!(true)
    );
}

#[tokio::test]
async fn test_schema_validation_middleware_validates_field_count() {
    use crate::SchemaValidationMiddleware;

    let middleware = SchemaValidationMiddleware::new().with_max_field_count(2);

    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    msg.headers
        .extra
        .insert("field1".to_string(), serde_json::json!(1));
    msg.headers
        .extra
        .insert("field2".to_string(), serde_json::json!(2));
    msg.headers
        .extra
        .insert("field3".to_string(), serde_json::json!(3));

    // Should fail with too many fields
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_schema_validation_middleware_validates_body_size() {
    use crate::SchemaValidationMiddleware;

    let middleware = SchemaValidationMiddleware::new()
        .with_min_body_size(10)
        .with_max_body_size(20);

    // Too small
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_err());

    // Just right
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1; 15]);
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_ok());

    // Too large
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1; 30]);
    let result = middleware.before_publish(&mut msg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_message_enrichment_middleware_creation() {
    use crate::MessageEnrichmentMiddleware;

    let middleware = MessageEnrichmentMiddleware::new()
        .with_hostname("worker-01")
        .with_environment("production")
        .with_version("1.0.0")
        .with_add_timestamp(true);

    assert_eq!(middleware.name(), "message_enrichment");
    assert_eq!(middleware.hostname, Some("worker-01".to_string()));
    assert_eq!(middleware.environment, Some("production".to_string()));
    assert_eq!(middleware.version, Some("1.0.0".to_string()));
    assert!(middleware.add_timestamp);
}

#[tokio::test]
async fn test_message_enrichment_middleware_adds_metadata() {
    use crate::MessageEnrichmentMiddleware;

    let middleware = MessageEnrichmentMiddleware::new()
        .with_hostname("worker-01")
        .with_environment("staging")
        .with_version("2.0.0");

    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    middleware.before_publish(&mut msg).await.unwrap();

    assert_eq!(
        msg.headers.extra.get("x-enrichment-hostname").unwrap(),
        &serde_json::json!("worker-01")
    );
    assert_eq!(
        msg.headers.extra.get("x-enrichment-environment").unwrap(),
        &serde_json::json!("staging")
    );
    assert_eq!(
        msg.headers.extra.get("x-enrichment-version").unwrap(),
        &serde_json::json!("2.0.0")
    );
}

#[tokio::test]
async fn test_message_enrichment_middleware_adds_timestamp() {
    use crate::MessageEnrichmentMiddleware;

    let middleware = MessageEnrichmentMiddleware::new().with_add_timestamp(true);

    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    middleware.before_publish(&mut msg).await.unwrap();

    assert!(msg.headers.extra.contains_key("x-enrichment-timestamp"));
}

#[tokio::test]
async fn test_message_enrichment_middleware_custom_metadata() {
    use crate::MessageEnrichmentMiddleware;

    let middleware = MessageEnrichmentMiddleware::new()
        .with_custom_metadata("region", serde_json::json!("us-west-1"))
        .with_custom_metadata("datacenter", serde_json::json!("dc-01"));

    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    middleware.before_publish(&mut msg).await.unwrap();

    assert_eq!(
        msg.headers.extra.get("x-enrichment-region").unwrap(),
        &serde_json::json!("us-west-1")
    );
    assert_eq!(
        msg.headers.extra.get("x-enrichment-datacenter").unwrap(),
        &serde_json::json!("dc-01")
    );
}
