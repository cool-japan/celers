use celers_kombu::*;
use celers_protocol::Message;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use uuid::Uuid;

// Mock broker for benchmarking (same as in tests but simplified)
#[allow(dead_code)]
struct BenchBroker {
    connected: bool,
}

#[allow(dead_code)]
impl BenchBroker {
    fn new() -> Self {
        Self { connected: false }
    }
}

#[async_trait::async_trait]
impl Transport for BenchBroker {
    async fn connect(&mut self) -> Result<()> {
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn name(&self) -> &str {
        "bench-broker"
    }
}

#[async_trait::async_trait]
impl Producer for BenchBroker {
    async fn publish(&mut self, _queue: &str, _message: Message) -> Result<()> {
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        _routing_key: &str,
        _message: Message,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Consumer for BenchBroker {
    async fn consume(&mut self, _queue: &str, _timeout: Duration) -> Result<Option<Envelope>> {
        Ok(None)
    }

    async fn ack(&mut self, _delivery_tag: &str) -> Result<()> {
        Ok(())
    }

    async fn reject(&mut self, _delivery_tag: &str, _requeue: bool) -> Result<()> {
        Ok(())
    }

    async fn queue_size(&mut self, _queue: &str) -> Result<usize> {
        Ok(0)
    }
}

#[async_trait::async_trait]
impl Broker for BenchBroker {
    async fn purge(&mut self, _queue: &str) -> Result<usize> {
        Ok(0)
    }

    async fn create_queue(&mut self, _queue: &str, _mode: QueueMode) -> Result<()> {
        Ok(())
    }

    async fn delete_queue(&mut self, _queue: &str) -> Result<()> {
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

fn bench_message_creation(c: &mut Criterion) {
    c.bench_function("message_creation", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let message = Message::new(
                black_box("test_task".to_string()),
                black_box(task_id),
                black_box(vec![1, 2, 3, 4, 5]),
            );
            black_box(message)
        })
    });
}

fn bench_envelope_creation(c: &mut Criterion) {
    c.bench_function("envelope_creation", |b| {
        let task_id = Uuid::new_v4();
        let message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
        b.iter(|| {
            let envelope = Envelope::new(
                black_box(message.clone()),
                black_box("delivery-tag-123".to_string()),
            );
            black_box(envelope)
        })
    });
}

fn bench_queue_config_builder(c: &mut Criterion) {
    c.bench_function("queue_config_builder", |b| {
        b.iter(|| {
            let config = QueueConfig::new(black_box("test_queue".to_string()))
                .with_mode(black_box(QueueMode::Priority))
                .with_ttl(black_box(Duration::from_secs(3600)))
                .with_durable(black_box(true))
                .with_max_message_size(black_box(1024 * 1024));
            black_box(config)
        })
    });
}

fn bench_retry_policy_delay(c: &mut Criterion) {
    let policy = RetryPolicy::new()
        .with_max_retries(10)
        .with_backoff_multiplier(2.0);

    c.bench_function("retry_policy_delay", |b| {
        b.iter(|| {
            for attempt in 0..10 {
                let delay = policy.delay_for_attempt(black_box(attempt));
                black_box(delay);
            }
        })
    });
}

fn bench_middleware_chain(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("middleware_chain_3_middlewares", |b| {
        b.to_async(&runtime).iter(|| async {
            let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::default()));
            let chain = MiddlewareChain::new()
                .with_middleware(Box::new(ValidationMiddleware::new()))
                .with_middleware(Box::new(LoggingMiddleware::new("[BENCH]")))
                .with_middleware(Box::new(MetricsMiddleware::new(metrics)));

            let task_id = Uuid::new_v4();
            let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

            let _ = chain.process_before_publish(&mut message).await;
            black_box(message)
        })
    });
}

fn bench_message_options(c: &mut Criterion) {
    c.bench_function("message_options_creation", |b| {
        b.iter(|| {
            let opts = MessageOptions::new()
                .with_priority(black_box(Priority::High))
                .with_ttl(black_box(Duration::from_secs(300)))
                .with_correlation_id(black_box("corr-123".to_string()));
            black_box(opts)
        })
    });
}

fn bench_dlq_config(c: &mut Criterion) {
    c.bench_function("dlq_config_creation", |b| {
        b.iter(|| {
            let config = DlqConfig::new(black_box("failed_tasks".to_string()))
                .with_max_retries(black_box(3))
                .with_ttl(black_box(Duration::from_secs(86400)));
            black_box(config)
        })
    });
}

fn bench_batch_publish_result(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_publish_result");

    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let result = BatchPublishResult::success(black_box(size));
                assert!(result.is_complete_success());
                black_box(result)
            })
        });
    }
    group.finish();
}

fn bench_broker_metrics(c: &mut Criterion) {
    c.bench_function("broker_metrics_increment", |b| {
        b.iter(|| {
            let mut metrics = BrokerMetrics::default();
            metrics.messages_published += black_box(1);
            metrics.messages_consumed += black_box(1);
            metrics.messages_acknowledged += black_box(1);
            black_box(metrics)
        })
    });
}

fn bench_new_middleware(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("tracing_middleware", |b| {
        b.to_async(&runtime).iter(|| async {
            let middleware = TracingMiddleware::new("bench-service");
            let task_id = Uuid::new_v4();
            let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
            let _ = middleware.before_publish(&mut message).await;
            black_box(message)
        })
    });

    c.bench_function("batching_middleware", |b| {
        b.to_async(&runtime).iter(|| async {
            let middleware = BatchingMiddleware::new(100, 5000);
            let task_id = Uuid::new_v4();
            let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
            let _ = middleware.before_publish(&mut message).await;
            black_box(message)
        })
    });

    c.bench_function("audit_middleware", |b| {
        b.to_async(&runtime).iter(|| async {
            let middleware = AuditMiddleware::new(true);
            let task_id = Uuid::new_v4();
            let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
            let _ = middleware.before_publish(&mut message).await;
            black_box(message)
        })
    });
}

fn bench_utility_functions(c: &mut Criterion) {
    c.bench_function("calculate_optimal_batch_size", |b| {
        b.iter(|| {
            let size = utils::calculate_optimal_batch_size(
                black_box(1024),
                black_box(1000),
                black_box(100),
            );
            black_box(size)
        })
    });

    c.bench_function("calculate_queue_capacity", |b| {
        b.iter(|| {
            let capacity =
                utils::calculate_queue_capacity(black_box(1000), black_box(800), black_box(60));
            black_box(capacity)
        })
    });

    c.bench_function("suggest_partition_count", |b| {
        b.iter(|| {
            let partitions =
                utils::suggest_partition_count(black_box(10000), black_box(5), black_box(1000));
            black_box(partitions)
        })
    });

    c.bench_function("analyze_message_patterns", |b| {
        let sizes = vec![100, 200, 150, 180, 220, 190, 210, 170, 195, 185];
        b.iter(|| {
            let result = utils::analyze_message_patterns(black_box(&sizes));
            black_box(result)
        })
    });

    c.bench_function("match_routing_pattern", |b| {
        b.iter(|| {
            let matched = utils::match_routing_pattern(
                black_box("stock.usd.nyse"),
                black_box("stock.*.nyse"),
            );
            black_box(matched)
        })
    });

    c.bench_function("suggest_retry_policy", |b| {
        b.iter(|| {
            let policy = utils::suggest_retry_policy(black_box(0.1), black_box(5));
            black_box(policy)
        })
    });

    c.bench_function("calculate_optimal_workers", |b| {
        b.iter(|| {
            let workers = utils::calculate_optimal_workers(
                black_box(5000),
                black_box(200),
                black_box(60),
                black_box(16),
            );
            black_box(workers)
        })
    });

    c.bench_function("analyze_broker_performance", |b| {
        let metrics = BrokerMetrics {
            messages_published: 10000,
            messages_consumed: 9500,
            messages_acknowledged: 9000,
            messages_rejected: 500,
            publish_errors: 50,
            consume_errors: 100,
            active_connections: 5,
            connection_attempts: 10,
            connection_failures: 1,
        };
        b.iter(|| {
            let perf = utils::analyze_broker_performance(black_box(&metrics));
            black_box(perf)
        })
    });

    c.bench_function("calculate_throughput", |b| {
        b.iter(|| {
            let throughput = utils::calculate_throughput(black_box(9500), black_box(60.0));
            black_box(throughput)
        })
    });

    c.bench_function("analyze_queue_health", |b| {
        b.iter(|| {
            let health =
                utils::analyze_queue_health(black_box(1500), black_box(1000), black_box(5000));
            black_box(health)
        })
    });

    c.bench_function("estimate_drain_time", |b| {
        b.iter(|| {
            let time = utils::estimate_drain_time(black_box(1500), black_box(100));
            black_box(time)
        })
    });

    c.bench_function("calculate_load_distribution", |b| {
        let queue_sizes = vec![100, 500, 200, 800, 300];
        b.iter(|| {
            let dist = utils::calculate_load_distribution(black_box(&queue_sizes), black_box(10));
            black_box(dist)
        })
    });

    c.bench_function("analyze_circuit_breaker", |b| {
        b.iter(|| {
            let result =
                utils::analyze_circuit_breaker(black_box(50), black_box(950), black_box(1000));
            black_box(result)
        })
    });

    c.bench_function("analyze_pool_health", |b| {
        b.iter(|| {
            let result = utils::analyze_pool_health(
                black_box(15),
                black_box(5),
                black_box(20),
                black_box(10000),
                black_box(5),
            );
            black_box(result)
        })
    });

    c.bench_function("calculate_backoff_delay", |b| {
        b.iter(|| {
            let delay = utils::calculate_backoff_delay(
                black_box(5),
                black_box(100),
                black_box(60000),
                black_box(0.1),
            );
            black_box(delay)
        })
    });

    c.bench_function("calculate_priority_score", |b| {
        b.iter(|| {
            let score = utils::calculate_priority_score(
                black_box(Priority::High),
                black_box(30),
                black_box(2),
            );
            black_box(score)
        })
    });

    c.bench_function("identify_stale_messages", |b| {
        let ages = vec![30, 150, 45, 200, 10, 180, 25];
        b.iter(|| {
            let stale = utils::identify_stale_messages(black_box(&ages), black_box(100));
            black_box(stale)
        })
    });

    c.bench_function("suggest_batch_groups", |b| {
        let sizes = vec![1024, 2048, 512, 4096, 1536, 3072, 768];
        b.iter(|| {
            let groups = utils::suggest_batch_groups(black_box(&sizes), black_box(8192));
            black_box(groups)
        })
    });

    c.bench_function("predict_throughput", |b| {
        let historical = vec![100.0, 150.0, 180.0, 220.0, 250.0];
        b.iter(|| {
            let predicted = utils::predict_throughput(black_box(&historical), black_box(5));
            black_box(predicted)
        })
    });

    c.bench_function("calculate_efficiency", |b| {
        b.iter(|| {
            let eff = utils::calculate_efficiency(black_box(9000), black_box(500), black_box(500));
            black_box(eff)
        })
    });

    c.bench_function("calculate_health_score", |b| {
        b.iter(|| {
            let score = utils::calculate_health_score(
                black_box(0.95),
                black_box(1500),
                black_box(0.05),
                black_box(25),
            );
            black_box(score)
        })
    });
}

fn bench_config_builders(c: &mut Criterion) {
    c.bench_function("backpressure_config", |b| {
        b.iter(|| {
            let config = BackpressureConfig::new()
                .with_max_pending(black_box(1000))
                .with_max_queue_size(black_box(10000))
                .with_high_watermark(black_box(0.8))
                .with_low_watermark(black_box(0.6));
            black_box(config)
        })
    });

    c.bench_function("circuit_breaker_config", |b| {
        b.iter(|| {
            let config = CircuitBreakerConfig::new()
                .with_failure_threshold(black_box(5))
                .with_success_threshold(black_box(2))
                .with_open_duration(black_box(Duration::from_secs(60)));
            black_box(config)
        })
    });

    c.bench_function("pool_config", |b| {
        b.iter(|| {
            let config = PoolConfig::new()
                .with_min_connections(black_box(2))
                .with_max_connections(black_box(10))
                .with_idle_timeout(black_box(Duration::from_secs(300)));
            black_box(config)
        })
    });
}

fn bench_monitoring_utilities(c: &mut Criterion) {
    c.bench_function("analyze_consumer_lag", |b| {
        b.iter(|| {
            let (lag, falling_behind, action) =
                utils::analyze_consumer_lag(black_box(1000), black_box(50), black_box(100));
            black_box((lag, falling_behind, action))
        })
    });

    c.bench_function("calculate_message_velocity", |b| {
        b.iter(|| {
            let (velocity, trend) =
                utils::calculate_message_velocity(black_box(200), black_box(100), black_box(10));
            black_box((velocity, trend))
        })
    });

    c.bench_function("suggest_worker_scaling", |b| {
        b.iter(|| {
            let (workers, action) = utils::suggest_worker_scaling(
                black_box(5000),
                black_box(5),
                black_box(100),
                black_box(60),
            );
            black_box((workers, action))
        })
    });

    c.bench_function("calculate_message_age_distribution", |b| {
        let ages: Vec<u64> = (1..=100).collect();
        b.iter(|| {
            let (p50, p95, p99, max) = utils::calculate_message_age_distribution(black_box(&ages));
            black_box((p50, p95, p99, max))
        })
    });

    c.bench_function("estimate_processing_capacity", |b| {
        b.iter(|| {
            let (per_sec, per_min, per_hour) =
                utils::estimate_processing_capacity(black_box(10), black_box(100), black_box(4));
            black_box((per_sec, per_min, per_hour))
        })
    });
}

fn bench_new_middleware_types(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("deadline_middleware_before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = DeadlineMiddleware::new(Duration::from_secs(300));
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("content_type_middleware_before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = ContentTypeMiddleware::new(vec![]);
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("routing_key_middleware_before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = RoutingKeyMiddleware::from_task_name();
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });
}

fn bench_v047_middleware(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("idempotency_middleware_after_consume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = IdempotencyMiddleware::new(10000);
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.after_consume(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("backoff_middleware_after_consume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = BackoffMiddleware::with_defaults();
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                msg.headers
                    .extra
                    .insert("retries".to_string(), serde_json::json!(3));
                middleware.after_consume(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("caching_middleware_after_consume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = CachingMiddleware::with_defaults();
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.after_consume(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });
}

fn bench_v047_utilities(c: &mut Criterion) {
    c.bench_function("detect_anomalies", |b| {
        b.iter(|| {
            let current = vec![100, 105, 98, 102, 500];
            let baseline = vec![100, 105, 98, 102, 100];
            black_box(utils::detect_anomalies(&current, &baseline, 2.0))
        })
    });

    c.bench_function("calculate_sla_compliance", |b| {
        b.iter(|| {
            let processing_times = vec![100, 150, 200, 120, 90, 180, 110, 130];
            black_box(utils::calculate_sla_compliance(&processing_times, 180))
        })
    });

    c.bench_function("estimate_infrastructure_cost", |b| {
        b.iter(|| black_box(utils::estimate_infrastructure_cost(1_000_000, 0.50, 30)))
    });

    c.bench_function("calculate_error_budget", |b| {
        b.iter(|| black_box(utils::calculate_error_budget(99.9, 100_000, 50, 10_000)))
    });

    c.bench_function("predict_queue_saturation", |b| {
        b.iter(|| {
            let samples = vec![1000, 1100, 1200, 1300, 1400];
            black_box(utils::predict_queue_saturation(&samples, 2000, 1.0))
        })
    });

    c.bench_function("calculate_consumer_efficiency", |b| {
        b.iter(|| black_box(utils::calculate_consumer_efficiency(8000, 2000, 100)))
    });

    c.bench_function("suggest_connection_pool_size", |b| {
        b.iter(|| black_box(utils::suggest_connection_pool_size(50, 20, 100)))
    });

    c.bench_function("calculate_message_processing_trend", |b| {
        b.iter(|| {
            let times = vec![100, 95, 90, 85, 80, 75, 70];
            black_box(utils::calculate_message_processing_trend(&times))
        })
    });

    c.bench_function("suggest_prefetch_count", |b| {
        b.iter(|| black_box(utils::suggest_prefetch_count(100, 10, 200)))
    });

    c.bench_function("analyze_dead_letter_queue", |b| {
        b.iter(|| black_box(utils::analyze_dead_letter_queue(50, 10000, 10)))
    });
}

fn bench_v0410_middleware(c: &mut Criterion) {
    c.bench_function("BulkheadMiddleware::new", |b| {
        b.iter(|| {
            let middleware = BulkheadMiddleware::new(black_box(50));
            black_box(middleware)
        })
    });

    c.bench_function("BulkheadMiddleware::try_acquire", |b| {
        let middleware = BulkheadMiddleware::new(50);
        b.iter(|| black_box(middleware.try_acquire("test_partition")))
    });

    c.bench_function("PriorityBoostMiddleware::new", |b| {
        b.iter(|| {
            let middleware = PriorityBoostMiddleware::new()
                .with_age_boost(Duration::from_secs(300), Priority::High);
            black_box(middleware)
        })
    });

    c.bench_function("ErrorClassificationMiddleware::classify", |b| {
        let middleware = ErrorClassificationMiddleware::new();
        b.iter(|| black_box(middleware.classify_error("connection timeout error")))
    });
}

fn bench_v0410_utilities(c: &mut Criterion) {
    c.bench_function("forecast_queue_capacity_ml", |b| {
        b.iter(|| {
            let history = vec![100, 150, 180, 220, 250, 280];
            black_box(utils::forecast_queue_capacity_ml(&history, 24))
        })
    });

    c.bench_function("optimize_batch_strategy", |b| {
        b.iter(|| black_box(utils::optimize_batch_strategy(1024, 50, 10, 1000)))
    });

    c.bench_function("calculate_multi_queue_efficiency", |b| {
        b.iter(|| {
            let sizes = vec![100, 150, 120, 200, 180];
            let rates = vec![10, 12, 11, 15, 13];
            black_box(utils::calculate_multi_queue_efficiency(&sizes, &rates))
        })
    });

    c.bench_function("predict_resource_exhaustion", |b| {
        b.iter(|| black_box(utils::predict_resource_exhaustion(7000, 10000, 500)))
    });

    c.bench_function("suggest_autoscaling_policy", |b| {
        b.iter(|| black_box(utils::suggest_autoscaling_policy(1000, 500, 100, 200)))
    });
}

fn bench_v0411_middleware(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("CorrelationMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = CorrelationMiddleware::new();
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("ThrottlingMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = ThrottlingMiddleware::new(100.0);
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("CircuitBreakerMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });
}

fn bench_v0412_middleware(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("SchemaValidationMiddleware::new", |b| {
        b.iter(|| {
            black_box(
                SchemaValidationMiddleware::new()
                    .with_required_field("user_id")
                    .with_max_field_count(20)
                    .with_max_body_size(1024 * 1024),
            )
        })
    });

    c.bench_function("SchemaValidationMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = SchemaValidationMiddleware::new().with_max_body_size(1024 * 1024);
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                msg.headers
                    .extra
                    .insert("user_id".to_string(), serde_json::json!("123"));
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("MessageEnrichmentMiddleware::new", |b| {
        b.iter(|| {
            black_box(
                MessageEnrichmentMiddleware::new()
                    .with_hostname("worker-01")
                    .with_environment("production")
                    .with_version("1.0.0")
                    .with_add_timestamp(true),
            )
        })
    });

    c.bench_function("MessageEnrichmentMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = MessageEnrichmentMiddleware::new()
                    .with_hostname("worker-01")
                    .with_environment("production");
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });
}

fn bench_v0412_utilities(c: &mut Criterion) {
    c.bench_function("calculate_message_affinity", |b| {
        b.iter(|| {
            black_box(utils::calculate_message_affinity(
                black_box("user:12345"),
                black_box(8),
            ))
        })
    });

    c.bench_function("analyze_queue_temperature", |b| {
        b.iter(|| {
            black_box(utils::analyze_queue_temperature(
                black_box(100),
                black_box(30),
            ))
        })
    });

    c.bench_function("detect_processing_bottleneck", |b| {
        b.iter(|| {
            black_box(utils::detect_processing_bottleneck(
                black_box(100),
                black_box(50),
                black_box(1000),
                black_box(500),
            ))
        })
    });

    c.bench_function("calculate_optimal_prefetch_multiplier", |b| {
        b.iter(|| {
            black_box(utils::calculate_optimal_prefetch_multiplier(
                black_box(100),
                black_box(10),
                black_box(4),
            ))
        })
    });

    c.bench_function("suggest_queue_consolidation", |b| {
        b.iter(|| {
            let sizes = vec![10, 5, 8];
            let rates = vec![2, 1, 3];
            black_box(utils::suggest_queue_consolidation(
                black_box(&sizes),
                black_box(&rates),
            ))
        })
    });
}

fn bench_v0416_middleware(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("HealthCheckMiddleware::new", |b| {
        b.iter(|| black_box(HealthCheckMiddleware::new().with_check_interval_secs(60)))
    });

    c.bench_function("HealthCheckMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware = HealthCheckMiddleware::new();
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("MessageTaggingMiddleware::new", |b| {
        b.iter(|| {
            black_box(
                MessageTaggingMiddleware::new("production")
                    .with_tag("region", "us-east-1")
                    .with_tag("team", "backend"),
            )
        })
    });

    c.bench_function("MessageTaggingMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware =
                    MessageTaggingMiddleware::new("production").with_tag("region", "us-east-1");
                let mut msg = Message::new(
                    "email_task".to_string(),
                    Uuid::new_v4(),
                    b"test body".to_vec(),
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });

    c.bench_function("CostAttributionMiddleware::new", |b| {
        b.iter(|| {
            black_box(
                CostAttributionMiddleware::new(0.001)
                    .with_compute_cost_per_sec(0.0001)
                    .with_storage_cost_per_mb(0.00001),
            )
        })
    });

    c.bench_function("CostAttributionMiddleware::before_publish", |b| {
        b.iter(|| {
            rt.block_on(async {
                let middleware =
                    CostAttributionMiddleware::new(0.001).with_storage_cost_per_mb(0.00001);
                let mut msg = Message::new(
                    "test_task".to_string(),
                    Uuid::new_v4(),
                    vec![0u8; 1024 * 1024], // 1MB
                );
                middleware.before_publish(&mut msg).await.unwrap();
                black_box(msg)
            })
        })
    });
}

fn bench_v0416_utilities(c: &mut Criterion) {
    c.bench_function("calculate_message_deduplication_window", |b| {
        b.iter(|| {
            black_box(utils::calculate_message_deduplication_window(
                black_box(100),
                black_box(3),
                black_box(5000),
            ))
        })
    });

    c.bench_function("analyze_retry_effectiveness", |b| {
        b.iter(|| {
            black_box(utils::analyze_retry_effectiveness(
                black_box(1000),
                black_box(100),
                black_box(80),
                black_box(20),
            ))
        })
    });

    c.bench_function("calculate_queue_overflow_risk", |b| {
        b.iter(|| {
            black_box(utils::calculate_queue_overflow_risk(
                black_box(8000),
                black_box(10000),
                black_box(100),
                black_box(50),
            ))
        })
    });
}

criterion_group!(
    benches,
    bench_message_creation,
    bench_envelope_creation,
    bench_queue_config_builder,
    bench_retry_policy_delay,
    bench_middleware_chain,
    bench_message_options,
    bench_dlq_config,
    bench_batch_publish_result,
    bench_broker_metrics,
    bench_new_middleware,
    bench_utility_functions,
    bench_config_builders,
    bench_monitoring_utilities,
    bench_new_middleware_types,
    bench_v047_middleware,
    bench_v047_utilities,
    bench_v0410_middleware,
    bench_v0410_utilities,
    bench_v0411_middleware,
    bench_v0412_middleware,
    bench_v0412_utilities,
    bench_v0416_middleware,
    bench_v0416_utilities,
);
criterion_main!(benches);
