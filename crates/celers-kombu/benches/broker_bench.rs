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
);
criterion_main!(benches);
