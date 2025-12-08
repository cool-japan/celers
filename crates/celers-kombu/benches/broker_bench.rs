use celers_kombu::*;
use celers_protocol::Message;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use uuid::Uuid;

// Mock broker for benchmarking (same as in tests but simplified)
struct BenchBroker {
    connected: bool,
}

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
            let chain = MiddlewareChain::new()
                .with_middleware(Box::new(ValidationMiddleware::new()))
                .with_middleware(Box::new(LoggingMiddleware::new()))
                .with_middleware(Box::new(MetricsMiddleware::new()));

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
                let result = BatchPublishResult::new(black_box(size), black_box(0));
                assert!(result.is_complete_success());
                black_box(result)
            })
        });
    }
    group.finish();
}

fn bench_broker_metrics(c: &mut Criterion) {
    c.bench_function("broker_metrics_increment", |b| {
        let mut metrics = BrokerMetrics::default();
        b.iter(|| {
            metrics.messages_published += 1;
            metrics.messages_consumed += 1;
            metrics.messages_acknowledged += 1;
            black_box(&metrics)
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
);
criterion_main!(benches);
