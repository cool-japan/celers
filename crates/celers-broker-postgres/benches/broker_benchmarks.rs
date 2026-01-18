use celers_broker_postgres::{DbTaskState, RetryStrategy};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
// Note: These benchmarks test the library's type operations and algorithms,
// not database operations which would require a test database.
// For database benchmarks, use integration tests with a real PostgreSQL instance.

fn retry_strategy_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("retry_strategies");

    group.bench_function("exponential_backoff", |b| {
        let strategy = RetryStrategy::Exponential {
            max_delay_secs: 3600,
        };
        b.iter(|| {
            for retry_count in 0..10 {
                black_box(strategy.calculate_backoff(black_box(retry_count)));
            }
        });
    });

    group.bench_function("exponential_with_jitter", |b| {
        let strategy = RetryStrategy::ExponentialWithJitter {
            max_delay_secs: 3600,
        };
        b.iter(|| {
            for retry_count in 0..10 {
                black_box(strategy.calculate_backoff(black_box(retry_count)));
            }
        });
    });

    group.bench_function("linear_backoff", |b| {
        let strategy = RetryStrategy::Linear {
            base_delay_secs: 10,
            max_delay_secs: 300,
        };
        b.iter(|| {
            for retry_count in 0..10 {
                black_box(strategy.calculate_backoff(black_box(retry_count)));
            }
        });
    });

    group.bench_function("fixed_delay", |b| {
        let strategy = RetryStrategy::Fixed { delay_secs: 30 };
        b.iter(|| {
            for retry_count in 0..10 {
                black_box(strategy.calculate_backoff(black_box(retry_count)));
            }
        });
    });

    group.bench_function("immediate", |b| {
        let strategy = RetryStrategy::Immediate;
        b.iter(|| {
            for retry_count in 0..10 {
                black_box(strategy.calculate_backoff(black_box(retry_count)));
            }
        });
    });

    group.finish();
}

fn state_conversion_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_conversions");

    group.bench_function("state_to_string", |b| {
        let state = DbTaskState::Processing;
        b.iter(|| black_box(state.to_string()));
    });

    group.bench_function("state_from_string", |b| {
        b.iter(|| black_box("processing".parse::<DbTaskState>().unwrap()));
    });

    group.bench_function("state_serialize", |b| {
        let state = DbTaskState::Completed;
        b.iter(|| black_box(serde_json::to_string(&state).unwrap()));
    });

    group.bench_function("state_deserialize", |b| {
        let json = "\"completed\"";
        b.iter(|| black_box(serde_json::from_str::<DbTaskState>(json).unwrap()));
    });

    group.finish();
}

fn retry_calculation_at_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("retry_calculation_scale");

    for retry_count in [1, 5, 10, 20].iter() {
        group.throughput(Throughput::Elements(*retry_count as u64));

        group.bench_with_input(
            BenchmarkId::new("exponential", retry_count),
            retry_count,
            |b, &retry_count| {
                let strategy = RetryStrategy::Exponential {
                    max_delay_secs: 3600,
                };
                b.iter(|| {
                    for i in 0..retry_count {
                        black_box(strategy.calculate_backoff(black_box(i)));
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("jitter", retry_count),
            retry_count,
            |b, &retry_count| {
                let strategy = RetryStrategy::ExponentialWithJitter {
                    max_delay_secs: 3600,
                };
                b.iter(|| {
                    for i in 0..retry_count {
                        black_box(strategy.calculate_backoff(black_box(i)));
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    retry_strategy_benchmarks,
    state_conversion_benchmarks,
    retry_calculation_at_scale
);
criterion_main!(benches);
