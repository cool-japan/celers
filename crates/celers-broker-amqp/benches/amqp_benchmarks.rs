//! Benchmarks for AMQP broker operations
//!
//! Note: These benchmarks are currently disabled due to limitations with
//! criterion's async API and mutable borrow requirements.
//!
//! For performance testing, please use the example programs with timing:
//! ```bash
//! time cargo run --release --example batch_publish
//! ```
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672

use criterion::{criterion_group, criterion_main, Criterion};

/// Placeholder benchmark - disabled due to criterion async API limitations
fn bench_placeholder(c: &mut Criterion) {
    c.bench_function("placeholder", |b| {
        b.iter(|| {
            // Benchmarks disabled - use examples for performance testing
            1 + 1
        });
    });
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
