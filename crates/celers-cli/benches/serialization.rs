//! Serialization benchmarks for `celers-cli`'s configuration and task data
//! structures.
//!
//! Benchmarks pure in-memory serde work only: TOML/YAML for
//! [`celers_cli::config::Config`] (the two formats `celers_cli::config`
//! actually supports, auto-detected by [`celers_cli::config::ConfigFormat`])
//! and JSON for `celers_core::SerializedTask` (the exact format
//! `celers-cli`'s `commands::task` functions use to move tasks between
//! queues, see `src/commands/task.rs`). No broker connection or filesystem
//! I/O is exercised here -- see `crates/celers-cli/TODO.md`'s package plan
//! P7 for the property-based-test companion suite
//! (`tests/proptest_cli.rs`), which does exercise `Config::to_file`.

use celers_cli::config::{
    AlertConfig, AutoScaleConfig, BrokerConfig, CacheConfig, Config, ConfigFormat, PoolConfig,
    WorkerConfig,
};
use celers_core::{SerializedTask, TaskMetadata, TaskState};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;

/// The configuration produced by `celers init` (see
/// `celers_cli::config::Config::default_config`) -- the smallest realistic
/// `Config`.
fn sample_config_minimal() -> Config {
    Config::default_config()
}

/// A `Config` with every optional section populated and several queues /
/// failover URLs, representative of a production deployment file.
fn sample_config_full() -> Config {
    Config {
        profile: Some("production".to_string()),
        broker: BrokerConfig {
            broker_type: "redis".to_string(),
            url: "redis://prod-cluster.internal:6379/0".to_string(),
            failover_urls: vec![
                "redis://prod-cluster-replica-1.internal:6379/0".to_string(),
                "redis://prod-cluster-replica-2.internal:6379/0".to_string(),
            ],
            failover_retries: 5,
            failover_timeout_secs: 10,
            queue: "default".to_string(),
            mode: "priority".to_string(),
        },
        worker: WorkerConfig {
            concurrency: 32,
            poll_interval_ms: 250,
            max_retries: 5,
            default_timeout_secs: 600,
        },
        queues: vec![
            "default".to_string(),
            "high-priority".to_string(),
            "low-priority".to_string(),
            "email".to_string(),
        ],
        autoscale: Some(AutoScaleConfig {
            enabled: true,
            min_workers: 4,
            max_workers: 64,
            scale_up_threshold: 500,
            scale_down_threshold: 50,
            check_interval_secs: 15,
        }),
        alerts: Some(AlertConfig {
            enabled: true,
            webhook_url: Some("https://hooks.example.com/services/T00/B00/XXXX".to_string()),
            dlq_threshold: 100,
            failed_threshold: 250,
            check_interval_secs: 30,
        }),
        pool: PoolConfig {
            max_size: 32,
            reuse_enabled: true,
        },
        cache: CacheConfig {
            ttl_secs: 30,
            enabled: true,
        },
        aliases: None,
    }
}

fn bench_config_toml_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("config_toml_serialize");
    group.throughput(Throughput::Elements(1));

    let minimal = sample_config_minimal();
    group.bench_function("minimal", |b| {
        b.iter(|| {
            let s = black_box(&minimal)
                .to_string_with_format(ConfigFormat::Toml)
                .expect("serialize minimal config to TOML");
            black_box(s);
        });
    });

    let full = sample_config_full();
    group.bench_function("full", |b| {
        b.iter(|| {
            let s = black_box(&full)
                .to_string_with_format(ConfigFormat::Toml)
                .expect("serialize full config to TOML");
            black_box(s);
        });
    });

    group.finish();
}

fn bench_config_toml_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("config_toml_deserialize");
    group.throughput(Throughput::Elements(1));

    let minimal_str = sample_config_minimal()
        .to_string_with_format(ConfigFormat::Toml)
        .expect("serialize minimal config to TOML");
    group.bench_function("minimal", |b| {
        b.iter(|| {
            let config = Config::from_str_with_format(black_box(&minimal_str), ConfigFormat::Toml)
                .expect("deserialize minimal config from TOML");
            black_box(config);
        });
    });

    let full_str = sample_config_full()
        .to_string_with_format(ConfigFormat::Toml)
        .expect("serialize full config to TOML");
    group.bench_function("full", |b| {
        b.iter(|| {
            let config = Config::from_str_with_format(black_box(&full_str), ConfigFormat::Toml)
                .expect("deserialize full config from TOML");
            black_box(config);
        });
    });

    group.finish();
}

fn bench_config_yaml_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("config_yaml_serialize");
    group.throughput(Throughput::Elements(1));

    let minimal = sample_config_minimal();
    group.bench_function("minimal", |b| {
        b.iter(|| {
            let s = black_box(&minimal)
                .to_string_with_format(ConfigFormat::Yaml)
                .expect("serialize minimal config to YAML");
            black_box(s);
        });
    });

    let full = sample_config_full();
    group.bench_function("full", |b| {
        b.iter(|| {
            let s = black_box(&full)
                .to_string_with_format(ConfigFormat::Yaml)
                .expect("serialize full config to YAML");
            black_box(s);
        });
    });

    group.finish();
}

fn bench_config_yaml_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("config_yaml_deserialize");
    group.throughput(Throughput::Elements(1));

    let minimal_str = sample_config_minimal()
        .to_string_with_format(ConfigFormat::Yaml)
        .expect("serialize minimal config to YAML");
    group.bench_function("minimal", |b| {
        b.iter(|| {
            let config = Config::from_str_with_format(black_box(&minimal_str), ConfigFormat::Yaml)
                .expect("deserialize minimal config from YAML");
            black_box(config);
        });
    });

    let full_str = sample_config_full()
        .to_string_with_format(ConfigFormat::Yaml)
        .expect("serialize full config to YAML");
    group.bench_function("full", |b| {
        b.iter(|| {
            let config = Config::from_str_with_format(black_box(&full_str), ConfigFormat::Yaml)
                .expect("deserialize full config from YAML");
            black_box(config);
        });
    });

    group.finish();
}

/// A `SerializedTask` with a `payload_len`-byte payload, mirroring what
/// `commands::task`'s Redis-backed helpers build via `serde_json` before
/// pushing onto a queue.
fn sample_task(payload_len: usize) -> SerializedTask {
    let mut metadata = TaskMetadata::new("bench_task".to_string());
    metadata.priority = 5;
    metadata.max_retries = 3;
    metadata.state = TaskState::Pending;
    SerializedTask {
        metadata,
        payload: vec![0xABu8; payload_len],
    }
}

fn bench_serialized_task_json_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialized_task_json_serialize");
    group.throughput(Throughput::Elements(1));

    for (label, len) in [
        ("small_16b", 16usize),
        ("medium_256b", 256),
        ("large_4kb", 4096),
    ] {
        let task = sample_task(len);
        group.bench_function(label, |b| {
            b.iter(|| {
                let s = serde_json::to_string(black_box(&task)).expect("serialize SerializedTask");
                black_box(s);
            });
        });
    }

    group.finish();
}

fn bench_serialized_task_json_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialized_task_json_deserialize");
    group.throughput(Throughput::Elements(1));

    for (label, len) in [
        ("small_16b", 16usize),
        ("medium_256b", 256),
        ("large_4kb", 4096),
    ] {
        let json = serde_json::to_string(&sample_task(len)).expect("serialize SerializedTask");
        group.bench_function(label, |b| {
            b.iter(|| {
                let task: SerializedTask =
                    serde_json::from_str(black_box(&json)).expect("deserialize SerializedTask");
                black_box(task);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_config_toml_serialize,
    bench_config_toml_deserialize,
    bench_config_yaml_serialize,
    bench_config_yaml_deserialize,
    bench_serialized_task_json_serialize,
    bench_serialized_task_json_deserialize,
);
criterion_main!(benches);
