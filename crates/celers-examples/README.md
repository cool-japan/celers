# celers-examples

Example applications and benchmarks for the CeleRS distributed task queue. Demonstrates common usage patterns across brokers, backends, workflows, and runtime features.

## Examples

| Example | Description |
|---------|-------------|
| `basic_processing` | Minimal task send/receive |
| `async_result` | Asynchronous result retrieval |
| `facade_usage` | Using the `celers` facade crate |
| `macro_tasks` | Task definition with proc macros |
| `canvas_workflows` | Chain, group, and chord workflows |
| `priority_queue` | Priority-based task scheduling |
| `dead_letter_queue` | Failed task routing and inspection |
| `task_cancellation` | Revoking in-flight tasks |
| `graceful_shutdown` | Clean worker shutdown handling |
| `health_checks` | Worker and broker health monitoring |
| `prometheus_metrics` | Metrics export (requires `metrics` feature) |
| `postgres_broker_example` | PostgreSQL as message broker |
| `image_processing` | Image processing pipeline |
| `web_scraper` | Distributed web scraping |
| `phase1_complete` | Full Phase 1 feature demonstration |

## Running an Example

```bash
cargo run --example basic_processing
cargo run --example prometheus_metrics --features metrics
cargo run --example canvas_workflows --features workflows
```

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `serialization` | Message serialization throughput |
| `queue_operations` | Enqueue/dequeue performance |
| `batch_operations` | Bulk task submission |

```bash
cargo bench --bench serialization
```

## Feature Flags

| Feature | Description |
|---------|-------------|
| `metrics` | Enable Prometheus metrics in worker/broker |
| `workflows` | Enable canvas workflow support |

## Part of CeleRS

This crate is part of the [CeleRS](https://github.com/cool-japan/celers) project, a Celery-compatible distributed task queue for Rust.

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
