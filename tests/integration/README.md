# Integration Tests

This directory contains integration tests that require actual broker instances (Redis, PostgreSQL, etc.).

## Running Integration Tests

```bash
# Start required services
docker-compose up -d redis postgres

# Run integration tests
cargo test --test '*' --features integration

# Clean up
docker-compose down
```

## Test Categories

- `test_redis_broker.rs` - Redis broker integration tests
- `test_postgres_broker.rs` - PostgreSQL broker integration tests
- `test_worker_lifecycle.rs` - Worker start/stop/graceful shutdown
- `test_task_execution.rs` - End-to-end task execution
- `test_priority_queue.rs` - Priority queue behavior
- `test_dlq.rs` - Dead Letter Queue operations

## Requirements

- Redis 6.0+
- PostgreSQL 13+
- Docker (for containerized testing)

## Future Tests

- [ ] Python-Rust interoperability tests (`../python-compat/`)
- [ ] Chord workflow tests
- [ ] Beat scheduler tests
- [ ] Performance benchmarks under load
