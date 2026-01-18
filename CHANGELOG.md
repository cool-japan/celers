# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-18

### Added

#### Core & Protocol Layer
- Add `celers-core` with core traits (`Task`, `Broker`, `ResultBackend`, `TaskExecutor`)
- Add `celers-protocol` with Celery Protocol v2/v5 message format compatibility
- Add `celers-kombu` for Kombu-compatible messaging abstraction
- Add `celers-macros` with `#[celers::task]` procedural macro for automatic task registration
- Add `celers` facade crate with unified API

#### Broker Layer (5 Implementations)
- Add `celers-broker-redis` with Lua scripts, pipelining, and Redis Streams support
- Add `celers-broker-postgres` with PostgreSQL and `FOR UPDATE SKIP LOCKED` optimization
- Add `celers-broker-sql` with MySQL/SQLite support and batch operations
- Add `celers-broker-amqp` with RabbitMQ/AMQP exchanges and routing
- Add `celers-broker-sqs` with AWS SQS cloud-native integration

#### Result Backend Layer (3 Implementations)
- Add `celers-backend-redis` with fast in-memory storage and automatic TTL
- Add `celers-backend-db` with PostgreSQL/MySQL durability and SQL analytics
- Add `celers-backend-rpc` with gRPC-based result storage for microservices
- Add chord support with distributed barrier synchronization across all backends

#### Runtime & Workflow Layer
- Add `celers-worker` with task execution, graceful shutdown, and concurrency control
- Add `celers-canvas` with workflow primitives (Chain, Group, Chord, Map, Starmap)
- Add `celers-beat` with periodic task scheduler using cron expressions and solar schedules

#### Utilities & Tooling
- Add `celers-cli` with worker management, queue inspection, and DLQ operations
- Add `celers-metrics` with Prometheus metrics integration (throughput, latency, queue depth)

#### Core Features
- Add type-safe task definitions with compile-time signature verification
- Add priority queues with multi-level task prioritization
- Add dead letter queue (DLQ) with automatic handling of permanently failed tasks
- Add task cancellation via Pub/Sub for in-flight tasks
- Add retry logic with exponential backoff and configurable max retries
- Add timeout enforcement at task and worker levels
- Add graceful shutdown with in-flight task completion
- Add health checks for Kubernetes liveness/readiness probes

#### Observability
- Add OpenTelemetry integration for distributed tracing
- Add structured logging with context propagation
- Add performance profiling and resource tracking
- Add Grafana dashboard templates

#### Interoperability
- Add binary-level Celery protocol compatibility
- Add support for interoperation with Python Celery workers
- Add message format negotiation (v2/v5)
- Add compression support (zlib, zstd, gzip)
- Add encryption support for sensitive task payloads

#### Advanced Features
- Add circuit breaker pattern for fault tolerance
- Add bulkhead isolation for resource management
- Add rate limiting with token bucket algorithm
- Add quota management for multi-tenant scenarios
- Add distributed locks with Redis-based implementation
- Add task groups for coordinated execution
- Add backup/restore utilities for broker state
- Add monitoring dashboards with real-time queue metrics

### Changed
- Replace all production `unwrap()` calls with proper error handling using `expect()` (120+ occurrences)
- Update telemetry ID generation to use randomness for uniqueness guarantees
- Improve error messages with descriptive context throughout the codebase

### Fixed
- Fix telemetry span ID and trace ID generation to ensure uniqueness
- Fix workspace configuration to match actual crate structure
- Correct Cargo.toml metadata for all 18 workspace crates

### Security
- Eliminate all `unwrap()` usage in production code following "No unwrap policy"
- Add encryption support for task payloads using AES-GCM
- Add HMAC-based message integrity verification
- Add secure credential handling for broker connections

## Project Information

### Workspace Structure
- Total crates: 18
- Facade: 1 (celers)
- Core/Protocol: 4 (celers-core, celers-protocol, celers-kombu, celers-macros)
- Brokers: 5 (Redis, PostgreSQL, SQL, AMQP, SQS)
- Result Backends: 3 (Redis, Database, RPC)
- Runtime: 3 (worker, canvas, beat)
- Utilities: 2 (cli, metrics)

### Supported Rust Versions
- Minimum Supported Rust Version (MSRV): 1.70+
- Edition: 2021

### License
- MIT OR Apache-2.0

### Authors
- COOLJAPAN OU (Team Kitasan)

### Repository
- https://github.com/cool-japan/celers

[unreleased]: https://github.com/cool-japan/celers/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cool-japan/celers/releases/tag/v0.1.0
