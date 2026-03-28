# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-03-28

### Added

#### Event Persistence
- File-based event storage with JSONL format and automatic log rotation for audit trails
- Database-backed event persistence for durable, queryable event history
- Event filtering and routing system with topic-based subscription and pattern matching
- AMQP event transport using fanout exchange for real-time event broadcasting

#### Result Chunking
- Auto-split large task results across multiple Redis keys to bypass size limits
- CRC32 checksum verification for chunked result integrity
- Transparent reassembly on result retrieval with configurable chunk size thresholds

#### Beat Heartbeat and Failover
- Leader election for beat scheduler instances using distributed locks
- Lease renewal with configurable heartbeat intervals
- Automatic standby failover when the active leader becomes unresponsive
- Distributed beat locks with Redis and database backends for single-leader scheduling

#### AMQP Topic Routing
- Glob pattern matching for task name to routing key mapping
- Wildcard-based topic exchange routing for flexible task distribution
- Configurable routing rules per task type with fallback defaults

#### Enhanced Configuration
- Support for 23+ `CELERY_*` environment variables for runtime configuration
- `validate_detailed()` method for comprehensive configuration validation with diagnostics
- Configuration export to TOML/JSON for reproducible deployments
- Centralized `celers-core::config` module for unified configuration management

#### Protocol and Serialization
- Serialization auto-detection for incoming messages (JSON, MessagePack, YAML, BSON)
- Dedicated `celers-protocol::serializer` module extracted for cleaner separation
- Compression type unification across crates (unified `CompressionType` enum shared by all broker and backend crates)

#### Other Additions
- Per-task TTL configuration and metadata storage in result backends
- Zstd compression support across all broker and backend crates via OxiARC

### Changed

- Massive codebase refactoring: split all source files to under 2000 lines each (552 Rust files, 198K SLoC total)
- Extracted large modules into focused sub-modules across all 18 workspace crates (net reduction of ~115K lines through deduplication and reorganization)
- Replaced compression backends with OxiARC (oxiarc-*) for Pure Rust compliance across celers-protocol, celers-broker-amqp, celers-broker-redis, and celers-backend-redis
- Upgraded dependencies: lapin 4.3, rand 0.10, sha2/hmac version alignment, OxiARC integration
- Moved CLI commands module out of monolithic file into dedicated sub-modules in celers-cli
- Reorganized celers-kombu, celers-canvas, celers-metrics, and celers-macros internals for maintainability
- Test suite expanded to 4075 tests (up from 3979 in 0.1.0)

### Fixed

- Compression round-trip correctness across all broker and backend crates after OxiARC migration
- Result backend key handling for large payloads that previously exceeded single-key Redis limits
- Beat scheduler stability under concurrent leader election scenarios
- Configuration validation edge cases for environment variable overrides

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
- Apache-2.0

### Authors
- COOLJAPAN OU (Team Kitasan)

### Repository
- https://github.com/cool-japan/celers

[unreleased]: https://github.com/cool-japan/celers/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/cool-japan/celers/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cool-japan/celers/releases/tag/v0.1.0
