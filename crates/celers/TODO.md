# celers TODO

> Facade crate providing unified API for CeleRS

## Status: ✅ FEATURE COMPLETE

All core features implemented and production-ready.

## Completed Features

### Core Exports ✅
- [x] Re-export core types (Broker, SerializedTask, TaskState)
- [x] Re-export protocol types (Message, MessageHeaders, etc.)
- [x] Re-export kombu types (Producer, Consumer, Transport)
- [x] Re-export worker types (Worker, WorkerConfig)
- [x] Re-export canvas types (Chain, Chord, Group, Map, Starmap)
- [x] Re-export macros (task, Task derive)

### Feature Flags ✅
- [x] Redis broker feature
- [x] PostgreSQL broker feature
- [x] MySQL broker feature
- [x] AMQP broker feature
- [x] SQS broker feature
- [x] Backend-redis feature
- [x] Metrics feature
- [x] Workflows feature
- [x] Beat feature

### Convenience Modules ✅
- [x] Prelude module for common imports
- [x] Error module for error types
- [x] Protocol module for advanced usage
- [x] Canvas module for workflows
- [x] Worker module for runtime

## Future Enhancements

### Additional Features
- [x] AMQP broker feature ✅
- [x] SQS broker feature ✅
- [x] MySQL broker feature ✅
- [x] Backend-db feature ✅
- [x] Backend-rpc feature ✅
- [ ] Tracing feature (OpenTelemetry)
  - [ ] Full tracing integration
  - [ ] Span propagation
  - [ ] Trace context management
- [ ] Additional broker features
  - [ ] Kafka broker feature
  - [ ] NATS broker feature
  - [ ] Azure Service Bus feature
  - [ ] Google Pub/Sub feature
- [ ] Additional backend features
  - [ ] S3 backend feature
  - [ ] Cassandra backend feature
  - [ ] MongoDB backend feature

### Documentation
- [x] Comprehensive README
- [x] Module-level documentation
- [x] Examples in documentation
- [ ] Tutorial/getting started guide
  - [ ] Installation and setup
  - [ ] First task example
  - [ ] Worker configuration
  - [ ] Production deployment
- [ ] Migration guide from Python Celery
  - [ ] Feature comparison
  - [ ] API mapping
  - [ ] Code conversion examples
  - [ ] Performance differences
- [ ] Architecture documentation
  - [ ] System design overview
  - [ ] Component interactions
  - [ ] Data flow diagrams
  - [ ] Scalability patterns
- [ ] Advanced guides
  - [ ] Performance tuning
  - [ ] Security best practices
  - [ ] Troubleshooting guide
  - [ ] Monitoring and alerting

### Examples
- [ ] Complete end-to-end example
  - [ ] Web application integration
  - [ ] Database tasks
  - [ ] Email sending
  - [ ] File processing
- [ ] Microservices example
  - [ ] Service-to-service communication
  - [ ] Event-driven architecture
  - [ ] Saga pattern implementation
- [ ] Distributed workflow example
  - [ ] Complex DAG workflows
  - [ ] Data pipeline
  - [ ] ETL processes
- [ ] High-throughput processing example
  - [ ] Batch processing
  - [ ] Stream processing
  - [ ] Real-time analytics
- [ ] Real-world use cases
  - [ ] Image processing pipeline
  - [ ] Video transcoding
  - [ ] Web scraping
  - [ ] Report generation
  - [ ] Notification system

### Quality of Life
- [x] Builder pattern for Worker configuration ✅
  - [x] Fluent API
  - [x] Validation on build
  - [x] Preset configurations
- [x] Default broker selection helper ✅
  - [x] Auto-detect from environment (CELERS_BROKER_TYPE, CELERS_BROKER_URL, CELERS_BROKER_QUEUE)
  - [x] Explicit broker creation (create_broker function)
  - [x] Feature-aware error messages
  - [x] Support for all broker types (Redis, PostgreSQL, MySQL, AMQP, SQS)
- [ ] Configuration validation
  - [ ] Schema validation
  - [ ] Runtime checks
  - [ ] Configuration preview
- [ ] Feature compatibility matrix
  - [ ] Document feature combinations
  - [ ] Warn on incompatibilities
  - [ ] Suggest alternatives
- [ ] Error messages and diagnostics
  - [ ] Helpful error messages
  - [ ] Suggestions for fixes
  - [ ] Context-aware errors
- [ ] Development utilities
  - [ ] Task testing helpers
  - [ ] Mock brokers for testing
  - [ ] Debugging tools

### Performance
- [ ] Compile-time feature validation
  - [ ] Feature conflict detection
  - [ ] Dead code elimination
  - [ ] Inline optimizations
- [ ] Zero-cost abstractions verification
  - [ ] Benchmark against raw implementations
  - [ ] Assembly inspection
  - [ ] Performance regression tests
- [ ] Bundle size optimization
  - [ ] Feature-specific builds
  - [ ] Link-time optimization
  - [ ] Binary size reporting
- [ ] Startup time optimization
  - [ ] Lazy initialization
  - [ ] Parallel initialization
  - [ ] Pre-compiled regex/parsers

### Developer Experience
- [ ] IDE support improvements
  - [ ] Better type hints
  - [ ] Code completion
  - [ ] Inline documentation
- [ ] Procedural macro enhancements
  - [ ] Better error messages
  - [ ] More derive options
  - [ ] Custom attributes
- [x] Prelude improvements ✅
  - [x] More convenient imports (WorkerConfigBuilder, TaskState, Starmap, TaskOptions, BrokerError, Beat types)
  - [ ] Context-aware re-exports
  - [ ] Version-specific preludes

## Testing

- [x] Basic facade exports test
- [ ] Feature flag compatibility tests
- [ ] Integration test with all brokers
- [ ] Documentation tests
- [ ] Example code tests

## Dependencies

All dependencies are re-exported from sub-crates.

## Notes

- This is a facade crate - implementation is in sub-crates
- Keep minimal code in this crate (just re-exports)
- All feature flags should pass through to sub-crates
- Documentation should link to sub-crate docs for details
