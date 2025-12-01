# ADR-001: Async Runtime Selection

## Status
Accepted

## Context

CeleRS requires an async runtime for handling I/O operations with brokers (Redis, PostgreSQL, AMQP, SQS) and result backends. The Rust ecosystem has two major async runtime options:

1. **Tokio**: The most widely adopted async runtime with extensive ecosystem support
2. **async-std**: An alternative runtime with different design philosophy

Most broker client libraries (redis-rs, lapin, sqlx, aws-sdk) are built specifically for Tokio, making integration with other runtimes challenging or requiring compatibility layers.

## Decision

We will use **Tokio** as the exclusive async runtime for CeleRS.

### Rationale

1. **Ecosystem Compatibility**
   - `redis-rs`: Tokio-native with `tokio-comp` feature
   - `lapin` (AMQP): Built on Tokio
   - `sqlx`: Tokio-native runtime
   - `aws-sdk-rust`: Tokio-based

2. **Performance**
   - Tokio's work-stealing scheduler is optimized for I/O-bound workloads
   - Well-tested in production at scale (Discord, AWS, Microsoft)

3. **Community Support**
   - Largest async runtime community
   - Extensive documentation and examples
   - Active maintenance and development

4. **Feature Set**
   - Comprehensive primitives (channels, mutexes, semaphores)
   - Built-in tracing integration
   - Robust testing utilities

## Consequences

### Positive

- Seamless integration with all broker and backend implementations
- Access to Tokio's rich ecosystem (tower, hyper, tonic)
- No need for compatibility layers or runtime adapters
- Better performance due to runtime-specific optimizations

### Negative

- Users cannot choose alternative runtimes (async-std, smol)
- Dependency on Tokio's API stability
- Larger binary size compared to minimal runtimes

### Neutral

- All examples and documentation will be Tokio-specific
- Testing infrastructure built around `#[tokio::test]`

## References

- [Tokio Documentation](https://tokio.rs)
- [Are We Async Yet?](https://areweasyncyet.rs)
- Celery Python uses gevent/eventlet (similar greenlet-based async)
