# ADR-005: SQS Broker Celery Compatibility

## Status
Proposed

## Context

CeleRS provides an SQS broker implementation (`celers-broker-sqs`) with advanced features including:
- DLQ analytics and management
- Cost tracking and optimization
- Circuit breaker pattern
- Rate limiting and backpressure
- Lambda integration helpers

However, the compatibility with Python Celery's SQS transport (via Kombu) has not been explicitly documented or tested. This creates uncertainty for users who want to:
1. Migrate from Python Celery SQS to CeleRS
2. Run mixed Python/Rust deployments with SQS
3. Understand visibility timeout and priority queue behavior

### Python Celery SQS Transport Behavior

Python Celery uses Kombu's SQS transport with the following characteristics:

1. **Visibility Timeout**: Uses SQS native visibility timeout (default: 30 seconds)
2. **Message Attributes**: Stores Celery headers as SQS MessageAttributes
3. **Priority Queues**: Not natively supported by SQS; requires separate queues
4. **Message Format**: JSON body with Celery protocol structure
5. **Queue Naming**: Configurable, typically `celery` or task-specific queues

### Current CeleRS SQS Implementation Gaps

Based on our investigation, the following areas need clarification:

| Feature | Status | Notes |
|---------|--------|-------|
| Visibility Timeout | ⚠️ Unknown | SQS native vs custom? |
| Message Attributes | ⚠️ Unknown | Header mapping? |
| Priority Strategy | ⚠️ Unknown | Queue partitioning vs attributes? |
| Queue Naming | ⚠️ Unknown | Kombu-compatible? |
| Protocol Format | ⚠️ Unknown | Wire-format compatible? |

## Decision

### 1. Visibility Timeout Strategy

**Decision**: Use SQS native visibility timeout with configurable extension.

```rust
pub struct SqsConfig {
    /// Default visibility timeout in seconds (SQS: 0-43200)
    pub visibility_timeout: u32,  // Default: 1800 (30 min)

    /// Auto-extend visibility before timeout
    pub auto_extend_visibility: bool,

    /// Extension interval (seconds before timeout to extend)
    pub extend_threshold: u32,
}
```

**Rationale**:
- SQS provides native visibility timeout, unlike Redis which requires Lua scripts
- Auto-extension prevents message re-delivery for long-running tasks
- Compatible with Python Celery's default behavior

### 2. Message Attribute Mapping

**Decision**: Map Celery headers to SQS MessageAttributes for interoperability.

```rust
// Celery header -> SQS MessageAttribute mapping
const ATTRIBUTE_MAPPINGS: &[(&str, &str)] = &[
    ("task", "celery-task"),
    ("id", "celery-id"),
    ("lang", "celery-lang"),
    ("root_id", "celery-root-id"),
    ("parent_id", "celery-parent-id"),
    ("group", "celery-group"),
    ("retries", "celery-retries"),
    ("eta", "celery-eta"),
    ("expires", "celery-expires"),
];
```

**Message Body**: JSON format matching Protocol v2 `[args, kwargs, embed]`

### 3. Priority Queue Strategy

**Decision**: Use separate SQS queues per priority level (matching Kombu pattern).

```
Queue naming:
- {base_queue}           (default, priority 0)
- {base_queue}-priority-3
- {base_queue}-priority-6
- {base_queue}-priority-9
```

**Rationale**:
- SQS does not support message-level priority
- Separate queues allow priority-based consumption
- Matches Python Celery/Kombu SQS behavior

### 4. Queue Naming Convention

**Decision**: Support both Kombu-compatible and custom naming.

```rust
pub enum QueueNamingStrategy {
    /// Kombu-compatible: {prefix}_{queue_name}
    Kombu { prefix: String },

    /// Custom naming with optional suffix
    Custom { template: String },

    /// Direct queue names (no transformation)
    Direct,
}
```

### 5. Serialization

**Decision**: JSON only (consistent with ADR-004).

- Content-Type: `application/json`
- Content-Encoding: `utf-8`
- Pickle: Rejected (same as Redis/AMQP brokers)

## Implementation Plan

### Phase 1: Documentation
- [ ] Document current SQS implementation behavior
- [ ] Add migration guide from Python Celery SQS
- [ ] Update README with compatibility notes

### Phase 2: Compatibility Layer
- [ ] Implement MessageAttribute mapping
- [ ] Add visibility timeout auto-extension
- [ ] Support Kombu queue naming

### Phase 3: Testing
- [ ] Integration tests with Python Celery SQS transport
- [ ] Bi-directional message flow tests
- [ ] Priority queue behavior verification

### Phase 4: Production Features
- [ ] Mixed deployment monitoring
- [ ] Compatibility validation at startup
- [ ] DLQ handling for incompatible messages

## Consequences

### Positive

1. **Clear Compatibility Path**: Users know exactly what works with Python Celery
2. **Migration Support**: Documented path from Python to Rust
3. **Mixed Deployments**: Validated interoperability for gradual migration
4. **Feature Parity**: Same capabilities as Redis/AMQP brokers

### Negative

1. **Additional Complexity**: MessageAttribute mapping overhead
2. **Queue Proliferation**: Priority queues multiply queue count
3. **SQS Limitations**: No true message priority, FIFO ordering constraints
4. **Cost Implications**: More queues = more SQS costs

### Mitigations

1. **Documentation**: Clear guidance on queue strategy trade-offs
2. **Configuration**: Flexible options for different use cases
3. **Monitoring**: Built-in cost tracking (already implemented)
4. **Validation**: Startup checks for configuration compatibility

## Migration Guide

### From Python Celery SQS to CeleRS

```python
# Python Celery configuration (before)
app = Celery('myapp', broker='sqs://')
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    broker_transport_options={
        'visibility_timeout': 3600,
        'polling_interval': 1,
        'queue_name_prefix': 'celery-',
    }
)
```

```rust
// CeleRS configuration (after)
let config = SqsConfig {
    visibility_timeout: 3600,
    queue_name_prefix: "celery-".to_string(),
    naming_strategy: QueueNamingStrategy::Kombu {
        prefix: "celery-".to_string(),
    },
    ..Default::default()
};
```

### Mixed Deployment

1. **Ensure JSON serialization** on Python side
2. **Match queue naming** between Python and Rust
3. **Same visibility timeout** configuration
4. **Monitor DLQ** for incompatible messages

## Testing Requirements

### Unit Tests
- MessageAttribute encoding/decoding
- Queue name generation
- Visibility timeout calculations

### Integration Tests
- Python producer -> CeleRS consumer
- CeleRS producer -> Python consumer
- Bi-directional message flow
- Priority queue ordering

### Compatibility Tests
- Verify wire-format with Python Celery
- Validate MessageAttribute structure
- Confirm DLQ behavior for edge cases

## References

- [Kombu SQS Transport](https://github.com/celery/kombu/blob/main/kombu/transport/SQS.py)
- [AWS SQS Documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/)
- [ADR-004: Celery Protocol Compatibility](./004-celery-protocol-compatibility.md)
- [Python Celery SQS Guide](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/sqs.html)
