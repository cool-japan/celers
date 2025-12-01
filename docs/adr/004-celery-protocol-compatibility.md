# ADR-004: Celery Protocol Compatibility

## Status
Accepted

## Context

For CeleRS to interoperate with Python Celery workers, we must implement wire-level protocol compatibility. Python Celery has evolved its protocol over versions:

- **Protocol v1**: Legacy (Celery 3.x and earlier)
- **Protocol v2**: Current standard (Celery 4.x+)
- **Protocol v5**: Latest (Celery 5.3+)

### Key Compatibility Requirements

1. **Message Format**: Headers, properties, body structure
2. **Serialization**: JSON, MessagePack, Pickle
3. **Task Metadata**: task_id, root_id, parent_id, group, retries, ETA
4. **Result Storage**: celery-task-meta-{uuid} key format in Redis
5. **Chord Protocol**: Counter-based barrier synchronization

### The Pickle Problem

Python's `pickle` serialization cannot be fully deserialized in Rust. This is a fundamental incompatibility.

## Decision

We will implement **Protocol v2** as the primary target with the following approach:

### 1. Message Format (Full Compatibility)

```rust
pub struct Message {
    pub headers: MessageHeaders,    // task, id, lang, root_id, parent_id
    pub properties: MessageProperties, // correlation_id, reply_to
    pub body: Vec<u8>,               // Serialized payload
    pub content_type: String,        // "application/json"
    pub content_encoding: String,    // "utf-8"
}
```

### 2. Serialization Strategy

- **Default**: JSON (enforced for Rust ↔ Python interop)
- **Optional**: MessagePack (via feature flag)
- **Rejected**: Pickle (security risk, Rust incompatible)

**Policy**: CeleRS will **enforce JSON** as the content-type. If a Python task sends Pickle, CeleRS will:
1. Log a warning
2. Send to Dead Letter Queue (DLQ)
3. OR return an error to the sender (depending on configuration)

### 3. Visibility Timeout (Kombu Redis Compatibility)

Python Kombu uses a Sorted Set (`ZADD`) to track in-flight tasks. We must replicate this exactly:

```lua
-- Atomic pop + visibility timeout
local msg = redis.call('BRPOP', KEYS[1], ARGV[1])
if msg then
    redis.call('ZADD', KEYS[2], ARGV[2], msg[2])
    return msg[2]
end
return nil
```

### 4. Priority Queues

Python Celery creates separate Redis lists per priority level:
- `_kombu.binding.celery` (default)
- `_kombu.binding.celery\x06\x163` (priority 3)
- `_kombu.binding.celery\x06\x169` (priority 9)

CeleRS will use `BRPOP` on multiple lists simultaneously.

### 5. Result Backend Compatibility

**Key Format**: `celery-task-meta-{task-uuid}`

**Value Format** (JSON):
```json
{
  "status": "SUCCESS",
  "result": <json value>,
  "traceback": null,
  "children": [],
  "date_done": "2025-01-15T10:30:00.000000"
}
```

### 6. Chord Protocol

Use Redis `INCR` for atomic counter:
- Each task in chord increments `celery-chord-counter-{group-id}`
- When `counter == total`, trigger callback
- Store chord state in `celery-chord-{group-id}`

## Consequences

### Positive

1. **Interoperability**: Rust and Python workers can coexist in same deployment
2. **Migration Path**: Gradual migration from Python to Rust without downtime
3. **Compatibility Testing**: Can verify against actual Python Celery workers
4. **Ecosystem Access**: Can use existing Celery monitoring tools (Flower, etc.)

### Negative

1. **No Pickle Support**: Cannot process tasks sent with Pickle serialization
2. **Protocol Constraints**: Must maintain wire format even if inefficient
3. **Complexity**: Lua scripts and Redis-specific logic required
4. **Version Lock-in**: Tied to Protocol v2 specification

### Mitigations

1. **Documentation**: Clear migration guide from Pickle to JSON
2. **Validation**: Warn users at startup if Python workers use Pickle
3. **DLQ**: Failed deserialization goes to DLQ for manual inspection
4. **Testing**: Integration test suite with real Python Celery workers

### Migration Guide for Mixed Deployments

```python
# Python Celery configuration
# Enforce JSON serialization for Rust compatibility
app = Celery('myapp', broker='redis://localhost')
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
)
```

## References

- [Celery Protocol Documentation](https://docs.celeryq.dev/en/stable/internals/protocol.html)
- [Kombu Redis Transport](https://github.com/celery/kombu/blob/main/kombu/transport/redis.py)
- Python Pickle security: [PEP 307](https://www.python.org/dev/peps/pep-0307/)
- Similar approach: gRPC (multiple language bindings with wire protocol)
