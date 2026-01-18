# CeleRS / Python Celery Compatibility Guide

This document provides a comprehensive overview of CeleRS compatibility with Python Celery, enabling mixed deployments and migrations.

## Quick Start

### Python Celery Configuration (Required)

```python
from celery import Celery

app = Celery('myapp', broker='redis://localhost:6379/0')
app.conf.update(
    # Required for CeleRS compatibility
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',

    # Recommended settings
    task_protocol=2,
    timezone='UTC',
    enable_utc=True,
)
```

### CeleRS Configuration

```rust
// Default configuration is compatible - no changes needed
// JSON serialization is enforced by default
```

## Compatibility Matrix

### Protocol Support

| Protocol | Python Celery | CeleRS | Interoperability |
|----------|--------------|--------|------------------|
| v1 (Legacy) | ✅ | ❌ | Not supported |
| v2 (Standard) | ✅ | ✅ | **Full** |
| v5 (Latest) | ✅ | ✅ | **Full** |

### Serialization

| Format | Python Celery | CeleRS | Interoperability |
|--------|--------------|--------|------------------|
| JSON | ✅ Default | ✅ Default | **Full** |
| MessagePack | ✅ | ✅ `msgpack` feature | **Full** |
| Pickle | ✅ | ❌ | **None** (security) |
| YAML | ✅ | ✅ `yaml` feature | Limited |

### Broker Support

| Broker | Python Celery | CeleRS | Interoperability | Notes |
|--------|--------------|--------|------------------|-------|
| Redis | ✅ | ✅ | **Full** | Kombu-compatible |
| RabbitMQ (AMQP) | ✅ | ✅ | **Full** | 100% compatible |
| Amazon SQS | ✅ | ⚠️ | **Partial** | See [ADR-005](adr/005-sqs-celery-compatibility.md) |
| PostgreSQL | ❌ | ✅ | N/A | CeleRS-specific |

### Result Backend

| Backend | Python Celery | CeleRS | Interoperability |
|---------|--------------|--------|------------------|
| Redis | ✅ | ✅ | **Full** |
| PostgreSQL | ✅ | ✅ | **Full** |
| RPC | ✅ | ✅ | **Full** |

### Canvas (Workflow) Primitives

| Primitive | Python Celery | CeleRS | Interoperability |
|-----------|--------------|--------|------------------|
| Chain | ✅ | ✅ | **Full** |
| Group | ✅ | ✅ | **Full** |
| Chord | ✅ | ✅ | **Full** (Redis required) |
| Map | ✅ | ✅ | **Full** |
| Starmap | ✅ | ✅ | **Full** |
| Chunks | ✅ | ✅ | **Full** |

### Beat (Scheduler)

| Schedule Type | Python Celery | CeleRS | Interoperability |
|---------------|--------------|--------|------------------|
| Interval | ✅ | ✅ | **Full** |
| Crontab | ✅ | ✅ | **Full** |
| Solar | ✅ | ✅ | **Full** |
| One-time | ✅ | ✅ | **Full** |

### Task Features

| Feature | Python Celery | CeleRS | Status |
|---------|--------------|--------|--------|
| Task ID (UUID) | ✅ | ✅ | ✅ Compatible |
| Task Name | ✅ | ✅ | ✅ Compatible |
| Positional Args | ✅ | ✅ | ✅ Compatible |
| Keyword Args | ✅ | ✅ | ✅ Compatible |
| ETA (Delayed) | ✅ | ✅ | ✅ ISO 8601 UTC |
| Expires | ✅ | ✅ | ✅ ISO 8601 UTC |
| Priority (0-9) | ✅ | ✅ | ✅ Compatible |
| Retries | ✅ | ✅ | ✅ Compatible |
| Callbacks (link) | ✅ | ✅ | ✅ Compatible |
| Error Callbacks | ✅ | ✅ | ✅ Compatible |
| Immutable (.si()) | ✅ | ✅ | ✅ Compatible |
| root_id | ✅ | ✅ | ✅ Compatible |
| parent_id | ✅ | ✅ | ✅ Compatible |
| group_id | ✅ | ✅ | ✅ Compatible |

## Message Format

### Wire Format (Protocol v2)

Both Python Celery and CeleRS use identical wire format:

```json
{
  "headers": {
    "task": "tasks.add",
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "lang": "py",
    "root_id": null,
    "parent_id": null,
    "group": null,
    "retries": 0,
    "eta": null,
    "expires": null
  },
  "properties": {
    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
    "reply_to": null,
    "delivery_mode": 2,
    "priority": null
  },
  "body": "base64-encoded([args, kwargs, embed])",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Body Format (EmbeddedBody)

```json
[
  [1, 2, 3],
  {"key": "value"},
  {
    "callbacks": [],
    "errbacks": [],
    "chain": [],
    "chord": null,
    "group": null,
    "parent_id": null,
    "root_id": null
  }
]
```

## Redis Broker Specifics

### Queue Naming (Kombu Compatible)

```
Default Queue:     celery
Priority Queue:    celery\x06\x16{priority}
Binding Key:       _kombu.binding.celery
```

### Key Formats

```
Task Result:       celery-task-meta-{task-uuid}
Chord Counter:     celery-chord-counter-{group-id}
Chord State:       celery-chord-{group-id}
Unacked Messages:  {queue}:unacked (Sorted Set)
```

### Visibility Timeout

CeleRS implements Kombu-compatible visibility timeout using Lua scripts:

```lua
-- Atomic pop with visibility timeout
local msg = redis.call('BRPOP', KEYS[1], ARGV[1])
if msg then
    redis.call('ZADD', KEYS[2], ARGV[2], msg[2])
    return msg[2]
end
return nil
```

## Migration Guide

### Phase 1: Configure Python Celery

```python
# Enforce JSON serialization
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
)
```

### Phase 2: Deploy CeleRS Workers

CeleRS workers can consume tasks from the same queues as Python workers.

### Phase 3: Gradual Migration

1. Start with low-risk tasks
2. Monitor DLQ for compatibility issues
3. Gradually shift load to CeleRS workers

### Phase 4: Full Migration (Optional)

Replace all Python workers with CeleRS if desired.

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Message rejected | Pickle serialization | Set `task_serializer='json'` |
| Task not found | Task name mismatch | Verify task registration |
| Chord not completing | Redis not configured | Enable Redis backend |
| Priority ignored | SQS broker | Use separate queues |

### Debugging

```python
# Python: Verify message format
from celery import current_app
print(current_app.conf.task_serializer)  # Should be 'json'
print(current_app.conf.accept_content)   # Should include 'json'
```

```rust
// Rust: Enable protocol logging
env_logger::init();
// Set RUST_LOG=celers_protocol=debug
```

## Testing Interoperability

### Test Files

- `crates/celers-protocol/examples/python_interop.rs` - Rust message generation
- `crates/celers-protocol/examples/python_consumer.py` - Python worker
- `crates/celers-protocol/examples/test_interop.sh` - Integration tests

### Running Tests

```bash
# Start Redis
redis-server

# Run Rust examples
cargo run --example python_interop

# Start Python worker
python examples/python_consumer.py worker

# Send test tasks
python examples/python_consumer.py send

# Run full test suite
bash examples/test_interop.sh
```

## References

- [ADR-004: Celery Protocol Compatibility](adr/004-celery-protocol-compatibility.md)
- [ADR-005: SQS Celery Compatibility](adr/005-sqs-celery-compatibility.md)
- [Celery Protocol Documentation](https://docs.celeryq.dev/en/stable/internals/protocol.html)
- [Kombu Redis Transport](https://github.com/celery/kombu/blob/main/kombu/transport/redis.py)
