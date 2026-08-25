# celers-protocol

Celery protocol implementation for CeleRS: the v2 wire format Python Celery actually speaks, plus a
CeleRS-internal "v5" version label for CeleRS-to-CeleRS use. This is the crate whose compatibility is
*proved* — see [Wire Format Compatibility](#wire-format-compatibility).

**Status: [Stable] — v0.3.1 (2026-08-26) — 608 tests + 27 doctests**

## Overview

Production-ready protocol implementation with:

- ✅ **Celery Protocol v2**: the format Celery 4.x/5.x puts on the wire — **interop-verified against a
  live Celery 5.6.3** (`tests/python_interop.rs`) and against verbatim captures (`tests/celery_golden.rs`)
- 🟢 **"Protocol v5"**: a **CeleRS-internal** version label, not something proved to exist on the
  Celery wire. Nothing in the interop suite or the recorded captures exercises it — the suite pins
  `task_protocol = 2` and every fixture was captured from Celery 5.6.3 at protocol 2. Use it between
  CeleRS peers
- ✅ **JSON Serialization**: Default, universally compatible
- ✅ **MessagePack**: Optional high-performance binary format
- ✅ **BSON Serialization**: Optional `bson-format` feature
- ✅ **YAML Serialization**: Optional `yaml` feature
- ✅ **Custom Serializers**: Runtime-registrable `CustomSerializer` trait + `CustomSerializerRegistry` for user-defined formats
- ✅ **HMAC Signing**: HMAC-SHA256 message authentication
- ✅ **AES-256-GCM Encryption**: Authenticated message encryption
- ✅ **AMQP Properties**: Correlation ID, reply-to, delivery mode
- ✅ **Workflow Headers**: Parent ID, root ID, group ID
- ✅ **Base64 Encoding**: Binary-safe message bodies
- ✅ **Full Metadata**: ETA, expiration, retries, priority
- ✅ **Message Timestamps**: Creation-time tracking (`MessageHeaders.created_at`) with `Message::created_at()` / `MessageExt::get_age_seconds()` age helpers
- ✅ **Protocol Negotiation**: Auto-detect protocol from message
- ✅ **Version Negotiation**: Mutual-version agreement (`negotiate_version`) and a native Celery v5 wire-format builder (`build_v5_message` / `to_v5_wire`)
- ✅ **Protocol Migration**: Real v2↔v5 migration — version stamping, AMQP priority mirroring on upgrade, non-destructive legacy-field mirroring on downgrade
- ✅ **Compression**: Gzip, Zstd, Zlib with auto-detection
- ✅ **Message Builder**: Fluent API for message construction
- ✅ **Result Messages**: Celery-compatible task result format
- ✅ **Event Messages**: Task and worker lifecycle events

## Quick Start

```rust
use celers_protocol::{Message, MessageHeaders, ContentType};
use uuid::Uuid;

// Create a simple task message
let task_id = Uuid::new_v4();
let body = serde_json::to_vec(&serde_json::json!({
    "args": [1, 2],
    "kwargs": {}
})).unwrap();

let message = Message::new("tasks.add".to_string(), task_id, body);

// Serialize to JSON for transport
let serialized = serde_json::to_string(&message).unwrap();
```

## Protocol Structure

### Complete Message Format

```rust
pub struct Message {
    /// Message headers (task metadata)
    pub headers: MessageHeaders,

    /// Message properties (AMQP-like)
    pub properties: MessageProperties,

    /// Serialized body (task arguments)
    pub body: Vec<u8>,

    /// Content type ("application/json", "application/x-msgpack")
    pub content_type: String,

    /// Content encoding ("utf-8", "binary")
    pub content_encoding: String,
}
```

**JSON representation:**
```json
{
  "headers": {
    "task": "tasks.add",
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "lang": "rust",
    "retries": 3
  },
  "properties": {
    "delivery_mode": 2,
    "priority": 5
  },
  "body": "eyJhcmdzIjogWzEsIDJdLCAia3dhcmdzIjoge319",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Message Headers

```rust
pub struct MessageHeaders {
    /// Task name (e.g., "tasks.add")
    pub task: String,

    /// Task ID (UUID)
    pub id: Uuid,

    /// Programming language ("rust", "py")
    pub lang: String,

    /// Root task ID (for workflow tracking)
    pub root_id: Option<Uuid>,

    /// Parent task ID (for nested tasks)
    pub parent_id: Option<Uuid>,

    /// Group ID (for grouped tasks)
    pub group: Option<Uuid>,

    /// Maximum retries
    pub retries: Option<u32>,

    /// ETA (Estimated Time of Arrival) for delayed tasks
    pub eta: Option<DateTime<Utc>>,

    /// Task expiration timestamp
    pub expires: Option<DateTime<Utc>>,

    /// Message creation timestamp (set automatically, used for age tracking)
    pub created_at: Option<DateTime<Utc>>,

    /// Additional custom headers
    pub extra: HashMap<String, serde_json::Value>,
}
```

### Message Properties

```rust
pub struct MessageProperties {
    /// Correlation ID for RPC-style calls
    pub correlation_id: Option<String>,

    /// Reply-to queue for results
    pub reply_to: Option<String>,

    /// Delivery mode (1 = non-persistent, 2 = persistent)
    pub delivery_mode: u8,

    /// Priority (0-9, higher = more priority)
    pub priority: Option<u8>,
}
```

## Creating Messages

### Basic Task

```rust
use celers_protocol::Message;
use uuid::Uuid;

let task_id = Uuid::new_v4();
let body = serde_json::to_vec(&serde_json::json!({
    "args": ["hello", "world"],
    "kwargs": {}
})).unwrap();

let message = Message::new("tasks.greet".to_string(), task_id, body);
```

### With Priority

```rust
let message = Message::new("urgent_task".to_string(), task_id, body)
    .with_priority(9);  // Highest priority
```

**Priority levels:**
- 0-3: Low priority
- 4-6: Normal priority
- 7-9: High priority

### With Parent/Root ID (Workflows)

```rust
let parent_id = Uuid::new_v4();
let root_id = Uuid::new_v4();

let message = Message::new("child_task".to_string(), task_id, body)
    .with_parent(parent_id)
    .with_root(root_id);
```

**Use cases:**
- Chain workflows (parent → child)
- Workflow tracking (all tasks share root_id)
- Result aggregation by root_id

### With Group ID

```rust
let group_id = Uuid::new_v4();

let message = Message::new("parallel_task".to_string(), task_id, body)
    .with_group(group_id);
```

**Use cases:**
- Group/Chord workflows
- Parallel task tracking
- Bulk operations

### With ETA (Delayed Execution)

```rust
use chrono::{Duration, Utc};

// Execute in 1 hour
let eta = Utc::now() + Duration::hours(1);
let message = Message::new("delayed_task".to_string(), task_id, body)
    .with_eta(eta);
```

### With Expiration

```rust
use chrono::{Duration, Utc};

// Task expires in 5 minutes
let expires = Utc::now() + Duration::minutes(5);
let message = Message::new("time_sensitive".to_string(), task_id, body)
    .with_expires(expires);
```

## Task Arguments

### Standard Format

```rust
use celers_protocol::TaskArgs;

let args = TaskArgs {
    args: vec![
        serde_json::json!(10),
        serde_json::json!(20),
    ],
    kwargs: HashMap::from([
        ("timeout".to_string(), serde_json::json!(300)),
        ("retries".to_string(), serde_json::json!(3)),
    ]),
};

let body = serde_json::to_vec(&args).unwrap();
let message = Message::new("tasks.add".to_string(), task_id, body);
```

**JSON representation:**
```json
{
  "args": [10, 20],
  "kwargs": {
    "timeout": 300,
    "retries": 3
  }
}
```

### Complex Arguments

```rust
let args = TaskArgs {
    args: vec![
        serde_json::json!({
            "user_id": 123,
            "data": [1, 2, 3]
        }),
    ],
    kwargs: HashMap::from([
        ("options".to_string(), serde_json::json!({
            "format": "json",
            "compress": true
        })),
    ]),
};
```

## Protocol Versions

### Version 2 (Default)

```rust
use celers_protocol::ProtocolVersion;

let version = ProtocolVersion::V2;  // Celery 4.x+
```

**Features:**
- JSON/MessagePack serialization
- Basic workflow support
- AMQP-style properties
- Task metadata

**Compatible with:**
- Celery 4.0+
- Celery 5.0+ (backward compatible)

### Version 5

```rust
let version = ProtocolVersion::V5;  // Celery 5.x+
```

**Additional features:**
- Extended workflow metadata
- Improved error handling
- Enhanced tracing
- Native wire-format builder (`celers_protocol::v5::build_v5_message` / `to_v5_wire`) and mutual version negotiation (`negotiate_version`)

## Content Types

### JSON (Default)

```rust
use celers_protocol::ContentType;

let content_type = ContentType::Json;
assert_eq!(content_type.as_str(), "application/json");
```

**Pros:**
- Human-readable
- Universally supported
- Easy debugging

**Cons:**
- Larger message size
- Slower serialization

### MessagePack (Optional)

```toml
[dependencies]
celers-protocol = { version = "0.3", features = ["msgpack"] }
```

```rust
use celers_protocol::ContentType;

let content_type = ContentType::MessagePack;
assert_eq!(content_type.as_str(), "application/x-msgpack");
```

**Pros:**
- Compact binary format
- Fast serialization
- Smaller message size

**Cons:**
- Not human-readable
- Requires msgpack feature

### Binary (Custom)

```toml
[dependencies]
celers-protocol = { version = "0.3", features = ["binary"] }
```

```rust
let content_type = ContentType::Binary;
assert_eq!(content_type.as_str(), "application/octet-stream");
```

### Custom Content Type

```rust
let content_type = ContentType::Custom("application/protobuf".to_string());
```

## Serialization

### Message Serialization

```rust
use celers_protocol::Message;

let message = Message::new("task".to_string(), task_id, body);

// To JSON
let json = serde_json::to_string(&message)?;

// To bytes (for broker)
let bytes = serde_json::to_vec(&message)?;
```

### Message Deserialization

```rust
// From JSON string
let message: Message = serde_json::from_str(&json)?;

// From bytes
let message: Message = serde_json::from_slice(&bytes)?;
```

### Base64 Encoding

Message bodies are automatically base64-encoded when serializing to JSON:

```rust
let body = vec![0xFF, 0xFE, 0xFD];  // Binary data
let message = Message::new("task".to_string(), task_id, body);

let json = serde_json::to_string(&message)?;
// body field in JSON: "//79" (base64)
```

## Celery Compatibility

### Python Celery Interoperability

> **Scope.** Interoperability lives at *this* layer. `celers-broker-redis` does **not** put a Celery
> envelope on the queue — it enqueues its own `SerializedTask` — so you cannot point a
> `celers_worker::Worker` at a Celery queue and you cannot use `Broker::enqueue` to reach a Celery
> worker. What works today is producing and consuming Celery envelopes with this crate and doing the
> transport yourself. See
> [docs/CELERY_COMPATIBILITY.md](../../docs/CELERY_COMPATIBILITY.md).

**Send from Rust, execute in Python.** Build the canonical envelope and put it on the queue with a
Redis client. Use `create_python_celery_message_on_queue` when the queue is not `celery`: the
`routing_key` is load-bearing, because a Celery worker re-publishes `task.retry()` through it.

```rust
use celers_protocol::compat::create_python_celery_message_on_queue;
use serde_json::json;
use uuid::Uuid;

let envelope = create_python_celery_message_on_queue(
    "tasks.add",
    Uuid::new_v4(),
    vec![json!(4), json!(5)],
    json!({}),
    "celery",
)?;
// `envelope` is the full kombu envelope (headers / properties / base64 body).
// LPUSH serde_json::to_string(&envelope)? onto the queue with your Redis client.
```

```python
# Python: a plain, unmodified Celery worker executes it
from celery import Celery

app = Celery('myapp', broker='redis://localhost:6379')

@app.task(name='tasks.add')
def add(x, y):
    return x + y
```

`MessageBuilder` is the ergonomic producer, and its envelope is deliverable as it stands: it
serializes `properties.delivery_tag` (a fresh one per message, as kombu's producer mints it) and
`properties.delivery_info` (`{exchange, routing_key}`, the routing key naming the queue
`.queue(...)` / `.routing_key(...)` chose), which kombu indexes without a default. The interop
suite publishes that envelope **unpatched** to a real worker. `compat::create_python_celery_message`
remains the deterministic *fixture* builder — reproducible bytes, so it pins a nil `delivery_tag`
rather than a unique one; publish real work through `MessageBuilder`.

**Send from Python, parse in Rust.** Pop the envelope off the queue yourself and hand the bytes to
this crate:

```rust
use celers_protocol::Message;

// `raw` is one JSON document popped from the Celery queue.
let message: Message = serde_json::from_slice(raw)?;
assert_eq!(message.headers.task, "rust_task");
// `message.body` is already base64-decoded: it holds the raw
// `[args, kwargs, embed]` JSON tuple, ready for serde_json::from_slice.
```

```python
# Python: send it
from celery import Celery

app = Celery('myapp', broker='redis://localhost:6379')
app.send_task('rust_task', args=[1, 2])
```

Both directions are exercised for real — against a live `celery` worker and a live Redis — by
[`tests/python-compat/`](../../tests/python-compat/), driven from Rust by
`cargo test -p celers-protocol --test python_interop`.

### Wire Format Compatibility

This crate's protocol-v2 wire format is **interop-verified against a live Python Celery 5.6.3** —
`tests/python_interop.rs` drives the suite in `tests/python-compat/`, and `tests/celery_golden.rs`
checks this crate against verbatim Celery captures in `tests/fixtures/` with no services required.

| Component | CeleRS | Celery | Status |
|---|---|---|---|
| Headers (`task`, `id`, `lang`, `eta`, `expires`, `group`, `root_id`, `parent_id`, …) | ✅ | ✅ | ✅ Interop-verified |
| `argsrepr` / `kwargsrepr` (Python literals, `saferepr` elision) | ✅ | ✅ | ✅ Interop-verified |
| Body framing (base64 `[args, kwargs, embed]`) | ✅ | ✅ | ✅ Interop-verified |
| Result records (`status` / `result` / `traceback` / `date_done`) | ✅ | ✅ | ✅ Interop-verified |
| Serialization | JSON, MessagePack, YAML | JSON, MessagePack, YAML, Pickle | ✅ JSON interop-verified; the others Rust-tested only |
| `properties.delivery_tag` / `delivery_info` | ✅ | required | ✅ Interop-verified — every producer path emits both (kombu indexes them without a default, so a missing one kills the consumer loop, not just the message) |
| `ResultMessage::children` | `ResultChild` trees | nested result tuples | ✅ Interop-verified — `[[id, parent], group_results]`, the shape `AsyncResult.as_tuple()` renders; legacy id lists and `children: null` still parse |
| Exception `exc_message` (Python's `exc.args`) | `Vec<Value>` | list | ✅ Interop-verified — a multi-argument exception keeps its argument boundaries through Celery's `exception_to_python` |
| Pickle | ⛔ | ✅ | ⛔ Deliberately absent (arbitrary-code-execution risk) |
| "Protocol v5" | ✅ | — | 🟢 A **CeleRS-internal** version label; nothing in the interop suite or the captures exercises it |

The full, workspace-wide picture — including which layers are *not* interoperable — is in
[docs/CELERY_COMPATIBILITY.md](../../docs/CELERY_COMPATIBILITY.md).

## Message Examples

The `properties` blocks below are **abridged** — they show the fields each example is about. A
message this crate serializes always also carries kombu's `body_encoding`, `delivery_tag` and
`delivery_info`; a consumer indexes the last two without a default, so do not treat these snippets
as a template for a hand-built envelope. `compat::verify_message_format` checks for all of them.

### Simple Task

```json
{
  "headers": {
    "task": "tasks.add",
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "lang": "rust"
  },
  "properties": {
    "delivery_mode": 2
  },
  "body": "eyJhcmdzIjogWzEsIDJdLCAia3dhcmdzIjoge319",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Priority Task

```json
{
  "headers": {
    "task": "urgent_task",
    "id": "...",
    "lang": "rust",
    "retries": 3
  },
  "properties": {
    "delivery_mode": 2,
    "priority": 9
  },
  "body": "...",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Workflow Task (Chain)

```json
{
  "headers": {
    "task": "child_task",
    "id": "...",
    "lang": "rust",
    "parent_id": "parent-uuid",
    "root_id": "root-uuid"
  },
  "properties": {
    "delivery_mode": 2
  },
  "body": "...",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Group Task

```json
{
  "headers": {
    "task": "parallel_task",
    "id": "...",
    "lang": "rust",
    "group": "group-uuid"
  },
  "properties": {
    "delivery_mode": 2
  },
  "body": "...",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Delayed Task (ETA)

```json
{
  "headers": {
    "task": "delayed_task",
    "id": "...",
    "lang": "rust",
    "eta": "2023-12-31T23:59:59Z"
  },
  "properties": {
    "delivery_mode": 2
  },
  "body": "...",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

## Best Practices

### 1. Always Set Task ID

```rust
// Good: Unique ID
let task_id = Uuid::new_v4();
let message = Message::new("task".to_string(), task_id, body);

// Bad: Reused ID (don't do this)
// let message = Message::new("task".to_string(), old_id, body);
```

### 2. Use Priority Sparingly

```rust
// Good: Reserve high priority for urgent tasks
let message = Message::new("critical_alert".to_string(), task_id, body)
    .with_priority(9);

// Bad: Everything is high priority (defeats the purpose)
// let message = Message::new("regular_task".to_string(), task_id, body)
//     .with_priority(9);
```

### 3. Set Expiration for Time-Sensitive Tasks

```rust
use chrono::{Duration, Utc};

// Task only relevant for 5 minutes
let expires = Utc::now() + Duration::minutes(5);
let message = Message::new("realtime_task".to_string(), task_id, body)
    .with_expires(expires);
```

### 4. Use Workflows for Related Tasks

```rust
// Parent task
let parent_id = Uuid::new_v4();
let root_id = parent_id;  // Root is the first task

let parent_msg = Message::new("parent".to_string(), parent_id, body1)
    .with_root(root_id);

// Child task
let child_id = Uuid::new_v4();
let child_msg = Message::new("child".to_string(), child_id, body2)
    .with_parent(parent_id)
    .with_root(root_id);
```

### 5. Choose Appropriate Content Type

```rust
// Small messages: JSON is fine
let json_msg = Message::new("small_task".to_string(), task_id, small_body);

// Large messages or high throughput: Use MessagePack
#[cfg(feature = "msgpack")]
let msgpack_msg = {
    let mut msg = Message::new("large_task".to_string(), task_id, large_body);
    msg.content_type = ContentType::MessagePack.as_str().to_string();
    msg
};
```

## Troubleshooting

### Messages not received by Python workers

**Cause:** Content type mismatch
**Solution:** Ensure `content-type` is `"application/json"` or `"application/x-msgpack"`

### Binary data corruption

**Cause:** Missing base64 encoding
**Solution:** Body is automatically base64-encoded when serializing to JSON

### Priority not working

**Cause:** Broker doesn't support priorities
**Solution:** Use Redis with sorted sets or RabbitMQ with priority queues

### ETA tasks executing immediately

**Cause:** Worker doesn't check ETA
**Solution:** Use `celers-worker` or Celery worker with ETA support

## Performance

### Message Size

| Content Type | Overhead | Typical Size |
|--------------|----------|--------------|
| JSON | ~30% | 200-500B + body |
| MessagePack | ~10% | 150-300B + body |
| Binary | Minimal | 100-200B + body |

### Serialization Speed

| Format | Serialize | Deserialize |
|--------|-----------|-------------|
| JSON | ~100K msg/sec | ~100K msg/sec |
| MessagePack | ~200K msg/sec | ~200K msg/sec |

**Recommendation:** Use MessagePack for high-throughput systems.

## Security Considerations

### No Pickle Support

Unlike Python Celery, CeleRS does **not** support Pickle serialization:

```python
# Python (INSECURE - don't use)
app.conf.task_serializer = 'pickle'  # ❌ Arbitrary code execution

# CeleRS (SECURE)
# Only JSON and MessagePack supported  # ✅ Safe
```

**Why:** Pickle allows arbitrary code execution, making it a security risk.

### Content-Type Validation

Always validate content-type before deserializing:

```rust
match message.content_type.as_str() {
    "application/json" => {
        // Safe to deserialize JSON
        let args: TaskArgs = serde_json::from_slice(&message.body)?;
    }
    _ => {
        return Err("Unsupported content type");
    }
}
```

## Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let message = Message::new("test".to_string(), task_id, body);

        assert_eq!(message.headers.task, "test");
        assert_eq!(message.headers.id, task_id);
        assert_eq!(message.headers.lang, "rust");
    }

    #[test]
    fn test_message_serialization() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&serde_json::json!({
            "args": [1, 2],
            "kwargs": {}
        })).unwrap();

        let message = Message::new("task".to_string(), task_id, body);
        let json = serde_json::to_string(&message).unwrap();

        assert!(json.contains("\"task\":\"task\""));
        assert!(json.contains("\"lang\":\"rust\""));
    }

    #[test]
    fn test_builder_pattern() {
        let message = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_priority(9)
            .with_group(Uuid::new_v4());

        assert_eq!(message.properties.priority, Some(9));
        assert!(message.headers.group.is_some());
    }
}
```

## See Also

- **Core**: `celers-core` - Task execution and registry
- **Broker**: `celers-broker-redis` - Redis broker implementation
- **Worker**: `celers-worker` - Worker runtime

## License

Apache-2.0
