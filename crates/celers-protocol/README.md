# celers-protocol

Celery protocol v2/v5 implementation for CeleRS. Ensures wire-level compatibility with Python Celery workers and brokers.

## Overview

Production-ready protocol implementation with:

- ✅ **Celery Protocol v2**: Compatible with Celery 4.x+
- ✅ **Celery Protocol v5**: Compatible with Celery 5.x+
- ✅ **JSON Serialization**: Default, universally compatible
- ✅ **MessagePack**: Optional high-performance binary format
- ✅ **AMQP Properties**: Correlation ID, reply-to, delivery mode
- ✅ **Workflow Headers**: Parent ID, root ID, group ID
- ✅ **Base64 Encoding**: Binary-safe message bodies
- ✅ **Full Metadata**: ETA, expiration, retries, priority

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
celers-protocol = { version = "0.1", features = ["msgpack"] }
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
celers-protocol = { version = "0.1", features = ["binary"] }
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

**Send from Rust, receive in Python:**

```rust
// Rust: Send task
let message = Message::new("python_task".to_string(), task_id, body);
broker.enqueue(message).await?;
```

```python
# Python: Receive and execute
from celery import Celery

app = Celery('myapp', broker='redis://localhost:6379')

@app.task(name='python_task')
def python_task(arg1, arg2):
    return arg1 + arg2
```

**Send from Python, receive in Rust:**

```python
# Python: Send task
from celery import Celery

app = Celery('myapp', broker='redis://localhost:6379')
app.send_task('rust_task', args=[1, 2])
```

```rust
// Rust: Receive and execute
use celers_core::TaskRegistry;

let mut registry = TaskRegistry::new();
registry.register("rust_task", |args: Vec<i32>| async move {
    Ok(args[0] + args[1])
});
```

### Wire Format Compatibility

CeleRS messages are 100% compatible with Celery wire format:

| Component | CeleRS | Celery | Compatible? |
|-----------|--------|--------|-------------|
| Headers | ✅ | ✅ | ✅ Yes |
| Properties | ✅ | ✅ | ✅ Yes |
| Body format | JSON/MessagePack | JSON/MessagePack/Pickle | ✅ Yes* |
| UUIDs | ✅ | ✅ | ✅ Yes |
| Timestamps | ISO8601 | ISO8601 | ✅ Yes |

*Pickle not supported in CeleRS (security reasons)

## Message Examples

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

MIT OR Apache-2.0
