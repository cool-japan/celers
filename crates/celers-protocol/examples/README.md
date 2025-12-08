# celers-protocol Examples

Comprehensive examples demonstrating Python Celery interoperability and protocol features.

## Running Examples

```bash
# Basic Python interop
cargo run --example python_interop

# Message validation
cargo run --example message_validation

# Advanced features (with all features)
cargo run --example advanced_features --all-features

# Performance optimization
cargo run --example performance --release

# Serialization formats comparison
cargo run --example serialization_formats --all-features
```

## Automation Scripts

```bash
# Run all benchmarks
./examples/run_benchmarks.sh

# Run integration tests (requires Redis and Python/Celery)
./examples/test_interop.sh
```

## Examples

### 1. python_interop.rs

Demonstrates creating Celery-compatible messages from Rust:

- Simple tasks with args/kwargs
- Delayed execution (ETA/countdown)
- Task expiration
- Task chains (callbacks)
- Error handlers
- Priority tasks
- Workflow tracking (parent/root/group)
- Manual message creation

**Run:**
```bash
cargo run --example python_interop
```

### 2. python_consumer.py

Python Celery worker that consumes Rust-generated messages:

- Registers all task handlers
- Demonstrates wire-format compatibility
- Includes helper to send test tasks
- Message format verification

**Prerequisites:**
```bash
pip install celery redis
redis-server
```

**Run worker:**
```bash
python examples/python_consumer.py worker
```

**Send test tasks:**
```bash
python examples/python_consumer.py send
```

**Verify format:**
```bash
python examples/python_consumer.py verify
```

### 3. message_validation.rs

Message validation, security policies, and content-type filtering:

- Basic message validation
- Invalid message examples
- Content-type whitelists (safe/strict/permissive)
- Security policies
- Size limit enforcement
- ETA/expiration validation
- Retry limit checking

**Run:**
```bash
cargo run --example message_validation
```

### 4. advanced_features.rs

Advanced protocol features:

- Protocol version detection
- Protocol negotiation
- Task result messages (success/failure)
- Workflow result tracking
- Task status checking
- Custom headers
- Compression (optional)
- Message signing (optional)
- Message encryption (optional)

**Run with all features:**
```bash
cargo run --example advanced_features --all-features
```

**Run with specific features:**
```bash
cargo run --example advanced_features --features crypto,compression
```

### 5. performance.rs

Performance optimization techniques:

- Standard deserialization baseline
- Zero-copy deserialization (MessageRef)
- Lazy deserialization (LazyMessage)
- Message pooling (MessagePool)
- TaskArgs pooling (TaskArgsPool)
- Benchmark comparisons
- Best practices

**Run (use --release for accurate benchmarks):**
```bash
cargo run --example performance --release
```

### 6. serialization_formats.rs

Serialization format comparison and examples:

- JSON serialization (default)
- MessagePack serialization (optional)
- BSON serialization (optional)
- YAML serialization (optional)
- Format size comparison
- Feature flag requirements
- Celery compatibility notes

**Run with all formats:**
```bash
cargo run --example serialization_formats --all-features
```

**Run with specific features:**
```bash
cargo run --example serialization_formats --features msgpack,bson-format
```

## Automation Scripts

### run_benchmarks.sh

Automated benchmark execution script:

- Runs all Criterion benchmarks
- Generates HTML reports
- Extracts performance summaries
- Compares optimization techniques

**Usage:**
```bash
./examples/run_benchmarks.sh
```

**View detailed results:**
```bash
open target/criterion/report/index.html
```

### test_interop.sh

Integration test automation script:

- Checks prerequisites (Redis, Python, Celery)
- Runs all Rust examples
- Validates Python consumer
- Executes unit tests
- Checks for warnings

**Usage:**
```bash
./examples/test_interop.sh
```

**Requirements:**
- Redis server running
- Python 3 with `celery` and `redis` installed

## Python Celery Interoperability

The Rust implementation is 100% wire-format compatible with Python Celery.

### Message Flow

```
Rust Producer → RabbitMQ/Redis → Python Celery Worker
Python Producer → RabbitMQ/Redis → Rust Consumer
```

### Protocol Versions

- **Protocol v2**: Celery 4.x+ (default)
- **Protocol v5**: Celery 5.x+

Both versions are fully supported.

### Message Format

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
  "body": "W1sxLDJdLHt9XQ==",
  "content-type": "application/json",
  "content-encoding": "utf-8"
}
```

### Content Types

- `application/json` - JSON (default, recommended)
- `application/x-msgpack` - MessagePack (optional)
- `application/bson` - BSON (optional)
- `application/protobuf` - Protobuf (optional)

**Security Note:** Pickle (`application/x-python-serialize`) is NOT supported due to security risks.

## Integration Testing

### Setup

1. **Start Redis:**
   ```bash
   redis-server
   ```

2. **Start Python Celery worker:**
   ```bash
   python examples/python_consumer.py worker
   ```

3. **Run Rust examples:**
   ```bash
   cargo run --example python_interop
   ```

4. **Send tasks from Python:**
   ```bash
   python examples/python_consumer.py send
   ```

### Verification

The Python worker should successfully process tasks sent from Rust, and vice versa.

Expected output:
```
[tasks.add] 2 + 3 = 5
[tasks.multiply] 10 * 20 = 200
[tasks.cleanup] Running cleanup...
```

## Features

### Optional Features

Enable features in `Cargo.toml`:

```toml
[dependencies]
celers-protocol = { version = "0.1", features = ["crypto", "compression"] }
```

Available features:
- `json` - JSON serialization (default)
- `msgpack` - MessagePack serialization
- `yaml` - YAML serialization
- `bson-format` - BSON serialization
- `protobuf` - Protobuf serialization
- `gzip` - Gzip compression
- `zstd-compression` - Zstandard compression
- `compression` - All compression formats
- `signing` - HMAC-SHA256 message signing
- `encryption` - AES-256-GCM encryption
- `crypto` - All cryptographic features
- `all-serializers` - All serialization formats

## Security

### Content-Type Whitelists

```rust
// Safe mode: JSON + MessagePack (blocks pickle)
let whitelist = ContentTypeWhitelist::safe();

// Strict mode: JSON only
let whitelist = ContentTypeWhitelist::strict();

// Permissive mode: Allow all except blocked
let whitelist = ContentTypeWhitelist::permissive()
    .block("application/x-python-serialize");
```

### Message Signing

```rust
use celers_protocol::auth::MessageSigner;

let signer = MessageSigner::new(b"secret-key");
let signature = signer.sign_hex(message_data);
let valid = signer.verify_hex(message_data, &signature);
```

### Message Encryption

```rust
use celers_protocol::crypto::MessageEncryptor;

let key = b"32-byte-key-for-aes-256-gcm!!!!";
let encryptor = MessageEncryptor::new(key);

let encrypted = encryptor.encrypt_hex(plaintext)?;
let decrypted = encryptor.decrypt_hex(&encrypted)?;
```

## Performance Tips

1. **Use zero-copy when possible:**
   ```rust
   let msg: MessageRef = serde_json::from_slice(&data)?;
   ```

2. **Use lazy loading for large messages:**
   ```rust
   let msg = LazyMessage::from_json(&data)?;
   let task_name = msg.task_name(); // No body parsing
   ```

3. **Use pooling for high throughput:**
   ```rust
   let pool = MessagePool::new();
   let msg = pool.acquire(); // Reuses allocations
   ```

4. **Access body only when needed:**
   ```rust
   let msg = LazyMessage::from_json(&data)?;
   if msg.task_name() == "important" {
       let body = msg.body()?; // Parse only if needed
   }
   ```

## Troubleshooting

### Redis Connection Error

Ensure Redis is running:
```bash
redis-server
```

### Python Import Error

Install dependencies:
```bash
pip install celery redis
```

### Protocol Version Mismatch

Both Rust and Python must use the same protocol version:
- Rust: Default is v2 (Celery 4.x+)
- Python: Set `task_protocol=2` in Celery config

### Pickle Security Warning

Pickle is disabled by default for security. Use JSON instead.

## Further Reading

- [Celery Protocol Documentation](http://docs.celeryproject.org/en/latest/internals/protocol.html)
- [Celery Message Format](https://docs.celeryproject.org/en/stable/internals/protocol.html#message-format)
- [AMQP 0-9-1 Specification](https://www.rabbitmq.com/amqp-0-9-1-reference.html)

## License

MIT OR Apache-2.0
