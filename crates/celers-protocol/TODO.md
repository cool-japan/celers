# celers-protocol TODO

> Celery protocol v2/v5 implementation

## Status: ✅ FEATURE COMPLETE

Full Celery protocol compatibility implemented.

## Completed Features

### Protocol Support ✅
- [x] Celery Protocol v2 (Celery 4.x+)
- [x] Celery Protocol v5 (Celery 5.x+)
- [x] Message headers (task, ID, retries, ETA, expires)
- [x] Message properties (correlation_id, reply_to, delivery_mode, priority)
- [x] Message body (JSON serialization)
- [x] Display implementations (ProtocolVersion, ContentType, ContentEncoding)
- [x] Message utility methods
  - [x] `has_eta()`, `has_expires()` - Check for delayed execution/expiration
  - [x] `has_group()`, `has_parent()`, `has_root()` - Check workflow relationships
  - [x] `is_persistent()` - Check delivery mode
  - [x] `task_id()`, `task_name()` - Convenience getters

### Content Types ✅
- [x] JSON serialization (application/json)
- [x] MessagePack serialization (application/x-msgpack) - optional
- [x] Binary serialization (application/octet-stream) - optional
- [x] Custom content types

### Encoding ✅
- [x] UTF-8 encoding
- [x] Binary encoding
- [x] Base64 encoding for binary data

### Message Features ✅
- [x] Task naming and identification
- [x] Parent/root ID tracking (workflows)
- [x] Group ID (parallel tasks)
- [x] ETA (delayed execution)
- [x] Expiration timestamps
- [x] Retry count tracking
- [x] Priority support

### Serializer Framework ✅ (NEW)
- [x] `Serializer` trait for pluggable serialization
- [x] `JsonSerializer` implementation
- [x] `MessagePackSerializer` implementation (optional)
- [x] `YamlSerializer` implementation (optional `yaml` feature)
- [x] `SerializerType` enum for dynamic dispatch
- [x] `SerializerRegistry` for managing serializers
- [x] Auto-detection by content type

### Result Messages ✅ (NEW)
- [x] `ResultMessage` - Celery-compatible task result format
- [x] `TaskStatus` enum (PENDING, RECEIVED, STARTED, SUCCESS, FAILURE, RETRY, REVOKED)
- [x] `ExceptionInfo` for error tracking
- [x] Result state helpers (is_ready, is_success, is_failure)
- [x] Workflow relationship tracking (parent_id, root_id, children)
- [x] JSON serialization/deserialization

### Event Messages ✅ (NEW)
- [x] `EventType` enum (task-sent, task-succeeded, worker-online, etc.)
- [x] `EventMessage` base event structure
- [x] `TaskEvent` for task lifecycle events
  - [x] task-sent, task-received, task-started
  - [x] task-succeeded, task-failed, task-retried
  - [x] task-revoked, task-rejected
- [x] `WorkerEvent` for worker lifecycle events
  - [x] worker-online, worker-offline
  - [x] worker-heartbeat with stats
- [x] Event timestamps and hostname tracking

### Compression Support ✅ (NEW)
- [x] `CompressionType` enum (None, Gzip, Zstd)
- [x] `Compressor` with configurable levels
- [x] Gzip compression (optional `gzip` feature)
- [x] Zstandard compression (optional `zstd-compression` feature)
- [x] Auto-detection from magic bytes
- [x] `auto_decompress()` helper function

### Embedded Body Format ✅ (NEW)
- [x] `EmbeddedBody` - Protocol v2 [args, kwargs, embed] format
- [x] `EmbedOptions` for workflow callbacks/chains
- [x] `CallbackSignature` for task callbacks
- [x] Support for link/errback callbacks
- [x] Chain and chord support
- [x] Python Celery compatibility verified

### Validation ✅
- [x] Message schema validation
  - [x] MessageHeaders::validate() - Task name, retries, eta/expires
  - [x] MessageProperties::validate() - Delivery mode, priority
  - [x] Message::validate() - Complete message validation
  - [x] Message::validate_with_limit() - Custom size limits
- [x] Content-type validation
- [x] Size limit enforcement

### Protocol Negotiation ✅ (NEW)
- [x] `ProtocolNegotiator` - Version negotiation between parties
- [x] `ProtocolDetection` - Auto-detect protocol from message
- [x] `ProtocolCapabilities` - Feature support per protocol version
- [x] `detect_protocol()` - Detect from JSON value
- [x] `detect_protocol_from_bytes()` - Detect from raw bytes
- [x] `negotiate_protocol()` - Helper for version agreement

### Security ✅ (NEW)
- [x] `ContentTypeWhitelist` - Allow/block content types
  - [x] safe() - JSON, MessagePack (block pickle)
  - [x] strict() - JSON only
  - [x] permissive() - Allow all except blocked
- [x] `SecurityPolicy` - Configurable security settings
  - [x] Content-type validation
  - [x] Message size limits
  - [x] Task name validation
  - [x] Strict mode for additional checks
- [x] `is_unsafe_content_type()` - Detect dangerous formats

### Message Builder ✅ (NEW)
- [x] `MessageBuilder` - Fluent API for message construction
- [x] Task arguments (args, kwargs)
- [x] Priority, queue, routing_key
- [x] ETA and countdown scheduling
- [x] Expiration settings
- [x] Workflow relationships (parent, root, group)
- [x] Callbacks (link, link_error)
- [x] Chain and chord support
- [x] Helper functions (task, delayed_task, scheduled_task)

## Future Enhancements

### Protocol Extensions
- [ ] Celery Protocol v6 (when released)
- [ ] Custom protocol extensions

### Serialization
- [ ] Protobuf support
- [ ] BSON support
- [ ] Encryption support

### Performance
- [ ] Zero-copy deserialization
- [ ] Lazy deserialization
- [ ] Message pooling

## Testing

- [x] Message serialization tests (27 tests)
- [x] Builder pattern tests (22 tests)
- [x] Serializer framework tests (9 tests)
- [x] Result message tests (12 tests)
- [x] Event message tests (16 tests)
- [x] Compression tests (10 tests)
- [x] Embedded body tests (13 tests)
- [x] Protocol negotiation tests (14 tests)
- [x] Security tests (15 tests)
- [x] Protocol v2 compatibility tests (compat.rs)
- [ ] Python Celery interop tests (integration)
- [ ] Fuzzing tests

## Documentation

- [x] Comprehensive README
- [x] Protocol specification
- [x] Python interoperability examples
- [x] Module-level documentation
- [ ] Protocol migration guide
- [ ] Wire format documentation

## Security

- [ ] Message signature verification
- [ ] Message encryption
- [x] Content-type whitelist ✅
- [x] Size limits (10MB default, configurable)
- [x] Task name validation ✅
- [x] Pickle/unsafe format blocking ✅

## Dependencies

- `serde` - Serialization
- `serde_json` - JSON support
- `chrono` - Timestamps
- `uuid` - Task IDs
- `base64` - Binary encoding
- `rmp-serde` - MessagePack (optional)
- `serde_yaml` - YAML (optional)
- `flate2` - Gzip compression (optional)
- `zstd` - Zstandard compression (optional)

## Notes

- 100% wire-format compatible with Python Celery
- Pickle serialization NOT supported (security risk)
- All timestamps use UTC
- UUIDs are v4 (random)
- 140 unit tests, all passing
- 6 doc tests, all passing
- 0 warnings, 0 clippy warnings
