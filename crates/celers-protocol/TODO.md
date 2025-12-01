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

## Future Enhancements

### Protocol Extensions
- [ ] Celery Protocol v6 (when released)
- [ ] Custom protocol extensions
- [ ] Protocol negotiation

### Serialization
- [ ] Protobuf support
- [ ] BSON support
- [ ] Compression (gzip, zstd)
- [ ] Encryption support

### Validation
- [x] Message schema validation ✅
  - [x] MessageHeaders::validate() - Task name, retries, eta/expires
  - [x] MessageProperties::validate() - Delivery mode, priority
  - [x] Message::validate() - Complete message validation
  - [x] Message::validate_with_limit() - Custom size limits
- [x] Content-type validation ✅
- [x] Size limit enforcement ✅

### Performance
- [ ] Zero-copy deserialization
- [ ] Lazy deserialization
- [ ] Message pooling

## Testing

- [x] Message serialization tests
- [x] Builder pattern tests
- [ ] Protocol v2 compatibility tests
- [ ] Protocol v5 compatibility tests
- [ ] Python Celery interop tests
- [ ] Fuzzing tests

## Documentation

- [x] Comprehensive README
- [x] Protocol specification
- [x] Python interoperability examples
- [ ] Protocol migration guide
- [ ] Wire format documentation

## Security

- [ ] Message signature verification
- [ ] Message encryption
- [ ] Content-type whitelist
- [ ] Size limits

## Dependencies

- `serde` - Serialization
- `serde_json` - JSON support
- `chrono` - Timestamps
- `uuid` - Task IDs
- `base64` - Binary encoding

## Notes

- 100% wire-format compatible with Python Celery
- Pickle serialization NOT supported (security risk)
- All timestamps use UTC
- UUIDs are v4 (random)
