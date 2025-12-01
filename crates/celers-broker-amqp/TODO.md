# celers-broker-amqp TODO

> RabbitMQ/AMQP broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

Full-featured AMQP broker with exchange/queue topology, message confirmation, priority support, DLX, TTL, and connection retry.

## Completed Features

### Connection Management ✅
- [x] Connection to RabbitMQ via AMQP
- [x] Channel creation and management
- [x] Connection status checking
- [x] Graceful disconnect
- [x] Connection retry with configurable attempts/delay
- [x] Virtual host support

### Message Publishing ✅
- [x] Basic message publishing to queues
- [x] Publishing with exchange/routing_key
- [x] Message persistence (delivery_mode = 2)
- [x] Publisher confirms (automatic)
- [x] Priority support via message properties
- [x] Correlation ID support
- [x] Message TTL support (`publish_with_ttl()`)

### Message Consumption ✅
- [x] Polling consumption via basic_get
- [x] Manual acknowledgment (ACK)
- [x] Message rejection with requeue
- [x] Delivery tag tracking
- [x] Redelivery flag support
- [x] QoS (prefetch) configuration

### Queue Management ✅
- [x] Queue declaration with options
- [x] Durable queues
- [x] Priority queue support (x-max-priority)
- [x] Queue purging
- [x] Queue deletion
- [x] Queue size inspection
- [x] Queue TTL (x-message-ttl)
- [x] Queue expiration (x-expires)
- [x] Max queue length (x-max-length)
- [x] Dead Letter Exchange configuration

### Exchange/Topology ✅
- [x] Exchange declaration (Direct, Fanout, Topic, Headers)
- [x] Queue binding to exchange
- [x] Queue unbinding from exchange
- [x] Default "celery" exchange setup
- [x] Routing key support
- [x] Exchange deletion
- [x] Dead Letter Exchange (DLX) setup

### Batch Operations ✅
- [x] `publish_batch()` - Publish multiple messages efficiently
- [x] Publisher confirms collected after all publishes
- [x] Reduces round-trips compared to individual publishes
- [x] Returns successful publish count

### Configuration ✅
- [x] `AmqpConfig` - Broker-level configuration
- [x] `QueueConfig` - Queue-level configuration
- [x] `DlxConfig` - Dead Letter Exchange configuration
- [x] `AmqpExchangeType` - Exchange type enum (Direct, Fanout, Topic, Headers)

### Consumer Streaming ✅
- [x] `start_consumer()` - Start a streaming consumer (basic_consume)
- [x] `cancel_consumer()` - Cancel a consumer by tag
- [x] Returns `lapin::Consumer` for async stream processing

### Health Monitoring ✅
- [x] `health_status()` - Get detailed health status
- [x] `is_healthy()` - Quick health check
- [x] `HealthStatus` struct with connection/channel state

### Transaction Support ✅
- [x] `start_transaction()` - Begin AMQP transaction
- [x] `commit_transaction()` - Commit transaction
- [x] `rollback_transaction()` - Rollback transaction
- [x] `transaction_state()` - Get current transaction state

## Future Enhancements

### Advanced Features
- [ ] Automatic reconnection on connection loss (during operation)

### Performance
- [x] Batch publishing ✅
- [ ] Connection pooling
- [ ] Channel pooling
- [ ] Pipeline publishing

### Monitoring
- [x] Connection health monitoring ✅
- [ ] Channel-level metrics
- [ ] Publisher confirm tracking
- [ ] RabbitMQ Management API integration

### Reliability
- [x] Transaction support ✅
- [ ] Message deduplication

## Testing Status

- [x] Broker creation test
- [x] Broker name test
- [x] Config builder tests
- [x] Queue config builder tests
- [x] DLX config tests
- [x] Exchange types tests
- [x] Virtual host URL tests
- [x] Health status tests
- [x] Transaction state tests
- [ ] Integration tests with RabbitMQ
- [ ] Connection recovery tests
- [ ] Message ordering tests
- [ ] Priority queue tests
- [ ] Concurrent publishing tests

## Documentation

- [x] Module-level documentation
- [x] API documentation
- [x] Usage examples
- [ ] RabbitMQ setup guide
- [ ] Topology design patterns
- [ ] Troubleshooting guide

## Dependencies

- `lapin` - RabbitMQ/AMQP client (v2.5)
- `celers-protocol` - Message types
- `celers-kombu` - Broker traits
- `async-trait` - Async trait support
- `tokio` - Async runtime
- `tracing` - Logging
- `serde_json` - JSON serialization

## RabbitMQ Configuration

Recommended settings:

```conf
# Enable lazy queues for better performance with large queues
queue_type = lazy

# Set memory threshold
vm_memory_high_watermark.relative = 0.6

# Enable publisher confirms
channel_max = 2047

# Connection tuning
heartbeat = 60
```

## Celery Compatibility

This implementation is **100% compatible** with Python Celery's AMQP backend:

- Uses same exchange ("celery") and routing patterns
- Compatible message format (JSON serialization)
- Supports priority queues (x-max-priority)
- Follows Celery's queue naming conventions

## Notes

- Uses `basic_get` for polling (compatible with Redis broker pattern)
- Publisher confirms are automatic via lapin
- Priority queues require RabbitMQ 3.5.0+
- All queues are durable by default
- Messages are persistent (delivery_mode = 2)
- `list_queues()` not supported via AMQP protocol (requires Management API)

## Known Limitations

- Single channel per connection (channel pooling not yet implemented)
- `list_queues()` requires RabbitMQ Management API (not available via AMQP protocol)
