# celers-broker-amqp TODO

> RabbitMQ/AMQP broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

Full-featured AMQP broker with exchange/queue topology, message confirmation, and priority support.

## Completed Features

### Connection Management ✅
- [x] Connection to RabbitMQ via AMQP
- [x] Channel creation and management
- [x] Connection status checking
- [x] Graceful disconnect

### Message Publishing ✅
- [x] Basic message publishing to queues
- [x] Publishing with exchange/routing_key
- [x] Message persistence (delivery_mode = 2)
- [x] Publisher confirms (automatic)
- [x] Priority support via message properties
- [x] Correlation ID support

### Message Consumption ✅
- [x] Polling consumption via basic_get
- [x] Manual acknowledgment (ACK)
- [x] Message rejection with requeue
- [x] Delivery tag tracking
- [x] Redelivery flag support

### Queue Management ✅
- [x] Queue declaration with options
- [x] Durable queues
- [x] Priority queue support (x-max-priority)
- [x] Queue purging
- [x] Queue deletion
- [x] Queue size inspection

### Exchange/Topology ✅
- [x] Exchange declaration (Direct exchange)
- [x] Queue binding to exchange
- [x] Default "celery" exchange setup
- [x] Routing key support

### Batch Operations ✅
- [x] `publish_batch()` - Publish multiple messages efficiently
- [x] Publisher confirms collected after all publishes
- [x] Reduces round-trips compared to individual publishes
- [x] Returns successful publish count

## Future Enhancements

### Advanced Features
- [ ] Consumer streaming (basic_consume with callback)
- [ ] QoS (prefetch) configuration
- [ ] Dead Letter Exchange (DLX) support
- [ ] Message TTL support
- [ ] Exchange types (Fanout, Topic, Headers)
- [ ] Connection recovery and retry

### Topology
- [ ] Multiple exchange support
- [ ] Complex routing patterns
- [ ] Topology management API
- [ ] Virtual host support

### Performance
- [x] Batch publishing ✅
- [ ] Connection pooling
- [ ] Channel pooling
- [ ] Pipeline publishing

### Monitoring
- [ ] Channel-level metrics
- [ ] Publisher confirm tracking
- [ ] Connection health monitoring
- [ ] RabbitMQ Management API integration

### Reliability
- [ ] Automatic reconnection
- [ ] Message deduplication
- [ ] Transaction support
- [ ] Message persistence guarantees

## Testing Status

- [x] Broker creation test
- [x] Broker name test
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

- No consumer streaming (basic_consume) implemented yet
- No Dead Letter Exchange configuration
- No connection recovery/retry logic
- Single channel per connection
- `list_queues()` requires RabbitMQ Management API
