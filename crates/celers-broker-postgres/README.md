# celers-broker-postgres

PostgreSQL-based broker implementation for CeleRS using `FOR UPDATE SKIP LOCKED`.

## Overview

**Status: Not Yet Implemented**

This broker will provide durable task queue functionality using PostgreSQL as the backend,
suitable for scenarios where:

- You want transactional consistency with your application database
- Redis is not available or desired
- You need stronger durability guarantees
- You're already using PostgreSQL

## Planned Architecture

### Queue Table Schema

```sql
CREATE TABLE celers_tasks (
    id UUID PRIMARY KEY,
    queue_name VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    state VARCHAR(50) NOT NULL,
    priority INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    reserved_at TIMESTAMP WITH TIME ZONE,
    reserved_by VARCHAR(255),
    timeout_secs INTEGER,
    INDEX idx_queue_state_priority (queue_name, state, priority DESC)
);
```

### Dequeue Pattern

```sql
SELECT id, payload, ...
FROM celers_tasks
WHERE queue_name = $1
  AND state = 'pending'
ORDER BY priority DESC, created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

## Features

- **SKIP LOCKED**: Prevents lock contention between workers
- **Transactional**: Queue operations can be part of larger transactions
- **Durable**: Survives process crashes and restarts
- **Priority Support**: Native priority queue support
- **Query-able**: Can inspect queue state with SQL

## Trade-offs

### Advantages
- Strong consistency guarantees
- Integrated with application database
- No additional infrastructure required
- Can participate in distributed transactions

### Disadvantages
- Lower throughput than Redis (~100-1000 tasks/sec vs ~10,000)
- Higher latency per operation
- Puts load on your main database

## Usage (Planned)

```rust
use celers_broker_postgres::PostgresBroker;

let broker = PostgresBroker::new("postgresql://localhost/mydb").await?;
```

## Implementation Status

- [ ] Database schema design
- [ ] Connection pool setup
- [ ] Enqueue implementation
- [ ] Dequeue with SKIP LOCKED
- [ ] Ack/Reject implementation
- [ ] Migration scripts
- [ ] Integration tests

## See Also

- `celers-core`: Core traits and types
- `celers-broker-redis`: Alternative Redis-based broker
