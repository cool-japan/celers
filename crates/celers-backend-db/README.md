# celers-backend-db

**Version: 0.2.0 | Status: [Alpha] | Updated: 2026-03-27**

Database (PostgreSQL/MySQL) result backend for CeleRS. Provides persistent task result storage, event persistence, chord state management, and optional distributed locks using SQL databases.

## Features

- PostgreSQL and MySQL support via SQLx
- Task result storage with configurable TTL
- Chord barrier synchronization (atomic counters)
- Event persistence for task lifecycle tracking
- Distributed locks (optional, feature-gated)
- SQL-based result queries and analytics

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-backend-db = { version = "0.2", features = ["postgres"] }
```

### PostgreSQL Backend

```rust
use celers_backend_db::PostgresResultBackend;
use celers_backend_redis::ResultBackend;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut backend = PostgresResultBackend::new("postgres://localhost/celers").await?;
    backend.migrate().await?;

    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "my_task".to_string());
    backend.store_result(task_id, &meta).await?;

    if let Some(result) = backend.get_result(task_id).await? {
        println!("Task result: {:?}", result.result);
    }

    Ok(())
}
```

### Event Persistence

```rust
use celers_backend_db::{DbEventPersister, DbEventPersisterConfig};

let config = DbEventPersisterConfig::default();
let persister = DbEventPersister::new(pool, config);
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `postgres` | Yes | PostgreSQL support |
| `mysql` | No | MySQL support |
| `distributed-locks` | No | Distributed lock primitives |

## Part of CeleRS

This crate is part of the [CeleRS](https://github.com/cool-japan/celers) project, a Celery-compatible distributed task queue for Rust.

## Testing

**7 tests passing** (unit tests; integration tests require PostgreSQL/MySQL)

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
