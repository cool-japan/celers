# celers-backend-db

**Version: 0.3.1 | Status: [Alpha] | Tests: 132 (`--all-features`, excluding `#[ignore]`d) + 1 doctest | Updated: 2026-08-26**

Database (PostgreSQL/MySQL) result backend for CeleRS. Provides persistent task result storage, event persistence, chord state management, database analytics, and optional distributed locks using SQL databases.

## Features

- PostgreSQL and MySQL support via OxiSQL (`oxisql-postgres` / `oxisql-mysql`) — Pure Rust, no `sqlx` dependency
- Task result storage with configurable TTL
- Chord barrier synchronization (atomic counters)
- Event persistence for task lifecycle tracking
- Database analytics: task success/failure rates, duration percentiles (mean/p50/p95/p99), per-worker
  throughput, storage sizing, and chord completion rate (`.analytics()`)
- TLS-aware connections: `sslmode` (PostgreSQL) / `ssl-mode`+`tls` (MySQL) query parameters are parsed
  from the connection URL and honored, instead of always connecting in plain text
- Distributed locks (optional, feature-gated)

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-backend-db = { version = "0.3", features = ["postgres"] }
```

### PostgreSQL Backend

```rust
use celers_backend_db::PostgresResultBackend;
use celers_backend_redis::{ResultBackend, TaskMeta};
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

### Database Analytics

```rust
use std::time::Duration;

let analytics = backend.analytics();
let stats = analytics.task_stats(Duration::from_secs(3600)).await?;
println!("success rate: {:.2}%", stats.success_rate * 100.0);

let latencies = analytics.percentile_latencies(Duration::from_secs(3600)).await?;
println!("p95 duration: {:?}", latencies.p95);
```

### Event Persistence

```rust
use celers_backend_db::{DbEventPersister, DbEventPersisterConfig};

// `conn` is an `oxisql_postgres::PgConnection` (e.g. `backend.connection().clone()`)
let config = DbEventPersisterConfig::default();
let persister = DbEventPersister::new(conn, config).await?;
persister.migrate().await?;
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

**132 tests passing** (`cargo nextest run --all-features`), **19 skipped** (marked `#[ignore]`; require a live
PostgreSQL/MySQL instance — run with `cargo nextest run --all-features --run-ignored all`). Plus **1 doc test passing** (5
additional doc tests intentionally `ignore`d as illustrative-only, since they also require a live
database).

### Live-database tests

Point `CELERS_TEST_POSTGRES_URL` and/or `CELERS_TEST_MYSQL_URL` at a real server:

```bash
CELERS_TEST_POSTGRES_URL=postgres://celers:celers_password@127.0.0.1:5432/celers \
CELERS_TEST_MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test \
    cargo nextest run -p celers-backend-db --all-features --run-ignored all
```

The bare `DATABASE_URL` / `MYSQL_URL` names this crate historically read are still accepted as a
documented fallback, so existing environments keep working; the `CELERS_TEST_*` names are the
workspace standard and take precedence.

There is **no** hardcoded default connection string. A variable that is unset (or set but blank)
makes the test print `SKIPPED: <test name> (set CELERS_TEST_POSTGRES_URL to run)` and return —
never connect to a `localhost` server nobody configured.

Pointing this crate's MySQL half at the **same** database as `celers-broker-sql` is supported: the
two used to collide on a shared `celers_task_results` table and a schema-scoped `chk_result_state`
constraint, and `celers-broker-sql` has since renamed both of its own objects. See
[../../tests/integration/README.md](../../tests/integration/README.md) for the full per-service
invocation table.

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
