# Integration Tests

This directory holds no test files. CeleRS's env-gated, live-service integration tests live inside each crate
that owns the code they exercise (`crates/<crate>/src/tests_*.rs`, or `crates/<crate>/tests/*.rs` for a small
number of black-box suites), not in a separate top-level `tests/` tree, and the root `Cargo.toml` is a virtual
workspace manifest with no `[package]` of its own -- there is no `--features integration` to pass, because no
crate declares that feature.

## Running the gated suites

Bring up the services with the root `docker-compose.yml` (add `--profile test` for MySQL and the SQS emulator,
which aren't part of the default stack):

```bash
docker-compose up -d
docker-compose --profile test up -d
```

Then export the matching environment variable and run the suite that variable gates, adding `--run-ignored all`
so both gating styles below are covered by one invocation. Every suite's own doc comment (top of the file listed)
documents exactly this same invocation.

| Service (compose) | Env var CeleRS actually reads today | Suite | Invocation |
|---|---|---|---|
| `redis` | `CELERS_TEST_REDIS_URL` | `celers-broker-redis::control` | `CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo nextest run -p celers-broker-redis --all-features --run-ignored all` |
| `redis` | `CELERS_TEST_REDIS_URL` | `celers-backend-redis::tests_ops`, `::tests_event_wire` | `CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo nextest run -p celers-backend-redis --all-features --run-ignored all` |
| `redis` | `CELERS_TEST_REDIS_URL` | `celers-worker::distributed_rate_limit` | `CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo nextest run -p celers-worker --all-features --run-ignored all` |
| `redis` | `CELERS_TEST_REDIS_URL` | `celers-cli` (`tests/control_redis.rs`, `tests/revocation_redis.rs`) | `CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo nextest run -p celers-cli --all-features --run-ignored all` |
| `postgres` | `CELERS_TEST_POSTGRES_URL` | `celers-broker-postgres` (`tests_pg.rs`, `tests.rs`) | `CELERS_TEST_POSTGRES_URL=postgres://celers:celers_password@127.0.0.1:5432/celers cargo nextest run -p celers-broker-postgres --all-features --run-ignored all` |
| `postgres` | `DATABASE_URL` (bare -- **not** `CELERS_TEST_POSTGRES_URL`; see note below) | `celers-backend-db` (`lib.rs`, `lock.rs`, postgres half of `analytics.rs`) | `DATABASE_URL=postgres://celers:celers_password@127.0.0.1:5432/celers cargo nextest run -p celers-backend-db --all-features --run-ignored all` |
| `rabbitmq` | `CELERS_TEST_AMQP_URL` | `celers-broker-amqp::tests_hardening` | `CELERS_TEST_AMQP_URL=amqp://celers:celers_password@127.0.0.1:5672/%2f cargo nextest run -p celers-broker-amqp --all-features --run-ignored all` |
| `mysql` (`--profile test`) | `CELERS_TEST_MYSQL_URL` | `celers-broker-sql::tests_hardening` | `CELERS_TEST_MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-broker-sql --all-features --run-ignored all` |
| `mysql` (`--profile test`) | `MYSQL_URL` (bare -- **not** `CELERS_TEST_MYSQL_URL`; see note below) | `celers-broker-sql::tests` (the older suite), `celers-backend-db` (mysql half) | `MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-broker-sql -p celers-backend-db --all-features --run-ignored all` |
| `localstack` (`--profile test`) | `CELERS_TEST_SQS_URL` | `celers-broker-sqs` (`src/tests.rs`, `tests/localstack.rs`) | `CELERS_TEST_SQS_URL=http://127.0.0.1:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 cargo test -p celers-broker-sqs --all-features --test localstack -- --ignored --test-threads=1` |

**The `DATABASE_URL`/`MYSQL_URL` rows are not typos.** Most suites above read a `CELERS_TEST_*`-prefixed variable,
but `celers-backend-db` (all of it) and `celers-broker-sql`'s older `src/tests.rs` read the bare `DATABASE_URL` /
`MYSQL_URL` instead -- the exact names several `examples/*.rs` files in those same crates use for real connection
strings. Exporting `CELERS_TEST_POSTGRES_URL`/`CELERS_TEST_MYSQL_URL` alone will silently run these two suites
with **no service configured** (see "How to tell a real run from a skip", below); you need the bare-named
variable too, pointed at the same server, to actually exercise them. This inconsistency is tracked as a
follow-up to rename the source-level constants -- fixing it is a Rust source change outside this README's scope.

Run everything at once with all eight variables exported and `--all-features --run-ignored all` on a full
`cargo nextest run --workspace`.

## How to tell a real run from a skip

Suites in the table above use one of two gating styles, and it matters which:

- **Early-return, no `#[ignore]`** (most of the table): the test runs in a plain `cargo nextest run`, no
  `--run-ignored` needed, but if the env var is unset it prints an `eprintln!` skip line (e.g. `"skipping
  {test_name}: CELERS_TEST_POSTGRES_URL is not set"`) and returns immediately -- **reporting PASS having run zero
  assertions.** `--run-ignored all` is still safe to add; it just has nothing extra to do for these.
- **`#[ignore]`-gated** (`celers-broker-sqs`'s two suites, one test in `celers-broker-sql::tests_hardening`, part
  of `celers-broker-postgres::tests.rs`): skipped entirely -- not even attempted -- unless you pass
  `--run-ignored all` (nextest) or `-- --ignored` (`cargo test`). Some of these additionally fall back to a
  hardcoded `localhost` connection string when the env var is unset, so passing `--run-ignored all` without the
  env var can mean "silently tried to connect to a database that probably isn't there" rather than "skipped" --
  always pair `--run-ignored all` with the real env var from the table.

Either way, **a green `cargo nextest run --workspace --all-features` with none of these variables set is not
evidence any of this code was exercised against a live service.** grep the test output for `skipping` /
`is not set` to check which suites actually ran, or watch for connection-refused failures (a sign the env var
*was* read but nothing was listening) versus silent passes (a sign it wasn't read at all).

## Python/Rust wire-protocol interop

See [`../python-compat/README.md`](../python-compat/README.md) for Celery-wire-format interop tests between a
real Python Celery worker and CeleRS.
