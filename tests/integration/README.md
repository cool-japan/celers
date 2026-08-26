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
| `mysql` (`--profile test`) | `MYSQL_URL` (bare -- **not** `CELERS_TEST_MYSQL_URL`; see note below) | `celers-broker-sql::tests` (the older suite) | `MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-broker-sql --all-features --run-ignored all` |
| `mysql` (`--profile test`) | `MYSQL_URL` (bare) | `celers-backend-db` (mysql half) | `MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-backend-db --all-features --run-ignored all` -- **run separately from the row above** (see the collision warning right after this table) |
| `localstack` (`--profile test`) | `CELERS_TEST_SQS_URL` | `celers-broker-sqs` (`src/tests.rs`, `tests/localstack.rs`) | `CELERS_TEST_SQS_URL=http://127.0.0.1:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 cargo test -p celers-broker-sqs --all-features --test localstack -- --ignored --test-threads=1` |

**The `DATABASE_URL`/`MYSQL_URL` rows are not typos.** Most suites above read a `CELERS_TEST_*`-prefixed variable,
but `celers-backend-db` (all of it) and `celers-broker-sql`'s older `src/tests.rs` read the bare `DATABASE_URL` /
`MYSQL_URL` instead -- the exact names several `examples/*.rs` files in those same crates use for real connection
strings. Exporting `CELERS_TEST_POSTGRES_URL`/`CELERS_TEST_MYSQL_URL` alone will silently run these two suites
with **no service configured** (see "How to tell a real run from a skip", below); you need the bare-named
variable too, pointed at the same server, to actually exercise them. This inconsistency is tracked as a
follow-up to rename the source-level constants -- fixing it is a Rust source change outside this README's scope.

**Pointing `celers-broker-sql` and `celers-backend-db` at the same MySQL database, as both rows above do by
default, hits a known bug.** Both crates auto-migrate a table named `celers_task_results` with incompatible
schemas; whichever one migrates second on a shared database fails its own follow-up DDL (`celers-backend-db`
fails with `ERROR 1072 (42000): Key column 'expires_at' doesn't exist in table`). This is not specific to a
long-lived database -- it reproduces on a freshly created one, in either migration order. Point the two crates
at *separate* databases (e.g. `celers_test` and `celers_backend_db_test` -- both need only `CREATE DATABASE` on
the same server, no separate service) if you need both suites green in the same session: doing exactly that
takes `celers-backend-db`'s full suite from 7 failures to a clean **150/150** (see "Per-service triage" below
for the exact commands). See
[TODO.md -> Known gaps #16](../../TODO.md#known-gaps--the-roadmap-after-031) for the full writeup; it is not
fixed by this README and no command below works around it, since it is a source-level naming collision.

## Per-service triage

Bringing up every service and exporting every variable for a full `cargo nextest run --workspace` works, but
when only one service's suite needs attention, scope the compose stack and the invocation to just that service
-- faster, and it will not incidentally exercise (or contend for ports with) services you are not looking at:

```bash
# Redis -- four crates share one server, no isolation between them beyond the
# UUID-suffixed key/queue names each test picks (see "Redis key hygiene" below)
docker-compose up -d redis
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 \
  cargo nextest run -p celers-broker-redis -p celers-backend-redis -p celers-worker -p celers-cli \
  --all-features --run-ignored all

# PostgreSQL -- broker and result backend can safely share one database (no table-name collision here)
docker-compose up -d postgres
CELERS_TEST_POSTGRES_URL=postgres://celers:celers_password@127.0.0.1:5432/celers \
  cargo nextest run -p celers-broker-postgres --all-features --run-ignored all
DATABASE_URL=postgres://celers:celers_password@127.0.0.1:5432/celers \
  cargo nextest run -p celers-backend-db --all-features --run-ignored all

# RabbitMQ
docker-compose up -d rabbitmq
CELERS_TEST_AMQP_URL=amqp://celers:celers_password@127.0.0.1:5672/%2f \
  cargo nextest run -p celers-broker-amqp --all-features --run-ignored all

# MySQL -- celers-broker-sql against the main database, celers-backend-db against a SEPARATE one
# (see the collision warning above): pointing both at `celers_test` is the reproduction for Known
# gaps #16, not a supported workflow -- confirmed by running celers-backend-db's full suite this
# way: 150/150 against its own database, 7 failures against celers-broker-sql's.
docker-compose --profile test up -d mysql
CELERS_TEST_MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test \
  MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test \
  cargo nextest run -p celers-broker-sql --all-features --run-ignored all
docker exec celers-mysql mysql -uroot -pcelers_root_password \
  -e "CREATE DATABASE IF NOT EXISTS celers_backend_db_test; \
      GRANT ALL PRIVILEGES ON celers_backend_db_test.* TO 'celers'@'%'; FLUSH PRIVILEGES;"
DATABASE_URL=postgres://celers:celers_password@127.0.0.1:5432/celers \
  MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_backend_db_test \
  cargo nextest run -p celers-backend-db --all-features --run-ignored all

# LocalStack (SQS) -- its two suites are cargo-test, not nextest (see the table above)
docker-compose --profile test up -d localstack
CELERS_TEST_SQS_URL=http://127.0.0.1:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  cargo test -p celers-broker-sqs --all-features --test localstack -- --ignored --test-threads=1
```

To run everything at once instead, bring up every service, export all seven variables, and add
`--all-features --run-ignored all` to one `cargo nextest run --workspace` -- accepting the MySQL collision above
(both `celers-broker-sql` and `celers-backend-db` will run against `celers_test`, and one of them will fail).

## How to tell a real run from a skip

Suites in the table above use one of two gating styles, and it matters which:

- **Early-return, no `#[ignore]`** (most of the table): the test runs in a plain `cargo nextest run`, no
  `--run-ignored` needed, but if the env var is unset it prints an `eprintln!` skip line (e.g. `"skipping
  {test_name}: CELERS_TEST_POSTGRES_URL is not set"`) and returns immediately -- **reporting PASS having run zero
  assertions.** `--run-ignored all` is still safe to add; it just has nothing extra to do for these.

  **That skip line is invisible in an ordinary run.** nextest captures stdout/stderr per test and only shows
  it for a test that *fails* -- a passing test's captured output is discarded, `eprintln!` skip line included.
  A green `cargo nextest run --workspace --all-features` therefore looks identical whether every gated suite
  above actually connected to a service or every one of them silently skipped; the exit code and the "N passed"
  summary do not distinguish the two. To actually see which happened, either add `--no-capture` (which also
  disables test parallelism, so scope it to one crate) or grep a run that already has it:

  ```bash
  # One crate, env var deliberately left unset:
  cargo nextest run -p celers-broker-postgres --all-features --no-capture 2>&1 | grep -i skip

  # The whole workspace -- slower (parallelism is off), but one invocation covers every gated suite:
  cargo nextest run --workspace --all-features --no-capture 2>&1 | grep -iE "skipping|is not set"
  ```

  A run with the real env var exported and `--no-capture` added should print **no** `skipping` lines for the
  suite(s) that variable gates; if it still does, the variable was not read the way you expected -- check the
  exact name against the table above (the `DATABASE_URL`/`MYSQL_URL` rows are the common trap).
- **`#[ignore]`-gated** (`celers-broker-sqs`'s two suites, one test in `celers-broker-sql::tests_hardening`, part
  of `celers-broker-postgres::tests.rs`): skipped entirely -- not even attempted -- unless you pass
  `--run-ignored all` (nextest) or `-- --ignored` (`cargo test`). Some of these additionally fall back to a
  hardcoded `localhost` connection string when the env var is unset, so passing `--run-ignored all` without the
  env var can mean "silently tried to connect to a database that probably isn't there" rather than "skipped" --
  always pair `--run-ignored all` with the real env var from the table.

Either way, **a green `cargo nextest run --workspace --all-features` with none of these variables set is not
evidence any of this code was exercised against a live service.** Add `--no-capture` (see above) and grep the
output for `skipping` / `is not set` to check which suites actually ran -- without `--no-capture` there is
nothing to grep for a passing test, skipped or not. A connection-refused *failure* is the one case that shows
up either way: it means the env var *was* read but nothing was listening, versus a silent pass, which means
either it connected successfully or it was never read at all -- `--no-capture` is what tells those two apart.

## Troubleshooting

### Docker Desktop port-forward wedge

On Docker Desktop (macOS/Windows), a compose service's forwarded port can silently wedge after the
container has been up for a while: `nc -z 127.0.0.1 <port>` still reports the port open, but a real
client connection hangs instead of completing or refusing. A gated suite that was passing starts
hanging indefinitely with no error -- not a connection-refused failure, which is the one failure mode
["How to tell a real run from a skip"](#how-to-tell-a-real-run-from-a-skip) above already explains how
to read. If a suite that was previously green now hangs rather than failing outright, suspect the
port forward before the code: `docker-compose restart <service>` (e.g. `docker-compose restart
mysql`) re-establishes the forward without losing the container's data volume, and is the fix in
every case seen so far. This is specific to Docker Desktop's networking layer, not to any one
service -- it has been observed on `postgres`, `mysql` and `rabbitmq` alike during long-running
sessions with the stack left up for hours.

### Redis key hygiene (db0)

All four Redis-gated crates (`celers-broker-redis`, `celers-backend-redis`, `celers-worker`,
`celers-cli`) point `CELERS_TEST_REDIS_URL` at the same database -- the URL in the table above never
carries a `/N` suffix, so every one of them lands on database 0 (`SELECT 0`, Redis's default) with no
`FLUSHDB` between runs and no per-suite database separation. Nothing cleans up after a run, so a
long-lived Redis instance shared across many sessions (as it typically is here) accumulates whatever
keys past runs wrote.

This works only because every gated test that writes a key or queue name embeds a fresh
`uuid::Uuid::new_v4()` in it -- see `celers-broker-redis::control`'s
`format!("celers.test.{label}.{}", uuid::Uuid::new_v4())` for the pattern. A new test that instead
uses a fixed, human-readable key (`"celers:test:my_queue"`) will collide with a leftover key from a
previous run, or with a concurrent `nextest` job running the same test binary in another thread, and
fail or pass for the wrong reason depending on what it finds there. Follow the same convention --
generate a unique suffix, never reuse a literal key across test runs -- for any new Redis-gated test.
There is no automatic cleanup, and a plain `docker-compose restart redis` will not clear anything --
the container's `appendonly` file lives in the named `redis-data` volume, which a restart keeps
intact by design. `redis-cli -h 127.0.0.1 FLUSHDB` is the actual manual reset if accumulated debris
ever needs clearing rather than merely tolerating (`docker-compose down -v` also clears it, but takes
every service down, not only Redis).

## Python/Rust wire-protocol interop

See [`../python-compat/README.md`](../python-compat/README.md) for Celery-wire-format interop tests between a
real Python Celery worker and CeleRS.
