# Integration Tests

This directory holds no test files. CeleRS's env-gated, live-service integration tests live inside each crate
that owns the code they exercise (`crates/<crate>/src/tests_*.rs`, or `crates/<crate>/tests/*.rs` for a small
number of black-box suites), not in a separate top-level `tests/` tree, and the root `Cargo.toml` is a virtual
workspace manifest with no `[package]` of its own -- there is no `--features integration` to pass, because no
crate declares that feature.

## Quick start: `scripts/test-integration.sh`

The front door for all of this is [`scripts/test-integration.sh`](../../scripts/test-integration.sh) at the
repo root. It brings up exactly the docker-compose services a run needs, waits for *real* health through the
host-forwarded port rather than just "the port is open" (see
["Docker Desktop port-forward wedge"](#docker-desktop-port-forward-wedge) below -- the script detects and
recovers from exactly that condition on its own, restarting a container once if it looks wedged), exports the
eight environment variables the table below documents, runs each crate's gated suite the same way the
"Per-service triage" section further down does by hand (`celers-broker-sql` included, both in nextest's normal
parallel mode and again serially), and prints a PASS/FAIL/SKIP summary with a non-zero exit on any failure:

```bash
scripts/test-integration.sh                            # bring up everything, run everything
scripts/test-integration.sh --only redis                # just the Redis-gated suites
scripts/test-integration.sh --only postgres              # celers-broker-postgres + celers-backend-db
scripts/test-integration.sh --only mysql                 # celers-broker-sql + celers-backend-db, including
                                                          # the separate-database dance the MySQL collision
                                                          # warning below describes
scripts/test-integration.sh --only rabbitmq              # celers-broker-amqp
scripts/test-integration.sh --only localstack            # celers-broker-sqs (alias: --only sqs)
scripts/test-integration.sh --full                       # also tests/python-compat/run.sh and a docker build
                                                          # smoke test
scripts/test-integration.sh --keep-up                    # leave whatever it started running afterwards
scripts/test-integration.sh --down-v --only postgres     # fresh postgres volume first (e.g. schema drift)
scripts/test-integration.sh --help                       # full flag reference
```

It only ever stops a service it started itself: one already answering when a run begins -- this stack is
routinely left running across sessions, and other developers or agents may be using it right now -- is left
running afterwards regardless of `--keep-up`.

The rest of this document -- the variable-by-variable table, the exact per-service commands, and the
troubleshooting notes -- is the manual reference the script's own steps are built from. Reach for it directly
when scoping a single suite by hand, diagnosing a failure the script's summary doesn't explain on its own, or
checking exactly what a given `--only` value actually runs before trusting the script to do it for you.

## Running the gated suites

Bring up the services with the root `docker-compose.yml` (add `--profile test` for MySQL and the SQS emulator,
which aren't part of the default stack) -- `scripts/test-integration.sh` above does exactly this for you,
including the one wrinkle this manual version does not handle: a wedged Docker Desktop port forward (see
"Troubleshooting" below):

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
| `rabbitmq` | `CELERS_TEST_AMQP_URL` | `celers-broker-amqp::tests_hardening` (early-return) and `celers-broker-amqp::tests`'s 21 `#[ignore]`-gated integration/management-API tests | `CELERS_TEST_AMQP_URL=amqp://celers:celers_password@127.0.0.1:5672/%2f cargo nextest run -p celers-broker-amqp --all-features --run-ignored all` |
| `mysql` (`--profile test`) | `CELERS_TEST_MYSQL_URL` | `celers-broker-sql::tests_hardening` | `CELERS_TEST_MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-broker-sql --all-features --run-ignored all` |
| `mysql` (`--profile test`) | `CELERS_TEST_MYSQL_UPGRADE_URL` | `celers-broker-sql::tests_hardening::migration_upgrade` (the `celers_task_results` -> `celers_broker_results` rename) | `CELERS_TEST_MYSQL_UPGRADE_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_upgrade_test cargo nextest run -p celers-broker-sql --all-features -E 'test(migration_upgrade)'` -- **must name a database of its own**: the suite drops and recreates both result tables, and refuses to run if this is the same database as `CELERS_TEST_MYSQL_URL`. Create it with the root credentials (`docker exec celers-mysql mysql -uroot -pcelers_root_password -e "CREATE DATABASE IF NOT EXISTS celers_upgrade_test; GRANT ALL PRIVILEGES ON celers_upgrade_test.* TO 'celers'@'%';"`), or let `scripts/test-integration.sh --only mysql` do it |
| `mysql` (`--profile test`) | `CELERS_TEST_MYSQL_URL`, falling back to bare `MYSQL_URL` | `celers-broker-sql::tests` (the older suite) | `CELERS_TEST_MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-broker-sql --all-features --run-ignored all` |
| `mysql` (`--profile test`) | `MYSQL_URL` (bare) | `celers-backend-db` (mysql half) | `MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test cargo nextest run -p celers-backend-db --all-features --run-ignored all` -- **run separately from the row above** (see the collision warning right after this table) |
| `localstack` (`--profile test`) | `CELERS_TEST_SQS_URL` | `celers-broker-sqs` (`src/tests.rs`, `tests/localstack.rs`) | `CELERS_TEST_SQS_URL=http://127.0.0.1:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 cargo test -p celers-broker-sqs --all-features --test localstack -- --ignored --test-threads=1` |
| all of the above | every variable in this column | `celers` (the facade's own `src/tests.rs`, one live round trip per re-exported broker/backend, plus `broker_helper`'s live AMQP module) | export all eight variables, then `cargo nextest run -p celers --all-features --run-ignored all` |

The table above is the *service-owning* suite for each variable, not the complete list of tests that read
one. These also gate on `CELERS_TEST_REDIS_URL` and are easy to miss because the crates they live in are
not "the Redis crates": `celers-beat::schedule_store::redis_store`, `celers-worker`'s
`worker_core::tests::dispatch_and_shutdown`, `celers-backend-redis`'s `tests_publish` / `tls`,
`celers-broker-redis`'s `connection` / `result_backend`, and `celers-protocol`'s `tests/python_interop.rs`
(which additionally needs `CELERS_PYTHON`; `tests/python-compat/run.sh` is the supported way to run it).
A `--no-capture` workspace run with **no** variables exported and **no** `--run-ignored all` prints a skip
line for every early-return-gated test -- 147 lines as of 2026-08-26 -- which is the authoritative inventory
for that gating style (see "How to tell a real run from a skip" for the two styles). It does **not** cover the
`#[ignore]`-gated tests (`celers-broker-sqs`'s two suites, one `celers-broker-sql::tests_hardening` test, part
of `celers-broker-postgres::tests.rs`, and `celers-broker-amqp::tests`'s 21): without `--run-ignored all`
nextest skips those itself before the test body ever runs, so none of them reach their own `eprintln!` --
confirmed for `celers-broker-amqp` specifically, both before and after this crate's own gating fix below (same
command, no `CELERS_TEST_AMQP_URL`, no `--run-ignored all`: exactly 6 lines from its early-return tests every
time, with nextest's own summary reporting "21 skipped" for the ignored ones separately and unchanged by that
fix). It is lines, not tests: a test that opens two connections prints two (`celers-broker-sql`'s helper
reports `file:line` rather than a test name, which is where most of the duplication lives). Treat 147 as a
snapshot, not a promise -- it will drift as gated tests are added or renamed elsewhere in the workspace;
re-running the recipe in "How to tell a real run from a skip" is cheap insurance before trusting it.

**The `DATABASE_URL`/`MYSQL_URL` rows are not typos, but the bare name is no longer the *only* one that works.**
`celers-backend-db`'s `test_env::resolve` (`src/lib.rs`) now accepts either name for both backends --
`CELERS_TEST_POSTGRES_URL`/`CELERS_TEST_MYSQL_URL` preferred, the bare `DATABASE_URL`/`MYSQL_URL` (the exact
names several `examples/*.rs` files in that crate use for real connection strings) as a documented fallback --
the same shape `celers-broker-sql`'s newer suite already had. Exporting only the `CELERS_TEST_*` name is
therefore enough on its own **for that crate alone**; the row above still shows the bare form because it is
what the rest of this document's commands consistently export together with the `CELERS_TEST_*` name (mixed
brokers still needing exactly one or the other would otherwise be easy to get backwards), and because
`--only mysql`/`--only postgres` in `scripts/test-integration.sh` export both for exactly this reason too (see
that script's `run_backend_db_postgres_half`, which has to unset *both* names -- not just the bare one -- to
keep celers-backend-db's postgres half from also picking up whatever `celers_test` the mysql leg pointed
`CELERS_TEST_MYSQL_URL`/`MYSQL_URL` at). Exporting **neither** name still means **no service configured**
(see "How to tell a real run from a skip", below).

**Pointing `celers-broker-sql` and `celers-backend-db` at the same MySQL database used to hit a known bug.
It is fixed.** Both crates auto-migrated a table named `celers_task_results` with incompatible schemas, so
whichever migrated second on a shared database failed its own follow-up DDL (`celers-backend-db` with
`ERROR 1072 (42000): Key column 'expires_at' doesn't exist in table`, the broker with the same error on
`status`). The broker's table is now `celers_broker_results`, `celers_task_results` belongs exclusively to the
result backend, and existing databases are upgraded in place by the broker's `migrate()` -- see
[TODO.md -> Known gaps #16](../../TODO.md#known-gaps--the-roadmap-after-031), now closed, and
[CHANGELOG.md](../../CHANGELOG.md)'s 0.3.1 "Breaking changes -> Database schema" for what an operator who
queries that table directly has to repoint.

Both topologies are verified: on a MySQL volume recreated with `docker compose down -v`,
`celers-broker-sql` passes **236/236** and `celers-backend-db` then passes **151/151 against that same
database** -- the configuration that used to produce 7 failures. Separate databases (e.g. `celers_test` and
`celers_backend_db_test`) still work and are what `scripts/test-integration.sh` uses, now for test-isolation
hygiene rather than out of necessity: the two suites otherwise churn each other's rows while running.

The same hazard reaches a third, easy-to-miss place: `celers`'s own facade suite
(`backend_db_integration::test_mysql_backend_integration` in `crates/celers/src/tests.rs`) also reads the bare
`MYSQL_URL` and runs the identical `celers-backend-db` migration -- its own doc comment already carries this
exact warning ("point it at a database of its own, not at the one `CELERS_TEST_MYSQL_URL` names"). The
unscoped `scripts/test-integration.sh` run (see "Quick start" above) follows that warning for you: its
`celers (facade, all backends)` step overrides `MYSQL_URL` to the same `celers_backend_db_test` database its
own `--only mysql` leg uses, so the facade run does not reintroduce the collision it would otherwise hit
running right after `celers-broker-sql`'s own suite has migrated `celers_test`. Doing this by hand (the
`cargo nextest run -p celers --all-features --run-ignored all` invocation in the table above) needs the same
care: export `MYSQL_URL` pointed at a database of its own, not at `celers_test`, before running it in the same
session as `celers-broker-sql`'s own suite.

## Per-service triage

`scripts/test-integration.sh --only <service>` (see "Quick start" above) automates everything in this section.
Reach for the commands below directly when triaging a single suite by hand -- to reproduce a failure with more
control than the script's summary gives you, to run something the script does not cover, or just to see
exactly what a given `--only` value does before trusting it.

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

# MySQL. Two extra databases are created with the root credentials: celers_backend_db_test keeps
# celers-backend-db's rows from churning celers-broker-sql's while both suites run (the two crates
# CAN share one database now that Known gaps #16 is fixed -- verified 151/151 -- this is isolation
# hygiene, not a workaround), and celers_upgrade_test is the disposable schema the rename suite
# drops tables in.
docker-compose --profile test up -d mysql
docker exec celers-mysql mysql -uroot -pcelers_root_password \
  -e "CREATE DATABASE IF NOT EXISTS celers_backend_db_test; \
      CREATE DATABASE IF NOT EXISTS celers_upgrade_test; \
      GRANT ALL PRIVILEGES ON celers_backend_db_test.* TO 'celers'@'%'; \
      GRANT ALL PRIVILEGES ON celers_upgrade_test.* TO 'celers'@'%'; FLUSH PRIVILEGES;"
CELERS_TEST_MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test \
  MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_test \
  CELERS_TEST_MYSQL_UPGRADE_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_upgrade_test \
  cargo nextest run -p celers-broker-sql --all-features --run-ignored all
DATABASE_URL=postgres://celers:celers_password@127.0.0.1:5432/celers \
  MYSQL_URL=mysql://celers:celers_password@127.0.0.1:3306/celers_backend_db_test \
  cargo nextest run -p celers-backend-db --all-features --run-ignored all

# LocalStack (SQS) -- its two suites are cargo-test, not nextest (see the table above)
docker-compose --profile test up -d localstack
CELERS_TEST_SQS_URL=http://127.0.0.1:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  cargo test -p celers-broker-sqs --all-features --test localstack -- --ignored --test-threads=1
```

To run everything at once instead, bring up every service, export all eight variables, and add
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
  of `celers-broker-postgres::tests.rs`, and `celers-broker-amqp::tests`'s 21 integration/management-API tests):
  skipped entirely -- not even attempted -- unless you pass `--run-ignored all` (nextest) or `-- --ignored`
  (`cargo test`). Some of these additionally fall back to a hardcoded `localhost` connection string when the env
  var is unset, so passing `--run-ignored all` without the env var can mean "silently tried to connect to a
  database that probably isn't there" rather than "skipped" -- always pair `--run-ignored all` with the real env
  var from the table. `celers-broker-amqp::tests`'s 21 no longer work this way -- they used to (permanently, not
  intermittently: they hardcoded `amqp://localhost:5672` with no credentials, and the docker-compose broker
  above does not create a `guest` user at all once `RABBITMQ_DEFAULT_USER` is set, so every one of them silently
  "passed" having connected to nothing at all, even paired with `--run-ignored all` and the real env var, until
  this was fixed) -- they now read `CELERS_TEST_AMQP_URL` and print the same greppable `SKIPPED:` line the
  early-return suites do when it is unset, and treat a connect failure as a real failure once a URL is
  configured.

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

`scripts/test-integration.sh` (see "Quick start" above) handles this automatically: its readiness checks are
real client round trips through each service's own protocol (a Postgres `SSLRequest`, an AMQP protocol header,
reading MySQL's own unprompted handshake packet, `redis-cli PING`, the same `curl` health check
`docker-compose.yml` itself uses for localstack) rather than a bare `nc -z`, specifically so a wedge shows up
as a failed probe instead of a hang; when one does, the script restarts that one container once and retries
before giving up. Running the script instead of the manual commands above is the easiest way to stop hitting
this by hand at all.

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
