-- CeleRS PostgreSQL broker — the broker's own result store,
-- `celers_broker_results`.
--
-- # Why this file exists
--
-- `results.rs` (`store_result` / `get_result` / `delete_result` /
-- `archive_results` / `get_results_batch` / `delete_results_batch`),
-- `analytics.rs`'s `store_results_batch` and `db_monitoring.rs`'s
-- `analyze_tables` / `vacuum_tables` all address the broker's own result
-- table. Until this migration **no migration created it**: every one of those
-- calls failed against a live server with
-- `ERROR: relation "..." does not exist`, and none of it was reachable
-- outside an in-process test.
--
-- # Why the table is not called `celers_task_results`
--
-- Because that name is already taken, by a table with an incompatible
-- schema, on the very same server.
--
-- `celers-backend-db`'s `PostgresResultBackend::migrate` applies
-- `celers-backend-db/migrations/001_init_postgres.sql`, which creates
-- `celers_task_results (task_id, task_name, result_state, result_data,
-- error_message, retry_count, created_at, started_at, completed_at, worker,
-- expires_at, extra)`. This crate's result storage binds a completely
-- different shape — `status`, `result`, `error`, `traceback`, `runtime_ms` —
-- and both crates auto-migrate, against the same database in the standard
-- deployment (see `docker-compose.yml`: `celers-broker-postgres` reads
-- `CELERS_TEST_POSTGRES_URL`, `celers-backend-db` reads `DATABASE_URL`, and
-- both are documented as pointing at one server). Creating
-- `celers_task_results` here would either be silently no-op'd by
-- `IF NOT EXISTS` against the backend's table — leaving every statement in
-- `results.rs` failing with `column "status" does not exist` — or, if this
-- crate migrated first, break the result backend instead.
--
-- `celers-broker-sql` hit exactly this collision on MySQL and resolved it by
-- renaming its broker-internal store to `celers_broker_results`, leaving
-- `celers_task_results` to the result backend (see that crate's
-- `sql_text.rs`'s `BROKER_RESULTS_TABLE` and
-- `migrations/010_broker_results.sql`). This file applies the same
-- resolution on PostgreSQL, so the two backends agree: the *broker's* result
-- store is `celers_broker_results` everywhere, the *result backend's* is
-- `celers_task_results` everywhere.
--
-- Unlike the MySQL side there is **no legacy-rename step** here, and none is
-- needed: `celers-broker-sql` had already shipped rows under the old name and
-- has `MysqlBroker::rename_legacy_broker_results_table` to carry them across,
-- whereas this crate never created any result table at all — that is the bug
-- this file fixes. Any `celers_task_results` on a PostgreSQL server therefore
-- belongs to `celers-backend-db` and is left strictly alone.
--
-- `002_results.sql`'s `celers_results` is a third, unrelated table: it is
-- created by this crate's migration set for schema compatibility with
-- `celers-broker-sql`'s SQLite/MySQL brokers and is read or written by
-- nothing in this crate.
--
-- # The Rust code is the spec
--
-- Every column below is pinned by a statement that already exists in the
-- crate, so the DDL is derived from the binds rather than the other way
-- round:
--
--  * `task_id UUID PRIMARY KEY` — `store_result` and `store_results_batch`
--    both end in `ON CONFLICT (task_id) DO UPDATE`, which needs a unique
--    index on exactly `task_id`; `get_result` / `delete_result` look a row up
--    by it; `get_results_batch` / `delete_results_batch` bind it through a
--    generated `IN ($1, .., $N)` list. All of them cast through
--    `$n::text::uuid` (see `row_ext::uuid_param`), so the column must really
--    be `UUID`, not `TEXT`.
--  * `task_name` — written by `store_result`, read back by `get_result` and
--    `get_results_batch` into `TaskResult::task_name`, a non-optional
--    `String`. `analytics.rs`'s `store_results_batch` does **not** bind it,
--    so it needs a default; it is `NOT NULL DEFAULT ''` rather than nullable
--    so the read side can never see a `NULL` it has no representation for.
--  * `status VARCHAR(20)` — `TaskResultStatus`'s `Display` writes the six
--    uppercase Celery status names and its `FromStr` accepts exactly those,
--    so the `CHECK` below is that enum transcribed. Longest is `PENDING` /
--    `REVOKED` at seven characters.
--  * `result JSONB` — bound through `$n::text::jsonb` and read back with an
--    explicit `result::text` projection. Nullable: `results.rs` deliberately
--    distinguishes a SQL `NULL` (no result) from the JSON literal `null`.
--  * `error` / `traceback TEXT` — nullable, bound as plain text.
--  * `created_at` — `store_result` writes `NOW()` explicitly;
--    `store_results_batch` does not bind it at all, hence the default.
--  * `completed_at` — nullable, set only once a result reaches a terminal
--    status. `archive_results` deletes on it, so it is indexed.
--  * `runtime_ms BIGINT` — nullable `Option<i64>`.
--  * `updated_at` — only ever written by `store_results_batch`'s
--    `ON CONFLICT DO UPDATE SET ... updated_at = NOW()`. `NOT NULL` with a
--    default so an insert that never reaches the conflict branch still has
--    one.
--
-- Every statement is idempotent (`IF NOT EXISTS`), matching the rest of the
-- migration set: `PostgresBroker::migrate()` records applied files in
-- `celers_schema_migrations` and skips them, but the files must still be safe
-- to replay (a database migrated by an older build has no ledger rows, and
-- the whole directory is also mounted into the dev container's
-- `docker-entrypoint-initdb.d`).

CREATE TABLE IF NOT EXISTS celers_broker_results (
    task_id      UUID PRIMARY KEY,
    task_name    VARCHAR(255) NOT NULL DEFAULT '',
    status       VARCHAR(20) NOT NULL,
    result       JSONB,
    error        TEXT,
    traceback    TEXT,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    runtime_ms   BIGINT,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CHECK (status IN ('PENDING', 'STARTED', 'SUCCESS', 'FAILURE', 'RETRY', 'REVOKED'))
);

-- Index names are prefixed `broker_results` rather than `task_results`:
-- PostgreSQL index names are database-wide, and `celers-backend-db`'s
-- `001_init_postgres.sql` already owns `idx_task_results_state`,
-- `idx_task_results_created` and `idx_task_results_expires` on its own
-- `celers_task_results`. Reusing those names would fail with
-- `ERROR: relation "idx_task_results_state" already exists` — the same
-- collision as the table name, one level down.

-- Status roll-ups ("how many stored results failed?") and the status filters
-- monitoring code applies.
CREATE INDEX IF NOT EXISTS idx_broker_results_status
    ON celers_broker_results (status);

-- Per-task-type result lookups and analytics grouping.
CREATE INDEX IF NOT EXISTS idx_broker_results_task_name
    ON celers_broker_results (task_name);

-- `get_results_batch` orders by `created_at DESC`; retention reporting scans
-- the same axis.
CREATE INDEX IF NOT EXISTS idx_broker_results_created_at
    ON celers_broker_results (created_at DESC);

-- `archive_results` is `DELETE ... WHERE completed_at < $1`. The partial
-- predicate keeps the index proportional to the finished-result backlog
-- rather than to every row ever stored.
CREATE INDEX IF NOT EXISTS idx_broker_results_completed_at
    ON celers_broker_results (completed_at)
    WHERE completed_at IS NOT NULL;
