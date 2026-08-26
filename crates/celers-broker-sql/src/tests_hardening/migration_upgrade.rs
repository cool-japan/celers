//! Live proof of the `celers_task_results` → `celers_broker_results` upgrade
//! path, gated on [`UPGRADE_URL_ENV`].
//!
//! # Why this needs a database of its own
//!
//! [`MysqlBroker::migrate`]'s rename step decides what to do by probing for
//! two *fixed* table names in the current schema, so exercising it means
//! putting the schema into states — "the legacy table is present and the new
//! one is not", "both are present" — that are destructive to any other
//! connection using that database. Every scenario below drops and recreates
//! `celers_broker_results`, which the rest of this crate's gated suite reads
//! and writes concurrently. Pointing this at `CELERS_TEST_MYSQL_URL` would
//! therefore corrupt the neighbouring tests rather than test anything, so it
//! reads a **separate** variable naming a **disposable** database, exactly the
//! way `scripts/test-integration.sh` already provisions
//! `celers_backend_db_test` as its own schema for `celers-backend-db`.
//!
//! The MySQL user the test suite runs as has `GRANT ALL` on its own databases
//! and no `CREATE DATABASE` privilege (that is the stock `mysql:8.0` image's
//! `MYSQL_USER` grant), so the database cannot be self-provisioned here; it is
//! created by `scripts/test-integration.sh`, which has the root credentials.
//!
//! # Why it is one test rather than four
//!
//! `cargo nextest` runs every test in its own process, in parallel. Four tests
//! sharing one database would race on the same two table names — the very
//! hazard this module isolates itself from. The scenarios are therefore steps
//! of a single serial test.
//!
//! # What is covered
//!
//! The four branches of `rename_legacy_broker_results_table`, each with the
//! migration ledger in the state a real upgrade would leave it:
//!
//! 1. a database migrated by an **older CeleRS build** — the legacy table
//!    holds this crate's schema and rows, and `celers_migrations` already
//!    records version `010`, so the tracked migration is skipped and the
//!    untracked rename is the only thing that can produce the new table;
//! 2. a database where `celers_task_results` belongs to **`celers-backend-db`**
//!    — left strictly alone, new table created alongside it;
//! 3. **both** names present with the legacy one still broker-shaped — left
//!    alone, no error, rows never touched;
//! 4. an **ambiguous** legacy table carrying both crates' signature columns —
//!    left alone.

use crate::row_ext::RowExt;
use crate::MysqlBroker;
use oxisql_core::Connection;
use uuid::Uuid;

/// Environment variable naming a MySQL database this test may freely destroy
/// and recreate tables in. Must not be the database
/// `CELERS_TEST_MYSQL_URL` names — see this module's doc comment.
const UPGRADE_URL_ENV: &str = "CELERS_TEST_MYSQL_UPGRADE_URL";

/// The variable the rest of the gated suite uses, checked here only so a
/// misconfiguration that would aim the destructive scenarios at the shared
/// test database is caught loudly instead of silently eating it.
const SHARED_URL_ENV: &str = "CELERS_TEST_MYSQL_URL";

/// The pre-0.3.1 name of this crate's broker-internal result store.
const LEGACY: &str = "celers_task_results";

/// The name it was renamed to, leaving `LEGACY` to `celers-backend-db`.
const CURRENT: &str = "celers_broker_results";

/// `migrations/010_task_results.sql` as it shipped before the rename,
/// reproduced verbatim so the fixture is the real legacy schema rather than a
/// paraphrase of it. Deleted from the tree by the rename, so it cannot be
/// `include_str!`d; kept in sync by
/// [`the_legacy_fixture_matches_the_current_schema_except_for_the_name`],
/// which diffs it against the migration that replaced it.
const LEGACY_BROKER_SCHEMA: &str = "CREATE TABLE celers_task_results (
    task_id CHAR(36) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    result LONGTEXT,
    error TEXT,
    traceback TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    runtime_ms BIGINT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

/// The three indexes the legacy table carried. `RENAME TABLE` brings them
/// across under these exact names, which is why `010_broker_results.sql` still
/// issues them unchanged.
const LEGACY_BROKER_INDEXES: &[&str] = &[
    "CREATE INDEX idx_task_results_name ON celers_task_results(task_name)",
    "CREATE INDEX idx_task_results_status ON celers_task_results(status)",
    "CREATE INDEX idx_task_results_completed ON celers_task_results(completed_at)",
];

/// `celers-backend-db`'s `migrations/001_init_mysql.sql` table, minus its
/// `chk_result_state` CHECK constraint.
///
/// The constraint is omitted deliberately: MySQL scopes CHECK constraint names
/// to the *schema*, so creating it here would make this fixture collide with
/// whatever `celers_results` carries, which is a different upgrade (see
/// `rename_legacy_result_state_constraint`) already covered end-to-end by
/// `integration::broker_and_result_backend_coexist_on_one_database` against
/// the real backend. What this fixture has to get right is the part the rename
/// probe actually reads: the `result_state` signature column, and the absence
/// of `traceback`.
const BACKEND_DB_SCHEMA: &str = "CREATE TABLE celers_task_results (
    task_id CHAR(36) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    result_state VARCHAR(20) NOT NULL,
    result_data JSON,
    error_message TEXT,
    retry_count INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    worker VARCHAR(255),
    expires_at TIMESTAMP NULL,
    extra JSON
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

/// A legacy table that looks like *both* crates' — the ambiguous case the
/// rename must refuse to act on.
const AMBIGUOUS_SCHEMA: &str = "CREATE TABLE celers_task_results (
    task_id CHAR(36) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    result LONGTEXT,
    traceback TEXT,
    result_state VARCHAR(20),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

/// The disposable database's URL, or `None` (with a greppable skip line) when
/// none is configured.
///
/// Panics rather than skips when it names the same database as
/// `CELERS_TEST_MYSQL_URL`: that is an operator mistake whose consequence is a
/// wiped shared test schema, and a silent skip would let it stand.
fn upgrade_url(test_name: &str) -> Option<String> {
    let url = match std::env::var(UPGRADE_URL_ENV) {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            eprintln!("SKIPPED: {test_name} (set {UPGRADE_URL_ENV} to run)");
            return None;
        }
    };
    if let Ok(shared) = std::env::var(SHARED_URL_ENV) {
        assert_ne!(
            database_of(&url),
            database_of(&shared),
            "{UPGRADE_URL_ENV} must name a database of its own: this test drops and recreates \
             `{CURRENT}`, which the rest of the gated suite is using on {SHARED_URL_ENV}"
        );
    }
    Some(url)
}

/// The database component of a `mysql://user:pass@host:port/db?opts` URL.
///
/// Deliberately crude — it is used only to compare two URLs' targets, so it
/// needs to be consistent, not to be a URL parser.
fn database_of(url: &str) -> &str {
    let after_host = url.rsplit('/').next().unwrap_or("");
    after_host.split('?').next().unwrap_or(after_host)
}

/// Run one DDL/DML statement, failing the test with the statement text.
async fn exec(broker: &MysqlBroker, sql: &str) {
    broker
        .connection()
        .execute(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("statement should succeed: {sql}\n{e}"));
}

/// Does `table` exist in the connected database?
async fn table_exists(broker: &MysqlBroker, table: &str) -> bool {
    !broker
        .connection()
        .query(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_name = ? LIMIT 1",
            &[&table],
        )
        .await
        .expect("information_schema query should succeed")
        .is_empty()
}

/// Does `table` carry `column`?
async fn column_exists(broker: &MysqlBroker, table: &str, column: &str) -> bool {
    !broker
        .connection()
        .query(
            "SELECT 1 FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ? LIMIT 1",
            &[&table, &column],
        )
        .await
        .expect("information_schema query should succeed")
        .is_empty()
}

/// The index names defined on `table`, excluding the primary key.
async fn index_names(broker: &MysqlBroker, table: &str) -> Vec<String> {
    let rows = broker
        .connection()
        .query(
            "SELECT DISTINCT index_name AS n FROM information_schema.statistics \
             WHERE table_schema = DATABASE() AND table_name = ? AND index_name <> 'PRIMARY' \
             ORDER BY index_name",
            &[&table],
        )
        .await
        .expect("information_schema query should succeed");
    rows.iter()
        .map(|row| {
            row.col::<String>("n")
                .expect("index_name should be a string")
        })
        .collect()
}

/// How many rows `table` holds. Takes the table name by interpolation because
/// a table name cannot be a bind parameter; every caller passes a constant.
async fn row_count(broker: &MysqlBroker, table: &str) -> i64 {
    let rows = broker
        .connection()
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"), &[])
        .await
        .expect("count should succeed");
    rows.first()
        .expect("COUNT(*) always returns a row")
        .col::<i64>("c")
        .expect("count should be an integer")
}

/// Put the database back to "the broker has fully migrated, and neither result
/// table exists", so each scenario starts from the same place.
///
/// Dropping the ledger row for `010` as well lets a scenario choose whether
/// the tracked migration should run (a database that never reached 010) or be
/// skipped (a database an older build already recorded it on).
async fn reset(broker: &MysqlBroker, ledger_records_010: bool) {
    exec(broker, &format!("DROP TABLE IF EXISTS {CURRENT}")).await;
    exec(broker, &format!("DROP TABLE IF EXISTS {LEGACY}")).await;
    if ledger_records_010 {
        exec(
            broker,
            "INSERT INTO celers_migrations (version, name) VALUES ('010', 'task_results') \
             ON DUPLICATE KEY UPDATE name = VALUES(name)",
        )
        .await;
    } else {
        exec(
            broker,
            "DELETE FROM celers_migrations WHERE version = '010'",
        )
        .await;
    }
}

/// Create the legacy, broker-shaped table with its indexes and one full row,
/// returning the row's task id.
async fn seed_legacy_broker_table(broker: &MysqlBroker) -> String {
    exec(broker, LEGACY_BROKER_SCHEMA).await;
    for index in LEGACY_BROKER_INDEXES {
        exec(broker, index).await;
    }
    let task_id = Uuid::new_v4().to_string();
    broker
        .connection()
        .execute(
            &format!(
                "INSERT INTO {LEGACY} \
                 (task_id, task_name, status, result, error, traceback, completed_at, runtime_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, NOW(), ?)"
            ),
            &[
                &task_id.as_str(),
                &"legacy.task",
                &"SUCCESS",
                &r#"{"carried":"across"}"#,
                &"legacy error",
                &"legacy traceback",
                &4242_i64,
            ],
        )
        .await
        .expect("seeding the legacy table should succeed");
    task_id
}

/// The upgrade path, all four branches, against a real server.
#[tokio::test]
async fn a_legacy_result_table_is_upgraded_without_touching_the_result_backends() {
    const NAME: &str = "a_legacy_result_table_is_upgraded_without_touching_the_result_backends";
    let Some(url) = upgrade_url(NAME) else {
        return;
    };

    let broker = MysqlBroker::new(&url)
        .await
        .unwrap_or_else(|e| panic!("connecting to {UPGRADE_URL_ENV} should succeed: {e}"));
    broker
        .migrate()
        .await
        .expect("the baseline migration must apply");

    // ── 1. A database migrated by an older build ────────────────────────
    //
    // The ledger already records `010`, so `run_migration_tracked` skips
    // `010_broker_results.sql` entirely: if the untracked rename did not run,
    // `celers_broker_results` would simply not exist afterwards, and every
    // result path would fail with "table doesn't exist" exactly as it did
    // before this upgrade step was written.
    reset(&broker, true).await;
    let carried_id = seed_legacy_broker_table(&broker).await;

    broker
        .migrate()
        .await
        .expect("migrate() must upgrade a database that still carries the legacy table");

    assert!(
        table_exists(&broker, CURRENT).await,
        "the rename must produce `{CURRENT}` even though the ledger records 010 as applied, \
         so the tracked migration never ran"
    );
    assert!(
        !table_exists(&broker, LEGACY).await,
        "`{LEGACY}` must be gone: it *became* `{CURRENT}`, and leaving a copy behind would \
         strand the rows in it"
    );

    let rows = broker
        .connection()
        .query(
            &format!(
                "SELECT task_name, status, result, error, traceback, runtime_ms \
                 FROM {CURRENT} WHERE task_id = ?"
            ),
            &[&carried_id.as_str()],
        )
        .await
        .expect("reading the carried row should succeed");
    let row = rows.first().expect(
        "the legacy row must survive the rename — RENAME TABLE moves the data, and an \
                 upgrade that silently dropped stored results would be worse than the collision",
    );
    assert_eq!(
        row.col::<String>("task_name").expect("task_name"),
        "legacy.task"
    );
    assert_eq!(row.col::<String>("status").expect("status"), "SUCCESS");
    // `result` is `LONGTEXT` and `error`/`traceback` are `TEXT`; MySQL returns
    // all three over the binary protocol as `Value::Blob`, so they go through
    // the same `opt_text_from_row` helper `broker_results.rs` reads them with
    // rather than `col::<String>` (which is a `TypeMismatch` on a Blob).
    for (column, expected) in [
        ("result", r#"{"carried":"across"}"#),
        ("error", "legacy error"),
        ("traceback", "legacy traceback"),
    ] {
        assert_eq!(
            crate::row_ext::opt_text_from_row(row, column)
                .unwrap_or_else(|e| panic!("reading `{column}` should succeed: {e}")),
            Some(expected.to_string()),
            "`{column}` must survive the rename byte for byte"
        );
    }
    assert_eq!(row.col::<i64>("runtime_ms").expect("runtime_ms"), 4242);

    // The indexes came across under their original names. This is what makes
    // `010_broker_results.sql` safe to re-issue against an upgraded database:
    // its three `CREATE INDEX` statements hit `ERROR 1061 Duplicate key name`,
    // which `execute_migration_statement` treats as the post-condition already
    // holding. If the rename had instead created fresh indexes, an upgraded
    // database would carry two per column.
    let mut indexes = index_names(&broker, CURRENT).await;
    indexes.sort();
    assert_eq!(
        indexes,
        vec![
            "idx_task_results_completed".to_string(),
            "idx_task_results_name".to_string(),
            "idx_task_results_status".to_string(),
        ],
        "RENAME TABLE must carry the legacy indexes across under their existing names"
    );

    // And the upgraded table is usable through the crate's own result API,
    // which is the point of the whole exercise.
    let fresh = Uuid::new_v4();
    broker
        .store_result(
            &fresh,
            "after.upgrade",
            crate::TaskResultStatus::Success,
            Some(serde_json::json!({"after": "upgrade"})),
            None,
            None,
            Some(11),
        )
        .await
        .expect("the upgraded table must accept a new result");
    assert_eq!(
        broker
            .get_result(&fresh)
            .await
            .expect("reading back should succeed")
            .expect("the result must be there")
            .result,
        Some(serde_json::json!({"after": "upgrade"}))
    );

    // Re-running migrate() over an already-upgraded database changes nothing.
    broker
        .migrate()
        .await
        .expect("migrate() must be replayable");
    assert!(table_exists(&broker, CURRENT).await);
    assert!(!table_exists(&broker, LEGACY).await);
    assert_eq!(
        row_count(&broker, CURRENT).await,
        2,
        "a replay must not drop, duplicate or recreate the upgraded table's rows"
    );

    // ── 2. `celers_task_results` belongs to celers-backend-db ───────────
    //
    // The complementary case, and the one that makes the rename safe to ship:
    // the probe must recognise the result backend's table by its own signature
    // column and leave it strictly alone.
    reset(&broker, false).await;
    exec(&broker, BACKEND_DB_SCHEMA).await;
    let backend_id = Uuid::new_v4().to_string();
    broker
        .connection()
        .execute(
            &format!("INSERT INTO {LEGACY} (task_id, task_name, result_state) VALUES (?, ?, ?)"),
            &[&backend_id.as_str(), &"backend.task", &"success"],
        )
        .await
        .expect("seeding the backend's table should succeed");

    broker
        .migrate()
        .await
        .expect("migrate() must coexist with the result backend's table");

    assert!(
        table_exists(&broker, LEGACY).await,
        "the result backend's `{LEGACY}` must still exist — renaming another crate's live \
         result store out from under it is the failure mode this probe exists to prevent"
    );
    assert!(
        column_exists(&broker, LEGACY, "result_state").await,
        "the result backend's table must still carry its own schema"
    );
    assert_eq!(
        row_count(&broker, LEGACY).await,
        1,
        "the result backend's rows must be untouched"
    );
    assert!(
        table_exists(&broker, CURRENT).await,
        "`{CURRENT}` must be created fresh alongside it"
    );
    assert!(
        column_exists(&broker, CURRENT, "traceback").await,
        "and with this crate's schema, not the backend's"
    );
    assert_eq!(
        row_count(&broker, CURRENT).await,
        0,
        "nothing may be copied out of the backend's table into ours"
    );

    // ── 3. Both names present, the legacy one still broker-shaped ───────
    //
    // An operator who half-completed the upgrade by hand. `RENAME TABLE` would
    // fail with ERROR 1050, so the step must notice and do nothing — and, in
    // particular, must not delete the rows it cannot move.
    reset(&broker, false).await;
    broker
        .migrate()
        .await
        .expect("a clean migration must apply");
    let stranded_id = seed_legacy_broker_table(&broker).await;

    broker
        .migrate()
        .await
        .expect("migrate() must not fail when both result tables exist");

    assert!(table_exists(&broker, CURRENT).await);
    assert!(
        table_exists(&broker, LEGACY).await,
        "with both present the legacy table is left in place, not dropped"
    );
    let stranded = broker
        .connection()
        .query(
            &format!("SELECT task_name FROM {LEGACY} WHERE task_id = ?"),
            &[&stranded_id.as_str()],
        )
        .await
        .expect("reading the stranded row should succeed");
    assert_eq!(
        stranded.len(),
        1,
        "the stranded rows must still be there for the operator to move by hand — the warning \
         `rename_legacy_broker_results_table` logs is the whole remedy"
    );

    // ── 4. An ambiguous legacy table ────────────────────────────────────
    //
    // Carries both crates' signature columns, so the probe cannot tell whose
    // it is. Refusing to guess is the only safe answer.
    reset(&broker, false).await;
    exec(&broker, AMBIGUOUS_SCHEMA).await;

    broker
        .migrate()
        .await
        .expect("migrate() must not fail on an ambiguous legacy table");

    assert!(
        table_exists(&broker, LEGACY).await,
        "an ambiguous table must be left exactly where it is"
    );
    assert!(
        column_exists(&broker, LEGACY, "result_state").await
            && column_exists(&broker, LEGACY, "traceback").await,
        "…with both signature columns intact"
    );
    assert!(
        table_exists(&broker, CURRENT).await,
        "and `{CURRENT}` is still created, so the broker works regardless"
    );

    // Leave the database in the shape a normal migration produces.
    reset(&broker, false).await;
    broker
        .migrate()
        .await
        .expect("the closing migration must apply");
}

/// The rename is a pure rename: `010_broker_results.sql` must declare exactly
/// the columns the deleted `010_task_results.sql` did, so a renamed table and a
/// freshly created one are the same table.
///
/// Server-free, so it protects [`LEGACY_BROKER_SCHEMA`] from drifting away from
/// the migration even when nobody runs the gated half. If a future migration
/// adds a column to `celers_broker_results`, this fails and points at the fact
/// that upgraded databases need the same column added.
#[test]
fn the_legacy_fixture_matches_the_current_schema_except_for_the_name() {
    /// The column names in a `CREATE TABLE` body, in declaration order.
    ///
    /// Anchored on the `CREATE TABLE` line itself rather than on the first
    /// line containing a `(`: `010_broker_results.sql` opens with a long prose
    /// comment whose parentheses would otherwise start the scan in the middle
    /// of it. Comment lines inside the body are dropped, and the body ends at
    /// the first line starting with `)`.
    fn columns(ddl: &str) -> Vec<String> {
        ddl.lines()
            .map(str::trim)
            .skip_while(|line| {
                !line
                    .to_ascii_uppercase()
                    .split_whitespace()
                    .take(2)
                    .eq(["CREATE", "TABLE"])
            })
            .skip(1)
            .take_while(|line| !line.starts_with(')'))
            .filter(|line| !line.is_empty() && !line.starts_with("--"))
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_ascii_lowercase)
            .collect()
    }

    let current = include_str!("../../migrations/010_broker_results.sql");
    assert_eq!(
        columns(LEGACY_BROKER_SCHEMA),
        columns(current),
        "the legacy fixture and `010_broker_results.sql` must declare the same columns: the \
         upgrade is a `RENAME TABLE`, which changes the name and nothing else, so any column \
         that exists in only one of them is a column upgraded databases would silently lack"
    );
    assert!(
        current.contains(CURRENT) && !current.contains(&format!("EXISTS {LEGACY}")),
        "`010_broker_results.sql` must create `{CURRENT}`, never `{LEGACY}`"
    );
}
