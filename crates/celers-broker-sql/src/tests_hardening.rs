//! Regression tests for the hardened claim / ack / reject spine.
//!
//! Split into two halves, one per file:
//!
//! * **Deterministic tests**, here, needing no server. These are the ones that
//!   actually protect the fixes in CI: SQL text, migration text, and the
//!   source-level guards (every task INSERT binds `queue_name`; no path
//!   hand-writes a locking `SELECT`; no path issues SQL against
//!   `celers-backend-db`'s result table; every bulk maintenance statement is
//!   deadlock-retried).
//! * **Integration tests** in [`integration`], gated on [`TEST_URL_ENV`].
//!   They early-return (rather than being `#[ignore]`d) so
//!   `cargo nextest run --all-features` is green with no server available and
//!   automatically exercises the real database when one is configured. That
//!   module lives in its own file because the two halves together exceeded the
//!   2000-line limit.

use crate::broker_migrate::strip_sql_line_comments;

/// Environment variable naming a MySQL 8.0.1+ / MariaDB 10.6+ instance to run
/// the integration half against, e.g.
/// `mysql://celers:celers_password@127.0.0.1:3306/celers_test`.
///
/// Read by [`integration::test_mysql_url`], which every gated test funnels
/// through.
const TEST_URL_ENV: &str = "CELERS_TEST_MYSQL_URL";

/// Every migration file `migrate()` applies, in application order.
const APPLIED_MIGRATIONS: &[(&str, &str)] = &[
    (
        "000_migrations.sql",
        include_str!("../migrations/000_migrations.sql"),
    ),
    ("001_init.sql", include_str!("../migrations/001_init.sql")),
    (
        "002_results.sql",
        include_str!("../migrations/002_results.sql"),
    ),
    (
        "003_performance_indexes.sql",
        include_str!("../migrations/003_performance_indexes.sql"),
    ),
    (
        "006_idempotency.sql",
        include_str!("../migrations/006_idempotency.sql"),
    ),
    (
        "007_workflow.sql",
        include_str!("../migrations/007_workflow.sql"),
    ),
    (
        "008_production_features.sql",
        include_str!("../migrations/008_production_features.sql"),
    ),
    (
        "009_queue_name.sql",
        include_str!("../migrations/009_queue_name.sql"),
    ),
    (
        "010_broker_results.sql",
        include_str!("../migrations/010_broker_results.sql"),
    ),
    (
        "011_revocation.sql",
        include_str!("../migrations/011_revocation.sql"),
    ),
];

/// Source files containing a `celers_tasks` INSERT.
const TASK_INSERT_SOURCES: &[(&str, &str)] = &[
    ("broker_trait.rs", include_str!("broker_trait.rs")),
    ("broker_dequeue.rs", include_str!("broker_dequeue.rs")),
    ("broker_core.rs", include_str!("broker_core.rs")),
    ("broker_hooks.rs", include_str!("broker_hooks.rs")),
    ("broker_batch.rs", include_str!("broker_batch.rs")),
    ("broker_advanced.rs", include_str!("broker_advanced.rs")),
    ("broker_enhanced.rs", include_str!("broker_enhanced.rs")),
    ("broker_resilience.rs", include_str!("broker_resilience.rs")),
];

/// Drop Rust comment lines so a source-scanning guard cannot trip over the
/// prose that documents the very pattern it forbids.
fn strip_rust_line_comments(source: &str) -> String {
    source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ========== Migration text ==========

/// `run_migration` splits on `;`, which puts each statement in the same chunk
/// as the comment block preceding it. It used to *skip* any chunk starting
/// with `--`, discarding the statement along with its comment — which dropped
/// `CREATE TABLE celers_migrations` and therefore made `migrate()` fail on
/// every fresh database.
#[test]
fn comment_stripping_keeps_the_statement_after_a_comment_block() {
    let chunk = "-- A title comment\n-- and a second line\n\nCREATE TABLE t (a INT)";
    let stripped = strip_sql_line_comments(chunk);
    assert!(stripped.contains("CREATE TABLE t (a INT)"));
    assert!(!stripped.contains("title comment"));
    assert!(!stripped.trim().is_empty());
}

#[test]
fn comment_stripping_leaves_a_comment_only_chunk_empty() {
    assert!(strip_sql_line_comments("-- only a comment\n-- and another")
        .trim()
        .is_empty());
    assert!(strip_sql_line_comments("").trim().is_empty());
}

#[test]
fn comment_stripping_does_not_touch_inline_dashes() {
    let chunk = "INSERT INTO t (v) VALUES ('a -- not a comment')";
    assert_eq!(strip_sql_line_comments(chunk), chunk);
}

/// Every migration file opens with a title comment; each must still yield at
/// least one executable statement after stripping.
#[test]
fn every_applied_migration_yields_executable_statements() {
    for (name, sql) in APPLIED_MIGRATIONS {
        let main_section = sql.split("DELIMITER //").next().unwrap_or(sql);
        let executable = strip_sql_line_comments(main_section)
            .split(';')
            .filter(|statement| !statement.trim().is_empty())
            .count();
        assert!(
            executable > 0,
            "{name} produced no executable statements; \
             the whole migration would silently do nothing"
        );
    }
}

/// `CREATE INDEX IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS` are
/// MariaDB-only; MySQL 8 rejects both with a parse error. Migration tracking
/// already provides run-once semantics, so the guards are unnecessary as well
/// as unportable.
#[test]
fn migrations_avoid_mariadb_only_if_not_exists_syntax() {
    for (name, sql) in APPLIED_MIGRATIONS {
        // Comments are allowed to *name* the forbidden syntax; statements are not.
        let normalized = strip_sql_line_comments(sql).to_ascii_uppercase();
        for forbidden in ["CREATE INDEX IF NOT EXISTS", "ADD COLUMN IF NOT EXISTS"] {
            assert!(
                !normalized.contains(forbidden),
                "{name} uses MariaDB-only syntax `{forbidden}`, which MySQL 8 rejects"
            );
        }
    }
}

/// `000_migrations.sql` runs untracked on *every* `migrate()` call, so every
/// statement in it must be idempotent.
#[test]
fn the_untracked_migration_is_idempotent() {
    let sql = include_str!("../migrations/000_migrations.sql");
    for statement in strip_sql_line_comments(sql).split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        let normalized = statement.to_ascii_uppercase();
        assert!(
            normalized.starts_with("CREATE TABLE IF NOT EXISTS"),
            "000_migrations.sql is re-run on every migrate() call, so `{statement}` \
             would fail the second time"
        );
    }
}

#[test]
fn queue_name_migration_adds_an_indexed_backfilled_column() {
    let sql = include_str!("../migrations/009_queue_name.sql");
    assert!(sql.contains("ADD COLUMN queue_name VARCHAR(255) NOT NULL DEFAULT 'default'"));
    assert!(
        sql.contains("JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.queue'))"),
        "existing rows must be backfilled from the JSON label they were enqueued with"
    );
    assert!(
        sql.contains("ON celers_tasks(queue_name, state, scheduled_at, priority, created_at)"),
        "the dequeue predicate needs a leading-queue_name composite index"
    );
}

#[test]
fn revocation_migration_creates_a_microsecond_precision_queue_scoped_table() {
    let sql = include_str!("../migrations/011_revocation.sql");
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS celers_revoked_tasks"));
    assert!(
        sql.contains("PRIMARY KEY (queue_name, task_id)"),
        "revocation is queue-scoped, not global"
    );
    assert!(
        sql.contains("revoked_at DATETIME(6)") && sql.contains("expires_at DATETIME(6)"),
        "the poller's cursor needs sub-second precision, or same-second \
         revocations tie far more often than necessary"
    );
    assert!(
        sql.contains("idx_revoked_tasks_poll") && sql.contains("(queue_name, revoked_at)"),
        "the poller's `queue_name = ? AND revoked_at > ?` query needs a covering index"
    );
}

/// Index names must be unique across the whole migration chain.
///
/// MySQL has no `CREATE INDEX IF NOT EXISTS`, so a name reused by a later
/// migration fails with "Duplicate key name" and aborts `migrate()` partway
/// through, on a fresh database, with some tables already created.
#[test]
fn migrations_do_not_reuse_index_names() {
    let mut seen: Vec<(String, &str)> = Vec::new();
    for (file, sql) in APPLIED_MIGRATIONS {
        for statement in strip_sql_line_comments(sql).split(';') {
            let statement = statement.trim();
            let Some(rest) = statement.strip_prefix("CREATE INDEX ") else {
                continue;
            };
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string();
            if let Some((_, first_file)) = seen.iter().find(|(known, _)| known == &name) {
                panic!("index `{name}` is created in both {first_file} and {file}");
            }
            seen.push((name, file));
        }
    }
    assert!(
        seen.len() >= 15,
        "expected the migration chain to create many indexes, found {}",
        seen.len()
    );
}

/// The claim path's covering index must exist somewhere in the chain.
#[test]
fn the_dequeue_covering_index_is_created() {
    let created = APPLIED_MIGRATIONS
        .iter()
        .any(|(_, sql)| sql.contains("idx_tasks_queue_dequeue"));
    assert!(
        created,
        "no migration creates the queue-scoped dequeue index"
    );
}

// ========== Source-level guards ==========

/// Every `INSERT INTO celers_tasks` must bind the `queue_name` column.
///
/// A missed INSERT site is invisible at compile time and silently drops the
/// task into the `'default'` queue, where the broker that enqueued it will
/// never claim it again.
#[test]
fn every_task_insert_binds_queue_name() {
    let mut checked = 0usize;
    for (name, raw) in TASK_INSERT_SOURCES {
        let source = strip_rust_line_comments(raw);
        for (offset, _) in source.match_indices("INSERT INTO celers_tasks") {
            // The column list ends at the first `)` after the table name.
            let tail = &source[offset..];
            let column_list_end = tail
                .find(')')
                .unwrap_or_else(|| panic!("{name}: unterminated INSERT column list"));
            let column_list = &tail[..column_list_end];
            assert!(
                column_list.contains("queue_name"),
                "{name}: an INSERT INTO celers_tasks does not bind queue_name:\n{column_list}"
            );
            checked += 1;
        }
    }
    assert!(
        checked >= 8,
        "expected to find every task INSERT site, only found {checked}"
    );
}

/// No dequeue path may embed its own copy of the claim statement again: all of
/// them must go through `sql_text::dequeue_claim_sql`.
#[test]
fn no_source_file_hand_writes_a_locking_select() {
    for (name, raw) in TASK_INSERT_SOURCES {
        let source = strip_rust_line_comments(raw);
        assert!(
            !source.contains("FOR UPDATE SKIP LOCKED"),
            "{name} embeds a hand-written locking SELECT; \
             use crate::sql_text::dequeue_claim_sql instead"
        );
    }
}

/// Source files that issue SQL against the broker's own result table.
const RESULT_SQL_SOURCES: &[(&str, &str)] = &[
    ("broker_results.rs", include_str!("broker_results.rs")),
    ("broker_batch.rs", include_str!("broker_batch.rs")),
    (
        "broker_diagnostics.rs",
        include_str!("broker_diagnostics.rs"),
    ),
    ("broker_resilience.rs", include_str!("broker_resilience.rs")),
    ("broker_advanced.rs", include_str!("broker_advanced.rs")),
    ("broker_core.rs", include_str!("broker_core.rs")),
    ("broker_migrate.rs", include_str!("broker_migrate.rs")),
    ("sql_text.rs", include_str!("sql_text.rs")),
];

/// `celers_task_results` belongs to `celers-backend-db` (Known gaps #16); the
/// broker's own store is `celers_broker_results`. A statement that reaches for
/// the old name on a shared database reads or writes the *result backend's*
/// rows, whose schema shares only `task_id`, `task_name` and `created_at`.
#[test]
fn no_source_file_issues_sql_against_the_backends_result_table() {
    for (name, raw) in RESULT_SQL_SOURCES {
        let source = strip_rust_line_comments(raw);
        for forbidden in [
            "INTO celers_task_results",
            "FROM celers_task_results",
            "UPDATE celers_task_results",
            "TABLE celers_task_results",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} still issues `{forbidden}`; that table belongs to \
                 celers-backend-db — the broker's own store is celers_broker_results"
            );
        }
    }
}

/// The rename is only half done if the migration still creates the old name.
#[test]
fn the_result_migration_creates_the_broker_specific_table() {
    let ddl = strip_sql_line_comments(include_str!("../migrations/010_broker_results.sql"));
    assert!(
        ddl.contains("CREATE TABLE IF NOT EXISTS celers_broker_results"),
        "migration 010 must create the broker's own result table"
    );
    assert!(
        !ddl.contains("celers_task_results"),
        "migration 010 must not touch celers-backend-db's table"
    );
    // `RENAME TABLE` carries indexes across under their existing names, so
    // re-creating them under *fresh* names would leave an upgraded database
    // with two indexes per column. Keeping the old names makes the re-issue a
    // tolerated `ERROR 1061` instead.
    for index in [
        "idx_task_results_name",
        "idx_task_results_status",
        "idx_task_results_completed",
    ] {
        assert!(
            ddl.contains(index),
            "migration 010 must keep the pre-rename index name `{index}`"
        );
    }
}

/// Every bulk maintenance statement must be wrapped in the crate's deadlock
/// retry.
///
/// One `DELETE`/`UPDATE` over an unbounded row set, touching every secondary
/// index on `celers_tasks`, is the shape most likely to lose an InnoDB lock
/// cycle to the claim/ack traffic running beside it. InnoDB resolves a cycle
/// by rolling one transaction back with `ERROR 1213 (40001)`, and unretried
/// that reaches the operator as a spurious maintenance failure.
#[test]
fn bulk_maintenance_statements_are_deadlock_retried() {
    let source = strip_rust_line_comments(include_str!("broker_core.rs"));
    for operation in [
        "purge_all",
        "purge_by_state",
        "purge_by_task_name",
        "archive_completed_tasks",
        "recover_stuck_tasks",
    ] {
        assert!(
            source.contains(&format!("with_deadlock_retry(\"{operation}\"")),
            "{operation} issues a bulk statement without with_deadlock_retry; \
             an ERROR 1213 there surfaces as a spurious failure"
        );
    }
}

/// The overflow-prone `2_i64.pow(retry_count as u32)` must not come back.
#[test]
fn no_source_file_uses_unchecked_pow_for_backoff() {
    let sources = TASK_INSERT_SOURCES.iter().copied().chain(std::iter::once((
        "broker_chain.rs",
        include_str!("broker_chain.rs"),
    )));
    for (name, raw) in sources {
        let source = strip_rust_line_comments(raw);
        assert!(
            !source.contains("2_i64.pow("),
            "{name} uses the panicking 2_i64.pow backoff; \
             use crate::backoff::retry_backoff_seconds instead"
        );
    }
}

// ========== Integration tests (require CELERS_TEST_MYSQL_URL) ==========

mod integration;

// ========== Migration upgrade path ==========
//
// The `celers_task_results` -> `celers_broker_results` rename, which needs a
// database it may destroy tables in and so reads its own
// `CELERS_TEST_MYSQL_UPGRADE_URL` rather than `TEST_URL_ENV`. See the module's
// doc comment for why it cannot share the suite's database.

mod migration_upgrade;
