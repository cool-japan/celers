//! Schema migration for `MysqlBroker`.
//!
//! Extracted from `broker_core.rs`, which exceeded the 2000-line limit once
//! the two coexistence upgrades below landed. Everything here is reachable
//! only through [`MysqlBroker::migrate`]:
//!
//! * the version-tracked migration runner (`celers_migrations`, the
//!   `DELIMITER`-aware statement splitter, and the concurrent-migrator error
//!   tolerances), and
//! * two **untracked** upgrade steps that must run even on a database whose
//!   tracked migrations are all recorded, because both rename an object that
//!   `celers-backend-db` also creates in the same schema:
//!   [`MysqlBroker::rename_legacy_broker_results_table`] and
//!   [`MysqlBroker::rename_legacy_result_state_constraint`].

use crate::broker_core::MysqlBroker;
use crate::mysql_error::{
    is_duplicate_column_name, is_duplicate_key_name, is_unsupported_in_prepared_protocol,
    with_deadlock_retry,
};
use crate::row_ext::RowExt;
use crate::sql_text::{
    BACKEND_RESULTS_SIGNATURE_COLUMN, BROKER_LEGACY_RESULTS_TABLE, BROKER_RESULTS_SIGNATURE_COLUMN,
    BROKER_RESULTS_TABLE, BROKER_RESULT_STATE_CONSTRAINT, BROKER_RESULT_STATE_PREDICATE,
    LEGACY_BROKER_RESULTS_TABLE, LEGACY_RESULT_STATE_CONSTRAINT,
};
use celers_core::{CelersError, Result};
use oxisql_core::Connection;

/// Remove whole-line `--` comments from one `;`-delimited migration chunk.
///
/// Line-based on purpose: it never inspects the interior of a statement, so a
/// `--` sequence inside a string literal or an identifier is untouched. Only
/// lines whose first non-whitespace characters are `--` are dropped.
pub(crate) fn strip_sql_line_comments(chunk: &str) -> String {
    chunk
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Is `statement` a `CREATE INDEX`?
///
/// Leading `--` comment lines are already removed by
/// [`strip_sql_line_comments`] before the `;` split, so the keyword is the
/// first token.
pub(crate) fn is_create_index(statement: &str) -> bool {
    let mut words = statement.split_whitespace();
    matches!(words.next(), Some(w) if w.eq_ignore_ascii_case("CREATE"))
        && matches!(words.next(), Some(w) if w.eq_ignore_ascii_case("INDEX"))
}

/// Is `statement` an `ALTER TABLE ... ADD COLUMN ...`?
pub(crate) fn is_add_column(statement: &str) -> bool {
    let upper = statement.to_ascii_uppercase();
    upper.trim_start().starts_with("ALTER TABLE") && upper.contains("ADD COLUMN")
}
impl MysqlBroker {
    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        // First, create migrations table if it doesn't exist
        self.run_migration_untracked(include_str!("../migrations/000_migrations.sql"))
            .await?;

        // Run migrations with tracking
        self.run_migration_tracked(
            "001",
            "initial_schema",
            include_str!("../migrations/001_init.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "002",
            "results_table",
            include_str!("../migrations/002_results.sql"),
        )
        .await?;

        // Untracked and after 002 on purpose: on a fresh database 002 has
        // just created the constraint under its new name and this is a
        // no-op, while on a pre-rename database 002 is recorded and skipped,
        // so this is the only thing that can upgrade it.
        self.rename_legacy_result_state_constraint().await?;

        self.run_migration_tracked(
            "003",
            "performance_indexes",
            include_str!("../migrations/003_performance_indexes.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "006",
            "idempotency_keys",
            include_str!("../migrations/006_idempotency.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "007",
            "workflow_dag",
            include_str!("../migrations/007_workflow.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "008",
            "production_features",
            include_str!("../migrations/008_production_features.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "009",
            "queue_name_column",
            include_str!("../migrations/009_queue_name.sql"),
        )
        .await?;

        // Untracked, idempotent, and deliberately *before* migration 010: it
        // must run even on a database that already recorded version `010`
        // under the old table name. See its own doc comment.
        self.rename_legacy_broker_results_table().await?;

        self.run_migration_tracked(
            "010",
            "broker_results_table",
            include_str!("../migrations/010_broker_results.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "011",
            "revocation",
            include_str!("../migrations/011_revocation.sql"),
        )
        .await?;

        Ok(())
    }

    /// Does `table` exist in the database this broker is connected to?
    ///
    /// `information_schema.tables` is the portable place to ask — it exists on
    /// MySQL 5.7, MySQL 8 and MariaDB alike. `table_schema = DATABASE()` is
    /// load-bearing: without it a same-named table in *another* schema on the
    /// same server is a false positive, and this probe decides whether to
    /// issue a `RENAME TABLE`.
    async fn table_exists(&self, table: &str) -> Result<bool> {
        let rows = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = DATABASE() \
                   AND table_name = ? \
                 LIMIT 1",
                &[&table],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to check for table '{table}': {e}")))?;
        Ok(!rows.is_empty())
    }

    /// Does `table` carry a column named `column` in the current database?
    ///
    /// Same shape (and same `table_schema = DATABASE()` scoping) as
    /// [`Self::table_exists`]; mirrors `celers-backend-db`'s
    /// `MysqlResultBackend::migrate` column probe.
    async fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let rows = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() \
                   AND table_name = ? \
                   AND column_name = ? \
                 LIMIT 1",
                &[&table, &column],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to check for column '{column}' on '{table}': {e}"
                ))
            })?;
        Ok(!rows.is_empty())
    }

    /// Upgrade a database that still carries this crate's result table under
    /// its old `celers_task_results` name.
    ///
    /// # Why the rename exists
    ///
    /// `celers-backend-db`'s `MysqlResultBackend` also auto-migrates a table
    /// called `celers_task_results`, with an *incompatible* schema
    /// (`result_state` / `result_data` / `extra` there;
    /// `status` / `result` / `traceback` here). On a shared MySQL database
    /// whichever crate migrated second failed: `CREATE TABLE IF NOT EXISTS`
    /// no-op'd against the other's table and the follow-up `CREATE INDEX`
    /// died with `ERROR 1072 (42000): Key column 'status' doesn't exist in
    /// table`. `celers-backend-db` is CeleRS's result *backend* and keeps the
    /// name; this broker-internal store moved to `celers_broker_results`
    /// (migration `010_broker_results.sql`).
    ///
    /// # The upgrade decision
    ///
    /// The probe is positive, narrow, and never destructive:
    ///
    /// * **No legacy table** — nothing to upgrade; migration 010 creates the
    ///   new name.
    /// * **Legacy table carries a broker-only column** ([`BROKER_RESULTS_SIGNATURE_COLUMN`],
    ///   which `celers-backend-db`'s schema has no counterpart for) **and the
    ///   new name is free** — `RENAME TABLE`, which preserves every row and
    ///   carries the table's indexes across under their existing names. This
    ///   is the actual upgrade path.
    /// * **Legacy table carries `celers-backend-db`'s signature column
    ///   instead** — it is the result backend's live store. Left completely
    ///   alone; migration 010 then creates `celers_broker_results` fresh
    ///   alongside it, which is the coexistence case.
    /// * **Both names already present** — an upgrade already happened (and
    ///   the remaining legacy table is the backend's), or an operator created
    ///   both. `RENAME TABLE` would fail with `ERROR 1050`, so nothing is
    ///   done; a legacy table that still looks like *this* crate's is warned
    ///   about, because rows in it would otherwise be silently unreachable.
    ///
    /// The legacy table is never dropped and never written to under any
    /// branch: on a shared database it is another crate's data.
    ///
    /// # Idempotence and concurrency
    ///
    /// Untracked on purpose — it must run even on a database that already
    /// recorded migration `010` under the old name, and every branch is a
    /// no-op once the upgrade has happened. Two brokers migrating
    /// concurrently can both observe "legacy present, new name free" and both
    /// issue the `RENAME`; the loser's failure is re-probed rather than
    /// propagated, exactly like `execute_migration_statement`'s duplicate-key
    /// handling.
    async fn rename_legacy_broker_results_table(&self) -> Result<()> {
        if !self.table_exists(LEGACY_BROKER_RESULTS_TABLE).await? {
            return Ok(());
        }

        let legacy_is_ours = self
            .column_exists(LEGACY_BROKER_RESULTS_TABLE, BROKER_RESULTS_SIGNATURE_COLUMN)
            .await?;

        if self.table_exists(BROKER_RESULTS_TABLE).await? {
            if legacy_is_ours {
                tracing::warn!(
                    legacy = LEGACY_BROKER_RESULTS_TABLE,
                    current = BROKER_RESULTS_TABLE,
                    "both result tables exist and the legacy one still carries this crate's \
                     schema; leaving it untouched — move any rows it still holds across by hand"
                );
            }
            return Ok(());
        }

        if !legacy_is_ours {
            tracing::debug!(
                legacy = LEGACY_BROKER_RESULTS_TABLE,
                "legacy result table has no `{}` column, so it belongs to celers-backend-db; \
                 leaving it alone",
                BROKER_RESULTS_SIGNATURE_COLUMN
            );
            return Ok(());
        }

        if self
            .column_exists(
                LEGACY_BROKER_RESULTS_TABLE,
                BACKEND_RESULTS_SIGNATURE_COLUMN,
            )
            .await?
        {
            tracing::warn!(
                legacy = LEGACY_BROKER_RESULTS_TABLE,
                "legacy result table carries both this crate's `{}` column and \
                 celers-backend-db's `{}` column; the schema is ambiguous, so it is left \
                 untouched",
                BROKER_RESULTS_SIGNATURE_COLUMN,
                BACKEND_RESULTS_SIGNATURE_COLUMN
            );
            return Ok(());
        }

        let rename_sql =
            format!("RENAME TABLE {LEGACY_BROKER_RESULTS_TABLE} TO {BROKER_RESULTS_TABLE}");
        match self.conn.execute(&rename_sql, &[]).await {
            Ok(_) => {
                tracing::info!(
                    from = LEGACY_BROKER_RESULTS_TABLE,
                    to = BROKER_RESULTS_TABLE,
                    "renamed the broker's legacy result table (rows and indexes preserved)"
                );
                Ok(())
            }
            // A concurrent migrator won the race: re-probe rather than fail.
            Err(e)
                if self
                    .table_exists(BROKER_RESULTS_TABLE)
                    .await
                    .unwrap_or(false) =>
            {
                tracing::debug!(
                    error = %e,
                    "result table was renamed by a concurrent migration; continuing"
                );
                Ok(())
            }
            Err(e) => Err(CelersError::Other(format!(
                "Failed to rename {LEGACY_BROKER_RESULTS_TABLE} to {BROKER_RESULTS_TABLE}: {e}"
            ))),
        }
    }

    /// Does `table` carry a CHECK constraint named `constraint_name` in the
    /// current database?
    ///
    /// `information_schema.table_constraints` is the portable place to ask.
    /// Note that MySQL 8.0.1–8.0.15 *parse and ignore* CHECK constraints
    /// rather than creating them, so on those servers this correctly reports
    /// `false` — there is nothing there to rename, and nothing that can
    /// collide either.
    async fn check_constraint_exists(&self, table: &str, constraint_name: &str) -> Result<bool> {
        let rows = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.table_constraints \
                 WHERE table_schema = DATABASE() \
                   AND table_name = ? \
                   AND constraint_name = ? \
                   AND constraint_type = 'CHECK' \
                 LIMIT 1",
                &[&table, &constraint_name],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to check for constraint '{constraint_name}' on '{table}': {e}"
                ))
            })?;
        Ok(!rows.is_empty())
    }

    /// Upgrade a database whose `celers_results` still carries the
    /// schema-colliding `chk_result_state` CHECK constraint.
    ///
    /// # Why
    ///
    /// MySQL scopes CHECK constraint names to the **schema**, not the table.
    /// `celers-backend-db`'s `celers_task_results` declares its own
    /// `chk_result_state`, so on the shared database a normal deployment uses,
    /// whichever crate migrated second failed with `ERROR 3822 (HY000):
    /// Duplicate check constraint name 'chk_result_state'`. That is the same
    /// coexistence defect as the `celers_task_results` table name (Known gaps
    /// #16) wearing a different hat, and it was found by the very test written
    /// to prove the table rename worked. `002_results.sql` now names the
    /// constraint `chk_broker_result_state`; this brings existing databases
    /// across.
    ///
    /// # Portability of the drop
    ///
    /// There is no single spelling that every supported server accepts:
    /// MySQL 8.0.16+ takes `DROP CHECK`, MariaDB (and MySQL 8.0.19+) take
    /// `DROP CONSTRAINT`. Both are attempted. A server that is running a
    /// CHECK constraint at all accepts one of them, so the fallback is not a
    /// guess — it is the two spellings of one operation.
    ///
    /// # Never fatal
    ///
    /// If, after both attempts, the legacy constraint is still there, this
    /// logs the manual remedy and returns `Ok`. Breaking `migrate()` — and
    /// therefore startup — on an existing deployment over a name collision
    /// with a crate that deployment may not even use would be a far worse
    /// outcome than the collision itself. Concurrent migrators are handled by
    /// the same re-probe: whichever loses simply finds the work already done.
    async fn rename_legacy_result_state_constraint(&self) -> Result<()> {
        if !self
            .check_constraint_exists(BROKER_LEGACY_RESULTS_TABLE, LEGACY_RESULT_STATE_CONSTRAINT)
            .await?
        {
            return Ok(());
        }

        for drop_clause in ["DROP CHECK", "DROP CONSTRAINT"] {
            let sql = format!(
                "ALTER TABLE {BROKER_LEGACY_RESULTS_TABLE} \
                 {drop_clause} {LEGACY_RESULT_STATE_CONSTRAINT}"
            );
            match self.conn.execute(&sql, &[]).await {
                Ok(_) => break,
                Err(e) => tracing::debug!(
                    error = %e,
                    "`{drop_clause}` is not the spelling this server accepts; trying the other"
                ),
            }
        }

        if self
            .check_constraint_exists(BROKER_LEGACY_RESULTS_TABLE, LEGACY_RESULT_STATE_CONSTRAINT)
            .await?
        {
            tracing::warn!(
                "could not drop the legacy `{LEGACY_RESULT_STATE_CONSTRAINT}` constraint on \
                 `{BROKER_LEGACY_RESULTS_TABLE}`; celers-backend-db's migrations will collide \
                 with it on this database. Remedy by hand: ALTER TABLE \
                 {BROKER_LEGACY_RESULTS_TABLE} DROP CHECK {LEGACY_RESULT_STATE_CONSTRAINT}; \
                 ALTER TABLE {BROKER_LEGACY_RESULTS_TABLE} ADD CONSTRAINT \
                 {BROKER_RESULT_STATE_CONSTRAINT} CHECK ({BROKER_RESULT_STATE_PREDICATE});"
            );
            return Ok(());
        }

        // Re-add under the broker-specific name. A concurrent migrator that
        // got here first makes this a duplicate, which is the post-condition
        // this wanted anyway.
        if !self
            .check_constraint_exists(BROKER_LEGACY_RESULTS_TABLE, BROKER_RESULT_STATE_CONSTRAINT)
            .await?
        {
            let sql = format!(
                "ALTER TABLE {BROKER_LEGACY_RESULTS_TABLE} \
                 ADD CONSTRAINT {BROKER_RESULT_STATE_CONSTRAINT} \
                 CHECK ({BROKER_RESULT_STATE_PREDICATE})"
            );
            if let Err(e) = self.conn.execute(&sql, &[]).await {
                if !self
                    .check_constraint_exists(
                        BROKER_LEGACY_RESULTS_TABLE,
                        BROKER_RESULT_STATE_CONSTRAINT,
                    )
                    .await?
                {
                    return Err(CelersError::Other(format!(
                        "Failed to re-add {BROKER_RESULT_STATE_CONSTRAINT} on \
                         {BROKER_LEGACY_RESULTS_TABLE}: {e}"
                    )));
                }
            }
        }

        tracing::info!(
            from = LEGACY_RESULT_STATE_CONSTRAINT,
            to = BROKER_RESULT_STATE_CONSTRAINT,
            table = BROKER_LEGACY_RESULTS_TABLE,
            "renamed the schema-scoped CHECK constraint so celers-backend-db can share the database"
        );
        Ok(())
    }

    /// Check if a migration has been applied
    async fn is_migration_applied(&self, version: &str) -> Result<bool> {
        let rows = self
            .conn
            .query(
                "SELECT COUNT(*) AS c FROM celers_migrations WHERE version = ?",
                &[&version],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to check migration status: {}", e)))?;

        let count: i64 = rows
            .first()
            .map(|row| row.col("c"))
            .transpose()
            .map_err(|e| CelersError::Other(format!("Failed to check migration status: {e}")))?
            .unwrap_or(0);

        Ok(count > 0)
    }

    /// Mark a migration as applied
    async fn mark_migration_applied(&self, version: &str, name: &str) -> Result<()> {
        // `ON DUPLICATE KEY UPDATE` rather than a bare `INSERT`: two brokers
        // starting together both pass `is_migration_applied` (a
        // check-then-act with nothing serialising it), both apply the — now
        // idempotent — DDL, and both reach here. A bare INSERT made the
        // loser fail with `Duplicate entry '001' for key
        // celers_migrations.version`, turning a harmless duplicate effort
        // into a failed `migrate()`. Re-asserting `name` is a no-op write
        // that keeps the statement a single round trip.
        self.conn
            .execute(
                "INSERT INTO celers_migrations (version, name) VALUES (?, ?) \
                 ON DUPLICATE KEY UPDATE name = VALUES(name)",
                &[&version, &name],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to mark migration as applied: {}", e))
            })?;

        tracing::info!(version = %version, name = %name, "Migration applied");
        Ok(())
    }

    /// Run a migration with tracking
    async fn run_migration_tracked(
        &self,
        version: &str,
        name: &str,
        migration_sql: &str,
    ) -> Result<()> {
        // Check if already applied
        if self.is_migration_applied(version).await? {
            tracing::debug!(version = %version, name = %name, "Migration already applied, skipping");
            return Ok(());
        }

        // Run the migration
        self.run_migration_untracked(migration_sql).await?;

        // Mark as applied
        self.mark_migration_applied(version, name).await?;

        Ok(())
    }

    /// Execute one migration DDL statement, tolerating the ways a *concurrent*
    /// migration of the same database can make it fail.
    ///
    /// `run_migration_tracked`'s "already applied?" lookup is a check-then-act
    /// with nothing serialising it, and MySQL offers no usable lock to close
    /// that window here: `CREATE INDEX IF NOT EXISTS` does not exist in any
    /// released MySQL, `ADD COLUMN IF NOT EXISTS` needs 8.0.29+ (this schema
    /// targets 5.7+), and `GET_LOCK` is session-scoped while every
    /// `execute` checks a fresh connection out of `mysql_async`'s pool.
    ///
    /// So each statement is made *effectively* idempotent instead. Every case
    /// below is one where the post-condition the migration wanted already
    /// holds, or where MySQL itself says to retry:
    ///
    /// * `1061 Duplicate key name` — another migrator created the index.
    /// * `1060 Duplicate column name` — another migrator added the column.
    /// * `1213 Deadlock found ... try restarting transaction` — concurrent DDL
    ///   on the same table. MySQL's own advice is to retry, which is what
    ///   [`with_deadlock_retry`] does (shared with the application statements
    ///   that can lose the same race, so there is one retry policy in the
    ///   crate rather than a bespoke loop here).
    ///
    /// Anything else is a real migration failure and propagates. Before this,
    /// any one of these aborted `migrate()` part-way, leaving later files —
    /// including `009_queue_name.sql`, whose column the statistics query
    /// needs — unapplied, so the broker then failed with
    /// `Unknown column 'queue_name'`.
    async fn execute_migration_statement(&self, statement: &str) -> Result<()> {
        with_deadlock_retry("migration statement", || async {
            match self.conn.execute(statement, &[]).await {
                Ok(_) => Ok(()),
                Err(e) if is_create_index(statement) && is_duplicate_key_name(&e) => {
                    tracing::debug!(
                        statement = %statement,
                        "index already exists (concurrent migration); continuing"
                    );
                    Ok(())
                }
                Err(e) if is_add_column(statement) && is_duplicate_column_name(&e) => {
                    tracing::debug!(
                        statement = %statement,
                        "column already exists (concurrent migration); continuing"
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        })
        .await
        .map_err(|e| CelersError::Other(format!("Migration failed: {}", e)))
    }

    /// Run a migration without tracking (for the migrations table itself)
    async fn run_migration_untracked(&self, migration_sql: &str) -> Result<()> {
        self.run_migration(migration_sql).await
    }

    /// Run a single migration file
    ///
    /// MySQL's `COM_STMT_EXECUTE` (extended/prepared-statement) protocol —
    /// which `oxisql_mysql::MyConnection::execute`/`query` always use —
    /// rejects multi-statement text, and a stored-procedure body itself
    /// contains internal `;`-separated statements that must not be split.
    /// This mirrors the pre-migration `sqlx` approach exactly (see the
    /// already-completed `celers-backend-db::MysqlResultBackend::migrate`
    /// for the identical, proven shape): split on the literal `DELIMITER //`
    /// / `DELIMITER ;` markers first, then split the *non-procedure* section
    /// on `;` and execute each statement individually. Deliberately does
    /// NOT use `Connection::execute_batch` here — `execute_batch`'s own
    /// naive `;` split (see its doc comment in `oxisql-core`) would itself
    /// break on a stored procedure body containing internal semicolons,
    /// which is exactly the hazard this hand-rolled DELIMITER-aware split
    /// exists to avoid.
    ///
    /// # Leading `--` comments
    ///
    /// Splitting on `;` puts each statement in the same chunk as the comment
    /// block that precedes it, so a chunk normally *starts* with `--`. This
    /// function previously skipped any chunk whose trimmed text started with
    /// `--`, which discarded the statement along with its comment. Because
    /// every migration file in this crate opens with a title comment, that
    /// dropped the first statement of every file — including
    /// `CREATE TABLE celers_migrations` in `000_migrations.sql`, which made
    /// the very next `is_migration_applied` query fail against a table that
    /// had never been created. `migrate()` therefore failed on every fresh
    /// database.
    ///
    /// [`strip_sql_line_comments`] now removes the comment *lines* and keeps
    /// the statement, and it runs **before** the `;` split rather than
    /// per-chunk: a prose comment containing a semicolon would otherwise be
    /// cut in half and its tail submitted to the server as a statement.
    async fn run_migration(&self, migration_sql: &str) -> Result<()> {
        let sections: Vec<&str> = migration_sql.split("DELIMITER //").collect();

        // Execute the main DDL statements (before DELIMITER)
        if let Some(main_sql) = sections.first() {
            for statement in strip_sql_line_comments(main_sql).split(';') {
                let trimmed = statement.trim();
                if trimmed.is_empty() {
                    continue;
                }
                self.execute_migration_statement(trimmed).await?;
            }
        }

        // Execute the stored procedure (between DELIMITER // and DELIMITER ;)
        if sections.len() > 1 {
            let proc_section = sections[1];
            if let Some(proc_sql) = proc_section.split("DELIMITER ;").next() {
                let stripped = strip_sql_line_comments(proc_sql);
                // The body ends with the client-side `//` delimiter marker,
                // which is not SQL: submitting `... END//` over
                // COM_STMT_PREPARE is a syntax error.
                let trimmed = stripped.trim().trim_end_matches("//").trim_end();
                if !trimmed.is_empty() {
                    match self.conn.execute(trimmed, &[]).await {
                        Ok(_) => {}
                        // MySQL will not accept `CREATE PROCEDURE` over the
                        // prepared-statement protocol, which is the only one
                        // this driver speaks — see
                        // the `dlq_move` module for the full explanation.
                        // Failing here aborted `migrate()` on every MySQL
                        // database, leaving the schema half-applied and the
                        // broker unusable. Nothing depends on the routine any
                        // more: the one caller (`move_to_dlq`) now issues the
                        // procedure's two statements directly.
                        Err(e) if is_unsupported_in_prepared_protocol(&e) => {
                            tracing::debug!(
                                error = %e,
                                "skipping stored-procedure creation: not sendable over the \
                                 prepared-statement protocol. CeleRS does not call it — the \
                                 DLQ move runs as plain SQL."
                            );
                        }
                        Err(e) => {
                            return Err(CelersError::Other(format!(
                                "Stored procedure creation failed: {}",
                                e
                            )))
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
