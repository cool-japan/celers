//! MySQL result backend implementation.
//!
//! Split out of `lib.rs` (which used to hold both the PostgreSQL and MySQL
//! backends plus their shared helpers in one 2300+ line file) purely for
//! file size — no API change. [`crate::decode_result_state`] and
//! [`crate::default_ttl_config`] stay in the crate root since both backends
//! share them.

use async_trait::async_trait;
use celers_backend_redis::{
    BackendError, ChordState, Result, ResultBackend, TaskMeta, TaskResult, TaskTtlConfig,
};
use chrono::{DateTime, Utc};
use oxisql_core::{Connection, ToSqlValue};
use std::time::Duration;
use uuid::Uuid;

use crate::analytics::MysqlAnalytics;
use crate::result_compression::{self, CompressionConfig};
use crate::row_ext::RowExt;
use crate::sql_split;
use crate::task_meta_extra::TaskMetaExtra;
use crate::tls_mode;
use crate::{decode_result_state, default_ttl_config};

/// Is this `ERROR 1061 (42000): Duplicate key name` — an index that already
/// exists?
///
/// Matched on the error *number*, which is stable across MySQL and MariaDB
/// versions and locales, rather than on the message text, which is neither.
fn is_duplicate_key_name(e: &oxisql_core::OxiSqlError) -> bool {
    e.to_string().contains("1061")
}

/// Is this the server refusing a statement the prepared-statement protocol
/// cannot carry (`ERROR 1295 (HY000)`)?
///
/// Matched on the error *number*, which is stable across MySQL and MariaDB
/// versions and locales, rather than on the message text, which is neither.
fn is_unsupported_in_prepared_protocol(e: &oxisql_core::OxiSqlError) -> bool {
    e.to_string().contains("1295")
}

/// Drop leading `--` line comments and whitespace from a statement.
///
/// [`sql_split::split_sql_statements`] keeps the comment lines that precede a
/// statement attached to it (`-- Indexes for performance\nCREATE INDEX ...`),
/// so a parser that looks at the first word sees `--` and gives up. That is
/// not cosmetic: it silently un-guarded the *first* index in the migration,
/// which is exactly the one that then failed with `Duplicate key name` on the
/// second `migrate()`.
fn strip_leading_sql_comments(statement: &str) -> &str {
    let mut rest = statement.trim_start();
    while let Some(after) = rest.strip_prefix("--") {
        match after.find('\n') {
            Some(nl) => rest = after[nl + 1..].trim_start(),
            // A statement that is nothing but a comment.
            None => return "",
        }
    }
    rest
}

/// A `CREATE INDEX <name> ON <table> (...)` statement, decomposed.
#[derive(Debug, PartialEq, Eq)]
struct CreateIndex<'a> {
    name: &'a str,
    table: &'a str,
}

/// Recognise `CREATE INDEX <name> ON <table>(...)` and pull out the two
/// identifiers, so the caller can ask `information_schema` whether the index
/// is already there.
///
/// Deliberately narrow: it matches the shape the bundled
/// `001_init_mysql.sql` actually uses and returns `None` for anything else
/// (including `CREATE UNIQUE INDEX`, which that file does not contain), so an
/// unrecognised statement is executed unchanged rather than silently skipped.
fn parse_create_index(statement: &str) -> Option<CreateIndex<'_>> {
    let mut words = strip_leading_sql_comments(statement).split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("CREATE") {
        return None;
    }
    if !words.next()?.eq_ignore_ascii_case("INDEX") {
        return None;
    }
    let name = words.next()?.trim_matches('`');
    if !words.next()?.eq_ignore_ascii_case("ON") {
        return None;
    }
    // `ON celers_task_results(expires_at)` — the table name may run straight
    // into the column list with no space.
    let table = words
        .next()?
        .split('(')
        .next()
        .map(|t| t.trim_matches('`'))
        .filter(|t| !t.is_empty())?;

    if name.is_empty() {
        return None;
    }
    Some(CreateIndex { name, table })
}

/// Recognise `CREATE PROCEDURE <name>(...)` and pull out the routine name.
///
/// Returns `None` for anything else, so an unrecognised body is executed
/// unchanged rather than skipped.
fn parse_create_procedure_name(statement: &str) -> Option<&str> {
    let mut words = statement.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("CREATE") {
        return None;
    }
    if !words.next()?.eq_ignore_ascii_case("PROCEDURE") {
        return None;
    }
    // `cleanup_expired_results(OUT deleted_count INT)` — the parameter list
    // may run straight into the name.
    words
        .next()?
        .split('(')
        .next()
        .map(|n| n.trim_matches('`'))
        .filter(|n| !n.is_empty())
}

/// MySQL result backend implementation
#[derive(Clone)]
pub struct MysqlResultBackend {
    conn: oxisql_mysql::MyConnection,
    ttl_config: TaskTtlConfig,
    compression: CompressionConfig,
}

impl MysqlResultBackend {
    /// Create a new MySQL result backend
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    ///
    /// `oxisql_mysql::MyConnection` is backed by a real `mysql_async::Pool`
    /// (confirmed against its own doc comments: "callers can share a
    /// `MyConnection` across async tasks without additional locking"), so
    /// unlike the Postgres backend this already pools connections and
    /// transparently discards/replaces a broken one on next checkout — no
    /// bespoke pooling wrapper is needed here. See [`Self::with_pool_size`]
    /// to configure the pool's connection limits explicitly.
    ///
    /// Results expire after `ttl::SUCCESS` (24 hours) by default, matching
    /// `RedisResultBackend::new` and Celery's `result_expires` — see
    /// `default_ttl_config`. Use [`Self::with_ttl_config`]`(TaskTtlConfig::new())`
    /// for permanent results, or `with_ttl_config` with a populated
    /// [`TaskTtlConfig`] for per-task-type expiry.
    pub async fn new(database_url: &str) -> Result<Self> {
        let tls = tls_mode::mysql_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let conn = oxisql_mysql::MyConnection::connect(database_url, tls)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to database: {}", e))
            })?;

        Ok(Self {
            conn,
            ttl_config: default_ttl_config(),
            compression: CompressionConfig::disabled(),
        })
    }

    /// Create a new MySQL result backend with an explicit pool connection
    /// limit (`mysql_async`'s default is 10 when unset).
    ///
    /// Installs the same 24-hour default TTL as [`Self::new`] — see its doc
    /// comment.
    pub async fn with_pool_size(database_url: &str, pool_max: usize) -> Result<Self> {
        let tls = tls_mode::mysql_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let url = url::Url::parse(database_url)
            .map_err(|e| BackendError::Connection(format!("Invalid database URL: {e}")))?;
        let conn = oxisql_mysql::MyConnectionBuilder::new()
            .host(url.host_str().unwrap_or("localhost"))
            .port(url.port().unwrap_or(3306))
            .user(url.username())
            .password(url.password().unwrap_or(""))
            .dbname(url.path().trim_start_matches('/'))
            .pool_max(pool_max.max(1))
            .tls_mode(tls)
            .connect()
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to connect to database: {e}")))?;

        Ok(Self {
            conn,
            ttl_config: default_ttl_config(),
            compression: CompressionConfig::disabled(),
        })
    }

    /// Configure per-task-type TTL
    pub fn with_ttl_config(mut self, config: TaskTtlConfig) -> Self {
        self.ttl_config = config;
        self
    }

    /// Configure compression for the `result_data` column.
    ///
    /// Disabled by default — see [`CompressionConfig::disabled`] for why.
    /// Decompression on read is unconditional regardless of this setting,
    /// so turning compression off here never breaks reading a row a
    /// differently-configured writer compressed.
    pub fn with_compression(mut self, config: CompressionConfig) -> Self {
        self.compression = config;
        self
    }

    /// Get the compression configuration.
    pub fn compression_config(&self) -> &CompressionConfig {
        &self.compression
    }

    /// Get the TTL configuration
    pub fn ttl_config(&self) -> &TaskTtlConfig {
        &self.ttl_config
    }

    /// Get a mutable reference to the TTL configuration
    pub fn ttl_config_mut(&mut self) -> &mut TaskTtlConfig {
        &mut self.ttl_config
    }

    /// Does `index` already exist on `table` in the current database?
    ///
    /// `information_schema.statistics` is the portable place to ask — it
    /// exists on MySQL 5.7, MySQL 8 and MariaDB alike, unlike
    /// `CREATE INDEX IF NOT EXISTS`, which none of them accept.
    async fn index_exists(&self, table: &str, index: &str) -> Result<bool> {
        let rows = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() \
                   AND table_name = ? \
                   AND index_name = ? \
                 LIMIT 1",
                &[&table, &index],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!(
                    "Failed to check for index '{index}' on '{table}': {e}"
                ))
            })?;
        Ok(!rows.is_empty())
    }

    /// Does stored routine `name` already exist in the current database?
    async fn routine_exists(&self, name: &str) -> Result<bool> {
        let rows = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.routines \
                 WHERE routine_schema = DATABASE() \
                   AND routine_name = ? \
                 LIMIT 1",
                &[&name],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to check for routine '{name}': {e}"))
            })?;
        Ok(!rows.is_empty())
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init_mysql.sql");

        // Split on the `DELIMITER //` marker: everything before it is
        // ordinary `;`-terminated DDL, everything after is one or more
        // stored-procedure bodies (each internally `;`-terminated between
        // `DELIMITER //` and `DELIMITER ;`, executed as a single statement).
        let sections: Vec<&str> = migration_sql.split("DELIMITER //").collect();

        // Execute the main DDL section via a real statement splitter that
        // respects string/identifier quoting and comments (see
        // `sql_split.rs`) instead of a naive `str::split(';')`, which both
        // breaks on any `;` inside a string/identifier and can silently
        // *drop* a statement that follows a comment line within the same
        // fragment.
        if let Some(main_sql) = sections.first() {
            for statement in sql_split::split_sql_statements(main_sql) {
                // MySQL has no `CREATE INDEX IF NOT EXISTS` in any released
                // version, so a bare `CREATE INDEX` makes `migrate()` a
                // one-shot: the second call — a worker restart, or simply a
                // second worker starting against the same database — fails
                // with `ERROR 1061 (42000): Duplicate key name`. Every other
                // statement in this file is already idempotent
                // (`CREATE TABLE IF NOT EXISTS`), and `migrate()` is
                // documented and used as safe to call repeatedly, so the
                // index statements are made idempotent the portable way:
                // ask `information_schema` first, exactly as the `extra`
                // column check below does.
                let creating_index = parse_create_index(&statement);
                if let Some(index) = &creating_index {
                    if self.index_exists(index.table, index.name).await? {
                        continue;
                    }
                }

                match self.conn.execute(&statement, &[]).await {
                    Ok(_) => {}
                    // The `information_schema` check above is a
                    // check-then-act, so two workers migrating at the same
                    // moment can both see the index missing and both issue
                    // the `CREATE`. MySQL has no `CREATE INDEX IF NOT
                    // EXISTS` to make that atomic, and its advisory lock
                    // (`GET_LOCK`) is session-scoped — useless here, because
                    // every `execute` checks a fresh connection out of the
                    // pool and would not be the session holding the lock.
                    // So the loser of the race tolerates `ERROR 1061
                    // (42000): Duplicate key name`: the post-condition it
                    // wanted (the index exists) is satisfied either way.
                    Err(e) if creating_index.is_some() && is_duplicate_key_name(&e) => {}
                    Err(e) => {
                        return Err(BackendError::Connection(format!("Migration failed: {}", e)))
                    }
                }
            }
        }

        // Execute stored procedures: each section between `DELIMITER //` and
        // the next `DELIMITER ;` is one procedure body, executed whole (its
        // internal `;`s are part of the procedure's `BEGIN...END` block, not
        // statement separators at this level).
        for &proc_section in sections.iter().skip(1) {
            if let Some(proc_sql) = proc_section.split("DELIMITER ;").next() {
                // `DELIMITER` is a *client* directive, so the server never
                // sees it — but the `//` that terminates the body inside the
                // block is likewise client-side syntax and is NOT valid SQL.
                // Sending the body with it attached made every single
                // migration fail with
                // `ERROR 1064 ... near '//'`, which meant neither
                // `cleanup_expired_results` nor `chord_increment_counter`
                // was ever created on any MySQL database.
                let trimmed = proc_sql.trim().trim_end_matches("//").trim_end();
                if trimmed.is_empty() {
                    continue;
                }
                // `CREATE PROCEDURE IF NOT EXISTS` needs MySQL 8.0.29+, and
                // this schema targets 5.7+; `DROP PROCEDURE IF EXISTS` first
                // would leave a window where a concurrently-starting worker
                // finds the procedure missing. Check instead, same as the
                // index and column cases.
                let name = parse_create_procedure_name(trimmed);
                if let Some(name) = name {
                    // Already there — e.g. an operator ran the migration
                    // through the `mysql` CLI, where `DELIMITER` works.
                    if self.routine_exists(name).await? {
                        continue;
                    }
                }

                match self.conn.execute(trimmed, &[]).await {
                    Ok(_) => {}
                    Err(e) if is_unsupported_in_prepared_protocol(&e) => {
                        // MySQL refuses `CREATE PROCEDURE` over the
                        // prepared-statement protocol (`ERROR 1295 (HY000):
                        // This command is not supported in the prepared
                        // statement protocol yet`), and oxisql-mysql 0.4.1
                        // routes both `execute` and `execute_batch` through
                        // `mysql_async`'s `exec_iter`, which always prepares
                        // — there is no text-protocol escape hatch to reach
                        // for. Creating these routines from CeleRS is
                        // therefore impossible until the driver grows one.
                        //
                        // This is downgraded to a warning rather than an
                        // error *only* because nothing in CeleRS calls
                        // either bundled routine: `cleanup_expired_results`
                        // and `chord_increment_counter` are invoked on
                        // PostgreSQL (where they are SQL functions created
                        // by 001_init_postgres.sql), while the MySQL paths
                        // do that work in inline SQL and
                        // `MysqlResultBackend` has no `cleanup_expired` at
                        // all. Failing here instead made `migrate()` return
                        // an error on *every* MySQL database, which left the
                        // whole MySQL result backend unusable — a far worse
                        // outcome than a routine nothing calls being absent.
                        //
                        // If a MySQL code path ever needs one of these, it
                        // must not rely on this migration creating it: give
                        // MySQL the same treatment as PostgreSQL in inline
                        // SQL, or wait for a driver that can send DDL over
                        // the text protocol.
                        tracing::warn!(
                            routine = name.unwrap_or("<unparsed>"),
                            error = %e,
                            "skipping stored-routine creation: MySQL rejects CREATE PROCEDURE \
                             over the prepared-statement protocol and the driver offers no \
                             text-protocol path. No CeleRS MySQL code path calls this routine, \
                             so the backend is fully functional without it."
                        );
                    }
                    Err(e) => {
                        return Err(BackendError::Connection(format!(
                            "Stored procedure creation failed: {}",
                            e
                        )))
                    }
                }
            }
        }

        // Backward-compatible column add for databases migrated before the
        // `extra` column existed. A fresh install already has it (via the
        // CREATE TABLE above); MySQL 5.7 has no `ADD COLUMN IF NOT EXISTS`
        // (that syntax needs 8.0.29+/newer MariaDB), so portably checking
        // `information_schema.columns` first and only running the `ALTER`
        // when the column is actually missing is what keeps `migrate()` safe
        // to call repeatedly across MySQL/MariaDB versions.
        let has_extra_column = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() \
                   AND table_name = 'celers_task_results' \
                   AND column_name = 'extra'",
                &[],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to check for extra column: {}", e))
            })?;
        if has_extra_column.is_empty() {
            self.conn
                .execute("ALTER TABLE celers_task_results ADD COLUMN extra JSON", &[])
                .await
                .map_err(|e| {
                    BackendError::Connection(format!("Failed to add extra column: {}", e))
                })?;
        }

        Ok(())
    }

    /// Return an analytics helper bound to the same connection.
    pub fn analytics(&self) -> MysqlAnalytics {
        MysqlAnalytics::new(self.conn.clone())
    }

    /// Check whether the backend can currently reach the database.
    ///
    /// Issues a trivial `SELECT 1` (benefiting from the underlying
    /// `mysql_async::Pool`'s own checkout/discard-broken-connection
    /// behavior — see [`MysqlResultBackend::new`]'s doc comment) and reports
    /// whether it succeeded. Mirrors
    /// `celers_backend_redis::RedisResultBackend::health_check`'s contract
    /// for use in the same monitoring/readiness-probe role: `Ok(true)` means
    /// healthy, `Err(_)` means a connection or other error occurred.
    pub async fn health_check(&self) -> Result<bool> {
        match self.conn.query("SELECT 1", &[]).await {
            Ok(rows) => Ok(!rows.is_empty()),
            Err(e) => Err(BackendError::Connection(format!(
                "health_check query failed: {}",
                e
            ))),
        }
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &oxisql_mysql::MyConnection {
        &self.conn
    }

    /// Serialize the extended [`TaskMeta`] fields (progress, tags, metadata,
    /// version, ...) into the JSON text stored in the `extra` column.
    fn extra_param(meta: &TaskMeta) -> Result<String> {
        TaskMetaExtra::from_meta(meta)
            .to_json_string()
            .map_err(|e| BackendError::Serialization(format!("Failed to serialize extra: {e}")))
    }

    /// Parse the `extra` column's text (if present) and overlay it onto `meta`.
    fn apply_extra(meta: &mut TaskMeta, raw: Option<&str>) -> Result<()> {
        let extra = TaskMetaExtra::from_column(raw)
            .map_err(|e| BackendError::Serialization(format!("Failed to parse extra: {e}")))?;
        extra.apply_to(meta);
        Ok(())
    }

    /// Compute the `expires_at` parameter for `store_result`'s INSERT from
    /// the per-task-type TTL config: `Some(MySQL DATETIME string)` when a TTL
    /// is configured for `task_name`, `None` otherwise. See
    /// `PostgresResultBackend::ttl_expires_at_param` for why this is folded
    /// into the initial INSERT rather than a second `set_expiration` UPDATE.
    fn ttl_expires_at_param(ttl_config: &TaskTtlConfig, task_name: &str) -> Result<Option<String>> {
        ttl_config
            .get_ttl(task_name)
            .map(|ttl| {
                let expires_at = Utc::now()
                    + chrono::Duration::from_std(ttl).map_err(|e| {
                        BackendError::Serialization(format!("Invalid TTL duration: {}", e))
                    })?;
                Ok(expires_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            })
            .transpose()
    }
}

#[async_trait]
impl ResultBackend for MysqlResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let (result_state, result_data, error_message, retry_count) = match &meta.result {
            TaskResult::Pending => ("pending", None, None, None),
            TaskResult::Started => ("started", None, None, None),
            TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
            TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
            TaskResult::Revoked => ("revoked", None, None, None),
            TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
        };
        // `result_data` is a native MySQL `JSON` column: compressing it can
        // only mean wrapping it in the self-describing envelope
        // `result_compression` builds, not storing raw bytes — see that
        // module's doc comment.
        let result_data = result_data
            .map(|v| result_compression::maybe_compress(&v, &self.compression))
            .transpose()?;

        let result_data_str =
            result_data.map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()));
        // MySQL DATETIME/TIMESTAMP grammar convention — see `row_ext.rs`'s
        // "DateTime<Utc> parameter convention (MySQL)" section for why
        // `.to_rfc3339()` is unsafe here and this format is the verified
        // MySQL-server-accepted one.
        let created_at_param = meta.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let started_at_param = meta
            .started_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        let completed_at_param = meta
            .completed_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        let extra_param = Self::extra_param(meta)?;
        let expires_at_param = Self::ttl_expires_at_param(&self.ttl_config, &meta.task_name)?;

        self.conn
            .execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    result_state = VALUES(result_state),
                    result_data = VALUES(result_data),
                    error_message = VALUES(error_message),
                    retry_count = VALUES(retry_count),
                    started_at = VALUES(started_at),
                    completed_at = VALUES(completed_at),
                    worker = VALUES(worker),
                    extra = VALUES(extra),
                    -- See PostgresResultBackend::store_result's ON CONFLICT
                    -- clause: only overwrite an existing expires_at when
                    -- THIS store configured a TTL, otherwise preserve it.
                    expires_at = COALESCE(VALUES(expires_at), expires_at)
                "#,
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data_str,
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                    &extra_param,
                    &expires_at_param,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT task_id, task_name, result_state, result_data, error_message,
                       retry_count, created_at, started_at, completed_at, worker, extra
                FROM celers_task_results
                WHERE task_id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_id_str: String = row
                    .col("task_id")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_state: String = row
                    .col("result_state")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data_str: Option<String> = row
                    .col("result_data")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                // MySQL TEXT arrives as `Value::Blob` (TEXT and BLOB share a
                // wire type), which `col::<Option<String>>` rejects — a
                // failed task's non-NULL `error_message` is exactly that
                // case. See `row_ext::opt_text_from_row`.
                let error_message: Option<String> =
                    crate::row_ext::opt_text_from_row(&row, "error_message").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?;
                let retry_count: Option<i32> = row
                    .col("retry_count")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let extra_raw: Option<String> = row
                    .col("extra")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;

                let result_data = result_data_str
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                    .map(|v| result_compression::maybe_decompress(v, &self.compression))
                    .transpose()?;

                let parsed_task_id = Uuid::parse_str(&task_id_str)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;
                let result = decode_result_state(
                    parsed_task_id,
                    &result_state,
                    result_data,
                    error_message,
                    retry_count,
                )?;

                let mut meta = TaskMeta {
                    task_id: parsed_task_id,
                    task_name: row.col("task_name").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    result,
                    created_at: row.col::<DateTime<Utc>>("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    started_at: row.col("started_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    completed_at: row.col("completed_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    worker: row.col("worker").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    progress: None,
                    version: 0,
                    tags: Vec::new(),
                    metadata: std::collections::HashMap::new(),
                    worker_hostname: None,
                    runtime_ms: None,
                    memory_bytes: None,
                    retries: None,
                    queue: None,
                    ignored_error: None,
                };
                Self::apply_extra(&mut meta, extra_raw.as_deref())?;

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM celers_task_results WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|e| BackendError::Serialization(format!("Invalid TTL duration: {}", e)))?;
        let expires_at_param = expires_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        self.conn
            .execute(
                "UPDATE celers_task_results SET expires_at = ? WHERE task_id = ?",
                &[&expires_at_param, &task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    // See the identical comment on
    // `PostgresResultBackend::chord_init`: this is a *create-or-reset*
    // primitive that must zero `completed` even on conflict, or a
    // `chord_retry` of an already-terminal chord leaves the counter at or
    // above `total` and the callback fires before any retried task reports
    // in. `completed = 0` (a literal, matching the INSERT branch's own
    // hardcoded `0`, not `VALUES(completed)`) makes the DUPLICATE KEY branch
    // agree with the INSERT branch. Callers that want to persist a state
    // mutation without losing progress must use `chord_update_state`
    // instead.
    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_string(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        let created_at_param = state.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    total = VALUES(total),
                    completed = 0,
                    callback = VALUES(callback),
                    task_ids = VALUES(task_ids),
                    timeout_seconds = VALUES(timeout_seconds),
                    cancelled = VALUES(cancelled),
                    cancellation_reason = VALUES(cancellation_reason)
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &task_ids,
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    // The `chord_init` upsert minus `completed`: see
    // `PostgresResultBackend::chord_update_state` for why the column is
    // absent from every clause rather than pinned to a value — omitted from
    // the INSERT branch's column/VALUES lists (the fresh-row case takes the
    // schema's own `DEFAULT 0`) and from the DUPLICATE KEY branch's SET
    // list (the existing-row case leaves it untouched, preserving in-flight
    // progress). Used by `chord_cancel`'s default implementation so
    // cancelling a chord never un-completes tasks that already reported in.
    async fn chord_update_state(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_string(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        let created_at_param = state.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    total = VALUES(total),
                    callback = VALUES(callback),
                    task_ids = VALUES(task_ids),
                    timeout_seconds = VALUES(timeout_seconds),
                    cancelled = VALUES(cancelled),
                    cancellation_reason = VALUES(cancellation_reason)
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &task_ids,
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to update chord state: {}", e))
            })?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        // Atomic increment-and-read: both statements MUST run on the same
        // physical connection, so this uses an explicit transaction (which
        // `MyTransaction` pins to one connection borrowed from the pool for
        // its whole lifetime — see `oxisql-mysql`'s `connection.rs` doc
        // comment) rather than `self.conn.execute`/`.query`, each of which
        // independently checks a connection out of `mysql_async::Pool` and
        // could land on two different physical connections.
        //
        // `LAST_INSERT_ID(expr)` is MySQL's own documented idiom for a
        // session-scoped atomic increment-and-read (see the MySQL Reference
        // Manual's "Obtaining the Unique ID": `UPDATE sequence SET
        // id=LAST_INSERT_ID(id+1); SELECT LAST_INSERT_ID();`): the UPDATE's
        // row-level lock serializes concurrent increments on the same row,
        // and `LAST_INSERT_ID()` returns exactly the value *this session's*
        // UPDATE computed, immune to another connection's concurrent
        // increment landing in between (unlike the previous separate
        // UPDATE-then-SELECT, where two concurrent callers could each read
        // back the OTHER's incremented value and both observe the terminal
        // count — firing the chord callback twice).
        let mut tx =
            self.conn.transaction().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        let affected = tx
            .execute(
                "UPDATE celers_chord_state SET completed = LAST_INSERT_ID(completed + 1) WHERE chord_id = ?",
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;

        if affected == 0 {
            // No such chord row: roll back (nothing to commit) and report a
            // meaningful not-found error rather than the previous behavior
            // of a generic "chord counter query returned no rows" — same
            // spirit as the caller-visible error identifying the missing id.
            tx.rollback().await.map_err(|e| {
                BackendError::Connection(format!("Failed to roll back transaction: {}", e))
            })?;
            return Err(BackendError::NotFound(chord_id));
        }

        let rows = tx
            .query("SELECT LAST_INSERT_ID() AS completed", &[])
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to read chord counter: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("LAST_INSERT_ID() query returned no rows".to_string())
        })?;
        let count: i64 = row
            .col("completed")
            .map_err(|e| BackendError::Connection(format!("Failed to read chord counter: {e}")))?;

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason
                FROM celers_chord_state
                WHERE chord_id = ?
                "#,
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let chord_id_str: String = row.col("chord_id").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                // Read through `json_from_row`, the crate's canonical JSON
                // column read, rather than `col::<String>`: `FromValue for
                // String` rejects `Value::Blob`, which is one of the shapes
                // `oxisql-mysql` returns a `JSON` column as, and this read
                // failed with `type mismatch: expected Text, got Blob`.
                let task_ids_json =
                    crate::row_ext::json_from_row(&row, "task_ids").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?;
                let task_ids: Vec<Uuid> = serde_json::from_value(task_ids_json)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let total: i64 = row.col("total").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let completed: i64 = row.col("completed").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let timeout_secs: Option<i64> = row.col("timeout_seconds").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;

                let state = ChordState {
                    chord_id: Uuid::parse_str(&chord_id_str)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?,
                    total: total as usize,
                    completed: completed as usize,
                    // `callback` and `cancellation_reason` are MySQL TEXT,
                    // which arrives as `Value::Blob` (TEXT and BLOB share a
                    // wire type) and which `col::<Option<String>>` rejects.
                    // See `row_ext::opt_text_from_row`.
                    callback: crate::row_ext::opt_text_from_row(&row, "callback").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    // See the identical gap called out in
                    // `PostgresResultBackend::chord_get_state`: no column
                    // for this field on `celers_chord_state`, same as
                    // `retry_count`/`max_retries` below.
                    callback_on_success_link: None,
                    task_ids,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    timeout: timeout_secs.map(|s| std::time::Duration::from_secs(s as u64)),
                    // MySQL BOOLEAN is TINYINT(1) and arrives as an integer,
                    // which `col::<bool>` rejects. The column is
                    // `NOT NULL DEFAULT FALSE`, so `false` is the right
                    // fallback. See `row_ext::bool_from_row`.
                    cancelled: crate::row_ext::bool_from_row(&row, "cancelled", false).map_err(
                        |e| BackendError::Connection(format!("Failed to get chord state: {e}")),
                    )?,
                    cancellation_reason: crate::row_ext::opt_text_from_row(
                        &row,
                        "cancellation_reason",
                    )
                    .map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    retry_count: 0,
                    max_retries: None,
                };

                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    // Batch operations using transactions for atomic multi-row operations

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let mut tx =
            self.conn.transaction().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        for (task_id, meta) in results {
            let (result_state, result_data, error_message, retry_count) = match &meta.result {
                TaskResult::Pending => ("pending", None, None, None),
                TaskResult::Started => ("started", None, None, None),
                TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
                TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
                TaskResult::Revoked => ("revoked", None, None, None),
                TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
            };
            let result_data = result_data
                .map(|v| result_compression::maybe_compress(&v, &self.compression))
                .transpose()?;
            let result_data_str = result_data
                .map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()));
            let created_at_param = meta.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            let started_at_param = meta
                .started_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
            let completed_at_param = meta
                .completed_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
            let extra_param = Self::extra_param(meta)?;

            tx.execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    result_state = VALUES(result_state),
                    result_data = VALUES(result_data),
                    error_message = VALUES(error_message),
                    retry_count = VALUES(retry_count),
                    started_at = VALUES(started_at),
                    completed_at = VALUES(completed_at),
                    worker = VALUES(worker),
                    extra = VALUES(extra)
                "#,
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data_str,
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                    &extra_param,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // MySQL requires IN clause with placeholders
        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker, extra
            FROM celers_task_results
            WHERE task_id IN ({})
            "#,
            placeholders
        );
        // oxisql_mysql::MyConnection::execute/query take `&str` directly —
        // sqlx's `AssertSqlSafe` opt-out wrapper has no equivalent (and none
        // is needed): only the placeholder *count* (never a value) was
        // spliced into `query_str` above, matching the exact same
        // static-fragment-only discipline `sqlx::AssertSqlSafe` was
        // previously asserting.
        let id_params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            id_params.iter().map(|s| s as &dyn ToSqlValue).collect();

        let rows = self
            .conn
            .query(&query_str, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id_str: String = row
                .col("task_id")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| BackendError::Serialization(e.to_string()))?;
            let result_state: String = row
                .col("result_state")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data_str: Option<String> = row
                .col("result_data")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = result_data_str
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .map(|v| result_compression::maybe_decompress(v, &self.compression))
                .transpose()?;
            // MySQL TEXT arrives as `Value::Blob`; see
            // `row_ext::opt_text_from_row`.
            let error_message: Option<String> =
                crate::row_ext::opt_text_from_row(&row, "error_message")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let retry_count: Option<i32> = row
                .col("retry_count")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let extra_raw: Option<String> = row
                .col("extra")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;

            let result = decode_result_state(
                task_id,
                &result_state,
                result_data,
                error_message,
                retry_count,
            )?;

            let mut meta = TaskMeta {
                task_id,
                task_name: row
                    .col("task_name")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                result,
                created_at: row
                    .col("created_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                completed_at: row
                    .col("completed_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                worker: row
                    .col("worker")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                progress: None,
                version: 0,
                tags: Vec::new(),
                metadata: std::collections::HashMap::new(),
                worker_hostname: None,
                runtime_ms: None,
                memory_bytes: None,
                retries: None,
                queue: None,
                ignored_error: None,
            };
            Self::apply_extra(&mut meta, extra_raw.as_deref())?;

            results_map.insert(task_id, meta);
        }

        // Return results in the same order as input task_ids
        Ok(task_ids
            .iter()
            .map(|id| results_map.get(id).cloned())
            .collect())
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        // MySQL requires IN clause with placeholders
        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            "DELETE FROM celers_task_results WHERE task_id IN ({})",
            placeholders
        );
        let id_params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            id_params.iter().map(|s| s as &dyn ToSqlValue).collect();

        self.conn
            .execute(&query_str, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete results: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bundled migration's procedure bodies are terminated by the
    /// client-side `//` delimiter, which is not SQL. Sending it to the server
    /// made *every* `migrate()` call fail with `ERROR 1064 ... near '//'`, so
    /// `cleanup_expired_results` and `chord_increment_counter` were never
    /// created on any MySQL database — and because the live tests that would
    /// have caught it are `#[ignore]`d, nothing noticed. This test needs no
    /// server.
    #[test]
    fn procedure_bodies_drop_the_client_side_delimiter() {
        let migration_sql = include_str!("../migrations/001_init_mysql.sql");
        let sections: Vec<&str> = migration_sql.split("DELIMITER //").collect();
        assert!(
            sections.len() > 1,
            "migration should contain at least one procedure"
        );

        let mut seen = Vec::new();
        for section in sections.iter().skip(1) {
            let body = section
                .split("DELIMITER ;")
                .next()
                .expect("split always yields one element")
                .trim()
                .trim_end_matches("//")
                .trim_end();
            if body.is_empty() {
                continue;
            }
            assert!(
                !body.ends_with("//"),
                "the `//` delimiter must be stripped before the body reaches \
                 the server, got:\n{body}"
            );
            seen.push(parse_create_procedure_name(body).expect("procedure name"));
        }

        assert_eq!(
            seen,
            vec!["cleanup_expired_results", "chord_increment_counter"],
            "both bundled procedures must be recognised, or migrate() will \
             try to re-create one that already exists"
        );
    }

    /// MySQL accepts `CREATE INDEX IF NOT EXISTS` in no released version, so
    /// every index statement in the migration has to be guarded by an
    /// `information_schema` lookup — which only happens for statements
    /// `parse_create_index` recognises. If it stops recognising one, the
    /// second `migrate()` call starts failing with `ERROR 1061 Duplicate key
    /// name` again.
    #[test]
    fn every_bundled_create_index_is_recognised() {
        let migration_sql = include_str!("../migrations/001_init_mysql.sql");
        let main_sql = migration_sql
            .split("DELIMITER //")
            .next()
            .expect("split always yields one element");

        let mut indexes = Vec::new();
        for statement in sql_split::split_sql_statements(main_sql) {
            let upper = strip_leading_sql_comments(&statement).to_ascii_uppercase();
            if upper.starts_with("CREATE INDEX") {
                let parsed = parse_create_index(&statement)
                    .unwrap_or_else(|| panic!("unrecognised CREATE INDEX:\n{statement}"));
                indexes.push((parsed.name.to_string(), parsed.table.to_string()));
            }
        }

        assert_eq!(
            indexes,
            vec![
                (
                    "idx_task_results_expires".to_string(),
                    "celers_task_results".to_string()
                ),
                (
                    "idx_task_results_state".to_string(),
                    "celers_task_results".to_string()
                ),
                (
                    "idx_task_results_created".to_string(),
                    "celers_task_results".to_string()
                ),
                (
                    "idx_chord_completed".to_string(),
                    "celers_chord_state".to_string()
                ),
            ],
            "the migration's indexes changed; keep them guarded"
        );
    }

    #[test]
    fn parse_create_index_sees_past_a_leading_comment() {
        // `split_sql_statements` hands back the comment that precedes a
        // statement as part of it. The first index in the migration is
        // preceded by `-- Indexes for performance`, and missing it left that
        // one index unguarded.
        assert_eq!(
            parse_create_index("-- Indexes for performance\nCREATE INDEX idx_a ON t(col)"),
            Some(CreateIndex {
                name: "idx_a",
                table: "t"
            })
        );
        assert_eq!(
            parse_create_index("-- one\n-- two\n  CREATE INDEX idx_b ON t(col)"),
            Some(CreateIndex {
                name: "idx_b",
                table: "t"
            })
        );
    }

    #[test]
    fn strip_leading_sql_comments_handles_edge_cases() {
        assert_eq!(strip_leading_sql_comments("-- only a comment"), "");
        assert_eq!(strip_leading_sql_comments("SELECT 1"), "SELECT 1");
        assert_eq!(strip_leading_sql_comments("  \n SELECT 1"), "SELECT 1");
        // A `--` inside the statement body must survive.
        assert_eq!(
            strip_leading_sql_comments("SELECT 1 -- trailing"),
            "SELECT 1 -- trailing"
        );
    }

    #[test]
    fn parse_create_index_handles_the_shapes_the_migration_uses() {
        assert_eq!(
            parse_create_index("CREATE INDEX idx_a ON t(col)"),
            Some(CreateIndex {
                name: "idx_a",
                table: "t"
            }),
            "table name running into the column list"
        );
        assert_eq!(
            parse_create_index("create index `idx_b` on `tbl` (a, b)"),
            Some(CreateIndex {
                name: "idx_b",
                table: "tbl"
            }),
            "lowercase keywords and backtick-quoted identifiers"
        );
    }

    #[test]
    fn parse_create_index_rejects_anything_it_does_not_understand() {
        // An unrecognised statement must return None so migrate() executes it
        // unchanged rather than silently skipping schema.
        for stmt in [
            "CREATE TABLE t (a int)",
            "CREATE UNIQUE INDEX idx ON t(a)",
            "SELECT 1",
            "CREATE INDEX",
            "",
        ] {
            assert_eq!(parse_create_index(stmt), None, "should not match: {stmt:?}");
        }
    }

    #[test]
    fn parse_create_procedure_name_handles_its_shapes() {
        assert_eq!(
            parse_create_procedure_name("CREATE PROCEDURE p(OUT x INT)\nBEGIN\nEND"),
            Some("p")
        );
        assert_eq!(
            parse_create_procedure_name("create procedure `q` ()\nBEGIN\nEND"),
            Some("q")
        );
        assert_eq!(parse_create_procedure_name("CREATE TABLE t (a int)"), None);
        assert_eq!(parse_create_procedure_name("SELECT 1"), None);
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_backend_creation() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let backend = MysqlResultBackend::new(&database_url).await;
        assert!(backend.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_store_get_roundtrip_with_compression_enabled() {
        // End-to-end proof that a compressed `result_data` (a JSON column)
        // round-trips through a real server: `store_result` must write a
        // valid JSON envelope (a raw compressed blob would be rejected by
        // the column's own JSON validation), and `get_result` must decode
        // it back to the exact original value.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let mut backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect")
            .with_compression(crate::result_compression::CompressionConfig::new(
                16, "zstd",
            ));
        backend.migrate().await.expect("migrate");

        let task_id = Uuid::new_v4();
        let large_value: Vec<serde_json::Value> = (0..512)
            .map(
                |i| serde_json::json!({"seq": i, "note": "same shape every time, compresses well"}),
            )
            .collect();
        let original = serde_json::json!({ "items": large_value });
        let mut meta = TaskMeta::new(task_id, "compression_roundtrip_test".to_string());
        meta.result = TaskResult::Success(original.clone());

        backend.store_result(task_id, &meta).await.expect("store");

        // The raw column value must be the small envelope, not the large
        // plain JSON -- proving compression actually happened, not just
        // that decode is lenient.
        let raw_rows = backend
            .connection()
            .query(
                "SELECT result_data FROM celers_task_results WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .expect("raw select");
        let raw_json: String = raw_rows[0].col("result_data").expect("result_data column");
        assert!(
            raw_json.contains("__celers_compressed"),
            "the stored column value must be the compression envelope: {raw_json}"
        );
        assert!(
            raw_json.len() < 2000,
            "the envelope must be much smaller than the ~20KB plain payload: {} bytes",
            raw_json.len()
        );

        let fetched = backend
            .get_result(task_id)
            .await
            .expect("get")
            .expect("row exists");
        match fetched.result {
            TaskResult::Success(v) => assert_eq!(v, original),
            other => panic!("expected Success, got {other:?}"),
        }

        backend.delete_result(task_id).await.expect("cleanup");
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_store_get_roundtrip_with_ignored_error() {
        // End-to-end proof that `TaskMeta::ignored_error` -- the marker
        // `celers_backend_db::result_store` uses to preserve
        // `TaskResultValue::Ignored`'s suppressed error text across the
        // `Success(null)` projection (see that module's doc comments) --
        // actually survives a real `extra` JSON column round trip, not
        // just the in-memory `TaskMetaExtra` unit tests.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let mut backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, "ignored_error_roundtrip_test".to_string());
        meta.result = TaskResult::Success(serde_json::Value::Null);
        meta.ignored_error = Some("task failed but ignore_errors was set".to_string());

        backend.store_result(task_id, &meta).await.expect("store");

        // The raw `extra` column must actually carry the marker -- proving
        // it was persisted, not merely echoed back from the in-memory
        // `TaskMeta` this same process just built.
        let raw_rows = backend
            .connection()
            .query(
                "SELECT extra FROM celers_task_results WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .expect("raw select");
        let raw_extra: String = raw_rows[0].col("extra").expect("extra column");
        assert!(
            raw_extra.contains("task failed but ignore_errors was set"),
            "the extra column must persist ignored_error: {raw_extra}"
        );

        let fetched = backend
            .get_result(task_id)
            .await
            .expect("get")
            .expect("row exists");
        assert!(matches!(
            fetched.result,
            TaskResult::Success(serde_json::Value::Null)
        ));
        assert_eq!(
            fetched.ignored_error.as_deref(),
            Some("task failed but ignore_errors was set"),
            "ignored_error must round-trip through a real MySQL connection"
        );

        backend.delete_result(task_id).await.expect("cleanup");
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_chord_complete_task_fires_callback_exactly_once_concurrently() {
        // Regression test for the non-atomic UPDATE-then-SELECT chord
        // counter: spawns `total` tasks all completing the SAME chord
        // concurrently and asserts exactly one of them observes
        // `count >= total` (the condition `celers-worker`'s `workflows.rs`
        // gates the chord callback dispatch on). Before the fix, the
        // separate UPDATE and SELECT statements let two concurrent callers
        // both read back the terminal count, firing the callback twice.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        let chord_id = Uuid::new_v4();
        const TOTAL: usize = 8;
        let state = ChordState {
            chord_id,
            total: TOTAL,
            completed: 0,
            callback: Some("noop".to_string()),
            callback_on_success_link: None,
            task_ids: (0..TOTAL).map(|_| Uuid::new_v4()).collect(),
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        };
        let mut init_backend = backend.clone();
        init_backend.chord_init(state).await.expect("chord_init");

        let mut handles = Vec::with_capacity(TOTAL);
        for _ in 0..TOTAL {
            let mut worker_backend = backend.clone();
            handles.push(tokio::spawn(async move {
                worker_backend
                    .chord_complete_task(chord_id)
                    .await
                    .expect("chord_complete_task")
            }));
        }

        let mut terminal_observations = 0usize;
        for handle in handles {
            let count = handle.await.expect("task join");
            if count >= TOTAL {
                terminal_observations += 1;
            }
        }

        assert_eq!(
            terminal_observations, 1,
            "exactly one concurrent caller must observe the terminal chord count \
             (Celery chord semantics: the callback fires exactly once)"
        );
    }

    /// Build a fresh, non-cancelled `ChordState` with `TOTAL` header tasks
    /// and no progress yet.
    fn fresh_chord_state(chord_id: Uuid, total: usize) -> ChordState {
        ChordState {
            chord_id,
            total,
            completed: 0,
            callback: Some("noop".to_string()),
            callback_on_success_link: None,
            task_ids: (0..total).map(|_| Uuid::new_v4()).collect(),
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        }
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_chord_init_resets_completed_counter_on_conflict() {
        // MySQL counterpart of
        // `test_postgres_chord_init_resets_completed_counter_on_conflict` —
        // see its comment. The `ON DUPLICATE KEY UPDATE` branch had the same
        // gap as Postgres's `ON CONFLICT DO UPDATE`.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let mut backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        const TOTAL: usize = 3;
        let chord_id = Uuid::new_v4();
        let state = fresh_chord_state(chord_id, TOTAL);
        backend.chord_init(state.clone()).await.expect("chord_init");

        for _ in 0..TOTAL {
            backend
                .chord_complete_task(chord_id)
                .await
                .expect("chord_complete_task");
        }
        let terminal = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            terminal.completed, TOTAL,
            "sanity check: counter must be terminal before the reset"
        );

        backend.chord_init(state).await.expect("chord_init (reset)");
        let reset = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            reset.completed, 0,
            "chord_init must reset the completion counter on conflict, matching its \
             documented create-or-reset contract, not leave a stale terminal count in place"
        );
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_chord_cancel_preserves_completed_counter() {
        // MySQL counterpart of
        // `test_postgres_chord_cancel_preserves_completed_counter` — see its
        // comment.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let mut backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        const TOTAL: usize = 3;
        let chord_id = Uuid::new_v4();
        let state = fresh_chord_state(chord_id, TOTAL);
        let task_ids = state.task_ids.clone();
        backend.chord_init(state).await.expect("chord_init");
        backend
            .chord_complete_task(chord_id)
            .await
            .expect("chord_complete_task");

        backend
            .chord_cancel(chord_id, Some("test cancel".to_string()))
            .await
            .expect("chord_cancel");

        let after = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            after.completed, 1,
            "chord_update_state must not reset in-flight progress"
        );
        assert!(
            after.cancelled,
            "chord_cancel must mark the chord cancelled"
        );
        assert_eq!(after.cancellation_reason.as_deref(), Some("test cancel"));
        assert_eq!(
            after.callback.as_deref(),
            Some("noop"),
            "chord_update_state must preserve the callback, not just the counter"
        );
        assert_eq!(
            after.task_ids, task_ids,
            "chord_update_state must preserve task_ids"
        );
    }

    // ── ttl_expires_at_param: the TTL-folded-into-INSERT helper ─────────

    #[test]
    fn mysql_ttl_expires_at_param_is_none_without_a_configured_ttl() {
        let ttl_config = TaskTtlConfig::new();
        let result = MysqlResultBackend::ttl_expires_at_param(&ttl_config, "untracked_task")
            .expect("no TTL configured must not error");
        assert!(result.is_none());
    }

    #[test]
    fn mysql_ttl_expires_at_param_matches_the_mysql_datetime_grammar_when_ttl_configured() {
        let mut ttl_config = TaskTtlConfig::new();
        ttl_config.set_task_ttl("ttl_task", Duration::from_secs(3600));

        let raw = MysqlResultBackend::ttl_expires_at_param(&ttl_config, "ttl_task")
            .expect("TTL config must not error")
            .expect("a configured TTL must produce Some(..)");

        // See row_ext.rs's MySQL DateTime<Utc> parameter convention: a space
        // separator, no 'T', no timezone suffix — never an RFC3339 string.
        assert!(
            !raw.contains('T'),
            "MySQL DATETIME grammar has no 'T' separator: {raw:?}"
        );
        assert!(
            !raw.contains('+') && !raw.contains('Z'),
            "MySQL DATETIME grammar has no timezone suffix: {raw:?}"
        );
        assert!(
            chrono::NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S%.6f").is_ok(),
            "must match MySQL's own DATETIME text grammar: {raw:?}"
        );
    }
}
