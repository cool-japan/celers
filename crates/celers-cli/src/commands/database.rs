//! Database operations command implementations.

use super::utils::mask_password;
use crate::row_ext::RowExt;
use crate::tls_mode::{mysql_tls_mode_for_url, pg_tls_mode_for_url};
use celers_core::Broker;
use colored::Colorize;
use oxisql_core::Connection;
use oxisql_mysql::MyConnection;
use oxisql_postgres::PgConnection;

/// Test database connection
pub async fn db_test_connection(url: &str, benchmark: bool) -> anyhow::Result<()> {
    println!("{}", "=== Database Connection Test ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    // Determine database type from URL
    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    println!("Database type: {}", db_type.yellow());
    println!();

    let start = std::time::Instant::now();

    // Test connection based on database type
    match db_type {
        "PostgreSQL" => {
            let tls_mode = pg_tls_mode_for_url(url)?;
            let conn = PgConnection::connect(url, tls_mode).await?;
            let elapsed = start.elapsed();
            println!(
                "{}",
                format!("✓ Connected successfully in {elapsed:?}").green()
            );

            // Get database version
            let rows = conn.query("SELECT version()", &[]).await?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("SELECT version() returned no rows"))?;
            let version: String = row.col("version")?;
            println!("  {} {}", "Version:".cyan(), version);

            // Get current database name
            let rows = conn.query("SELECT current_database()", &[]).await?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("SELECT current_database() returned no rows"))?;
            let database: String = row.col("current_database")?;
            println!("  {} {}", "Database:".cyan(), database);

            if benchmark {
                println!();
                println!("{}", "Running benchmark...".cyan().bold());
                let mut times = Vec::new();
                for _ in 0..10 {
                    let query_start = std::time::Instant::now();
                    let rows = conn.query("SELECT 1", &[]).await?;
                    rows.into_iter()
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("SELECT 1 returned no rows"))?;
                    times.push(query_start.elapsed());
                }

                let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / times.len() as u128;
                let min = times.iter().map(|t| t.as_micros()).min().unwrap_or(0);
                let max = times.iter().map(|t| t.as_micros()).max().unwrap_or(0);

                println!("  {} {avg}µs", "Avg query time:".yellow());
                println!("  {} {min}µs", "Min query time:".yellow());
                println!("  {} {max}µs", "Max query time:".yellow());
            }
        }
        "MySQL" => {
            let tls_mode = mysql_tls_mode_for_url(url)?;
            let conn = MyConnection::connect(url, tls_mode).await?;
            let elapsed = start.elapsed();
            println!(
                "{}",
                format!("✓ Connected successfully in {elapsed:?}").green()
            );

            let rows = conn.query("SELECT VERSION()", &[]).await?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("SELECT VERSION() returned no rows"))?;
            let version: String = row.col("VERSION()")?;
            println!("  {} {}", "Version:".cyan(), version);

            if benchmark {
                println!();
                println!("{}", "Running benchmark...".cyan().bold());
                let mut times = Vec::new();
                for _ in 0..10 {
                    let query_start = std::time::Instant::now();
                    let rows = conn.query("SELECT 1", &[]).await?;
                    rows.into_iter()
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("SELECT 1 returned no rows"))?;
                    times.push(query_start.elapsed());
                }

                let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / times.len() as u128;
                let min = times.iter().map(|t| t.as_micros()).min().unwrap_or(0);
                let max = times.iter().map(|t| t.as_micros()).max().unwrap_or(0);

                println!("  {} {avg}µs", "Avg query time:".yellow());
                println!("  {} {min}µs", "Min query time:".yellow());
                println!("  {} {max}µs", "Max query time:".yellow());
            }
        }
        _ => {
            println!(
                "{}",
                "✗ Unsupported database type. Use PostgreSQL or MySQL URL."
                    .red()
                    .bold()
            );
            anyhow::bail!("unsupported database type");
        }
    }

    Ok(())
}

/// Database health check
pub async fn db_health(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Database Health Check ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let tls_mode = pg_tls_mode_for_url(url)?;
        let conn = PgConnection::connect(url, tls_mode).await?;

        // Check connection count
        let rows = conn
            .query(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
                &[],
            )
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| {
            anyhow::anyhow!("SELECT count(*) FROM pg_stat_activity ... returned no rows")
        })?;
        let active_connections: i64 = row.col("count")?;
        println!("  {} {}", "Active connections:".cyan(), active_connections);

        // Check database size
        let rows = conn
            .query(
                "SELECT pg_size_pretty(pg_database_size(current_database()))",
                &[],
            )
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| {
            anyhow::anyhow!("SELECT pg_size_pretty(pg_database_size(...)) returned no rows")
        })?;
        let database_size: String = row.col("pg_size_pretty")?;
        println!("  {} {}", "Database size:".cyan(), database_size);

        // Check uptime
        let uptime: String = match conn
            .query("SELECT now() - pg_postmaster_start_time()::text", &[])
            .await
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.col_idx::<String>(0).ok())
        {
            Some(v) => v,
            None => "N/A".to_string(),
        };
        println!("  {} {}", "Uptime:".cyan(), uptime);

        // Check for locks
        let rows = conn
            .query("SELECT count(*) FROM pg_locks WHERE NOT granted", &[])
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("SELECT count(*) FROM pg_locks ... returned no rows"))?;
        let waiting_locks: i64 = row.col("count")?;
        if waiting_locks > 0 {
            println!(
                "  {} {} waiting locks",
                "⚠".yellow(),
                waiting_locks.to_string().yellow()
            );
        } else {
            println!("  {} No waiting locks", "✓".green());
        }

        println!();
        println!("{}", "✓ Database health check complete".green().bold());
    } else if url.starts_with("mysql://") {
        let tls_mode = mysql_tls_mode_for_url(url)?;
        let conn = MyConnection::connect(url, tls_mode).await?;

        let rows = conn.query("SELECT VERSION()", &[]).await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("SELECT VERSION() returned no rows"))?;
        let version: String = row.col("VERSION()")?;
        println!("  {} {}", "Version:".cyan(), version);

        println!();
        println!("{}", "✓ Database health check complete".green().bold());
    } else {
        println!("{}", "✗ Unsupported database URL format".red().bold());
        anyhow::bail!("unsupported database URL format");
    }

    Ok(())
}

/// Database pool statistics
pub async fn db_pool_stats(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Database Pool Statistics ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        // NOTE: `oxisql_postgres::PgConnection` is a single mutex-serialized
        // connection (`Arc<Mutex<tokio_postgres::Client>>`), not a real
        // multi-connection pool like the previous SQL toolkit's
        // `PgPoolOptions` — there is no `oxisql-pool` wiring in this crate.
        // `PgConnection` is `Clone` (the clone shares the same underlying
        // connection), which lets the concurrent-query throughput shape of
        // this probe survive the migration, but the "Max connections" /
        // "Current size" pool metrics the old toolkit exposed have no
        // equivalent here and are intentionally dropped rather than
        // fabricated.
        let tls_mode = pg_tls_mode_for_url(url)?;
        let conn = PgConnection::connect(url, tls_mode).await?;

        // Test concurrent query performance
        println!("{}", "Concurrent Query Test:".cyan().bold());

        let start = std::time::Instant::now();
        let mut handles = vec![];
        for _ in 0..10 {
            let conn = conn.clone();
            handles.push(tokio::spawn(async move {
                let rows = conn.query("SELECT 1", &[]).await.ok()?;
                rows.into_iter().next()?;
                Some(())
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        let elapsed = start.elapsed();
        println!("  {} {elapsed:?}", "10 concurrent queries:".yellow());
        println!("  {} {:?}", "Avg per query:".yellow(), elapsed / 10);
    } else {
        println!(
            "{}",
            "✗ Pool statistics only available for PostgreSQL".red()
        );
        anyhow::bail!("pool statistics only available for PostgreSQL");
    }

    Ok(())
}

/// One embedded schema migration: a stable `version` id, a short human
/// `name`, and the DDL `sql` to run.
///
/// `sql` is embedded read-only via `include_str!` directly from the
/// matching broker crate's own `migrations/` directory (`celers-broker-postgres`
/// / `celers-broker-sql`), so `celers db migrate` applies exactly the same
/// schema those crates' own (private, unreachable from here) `migrate()`
/// methods bootstrap -- there is exactly one source of truth for the SQL,
/// this file only adds the CLI-facing tracked runner (status / up / down)
/// around it.
#[derive(Debug, Clone, Copy)]
struct MigrationFile {
    version: &'static str,
    name: &'static str,
    sql: &'static str,
}

/// Ordered Postgres migrations applied by `celers db migrate up`.
///
/// Mirrors `celers-broker-postgres::PostgresBroker::migrate()`'s own
/// selection exactly: `003_partitioning.sql` is intentionally excluded
/// (its own header documents it as optional, manual, and
/// data-destructive on existing tables -- not safe to run unattended).
const POSTGRES_MIGRATIONS: &[MigrationFile] = &[
    MigrationFile {
        version: "001",
        name: "init",
        sql: include_str!("../../../celers-broker-postgres/migrations/001_init.sql"),
    },
    MigrationFile {
        version: "002",
        name: "results",
        sql: include_str!("../../../celers-broker-postgres/migrations/002_results.sql"),
    },
    MigrationFile {
        version: "004",
        name: "deduplication",
        sql: include_str!("../../../celers-broker-postgres/migrations/004_deduplication.sql"),
    },
    MigrationFile {
        version: "005",
        name: "snapshots",
        sql: include_str!("../../../celers-broker-postgres/migrations/005_snapshots.sql"),
    },
    MigrationFile {
        version: "006",
        name: "deduplication_columns",
        sql: include_str!(
            "../../../celers-broker-postgres/migrations/006_deduplication_columns.sql"
        ),
    },
];

/// Ordered MySQL migrations applied by `celers db migrate up`.
///
/// Mirrors `celers-broker-sql::MysqlBroker::migrate()`'s own selection
/// exactly: `004_partitioning_guide.sql` and `005_uuid_optimization.sql`
/// are intentionally excluded (optional / manual, matching that broker's
/// own curated list).
const MYSQL_MIGRATIONS: &[MigrationFile] = &[
    MigrationFile {
        version: "001",
        name: "init",
        sql: include_str!("../../../celers-broker-sql/migrations/001_init.sql"),
    },
    MigrationFile {
        version: "002",
        name: "results",
        sql: include_str!("../../../celers-broker-sql/migrations/002_results.sql"),
    },
    MigrationFile {
        version: "003",
        name: "performance_indexes",
        sql: include_str!("../../../celers-broker-sql/migrations/003_performance_indexes.sql"),
    },
    MigrationFile {
        version: "006",
        name: "idempotency_keys",
        sql: include_str!("../../../celers-broker-sql/migrations/006_idempotency.sql"),
    },
    MigrationFile {
        version: "007",
        name: "workflow_dag",
        sql: include_str!("../../../celers-broker-sql/migrations/007_workflow.sql"),
    },
    MigrationFile {
        version: "008",
        name: "production_features",
        sql: include_str!("../../../celers-broker-sql/migrations/008_production_features.sql"),
    },
];

/// `celers_migrations` tracking-table DDL for Postgres. No equivalent file
/// ships in `celers-broker-postgres/migrations/` (that crate's `migrate()`
/// applies its DDL unconditionally rather than tracking versions), so this
/// is authored directly here, in the same shape as the MySQL tracking table
/// `celers-broker-sql` already ships (see [`MYSQL_MIGRATIONS_TABLE_SQL`]).
const POSTGRES_MIGRATIONS_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS celers_migrations (
    id BIGSERIAL PRIMARY KEY,
    version VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_celers_migrations_version ON celers_migrations(version);
";

/// `celers_migrations` tracking-table DDL for MySQL, reused (read-only)
/// from `celers-broker-sql`, which already ships and applies this exact
/// table for the same purpose.
const MYSQL_MIGRATIONS_TABLE_SQL: &str =
    include_str!("../../../celers-broker-sql/migrations/000_migrations.sql");

/// Which SQL dialect/driver a `celers db migrate` invocation targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbFlavor {
    Postgres,
    MySql,
}

impl DbFlavor {
    fn migrations(self) -> &'static [MigrationFile] {
        match self {
            DbFlavor::Postgres => POSTGRES_MIGRATIONS,
            DbFlavor::MySql => MYSQL_MIGRATIONS,
        }
    }

    fn migrations_table_sql(self) -> &'static str {
        match self {
            DbFlavor::Postgres => POSTGRES_MIGRATIONS_TABLE_SQL,
            DbFlavor::MySql => MYSQL_MIGRATIONS_TABLE_SQL,
        }
    }

    fn insert_applied_sql(self) -> &'static str {
        match self {
            DbFlavor::Postgres => "INSERT INTO celers_migrations (version, name) VALUES ($1, $2)",
            DbFlavor::MySql => "INSERT INTO celers_migrations (version, name) VALUES (?, ?)",
        }
    }

    /// Run the full text of one migration file's DDL against `conn`.
    ///
    /// Postgres: sent verbatim via `execute_batch` (the simple query
    /// protocol), which the server itself parses statement-by-statement --
    /// correctly ignoring semicolons inside `$$...$$`-quoted plpgsql
    /// function bodies, so no client-side splitting is needed (mirrors
    /// `PostgresBroker::migrate()` exactly).
    ///
    /// MySQL: `Connection::execute`/`execute_batch` both use the
    /// prepared-statement protocol underneath, which rejects multi-statement
    /// text, and `execute_batch`'s naive `;`-split would itself corrupt a
    /// `DELIMITER //`-wrapped stored-procedure body's internal semicolons.
    /// [`split_mysql_migration_statements`] mirrors `MysqlBroker`'s own
    /// (private, unreachable from here) DELIMITER-aware splitter instead.
    async fn run_migration_sql<C: Connection>(self, conn: &C, sql: &str) -> anyhow::Result<()> {
        match self {
            DbFlavor::Postgres => {
                conn.execute_batch(sql).await?;
            }
            DbFlavor::MySql => {
                for statement in split_mysql_migration_statements(sql) {
                    conn.execute(&statement, &[]).await?;
                }
            }
        }
        Ok(())
    }
}

/// Split a MySQL migration file into individual statements ready for
/// [`Connection::execute`].
///
/// Pure text processing (no I/O), so it is directly unit-testable against
/// the real embedded migration files. Handles the one shape this
/// codebase's MySQL migrations use for stored procedures: a
/// `DELIMITER //` ... `DELIMITER ;` section whose body contains semicolons
/// of its own that must not be split on. Statements are returned in
/// execution order; comment-only (`--`) and blank statements are dropped.
///
/// A `;`-delimited segment is dropped only when *every* line in it is blank
/// or a `--` comment -- not merely when the segment's first line is a
/// comment. Every real migration file in this codebase precedes each
/// `CREATE TABLE`/`CREATE INDEX` with a one-line `-- description` comment
/// *within the same semicolon-delimited segment* (the comment and the
/// statement are separated only by a newline, not the previous statement's
/// semicolon), so a naive "does the trimmed segment start with `--`" check
/// discards the real statement along with its leading comment -- silently
/// dropping essentially the entire schema. (`celers-broker-sql`'s own
/// private `MysqlBroker::run_migration` has this exact bug against this
/// exact file; not fixed here since that crate is outside this package's
/// ownership -- see followups.)
#[must_use]
fn split_mysql_migration_statements(sql: &str) -> Vec<String> {
    let sections: Vec<&str> = sql.split("DELIMITER //").collect();
    let mut statements = Vec::new();

    if let Some(main_sql) = sections.first() {
        for statement in main_sql.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            let has_real_sql = trimmed.lines().any(|line| {
                let line = line.trim();
                !line.is_empty() && !line.starts_with("--")
            });
            if has_real_sql {
                statements.push(trimmed.to_string());
            }
        }
    }

    if sections.len() > 1 {
        if let Some(proc_sql) = sections[1].split("DELIMITER ;").next() {
            let trimmed = proc_sql.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
        }
    }

    statements
}

/// Pure: which of `migrations` are not yet in `applied`, in file order.
#[must_use]
fn pending_migrations<'a>(
    migrations: &'a [MigrationFile],
    applied: &std::collections::HashSet<String>,
) -> Vec<&'a MigrationFile> {
    migrations
        .iter()
        .filter(|m| !applied.contains(m.version))
        .collect()
}

/// Pure: normalize a `db migrate` action string.
///
/// Accepts both the CLI's actual default/help vocabulary (`"apply"`,
/// `"rollback"`) and this module's internal vocabulary (`"up"`, `"down"`)
/// as synonyms, plus `"status"`. `celers-cli`'s clap definition
/// (`DbCommands::Migrate`) defaults `--action` to `"apply"` and documents it
/// as "apply, rollback, status" -- without this normalization the bare
/// `celers db migrate` (no explicit `--action`) would always hit the
/// "unknown action" error.
#[must_use]
fn normalize_migrate_action(action: &str) -> Option<&'static str> {
    match action {
        "status" => Some("status"),
        "up" | "apply" => Some("up"),
        "down" | "rollback" => Some("down"),
        _ => None,
    }
}

/// Whether `celers_migrations` exists yet, via schema introspection (never
/// mutates, so `status`/`down` can report accurately even before the first
/// `up` has ever run).
async fn migrations_table_exists<C: Connection>(conn: &C) -> anyhow::Result<bool> {
    let tables = conn
        .tables()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list tables: {e}"))?;
    Ok(tables.iter().any(|t| t.name == "celers_migrations"))
}

/// Fetch the set of applied migration versions. Returns an empty set
/// (rather than erroring) when the tracking table does not exist yet.
async fn applied_migration_versions<C: Connection>(
    conn: &C,
) -> anyhow::Result<std::collections::HashSet<String>> {
    if !migrations_table_exists(conn).await? {
        return Ok(std::collections::HashSet::new());
    }
    let rows = conn
        .query("SELECT version FROM celers_migrations", &[])
        .await
        .map_err(|e| anyhow::anyhow!("failed to read celers_migrations: {e}"))?;
    let mut versions = std::collections::HashSet::with_capacity(rows.len());
    for row in rows {
        versions.insert(row.col::<String>("version")?);
    }
    Ok(versions)
}

async fn ensure_migrations_table<C: Connection>(conn: &C, flavor: DbFlavor) -> anyhow::Result<()> {
    conn.execute_batch(flavor.migrations_table_sql())
        .await
        .map_err(|e| anyhow::anyhow!("failed to create celers_migrations tracking table: {e}"))?;
    Ok(())
}

async fn print_migration_status<C: Connection>(conn: &C, flavor: DbFlavor) -> anyhow::Result<()> {
    println!("{}", "Migration Status:".cyan().bold());
    println!();

    let table_exists = migrations_table_exists(conn).await?;
    let applied = if table_exists {
        applied_migration_versions(conn).await?
    } else {
        std::collections::HashSet::new()
    };

    if !table_exists {
        println!(
            "{}",
            "ℹ celers_migrations tracking table does not exist yet (no `up` has run)".dimmed()
        );
        println!();
    }

    for m in flavor.migrations() {
        if applied.contains(m.version) {
            println!("  {} {} ({})", "✓".green(), m.version, m.name);
        } else {
            println!("  {} {} ({}) -- pending", "○".yellow(), m.version, m.name);
        }
    }

    let pending = pending_migrations(flavor.migrations(), &applied);
    println!();
    if pending.is_empty() {
        println!("{}", "✓ Database is up to date".green().bold());
    } else {
        println!(
            "{}",
            format!(
                "{} of {} migration(s) pending -- run `celers db migrate --action up`",
                pending.len(),
                flavor.migrations().len()
            )
            .yellow()
        );
    }

    Ok(())
}

async fn migrate_up<C: Connection>(conn: &C, flavor: DbFlavor) -> anyhow::Result<()> {
    ensure_migrations_table(conn, flavor).await?;
    let applied = applied_migration_versions(conn).await?;
    let pending = pending_migrations(flavor.migrations(), &applied);

    if pending.is_empty() {
        println!(
            "{}",
            format!(
                "✓ Database is already up to date ({} migration(s) applied)",
                applied.len()
            )
            .green()
            .bold()
        );
        return Ok(());
    }

    println!(
        "{}",
        format!("Running {} pending migration(s)...", pending.len()).cyan()
    );
    println!();

    for m in pending {
        print!("  {} {} ({})... ", "→".cyan(), m.version, m.name);
        use std::io::Write;
        std::io::stdout().flush().ok();

        flavor
            .run_migration_sql(conn, m.sql)
            .await
            .map_err(|e| anyhow::anyhow!("migration {} ({}) failed: {e}", m.version, m.name))?;
        conn.execute(flavor.insert_applied_sql(), &[&m.version, &m.name])
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "migration {} ({}) applied but failed to record in celers_migrations: {e}",
                    m.version,
                    m.name
                )
            })?;

        println!("{}", "done".green());
    }

    println!();
    println!("{}", "✓ Migrations complete".green().bold());
    Ok(())
}

async fn migrate_down<C: Connection>(
    conn: &C,
    flavor: DbFlavor,
    steps: usize,
) -> anyhow::Result<()> {
    let applied = applied_migration_versions(conn).await?;

    // "Most recently applied" per the curated, ordered `flavor.migrations()`
    // list (the tracking table records *that* it applied, not a separate
    // timestamp-derived order): the tail of the migrations whose version is
    // in `applied`.
    let mut applied_in_order: Vec<&MigrationFile> = flavor
        .migrations()
        .iter()
        .filter(|m| applied.contains(m.version))
        .collect();
    applied_in_order.reverse();
    let steps = steps.max(1);
    let to_roll_back: Vec<&MigrationFile> = applied_in_order.into_iter().take(steps).collect();

    if to_roll_back.is_empty() {
        println!("{}", "✓ Nothing to roll back".green());
        return Ok(());
    }

    println!(
        "{}",
        format!(
            "The following {} migration(s) would need to be rolled back:",
            to_roll_back.len()
        )
        .yellow()
        .bold()
    );
    for m in &to_roll_back {
        println!("  {} {} ({})", "•".yellow(), m.version, m.name);
    }
    println!();
    println!(
        "{}",
        "✗ No rollback SQL is shipped for these migrations."
            .red()
            .bold()
    );
    println!(
        "  The embedded migration files are forward-only DDL (mostly \
         `CREATE TABLE IF NOT EXISTS`); an automatic DROP-based rollback \
         would risk destroying data this tool cannot know is safe to lose."
    );
    println!("  Roll back manually, then remove the corresponding row(s) from");
    println!("  celers_migrations once the manual rollback is confirmed safe.");

    anyhow::bail!(
        "no rollback SQL available for {} pending migration(s); manual rollback required",
        to_roll_back.len()
    );
}

/// Database migration management.
///
/// Applies real, tracked schema migrations against a Postgres or MySQL
/// broker database: a `celers_migrations` version table records what has
/// been applied, `up` applies every pending migration (SQL embedded
/// read-only from the matching broker crate's own `migrations/` directory),
/// `status` reports applied/pending without mutating anything, and `down`
/// -- since no rollback SQL ships for these forward-only migrations --
/// reports exactly what would need manual rollback and fails loudly rather
/// than either faking success or guessing at destructive `DROP` statements.
pub async fn db_migrate(url: &str, action: &str, steps: usize) -> anyhow::Result<()> {
    println!("{}", "=== Database Migration ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!("Action: {}", action.yellow());
    println!();

    let Some(normalized_action) = normalize_migrate_action(action) else {
        println!("{}", format!("✗ Unknown migration action: {action}").red());
        println!("  Available actions: status, up (apply), down (rollback)");
        anyhow::bail!("unknown migration action: {action}");
    };

    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let tls_mode = pg_tls_mode_for_url(url)?;
        let conn = PgConnection::connect(url, tls_mode).await?;
        match normalized_action {
            "status" => print_migration_status(&conn, DbFlavor::Postgres).await,
            "up" => migrate_up(&conn, DbFlavor::Postgres).await,
            "down" => migrate_down(&conn, DbFlavor::Postgres, steps).await,
            _ => unreachable!("normalize_migrate_action only returns status/up/down"),
        }
    } else if url.starts_with("mysql://") {
        let tls_mode = mysql_tls_mode_for_url(url)?;
        let conn = MyConnection::connect(url, tls_mode).await?;
        match normalized_action {
            "status" => print_migration_status(&conn, DbFlavor::MySql).await,
            "up" => migrate_up(&conn, DbFlavor::MySql).await,
            "down" => migrate_down(&conn, DbFlavor::MySql, steps).await,
            _ => unreachable!("normalize_migrate_action only returns status/up/down"),
        }
    } else {
        println!(
            "{}",
            "✗ Unsupported database type. Use PostgreSQL or MySQL URL."
                .red()
                .bold()
        );
        anyhow::bail!("unsupported database type for migrations");
    }
}

/// Run live dashboard
pub async fn run_dashboard(broker_url: &str, queue: &str, refresh_secs: u64) -> anyhow::Result<()> {
    println!("{}", "=== CeleRS Live Dashboard ===".bold().green());
    println!(
        "{}",
        format!("Refreshing every {refresh_secs} seconds (Ctrl+C to stop)").dimmed()
    );
    println!();

    let broker = celers_broker_redis::RedisBroker::new(broker_url, queue)?;

    // Hoisted out of the loop: a fresh client/connection every refresh tick
    // churned TCP connections for the lifetime of the dashboard.
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    loop {
        // Clear screen
        print!("\x1B[2J\x1B[1;1H");

        println!("{}", "╔══════════════════════════════════════╗".cyan());
        println!(
            "{}",
            "║      CeleRS Live Dashboard           ║".cyan().bold()
        );
        println!("{}", "╚══════════════════════════════════════╝".cyan());
        println!();

        let now = chrono::Utc::now();
        println!(
            "{}",
            format!("Last updated: {}", now.format("%Y-%m-%d %H:%M:%S")).dimmed()
        );
        println!();

        // Queue metrics
        let queue_size = broker.queue_size().await.unwrap_or(0);
        let dlq_size = broker.dlq_size().await.unwrap_or(0);

        println!("{}", "Queue Status:".cyan().bold());
        println!("  {} {}", "Pending:".yellow(), queue_size);
        println!("  {} {}", "DLQ:".yellow(), dlq_size);
        println!();

        // Worker metrics
        let worker_keys =
            crate::commands::monitoring::report::scan_worker_heartbeat_keys(&mut conn)
                .await
                .unwrap_or_default();

        println!("{}", "Workers:".cyan().bold());
        println!("  {} {}", "Active:".yellow(), worker_keys.len());
        println!();

        // Memory
        if let Ok(info) = redis::cmd("INFO")
            .arg("memory")
            .query_async::<String>(&mut conn)
            .await
        {
            for line in info.lines() {
                if line.starts_with("used_memory_human:") {
                    let memory = line.split(':').nth(1).unwrap_or("N/A");
                    println!("{}", "Redis Memory:".cyan().bold());
                    println!("  {} {}", "Used:".yellow(), memory);
                    break;
                }
            }
        }

        println!();

        // Health status
        if dlq_size > 0 {
            println!("{}", format!("⚠ {dlq_size} tasks in DLQ").yellow().bold());
        }
        if worker_keys.is_empty() && queue_size > 0 {
            println!("{}", "⚠ No workers available!".red().bold());
        }
        if dlq_size == 0 && (worker_keys.is_empty() || queue_size == 0) {
            println!("{}", "✓ System healthy".green().bold());
        }

        println!();
        println!(
            "{}",
            format!("Press Ctrl+C to exit | Refresh: {refresh_secs}s").dimmed()
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(refresh_secs)).await;
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;

    // ---- migration list integrity (static sanity checks) -----------------

    fn assert_no_duplicate_versions(migrations: &[MigrationFile]) {
        let mut seen = std::collections::HashSet::new();
        for m in migrations {
            assert!(
                seen.insert(m.version),
                "duplicate migration version {} in list",
                m.version
            );
        }
    }

    #[test]
    fn postgres_migrations_have_unique_versions_and_nonempty_sql() {
        assert!(!POSTGRES_MIGRATIONS.is_empty());
        assert_no_duplicate_versions(POSTGRES_MIGRATIONS);
        for m in POSTGRES_MIGRATIONS {
            assert!(!m.sql.trim().is_empty(), "{} has empty SQL", m.version);
            assert!(!m.name.is_empty());
        }
    }

    #[test]
    fn mysql_migrations_have_unique_versions_and_nonempty_sql() {
        assert!(!MYSQL_MIGRATIONS.is_empty());
        assert_no_duplicate_versions(MYSQL_MIGRATIONS);
        for m in MYSQL_MIGRATIONS {
            assert!(!m.sql.trim().is_empty(), "{} has empty SQL", m.version);
            assert!(!m.name.is_empty());
        }
    }

    #[test]
    fn migrations_table_ddl_is_nonempty_for_both_flavors() {
        assert!(DbFlavor::Postgres
            .migrations_table_sql()
            .contains("celers_migrations"));
        assert!(DbFlavor::MySql
            .migrations_table_sql()
            .contains("celers_migrations"));
    }

    // ---- normalize_migrate_action -----------------------------------------

    #[test]
    fn normalize_migrate_action_accepts_cli_default_vocabulary() {
        // clap's DbCommands::Migrate defaults --action to "apply" and
        // documents "apply, rollback, status" -- these must all resolve.
        assert_eq!(normalize_migrate_action("apply"), Some("up"));
        assert_eq!(normalize_migrate_action("rollback"), Some("down"));
        assert_eq!(normalize_migrate_action("status"), Some("status"));
    }

    #[test]
    fn normalize_migrate_action_accepts_internal_vocabulary() {
        assert_eq!(normalize_migrate_action("up"), Some("up"));
        assert_eq!(normalize_migrate_action("down"), Some("down"));
    }

    #[test]
    fn normalize_migrate_action_rejects_unknown() {
        assert_eq!(normalize_migrate_action("bogus"), None);
        assert_eq!(normalize_migrate_action(""), None);
    }

    // ---- pending_migrations ------------------------------------------------

    fn sample_migrations() -> Vec<MigrationFile> {
        vec![
            MigrationFile {
                version: "001",
                name: "a",
                sql: "SELECT 1;",
            },
            MigrationFile {
                version: "002",
                name: "b",
                sql: "SELECT 2;",
            },
            MigrationFile {
                version: "003",
                name: "c",
                sql: "SELECT 3;",
            },
        ]
    }

    #[test]
    fn pending_migrations_excludes_applied_and_preserves_order() {
        let migrations = sample_migrations();
        let mut applied = std::collections::HashSet::new();
        applied.insert("002".to_string());

        let pending = pending_migrations(&migrations, &applied);
        let versions: Vec<&str> = pending.iter().map(|m| m.version).collect();
        assert_eq!(versions, vec!["001", "003"]);
    }

    #[test]
    fn pending_migrations_empty_when_all_applied() {
        let migrations = sample_migrations();
        let applied: std::collections::HashSet<String> =
            migrations.iter().map(|m| m.version.to_string()).collect();
        assert!(pending_migrations(&migrations, &applied).is_empty());
    }

    #[test]
    fn pending_migrations_all_pending_when_none_applied() {
        let migrations = sample_migrations();
        let applied = std::collections::HashSet::new();
        assert_eq!(pending_migrations(&migrations, &applied).len(), 3);
    }

    // ---- split_mysql_migration_statements ----------------------------------

    #[test]
    fn splits_plain_ddl_on_semicolons() {
        let sql = "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);\n";
        let statements = split_mysql_migration_statements(sql);
        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("CREATE TABLE a"));
        assert!(statements[1].starts_with("CREATE TABLE b"));
    }

    #[test]
    fn skips_blank_and_comment_only_statements() {
        let sql = "CREATE TABLE a (id INT);\n\n-- just a comment\n;\nCREATE TABLE b (id INT);";
        let statements = split_mysql_migration_statements(sql);
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn keeps_statement_preceded_by_a_leading_comment_line() {
        // Every real migration file in this codebase writes exactly this
        // shape: a one-line `-- description` comment immediately before a
        // `CREATE TABLE`, both landing in the *same* `;`-delimited segment
        // (the comment is separated from the statement only by a newline,
        // not the previous statement's semicolon). A naive "does the
        // trimmed segment start with `--`" check discards the real
        // statement along with its leading comment; this must not happen.
        let sql = "\
-- Dead Letter Queue for permanently failed tasks
CREATE TABLE IF NOT EXISTS celers_dead_letter_queue (
    id CHAR(36) PRIMARY KEY
) ENGINE=InnoDB;

-- Index for DLQ queries
CREATE INDEX idx_dlq_failed_at ON celers_dead_letter_queue(failed_at DESC);
";
        let statements = split_mysql_migration_statements(sql);
        assert_eq!(
            statements.len(),
            2,
            "both comment-prefixed statements must survive: {statements:?}"
        );
        assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS celers_dead_letter_queue"));
        assert!(statements[1].contains("CREATE INDEX idx_dlq_failed_at"));
    }

    #[test]
    fn drops_segment_that_is_multiple_comment_lines_with_no_real_sql() {
        let sql = "\
-- header comment line one
-- header comment line two
CREATE TABLE a (id INT);
";
        let statements = split_mysql_migration_statements(sql);
        // Only the final segment (after the last `;`) is pure header
        // comment with nothing else -- but here there is no trailing
        // segment at all after the one real statement, so this simply
        // confirms the leading two-comment-line header does not itself
        // spawn a phantom statement and the real one is kept exactly once.
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("CREATE TABLE a"));
    }

    #[test]
    fn keeps_delimiter_wrapped_procedure_body_as_one_statement() {
        let sql = "\
CREATE TABLE celers_tasks (id INT);
DELIMITER //
CREATE PROCEDURE move_to_dlq(IN task_id VARCHAR(36))
BEGIN
    INSERT INTO celers_dead_letter_queue SELECT * FROM celers_tasks WHERE id = task_id;
    DELETE FROM celers_tasks WHERE id = task_id;
END //
DELIMITER ;
";
        let statements = split_mysql_migration_statements(sql);
        // One statement for the leading DDL, one for the whole procedure body.
        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("CREATE TABLE celers_tasks"));
        let proc_stmt = &statements[1];
        assert!(proc_stmt.contains("CREATE PROCEDURE move_to_dlq"));
        // The procedure body's internal semicolons must survive intact --
        // this is exactly what a naive `;`-split would have destroyed.
        assert!(proc_stmt.contains("INSERT INTO celers_dead_letter_queue"));
        assert!(proc_stmt.contains("DELETE FROM celers_tasks"));
        assert_eq!(proc_stmt.matches(';').count(), 2);
    }

    #[test]
    fn real_mysql_init_migration_splits_without_losing_the_procedure() {
        // Regression guard against the real embedded file: confirms the
        // splitter (not just a hand-written fixture) handles it, and that
        // the procedure body is not fragmented into partial statements.
        let statements = split_mysql_migration_statements(MYSQL_MIGRATIONS[0].sql);
        assert!(statements.len() >= 2);
        let has_procedure_statement = statements
            .iter()
            .any(|s| s.to_uppercase().contains("CREATE PROCEDURE"));
        assert!(
            has_procedure_statement,
            "expected one statement containing the stored procedure body"
        );
    }

    #[test]
    fn no_delimiter_section_only_splits_main_sql() {
        let sql = "CREATE TABLE a (id INT);\nCREATE INDEX idx_a ON a(id);";
        let statements = split_mysql_migration_statements(sql);
        assert_eq!(statements.len(), 2);
    }
}
