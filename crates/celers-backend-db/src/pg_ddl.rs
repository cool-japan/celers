//! Concurrency-safe PostgreSQL auto-migration helpers.
//!
//! # Why `CREATE TABLE IF NOT EXISTS` is not enough
//!
//! Every CeleRS PostgreSQL component auto-migrates on start-up
//! ([`crate::PostgresResultBackend::migrate`],
//! [`crate::event_persistence`]'s `migrate`, [`crate::lock::DbLockBackend`]'s
//! `ensure_table`), and a normal deployment starts *many* workers at once —
//! so those migrations genuinely run concurrently against one database.
//!
//! `CREATE TABLE IF NOT EXISTS` (and `CREATE INDEX IF NOT EXISTS`) is **not**
//! atomic with respect to a concurrent creation of the same object. The
//! existence check and the catalog insert are separate steps, so two sessions
//! can both observe "absent" and then both insert, and the loser fails with
//!
//! ```text
//! ERROR: duplicate key value violates unique constraint "pg_type_typname_nsp_index"
//! ```
//!
//! This is documented upstream behaviour, not a PostgreSQL bug: the
//! `IF NOT EXISTS` forms are advertised as avoiding an error when the object
//! *already* exists, not as being safe against a simultaneous `CREATE`. The
//! observable failure mode for CeleRS was a fraction of workers dying at
//! start-up with an opaque `Migration failed: execution error: db error`,
//! with the real cause visible only in the server log.
//!
//! # The fix
//!
//! Serialize the DDL on a session-independent
//! [advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS).
//! [`advisory_locked_migration`] wraps a migration script so that it takes
//! [`MIGRATION_ADVISORY_LOCK_KEY`] before running any DDL: the first caller
//! proceeds, later callers block until it commits and then find every object
//! already present, which is the case `IF NOT EXISTS` *does* handle safely.
//!
//! Two details matter:
//!
//! * **`pg_advisory_xact_lock`, not `pg_advisory_lock`.** The transaction-
//!   scoped variant is released automatically when the wrapping transaction
//!   ends, including on error. The session-scoped variant would have to be
//!   released by hand and would leak on any early return — and because these
//!   connections are pooled and reused ([`crate::PgConnPool`]), a leaked
//!   session lock would outlive the migration and deadlock the next caller
//!   that borrowed the same slot.
//!
//! * **One connection, one transaction.** The returned script is a single
//!   `;`-separated batch meant for `execute_batch` (`batch_execute`, the
//!   simple-query protocol), which `oxisql_postgres` passes to the server
//!   verbatim — no client-side statement splitting, so dollar-quoted bodies
//!   such as `CREATE OR REPLACE FUNCTION ... AS $$ ... $$` survive intact.
//!   [`crate::PgConnPool::execute_batch`] picks one pool slot and runs the
//!   whole string on it, so the `BEGIN`, the lock and the DDL all share a
//!   connection, which is what makes the transaction-scoped lock cover the
//!   DDL at all.
//!
//! PostgreSQL has fully transactional DDL, and none of CeleRS's migrations
//! use `CREATE INDEX CONCURRENTLY` (the one form that cannot run inside a
//! transaction block), so wrapping them in `BEGIN`/`COMMIT` is safe and has
//! the bonus that a failed migration leaves no half-created schema behind.
//!
//! MySQL has no `pg_advisory_*`; its auto-migrations are not wrapped by this
//! module.

/// The advisory-lock key every CeleRS PostgreSQL auto-migration takes.
///
/// The value is arbitrary but must be *stable* and shared by every migration
/// path in the process tree, since two callers only serialize against each
/// other when they contend on the same key. It is the ASCII bytes of
/// `"celers"` read as a big-endian integer, which keeps it recognisable in
/// `pg_locks` (`SELECT * FROM pg_locks WHERE locktype = 'advisory'`) and
/// makes an accidental collision with an application's own advisory locks
/// vanishingly unlikely.
pub const MIGRATION_ADVISORY_LOCK_KEY: i64 = 0x63_65_6c_65_72_73;

/// Wrap a migration script so concurrent callers serialize instead of racing.
///
/// See the [module documentation](self) for why this is necessary. The result
/// is intended for `execute_batch` (simple-query protocol) — it contains
/// multiple `;`-separated statements, which the extended/prepared-statement
/// protocol rejects.
///
/// A statement failing anywhere in the batch aborts it, so the trailing
/// `COMMIT` is never reached and the server rolls the transaction back,
/// releasing the advisory lock with it.
///
/// A `;` is appended when `ddl` does not already end with one: a single-
/// statement script (such as [`crate::lock::DbLockBackend`]'s
/// `CREATE TABLE IF NOT EXISTS`) has no reason to carry a terminator of its
/// own, and without one it would run straight into the `COMMIT` and fail with
/// `syntax error at or near "COMMIT"`.
pub fn advisory_locked_migration(ddl: &str) -> String {
    let ddl = ddl.trim_end();
    let terminator = if ddl.ends_with(';') { "" } else { ";" };
    format!(
        "BEGIN;\nSELECT pg_advisory_xact_lock({MIGRATION_ADVISORY_LOCK_KEY});\n\
         {ddl}{terminator}\nCOMMIT;\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapped_migration_takes_the_lock_before_any_ddl() {
        let out = advisory_locked_migration("CREATE TABLE IF NOT EXISTS t (a int);");

        let lock_at = out.find("pg_advisory_xact_lock").expect("lock statement");
        let ddl_at = out.find("CREATE TABLE").expect("ddl");
        assert!(
            lock_at < ddl_at,
            "the advisory lock must be acquired before any DDL runs, got:\n{out}"
        );
    }

    #[test]
    fn wrapped_migration_is_a_single_transaction() {
        let out = advisory_locked_migration("CREATE TABLE IF NOT EXISTS t (a int);");
        assert!(out.starts_with("BEGIN;"), "must open a transaction: {out}");
        assert!(out.trim_end().ends_with("COMMIT;"), "must commit: {out}");
    }

    #[test]
    fn wrapped_migration_uses_the_transaction_scoped_lock() {
        // `pg_advisory_lock` (session-scoped) would leak across the pooled
        // connection's next use; only the `_xact_` variant self-releases.
        let out = advisory_locked_migration("CREATE TABLE IF NOT EXISTS t (a int);");
        assert!(out.contains("pg_advisory_xact_lock"));
        assert!(
            !out.contains("SELECT pg_advisory_lock("),
            "must not use the session-scoped advisory lock: {out}"
        );
    }

    #[test]
    fn unterminated_ddl_gets_a_semicolon_before_the_commit() {
        // `DbLockBackend::ensure_table`'s script is a single `CREATE TABLE`
        // with no trailing `;`. Without one it runs into `COMMIT` and the
        // server reports `syntax error at or near "COMMIT"`.
        let out = advisory_locked_migration("CREATE TABLE IF NOT EXISTS t (a int)\n");
        assert!(
            out.contains("(a int);\nCOMMIT;"),
            "unterminated DDL must be terminated before COMMIT, got:\n{out}"
        );
    }

    #[test]
    fn already_terminated_ddl_is_not_double_terminated() {
        let out = advisory_locked_migration("CREATE TABLE IF NOT EXISTS t (a int);\n");
        assert!(!out.contains(";;"), "must not double-terminate:\n{out}");
        assert!(out.contains("(a int);\nCOMMIT;"), "got:\n{out}");
    }

    #[test]
    fn wrapped_migration_preserves_the_script_verbatim() {
        // Dollar-quoted function bodies contain `;` and must not be mangled.
        let ddl = "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;";
        let out = advisory_locked_migration(ddl);
        assert!(out.contains(ddl), "script must be embedded verbatim: {out}");
    }

    #[test]
    fn every_migration_path_shares_one_key() {
        // Two callers only serialize when they contend on the same key, so
        // this constant is load-bearing: a per-call-site key would restore
        // the race it exists to remove.
        assert_eq!(MIGRATION_ADVISORY_LOCK_KEY, 0x63_65_6c_65_72_73);
        assert_eq!(
            MIGRATION_ADVISORY_LOCK_KEY.to_be_bytes()[2..],
            *b"celers",
            "key should stay recognisable as \"celers\" in pg_locks"
        );
    }
}
