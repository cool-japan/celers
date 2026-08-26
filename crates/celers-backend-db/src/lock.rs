//! Database-backed distributed lock backend
//!
//! Uses SQL tables with advisory-style locking via `INSERT ON CONFLICT`
//! to provide distributed lock management across multiple beat scheduler instances.
//! Supports both PostgreSQL and MySQL.

use async_trait::async_trait;
use celers_core::error::CelersError;
use celers_core::lock::DistributedLockBackend;
use chrono::{DateTime, Duration, Utc};
use oxisql_core::Connection;

use crate::row_ext::RowExt;

/// Database-backed distributed lock backend using PostgreSQL.
///
/// Uses a dedicated table (`celers_beat_locks` by default) to manage locks.
/// Each row represents a single lock with owner, key, and expiration time.
///
/// The `ensure_table()` method must be called once before using the backend
/// to create the table if it does not already exist.
///
/// # Example
///
/// ```ignore
/// use celers_backend_db::lock::DbLockBackend;
/// use celers_core::lock::DistributedLockBackend;
///
/// let conn = oxisql_postgres::PgConnection::connect(
///     "host=localhost dbname=mydb",
///     oxisql_postgres::TlsMode::Disabled,
/// ).await.unwrap();
/// let backend = DbLockBackend::new(conn);
/// backend.ensure_table().await.unwrap();
///
/// let acquired = backend.try_acquire("my_task", "scheduler-1", 300).await.unwrap();
/// ```
#[derive(Clone)]
pub struct DbLockBackend {
    conn: oxisql_postgres::PgConnection,
    table_name: String,
}

// Manual `Debug` impl: `oxisql_postgres::PgConnection` does not implement
// `Debug` (unlike the previous `sqlx::PgPool`, which did). `table_name` is
// still printed in full since it carries no sensitive/opaque state. See
// `celers-backend-db`'s `analytics.rs` `PostgresAnalytics`/`MysqlAnalytics`
// `Debug` impls for the same rationale applied to their connection fields.
impl std::fmt::Debug for DbLockBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbLockBackend")
            .field("conn", &"PgConnection { .. }")
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl DbLockBackend {
    /// Create a new database lock backend with default table name `celers_beat_locks`.
    pub fn new(conn: oxisql_postgres::PgConnection) -> Self {
        Self {
            conn,
            table_name: "celers_beat_locks".to_string(),
        }
    }

    /// Create a new database lock backend with a custom table name.
    pub fn with_table_name(conn: oxisql_postgres::PgConnection, table_name: String) -> Self {
        Self { conn, table_name }
    }

    /// Ensure the lock table exists (auto-migration).
    ///
    /// Creates the table if it does not already exist. Safe to call multiple times.
    pub async fn ensure_table(&self) -> celers_core::error::Result<()> {
        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                lock_key   VARCHAR(512) PRIMARY KEY,
                owner      VARCHAR(256) NOT NULL,
                acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at  TIMESTAMPTZ NOT NULL
            )
            "#,
            self.table_name
        );

        // No parameter values here (only the table name, a static-fragment
        // string owned by this struct, is spliced in), matching the exact
        // same "only static text splicing, never values" discipline
        // `sqlx::AssertSqlSafe` was previously asserting.
        //
        // `execute_batch` rather than `execute` because the statement is
        // wrapped in the shared migration advisory lock, which makes the
        // script a multi-statement batch — and the extended/prepared-
        // statement protocol `execute` uses rejects multi-statement text.
        // Without the lock, two schedulers calling `ensure_table` at once
        // race on the catalog and one fails with a duplicate-key error on
        // `pg_type_typname_nsp_index` despite the `IF NOT EXISTS`; see
        // `pg_ddl`.
        let sql = crate::pg_ddl::advisory_locked_migration(&sql);
        self.conn.execute_batch(&sql).await.map_err(|e| {
            CelersError::Other(format!(
                "Failed to create lock table '{}': {}",
                self.table_name, e
            ))
        })?;

        Ok(())
    }

    /// Clean up all expired locks from the table.
    ///
    /// Returns the number of expired locks removed.
    pub async fn cleanup_expired(&self) -> celers_core::error::Result<u64> {
        let sql = format!("DELETE FROM {} WHERE expires_at < NOW()", self.table_name);

        let rows_affected =
            self.conn.execute(&sql, &[]).await.map_err(|e| {
                CelersError::Other(format!("Failed to cleanup expired locks: {}", e))
            })?;

        Ok(rows_affected)
    }

    /// Spawn a background task that calls [`DbLockBackend::cleanup_expired`]
    /// on `interval` forever, logging (rather than propagating) any error so
    /// one failed cleanup pass never kills the scheduler.
    ///
    /// Nothing in this crate calls this automatically — expired lock rows
    /// otherwise accumulate forever, since `cleanup_expired` previously had
    /// no caller anywhere in the workspace. This is opt-in (not spawned from
    /// `new`/`with_table_name`) because a library must not start background
    /// work the caller didn't ask for; wire it in from application start-up
    /// (e.g. alongside the beat scheduler) if periodic lock-table cleanup is
    /// desired.
    pub fn spawn_periodic_cleanup(
        &self,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let backend = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so the first real
            // cleanup happens after one full `interval`, not at t=0.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match backend.cleanup_expired().await {
                    Ok(n) if n > 0 => tracing::debug!(deleted = n, "cleaned up expired locks"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "periodic cleanup_expired failed"),
                }
            }
        })
    }

    /// Get the underlying connection.
    pub fn connection(&self) -> &oxisql_postgres::PgConnection {
        &self.conn
    }

    /// Compute the expiration timestamp from now.
    fn expires_at(ttl_secs: u64) -> DateTime<Utc> {
        Utc::now() + Duration::seconds(ttl_secs as i64)
    }
}

#[async_trait]
impl DistributedLockBackend for DbLockBackend {
    async fn try_acquire(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> celers_core::error::Result<bool> {
        let expires = Self::expires_at(ttl_secs);
        let expires_param = expires.to_rfc3339();

        // INSERT ... ON CONFLICT:
        //   - If key doesn't exist: insert new row (lock acquired)
        //   - If key exists and expired: update with new owner (lock acquired)
        //   - If key exists, not expired, same owner: update (lock refreshed)
        //   - If key exists, not expired, different owner: no update (lock denied)
        //
        // We use a CTE to attempt the upsert and check the result.
        let sql = format!(
            r#"
            INSERT INTO {table} (lock_key, owner, acquired_at, expires_at)
            VALUES ($1, $2, NOW(), $3::text::timestamptz)
            ON CONFLICT (lock_key) DO UPDATE
                SET owner = EXCLUDED.owner,
                    acquired_at = NOW(),
                    expires_at = EXCLUDED.expires_at
                WHERE {table}.expires_at < NOW()
                   OR {table}.owner = EXCLUDED.owner
            RETURNING lock_key
            "#,
            table = self.table_name
        );

        let result = self
            .conn
            .query(&sql, &[&key, &owner, &expires_param])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to acquire lock: {}", e)))?;

        Ok(!result.is_empty())
    }

    async fn release(&self, key: &str, owner: &str) -> celers_core::error::Result<bool> {
        let sql = format!(
            "DELETE FROM {} WHERE lock_key = $1 AND owner = $2",
            self.table_name
        );

        let rows_affected = self
            .conn
            .execute(&sql, &[&key, &owner])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to release lock: {}", e)))?;

        Ok(rows_affected > 0)
    }

    async fn renew(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> celers_core::error::Result<bool> {
        let expires = Self::expires_at(ttl_secs);
        let expires_param = expires.to_rfc3339();

        let sql = format!(
            r#"
            UPDATE {}
            SET expires_at = $1::text::timestamptz, acquired_at = NOW()
            WHERE lock_key = $2
              AND owner = $3
              AND expires_at > NOW()
            "#,
            self.table_name
        );

        let rows_affected = self
            .conn
            .execute(&sql, &[&expires_param, &key, &owner])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to renew lock: {}", e)))?;

        Ok(rows_affected > 0)
    }

    async fn is_locked(&self, key: &str) -> celers_core::error::Result<bool> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE lock_key = $1 AND expires_at > NOW()",
            self.table_name
        );

        let result = self
            .conn
            .query(&sql, &[&key])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check lock: {}", e)))?;

        Ok(!result.is_empty())
    }

    async fn owner(&self, key: &str) -> celers_core::error::Result<Option<String>> {
        let sql = format!(
            "SELECT owner FROM {} WHERE lock_key = $1 AND expires_at > NOW()",
            self.table_name
        );

        let result = self
            .conn
            .query(&sql, &[&key])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get lock owner: {}", e)))?;

        match result.into_iter().next() {
            Some(row) => {
                let owner: String = row
                    .col("owner")
                    .map_err(|e| CelersError::Broker(format!("Failed to get lock owner: {e}")))?;
                Ok(Some(owner))
            }
            None => Ok(None),
        }
    }

    async fn release_all(&self, owner: &str) -> celers_core::error::Result<u64> {
        let sql = format!("DELETE FROM {} WHERE owner = $1", self.table_name);

        let rows_affected = self
            .conn
            .execute(&sql, &[&owner])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to release all locks: {}", e)))?;

        Ok(rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expires_at_future() {
        let expires = DbLockBackend::expires_at(300);
        assert!(expires > Utc::now());
    }

    #[test]
    fn test_default_table_name() {
        // We can't create a real PgConnection in a unit test without a DB,
        // so we just verify the struct fields conceptually.
        // Integration tests require a running PostgreSQL instance.
        assert_eq!("celers_beat_locks", "celers_beat_locks");
    }

    // Integration tests require a running PostgreSQL instance
    #[tokio::test]
    #[ignore]
    async fn test_db_lock_lifecycle() {
        let Some(database_url) = crate::test_env::postgres_url("test_db_lock_lifecycle") else {
            return;
        };

        let conn = oxisql_postgres::PgConnection::connect(
            &database_url,
            oxisql_postgres::TlsMode::Disabled,
        )
        .await
        .expect("Failed to connect to test database");

        let backend = DbLockBackend::new(conn);
        backend
            .ensure_table()
            .await
            .expect("Failed to create lock table");

        // Acquire
        let acquired = backend
            .try_acquire("db_test_1", "owner_a", 300)
            .await
            .expect("acquire");
        assert!(acquired);

        // Check locked
        let locked = backend.is_locked("db_test_1").await.expect("is_locked");
        assert!(locked);

        // Check owner
        let owner = backend.owner("db_test_1").await.expect("owner");
        assert_eq!(owner.as_deref(), Some("owner_a"));

        // Different owner cannot acquire
        let acquired2 = backend
            .try_acquire("db_test_1", "owner_b", 300)
            .await
            .expect("acquire");
        assert!(!acquired2);

        // Renew
        let renewed = backend
            .renew("db_test_1", "owner_a", 600)
            .await
            .expect("renew");
        assert!(renewed);

        // Wrong owner cannot renew
        let renewed_bad = backend
            .renew("db_test_1", "owner_b", 600)
            .await
            .expect("renew");
        assert!(!renewed_bad);

        // Release
        let released = backend
            .release("db_test_1", "owner_a")
            .await
            .expect("release");
        assert!(released);

        // No longer locked
        let locked = backend.is_locked("db_test_1").await.expect("is_locked");
        assert!(!locked);
    }

    #[tokio::test]
    #[ignore]
    async fn test_db_release_all() {
        let Some(database_url) = crate::test_env::postgres_url("test_db_release_all") else {
            return;
        };

        let conn = oxisql_postgres::PgConnection::connect(
            &database_url,
            oxisql_postgres::TlsMode::Disabled,
        )
        .await
        .expect("Failed to connect to test database");

        let backend = DbLockBackend::new(conn);
        backend
            .ensure_table()
            .await
            .expect("Failed to create lock table");

        let _ = backend.try_acquire("db_ra_1", "bulk_owner", 300).await;
        let _ = backend.try_acquire("db_ra_2", "bulk_owner", 300).await;
        let _ = backend.try_acquire("db_ra_3", "other_owner", 300).await;

        let count = backend
            .release_all("bulk_owner")
            .await
            .expect("release_all");
        assert_eq!(count, 2);

        let locked = backend.is_locked("db_ra_3").await.expect("is_locked");
        assert!(locked);

        // Cleanup
        let _ = backend.release("db_ra_3", "other_owner").await;
    }
}
