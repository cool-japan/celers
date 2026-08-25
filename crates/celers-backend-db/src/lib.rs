//! Database result backend for CeleRS
//!
//! This crate provides PostgreSQL and MySQL-based storage for task results and workflow state.
//!
//! # Features
//!
//! - Task result storage with expiration
//! - Chord state management (barrier synchronization)
//! - Atomic counter operations
//! - SQL-based result queries and analytics
//! - Support for both PostgreSQL and MySQL, independently selectable via the
//!   `postgres`/`mysql` Cargo features (both enabled by default)
//!
//! # Example
//!
//! ```ignore
//! use celers_backend_db::PostgresResultBackend;
//! use celers_backend_redis::ResultBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = PostgresResultBackend::new("postgres://localhost/celers").await?;
//! backend.migrate().await?;
//!
//! // Store task result
//! let meta = TaskMeta::new(task_id, "my_task".to_string());
//! backend.store_result(task_id, &meta).await?;
//! # Ok(())
//! # }
//! ```

pub mod analytics;
#[cfg(feature = "postgres")]
pub mod event_persistence;
#[cfg(all(feature = "distributed-locks", feature = "postgres"))]
pub mod lock;
#[cfg(feature = "mysql")]
mod mysql_backend;
#[cfg(feature = "postgres")]
mod pg_pool;
#[cfg(feature = "postgres")]
mod postgres_backend;
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub mod result_compression;
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub mod result_store;
mod row_ext;
#[cfg(feature = "mysql")]
mod sql_split;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod task_meta_extra;
mod tls_mode;

#[cfg(feature = "mysql")]
pub use analytics::MysqlAnalytics;
#[cfg(feature = "postgres")]
pub use analytics::PostgresAnalytics;
pub use analytics::{PercentileLatencies, StorageStats, TaskStats, WorkerStat};
#[cfg(feature = "postgres")]
pub use event_persistence::{DbEventPersister, DbEventPersisterConfig};
#[cfg(feature = "mysql")]
pub use mysql_backend::MysqlResultBackend;
#[cfg(feature = "postgres")]
pub use pg_pool::{PgConnPool, DEFAULT_POOL_SIZE};
#[cfg(feature = "postgres")]
pub use postgres_backend::PostgresResultBackend;

pub use celers_backend_redis::{
    BackendError, ChordState, ProgressInfo, Result, ResultBackend, TaskMeta, TaskResult,
    TaskTtlConfig,
};
// Only reached from `default_ttl_config` (postgres/mysql constructors), not
// re-exported: `RedisResultBackend::new`'s own default lives behind this
// same path, so importing it here — rather than repeating the bare
// `Duration::from_secs(86400)` literal it expands to — keeps both backends'
// "24 hours" defined in exactly one place.
#[cfg(any(feature = "postgres", feature = "mysql"))]
use celers_backend_redis::ttl;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use serde_json::json;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use uuid::Uuid;

/// Decode a stored `result_state` string (plus its accompanying columns)
/// into a [`TaskResult`].
///
/// Shared by every DB read path (Postgres/MySQL, single/batch) so the
/// mapping is defined exactly once. Unlike the ad-hoc `match` this replaces,
/// an unrecognised state is a hard [`BackendError::Serialization`] rather
/// than a silent `TaskResult::Pending` — a schema/code vocabulary drift
/// (a newer writer, a manual data fix, a partially-applied migration, a
/// mixed-version rolling deploy) must be loud, not indistinguishable from a
/// task that simply hasn't run yet: a caller polling `is_task_complete` on a
/// silently-mispapped row would otherwise wait forever, and it would count in
/// neither the success nor the failure bucket of `TaskStats`.
///
/// # Errors
///
/// Returns [`BackendError::Serialization`] if `state` is not one of
/// `"pending"`/`"started"`/`"success"`/`"failure"`/`"revoked"`/`"retry"`.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(crate) fn decode_result_state(
    task_id: Uuid,
    state: &str,
    result_data: Option<serde_json::Value>,
    error_message: Option<String>,
    retry_count: Option<i32>,
) -> Result<TaskResult> {
    Ok(match state {
        "pending" => TaskResult::Pending,
        "started" => TaskResult::Started,
        "success" => TaskResult::Success(result_data.unwrap_or(json!(null))),
        // A NULL error_message on a `failure` row is a genuine anomaly (the
        // failure path should always record a reason), but it must not
        // silently downgrade to an empty string that looks like "no error" —
        // surface it as an explicit placeholder instead.
        "failure" => TaskResult::Failure(
            error_message.unwrap_or_else(|| "unknown error (error_message was NULL)".to_string()),
        ),
        "revoked" => TaskResult::Revoked,
        "retry" => TaskResult::Retry(retry_count.unwrap_or(0) as u32),
        other => {
            return Err(BackendError::Serialization(format!(
                "unknown result_state {other:?} for task {task_id}"
            )))
        }
    })
}

/// The TTL configuration every `*ResultBackend::new`/`with_pool_size`
/// constructor installs: results expire after [`ttl::SUCCESS`] (24 hours),
/// matching [`RedisResultBackend`](celers_backend_redis::RedisResultBackend)'s
/// own `new`-time default and Celery's `result_expires`.
///
/// Without a default TTL, `store_result`'s `expires_at` column is left NULL
/// for every task type that has no explicit `set_task_ttl`/`with_ttl_config`
/// override (see `ttl_expires_at_param`), and `cleanup_expired_results()`
/// only ever deletes rows `WHERE expires_at IS NOT NULL` — so with no
/// default, cleanup silently collects nothing and `celers_task_results`
/// grows without bound. Shared by every constructor (rather than each
/// repeating `TaskTtlConfig::with_default(ttl::SUCCESS)`) so the "24 hours"
/// is defined exactly once and is independently unit-testable without a
/// live database connection.
///
/// Callers that genuinely want permanent, never-expiring results can still
/// opt out with `.with_ttl_config(TaskTtlConfig::new())` after construction.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(crate) fn default_ttl_config() -> TaskTtlConfig {
    TaskTtlConfig::with_default(ttl::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    use std::time::Duration;

    #[test]
    fn decode_result_state_maps_every_known_state() {
        let task_id = Uuid::new_v4();
        assert!(matches!(
            decode_result_state(task_id, "pending", None, None, None).unwrap(),
            TaskResult::Pending
        ));
        assert!(matches!(
            decode_result_state(task_id, "started", None, None, None).unwrap(),
            TaskResult::Started
        ));
        assert!(matches!(
            decode_result_state(task_id, "success", Some(json!({"a":1})), None, None).unwrap(),
            TaskResult::Success(_)
        ));
        assert!(matches!(
            decode_result_state(task_id, "revoked", None, None, None).unwrap(),
            TaskResult::Revoked
        ));
        match decode_result_state(task_id, "retry", None, None, Some(3)).unwrap() {
            TaskResult::Retry(n) => assert_eq!(n, 3),
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn decode_result_state_errors_on_unknown_state_instead_of_silently_pending() {
        let task_id = Uuid::new_v4();
        let err = decode_result_state(task_id, "some_future_state", None, None, None)
            .expect_err("unknown state must error, not silently map to Pending");
        assert!(matches!(err, BackendError::Serialization(_)));
        let msg = err.to_string();
        assert!(msg.contains("some_future_state"));
        assert!(msg.contains(&task_id.to_string()));
    }

    #[test]
    fn decode_result_state_failure_with_null_error_message_gets_explicit_placeholder() {
        let task_id = Uuid::new_v4();
        match decode_result_state(task_id, "failure", None, None, None).unwrap() {
            TaskResult::Failure(msg) => {
                assert!(
                    !msg.is_empty(),
                    "a NULL error_message must not silently become an empty-string failure reason"
                );
                assert!(msg.to_lowercase().contains("unknown"));
            }
            other => panic!("expected Failure, got {other:?}"),
        }
    }

    #[test]
    fn decode_result_state_failure_with_present_message_is_preserved() {
        let task_id = Uuid::new_v4();
        match decode_result_state(task_id, "failure", None, Some("boom".to_string()), None).unwrap()
        {
            TaskResult::Failure(msg) => assert_eq!(msg, "boom"),
            other => panic!("expected Failure, got {other:?}"),
        }
    }

    // ── default_ttl_config: the constructors' 24h default ───────────────
    //
    // Pure, DB-free unit test: every `*ResultBackend::new`/`with_pool_size`
    // constructor installs this exact config (see their doc comments), but
    // exercising that live would require a real database connection. This
    // is what actually gets asserted; the constructors are trusted to call
    // the same shared function, which every one of them does.

    #[cfg(any(feature = "postgres", feature = "mysql"))]
    #[test]
    fn default_ttl_config_matches_redis_backends_24_hour_default() {
        let config = default_ttl_config();
        assert_eq!(
            config.default_ttl(),
            Some(Duration::from_secs(86400)),
            "the DB backends' default TTL must match RedisResultBackend::new's 24-hour \
             default (ttl::SUCCESS): without it, store_result never populates expires_at, \
             and cleanup_expired_results() — which only deletes rows WHERE expires_at IS NOT \
             NULL — silently collects nothing"
        );
    }
}
