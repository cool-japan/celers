//! Regression tests for the hardened claim / ack / reject spine.
//!
//! Split into two halves:
//!
//! * **Deterministic tests** that need no server. These are the ones that
//!   actually protect the fixes in CI: SQL text, migration text, and a
//!   source-level guard that every task INSERT binds `queue_name`.
//! * **Integration tests** gated on the `CELERS_TEST_MYSQL_URL` environment
//!   variable. They early-return (rather than being `#[ignore]`d) so
//!   `cargo nextest run --all-features` is green with no server available and
//!   automatically exercises the real database when one is configured.

use crate::broker_core::strip_sql_line_comments;

/// Environment variable naming a MySQL 8.0.1+ / MariaDB 10.6+ instance to run
/// the integration half against, e.g.
/// `mysql://root:password@127.0.0.1:3306/celers_test`.
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
        "010_task_results.sql",
        include_str!("../migrations/010_task_results.sql"),
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
/// them must go through `sql_text::dequeue_select_sql`.
#[test]
fn no_source_file_hand_writes_a_locking_select() {
    for (name, raw) in TASK_INSERT_SOURCES {
        let source = strip_rust_line_comments(raw);
        assert!(
            !source.contains("FOR UPDATE SKIP LOCKED"),
            "{name} embeds a hand-written locking SELECT; \
             use crate::sql_text::dequeue_select_sql instead"
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

mod integration {
    use super::TEST_URL_ENV;
    use crate::row_ext::RowExt;
    use crate::MysqlBroker;
    use celers_core::{Broker, BrokerMessage, SerializedTask};
    use oxisql_core::Connection;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    /// The connection string to test against, printing a visible, greppable
    /// skip line when it is not configured.
    ///
    /// A bare early-return with no message here is exactly the defect this
    /// suite exists to avoid: a skipped run and a real run both report `ok`,
    /// so without this line the test count cannot tell them apart (see this
    /// module's own doc comment). `#[track_caller]` would name the exact test
    /// call site rather than this function's own call to it, but is a no-op
    /// on an `async fn` caller on stable Rust (rust-lang/rust#110011) — both
    /// of this helper's callers below are async, so the location this prints
    /// is this function's own, not the individual test's; every skip is
    /// still visible, just not disambiguated between two skip sites in the
    /// same test (e.g. `queues_are_isolated_from_each_other`, which opens two
    /// brokers).
    fn test_mysql_url() -> Option<String> {
        match std::env::var(TEST_URL_ENV) {
            Ok(url) if !url.trim().is_empty() => Some(url),
            _ => {
                let location = std::panic::Location::caller();
                eprintln!("SKIPPED: {location} (set {TEST_URL_ENV} to run)");
                None
            }
        }
    }

    /// Build a broker on an isolated logical queue, or `None` when no test
    /// server is configured.
    async fn broker_on_fresh_queue() -> Option<(MysqlBroker, String)> {
        let url = test_mysql_url()?;
        let queue = format!("test_{}", Uuid::new_v4().simple());
        let broker = MysqlBroker::with_queue(&url, &queue)
            .await
            .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
        broker.migrate().await.expect("migrations should apply");
        Some((broker, queue))
    }

    /// How many times [`claim_from`] re-issues a claim that came back empty,
    /// and how long it waits in between.
    const CLAIM_ATTEMPTS: usize = 50;
    const CLAIM_RETRY_DELAY: Duration = Duration::from_millis(20);

    /// Claim one task, retrying briefly while the claim comes back empty.
    ///
    /// A claim takes a next-key lock on the index record *after* the range it
    /// scanned, and under parallel test execution that neighbour usually
    /// belongs to another test's queue. Measured on this suite's MySQL 8.0
    /// via `performance_schema.data_locks`: claiming from queue `zzprobe_a`
    /// held `X` on the `idx_tasks_queue_dequeue` record of `zzprobe_b` *and*
    /// `X,REC_NOT_GAP` on that row's primary key, so while such a claim is
    /// open, `SKIP LOCKED` skips the other queue's own pending row and
    /// `dequeue()` returns `Ok(None)`.
    ///
    /// That is a genuine limitation of the claim statement (reported
    /// separately; the remedy is a READ COMMITTED claim transaction, or a
    /// two-step claim that locks by primary key), not something a test can
    /// fix. Tests whose subject is not claim concurrency claim through this
    /// helper so a neighbouring queue cannot decide their outcome.
    async fn claim_from(broker: &MysqlBroker) -> BrokerMessage {
        for _ in 0..CLAIM_ATTEMPTS {
            let claimed = broker.dequeue().await.expect("dequeue should succeed");
            if let Some(message) = claimed {
                return message;
            }
            tokio::time::sleep(CLAIM_RETRY_DELAY).await;
        }
        panic!(
            "no task became claimable on queue {} within {CLAIM_ATTEMPTS} attempts",
            broker.queue_name()
        );
    }

    async fn broker_on_queue(queue: &str) -> Option<MysqlBroker> {
        let url = test_mysql_url()?;
        Some(
            MysqlBroker::with_queue(&url, queue)
                .await
                .expect("connecting to CELERS_TEST_MYSQL_URL should succeed"),
        )
    }

    /// The headline regression: the dequeued task must carry the row's id, and
    /// the subsequent ack must actually transition that row.
    #[tokio::test]
    async fn enqueue_dequeue_ack_round_trip_preserves_the_task_id() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("round_trip".to_string(), b"payload".to_vec())
            .with_priority(5)
            .with_max_retries(9)
            .with_timeout(45);
        let enqueued_id = broker.enqueue(task).await.expect("enqueue should succeed");

        let message = claim_from(&broker).await;

        assert_eq!(
            message.task.metadata.id, enqueued_id,
            "dequeue must return the persisted task id, not a fresh uuid"
        );
        assert_eq!(message.task.payload, b"payload".to_vec());
        assert_eq!(message.task.metadata.priority, 5);
        assert_eq!(message.task.metadata.max_retries, 9);
        assert_eq!(
            message.task.metadata.timeout_secs,
            Some(45),
            "the execution timeout must survive the round trip"
        );

        broker
            .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
            .await
            .expect("ack should succeed");

        let info = broker
            .get_task(&enqueued_id)
            .await
            .expect("get_task should succeed")
            .expect("the acked task row must still exist");
        assert_eq!(info.state.to_string(), "completed");
    }

    /// Two brokers on the same database but different logical queues must not
    /// see each other's tasks.
    #[tokio::test]
    async fn queues_are_isolated_from_each_other() {
        let Some((alpha, _alpha_queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let Some((beta, _beta_queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("isolated".to_string(), b"x".to_vec());
        let task_id = alpha.enqueue(task).await.expect("enqueue should succeed");

        assert_eq!(beta.queue_size().await.expect("queue_size"), 0);
        assert_eq!(alpha.queue_size().await.expect("queue_size"), 1);
        assert!(
            beta.dequeue()
                .await
                .expect("dequeue should succeed")
                .is_none(),
            "a broker must never claim another logical queue's task"
        );

        let claimed = claim_from(&alpha).await;
        assert_eq!(claimed.task.metadata.id, task_id);
    }

    /// A broker reconnected to the same queue name sees the same backlog —
    /// the queue label is persisted, not per-instance state.
    #[tokio::test]
    async fn queue_membership_survives_reconnection() {
        let Some((broker, queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let task = SerializedTask::new("persisted_queue".to_string(), b"x".to_vec());
        broker.enqueue(task).await.expect("enqueue should succeed");

        let Some(reconnected) = broker_on_queue(&queue).await else {
            return;
        };
        assert_eq!(reconnected.queue_size().await.expect("queue_size"), 1);
        let reclaimed = claim_from(&reconnected).await;
        assert_eq!(reclaimed.task.metadata.name, "persisted_queue");
    }

    /// `max_retries` large enough to overflow the old `2_i64.pow` backoff.
    #[tokio::test]
    async fn reject_with_a_huge_retry_budget_does_not_panic() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("huge_retry_budget".to_string(), b"x".to_vec())
            .with_max_retries(1000);
        broker.enqueue(task).await.expect("enqueue should succeed");

        let message = claim_from(&broker).await;

        broker
            .reject(
                &message.task.metadata.id,
                message.receipt_handle.as_deref(),
                true,
            )
            .await
            .expect("reject must not panic or fail for a large retry budget");

        let info = broker
            .get_task(&message.task.metadata.id)
            .await
            .expect("get_task")
            .expect("rejected task must be requeued, not dropped");
        assert_eq!(info.state.to_string(), "pending");
    }

    /// Ack and reject hooks used to be accepted and never run.
    #[tokio::test]
    async fn ack_hooks_actually_fire() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let dequeued = Arc::new(AtomicUsize::new(0));

        let before_counter = Arc::clone(&before);
        broker
            .add_hook(crate::TaskHook::BeforeAck(Arc::new(move |_ctx, _task| {
                let counter = Arc::clone(&before_counter);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })))
            .await;

        let after_counter = Arc::clone(&after);
        broker
            .add_hook(crate::TaskHook::AfterAck(Arc::new(move |_ctx, _task| {
                let counter = Arc::clone(&after_counter);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })))
            .await;

        let dequeue_counter = Arc::clone(&dequeued);
        broker
            .add_hook(crate::TaskHook::AfterDequeue(Arc::new(
                move |_ctx, _task| {
                    let counter = Arc::clone(&dequeue_counter);
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                },
            )))
            .await;

        let task = SerializedTask::new("hooked".to_string(), b"x".to_vec());
        broker.enqueue(task).await.expect("enqueue should succeed");
        let message = claim_from(&broker).await;
        broker
            .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
            .await
            .expect("ack should succeed");

        assert_eq!(dequeued.load(Ordering::SeqCst), 1, "AfterDequeue must fire");
        assert_eq!(before.load(Ordering::SeqCst), 1, "BeforeAck must fire");
        assert_eq!(after.load(Ordering::SeqCst), 1, "AfterAck must fire");
    }

    /// A `BeforeDequeue` hook that errors must leave the task claimable.
    #[tokio::test]
    async fn before_dequeue_hook_error_rolls_the_claim_back() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("vetoed".to_string(), b"x".to_vec());
        let task_id = broker.enqueue(task).await.expect("enqueue should succeed");

        broker
            .add_hook(crate::TaskHook::BeforeDequeue(Arc::new(|_ctx, _task| {
                Box::pin(async move { Err(celers_core::CelersError::Other("vetoed".to_string())) })
            })))
            .await;

        // Retried like every other claim in this module: a claim starved by
        // a neighbouring queue's lock returns `Ok(None)` without ever
        // reaching the hook, which would read here as "the hook did not
        // veto". See `claim_from` for the measured mechanism.
        let mut vetoed = false;
        for _ in 0..CLAIM_ATTEMPTS {
            if broker.dequeue().await.is_err() {
                vetoed = true;
                break;
            }
            tokio::time::sleep(CLAIM_RETRY_DELAY).await;
        }
        assert!(
            vetoed,
            "a vetoing BeforeDequeue hook must surface its error"
        );

        broker.clear_hooks().await;
        let info = broker
            .get_task(&task_id)
            .await
            .expect("get_task")
            .expect("the vetoed task must still exist");
        assert_eq!(
            info.state.to_string(),
            "pending",
            "a vetoed claim must be rolled back, leaving the task pending"
        );
        assert_eq!(info.retry_count, 0, "a vetoed claim must not burn a retry");
    }

    /// `with_transaction` used to drop the transaction without committing, so
    /// every write inside was silently rolled back.
    #[tokio::test]
    async fn with_transaction_commits_its_writes() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("committed".to_string(), b"x".to_vec());
        let task_id = broker.enqueue(task).await.expect("enqueue should succeed");
        let id_text = task_id.to_string();

        broker
            .with_transaction(move |tx| {
                let id_text = id_text.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE celers_tasks SET priority = 77 WHERE id = ?",
                        &[&id_text],
                    )
                    .await
                    .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
                    Ok(())
                })
            })
            .await
            .expect("with_transaction should succeed");

        let info = broker
            .get_task(&task_id)
            .await
            .expect("get_task")
            .expect("task must exist");
        assert_eq!(
            info.priority, 77,
            "with_transaction must commit the callback's writes"
        );
    }

    /// A failing callback must roll back rather than partially commit.
    #[tokio::test]
    async fn with_transaction_rolls_back_on_error() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("rolled_back".to_string(), b"x".to_vec());
        let task_id = broker.enqueue(task).await.expect("enqueue should succeed");
        let id_text = task_id.to_string();

        let outcome: celers_core::Result<()> = broker
            .with_transaction(move |tx| {
                let id_text = id_text.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE celers_tasks SET priority = 55 WHERE id = ?",
                        &[&id_text],
                    )
                    .await
                    .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
                    Err(celers_core::CelersError::Other("deliberate".to_string()))
                })
            })
            .await;

        assert!(outcome.is_err());
        let info = broker
            .get_task(&task_id)
            .await
            .expect("get_task")
            .expect("task must exist");
        assert_ne!(info.priority, 55, "a failed callback must roll back");
    }

    /// Count the tasks named `task_name` that are sitting in `celers_tasks`.
    ///
    /// `process_recurring_tasks` enqueues through `self.enqueue`, so the rows
    /// it writes land in the calling broker's own queue — but the *name* is
    /// what identifies them as this test's, and a uuid-suffixed name cannot
    /// collide with anything another run left behind.
    async fn tasks_named(broker: &MysqlBroker, task_name: &str) -> i64 {
        let rows = broker
            .connection()
            .query(
                "SELECT COUNT(*) AS c FROM celers_tasks WHERE task_name = ?",
                &[&task_name],
            )
            .await
            .expect("counting enqueued rows should succeed");
        rows.first()
            .map(|row| row.col::<i64>("c"))
            .transpose()
            .expect("the count column must decode")
            .unwrap_or(0)
    }

    /// Two concurrent schedulers must enqueue a due recurring task once, not
    /// twice.
    ///
    /// # Why this counts rows instead of return values
    ///
    /// `process_recurring_tasks` scans **every** `__recurring__%` row in the
    /// database (`celers_task_results` is not queue-scoped) and returns how
    /// many of them *it* claimed, so its return value counts every other
    /// run's leftover configuration as well as this test's own. Registrations
    /// are durable by design and nothing sweeps them, so on a database that
    /// has served this suite more than once the old `first + second == 1`
    /// assertion fails on arithmetic that has nothing to do with the property
    /// under test: a live MySQL carrying 39 leftover due configurations
    /// returned `20 + 19`, which is 39 configurations claimed exactly once
    /// each — the correct behaviour — reported as a failure.
    ///
    /// The property is "*this* configuration is enqueued exactly once", so
    /// this counts the rows carrying this test's uuid-suffixed task name.
    /// That is strictly stronger than the sum: a double claim would show up
    /// as 2 no matter how many neighbours are due, and no number of
    /// neighbours can make it anything but 1 when the claim works.
    #[tokio::test]
    async fn recurring_tasks_are_claimed_exactly_once() {
        use crate::{RecurringSchedule, RecurringTaskConfig};

        let Some((broker, queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let Some(rival) = broker_on_queue(&queue).await else {
            return;
        };

        let task_name = format!("recurring_{}", Uuid::new_v4().simple());
        let config = RecurringTaskConfig {
            task_name: task_name.clone(),
            schedule: RecurringSchedule::EverySeconds(3600),
            payload: b"x".to_vec(),
            priority: 0,
            enabled: true,
            last_run: None,
            next_run: chrono::Utc::now() - chrono::Duration::seconds(60),
        };
        let config_id = broker
            .register_recurring_task(config)
            .await
            .expect("registering a recurring task should succeed");

        let (first, second) = tokio::join!(
            broker.process_recurring_tasks(),
            rival.process_recurring_tasks()
        );
        let first = first.expect("process_recurring_tasks should succeed");
        let second = second.expect("process_recurring_tasks should succeed");

        let enqueued = tasks_named(&broker, &task_name).await;

        // Clean up before asserting, so a failure does not also leak this
        // test's configuration into the next run the way the rows that
        // exposed this defect were leaked.
        broker
            .delete_recurring_task(&config_id)
            .await
            .expect("deleting this test's recurring configuration should succeed");
        broker
            .connection()
            .execute(
                "DELETE FROM celers_tasks WHERE task_name = ?",
                &[&task_name],
            )
            .await
            .expect("deleting this test's enqueued rows should succeed");

        assert_eq!(
            enqueued, 1,
            "a due recurring task must be enqueued by exactly one scheduler, \
             got {enqueued} rows named {task_name} (the two schedulers claimed \
             {first} and {second} configurations in total, this test's included)"
        );
    }

    /// `purge_terminal_tasks` must delete only terminal (acked) rows, never a
    /// still-pending one — and the nested-derived-table `DELETE` built by
    /// `sql_text::purge_terminal_tasks_sql` must actually parse and execute
    /// on a live server (its two MySQL-specific workarounds, for `ERROR
    /// 1093` and `ERROR 1235`, are untestable without one).
    #[tokio::test]
    async fn retention_purges_only_terminal_tasks() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let completed_id = broker
            .enqueue(SerializedTask::new("done".to_string(), b"x".to_vec()))
            .await
            .expect("enqueue should succeed");
        let msg = claim_from(&broker).await;
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .expect("ack should succeed");

        let pending_id = broker
            .enqueue(SerializedTask::new(
                "still_waiting".to_string(),
                b"x".to_vec(),
            ))
            .await
            .expect("enqueue should succeed");

        // Zero retention: everything terminal is eligible immediately, so
        // the assertion needs no sleeping.
        let deleted = broker
            .purge_terminal_tasks(std::time::Duration::from_secs(0), 100, 5)
            .await
            .expect("purge_terminal_tasks should succeed");
        assert_eq!(deleted, 1, "exactly the one acked task must be purged");

        assert!(
            broker
                .get_task(&completed_id)
                .await
                .expect("get_task should succeed")
                .is_none(),
            "the completed task must be gone"
        );
        assert!(
            broker
                .get_task(&pending_id)
                .await
                .expect("get_task should succeed")
                .is_some(),
            "the still-pending task must survive"
        );
    }

    /// A broker's purge must never delete another logical queue's terminal
    /// tasks, matching every other claim/read path's queue isolation.
    #[tokio::test]
    async fn retention_purge_is_scoped_to_the_owning_queue() {
        let Some((alpha, _alpha_queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let Some((beta, _beta_queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("alpha_done".to_string(), b"x".to_vec());
        alpha.enqueue(task).await.expect("enqueue should succeed");
        let msg = claim_from(&alpha).await;
        alpha
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .expect("ack should succeed");

        let deleted = beta
            .purge_terminal_tasks(std::time::Duration::from_secs(0), 100, 5)
            .await
            .expect("purge_terminal_tasks should succeed");
        assert_eq!(
            deleted, 0,
            "beta must not purge alpha's terminal tasks from a different queue"
        );

        assert_eq!(
            alpha
                .get_task(&msg.task.metadata.id)
                .await
                .expect("get_task should succeed")
                .expect("alpha's completed task must be untouched")
                .state
                .to_string(),
            "completed"
        );
    }

    /// Two brokers on different logical queues must not dedupe against each
    /// other: an identical `dedup_key` must not make the second broker's
    /// `enqueue_deduplicated` hand back the first broker's task id — that id
    /// is invisible to the second broker's queue-scoped `dequeue`, so doing
    /// so would silently lose the second task.
    #[tokio::test]
    async fn dedup_does_not_cross_queue_boundaries() {
        let Some((alpha, _alpha_queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let Some((beta, _beta_queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let dedup_key = format!("shared_key_{}", Uuid::new_v4().simple());
        let alpha_task = SerializedTask::new("alpha_job".to_string(), b"a".to_vec());
        let beta_task = SerializedTask::new("beta_job".to_string(), b"b".to_vec());

        let alpha_id = alpha
            .enqueue_deduplicated(alpha_task, &dedup_key)
            .await
            .expect("alpha's enqueue_deduplicated should succeed");
        let beta_id = beta
            .enqueue_deduplicated(beta_task, &dedup_key)
            .await
            .expect("beta's enqueue_deduplicated should succeed");

        assert_ne!(
            alpha_id, beta_id,
            "the same dedup_key on two different queues must not collide"
        );
        assert_eq!(
            alpha.queue_size().await.expect("queue_size"),
            1,
            "alpha's own task must have been inserted, not skipped"
        );
        assert_eq!(
            beta.queue_size().await.expect("queue_size"),
            1,
            "beta's own task must have been inserted, not skipped"
        );
    }

    /// The same `dedup_key` submitted twice on the *same* queue must still
    /// dedupe — the queue predicate narrows the match, it must not disable
    /// deduplication altogether.
    #[tokio::test]
    async fn dedup_still_applies_within_the_same_queue() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let dedup_key = format!("same_queue_key_{}", Uuid::new_v4().simple());
        let first_id = broker
            .enqueue_deduplicated(
                SerializedTask::new("first".to_string(), b"x".to_vec()),
                &dedup_key,
            )
            .await
            .expect("first enqueue_deduplicated should succeed");
        let second_id = broker
            .enqueue_deduplicated(
                SerializedTask::new("second".to_string(), b"y".to_vec()),
                &dedup_key,
            )
            .await
            .expect("second enqueue_deduplicated should succeed");

        assert_eq!(
            first_id, second_id,
            "a repeated dedup_key on the same queue must return the existing task id"
        );
        assert_eq!(
            broker.queue_size().await.expect("queue_size"),
            1,
            "the second call must not have inserted a duplicate row"
        );
    }

    // ========== Revocation (idx 1: durable revoked-task set, polled) ==========

    #[tokio::test]
    async fn revoke_removes_a_pending_task_and_is_revoked_reflects_it() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("revoke_pending".to_string(), vec![]);
        let task_id = broker.enqueue(task).await.expect("enqueue");

        assert!(
            !broker.is_revoked(&task_id).await.expect("is_revoked"),
            "a freshly enqueued task must not already be revoked"
        );

        let recorded = broker.revoke(&task_id, false).await.expect("revoke");
        assert!(recorded, "revoke() must report the revocation as recorded");

        assert!(
            broker.is_revoked(&task_id).await.expect("is_revoked"),
            "revoke() must be durably visible through is_revoked()"
        );

        assert!(
            broker.dequeue().await.expect("dequeue").is_none(),
            "a revoked pending task must not be claimable"
        );
    }

    #[tokio::test]
    async fn revoke_is_observed_by_the_poller_within_one_interval() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };
        // A one-second poll interval keeps the test's wait bounded without
        // being so short it turns into a busy loop against the server.
        let broker = broker.with_revocation_poll_interval(1);

        let mut stream = broker
            .subscribe_revocations()
            .await
            .expect("subscribe_revocations")
            .expect("MysqlBroker must publish a revocation stream");

        let task = SerializedTask::new("revoke_notice".to_string(), vec![]);
        let task_id = broker.enqueue(task).await.expect("enqueue");

        broker
            .revoke(&task_id, true)
            .await
            .expect("revoke with terminate=true");

        let notice = tokio::time::timeout(std::time::Duration::from_secs(10), stream.recv())
            .await
            .expect("a notice must arrive within a few poll intervals")
            .expect("recv must not error")
            .expect("recv must not report the stream as ended");

        assert_eq!(notice.task_id, task_id);
        assert!(
            notice.terminate,
            "terminate=true must survive the round trip through TINYINT(1)"
        );
    }

    /// Regression test for a real bug caught in review: an earlier draft of
    /// `POLL_REVOCATIONS` used a `revoked_at >= ?` cursor, which — once the
    /// watermark reached a row's own timestamp — matched that same row on
    /// every subsequent poll forever (the watermark can only advance past a
    /// *different*, newer row), redelivering the same notice indefinitely
    /// whenever nothing newer ever arrives. This is the common case, not a
    /// rare one: it fires for every revocation that happens to be the most
    /// recent one so far, which is most revocations most of the time.
    #[tokio::test]
    async fn a_single_revocation_is_delivered_exactly_once_not_forever() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let broker = broker.with_revocation_poll_interval(1);

        let mut stream = broker
            .subscribe_revocations()
            .await
            .expect("subscribe_revocations")
            .expect("MysqlBroker must publish a revocation stream");

        let task = SerializedTask::new("revoke_once".to_string(), vec![]);
        let task_id = broker.enqueue(task).await.expect("enqueue");
        broker.revoke(&task_id, false).await.expect("revoke");

        let first = tokio::time::timeout(std::time::Duration::from_secs(10), stream.recv())
            .await
            .expect("a notice must arrive within a few poll intervals")
            .expect("recv must not error")
            .expect("recv must not report the stream as ended");
        assert_eq!(first.task_id, task_id);

        // No second revocation is ever issued. Across several more poll
        // intervals, `recv()` must not produce another notice for the same
        // (never re-revoked) task — that would be the infinite-redelivery
        // bug the `>` cursor exists to prevent.
        let second = tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv()).await;
        assert!(
            second.is_err(),
            "a single revocation must be delivered exactly once, not redelivered every poll \
             (got a second notice: {second:?})"
        );
    }

    // ---------- The dead-letter move ----------

    /// Count the dead-letter rows carrying `task_id`.
    ///
    /// `celers_dead_letter_queue` has no `queue_name` column, so the DLQ is
    /// database-wide: `get_statistics().dlq` and `list_dlq()` see every
    /// queue's rows and every previous run's leftovers. A test can only
    /// address its own row, and only by the task id it minted.
    async fn dlq_rows_for(broker: &MysqlBroker, task_id: &Uuid) -> i64 {
        let rows = broker
            .connection()
            .query(
                "SELECT COUNT(*) AS c FROM celers_dead_letter_queue WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .expect("counting dead-letter rows should succeed");
        rows.first()
            .map(|row| row.col::<i64>("c"))
            .transpose()
            .expect("the count column must decode")
            .unwrap_or(0)
    }

    /// Remove this test's dead-letter row, which queue-scoped cleanup cannot
    /// reach.
    async fn purge_dlq_rows_for(broker: &MysqlBroker, task_id: &Uuid) {
        broker
            .connection()
            .execute(
                "DELETE FROM celers_dead_letter_queue WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .expect("dead-letter cleanup should succeed");
    }

    /// The headline check for the stored-procedure replacement: a task that
    /// exhausts its retry budget must actually land in the dead-letter queue,
    /// and leave `celers_tasks`.
    ///
    /// `CALL move_to_dlq(?)` could never work — MySQL will not create the
    /// procedure over the prepared-statement protocol — so this path was
    /// rewritten as [`crate::dlq_move::DLQ_INSERT_SQL`] +
    /// [`crate::dlq_move::DLQ_DELETE_SQL`] in one transaction. Nothing had
    /// ever run that rewrite against a real server.
    #[tokio::test]
    async fn retry_exhaustion_moves_the_task_into_the_dead_letter_queue() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        // `max_retries = 1` and a claim that increments `retry_count` to 1
        // make the very next reject the exhausting one: `reject` moves the
        // task to the DLQ once `retry_count >= max_retries`.
        let task = SerializedTask::new("exhausted".to_string(), b"dlq-payload".to_vec())
            .with_max_retries(1);
        let task_id = broker.enqueue(task).await.expect("enqueue should succeed");

        let message = claim_from(&broker).await;
        // The claim increments the persisted `retry_count` (the dequeued
        // message still carries the pre-increment value it selected), which
        // is what `reject` compares against `max_retries`.
        let claimed = broker
            .get_task(&task_id)
            .await
            .expect("get_task should succeed")
            .expect("the claimed task must still exist");
        assert_eq!(
            claimed.retry_count, 1,
            "the claim burns the single retry this task was given"
        );

        assert_eq!(
            dlq_rows_for(&broker, &task_id).await,
            0,
            "nothing may be dead-lettered before the budget is spent"
        );

        broker
            .reject(
                &message.task.metadata.id,
                message.receipt_handle.as_deref(),
                true,
            )
            .await
            .expect("reject should succeed");

        assert_eq!(
            dlq_rows_for(&broker, &task_id).await,
            1,
            "an exhausted task must be copied into the dead-letter queue"
        );
        assert!(
            broker
                .get_task(&task_id)
                .await
                .expect("get_task should succeed")
                .is_none(),
            "the source row must be deleted, not left behind as a duplicate"
        );
        assert_eq!(
            broker.queue_size().await.expect("queue_size"),
            0,
            "a dead-lettered task must not still count against the queue"
        );

        purge_dlq_rows_for(&broker, &task_id).await;
    }

    /// The dead-lettered copy must carry the task's payload and retry count,
    /// not an empty shell — `DLQ_INSERT_SQL` selects those columns from the
    /// row it is about to delete, so a wrong column order would silently
    /// scramble them.
    #[tokio::test]
    async fn the_dead_letter_copy_keeps_the_payload_and_retry_count() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let task = SerializedTask::new("exhausted_body".to_string(), b"body-bytes".to_vec())
            .with_max_retries(1);
        let task_id = broker.enqueue(task).await.expect("enqueue should succeed");

        let message = claim_from(&broker).await;
        broker
            .reject(
                &message.task.metadata.id,
                message.receipt_handle.as_deref(),
                true,
            )
            .await
            .expect("reject should succeed");

        let rows = broker
            .connection()
            .query(
                "SELECT task_name, payload, retry_count FROM celers_dead_letter_queue \
                 WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .expect("reading the dead-letter row should succeed");
        let row = rows.first().expect("the dead-letter row must exist");

        let task_name: String = row.col("task_name").expect("task_name must decode");
        let payload: Vec<u8> = row.col("payload").expect("payload must decode");
        let retry_count: i32 = row.col("retry_count").expect("retry_count must decode");

        assert_eq!(task_name, "exhausted_body");
        assert_eq!(payload, b"body-bytes".to_vec());
        assert_eq!(retry_count, 1);

        purge_dlq_rows_for(&broker, &task_id).await;
    }

    /// `reject_batch` carries its own copy of the two dead-letter statements
    /// (it runs them inside the batch's transaction rather than opening a
    /// second one), so it needs its own live check.
    #[tokio::test]
    async fn reject_batch_moves_an_exhausted_task_into_the_dead_letter_queue() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let exhausted =
            SerializedTask::new("batch_exhausted".to_string(), b"x".to_vec()).with_max_retries(1);
        let exhausted_id = broker
            .enqueue(exhausted)
            .await
            .expect("enqueue should succeed");
        let survivor =
            SerializedTask::new("batch_survivor".to_string(), b"y".to_vec()).with_max_retries(9);
        let survivor_id = broker
            .enqueue(survivor)
            .await
            .expect("enqueue should succeed");

        let first = claim_from(&broker).await;
        let second = claim_from(&broker).await;

        let rejected = broker
            .reject_batch(&[
                (first.task.metadata.id, first.receipt_handle.clone(), true),
                (second.task.metadata.id, second.receipt_handle.clone(), true),
            ])
            .await
            .expect("reject_batch should succeed");
        assert_eq!(rejected, 2, "both tasks must be accounted for");

        assert_eq!(
            dlq_rows_for(&broker, &exhausted_id).await,
            1,
            "the task past its retry budget must be dead-lettered"
        );
        assert!(
            broker
                .get_task(&exhausted_id)
                .await
                .expect("get_task should succeed")
                .is_none(),
            "the dead-lettered task's source row must be gone"
        );

        assert_eq!(
            dlq_rows_for(&broker, &survivor_id).await,
            0,
            "a task with retries left must not be dead-lettered"
        );
        let survivor_info = broker
            .get_task(&survivor_id)
            .await
            .expect("get_task should succeed")
            .expect("the requeued task must still exist");
        assert_eq!(survivor_info.state.to_string(), "pending");

        purge_dlq_rows_for(&broker, &exhausted_id).await;
    }

    // ---------- Server-error classification ----------

    /// Pins the formatting `crate::mysql_error`'s predicates parse.
    ///
    /// Those predicates recover a MySQL error *number* from the formatted
    /// message, because `oxisql-mysql` discards the numeric code when it maps
    /// a server error to `OxiSqlError::Execution(String)`. That only works as
    /// long as the driver keeps rendering server errors as
    /// `ERROR <code> (<sqlstate>): <message>`. The hermetic tests in
    /// `mysql_error.rs` assert the parsing; this one asserts the *format*, by
    /// provoking a real server error (`1146`, unknown table) through the real
    /// driver and matching it by number.
    #[tokio::test]
    async fn a_real_server_error_is_recognised_by_its_code() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };

        let error = broker
            .connection()
            .query("SELECT 1 FROM celers_no_such_table_exists", &[])
            .await
            .expect_err("querying a missing table must fail");

        assert!(
            crate::mysql_error::is_mysql_server_error(&error, 1146),
            "a real ER_NO_SUCH_TABLE must be matched by number; \
             the driver's formatting may have changed: {error}"
        );
        assert!(
            !crate::mysql_error::is_deadlock(&error),
            "an unrelated server error must not be mistaken for a deadlock"
        );
    }

    #[tokio::test]
    async fn revocation_lapses_once_its_ttl_elapses() {
        let Some((broker, _queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let broker = broker.with_revocation_ttl(1);

        let task = SerializedTask::new("revoke_ttl".to_string(), vec![]);
        let task_id = broker.enqueue(task).await.expect("enqueue");
        broker.revoke(&task_id, false).await.expect("revoke");
        assert!(broker.is_revoked(&task_id).await.expect("is_revoked"));

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            !broker.is_revoked(&task_id).await.expect("is_revoked"),
            "a revocation older than its TTL must lapse"
        );
    }
}
