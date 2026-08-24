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
    use crate::MysqlBroker;
    use celers_core::{Broker, SerializedTask};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    /// Build a broker on an isolated logical queue, or `None` when no test
    /// server is configured.
    async fn broker_on_fresh_queue() -> Option<(MysqlBroker, String)> {
        let url = std::env::var(TEST_URL_ENV).ok()?;
        let queue = format!("test_{}", Uuid::new_v4().simple());
        let broker = MysqlBroker::with_queue(&url, &queue)
            .await
            .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
        broker.migrate().await.expect("migrations should apply");
        Some((broker, queue))
    }

    async fn broker_on_queue(queue: &str) -> Option<MysqlBroker> {
        let url = std::env::var(TEST_URL_ENV).ok()?;
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

        let message = broker
            .dequeue()
            .await
            .expect("dequeue should succeed")
            .expect("the task just enqueued must be claimable");

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

        let claimed = alpha
            .dequeue()
            .await
            .expect("dequeue should succeed")
            .expect("the owning broker must claim its own task");
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
        assert!(reconnected
            .dequeue()
            .await
            .expect("dequeue should succeed")
            .is_some());
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

        let message = broker
            .dequeue()
            .await
            .expect("dequeue should succeed")
            .expect("task must be claimable");

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
        let message = broker
            .dequeue()
            .await
            .expect("dequeue should succeed")
            .expect("task must be claimable");
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

        assert!(
            broker.dequeue().await.is_err(),
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

    /// Two concurrent schedulers must enqueue a due recurring task once, not
    /// twice.
    #[tokio::test]
    async fn recurring_tasks_are_claimed_exactly_once() {
        use crate::{RecurringSchedule, RecurringTaskConfig};

        let Some((broker, queue)) = broker_on_fresh_queue().await else {
            return;
        };
        let Some(rival) = broker_on_queue(&queue).await else {
            return;
        };

        let config = RecurringTaskConfig {
            task_name: format!("recurring_{}", Uuid::new_v4().simple()),
            schedule: RecurringSchedule::EverySeconds(3600),
            payload: b"x".to_vec(),
            priority: 0,
            enabled: true,
            last_run: None,
            next_run: chrono::Utc::now() - chrono::Duration::seconds(60),
        };
        broker
            .register_recurring_task(config)
            .await
            .expect("registering a recurring task should succeed");

        let (first, second) = tokio::join!(
            broker.process_recurring_tasks(),
            rival.process_recurring_tasks()
        );
        let first = first.expect("process_recurring_tasks should succeed");
        let second = second.expect("process_recurring_tasks should succeed");

        assert_eq!(
            first + second,
            1,
            "a due recurring task must be claimed by exactly one scheduler, \
             got {first} + {second}"
        );
    }
}
