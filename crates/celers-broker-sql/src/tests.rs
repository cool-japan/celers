#![cfg(test)]

use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerStateInternal};
use crate::*;
use celers_core::{Broker, BrokerMessage, SerializedTask};
use chrono::Utc;
use oxisql_core::Connection;
use uuid::Uuid;

#[test]
fn test_db_task_state_display() {
    assert_eq!(DbTaskState::Pending.to_string(), "pending");
    assert_eq!(DbTaskState::Processing.to_string(), "processing");
    assert_eq!(DbTaskState::Completed.to_string(), "completed");
    assert_eq!(DbTaskState::Failed.to_string(), "failed");
    assert_eq!(DbTaskState::Cancelled.to_string(), "cancelled");
}

#[test]
fn test_db_task_state_from_str() {
    assert_eq!(
        "pending".parse::<DbTaskState>().unwrap(),
        DbTaskState::Pending
    );
    assert_eq!(
        "processing".parse::<DbTaskState>().unwrap(),
        DbTaskState::Processing
    );
    assert_eq!(
        "completed".parse::<DbTaskState>().unwrap(),
        DbTaskState::Completed
    );
    assert_eq!(
        "failed".parse::<DbTaskState>().unwrap(),
        DbTaskState::Failed
    );
    assert_eq!(
        "cancelled".parse::<DbTaskState>().unwrap(),
        DbTaskState::Cancelled
    );
    // Case insensitive
    assert_eq!(
        "PENDING".parse::<DbTaskState>().unwrap(),
        DbTaskState::Pending
    );
    assert_eq!(
        "Completed".parse::<DbTaskState>().unwrap(),
        DbTaskState::Completed
    );
}

#[test]
fn test_db_task_state_invalid() {
    assert!("invalid".parse::<DbTaskState>().is_err());
    assert!("".parse::<DbTaskState>().is_err());
}

#[test]
fn test_queue_statistics_default() {
    let stats = QueueStatistics::default();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.processing, 0);
    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.cancelled, 0);
    assert_eq!(stats.dlq, 0);
    assert_eq!(stats.total, 0);
}

#[test]
fn test_db_task_state_serialization() {
    let state = DbTaskState::Pending;
    let json = serde_json::to_string(&state).unwrap();
    assert_eq!(json, "\"pending\"");

    let deserialized: DbTaskState = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, state);
}

#[test]
fn test_task_result_status_display() {
    assert_eq!(TaskResultStatus::Pending.to_string(), "PENDING");
    assert_eq!(TaskResultStatus::Started.to_string(), "STARTED");
    assert_eq!(TaskResultStatus::Success.to_string(), "SUCCESS");
    assert_eq!(TaskResultStatus::Failure.to_string(), "FAILURE");
    assert_eq!(TaskResultStatus::Retry.to_string(), "RETRY");
    assert_eq!(TaskResultStatus::Revoked.to_string(), "REVOKED");
}

#[test]
fn test_task_result_status_from_str() {
    assert_eq!(
        "PENDING".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Pending
    );
    assert_eq!(
        "STARTED".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Started
    );
    assert_eq!(
        "SUCCESS".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Success
    );
    assert_eq!(
        "FAILURE".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Failure
    );
    assert_eq!(
        "RETRY".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Retry
    );
    assert_eq!(
        "REVOKED".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Revoked
    );
    // Case insensitive
    assert_eq!(
        "pending".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Pending
    );
    assert_eq!(
        "Success".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Success
    );
}

#[test]
fn test_task_result_status_invalid() {
    assert!("invalid".parse::<TaskResultStatus>().is_err());
    assert!("".parse::<TaskResultStatus>().is_err());
}

#[test]
fn test_task_result_status_serialization() {
    let status = TaskResultStatus::Success;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"success\"");

    let deserialized: TaskResultStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, status);
}

// ========== Legacy integration tests (require CELERS_TEST_MYSQL_URL) ==========
//
// These tests are `#[ignore]`d and selected by `--run-ignored all`, the
// invocation `tests/integration/README.md` documents for this suite.

/// The MySQL connection string for the legacy `#[ignore]`-gated integration
/// tests below, or `None` — with a visible, greppable skip line — when none
/// is configured.
///
/// Prefers `CELERS_TEST_MYSQL_URL`, the name every other integration test in
/// the workspace (this crate's [`test_mysql_url`] helper, `celers-broker-sql`'s
/// `tests_hardening` module, `celers-broker-postgres`'s `CELERS_TEST_POSTGRES_URL`
/// equivalent) already standardized on. The old bare `MYSQL_URL` name is kept
/// as a documented fallback so any environment still exporting it keeps
/// working unchanged. An empty value (set-but-blank, e.g. `FOO=`) is treated
/// the same as unset for both names, matching [`test_mysql_url`]'s guard
/// below.
///
/// There is deliberately no hardcoded default any more. The previous
/// `mysql://root:password@localhost/celers_test` fallback meant a
/// `--run-ignored all` run with no variable exported failed against a server
/// nobody had configured, which reads as a broken suite rather than an
/// unconfigured one.
fn legacy_mysql_url(test_name: &str) -> Option<String> {
    let non_empty = |key: &str| std::env::var(key).ok().filter(|url| !url.trim().is_empty());
    match non_empty("CELERS_TEST_MYSQL_URL").or_else(|| non_empty("MYSQL_URL")) {
        Some(url) => Some(url),
        None => {
            eprintln!("SKIPPED: {test_name} (set CELERS_TEST_MYSQL_URL to run)");
            None
        }
    }
}

/// One legacy integration test's isolated slice of the shared database.
///
/// Every test in this half used to open `MysqlBroker::new(&url)` — the same
/// *default* logical queue, over the same tables, with no cleanup. Counts
/// therefore accumulated across tests within a run and across runs, so an
/// assertion like `assert_eq!(pending, 5)` only ever held for the first test
/// to touch a virgin database. Nine of them failed even under
/// `--test-threads 1`, and four more only under parallelism.
///
/// The fixture gives each test a UUID-suffixed queue of its own — the same
/// isolation shape `tests_hardening`'s `broker_on_fresh_queue` and the Redis
/// broker's tests use — so the exact-count assertions mean exactly what they
/// say again, in parallel as well as serially. Worker ids are namespaced the
/// same way through [`LegacyFixture::worker_id`], because the worker-facing
/// queries key on `worker_id` rather than on the queue.
///
/// [`LegacyFixture::cleanup`] drops the queue's rows at the end of a test.
/// Isolation does *not* depend on it — a queue name is never reused, so
/// residue from a panicking test can never reach another test — it exists so
/// that repeatedly running this suite does not grow the shared table without
/// bound.
struct LegacyFixture {
    broker: MysqlBroker,
    url: String,
    queue: String,
}

/// How many times [`LegacyFixture::claim`] re-issues a claim that came back
/// empty. Generous on purpose: a neighbouring claim transaction lasts
/// milliseconds, so this is roughly a second of headroom against a starvation
/// window that should never come close to it.
const CLAIM_ATTEMPTS: usize = 50;

/// Pause between claim attempts.
const CLAIM_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

impl LegacyFixture {
    /// Connect, migrate, and hand back a fixture on a queue no other test
    /// uses; `None` (with a visible skip line) when no server is configured.
    async fn open(test_name: &str) -> Option<Self> {
        let url = legacy_mysql_url(test_name)?;
        let queue = format!("legacy_{}", Uuid::new_v4().simple());
        let broker = MysqlBroker::with_queue(&url, &queue)
            .await
            .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
        broker.migrate().await.expect("migrations should apply");
        Some(Self { broker, url, queue })
    }

    /// A second broker on the *same* isolated queue — a separate "process"
    /// competing for the same tasks.
    async fn peer(&self) -> MysqlBroker {
        MysqlBroker::with_queue(&self.url, &self.queue)
            .await
            .expect("connecting to CELERS_TEST_MYSQL_URL should succeed")
    }

    /// A worker id that belongs to this fixture alone.
    ///
    /// `list_active_workers` and `get_worker_statistics` are queue-scoped, so
    /// this is belt and braces — but a shared literal like `"worker-1"` is
    /// exactly the kind of collision that made these tests order-dependent,
    /// and a namespaced id keeps the failure legible if one ever leaks.
    fn worker_id(&self, label: &str) -> String {
        format!("{label}-{}", self.queue)
    }

    /// Claim one task from this fixture's queue, retrying briefly while the
    /// claim comes back empty.
    ///
    /// A claim takes a next-key lock on the index record *after* the range it
    /// scanned, and that neighbour routinely belongs to a different queue:
    /// measured on this suite's MySQL 8.0 through
    /// `performance_schema.data_locks`, a single-task claim against queue
    /// `zzprobe_a` held `X` on the `idx_tasks_queue_dequeue` record of
    /// `zzprobe_b` and `X,REC_NOT_GAP` on that row's primary key. While such
    /// a neighbouring claim transaction is open, `SKIP LOCKED` skips this
    /// queue's own pending row and `dequeue()` returns `Ok(None)` — which,
    /// under `--test-threads` greater than one, turned an unrelated test red
    /// at random.
    ///
    /// That starvation is a real production limitation of the claim
    /// statement (reported separately; the remedy is a READ COMMITTED claim
    /// transaction or a primary-key-locking two-step claim), not something a
    /// test can fix. Tests whose subject is *not* claim concurrency therefore
    /// claim through this helper, so a neighbouring queue cannot decide their
    /// outcome. Tests that *are* about claim concurrency
    /// (`test_skip_locked_behavior`, `test_concurrent_dequeue`) deliberately
    /// call `dequeue` directly.
    async fn claim(&self) -> BrokerMessage {
        self.claim_with(None).await
    }

    /// [`Self::claim`], attributing the claim to `worker_id`.
    async fn claim_as(&self, worker_id: &str) -> BrokerMessage {
        self.claim_with(Some(worker_id)).await
    }

    async fn claim_with(&self, worker_id: Option<&str>) -> BrokerMessage {
        for _ in 0..CLAIM_ATTEMPTS {
            let claimed = match worker_id {
                Some(worker) => self.broker.dequeue_with_worker_id(worker).await,
                None => self.broker.dequeue().await,
            }
            .expect("dequeue should succeed");

            if let Some(message) = claimed {
                return message;
            }
            tokio::time::sleep(CLAIM_RETRY_DELAY).await;
        }
        panic!(
            "no task became claimable on queue {} within {CLAIM_ATTEMPTS} attempts",
            self.queue
        );
    }

    /// Claim exactly `count` tasks, retrying the batch claim the same way
    /// [`Self::claim`] retries a single one.
    async fn claim_batch(&self, count: usize) -> Vec<BrokerMessage> {
        let mut claimed = Vec::with_capacity(count);
        for _ in 0..CLAIM_ATTEMPTS {
            let remaining = count - claimed.len();
            if remaining == 0 {
                return claimed;
            }
            claimed.extend(
                self.broker
                    .dequeue_batch(remaining)
                    .await
                    .expect("dequeue_batch should succeed"),
            );
            if claimed.len() < count {
                tokio::time::sleep(CLAIM_RETRY_DELAY).await;
            }
        }
        assert_eq!(
            claimed.len(),
            count,
            "only {} of {count} tasks became claimable on queue {} within \
             {CLAIM_ATTEMPTS} attempts",
            claimed.len(),
            self.queue
        );
        claimed
    }

    /// Delete this queue's rows. See the type's documentation for why this is
    /// hygiene rather than the isolation mechanism.
    ///
    /// Retried through the crate's own deadlock loop: a ranged `DELETE`
    /// contends with concurrent claims exactly like the production statements
    /// do, and a live parallel run of this suite really did see it fail with
    /// `ERROR 1213`.
    async fn cleanup(&self) {
        crate::mysql_error::with_deadlock_retry("test cleanup", || async {
            self.broker
                .connection()
                .execute(
                    "DELETE FROM celers_tasks WHERE queue_name = ?",
                    &[&self.queue],
                )
                .await
        })
        .await
        .expect("cleanup of the test queue should succeed");
    }
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_mysql_broker_creation() {
    let Some(database_url) = legacy_mysql_url("test_mysql_broker_creation") else {
        return;
    };

    let broker = MysqlBroker::new(&database_url).await;
    assert!(broker.is_ok());
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_mysql_broker_lifecycle() {
    let Some(fixture) = LegacyFixture::open("test_mysql_broker_lifecycle").await else {
        return;
    };
    let broker = &fixture.broker;

    // Test enqueue
    let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4]);
    let task_id = task.metadata.id;

    let returned_id = broker.enqueue(task.clone()).await.unwrap();
    assert_eq!(returned_id, task_id);

    // Test queue size. Exact rather than `>= 1`: on an isolated queue the
    // enqueue above is the only thing that can be counted.
    let size = broker.queue_size().await.unwrap();
    assert_eq!(size, 1);

    // Test dequeue
    let msg = fixture.claim().await;
    assert_eq!(msg.task.metadata.name, "test_task");

    // Test ack
    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .unwrap();

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_mysql_queue_pause_resume() {
    let Some(fixture) = LegacyFixture::open("test_mysql_queue_pause_resume").await else {
        return;
    };
    let broker = &fixture.broker;

    // Initially not paused
    assert!(!broker.is_paused());

    // Pause
    broker.pause();
    assert!(broker.is_paused());

    // Dequeue should return None when paused
    let task = SerializedTask::new("pause_test".to_string(), vec![1, 2, 3]);
    broker.enqueue(task).await.unwrap();

    let msg = broker.dequeue().await.unwrap();
    assert!(msg.is_none());

    // Resume
    broker.resume();
    assert!(!broker.is_paused());

    // Now dequeue should work
    let msg = fixture.claim().await;
    assert_eq!(msg.task.metadata.name, "pause_test");

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_mysql_statistics() {
    let Some(fixture) = LegacyFixture::open("test_mysql_statistics").await else {
        return;
    };
    let broker = &fixture.broker;

    // A fresh queue starts empty. The previous `assert!(stats.total >= 0)`
    // was true of any `i64` the query could possibly return; an isolated
    // queue lets the same call be checked against a known number instead.
    //
    // `stats.dlq` is deliberately not asserted on: `celers_dead_letter_queue`
    // has no `queue_name` column, so that one field really is database-wide.
    let before = broker.get_statistics().await.unwrap();
    assert_eq!(before.total, 0);
    assert_eq!(before.pending, 0);

    for i in 0..3 {
        let task = SerializedTask::new(format!("stats_task_{}", i), vec![i as u8]);
        broker.enqueue(task).await.unwrap();
    }

    let after = broker.get_statistics().await.unwrap();
    assert_eq!(after.total, 3);
    assert_eq!(after.pending, 3);
    assert_eq!(after.processing, 0);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_mysql_health_check() {
    let Some(database_url) = legacy_mysql_url("test_mysql_health_check") else {
        return;
    };

    let broker = MysqlBroker::new(&database_url).await.unwrap();

    let health = broker.check_health().await.unwrap();
    assert!(health.healthy);
    assert!(!health.database_version.is_empty());
}

// ========== NEW: Additional Integration Tests ==========

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_batch_operations() {
    let Some(fixture) = LegacyFixture::open("test_batch_operations").await else {
        return;
    };
    let broker = &fixture.broker;

    // Batch enqueue
    let tasks: Vec<_> = (0..10)
        .map(|i| SerializedTask::new(format!("task_{}", i), vec![i as u8]))
        .collect();

    let task_ids = broker.enqueue_batch(tasks).await.unwrap();
    assert_eq!(task_ids.len(), 10);

    // Batch dequeue
    let messages = fixture.claim_batch(5).await;
    assert_eq!(messages.len(), 5);

    // Batch ack
    let ack_tasks: Vec<_> = messages
        .iter()
        .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
        .collect();
    broker.ack_batch(&ack_tasks).await.unwrap();

    // Verify remaining tasks
    let remaining = broker.queue_size().await.unwrap();
    assert_eq!(remaining, 5);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_task_chain() {
    let Some(fixture) = LegacyFixture::open("test_task_chain").await else {
        return;
    };
    let broker = &fixture.broker;

    // Create task chain
    let chain = TaskChain::new()
        .then(SerializedTask::new("step1".to_string(), vec![1]))
        .then(SerializedTask::new("step2".to_string(), vec![2]))
        .then(SerializedTask::new("step3".to_string(), vec![3]))
        .with_delay(2);

    let task_ids = broker.enqueue_chain(chain).await.unwrap();
    assert_eq!(task_ids.len(), 3);

    // Verify scheduled tasks. Exactly two: the first link runs immediately
    // (its `scheduled_at` is now, not in the future), the other two are
    // pushed out by the chain delay.
    let scheduled = broker.list_scheduled_tasks(10, 0).await.unwrap();
    assert_eq!(scheduled.len(), 2);
    assert_eq!(broker.count_scheduled_tasks().await.unwrap(), 2);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_connection_diagnostics() {
    let Some(database_url) = legacy_mysql_url("test_connection_diagnostics") else {
        return;
    };

    let broker = MysqlBroker::new(&database_url).await.unwrap();

    let diag = broker.get_connection_diagnostics();
    assert!(diag.max_connections > 0);
    assert!(diag.pool_utilization_percent >= 0.0);
    assert!(diag.pool_utilization_percent <= 100.0);
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_performance_metrics() {
    let Some(fixture) = LegacyFixture::open("test_performance_metrics").await else {
        return;
    };

    let metrics = fixture.broker.get_performance_metrics().await.unwrap();
    assert!(metrics.queue_depth >= 0);
    assert!(metrics.processing_tasks >= 0);
    assert!(metrics.dlq_size >= 0);
    assert!(metrics.connection_pool.max_connections > 0);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_migration_tracking() {
    let Some(fixture) = LegacyFixture::open("test_migration_tracking").await else {
        return;
    };

    // `celers_migrations` is one table for the whole database — there is no
    // per-queue schema — so this reads the same rows every queue's broker
    // sees. That is the property under test, not a leak.
    let migrations = fixture.broker.list_migrations().await.unwrap();
    assert!(migrations.len() >= 3); // At least 001, 002, 003

    // Verify migration names
    let versions: Vec<_> = migrations.iter().map(|m| m.version.as_str()).collect();
    assert!(versions.contains(&"001"));
    assert!(versions.contains(&"002"));
    assert!(versions.contains(&"003"));

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_is_ready() {
    let Some(database_url) = legacy_mysql_url("test_is_ready") else {
        return;
    };

    let broker = MysqlBroker::new(&database_url).await.unwrap();

    let ready = broker.is_ready().await;
    assert!(ready);
}

// ========== Concurrency Tests ==========

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_concurrent_dequeue() {
    let Some(fixture) = LegacyFixture::open("test_concurrent_dequeue").await else {
        return;
    };

    // Enqueue tasks
    let tasks: Vec<_> = (0..20)
        .map(|i| SerializedTask::new(format!("concurrent_{}", i), vec![i as u8]))
        .collect();
    fixture.broker.enqueue_batch(tasks).await.unwrap();

    // Spawn multiple workers dequeuing concurrently. Each opens its own
    // connection, but on *this fixture's* queue — a worker built with
    // `MysqlBroker::new` would land on the default queue and drain whatever
    // any other test happened to leave there, which is what made the
    // `total_dequeued == 20` assertion below unreliable.
    let mut handles = vec![];
    for worker_id in 0..5 {
        let db_url = fixture.url.clone();
        let queue = fixture.queue.clone();
        let handle = tokio::spawn(async move {
            let worker_broker = MysqlBroker::with_queue(&db_url, &queue).await.unwrap();
            let mut dequeued = 0;

            // 20 attempts per worker, not 10: five workers sharing one
            // queue serialise on the claim (a claim locks every row it
            // sorts, so only one of them can succeed at a time), and the
            // assertion below needs all 20 tasks drained.
            for _ in 0..20 {
                if let Ok(Some(msg)) = worker_broker.dequeue().await {
                    dequeued += 1;
                    // Acknowledge immediately
                    let _ = worker_broker
                        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                        .await;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }

            (worker_id, dequeued)
        });
        handles.push(handle);
    }

    // Wait for all workers
    let results = futures::future::join_all(handles).await;

    let total_dequeued: usize = results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|(_, count)| *count)
        .sum();

    // Should dequeue all 20 tasks across workers
    assert_eq!(total_dequeued, 20);

    fixture.cleanup().await;
}

/// Two competing consumers on one queue must never be handed the same task.
///
/// The original test claimed to prove that a concurrent pair of `dequeue()`
/// calls *both* return a task — but it asserted that only inside
/// `if let (Ok(Some(m1)), Ok(Some(m2)))`, so it passed silently whenever one
/// of them returned `None`. `None` turns out to be the ordinary case, not the
/// rare one: `ORDER BY priority DESC, created_at ASC` cannot be served by
/// `idx_tasks_queue_dequeue` (the `scheduled_at <= NOW()` range stops the
/// index before the ordering columns), so InnoDB reads and locks *every*
/// qualifying row before the sort applies `LIMIT 1`. Measured on this suite's
/// MySQL 8.0: one open claim against a ten-task queue holds 22 record locks,
/// and a competing claim issued while it is open returns nothing at all.
///
/// So "both consumers claim simultaneously" is not a property this statement
/// has, and asserting it would only make the suite lie in a new direction.
/// What `SKIP LOCKED` does guarantee — and what a task queue actually depends
/// on — is that a task is never handed to two consumers. This drains the
/// queue through two brokers and asserts exactly that.
#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_skip_locked_behavior() {
    let Some(fixture) = LegacyFixture::open("test_skip_locked_behavior").await else {
        return;
    };
    let broker1 = &fixture.broker;

    // Enqueue multiple tasks
    for i in 0..10 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker1.enqueue(task).await.unwrap();
    }

    // A second broker on the same queue, i.e. a competing consumer.
    let broker2 = fixture.peer().await;

    // Claim concurrently from both until the queue is drained. `CLAIM_ATTEMPTS`
    // rounds of two claims each is ample headroom for ten tasks even if every
    // single round serialises down to one successful claim.
    let mut claimed: Vec<uuid::Uuid> = Vec::with_capacity(10);
    for _ in 0..CLAIM_ATTEMPTS {
        if claimed.len() == 10 {
            break;
        }
        let (msg1, msg2) = tokio::join!(broker1.dequeue(), broker2.dequeue());
        for message in [
            msg1.expect("dequeue should succeed"),
            msg2.expect("dequeue should succeed"),
        ]
        .into_iter()
        .flatten()
        {
            claimed.push(message.task.metadata.id);
        }
        tokio::time::sleep(CLAIM_RETRY_DELAY).await;
    }

    assert_eq!(claimed.len(), 10, "every enqueued task must be claimable");

    let distinct: std::collections::HashSet<_> = claimed.iter().collect();
    assert_eq!(
        distinct.len(),
        10,
        "SKIP LOCKED must never hand the same task to two consumers"
    );

    fixture.cleanup().await;
}

// ========== Unit Tests for New Structures ==========

#[test]
fn test_pool_config_default() {
    let config = PoolConfig::default();
    assert_eq!(config.max_connections, 20);
    assert_eq!(config.min_connections, 2);
    assert_eq!(config.acquire_timeout_secs, 5);
    assert_eq!(config.max_lifetime_secs, Some(1800));
    assert_eq!(config.idle_timeout_secs, Some(600));
}

#[test]
fn test_task_chain_builder() {
    let task1 = SerializedTask::new("task1".to_string(), vec![1]);
    let task2 = SerializedTask::new("task2".to_string(), vec![2]);

    let chain = TaskChain::new().then(task1).then(task2).with_delay(5);

    assert_eq!(chain.tasks().len(), 2);
    assert_eq!(chain.delay_between_secs(), Some(5));
}

// ========== Unit Tests for Enhancement Methods ==========

#[test]
fn test_worker_statistics_serialization() {
    let stats = WorkerStatistics {
        worker_id: "worker-123".to_string(),
        active_tasks: 5,
        completed_tasks: 100,
        failed_tasks: 3,
        last_seen: Utc::now(),
        avg_task_duration_secs: 2.5,
    };

    // Should serialize and deserialize correctly
    let json = serde_json::to_string(&stats).unwrap();
    let deserialized: WorkerStatistics = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.worker_id, "worker-123");
    assert_eq!(deserialized.active_tasks, 5);
    assert_eq!(deserialized.completed_tasks, 100);
    assert_eq!(deserialized.failed_tasks, 3);
    assert_eq!(deserialized.avg_task_duration_secs, 2.5);
}

#[test]
fn test_task_age_distribution_serialization() {
    let dist = TaskAgeDistribution {
        bucket_label: "< 1 min".to_string(),
        task_count: 42,
        oldest_task_age_secs: 55,
    };

    let json = serde_json::to_string(&dist).unwrap();
    let deserialized: TaskAgeDistribution = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.bucket_label, "< 1 min");
    assert_eq!(deserialized.task_count, 42);
    assert_eq!(deserialized.oldest_task_age_secs, 55);
}

#[test]
fn test_retry_statistics_serialization() {
    let stats = RetryStatistics {
        task_name: "failing_task".to_string(),
        total_retries: 150,
        unique_tasks: 50,
        avg_retries_per_task: 3.0,
        max_retries_observed: 5,
    };

    let json = serde_json::to_string(&stats).unwrap();
    let deserialized: RetryStatistics = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.task_name, "failing_task");
    assert_eq!(deserialized.total_retries, 150);
    assert_eq!(deserialized.unique_tasks, 50);
    assert_eq!(deserialized.avg_retries_per_task, 3.0);
    assert_eq!(deserialized.max_retries_observed, 5);
}

// ========== Integration Tests for Enhancement Methods ==========

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_cancel_batch() {
    let Some(fixture) = LegacyFixture::open("test_cancel_batch").await else {
        return;
    };
    let broker = &fixture.broker;

    // Enqueue multiple tasks
    let mut task_ids = Vec::new();
    for i in 0..10 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        let task_id = broker.enqueue(task).await.unwrap();
        task_ids.push(task_id);
    }

    // Cancel half of them in batch
    let to_cancel = &task_ids[0..5];
    let cancelled = broker.cancel_batch(to_cancel).await.unwrap();
    assert_eq!(cancelled, 5);

    // Verify they're cancelled
    let stats = broker.get_statistics().await.unwrap();
    assert_eq!(stats.cancelled, 5);
    assert_eq!(stats.pending, 5);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_worker_statistics() {
    let Some(fixture) = LegacyFixture::open("test_worker_statistics").await else {
        return;
    };
    let broker = &fixture.broker;
    let worker = fixture.worker_id("test-worker");

    // Enqueue and dequeue a task with worker ID
    let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3]);
    broker.enqueue(task).await.unwrap();

    let msg = fixture.claim_as(&worker).await;

    // Get worker statistics
    let stats = broker.get_worker_statistics(&worker).await.unwrap();

    assert_eq!(stats.worker_id, worker);
    assert_eq!(stats.active_tasks, 1);

    // Acknowledge the task
    broker
        .ack(&msg.task_id(), msg.receipt_handle.as_deref())
        .await
        .unwrap();

    // Stats should update
    let stats = broker.get_worker_statistics(&worker).await.unwrap();
    assert_eq!(stats.active_tasks, 0);
    assert_eq!(stats.completed_tasks, 1);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_count_by_state_quick() {
    let Some(fixture) = LegacyFixture::open("test_count_by_state_quick").await else {
        return;
    };
    let broker = &fixture.broker;

    // Enqueue tasks
    for i in 0..5 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker.enqueue(task).await.unwrap();
    }

    // Count pending tasks
    let pending_count = broker
        .count_by_state_quick(DbTaskState::Pending)
        .await
        .unwrap();
    assert_eq!(pending_count, 5);

    // Dequeue one
    fixture.claim().await;

    // Check processing count
    let processing_count = broker
        .count_by_state_quick(DbTaskState::Processing)
        .await
        .unwrap();
    assert_eq!(processing_count, 1);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_task_age_distribution() {
    let Some(fixture) = LegacyFixture::open("test_task_age_distribution").await else {
        return;
    };
    let broker = &fixture.broker;

    // Enqueue some tasks
    for i in 0..10 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker.enqueue(task).await.unwrap();
    }

    // Get age distribution
    let distribution = broker.get_task_age_distribution().await.unwrap();

    // Should have at least one bucket
    assert!(!distribution.is_empty());

    // All tasks should be in the youngest bucket
    let youngest = distribution.first().unwrap();
    assert_eq!(youngest.bucket_label, "< 1 min");
    assert_eq!(youngest.task_count, 10);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_retry_statistics() {
    let Some(fixture) = LegacyFixture::open("test_retry_statistics").await else {
        return;
    };
    let broker = &fixture.broker;

    // Enqueue and fail some tasks to generate retries
    for i in 0..3 {
        let task = SerializedTask::new("failing_task".to_string(), vec![i as u8]);
        let _task_id = broker.enqueue(task).await.unwrap();

        // Dequeue and reject to trigger retry
        let msg = fixture.claim().await;
        broker
            .reject(&msg.task_id(), msg.receipt_handle.as_deref(), true)
            .await
            .unwrap();
    }

    // Get retry statistics. The assertion used to hide inside
    // `if !stats.is_empty()`, which made it vacuous whenever the query
    // returned nothing — and on the shared queue `stats[0]` was whichever
    // task name some *other* test had retried most. On an isolated queue
    // there is exactly one retried task name, and it must be this one.
    let stats = broker.get_retry_statistics().await.unwrap();
    assert_eq!(stats.len(), 1, "only this test's tasks have retried");

    let task_stats = &stats[0];
    assert_eq!(task_stats.task_name, "failing_task");
    assert!(task_stats.total_retries > 0);
    assert_eq!(task_stats.unique_tasks, 3);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_list_active_workers() {
    let Some(fixture) = LegacyFixture::open("test_list_active_workers").await else {
        return;
    };
    let broker = &fixture.broker;
    let worker_1 = fixture.worker_id("worker-1");
    let worker_2 = fixture.worker_id("worker-2");

    // Enqueue tasks
    for i in 0..3 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker.enqueue(task).await.unwrap();
    }

    // Dequeue with different workers
    let _msg1 = fixture.claim_as(&worker_1).await;
    let _msg2 = fixture.claim_as(&worker_2).await;

    // List active workers
    let workers = broker.list_active_workers().await.unwrap();
    assert_eq!(workers.len(), 2);
    assert!(workers.contains(&worker_1));
    assert!(workers.contains(&worker_2));

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires MySQL running
async fn test_get_all_worker_statistics() {
    let Some(fixture) = LegacyFixture::open("test_get_all_worker_statistics").await else {
        return;
    };
    let broker = &fixture.broker;
    let worker_alpha = fixture.worker_id("worker-alpha");
    let worker_beta = fixture.worker_id("worker-beta");

    // Enqueue tasks
    for i in 0..2 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker.enqueue(task).await.unwrap();
    }

    // Dequeue with workers
    let _msg1 = fixture.claim_as(&worker_alpha).await;
    let _msg2 = fixture.claim_as(&worker_beta).await;

    // Get all worker statistics
    let all_stats = broker.get_all_worker_statistics().await.unwrap();
    assert_eq!(all_stats.len(), 2);

    // Verify each worker has stats
    for stats in &all_stats {
        assert!(stats.worker_id == worker_alpha || stats.worker_id == worker_beta);
        assert_eq!(stats.active_tasks, 1);
    }

    fixture.cleanup().await;
}

#[test]
fn test_circuit_breaker_initial_state() {
    let config = CircuitBreakerConfig::default();
    let cb_internal = CircuitBreakerStateInternal::new(config);

    assert_eq!(cb_internal.state, CircuitBreakerState::Closed);
    assert_eq!(cb_internal.failure_count, 0);
    assert_eq!(cb_internal.success_count, 0);
    assert!(cb_internal.last_failure_time.is_none());
}

#[test]
fn test_circuit_breaker_config_default() {
    let config = CircuitBreakerConfig::default();
    assert_eq!(config.failure_threshold, 5);
    assert_eq!(config.timeout_secs, 60);
    assert_eq!(config.success_threshold, 2);
}

/// The connection string to test against, printing a visible, greppable skip
/// line when it is not configured.
///
/// `test_circuit_breaker_stats`/`test_circuit_breaker_reset` used to connect
/// to a hardcoded, never-reachable `mysql://test:test@localhost/test` and
/// silently `return` on any connection error — a skipped run and a real run
/// both reported `ok`, and there was no way to tell the two apart from the
/// test output, or to actually run this pair anywhere. Gating on
/// `CELERS_TEST_MYSQL_URL`, like every other integration test in this crate,
/// fixes both: it is real gating (a wrong-but-set URL now fails loudly via
/// `.expect`, rather than being swallowed as "connection failed, skip"), and
/// it is visible (the skip line names the test and the variable to set).
fn test_mysql_url(test_name: &str) -> Option<String> {
    match std::env::var("CELERS_TEST_MYSQL_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("SKIPPED: {test_name} (set CELERS_TEST_MYSQL_URL to run)");
            None
        }
    }
}

#[tokio::test]
async fn test_circuit_breaker_stats() {
    let Some(database_url) = test_mysql_url("test_circuit_breaker_stats") else {
        return;
    };

    let broker = MysqlBroker::new(&database_url)
        .await
        .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
    let stats = broker.get_circuit_breaker_stats();

    assert_eq!(stats.state, CircuitBreakerState::Closed);
    assert_eq!(stats.failure_count, 0);
    assert_eq!(stats.success_count, 0);
}

#[tokio::test]
async fn test_circuit_breaker_reset() {
    let Some(database_url) = test_mysql_url("test_circuit_breaker_reset") else {
        return;
    };

    let broker = MysqlBroker::new(&database_url)
        .await
        .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");

    // Manually trigger some failures
    for _ in 0..3 {
        broker.record_failure();
    }

    let stats_before = broker.get_circuit_breaker_stats();
    assert_eq!(stats_before.failure_count, 3);

    // Reset the circuit breaker
    broker.reset_circuit_breaker();

    let stats_after = broker.get_circuit_breaker_stats();
    assert_eq!(stats_after.state, CircuitBreakerState::Closed);
    assert_eq!(stats_after.failure_count, 0);
    assert_eq!(stats_after.success_count, 0);
}

#[test]
fn test_circuit_breaker_state_serialization() {
    // Test Closed
    let state = CircuitBreakerState::Closed;
    let json = serde_json::to_string(&state).unwrap();
    let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
    assert_eq!(state, deserialized);

    // Test Open
    let state = CircuitBreakerState::Open;
    let json = serde_json::to_string(&state).unwrap();
    let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
    assert_eq!(state, deserialized);

    // Test HalfOpen
    let state = CircuitBreakerState::HalfOpen;
    let json = serde_json::to_string(&state).unwrap();
    let deserialized: CircuitBreakerState = serde_json::from_str(&json).unwrap();
    assert_eq!(state, deserialized);
}

#[test]
fn test_circuit_breaker_stats_serialization() {
    let stats = CircuitBreakerStats {
        state: CircuitBreakerState::Open,
        failure_count: 5,
        success_count: 0,
        last_failure_time: Some(Utc::now()),
        last_state_change: Utc::now(),
    };

    let json = serde_json::to_string(&stats).unwrap();
    let deserialized: CircuitBreakerStats = serde_json::from_str(&json).unwrap();

    assert_eq!(stats.state, deserialized.state);
    assert_eq!(stats.failure_count, deserialized.failure_count);
    assert_eq!(stats.success_count, deserialized.success_count);
}
