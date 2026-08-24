//! Integration tests for [`RedisBroker`]'s delivery guarantees.
//!
//! These exercise the real Redis protocol (against a local server) because
//! the properties under test — atomicity, FIFO ordering, visibility timeouts,
//! revocation — live in Lua scripts and command semantics, not in Rust logic
//! that could be unit-tested in isolation.
//!
//! Every test uses a UUID-suffixed queue so they can run concurrently, and
//! none of them sleeps: expiry is exercised by setting the visibility timeout
//! to zero rather than by waiting for wall-clock time.

use crate::{QueueMode, RedisBroker};
use celers_core::{Broker, SerializedTask, TaskId};
use redis::AsyncCommands;

const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

fn test_broker(mode: QueueMode) -> RedisBroker {
    let queue = format!("test-broker-{}", uuid::Uuid::new_v4());
    RedisBroker::with_mode(TEST_REDIS_URL, &queue, mode).expect("broker")
}

fn task_named(name: &str) -> SerializedTask {
    SerializedTask::new(name.to_string(), name.as_bytes().to_vec())
}

fn task_with_priority(name: &str, priority: i32) -> SerializedTask {
    let mut task = task_named(name);
    task.metadata.priority = priority;
    task
}

async fn raw_connection() -> redis::aio::MultiplexedConnection {
    redis::Client::open(TEST_REDIS_URL)
        .expect("client")
        .get_multiplexed_async_connection()
        .await
        .expect("connection")
}

async fn cleanup(broker: &RedisBroker) {
    let mut conn = raw_connection().await;
    let mut keys = broker.queue_names();
    keys.push(broker.keys().revoked.clone());
    keys.push(broker.keys().pause.clone());
    let _: i64 = conn.del(&keys).await.unwrap_or(0);
}

async fn dequeued_name(broker: &RedisBroker) -> Option<String> {
    broker
        .dequeue()
        .await
        .expect("dequeue")
        .map(|message| message.task.metadata.name)
}

/// FIFO mode must actually be FIFO. Appending with `RPUSH` while consuming
/// with `BRPOPLPUSH` (which pops the *tail*) makes the queue a stack: the
/// newest task is delivered first and the oldest can starve forever.
#[tokio::test]
async fn test_fifo_mode_delivers_in_enqueue_order() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    for name in ["first", "second", "third"] {
        broker.enqueue(task_named(name)).await.expect("enqueue");
    }

    // Peek must agree with what dequeue is about to hand out.
    assert_eq!(
        broker
            .peek_next()
            .await
            .expect("peek")
            .map(|task| task.metadata.name),
        Some("first".to_string())
    );

    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("first"));
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("second"));
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("third"));
    assert_eq!(dequeued_name(&broker).await, None);

    cleanup(&broker).await;
}

/// Batch enqueue and batch dequeue must observe the same order as the
/// single-message path.
#[tokio::test]
async fn test_fifo_batch_round_trip_preserves_order() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    broker
        .enqueue_batch(vec![task_named("a"), task_named("b"), task_named("c")])
        .await
        .expect("enqueue batch");

    let names: Vec<String> = broker
        .dequeue_batch(3)
        .await
        .expect("dequeue batch")
        .into_iter()
        .map(|message| message.task.metadata.name)
        .collect();

    assert_eq!(names, vec!["a", "b", "c"]);

    cleanup(&broker).await;
}

/// A retried task must go to the *back* of the queue. Requeueing at the
/// delivery front produces a hot retry loop that starves everything behind it.
#[tokio::test]
async fn test_requeued_task_does_not_jump_the_queue() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    broker.enqueue(task_named("first")).await.expect("enqueue");
    broker.enqueue(task_named("second")).await.expect("enqueue");

    let message = broker.dequeue().await.expect("dequeue").expect("a message");
    assert_eq!(message.task.metadata.name, "first");

    broker
        .reject(
            &message.task.metadata.id,
            message.receipt_handle.as_deref(),
            true,
        )
        .await
        .expect("reject");

    // "second" was already waiting, so it must be served before the retry.
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("second"));
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("first"));

    cleanup(&broker).await;
}

/// Priority dequeue must stage the message in one atomic step. The old
/// `ZPOPMIN` + separate `LPUSH` destroyed the task if anything failed in
/// between: it was gone from the sorted set and had never reached the
/// processing list.
#[tokio::test]
async fn test_priority_dequeue_stages_in_flight_atomically() {
    let broker = test_broker(QueueMode::Priority);

    broker
        .enqueue(task_with_priority("low", 1))
        .await
        .expect("enqueue");
    broker
        .enqueue(task_with_priority("high", 9))
        .await
        .expect("enqueue");

    let message = broker.dequeue().await.expect("dequeue").expect("a message");
    assert_eq!(message.task.metadata.name, "high");

    let mut conn = raw_connection().await;
    let staged: usize = conn
        .llen(broker.processing_queue_name())
        .await
        .expect("llen");
    let in_flight: usize = conn.zcard(broker.unacked_set_name()).await.expect("zcard");
    assert_eq!(
        staged, 1,
        "the message must be staged on the processing list"
    );
    assert_eq!(in_flight, 1, "and must carry a visibility deadline");

    // Acking clears both structures, so nothing is left for the reaper.
    broker
        .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
        .await
        .expect("ack");

    let staged: usize = conn
        .llen(broker.processing_queue_name())
        .await
        .expect("llen");
    let in_flight: usize = conn.zcard(broker.unacked_set_name()).await.expect("zcard");
    assert_eq!(staged, 0);
    assert_eq!(in_flight, 0);

    cleanup(&broker).await;
}

/// A worker that dies mid-task must not hold the message hostage forever.
/// With a zero visibility timeout the deadline is already in the past, so the
/// reaper reclaims it deterministically -- no sleeping.
#[tokio::test]
async fn test_expired_in_flight_task_is_redelivered() {
    let broker = test_broker(QueueMode::Fifo)
        .with_block_timeout(0.0)
        .with_visibility_timeout(0);

    broker
        .enqueue(task_named("orphaned"))
        .await
        .expect("enqueue");
    let message = broker.dequeue().await.expect("dequeue").expect("a message");
    assert_eq!(message.task.metadata.name, "orphaned");
    assert_eq!(broker.queue_size().await.expect("size"), 0);

    // Simulate the worker crashing: never ack.
    let recovered = broker.recover_processing_tasks().await.expect("recover");
    assert_eq!(recovered, 1);
    assert_eq!(broker.queue_size().await.expect("size"), 1);

    let mut conn = raw_connection().await;
    let staged: usize = conn
        .llen(broker.processing_queue_name())
        .await
        .expect("llen");
    let in_flight: usize = conn.zcard(broker.unacked_set_name()).await.expect("zcard");
    assert_eq!(staged, 0, "recovery must clear the processing list");
    assert_eq!(in_flight, 0, "recovery must clear the deadline");

    // And it really is deliverable again.
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("orphaned"));

    cleanup(&broker).await;
}

/// An acknowledged task must never be reclaimed, however long the worker
/// took: recovery keys on the deadline, not on the processing list alone.
#[tokio::test]
async fn test_acked_task_is_never_redelivered() {
    let broker = test_broker(QueueMode::Fifo)
        .with_block_timeout(0.0)
        .with_visibility_timeout(0);

    broker.enqueue(task_named("done")).await.expect("enqueue");
    let message = broker.dequeue().await.expect("dequeue").expect("a message");
    broker
        .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
        .await
        .expect("ack");

    assert_eq!(broker.recover_processing_tasks().await.expect("recover"), 0);
    assert_eq!(broker.queue_size().await.expect("size"), 0);

    cleanup(&broker).await;
}

/// A message staged on the processing list whose deadline write was lost --
/// the worker died between the blocking pop and the `ZADD` -- must still be
/// recoverable. The reaper adopts such orphans and gives them a deadline.
#[tokio::test]
async fn test_orphan_without_deadline_is_adopted_and_recovered() {
    let broker = test_broker(QueueMode::Fifo)
        .with_block_timeout(0.0)
        .with_visibility_timeout(0);

    let orphan = serde_json::to_string(&task_named("stranded")).expect("serialize");
    let mut conn = raw_connection().await;
    let _: i64 = conn
        .lpush(broker.processing_queue_name(), &orphan)
        .await
        .expect("stage orphan");

    let recovered = broker.recover_processing_tasks().await.expect("recover");
    assert_eq!(recovered, 1, "an orphan with no deadline must be adopted");
    assert_eq!(broker.queue_size().await.expect("size"), 1);

    cleanup(&broker).await;
}

/// Delayed promotion must be exactly-once even when several workers sweep at
/// the same moment. The old read-then-write pipeline let every sweeper read
/// the same ready set and re-enqueue it before any of them removed it.
#[tokio::test]
async fn test_delayed_promotion_is_exactly_once_under_concurrency() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    let due = crate::now_secs() as i64 - 10;
    broker
        .enqueue_at(task_named("scheduled"), due)
        .await
        .expect("schedule");

    let (a, b, c, d) = tokio::join!(
        broker.promote_delayed_tasks(),
        broker.promote_delayed_tasks(),
        broker.promote_delayed_tasks(),
        broker.promote_delayed_tasks(),
    );

    let total =
        a.expect("promote") + b.expect("promote") + c.expect("promote") + d.expect("promote");
    assert_eq!(total, 1, "the task may only be claimed once");
    assert_eq!(
        broker.queue_size().await.expect("size"),
        1,
        "concurrent sweeps must not duplicate the task"
    );

    let mut conn = raw_connection().await;
    let remaining: usize = conn
        .zcard(broker.delayed_queue_name())
        .await
        .expect("zcard");
    assert_eq!(remaining, 0, "a promoted task must leave the delayed set");

    cleanup(&broker).await;
}

/// A task that is not yet due must stay in the delayed set.
#[tokio::test]
async fn test_delayed_task_is_not_promoted_before_its_time() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    let future = crate::now_secs() as i64 + 3_600;
    broker
        .enqueue_at(task_named("later"), future)
        .await
        .expect("schedule");

    assert_eq!(broker.promote_delayed_tasks().await.expect("promote"), 0);
    assert_eq!(broker.queue_size().await.expect("size"), 0);
    assert_eq!(dequeued_name(&broker).await, None);

    cleanup(&broker).await;
}

/// Cancelling a task that is still queued must actually cancel it: remove it
/// from the pending structures *and* record the revocation, so a copy that is
/// already in flight elsewhere is dropped rather than executed.
#[tokio::test]
async fn test_cancel_removes_pending_task_and_records_revocation() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    let task = task_named("doomed");
    let task_id: TaskId = task.metadata.id;
    broker.enqueue(task.clone()).await.expect("enqueue");
    broker
        .enqueue_at(task_named("also_doomed"), crate::now_secs() as i64 + 3_600)
        .await
        .expect("schedule");

    assert!(broker.cancel(&task_id).await.expect("cancel"));
    assert_eq!(
        broker.queue_size().await.expect("size"),
        0,
        "a revoked task must not stay in the queue"
    );
    assert_eq!(dequeued_name(&broker).await, None);

    // A revoked id stays revoked: re-enqueueing the same task must not
    // resurrect it, because every dequeue path consults the revoked set.
    broker.enqueue(task).await.expect("re-enqueue");
    assert_eq!(dequeued_name(&broker).await, None);
    assert_eq!(broker.queue_size().await.expect("size"), 0);

    cleanup(&broker).await;
}

/// A paused queue must stop delivering. Nothing consulted the pause flag
/// before, so `QueueController::pause` did not actually pause anything.
#[tokio::test]
async fn test_paused_queue_delivers_nothing() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);
    let controller = broker.queue_controller();

    broker
        .enqueue(task_named("waiting"))
        .await
        .expect("enqueue");
    controller.pause().await.expect("pause");

    assert_eq!(dequeued_name(&broker).await, None, "paused: no delivery");
    assert_eq!(
        broker.queue_size().await.expect("size"),
        1,
        "the task must still be in the queue"
    );

    controller.resume().await.expect("resume");
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("waiting"));

    cleanup(&broker).await;
}

/// Bulk replay must read the dead letter queue once, not once per task.
/// Correctness-wise: every entry moves, and the DLQ ends up empty.
#[tokio::test]
async fn test_bulk_replay_moves_every_dlq_entry() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    for name in ["one", "two", "three"] {
        broker.enqueue(task_named(name)).await.expect("enqueue");
        let message = broker.dequeue().await.expect("dequeue").expect("a message");
        broker
            .reject(
                &message.task.metadata.id,
                message.receipt_handle.as_deref(),
                false,
            )
            .await
            .expect("reject to DLQ");
    }

    assert_eq!(broker.dlq_size().await.expect("dlq size"), 3);
    assert_eq!(broker.queue_size().await.expect("size"), 0);

    let replayed = broker
        .bulk_replay_from_dlq(None)
        .await
        .expect("bulk replay");
    assert_eq!(replayed, 3);
    assert_eq!(broker.dlq_size().await.expect("dlq size"), 0);
    assert_eq!(broker.queue_size().await.expect("size"), 3);

    // Replayed tasks are pending again, not stuck in their failed state.
    let message = broker.dequeue().await.expect("dequeue").expect("a message");
    assert_eq!(message.task.metadata.state, celers_core::TaskState::Pending);

    cleanup(&broker).await;
}

/// Replaying a single id must move exactly that entry.
#[tokio::test]
async fn test_replay_single_task_from_dlq() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    let mut ids = Vec::new();
    for name in ["keep", "replay"] {
        let task = task_named(name);
        ids.push(task.metadata.id);
        broker.enqueue(task).await.expect("enqueue");
        let message = broker.dequeue().await.expect("dequeue").expect("a message");
        broker
            .reject(
                &message.task.metadata.id,
                message.receipt_handle.as_deref(),
                false,
            )
            .await
            .expect("reject to DLQ");
    }

    assert!(broker.replay_from_dlq(&ids[1]).await.expect("replay"));
    assert_eq!(broker.dlq_size().await.expect("dlq size"), 1);
    assert_eq!(dequeued_name(&broker).await.as_deref(), Some("replay"));

    // A second replay of the same id finds nothing left to move.
    assert!(!broker.replay_from_dlq(&ids[1]).await.expect("replay"));

    cleanup(&broker).await;
}

/// The blocking tail of the dequeue path must work and must still record a
/// visibility deadline: it runs on a pooled connection outside the atomic
/// script, so it is the one path where staging and the deadline are two
/// steps.
#[tokio::test]
async fn test_blocking_dequeue_receives_and_stages_a_late_arrival() {
    // The deadline is written when the message is delivered, so the timeout
    // has to be set up front: lowering it afterwards cannot retroactively
    // expire an already-recorded deadline.
    let broker = test_broker(QueueMode::Fifo)
        .with_block_timeout(2.0)
        .with_visibility_timeout(0);

    // The queue is empty when the dequeue starts, so it falls through to the
    // blocking pop; the enqueue then satisfies it. No sleeping: whichever
    // order the two futures interleave in, the message must arrive.
    let (dequeued, enqueued) = tokio::join!(broker.dequeue(), async {
        broker.enqueue(task_named("late")).await
    });

    enqueued.expect("enqueue");
    let message = dequeued.expect("dequeue").expect("a message");
    assert_eq!(message.task.metadata.name, "late");

    let mut conn = raw_connection().await;
    let in_flight: usize = conn.zcard(broker.unacked_set_name()).await.expect("zcard");
    assert_eq!(
        in_flight, 1,
        "a message taken by the blocking path still needs a deadline"
    );

    // And it is recoverable if this worker dies without acking.
    assert_eq!(broker.recover_processing_tasks().await.expect("recover"), 1);

    cleanup(&broker).await;
}

/// The broker must reuse one connection instead of opening a new TCP
/// connection -- plus handshake, plus tokio task, plus a socket left in
/// `TIME_WAIT` -- for every operation.
///
/// Asking Redis for `CLIENT ID` over the broker's own connection makes this
/// exact rather than statistical: the id is per-connection, so an unchanged
/// id across a burst of operations proves the socket was reused.
#[tokio::test]
async fn test_broker_reuses_a_single_connection() {
    let broker = test_broker(QueueMode::Fifo).with_block_timeout(0.0);

    async fn client_id(broker: &RedisBroker) -> i64 {
        let mut conn = broker.get_connection().await.expect("connection");
        redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut conn)
            .await
            .expect("client id")
    }

    let before = client_id(&broker).await;

    for _ in 0..20 {
        broker.enqueue(task_named("churn")).await.expect("enqueue");
        broker.queue_size().await.expect("size");
    }
    for _ in 0..20 {
        let message = broker.dequeue().await.expect("dequeue").expect("a message");
        broker
            .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
            .await
            .expect("ack");
    }

    assert_eq!(
        before,
        client_id(&broker).await,
        "80 operations must all run over the same connection"
    );

    // Helpers built from the broker share it rather than opening their own.
    let deduplicator = broker.deduplicator();
    deduplicator
        .check(&task_named("churn"))
        .await
        .expect("dedup check");
    assert_eq!(
        before,
        client_id(&broker).await,
        "helpers must inherit the broker's connection"
    );

    cleanup(&broker).await;
}
