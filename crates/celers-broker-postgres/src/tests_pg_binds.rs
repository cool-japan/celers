//! Live-PostgreSQL regression tests for this crate's **parameter binding
//! conventions**.
//!
//! `oxisql-postgres` sends every bound parameter in PostgreSQL's *binary*
//! wire format while rendering the non-primitive `oxisql_core::Value`
//! variants as text (see [`crate::row_ext::uuid_param`]). A `UUID`, `JSONB`,
//! `TIMESTAMPTZ` or `JSONPATH` placeholder therefore only works when the SQL
//! text pins it to `text` first — `$n::text::uuid`, `$n::text::jsonb`,
//! `$n::text::timestamptz`, `$n::text::jsonpath`. Without the cast the server
//! rejects the statement outright with
//! `incorrect binary data format in bind parameter n`, and no amount of
//! in-process testing catches it: the SQL is only ever validated by a real
//! server.
//!
//! `sql.rs`'s own unit tests assert the *text* of the centralised statements.
//! These tests are the other half: they execute the code paths whose SQL is
//! built inline or generated (the dynamic `IN (...)` lists especially), each
//! of which was a live-server failure before the casts were added, and none
//! of which `tests_pg.rs` reaches.
//!
//! Gated on `CELERS_TEST_POSTGRES_URL` exactly like `tests_pg.rs`: without it
//! each test logs a skip line and returns.

#![cfg(test)]

use celers_core::{Broker, SerializedTask};
use uuid::Uuid;

use crate::tests_pg::broker_on_new_queue;
use crate::{DbTaskState, PostgresBroker, TaskChain, TraceContext};

/// Macro sugar for the "skip when unconfigured" preamble, mirroring
/// `tests_pg.rs`'s own.
macro_rules! broker_or_skip {
    ($name:literal) => {
        match broker_on_new_queue($name).await {
            Some(pair) => pair,
            None => return,
        }
    };
}

/// Enqueue `count` plain pending tasks named `task_name` and return their ids.
async fn enqueue_pending(broker: &PostgresBroker, task_name: &str, count: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(
            broker
                .enqueue(SerializedTask::new(task_name.to_string(), vec![]))
                .await
                .expect("enqueue"),
        );
    }
    ids
}

// ── broker_core.rs: `SELECT metadata::text ... WHERE id = $1::text::uuid` ───

#[tokio::test]
async fn trace_context_round_trips_through_the_metadata_column() {
    let (broker, _queue) = broker_or_skip!("trace_context_round_trips");

    let trace_ctx = TraceContext::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    let expected_trace_id = trace_ctx.trace_id.clone();
    let task_id = broker
        .enqueue_with_trace_context(
            SerializedTask::new("traced_task".to_string(), vec![]),
            trace_ctx,
        )
        .await
        .expect("enqueue_with_trace_context");

    // Reads back through `SELECT metadata::text ... WHERE id = $1::text::uuid`:
    // the `::text` projection is what lets the driver decode a `jsonb` column
    // at all, and the `::text::uuid` cast is what lets the id bind.
    let read_back = broker
        .extract_trace_context(&task_id)
        .await
        .expect("extract_trace_context")
        .expect("the task carries a trace context");
    assert_eq!(read_back.trace_id, expected_trace_id);

    // An id that is not in the table is `None`, not an error.
    assert!(broker
        .extract_trace_context(&Uuid::new_v4())
        .await
        .expect("extract_trace_context for an unknown id")
        .is_none());
}

// ── dlq.rs: DLQ read/insert/delete, all four placeholders ──────────────────

#[tokio::test]
async fn requeue_from_dlq_moves_the_row_back_into_the_dispatch_queue() {
    let (broker, _queue) = broker_or_skip!("requeue_from_dlq");

    // Drive one task into the DLQ: `max_retries = 0` means the first
    // reject-with-requeue already exhausts the budget.
    let task = SerializedTask::new("dlq_requeue_me".to_string(), vec![7]).with_max_retries(0);
    let task_id = broker.enqueue(task).await.expect("enqueue");
    let msg = broker.dequeue().await.expect("dequeue").expect("a task");
    broker
        .reject(&task_id, msg.receipt_handle.as_deref(), true)
        .await
        .expect("reject into the DLQ");

    let dlq = broker.list_dlq(10, 0).await.expect("list_dlq");
    let entry = dlq
        .iter()
        .find(|t| t.task_id == task_id)
        .expect("the failed task is in the DLQ");

    let new_task_id = broker
        .requeue_from_dlq(&entry.id)
        .await
        .expect("requeue_from_dlq");
    assert_ne!(new_task_id, task_id, "requeue mints a fresh row id");

    // The requeued row is claimable from this queue, which is only true if the
    // INSERT carried `queue_name` and the id bound correctly.
    let claimed = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("the requeued task");
    assert_eq!(claimed.task.metadata.id, new_task_id);
    assert_eq!(claimed.task.metadata.name, "dlq_requeue_me");

    // And the DLQ row it came from is gone.
    let after = broker
        .list_dlq(10, 0)
        .await
        .expect("list_dlq after requeue");
    assert!(!after.iter().any(|t| t.id == entry.id));
}

// ── analytics.rs: `id = $2::text::uuid` and the `metadata->$2` projection ──

#[tokio::test]
async fn get_task_lifecycle_reads_a_task_by_its_uuid() {
    let (broker, _queue) = broker_or_skip!("get_task_lifecycle");

    let task_id = broker
        .enqueue(SerializedTask::new("lifecycle_probe".to_string(), vec![]))
        .await
        .expect("enqueue");

    let lifecycle = broker
        .get_task_lifecycle(&task_id)
        .await
        .expect("get_task_lifecycle")
        .expect("the task exists");
    assert_eq!(lifecycle.task_id, task_id);
    assert_eq!(lifecycle.task_name, "lifecycle_probe");
    assert_eq!(lifecycle.current_state, "pending");

    assert!(broker
        .get_task_lifecycle(&Uuid::new_v4())
        .await
        .expect("get_task_lifecycle for an unknown id")
        .is_none());
}

#[tokio::test]
async fn aggregate_by_metadata_groups_a_jsonb_key_read_as_text() {
    let (broker, _queue) = broker_or_skip!("aggregate_by_metadata");

    enqueue_pending(&broker, "aggregated_a", 2).await;
    enqueue_pending(&broker, "aggregated_b", 1).await;

    // `name` is written into every task's metadata by `enqueue`. The
    // projection is `(metadata->$2::text)::text`: without the outer `::text`
    // the driver cannot decode the `jsonb` expression result at all.
    let mut aggregated = broker
        .aggregate_by_metadata("name", 10)
        .await
        .expect("aggregate_by_metadata");
    aggregated.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        aggregated,
        vec![
            (Some("aggregated_a".to_string()), 2),
            (Some("aggregated_b".to_string()), 1),
        ]
    );
}

// ── queue_ops.rs: `$n::text::jsonpath` and jsonb-valued comparison ─────────

#[tokio::test]
async fn search_tasks_by_jsonpath_matches_string_and_numeric_metadata() {
    let (broker, _queue) = broker_or_skip!("search_tasks_by_jsonpath");

    let mut task = SerializedTask::new("jsonpath_probe".to_string(), vec![]);
    task.metadata.priority = 7;
    let task_id = broker.enqueue(task).await.expect("enqueue");
    enqueue_pending(&broker, "jsonpath_other", 1).await;

    // A string-valued key.
    let by_name = broker
        .search_tasks_by_jsonpath("$.name", &serde_json::json!("jsonpath_probe"), 10)
        .await
        .expect("search_tasks_by_jsonpath (string)");
    assert_eq!(by_name.len(), 1, "exactly the probe task matches");
    assert_eq!(by_name[0].id, task_id);

    // A *numeric*-valued key. The previous implementation compared
    // `value.as_str().unwrap_or("")`, so a non-string `value` could never
    // match anything; comparing as `jsonb` matches by value instead.
    let by_priority = broker
        .search_tasks_by_jsonpath("$.priority", &serde_json::json!(7), 10)
        .await
        .expect("search_tasks_by_jsonpath (number)");
    assert!(
        by_priority.iter().any(|t| t.id == task_id),
        "a numeric metadata value must be matchable"
    );

    // A value that is present nowhere matches nothing rather than erroring.
    let none = broker
        .search_tasks_by_jsonpath("$.name", &serde_json::json!("absent"), 10)
        .await
        .expect("search_tasks_by_jsonpath (miss)");
    assert!(none.is_empty());
}

#[tokio::test]
async fn find_and_count_tasks_by_metadata_bind_json_through_a_text_cast() {
    let (broker, _queue) = broker_or_skip!("find_tasks_by_metadata");

    let task_id = broker
        .enqueue(SerializedTask::new("metadata_probe".to_string(), vec![]))
        .await
        .expect("enqueue");
    enqueue_pending(&broker, "metadata_other", 2).await;

    let found = broker
        .find_tasks_by_metadata("name", &serde_json::json!("metadata_probe"), 10, 0)
        .await
        .expect("find_tasks_by_metadata");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, task_id);

    let count = broker
        .count_tasks_by_metadata("name", &serde_json::json!("metadata_other"))
        .await
        .expect("count_tasks_by_metadata");
    assert_eq!(count, 2);
}

// ── advanced_ops.rs: the two `${n}::text::uuid` IN-list generators ─────────

#[tokio::test]
async fn task_group_membership_is_written_through_a_generated_uuid_in_list() {
    let (broker, _queue) = broker_or_skip!("task_group_membership");

    let group_id = broker
        .create_task_group("bind-regression-group", Some("membership"))
        .await
        .expect("create_task_group");
    let ids = enqueue_pending(&broker, "grouped_task", 3).await;

    let added = broker
        .add_tasks_to_group(&group_id, &ids)
        .await
        .expect("add_tasks_to_group");
    assert_eq!(added, 3, "every listed id must be tagged into the group");

    let status = broker
        .get_task_group_status(&group_id)
        .await
        .expect("get_task_group_status")
        .expect("the group exists");
    assert_eq!(status.group_name, "bind-regression-group");
    assert_eq!(status.total_tasks, 3);
    assert_eq!(status.pending_count, 3);
}

#[tokio::test]
async fn tagging_tasks_is_written_through_a_generated_uuid_in_list() {
    let (broker, _queue) = broker_or_skip!("tag_tasks");

    let tagged_ids = enqueue_pending(&broker, "taggable", 2).await;
    enqueue_pending(&broker, "untagged", 1).await;

    let tagged = broker
        .tag_tasks(&tagged_ids, &["urgent", "bind-regression"])
        .await
        .expect("tag_tasks");
    assert_eq!(tagged, 2);

    let found = broker
        .find_tasks_by_tag("bind-regression", None, 10)
        .await
        .expect("find_tasks_by_tag");
    let mut found_ids: Vec<Uuid> = found.iter().map(|t| t.id).collect();
    found_ids.sort();
    let mut expected = tagged_ids.clone();
    expected.sort();
    assert_eq!(found_ids, expected, "only the tagged tasks carry the tag");
}

#[tokio::test]
async fn estimate_task_completion_time_reads_a_task_by_its_uuid() {
    let (broker, _queue) = broker_or_skip!("estimate_task_completion_time");

    let task_id = broker
        .enqueue(SerializedTask::new("estimated".to_string(), vec![]))
        .await
        .expect("enqueue");

    let estimate = broker
        .estimate_task_completion_time(&task_id)
        .await
        .expect("estimate_task_completion_time")
        .expect("a pending task has an estimate");
    assert_eq!(estimate.task_id, task_id);

    assert!(broker
        .estimate_task_completion_time(&Uuid::new_v4())
        .await
        .expect("estimate_task_completion_time for an unknown id")
        .is_none());
}

// ── workflows.rs: chain metadata read-back and the bulk-update IN list ─────

#[tokio::test]
async fn completing_a_chain_link_schedules_the_next_one() {
    let (broker, _queue) = broker_or_skip!("complete_chain_task");

    let chain = TaskChain {
        tasks: vec![
            SerializedTask::new("chain_first".to_string(), vec![]),
            SerializedTask::new("chain_second".to_string(), vec![]),
        ],
        stop_on_failure: true,
    };
    let ids = broker.enqueue_chain(chain).await.expect("enqueue_chain");
    assert_eq!(ids.len(), 2);

    // Only the head of the chain is due; the tail is parked 100 years out.
    let first = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("the chain head");
    assert_eq!(first.task.metadata.name, "chain_first");
    assert!(broker.dequeue().await.expect("dequeue").is_none());

    broker
        .ack(&first.task.metadata.id, first.receipt_handle.as_deref())
        .await
        .expect("ack");
    // Reads the completed task's metadata (`SELECT metadata::text ... WHERE
    // id = $1::text::uuid`) and pulls the successor's `scheduled_at` forward.
    broker
        .complete_chain_task(&first.task.metadata.id)
        .await
        .expect("complete_chain_task");

    let second = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("the next chain link is now due");
    assert_eq!(second.task.metadata.name, "chain_second");
}

#[tokio::test]
async fn bulk_update_state_touches_only_the_listed_ids() {
    let (broker, _queue) = broker_or_skip!("bulk_update_state");

    let ids = enqueue_pending(&broker, "bulk_updated", 3).await;
    let untouched = enqueue_pending(&broker, "bulk_untouched", 1).await;

    let updated = broker
        .bulk_update_state(&ids, DbTaskState::Cancelled)
        .await
        .expect("bulk_update_state");
    assert_eq!(updated, 3);

    for id in &ids {
        let info = broker
            .get_task(id)
            .await
            .expect("get_task")
            .expect("the task exists");
        assert_eq!(info.state, DbTaskState::Cancelled);
    }
    let survivor = broker
        .get_task(&untouched[0])
        .await
        .expect("get_task")
        .expect("the task exists");
    assert_eq!(survivor.state, DbTaskState::Pending);
}

// ── convenience.rs: `in_clause_placeholders` and the ack/reject closures ───

#[tokio::test]
async fn batch_cancel_cancels_only_the_listed_ids() {
    let (broker, _queue) = broker_or_skip!("batch_cancel");

    let cancel_ids = enqueue_pending(&broker, "batch_cancelled", 2).await;
    let survivors = enqueue_pending(&broker, "batch_survivor", 1).await;

    let cancelled = broker
        .batch_cancel(&cancel_ids)
        .await
        .expect("batch_cancel");
    assert_eq!(cancelled, 2);

    for id in &cancel_ids {
        let info = broker
            .get_task(id)
            .await
            .expect("get_task")
            .expect("the task exists");
        assert_eq!(info.state, DbTaskState::Cancelled);
    }
    let survivor = broker
        .get_task(&survivors[0])
        .await
        .expect("get_task")
        .expect("the task exists");
    assert_eq!(survivor.state, DbTaskState::Pending);
}

#[tokio::test]
async fn dequeue_with_handlers_acks_the_row_it_claimed() {
    let (broker, _queue) = broker_or_skip!("dequeue_with_handlers");

    let task_id = broker
        .enqueue(SerializedTask::new("handled".to_string(), vec![]))
        .await
        .expect("enqueue");

    let (task, ack, _reject) = broker
        .dequeue_with_handlers()
        .await
        .expect("dequeue_with_handlers")
        .expect("a task");
    assert_eq!(task.metadata.id, task_id);

    // The closure runs its own `UPDATE ... WHERE id = $1::text::uuid`, built
    // inline rather than through `sql.rs`.
    ack().await.expect("ack handler");

    let info = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("the task exists");
    assert_eq!(info.state, DbTaskState::Completed);
}

// ── NUMERIC projections read as `f64`/`i64` ────────────────────────────────
//
// `EXTRACT(EPOCH FROM ...)`, `AVG(...)` and `STDDEV(...)` all return
// PostgreSQL `NUMERIC`, which `oxisql-postgres` maps to
// `oxisql_core::Value::Decimal(String)` — and `FromValue` implements `f64`
// and `i64` against `Value::F64`/`Value::I64` only. Reading such a column
// with `row.col::<f64>(..)` is therefore always a `TypeMismatch` once a row
// actually exists, which is why these paths only fail when there is data to
// aggregate. Every such projection in this crate now carries an explicit
// `::double precision` / `::BIGINT` cast in the SQL, and these tests run each
// of them against rows that exercise the aggregate.

/// Complete one task so the duration aggregates below have a row to chew on.
async fn one_completed_task(broker: &PostgresBroker, task_name: &str) -> Uuid {
    let task_id = broker
        .enqueue(SerializedTask::new(task_name.to_string(), vec![]))
        .await
        .expect("enqueue");
    let msg = broker.dequeue().await.expect("dequeue").expect("a task");
    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .expect("ack");
    task_id
}

#[tokio::test]
async fn duration_aggregates_survive_a_completed_task() {
    let (broker, _queue) = broker_or_skip!("duration_aggregates");

    one_completed_task(&broker, "aggregate_target").await;
    one_completed_task(&broker, "aggregate_target").await;

    // `AVG(EXTRACT(EPOCH ...) * 1000)` per task name.
    let durations = broker
        .get_avg_task_duration_by_name()
        .await
        .expect("get_avg_task_duration_by_name");
    let avg = durations
        .get("aggregate_target")
        .copied()
        .expect("the completed task type is present");
    assert!(
        avg.is_finite() && avg >= 0.0,
        "average duration must be a real number, got {avg}"
    );

    // `AVG(EXTRACT(EPOCH ...))` over pending/processing spans.
    let stats = broker
        .get_state_transition_stats(24)
        .await
        .expect("get_state_transition_stats");
    assert!(stats.avg_time_pending_secs.is_finite());
    assert!(stats.avg_time_processing_secs.is_finite());
    assert_eq!(stats.completed_count, 2);
    assert!((stats.success_rate - 1.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn worker_stats_read_their_numeric_averages() {
    let (broker, _queue) = broker_or_skip!("worker_stats");

    // `get_worker_stats` only counts rows with a `worker_id`, which nothing in
    // this crate's public API sets — so the aggregate is exercised for its
    // *decoding*, and an empty result is the correct answer here.
    let stats = broker.get_worker_stats(10).await.expect("get_worker_stats");
    assert!(stats.iter().all(|(_, processed, avg, rate)| {
        *processed >= 0 && avg.is_none_or(f64::is_finite) && rate.is_none_or(f64::is_finite)
    }));

    // Server-wide (not queue-scoped) average over completed tasks: `NULL` when
    // nothing has completed anywhere, a finite number otherwise.
    one_completed_task(&broker, "avg_processing_probe").await;
    let avg = broker
        .avg_processing_time_ms()
        .await
        .expect("avg_processing_time_ms");
    assert!(avg.is_none_or(f64::is_finite));
}

#[tokio::test]
async fn queue_forecast_and_trend_read_their_numeric_averages() {
    let (broker, _queue) = broker_or_skip!("queue_forecast_and_trend");

    enqueue_pending(&broker, "forecast_probe", 3).await;
    one_completed_task(&broker, "forecast_probe").await;

    let forecast = broker
        .forecast_queue_depth(2)
        .await
        .expect("forecast_queue_depth");
    assert_eq!(forecast.forecast_hours, 2);
    assert!(forecast.avg_arrival_rate.is_finite());
    assert!(forecast.avg_completion_rate.is_finite());
    assert!(forecast.confidence.is_finite());

    let trend = broker
        .get_queue_trend_analysis(24)
        .await
        .expect("get_queue_trend_analysis");
    assert_eq!(trend.hours_analyzed, 24);
    assert!(trend.avg_pending.is_finite());
    assert!(
        trend.avg_pending > 0.0,
        "three pending tasks must show up in the hourly average, got {}",
        trend.avg_pending
    );
}

// ── `INTERVAL '1 hour' * $n` and `EXTRACT(EPOCH ...) > $n` ─────────────────
//
// Both operands are the same hazard from the other direction: the *server*
// infers a non-integer type for the placeholder (`double precision` for the
// interval multiplication, `numeric` for the EXTRACT comparison), while
// `oxisql-postgres` writes an `i64` parameter as raw 8-byte `INT8` binary
// whatever the inferred type is — `OwnedParam::accepts()` returns `true`
// unconditionally, so nothing rejects it client-side. The comparison then
// either fails outright (`incorrect binary data format`) or, for the interval
// case, silently reinterprets the integer's bit pattern as a float: `24`
// becomes ~1.2e-322, so `NOW() - INTERVAL '1 hour' * 24` collapses to `NOW()`
// and every window query quietly returns nothing. A `$n::bigint` cast pins
// the parameter to `int8` and lets the server widen it.

#[tokio::test]
async fn hour_window_parameters_actually_span_the_window() {
    let (broker, _queue) = broker_or_skip!("hour_window_parameters");

    one_completed_task(&broker, "windowed").await;

    // A 24-hour window must see a task completed a moment ago. With a bare
    // `INTERVAL '1 hour' * $2` this returned zero rows for every window.
    let stats = broker
        .get_state_transition_stats(24)
        .await
        .expect("get_state_transition_stats");
    assert_eq!(
        stats.completed_count, 1,
        "a task completed seconds ago must fall inside a 24-hour window"
    );
}

#[tokio::test]
async fn age_thresholds_compare_against_a_numeric_extract() {
    let (broker, _queue) = broker_or_skip!("age_thresholds");

    let ids = enqueue_pending(&broker, "aging", 2).await;

    // `EXTRACT(EPOCH FROM (NOW() - created_at)) > $2::bigint`: a zero-second
    // threshold matches everything pending, and the statement must execute at
    // all — an uncast `$2` is rejected by the server as an `int8` payload for
    // a `numeric` parameter.
    let boosted = broker
        .auto_adjust_priority_by_age(0, 5)
        .await
        .expect("auto_adjust_priority_by_age");
    assert_eq!(boosted, 2, "both pending tasks are older than zero seconds");

    for id in &ids {
        let info = broker
            .get_task(id)
            .await
            .expect("get_task")
            .expect("the task exists");
        assert_eq!(info.priority, 5);
    }

    // A threshold nothing can exceed matches nothing, rather than erroring.
    let none = broker
        .auto_adjust_priority_by_age(86_400, 5)
        .await
        .expect("auto_adjust_priority_by_age (wide threshold)");
    assert_eq!(none, 0);

    // Same comparison inside `detect_abnormal_state_duration`'s pending arm.
    let stuck = broker
        .detect_abnormal_state_duration("pending", 0, 10)
        .await
        .expect("detect_abnormal_state_duration");
    assert_eq!(stuck.len(), 2);
}
