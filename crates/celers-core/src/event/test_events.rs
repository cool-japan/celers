//! Event fixtures shared by this module's test suites.
//!
//! [`wire`](super::wire) and [`message`](super::message) both need "one of
//! every event a worker can publish", and they must agree on exactly which
//! events those are — a variant covered by one round-trip test and missed by
//! the other is the gap the byte-for-byte fixture exists to close. Keeping the
//! list here makes that impossible.

use super::{Event, TaskEvent, WorkerEvent};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Parse an RFC3339 literal used as a test fixture.
pub(crate) fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid RFC3339 fixture")
        .with_timezone(&Utc)
}

/// Every event a worker can publish, with fixed microsecond timestamps so
/// the round trip can be asserted for exact equality.
pub(crate) fn every_event() -> Vec<Event> {
    let task_id = Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
    let timestamp = at("2026-03-04T05:06:07.123456Z");
    vec![
        Event::Task(TaskEvent::Sent {
            task_id,
            task_name: "tasks.add".to_string(),
            queue: "celery".to_string(),
            timestamp,
            args: Some("[1, 2]".to_string()),
            kwargs: Some("{}".to_string()),
            eta: Some(at("2026-03-04T05:10:00Z")),
            expires: Some(at("2026-03-04T06:00:00Z")),
            retries: Some(1),
        }),
        Event::Task(TaskEvent::Received {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            pid: 4242,
        }),
        Event::Task(TaskEvent::Started {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            pid: 4242,
        }),
        Event::Task(TaskEvent::Succeeded {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            runtime: 1.5,
            result: Some("3".to_string()),
        }),
        Event::Task(TaskEvent::Failed {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            exception: "ValueError('bad')".to_string(),
            traceback: Some("Traceback...".to_string()),
        }),
        Event::Task(TaskEvent::Retried {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            exception: "Timeout".to_string(),
            retries: 2,
        }),
        Event::Task(TaskEvent::Revoked {
            task_id,
            task_name: Some("tasks.add".to_string()),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            terminated: true,
            signum: Some(9),
            expired: false,
        }),
        Event::Task(TaskEvent::Rejected {
            task_id,
            task_name: Some("tasks.add".to_string()),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            reason: "queue full".to_string(),
        }),
        Event::Task(TaskEvent::SoftTimeLimitExceeded {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp,
            elapsed_secs: 30.25,
            limit_secs: 30.0,
        }),
        Event::Worker(WorkerEvent::Online {
            hostname: "celery@worker-1".to_string(),
            timestamp,
            sw_ident: "celers".to_string(),
            sw_ver: "0.3.1".to_string(),
            sw_sys: "linux".to_string(),
        }),
        Event::Worker(WorkerEvent::Offline {
            hostname: "celery@worker-1".to_string(),
            timestamp,
        }),
        Event::Worker(WorkerEvent::Heartbeat {
            hostname: "celery@worker-1".to_string(),
            timestamp,
            active: 3,
            processed: 100,
            loadavg: Some([1.0, 0.8, 0.5]),
            freq: 2.0,
        }),
    ]
}
