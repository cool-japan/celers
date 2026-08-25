//! The event wire contract, proven end to end.
//!
//! `celers-backend-redis` is one of only two crates that depend on **both**
//! `celers-core` (the typed internal event model) and `celers-protocol` (the
//! Celery wire model), which makes it the natural home for the tests that pin
//! the two together. Every assertion here is the shape a real Celery monitor
//! (`celery events`, Flower) reads:
//!
//! * what a worker emits renders into `celers_protocol::event::EventMessage`;
//! * the task fields land on `celers_protocol::event::TaskEvent`'s *typed*
//!   `uuid` / `name` / `runtime` / ... members, not in the untyped `fields`
//!   catch-all — that is what actually pins the field names;
//! * the same payload parses back into the typed model unchanged.
//!
//! Tests are split in two layers:
//!
//! * plain `#[test]`s covering everything decidable without a server;
//! * `#[tokio::test] #[ignore]` integration tests requiring a live Redis, run
//!   with `cargo nextest run -p celers-backend-redis --all-features
//!   --run-ignored all` (override the server with `CELERS_TEST_REDIS_URL`).

use celers_core::event::{Event, EventEmitter, TaskEvent, TaskEventBuilder, WorkerEventBuilder};
use celers_protocol::event as protocol;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use crate::event_transport::{RedisEventConfig, RedisEventEmitter};

// =============================================================================
// Helpers
// =============================================================================

fn redis_url() -> String {
    std::env::var("CELERS_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

const HOSTNAME: &str = "celery@worker-1";

/// One event of every kind a CeleRS worker can publish, built exactly the way
/// `celers-worker` builds them.
fn worker_emitted_events() -> Vec<Event> {
    let task_id = Uuid::new_v4();
    let builder = || {
        TaskEventBuilder::new(task_id, "tasks.add")
            .hostname(HOSTNAME)
            .pid(4242)
    };

    vec![
        builder().sent("celery"),
        builder().received(),
        builder().started(),
        builder().succeeded(1.25),
        builder().failed("ValueError('bad input')"),
        builder().retried("Timeout", 2),
        builder().soft_time_limit_exceeded(Duration::from_millis(30_500), Duration::from_secs(30)),
        Event::Task(TaskEvent::Revoked {
            task_id,
            task_name: Some("tasks.add".to_string()),
            hostname: HOSTNAME.to_string(),
            timestamp: celers_core::event::event_timestamp_now(),
            terminated: true,
            signum: Some(9),
            expired: false,
        }),
        Event::Task(TaskEvent::Rejected {
            task_id,
            task_name: Some("tasks.add".to_string()),
            hostname: HOSTNAME.to_string(),
            timestamp: celers_core::event::event_timestamp_now(),
            reason: "queue full".to_string(),
        }),
        WorkerEventBuilder::new(HOSTNAME).online(),
        WorkerEventBuilder::new(HOSTNAME).heartbeat(3, 100, [1.0, 0.8, 0.5], 2.0),
        WorkerEventBuilder::new(HOSTNAME).offline(),
    ]
}

/// Events with every varying part pinned, so their wire rendering is a
/// constant that can be compared byte for byte.
fn golden_events() -> Vec<Event> {
    use celers_core::event::WorkerEvent;
    let task_id: Uuid = "7b1a0d1e-0000-4000-8000-000000000001"
        .parse()
        .expect("fixed task id");
    let at = |secs: i64, micros: u32| -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(secs, micros * 1000).expect("valid instant")
    };
    vec![
        Event::Task(TaskEvent::Sent {
            task_id,
            task_name: "tasks.add".to_string(),
            queue: "celery".to_string(),
            timestamp: at(1_774_000_000, 100),
            args: None,
            kwargs: None,
            eta: None,
            expires: None,
            retries: None,
        }),
        Event::Task(TaskEvent::Received {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_000, 200),
            pid: 4242,
        }),
        Event::Task(TaskEvent::Started {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_000, 300),
            pid: 4242,
        }),
        Event::Task(TaskEvent::Succeeded {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_000, 400),
            runtime: 1.25,
            result: None,
        }),
        Event::Task(TaskEvent::Failed {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_001, 0),
            exception: "ValueError('bad input')".to_string(),
            traceback: None,
        }),
        Event::Task(TaskEvent::Retried {
            task_id,
            task_name: "tasks.add".to_string(),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_001, 100),
            exception: "Timeout".to_string(),
            retries: 2,
        }),
        Event::Task(TaskEvent::Revoked {
            task_id,
            task_name: Some("tasks.add".to_string()),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_001, 200),
            terminated: true,
            signum: Some(9),
            expired: false,
        }),
        Event::Task(TaskEvent::Rejected {
            task_id,
            task_name: Some("tasks.add".to_string()),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_001, 300),
            reason: "queue full".to_string(),
        }),
        Event::Task(TaskEvent::SoftTimeLimitExceeded {
            task_id,
            task_name: "tasks.slow".to_string(),
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_001, 400),
            elapsed_secs: 30.5,
            limit_secs: 30.0,
        }),
        Event::Worker(WorkerEvent::Online {
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_002, 0),
            sw_ident: "celers".to_string(),
            sw_ver: "0.3.1".to_string(),
            sw_sys: "Linux".to_string(),
        }),
        Event::Worker(WorkerEvent::Heartbeat {
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_002, 100),
            active: 3,
            processed: 100,
            loadavg: Some([1.0, 0.8, 0.5]),
            freq: 2.0,
        }),
        Event::Worker(WorkerEvent::Offline {
            hostname: HOSTNAME.to_string(),
            timestamp: at(1_774_000_002, 200),
        }),
    ]
}

/// The wire payload for an event, exactly as the transports publish it.
fn wire(event: &Event) -> String {
    event
        .to_wire_json()
        .unwrap_or_else(|e| panic!("{} should render to the wire: {e}", event.event_type()))
}

// =============================================================================
// The wire contract
// =============================================================================

#[test]
fn every_worker_event_parses_as_a_celery_event_message() {
    for event in worker_emitted_events() {
        let payload = wire(&event);
        let message = protocol::EventMessage::from_json(payload.as_bytes())
            .unwrap_or_else(|e| panic!("{} should parse as EventMessage: {e}", event.event_type()));

        assert_eq!(
            message.get_type(),
            event.event_type(),
            "wire `type` must match the event"
        );
        assert!(
            message.timestamp > 1_700_000_000.0,
            "{}: `timestamp` must be float Unix seconds, got {}",
            event.event_type(),
            message.timestamp
        );
        assert_eq!(
            message.utcoffset,
            Some(0),
            "{}: CeleRS timestamps are UTC",
            event.event_type()
        );
        assert!(
            message.clock.is_some(),
            "{}: a monitor needs `clock` to order events",
            event.event_type()
        );
        assert!(
            message.pid.is_some(),
            "{}: `pid` identifies the emitting process",
            event.event_type()
        );

        // The float timestamp must decode to the instant the worker recorded.
        assert_eq!(
            message.datetime(),
            Some(event.timestamp()),
            "{}: timestamp must survive the float",
            event.event_type()
        );

        // The internal field names must never reach a monitor.
        assert!(
            !message.fields.contains_key("task_id"),
            "{}: `task_id` leaked onto the wire (Celery calls it `uuid`)",
            event.event_type()
        );
        assert!(
            !message.fields.contains_key("task_name"),
            "{}: `task_name` leaked onto the wire (Celery calls it `name`)",
            event.event_type()
        );
    }
}

#[test]
fn task_events_populate_the_typed_celery_task_fields() {
    let task_id = Uuid::new_v4();
    let builder = || {
        TaskEventBuilder::new(task_id, "tasks.add")
            .hostname(HOSTNAME)
            .pid(4242)
    };

    // task-sent
    let event = builder().sent("celery");
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-sent");
    assert_eq!(parsed.base.event_type, "task-sent");
    assert_eq!(parsed.uuid, task_id);
    assert_eq!(parsed.name.as_deref(), Some("tasks.add"));
    assert_eq!(parsed.queue.as_deref(), Some("celery"));

    // task-received
    let event = builder().received();
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-received");
    assert_eq!(parsed.base.event_type, "task-received");
    assert_eq!(parsed.uuid, task_id);
    assert_eq!(parsed.base.hostname.as_deref(), Some(HOSTNAME));
    assert_eq!(parsed.base.pid, Some(4242));

    // task-started
    let event = builder().started();
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-started");
    assert_eq!(parsed.base.event_type, "task-started");
    assert_eq!(parsed.uuid, task_id);
    assert_eq!(parsed.base.pid, Some(4242));

    // task-succeeded
    let event = builder().succeeded(1.25);
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-succeeded");
    assert_eq!(parsed.base.event_type, "task-succeeded");
    assert_eq!(parsed.runtime, Some(1.25));

    // task-failed
    let event = builder().failed("ValueError('bad input')");
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-failed");
    assert_eq!(parsed.base.event_type, "task-failed");
    assert_eq!(parsed.exception.as_deref(), Some("ValueError('bad input')"));

    // task-retried
    let event = builder().retried("Timeout", 2);
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-retried");
    assert_eq!(parsed.base.event_type, "task-retried");
    assert_eq!(parsed.exception.as_deref(), Some("Timeout"));
    assert_eq!(parsed.retries, Some(2));

    // task-revoked
    let event = Event::Task(TaskEvent::Revoked {
        task_id,
        task_name: Some("tasks.add".to_string()),
        hostname: HOSTNAME.to_string(),
        timestamp: celers_core::event::event_timestamp_now(),
        terminated: true,
        signum: Some(9),
        expired: false,
    });
    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("task-revoked");
    assert_eq!(parsed.base.event_type, "task-revoked");
    assert_eq!(parsed.uuid, task_id);
    assert_eq!(
        parsed.base.fields.get("terminated"),
        Some(&Value::Bool(true))
    );
    assert_eq!(parsed.base.fields.get("signum"), Some(&Value::from(9)));
}

#[test]
fn a_worker_event_is_not_mistaken_for_a_task_event() {
    // `protocol::TaskEvent::uuid` is non-optional, so a worker payload must be
    // rejected outright rather than yielding a plausible nil-UUID task. Without
    // this the typed assertions above would prove much less than they look.
    let event = WorkerEventBuilder::new(HOSTNAME).heartbeat(3, 100, [1.0, 0.8, 0.5], 2.0);
    let outcome = protocol::TaskEvent::from_json(wire(&event).as_bytes());
    assert!(
        outcome.is_err(),
        "a worker-heartbeat must not parse as a task event: {outcome:?}"
    );

    let event = WorkerEventBuilder::new(HOSTNAME).online();
    assert!(protocol::TaskEvent::from_json(wire(&event).as_bytes()).is_err());
}

#[test]
fn worker_heartbeat_populates_the_typed_celery_worker_fields() {
    let event = WorkerEventBuilder::new(HOSTNAME).heartbeat(3, 100, [1.0, 0.8, 0.5], 2.0);
    let parsed = protocol::WorkerEvent::from_json(wire(&event).as_bytes()).expect("heartbeat");

    assert_eq!(parsed.base.event_type, "worker-heartbeat");
    assert_eq!(parsed.base.hostname.as_deref(), Some(HOSTNAME));
    assert_eq!(parsed.active, Some(3));
    assert_eq!(parsed.processed, Some(100));
    assert_eq!(parsed.loadavg, Some([1.0, 0.8, 0.5]));
    assert_eq!(parsed.freq, Some(2.0));

    let event = WorkerEventBuilder::new(HOSTNAME).online();
    let parsed = protocol::WorkerEvent::from_json(wire(&event).as_bytes()).expect("online");
    assert_eq!(parsed.base.event_type, "worker-online");
    assert_eq!(parsed.software_identity.as_deref(), Some("celers"));
    assert!(parsed.software_version.is_some());
    assert!(parsed.software_system.is_some());
}

#[test]
fn soft_time_limit_event_is_a_recognised_task_event_type() {
    use std::str::FromStr;

    let event = TaskEventBuilder::new(Uuid::new_v4(), "tasks.slow")
        .hostname(HOSTNAME)
        .soft_time_limit_exceeded(Duration::from_millis(30_500), Duration::from_secs(30));

    let parsed = protocol::TaskEvent::from_json(wire(&event).as_bytes()).expect("soft limit event");
    assert_eq!(parsed.base.event_type, "task-soft-time-limit-exceeded");
    assert_eq!(parsed.name.as_deref(), Some("tasks.slow"));
    assert_eq!(
        parsed.base.fields.get("elapsed_secs"),
        Some(&Value::from(30.5))
    );
    assert_eq!(
        parsed.base.fields.get("limit_secs"),
        Some(&Value::from(30.0))
    );

    let event_type =
        protocol::EventType::from_str("task-soft-time-limit-exceeded").expect("infallible");
    assert_eq!(event_type, protocol::EventType::TaskSoftTimeLimitExceeded);
    assert!(event_type.is_task_event());
}

#[test]
fn the_wire_payload_round_trips_back_into_the_typed_model() {
    for event in worker_emitted_events() {
        let payload = wire(&event);
        let parsed = Event::from_wire_str(&payload)
            .unwrap_or_else(|e| panic!("{} should parse back: {e}", event.event_type()));
        assert_eq!(
            parsed,
            event,
            "{} changed across the wire",
            event.event_type()
        );
    }
}

/// A mixed cluster publishes onto one `celeryev` channel, so the receiver has
/// to read a Python worker's events as readily as CeleRS' own.
#[test]
fn a_python_celery_payload_survives_the_receiver_path() {
    let python_task_started = r#"{"type":"task-started","uuid":"7b1a0d1e-0000-4000-8000-000000000001","hostname":"celery@py-worker","timestamp":1774000000.323456,"pid":42,"clock":3,"utcoffset":0}"#;

    // The protocol model reads it (that is what makes it a Celery event) ...
    let message = protocol::EventMessage::from_json(python_task_started.as_bytes())
        .expect("a Python Celery event is an EventMessage");
    assert_eq!(message.get_type(), "task-started");
    assert_eq!(message.clock, Some(3));

    // ... and so does the typed model the receiver hands to its handler.
    let event = Event::from_wire_str(python_task_started)
        .expect("the receiver must accept a Python Celery event");
    assert_eq!(event.event_type(), "task-started");
    assert_eq!(event.hostname(), Some("celery@py-worker"));
    assert_eq!(
        event.task_id().map(|id| id.to_string()).as_deref(),
        Some("7b1a0d1e-0000-4000-8000-000000000001")
    );
}

#[test]
fn the_emitter_stamps_a_fallback_hostname_on_events_that_lack_one() {
    // `task-sent` carries no hostname of its own, so a monitor can only tell
    // which node published it if the transport supplies one.
    let config = RedisEventConfig::new().hostname("celery@publisher");
    assert_eq!(config.hostname.as_deref(), Some("celery@publisher"));

    let envelope = celers_core::event::EventEnvelope::stamp().with_hostname("celery@publisher");
    let event = TaskEventBuilder::new(Uuid::new_v4(), "tasks.add").sent("celery");
    let payload = event.to_wire_json_with(&envelope).expect("renders");

    let message = protocol::EventMessage::from_json(payload.as_bytes()).expect("parses");
    assert_eq!(message.hostname.as_deref(), Some("celery@publisher"));

    // An event that knows its own hostname keeps it.
    let event = WorkerEventBuilder::new(HOSTNAME).offline();
    let payload = event.to_wire_json_with(&envelope).expect("renders");
    let message = protocol::EventMessage::from_json(payload.as_bytes()).expect("parses");
    assert_eq!(message.hostname.as_deref(), Some(HOSTNAME));
}

#[test]
fn the_logical_clock_orders_events_published_back_to_back() {
    let events = worker_emitted_events();
    let mut clocks = Vec::with_capacity(events.len());
    for event in &events {
        let message = protocol::EventMessage::from_json(wire(event).as_bytes()).expect("parses");
        clocks.push(message.clock.expect("clock is always stamped"));
    }

    assert!(
        clocks.windows(2).all(|pair| pair[1] > pair[0]),
        "the logical clock must strictly increase across publishes: {clocks:?}"
    );
}

// =============================================================================
// Integration tests (require a live Redis)
// =============================================================================

/// What a Celery monitor subscribed to `celeryev` actually receives.
#[tokio::test]
#[ignore]
async fn published_events_reach_subscribers_in_the_celery_wire_shape() {
    use futures_util::StreamExt;

    let channel = format!("celers-test-celeryev-{}", Uuid::new_v4());
    let config = RedisEventConfig::new()
        .channel(channel.clone())
        .publish_to_type_channels(false)
        .hostname("celery@publisher");

    let emitter =
        RedisEventEmitter::with_config(&redis_url(), config).expect("event emitter connects");

    let client = redis::Client::open(redis_url()).expect("redis client");
    let mut pubsub = client.get_async_pubsub().await.expect("pubsub connection");
    pubsub.subscribe(&channel).await.expect("subscribe");

    let events = worker_emitted_events();
    let expected = events.len();

    let publisher = tokio::spawn(async move {
        // Give the subscription a moment to register before publishing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        for event in events {
            emitter.emit(event).await.expect("publish succeeds");
        }
    });

    let mut stream = pubsub.on_message();
    let mut received = Vec::with_capacity(expected);
    while received.len() < expected {
        let message = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("a subscriber should receive every published event")
            .expect("the pub/sub stream should stay open");
        let payload: String = message.get_payload().expect("payload is a string");
        received.push(payload);
    }
    publisher.await.expect("publisher task");

    for payload in &received {
        // A Celery monitor parses the raw payload straight off the channel.
        let message = protocol::EventMessage::from_json(payload.as_bytes())
            .unwrap_or_else(|e| panic!("payload is not a Celery event: {e}\n{payload}"));
        assert!(message.clock.is_some());
        assert_eq!(message.utcoffset, Some(0));
        assert!(message.timestamp > 1_700_000_000.0);
        assert!(message.hostname.is_some());

        // ...and CeleRS reads its own typed model back out of it.
        let event = Event::from_wire_str(payload).expect("round trips into the typed model");
        assert_eq!(event.event_type(), message.get_type());
    }

    let types: Vec<String> = received
        .iter()
        .filter_map(|payload| {
            protocol::EventMessage::from_json(payload.as_bytes())
                .ok()
                .map(|message| message.event_type)
        })
        .collect();

    for expected_type in [
        "task-sent",
        "task-received",
        "task-started",
        "task-succeeded",
        "task-failed",
        "task-retried",
        "task-revoked",
        "task-rejected",
        "task-soft-time-limit-exceeded",
        "worker-online",
        "worker-offline",
        "worker-heartbeat",
    ] {
        assert!(
            types.iter().any(|seen| seen == expected_type),
            "{expected_type} never reached the subscriber: {types:?}"
        );
    }
}

// =============================================================================
// Outbound goldens: the exact bytes a Celery monitor receives
// =============================================================================

/// The envelope every outbound golden is rendered with.
///
/// `EventEnvelope::stamp()` reads a process-wide logical clock and the real
/// pid, so a golden has to pin both or it could never be a constant.
fn golden_envelope() -> celers_core::event::EventEnvelope {
    celers_core::event::EventEnvelope::stamp()
        .with_clock(7)
        .with_pid(4242)
        .with_hostname("celery@publisher")
}

/// What each event family renders to, byte for byte.
///
/// Every string here was produced by this code and then read: they are a
/// *snapshot*, not an independent source of truth, and that is precisely their
/// job. The inbound direction is pinned by verbatim Python captures
/// (`celers-core/src/event/wire.rs`, `python_celery_events_parse_into_the_typed_model`);
/// what was missing was any assertion that an added, renamed or dropped
/// **outbound** key would be noticed. `every_worker_event_parses_as_a_celery_event_message`
/// cannot notice: it serializes with CeleRS and parses with CeleRS, so both
/// sides move together.
///
/// A diff here means the wire contract changed. That is allowed -- but it must
/// be a decision, checked against what a Python monitor reads, not a side
/// effect of an unrelated edit.
const OUTBOUND_GOLDENS: &[(&str, &str)] = &[
    (
        "task-sent",
        r#"{"clock":7,"hostname":"celery@publisher","name":"tasks.add","pid":4242,"queue":"celery","timestamp":1774000000.0001,"type":"task-sent","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-received",
        r#"{"clock":7,"hostname":"celery@worker-1","name":"tasks.add","pid":4242,"timestamp":1774000000.0002,"type":"task-received","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-started",
        r#"{"clock":7,"hostname":"celery@worker-1","name":"tasks.add","pid":4242,"timestamp":1774000000.0003,"type":"task-started","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-succeeded",
        r#"{"clock":7,"hostname":"celery@worker-1","name":"tasks.add","pid":4242,"runtime":1.25,"timestamp":1774000000.0004,"type":"task-succeeded","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-failed",
        r#"{"clock":7,"exception":"ValueError('bad input')","hostname":"celery@worker-1","name":"tasks.add","pid":4242,"timestamp":1774000001.0,"type":"task-failed","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-retried",
        r#"{"clock":7,"exception":"Timeout","hostname":"celery@worker-1","name":"tasks.add","pid":4242,"retries":2,"timestamp":1774000001.0001,"type":"task-retried","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-revoked",
        r#"{"clock":7,"expired":false,"hostname":"celery@worker-1","name":"tasks.add","pid":4242,"signum":9,"terminated":true,"timestamp":1774000001.0002,"type":"task-revoked","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-rejected",
        r#"{"clock":7,"hostname":"celery@worker-1","name":"tasks.add","pid":4242,"reason":"queue full","timestamp":1774000001.0003,"type":"task-rejected","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "task-soft-time-limit-exceeded",
        r#"{"clock":7,"elapsed_secs":30.5,"hostname":"celery@worker-1","limit_secs":30.0,"name":"tasks.slow","pid":4242,"timestamp":1774000001.0004,"type":"task-soft-time-limit-exceeded","utcoffset":0,"uuid":"7b1a0d1e-0000-4000-8000-000000000001"}"#,
    ),
    (
        "worker-online",
        r#"{"clock":7,"hostname":"celery@worker-1","pid":4242,"sw_ident":"celers","sw_sys":"Linux","sw_ver":"0.3.1","timestamp":1774000002.0,"type":"worker-online","utcoffset":0}"#,
    ),
    (
        "worker-heartbeat",
        r#"{"active":3,"clock":7,"freq":2.0,"hostname":"celery@worker-1","loadavg":[1.0,0.8,0.5],"pid":4242,"processed":100,"timestamp":1774000002.0001,"type":"worker-heartbeat","utcoffset":0}"#,
    ),
    (
        "worker-offline",
        r#"{"clock":7,"hostname":"celery@worker-1","pid":4242,"timestamp":1774000002.0002,"type":"worker-offline","utcoffset":0}"#,
    ),
];

#[test]
fn every_outbound_event_renders_to_its_golden_bytes() {
    let events = golden_events();
    assert_eq!(
        events.len(),
        OUTBOUND_GOLDENS.len(),
        "every event family needs a golden; add one for the new variant"
    );

    for (event, (expected_type, expected)) in events.iter().zip(OUTBOUND_GOLDENS) {
        assert_eq!(
            &event.event_type(),
            expected_type,
            "the goldens are out of order with `golden_events`"
        );
        let rendered = event
            .to_wire_json_with(&golden_envelope())
            .unwrap_or_else(|e| panic!("{expected_type} should render: {e}"));
        assert_eq!(
            rendered, *expected,
            "{expected_type} no longer renders to the bytes a Celery monitor was \
             pinned against. If the change is intended, verify against a real \
             monitor and update the golden."
        );
    }
}

/// The goldens must stay parseable, or they would pin a shape nothing reads.
///
/// This is the guard against "fixed the diff, broke the contract": a golden
/// updated carelessly still has to satisfy the typed model in both directions.
#[test]
fn the_golden_bytes_are_still_celery_events() {
    for (expected_type, payload) in OUTBOUND_GOLDENS {
        let message = protocol::EventMessage::from_json(payload.as_bytes())
            .unwrap_or_else(|e| panic!("{expected_type} golden is not a Celery event: {e}"));
        assert_eq!(message.get_type(), *expected_type);
        assert_eq!(message.clock, Some(7));
        assert_eq!(message.pid, Some(4242));
        assert_eq!(message.utcoffset, Some(0));
        assert!(
            message.hostname.is_some(),
            "{expected_type} needs a hostname"
        );

        let event = Event::from_wire_str(payload)
            .unwrap_or_else(|e| panic!("{expected_type} golden does not parse back: {e}"));
        assert_eq!(event.event_type(), *expected_type);
    }
}

/// The key names, spelled out. A golden diff shows *that* something changed;
/// this says which names a Python monitor actually indexes.
#[test]
fn the_golden_bytes_use_celerys_key_names() {
    for (expected_type, payload) in OUTBOUND_GOLDENS {
        let value: Value = serde_json::from_str(payload).expect("golden is valid JSON");
        let object = value.as_object().expect("an event is a JSON object");

        for required in ["type", "timestamp", "clock", "utcoffset", "pid", "hostname"] {
            assert!(
                object.contains_key(required),
                "{expected_type}: a Celery monitor reads `{required}`"
            );
        }
        // Celery's names, not CeleRS' internal ones.
        for internal in ["task_id", "task_name", "event_type"] {
            assert!(
                !object.contains_key(internal),
                "{expected_type}: internal name `{internal}` reached the wire"
            );
        }
        if expected_type.starts_with("task-") {
            assert!(
                object.contains_key("uuid"),
                "{expected_type}: Celery identifies a task by `uuid`"
            );
        }
        // A float, not a string or an integer: Celery does arithmetic on it.
        assert!(
            object["timestamp"].is_f64(),
            "{expected_type}: `timestamp` must be float Unix seconds"
        );
    }
}
