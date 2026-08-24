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
