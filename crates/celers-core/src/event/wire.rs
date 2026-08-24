//! The Celery-compatible **wire** representation of [`Event`].
//!
//! [`Event`] and its [`TaskEvent`](super::TaskEvent) /
//! [`WorkerEvent`](super::WorkerEvent) variants are CeleRS' *typed internal
//! model*: strongly typed, `DateTime<Utc>` timestamps, Rust field names. That
//! model is what workers pass around in-process and what the event persisters
//! write to their own storage.
//!
//! It is **not** what a Celery monitor parses. `celery events`, Flower and
//! every other consumer of the Celery event stream expect the shape described
//! by `celers_protocol::event::EventMessage`:
//!
//! | wire field   | type                | source in the typed model            |
//! |--------------|---------------------|--------------------------------------|
//! | `type`       | string              | [`Event::event_type`]                |
//! | `timestamp`  | float Unix seconds  | [`Event::timestamp`]                 |
//! | `uuid`       | string (UUID)       | `task_id`                            |
//! | `name`       | string              | `task_name`                          |
//! | `hostname`   | string              | `hostname`, else the envelope        |
//! | `pid`        | integer             | `pid`, else the envelope             |
//! | `clock`      | integer             | the process-wide logical clock       |
//! | `utcoffset`  | integer (hours)     | the envelope (always `0`, see below) |
//!
//! Everything else (`args`, `kwargs`, `retries`, `eta`, `expires`, `queue`,
//! `runtime`, `result`, `exception`, `traceback`, `terminated`, `signum`,
//! `expired`, `sw_ident`, `sw_ver`, `sw_sys`, `active`, `processed`,
//! `loadavg`, `freq`) already uses Celery's own field names and travels
//! through unchanged.
//!
//! This module is the single bridge between the two: [`Event::to_wire_value`]
//! and friends render the wire shape, [`Event::from_wire_value`] and friends
//! parse it back. The conversion is lossless for every event produced by the
//! builders in this crate, whose timestamps carry the microsecond resolution a
//! float Unix timestamp can represent exactly (see
//! [`event_timestamp_now`]).
//!
//! # Reading a Python Celery worker's events
//!
//! The parse direction also accepts events published by a real Python Celery
//! worker, which matters because CeleRS' event receivers subscribe to
//! `celeryev` — the very channel such a worker publishes to. Celery is
//! sparser than CeleRS on two points, and the typed model fills those gaps
//! with documented defaults rather than rejecting the event:
//!
//! * Celery repeats `name` only on `task-sent` and `task-received`; later
//!   events identify the task by `uuid` alone, so `task_name` comes back
//!   empty;
//! * Celery's `task-retried` carries no retry count, so `retries` comes back
//!   `0`, and its `task-rejected` reports `requeue` rather than a reason, so
//!   `reason` comes back empty.
//!
//! Events CeleRS itself publishes always carry all three, so this never
//! weakens a CeleRS-to-CeleRS round trip.
//!
//! # UTC offset
//!
//! CeleRS always timestamps events in UTC, so the `utcoffset` it publishes is
//! always `0`: a receiver applying Celery's
//! `ts - (utcoffset - local_utcoffset) * 3600` adjustment recovers exactly the
//! instant the worker recorded. [`EventEnvelope::with_utcoffset`] exists for
//! bridges that re-publish events captured from a non-UTC producer.
//!
//! # Logical clock
//!
//! Celery stamps every event with a Lamport clock so a monitor can order
//! events whose wall-clock timestamps are unreliable (clock skew between
//! workers, two events inside the same millisecond). [`forward_event_clock`]
//! implements the `forward()` half — one tick per emitted event — and
//! [`adjust_event_clock`] the `adjust()` half, for consumers folding a remote
//! worker's clock into their own.
//!
//! # Example
//!
//! ```rust
//! use celers_core::event::{Event, TaskEventBuilder};
//! use uuid::Uuid;
//!
//! let task_id = Uuid::new_v4();
//! let event = TaskEventBuilder::new(task_id, "tasks.add")
//!     .hostname("celery@worker-1")
//!     .pid(4242)
//!     .started();
//!
//! // What actually goes on the wire.
//! let wire = event.to_wire_value().expect("event renders to the wire shape");
//! assert_eq!(wire["type"], "task-started");
//! assert_eq!(wire["uuid"], task_id.to_string());
//! assert_eq!(wire["name"], "tasks.add");
//! assert!(wire["timestamp"].is_f64());
//! assert!(wire["clock"].is_u64());
//! assert_eq!(wire["utcoffset"], 0);
//!
//! // ...and back into the typed model, unchanged.
//! let parsed = Event::from_wire_value(&wire).expect("wire shape parses back");
//! assert_eq!(parsed, event);
//! ```

use super::Event;
use crate::error::{CelersError, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};

/// Wire name of the event type field.
pub const WIRE_TYPE: &str = "type";
/// Wire name of the task identifier field (Celery calls it `uuid`).
pub const WIRE_UUID: &str = "uuid";
/// Wire name of the task name field (Celery calls it `name`).
pub const WIRE_NAME: &str = "name";
/// Wire name of the float Unix timestamp field.
pub const WIRE_TIMESTAMP: &str = "timestamp";
/// Wire name of the logical clock field.
pub const WIRE_CLOCK: &str = "clock";
/// Wire name of the UTC offset field.
pub const WIRE_UTCOFFSET: &str = "utcoffset";
/// Wire name of the emitting process id field.
pub const WIRE_PID: &str = "pid";
/// Wire name of the emitting host field.
pub const WIRE_HOSTNAME: &str = "hostname";

/// Field name the typed model uses for the task identifier.
const INTERNAL_TASK_ID: &str = "task_id";
/// Field name the typed model uses for the task name.
const INTERNAL_TASK_NAME: &str = "task_name";

/// The UTC offset CeleRS publishes.
///
/// Event timestamps are always UTC, so the offset is always zero. See the
/// module documentation for why a receiver still needs the field.
pub const WIRE_UTC_OFFSET_HOURS: i32 = 0;

/// The process-wide logical (Lamport) clock stamped onto outgoing events.
static EVENT_CLOCK: AtomicU64 = AtomicU64::new(0);

/// Advance the process-wide logical clock and return the new value.
///
/// This is Celery's `LamportClock.forward()`: one tick per event leaving this
/// process, giving a monitor a total order over this worker's events even when
/// two of them share a wall-clock timestamp.
#[inline]
pub fn forward_event_clock() -> u64 {
    EVENT_CLOCK.fetch_add(1, Ordering::SeqCst).saturating_add(1)
}

/// Read the process-wide logical clock without advancing it.
#[inline]
#[must_use]
pub fn current_event_clock() -> u64 {
    EVENT_CLOCK.load(Ordering::SeqCst)
}

/// Fold a clock value observed on an incoming event into the local clock.
///
/// This is Celery's `LamportClock.adjust()`: the local clock becomes
/// `max(local, remote) + 1`, so an event this process emits after seeing a
/// remote event always sorts after it. Returns the new local value.
pub fn adjust_event_clock(remote: u64) -> u64 {
    let mut current = EVENT_CLOCK.load(Ordering::SeqCst);
    loop {
        let next = current.max(remote).saturating_add(1);
        match EVENT_CLOCK.compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

/// The per-emission metadata Celery carries outside the event's own fields.
///
/// A typed [`Event`] describes *what happened*; the envelope describes *who
/// published it and when in the logical order*. Transports stamp one envelope
/// per emitted event.
///
/// Build one with [`EventEnvelope::stamp`]. The [`Default`] impl deliberately
/// carries `clock: 0` and `pid: 0` — a monitor reads those as "process 0, clock
/// never advanced" — so it is only useful as a starting point for the `with_*`
/// setters (tests, bridges that supply their own clock), never as the envelope
/// for a live emission.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventEnvelope {
    /// Logical clock value used to order events (Celery's `clock`).
    pub clock: u64,
    /// Hours the timestamps are offset from UTC (Celery's `utcoffset`).
    pub utcoffset: i32,
    /// Process id of the emitting process.
    pub pid: u32,
    /// Host the event was published from, when the event does not carry one.
    pub hostname: Option<String>,
}

impl EventEnvelope {
    /// Stamp a fresh envelope: the next logical clock tick, this process' id,
    /// and a zero UTC offset.
    ///
    /// Each call advances the process-wide clock, so call it once per event
    /// actually put on a wire.
    #[must_use]
    pub fn stamp() -> Self {
        Self {
            clock: forward_event_clock(),
            utcoffset: WIRE_UTC_OFFSET_HOURS,
            pid: std::process::id(),
            hostname: None,
        }
    }

    /// Override the logical clock value.
    #[must_use]
    pub const fn with_clock(mut self, clock: u64) -> Self {
        self.clock = clock;
        self
    }

    /// Override the UTC offset, in hours.
    #[must_use]
    pub const fn with_utcoffset(mut self, utcoffset: i32) -> Self {
        self.utcoffset = utcoffset;
        self
    }

    /// Override the process id.
    #[must_use]
    pub const fn with_pid(mut self, pid: u32) -> Self {
        self.pid = pid;
        self
    }

    /// Set the fallback hostname, used only when the event carries none.
    #[must_use]
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }
}

/// Render a UTC timestamp as the float Unix seconds Celery puts on the wire.
///
/// The result carries microsecond resolution: an IEEE-754 double has ~0.2 µs
/// of resolution left at present-day epoch values, which is why
/// [`event_timestamp_now`] trims sub-microsecond digits before they can be
/// silently rounded away.
#[must_use]
pub fn to_wire_timestamp(timestamp: DateTime<Utc>) -> f64 {
    let seconds = timestamp.timestamp();
    let micros = timestamp.timestamp_subsec_micros();
    #[allow(clippy::cast_precision_loss)]
    let seconds = seconds as f64;
    seconds + f64::from(micros) / 1_000_000.0
}

/// Rebuild a UTC timestamp from Celery's float Unix seconds.
///
/// The value is rounded to the nearest microsecond, which recovers exactly the
/// instant [`to_wire_timestamp`] was handed as long as that instant had
/// microsecond resolution.
///
/// # Errors
///
/// Returns [`CelersError::Deserialization`] if the value is not finite or
/// falls outside the range `DateTime<Utc>` can represent.
pub fn from_wire_timestamp(seconds: f64) -> Result<DateTime<Utc>> {
    if !seconds.is_finite() {
        return Err(CelersError::Deserialization(format!(
            "event timestamp {seconds} is not a finite number"
        )));
    }
    let micros = (seconds * 1_000_000.0).round();
    #[allow(clippy::cast_possible_truncation)]
    let micros = micros as i64;
    DateTime::from_timestamp_micros(micros).ok_or_else(|| {
        CelersError::Deserialization(format!(
            "event timestamp {seconds} is outside the representable range"
        ))
    })
}

/// Trim a timestamp to the microsecond resolution the wire format carries.
///
/// Sub-microsecond digits cannot survive a float Unix timestamp, so trimming
/// them up front is what makes the wire round-trip exactly lossless instead of
/// merely accurate.
#[must_use]
pub fn truncate_to_wire_precision(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let sub_micro = timestamp.timestamp_subsec_nanos() % 1_000;
    if sub_micro == 0 {
        timestamp
    } else {
        timestamp - chrono::Duration::nanoseconds(i64::from(sub_micro))
    }
}

/// The current time, at the resolution the Celery wire format can carry.
///
/// Every event builder in this crate stamps its timestamp with this rather
/// than [`Utc::now`], so `event -> wire -> event` is an exact round trip.
#[must_use]
pub fn event_timestamp_now() -> DateTime<Utc> {
    truncate_to_wire_precision(Utc::now())
}

/// Move `map[from]` to `map[to]`, leaving an already-present `to` untouched.
fn rename_key(map: &mut Map<String, Value>, from: &str, to: &str) {
    if map.contains_key(to) {
        return;
    }
    if let Some(value) = map.remove(from) {
        map.insert(to.to_string(), value);
    }
}

impl Event {
    /// Render this event in the Celery wire format, stamping a fresh envelope.
    ///
    /// Each call advances the process-wide logical clock, so call it once per
    /// event actually published. Use [`Event::to_wire_value_with`] to publish
    /// the same event to several channels under one clock value.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the event cannot be rendered
    /// as JSON.
    pub fn to_wire_value(&self) -> Result<Value> {
        self.to_wire_value_with(&EventEnvelope::stamp())
    }

    /// Render this event in the Celery wire format using a caller-supplied
    /// envelope.
    ///
    /// Fields the event itself carries win over the envelope: an event that
    /// already knows its `hostname` and `pid` keeps them, and only the gaps are
    /// filled from the envelope.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the event cannot be rendered
    /// as JSON.
    pub fn to_wire_value_with(&self, envelope: &EventEnvelope) -> Result<Value> {
        let value = serde_json::to_value(self)
            .map_err(|e| CelersError::Serialization(format!("event to JSON: {e}")))?;
        let Value::Object(mut map) = value else {
            return Err(CelersError::Serialization(
                "event did not serialize into a JSON object".to_string(),
            ));
        };

        rename_key(&mut map, INTERNAL_TASK_ID, WIRE_UUID);
        rename_key(&mut map, INTERNAL_TASK_NAME, WIRE_NAME);

        // Celery timestamps are float Unix seconds, never RFC3339 strings.
        let timestamp = serde_json::Number::from_f64(to_wire_timestamp(self.timestamp()))
            .ok_or_else(|| {
                CelersError::Serialization(
                    "event timestamp is not representable as a JSON number".to_string(),
                )
            })?;
        map.insert(WIRE_TIMESTAMP.to_string(), Value::Number(timestamp));

        map.insert(WIRE_CLOCK.to_string(), Value::from(envelope.clock));
        map.insert(WIRE_UTCOFFSET.to_string(), Value::from(envelope.utcoffset));
        map.entry(WIRE_PID.to_string())
            .or_insert_with(|| Value::from(envelope.pid));
        if let Some(ref hostname) = envelope.hostname {
            map.entry(WIRE_HOSTNAME.to_string())
                .or_insert_with(|| Value::from(hostname.clone()));
        }

        Ok(Value::Object(map))
    }

    /// Render this event as a Celery wire JSON string.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the event cannot be rendered
    /// as JSON.
    pub fn to_wire_json(&self) -> Result<String> {
        self.to_wire_json_with(&EventEnvelope::stamp())
    }

    /// Render this event as a Celery wire JSON string using a caller-supplied
    /// envelope.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the event cannot be rendered
    /// as JSON.
    pub fn to_wire_json_with(&self, envelope: &EventEnvelope) -> Result<String> {
        serde_json::to_string(&self.to_wire_value_with(envelope)?)
            .map_err(|e| CelersError::Serialization(format!("event to JSON: {e}")))
    }

    /// Render this event as Celery wire JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the event cannot be rendered
    /// as JSON.
    pub fn to_wire_bytes(&self) -> Result<Vec<u8>> {
        self.to_wire_json().map(String::into_bytes)
    }

    /// Parse a Celery wire event back into the typed model.
    ///
    /// Envelope-only fields (`clock`, `utcoffset`) are dropped: they describe
    /// the transmission, not the event. Payloads still using the internal
    /// field names (`task_id`, `task_name`, an RFC3339 `timestamp`) are also
    /// accepted, so a consumer can read both shapes during a rollout.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Deserialization`] if the payload is not a JSON
    /// object, carries an unrepresentable timestamp, or does not match any
    /// known event.
    pub fn from_wire_value(value: &Value) -> Result<Self> {
        let Value::Object(source) = value else {
            return Err(CelersError::Deserialization(
                "wire event is not a JSON object".to_string(),
            ));
        };
        let mut map = source.clone();

        rename_key(&mut map, WIRE_UUID, INTERNAL_TASK_ID);
        rename_key(&mut map, WIRE_NAME, INTERNAL_TASK_NAME);

        if let Some(Value::Number(number)) = map.get(WIRE_TIMESTAMP) {
            let seconds = number.as_f64().ok_or_else(|| {
                CelersError::Deserialization(format!(
                    "wire event timestamp {number} is not a number"
                ))
            })?;
            let timestamp = from_wire_timestamp(seconds)?;
            map.insert(
                WIRE_TIMESTAMP.to_string(),
                Value::String(timestamp.to_rfc3339_opts(SecondsFormat::Micros, true)),
            );
        }

        // Transmission metadata, not part of the typed model. `pid` is kept:
        // the received/started events carry it as a real field.
        map.remove(WIRE_CLOCK);
        map.remove(WIRE_UTCOFFSET);

        let event_type = source
            .get(WIRE_TYPE)
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_string();

        serde_json::from_value(Value::Object(map))
            .map_err(|e| CelersError::Deserialization(format!("wire event '{event_type}': {e}")))
    }

    /// Parse a Celery wire event from a JSON string.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Deserialization`] if the string is not valid
    /// JSON or does not describe a known event.
    pub fn from_wire_str(json: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(json)
            .map_err(|e| CelersError::Deserialization(format!("wire event JSON: {e}")))?;
        Self::from_wire_value(&value)
    }

    /// Parse a Celery wire event from JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Deserialization`] if the bytes are not valid
    /// JSON or do not describe a known event.
    pub fn from_wire_slice(json: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(json)
            .map_err(|e| CelersError::Deserialization(format!("wire event JSON: {e}")))?;
        Self::from_wire_value(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{TaskEvent, TaskEventBuilder, WorkerEvent, WorkerEventBuilder};
    use uuid::Uuid;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("valid RFC3339 fixture")
            .with_timezone(&Utc)
    }

    /// Every event a worker can publish, with fixed microsecond timestamps so
    /// the round trip can be asserted for exact equality.
    fn every_event() -> Vec<Event> {
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

    #[test]
    fn wire_round_trip_is_lossless_for_every_event() {
        // A fully populated envelope too: its `hostname` and `pid` are extra
        // keys on events that have no such field, and must be discarded on the
        // way back rather than corrupting the typed model.
        let envelope = EventEnvelope::stamp()
            .with_hostname("celery@publisher")
            .with_utcoffset(-9);

        for event in every_event() {
            for json in [
                event
                    .to_wire_json()
                    .unwrap_or_else(|e| panic!("{} should render: {e}", event.event_type())),
                event
                    .to_wire_json_with(&envelope)
                    .unwrap_or_else(|e| panic!("{} should render: {e}", event.event_type())),
            ] {
                let parsed = Event::from_wire_str(&json)
                    .unwrap_or_else(|e| panic!("{} should parse back: {e}", event.event_type()));
                assert_eq!(parsed, event, "round trip changed {}", event.event_type());
            }
        }
    }

    #[test]
    fn builder_timestamps_survive_the_wire_exactly() {
        // Builders trim sub-microsecond digits precisely so this holds.
        let events = vec![
            TaskEventBuilder::new(Uuid::new_v4(), "tasks.add")
                .hostname("celery@w")
                .pid(7)
                .started(),
            TaskEventBuilder::new(Uuid::new_v4(), "tasks.add").sent("celery"),
            WorkerEventBuilder::new("celery@w").heartbeat(1, 2, [0.1, 0.2, 0.3], 2.0),
            WorkerEventBuilder::new("celery@w").online(),
        ];
        for event in events {
            let json = event.to_wire_json().expect("renders");
            let parsed = Event::from_wire_str(&json).expect("parses");
            assert_eq!(parsed.timestamp(), event.timestamp());
            assert_eq!(parsed, event);
        }
    }

    #[test]
    fn byte_oriented_helpers_match_the_string_ones() {
        // Transports that write bytes must get the same payload as those that
        // write strings.
        let event = TaskEventBuilder::new(Uuid::new_v4(), "tasks.add")
            .hostname("celery@w")
            .pid(7)
            .started();

        let bytes = event.to_wire_bytes().expect("renders");
        let parsed = Event::from_wire_slice(&bytes).expect("parses");
        assert_eq!(parsed, event);

        let value: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(value[WIRE_TYPE], "task-started");
        assert!(value[WIRE_CLOCK].is_u64());
    }

    #[test]
    fn wire_uses_celery_field_names() {
        let task_id = Uuid::new_v4();
        let event = TaskEventBuilder::new(task_id, "tasks.add")
            .hostname("celery@worker-1")
            .pid(99)
            .succeeded(2.5);

        let wire = event.to_wire_value().expect("renders");

        assert_eq!(wire[WIRE_TYPE], "task-succeeded");
        assert_eq!(wire[WIRE_UUID], task_id.to_string());
        assert_eq!(wire[WIRE_NAME], "tasks.add");
        assert_eq!(wire[WIRE_HOSTNAME], "celery@worker-1");
        assert_eq!(wire[WIRE_UTCOFFSET], 0);
        assert_eq!(wire["runtime"], 2.5);
        assert!(wire[WIRE_TIMESTAMP].is_f64(), "timestamp must be a float");
        assert!(wire[WIRE_CLOCK].is_u64(), "clock must be an integer");
        // task-succeeded has no pid of its own, so the envelope supplies one.
        assert_eq!(wire[WIRE_PID], std::process::id());

        // The internal names must not leak onto the wire.
        let object = wire.as_object().expect("wire event is an object");
        assert!(!object.contains_key("task_id"));
        assert!(!object.contains_key("task_name"));

        // An event that does carry a pid keeps it.
        let started = TaskEventBuilder::new(task_id, "tasks.add")
            .hostname("celery@worker-1")
            .pid(99)
            .started();
        let wire = started.to_wire_value().expect("renders");
        assert_eq!(wire[WIRE_PID], 99);
        assert_eq!(wire[WIRE_UUID], task_id.to_string());
    }

    #[test]
    fn eta_and_expires_stay_iso8601_strings() {
        // Only `timestamp` is a float in Celery's event stream; `eta` and
        // `expires` are ISO-8601 strings.
        let event = Event::Task(TaskEvent::Sent {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            queue: "celery".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            args: None,
            kwargs: None,
            eta: Some(at("2026-03-04T05:10:00Z")),
            expires: Some(at("2026-03-04T06:00:00Z")),
            retries: None,
        });

        let wire = event.to_wire_value().expect("renders");
        assert!(wire["eta"].is_string(), "eta must stay an ISO-8601 string");
        assert!(
            wire["expires"].is_string(),
            "expires must stay an ISO-8601 string"
        );
    }

    #[test]
    fn envelope_fills_only_the_gaps() {
        let envelope = EventEnvelope::default()
            .with_clock(77)
            .with_pid(1234)
            .with_hostname("fallback-host");

        // Worker events carry a hostname but no pid.
        let event = WorkerEventBuilder::new("celery@real-host").offline();
        let wire = event.to_wire_value_with(&envelope).expect("renders");
        assert_eq!(wire[WIRE_HOSTNAME], "celery@real-host");
        assert_eq!(wire[WIRE_PID], 1234);
        assert_eq!(wire[WIRE_CLOCK], 77);

        // task-sent carries neither, so both come from the envelope.
        let sent = TaskEventBuilder::new(Uuid::new_v4(), "tasks.add").sent("celery");
        let wire = sent.to_wire_value_with(&envelope).expect("renders");
        assert_eq!(wire[WIRE_HOSTNAME], "fallback-host");
        assert_eq!(wire[WIRE_PID], 1234);
    }

    #[test]
    fn clock_advances_once_per_rendered_event() {
        let event = WorkerEventBuilder::new("celery@w").offline();

        let first = event.to_wire_value().expect("renders")[WIRE_CLOCK]
            .as_u64()
            .expect("clock is an integer");
        let second = event.to_wire_value().expect("renders")[WIRE_CLOCK]
            .as_u64()
            .expect("clock is an integer");

        assert!(
            second > first,
            "logical clock must advance ({first} -> {second})"
        );
        assert!(current_event_clock() >= second);
    }

    #[test]
    fn adjust_clock_follows_a_remote_worker() {
        let remote = current_event_clock() + 5_000;
        let local = adjust_event_clock(remote);
        assert_eq!(local, remote + 1);
        assert!(forward_event_clock() > local);
    }

    /// Events captured from a real Python Celery 5.x worker, verbatim.
    ///
    /// `celers_backend_redis::event_transport::RedisEventReceiver` subscribes
    /// to `celeryev` — the very channel a Python worker publishes to — so in a
    /// mixed cluster these arrive whether or not anyone planned for it.
    #[test]
    fn python_celery_events_parse_into_the_typed_model() {
        let cases: [(&str, &str); 7] = [
            (
                "task-sent",
                r#"{"type":"task-sent","uuid":"7b1a0d1e-0000-4000-8000-000000000001","name":"tasks.add","args":"(1, 2)","kwargs":"{}","retries":0,"eta":null,"expires":null,"queue":"celery","exchange":"","routing_key":"celery","root_id":null,"parent_id":null,"hostname":"gen12345@client","timestamp":1774000000.123456,"pid":12345,"clock":1,"utcoffset":0}"#,
            ),
            (
                "task-received",
                r#"{"type":"task-received","uuid":"7b1a0d1e-0000-4000-8000-000000000001","name":"tasks.add","args":"(1, 2)","kwargs":"{}","retries":0,"eta":null,"hostname":"celery@worker-1","timestamp":1774000000.223456,"pid":42,"clock":2,"utcoffset":0}"#,
            ),
            (
                // From here on Celery stops repeating `name`.
                "task-started",
                r#"{"type":"task-started","uuid":"7b1a0d1e-0000-4000-8000-000000000001","hostname":"celery@worker-1","timestamp":1774000000.323456,"pid":42,"clock":3,"utcoffset":0}"#,
            ),
            (
                "task-succeeded",
                r#"{"type":"task-succeeded","uuid":"7b1a0d1e-0000-4000-8000-000000000001","result":"3","runtime":0.0102,"hostname":"celery@worker-1","timestamp":1774000000.423456,"pid":42,"clock":4,"utcoffset":0}"#,
            ),
            (
                "task-failed",
                r#"{"type":"task-failed","uuid":"7b1a0d1e-0000-4000-8000-000000000002","exception":"ValueError('bad')","traceback":"Traceback (most recent call last):\n","hostname":"celery@worker-1","timestamp":1774000001.023456,"pid":42,"clock":5,"utcoffset":0}"#,
            ),
            (
                // Celery's task-retried carries no retry count.
                "task-retried",
                r#"{"type":"task-retried","uuid":"7b1a0d1e-0000-4000-8000-000000000002","exception":"Timeout()","traceback":"Traceback (most recent call last):\n","hostname":"celery@worker-1","timestamp":1774000001.123456,"pid":42,"clock":6,"utcoffset":0}"#,
            ),
            (
                "worker-heartbeat",
                r#"{"type":"worker-heartbeat","hostname":"celery@worker-1","timestamp":1774000002.0,"pid":42,"clock":7,"utcoffset":0,"freq":2.0,"active":0,"processed":2,"loadavg":[0.1,0.2,0.3],"sw_ident":"py-celery","sw_ver":"5.3.6","sw_sys":"Linux"}"#,
            ),
        ];

        for (event_type, json) in cases {
            let event = Event::from_wire_str(json)
                .unwrap_or_else(|e| panic!("Celery {event_type} should parse: {e}"));
            assert_eq!(event.event_type(), event_type);
        }

        // The fields Celery omits come back at their documented defaults.
        let started = Event::from_wire_str(cases[2].1).expect("task-started");
        let Event::Task(TaskEvent::Started { ref task_name, .. }) = started else {
            panic!("task-started maps onto TaskEvent::Started");
        };
        assert_eq!(
            task_name, "",
            "an inbound Celery event has no name; a monitor correlates by uuid"
        );

        let retried = Event::from_wire_str(cases[5].1).expect("task-retried");
        let Event::Task(TaskEvent::Retried { retries, .. }) = retried else {
            panic!("task-retried maps onto TaskEvent::Retried");
        };
        assert_eq!(retries, 0, "Celery's task-retried carries no retry count");
    }

    #[test]
    fn from_wire_accepts_the_internal_shape() {
        // A payload written by an older CeleRS (internal names, RFC3339
        // timestamp) must still parse, so a rollout can read both.
        let json = r#"{
            "type": "task-started",
            "task_id": "00000000-0000-0000-0000-000000000001",
            "task_name": "tasks.add",
            "hostname": "celery@worker-1",
            "timestamp": "2026-03-04T05:06:07.123456Z",
            "pid": 42
        }"#;

        let event = Event::from_wire_str(json).expect("legacy payload parses");
        assert_eq!(event.event_type(), "task-started");
        assert_eq!(event.hostname(), Some("celery@worker-1"));
    }

    #[test]
    fn from_wire_rejects_garbage_with_a_useful_message() {
        let err = Event::from_wire_str(r#"{"type":"task-started"}"#)
            .expect_err("a task-started without a uuid cannot parse");
        assert!(
            err.to_string().contains("task-started"),
            "error should name the event type: {err}"
        );

        let err = Event::from_wire_str("[]").expect_err("an array is not an event");
        assert!(err.to_string().contains("not a JSON object"), "{err}");
    }

    #[test]
    fn timestamp_helpers_round_trip_at_microsecond_resolution() {
        let timestamp = at("2026-03-04T05:06:07.123456Z");
        let seconds = to_wire_timestamp(timestamp);
        assert_eq!(
            from_wire_timestamp(seconds).expect("in range"),
            timestamp,
            "microsecond timestamps must survive the float exactly"
        );

        // Pre-epoch instants are negative floats and must still round trip.
        let ancient = at("1969-07-20T20:17:40.000001Z");
        let seconds = to_wire_timestamp(ancient);
        assert!(seconds < 0.0);
        assert_eq!(from_wire_timestamp(seconds).expect("in range"), ancient);
    }

    #[test]
    fn truncate_drops_only_sub_microsecond_digits() {
        let exact = at("2026-03-04T05:06:07.123456Z");
        assert_eq!(truncate_to_wire_precision(exact), exact);

        let nanos = exact + chrono::Duration::nanoseconds(789);
        assert_eq!(truncate_to_wire_precision(nanos), exact);

        let now = event_timestamp_now();
        assert_eq!(now.timestamp_subsec_nanos() % 1_000, 0);
    }

    #[test]
    fn from_wire_timestamp_rejects_non_finite_values() {
        assert!(from_wire_timestamp(f64::NAN).is_err());
        assert!(from_wire_timestamp(f64::INFINITY).is_err());
        assert!(from_wire_timestamp(f64::NEG_INFINITY).is_err());
        assert!(from_wire_timestamp(1e30).is_err());
    }
}
