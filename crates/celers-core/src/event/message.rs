//! The typed bridge between [`Event`] and `celers_protocol`'s
//! [`EventMessage`].
//!
//! [`Event`] is CeleRS' internal model — Rust field names, `DateTime<Utc>`
//! timestamps, one enum variant per event. [`EventMessage`] is the Celery wire
//! model — `uuid`/`name`, a float Unix `timestamp`, and the
//! `clock`/`utcoffset`/`pid`/`hostname` envelope, with everything else
//! flattened alongside. Both directions of the conversion live here:
//!
//! * [`EventMessage::from(&event)`](EventMessage) — total, one arm per variant;
//! * [`Event::try_from(&message)`](Event) — fallible, because a message that
//!   arrived over a network can name an event type CeleRS does not know or
//!   omit a field the typed model requires.
//!
//! [`wire`](super::wire) is layered on top: it adds the envelope and renders
//! or parses the JSON. Nothing here touches JSON text.
//!
//! # Why typed
//!
//! This conversion used to be JSON-mediated — serialize the event, rename
//! `task_id` to `uuid` and `task_name` to `name` in the resulting map, patch
//! the timestamp — because celers-core could not depend on celers-protocol.
//! A string-keyed rename layer type-checks no matter what the two models
//! actually contain, so a field added to one side and not the other was
//! invisible until a monitor mis-parsed it in production. The `match` below
//! is exhaustive: adding a variant to [`Event`] stops this file compiling.
//!
//! The rendered bytes are unchanged by the switch, and
//! [`super::wire`]'s `wire_json_is_byte_for_byte_stable` fixture proves it.

use super::{Event, TaskEvent, WorkerEvent};
use crate::error::{CelersError, Result};
use celers_protocol::event::EventMessage;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use super::wire::{to_wire_timestamp, WIRE_NAME, WIRE_UUID};

/// Render a timestamp exactly the way `chrono`'s own `Serialize` impl does.
///
/// Used for the `eta`/`expires` fields, which stay ISO-8601 strings on the
/// wire (only `timestamp` becomes a float). Going through the string form
/// rather than `serde_json::to_value` keeps the conversion infallible;
/// `rfc3339_matches_chrono_serde` in this file's tests pins the two together.
fn rfc3339(at: DateTime<Utc>) -> Value {
    Value::String(at.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// Insert `key` only when there is a value for it.
///
/// The wire format omits absent optional fields rather than writing `null`,
/// which is what a Python Celery monitor expects.
fn put_opt<T: Into<Value>>(fields: &mut HashMap<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        fields.insert(key.to_string(), value.into());
    }
}

/// Insert `key` unconditionally.
fn put<T: Into<Value>>(fields: &mut HashMap<String, Value>, key: &str, value: T) {
    fields.insert(key.to_string(), value.into());
}

impl From<&Event> for EventMessage {
    fn from(event: &Event) -> Self {
        let mut fields: HashMap<String, Value> = HashMap::new();
        let mut hostname: Option<String> = None;
        let mut pid: Option<u32> = None;

        if let Some(task_id) = event.task_id() {
            put(&mut fields, WIRE_UUID, task_id.to_string());
        }

        match event {
            Event::Task(task) => match task {
                TaskEvent::Sent {
                    task_name,
                    queue,
                    args,
                    kwargs,
                    eta,
                    expires,
                    retries,
                    ..
                } => {
                    put(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "queue", queue.clone());
                    put_opt(&mut fields, "args", args.clone());
                    put_opt(&mut fields, "kwargs", kwargs.clone());
                    put_opt(&mut fields, "eta", eta.map(rfc3339));
                    put_opt(&mut fields, "expires", expires.map(rfc3339));
                    put_opt(&mut fields, "retries", *retries);
                }
                TaskEvent::Received {
                    task_name,
                    hostname: host,
                    pid: process,
                    ..
                }
                | TaskEvent::Started {
                    task_name,
                    hostname: host,
                    pid: process,
                    ..
                } => {
                    put(&mut fields, WIRE_NAME, task_name.clone());
                    hostname = Some(host.clone());
                    pid = Some(*process);
                }
                TaskEvent::Succeeded {
                    task_name,
                    hostname: host,
                    runtime,
                    result,
                    ..
                } => {
                    put(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "runtime", *runtime);
                    put_opt(&mut fields, "result", result.clone());
                    hostname = Some(host.clone());
                }
                TaskEvent::Failed {
                    task_name,
                    hostname: host,
                    exception,
                    traceback,
                    ..
                } => {
                    put(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "exception", exception.clone());
                    put_opt(&mut fields, "traceback", traceback.clone());
                    hostname = Some(host.clone());
                }
                TaskEvent::Retried {
                    task_name,
                    hostname: host,
                    exception,
                    retries,
                    ..
                } => {
                    put(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "exception", exception.clone());
                    put(&mut fields, "retries", *retries);
                    hostname = Some(host.clone());
                }
                TaskEvent::Revoked {
                    task_name,
                    terminated,
                    signum,
                    expired,
                    ..
                } => {
                    put_opt(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "terminated", *terminated);
                    put_opt(&mut fields, "signum", *signum);
                    put(&mut fields, "expired", *expired);
                }
                TaskEvent::Rejected {
                    task_name,
                    hostname: host,
                    reason,
                    ..
                } => {
                    put_opt(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "reason", reason.clone());
                    hostname = Some(host.clone());
                }
                TaskEvent::SoftTimeLimitExceeded {
                    task_name,
                    hostname: host,
                    elapsed_secs,
                    limit_secs,
                    ..
                } => {
                    put(&mut fields, WIRE_NAME, task_name.clone());
                    put(&mut fields, "elapsed_secs", *elapsed_secs);
                    put(&mut fields, "limit_secs", *limit_secs);
                    hostname = Some(host.clone());
                }
            },
            Event::Worker(worker) => match worker {
                WorkerEvent::Online {
                    hostname: host,
                    sw_ident,
                    sw_ver,
                    sw_sys,
                    ..
                } => {
                    put(&mut fields, "sw_ident", sw_ident.clone());
                    put(&mut fields, "sw_ver", sw_ver.clone());
                    put(&mut fields, "sw_sys", sw_sys.clone());
                    hostname = Some(host.clone());
                }
                WorkerEvent::Offline { hostname: host, .. } => {
                    hostname = Some(host.clone());
                }
                WorkerEvent::Heartbeat {
                    hostname: host,
                    active,
                    processed,
                    loadavg,
                    freq,
                    ..
                } => {
                    put(&mut fields, "active", *active);
                    put(&mut fields, "processed", *processed);
                    put_opt(
                        &mut fields,
                        "loadavg",
                        loadavg.map(|load| {
                            Value::Array(load.iter().copied().map(Value::from).collect())
                        }),
                    );
                    put(&mut fields, "freq", *freq);
                    hostname = Some(host.clone());
                }
            },
        }

        Self {
            event_type: event.event_type().to_string(),
            timestamp: to_wire_timestamp(event.timestamp()),
            hostname,
            // Envelope metadata. `Event` carries none of it — a typed event
            // says what happened, not who published it in which logical order
            // — so a transport fills these in from its `EventEnvelope`.
            utcoffset: None,
            pid,
            clock: None,
            fields,
        }
    }
}

/// Reads the flattened fields of an [`EventMessage`] with errors that name the
/// event type and the field, so a mis-shaped inbound event is diagnosable from
/// the message alone.
struct Reader<'a> {
    message: &'a EventMessage,
}

impl<'a> Reader<'a> {
    const fn new(message: &'a EventMessage) -> Self {
        Self { message }
    }

    fn event_type(&self) -> &str {
        &self.message.event_type
    }

    fn missing(&self, key: &str) -> CelersError {
        CelersError::Deserialization(format!(
            "wire event '{}': missing '{key}'",
            self.event_type()
        ))
    }

    fn invalid(&self, key: &str, expected: &str) -> CelersError {
        CelersError::Deserialization(format!(
            "wire event '{}': '{key}' is not {expected}",
            self.event_type()
        ))
    }

    fn get(&self, key: &str) -> Option<&'a Value> {
        self.message.fields.get(key)
    }

    fn opt_str(&self, key: &str) -> Result<Option<String>> {
        match self.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(_) => Err(self.invalid(key, "a string")),
        }
    }

    /// A required string.
    fn str(&self, key: &str) -> Result<String> {
        self.opt_str(key)?.ok_or_else(|| self.missing(key))
    }

    /// A string the typed model defaults to `""` when absent.
    ///
    /// Python Celery stops repeating `name` after `task-received`, so this is
    /// the difference between reading a mixed cluster and rejecting half of it.
    fn str_or_default(&self, key: &str) -> Result<String> {
        Ok(self.opt_str(key)?.unwrap_or_default())
    }

    fn uuid(&self) -> Result<Uuid> {
        let text = self.str(WIRE_UUID)?;
        Uuid::parse_str(&text).map_err(|e| {
            CelersError::Deserialization(format!(
                "wire event '{}': '{WIRE_UUID}' is not a UUID: {e}",
                self.event_type()
            ))
        })
    }

    fn opt_f64(&self, key: &str) -> Result<Option<f64>> {
        match self.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_f64()
                .map(Some)
                .ok_or_else(|| self.invalid(key, "a number")),
        }
    }

    fn f64(&self, key: &str) -> Result<f64> {
        self.opt_f64(key)?.ok_or_else(|| self.missing(key))
    }

    fn opt_u64(&self, key: &str) -> Result<Option<u64>> {
        match self.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_u64()
                .map(Some)
                .ok_or_else(|| self.invalid(key, "a non-negative integer")),
        }
    }

    fn u64(&self, key: &str) -> Result<u64> {
        self.opt_u64(key)?.ok_or_else(|| self.missing(key))
    }

    fn opt_u32(&self, key: &str) -> Result<Option<u32>> {
        match self.opt_u64(key)? {
            None => Ok(None),
            Some(value) => u32::try_from(value)
                .map(Some)
                .map_err(|_| self.invalid(key, "a 32-bit unsigned integer")),
        }
    }

    /// A `u32` the typed model defaults to `0` when absent (Celery's
    /// `task-retried` carries no retry count).
    fn u32_or_default(&self, key: &str) -> Result<u32> {
        Ok(self.opt_u32(key)?.unwrap_or(0))
    }

    fn opt_i32(&self, key: &str) -> Result<Option<i32>> {
        match self.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Some)
                .ok_or_else(|| self.invalid(key, "a 32-bit signed integer")),
        }
    }

    fn bool(&self, key: &str) -> Result<bool> {
        match self.get(key) {
            None | Some(Value::Null) => Err(self.missing(key)),
            Some(Value::Bool(flag)) => Ok(*flag),
            Some(_) => Err(self.invalid(key, "a boolean")),
        }
    }

    fn opt_datetime(&self, key: &str) -> Result<Option<DateTime<Utc>>> {
        let Some(text) = self.opt_str(key)? else {
            return Ok(None);
        };
        DateTime::parse_from_rfc3339(&text)
            .map(|at| Some(at.with_timezone(&Utc)))
            .map_err(|e| {
                CelersError::Deserialization(format!(
                    "wire event '{}': '{key}' is not an RFC3339 timestamp: {e}",
                    self.event_type()
                ))
            })
    }

    fn opt_loadavg(&self) -> Result<Option<[f64; 3]>> {
        let Some(value) = self.get("loadavg") else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        let Some(items) = value.as_array() else {
            return Err(self.invalid("loadavg", "an array"));
        };
        if items.len() != 3 {
            return Err(self.invalid("loadavg", "an array of exactly 3 numbers"));
        }
        let mut load = [0.0_f64; 3];
        for (slot, item) in load.iter_mut().zip(items) {
            *slot = item
                .as_f64()
                .ok_or_else(|| self.invalid("loadavg", "an array of numbers"))?;
        }
        Ok(Some(load))
    }

    /// The emitting host. A named field on [`EventMessage`], not a flattened
    /// one, so it does not go through [`Reader::str`].
    fn hostname(&self) -> Result<String> {
        self.message
            .hostname
            .clone()
            .ok_or_else(|| self.missing("hostname"))
    }

    /// The emitting process id, likewise a named field.
    fn pid(&self) -> Result<u32> {
        self.message.pid.ok_or_else(|| self.missing("pid"))
    }

    fn timestamp(&self) -> Result<DateTime<Utc>> {
        super::wire::from_wire_timestamp(self.message.timestamp)
    }
}

impl TryFrom<&EventMessage> for Event {
    type Error = CelersError;

    fn try_from(message: &EventMessage) -> Result<Self> {
        let read = Reader::new(message);
        let timestamp = read.timestamp()?;

        let event = match message.event_type.as_str() {
            "task-sent" => Event::Task(TaskEvent::Sent {
                task_id: read.uuid()?,
                task_name: read.str(WIRE_NAME)?,
                queue: read.str("queue")?,
                timestamp,
                args: read.opt_str("args")?,
                kwargs: read.opt_str("kwargs")?,
                eta: read.opt_datetime("eta")?,
                expires: read.opt_datetime("expires")?,
                retries: read.opt_u32("retries")?,
            }),
            "task-received" => Event::Task(TaskEvent::Received {
                task_id: read.uuid()?,
                task_name: read.str_or_default(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                pid: read.pid()?,
            }),
            "task-started" => Event::Task(TaskEvent::Started {
                task_id: read.uuid()?,
                task_name: read.str_or_default(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                pid: read.pid()?,
            }),
            "task-succeeded" => Event::Task(TaskEvent::Succeeded {
                task_id: read.uuid()?,
                task_name: read.str_or_default(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                runtime: read.f64("runtime")?,
                result: read.opt_str("result")?,
            }),
            "task-failed" => Event::Task(TaskEvent::Failed {
                task_id: read.uuid()?,
                task_name: read.str_or_default(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                exception: read.str("exception")?,
                traceback: read.opt_str("traceback")?,
            }),
            "task-retried" => Event::Task(TaskEvent::Retried {
                task_id: read.uuid()?,
                task_name: read.str_or_default(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                exception: read.str("exception")?,
                retries: read.u32_or_default("retries")?,
            }),
            "task-revoked" => Event::Task(TaskEvent::Revoked {
                task_id: read.uuid()?,
                task_name: read.opt_str(WIRE_NAME)?,
                timestamp,
                terminated: read.bool("terminated")?,
                signum: read.opt_i32("signum")?,
                expired: read.bool("expired")?,
            }),
            "task-rejected" => Event::Task(TaskEvent::Rejected {
                task_id: read.uuid()?,
                task_name: read.opt_str(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                reason: read.str_or_default("reason")?,
            }),
            "task-soft-time-limit-exceeded" => Event::Task(TaskEvent::SoftTimeLimitExceeded {
                task_id: read.uuid()?,
                task_name: read.str_or_default(WIRE_NAME)?,
                hostname: read.hostname()?,
                timestamp,
                elapsed_secs: read.f64("elapsed_secs")?,
                limit_secs: read.f64("limit_secs")?,
            }),
            "worker-online" => Event::Worker(WorkerEvent::Online {
                hostname: read.hostname()?,
                timestamp,
                sw_ident: read.str("sw_ident")?,
                sw_ver: read.str("sw_ver")?,
                sw_sys: read.str("sw_sys")?,
            }),
            "worker-offline" => Event::Worker(WorkerEvent::Offline {
                hostname: read.hostname()?,
                timestamp,
            }),
            "worker-heartbeat" => Event::Worker(WorkerEvent::Heartbeat {
                hostname: read.hostname()?,
                timestamp,
                active: read
                    .opt_u32("active")?
                    .ok_or_else(|| read.missing("active"))?,
                processed: read.u64("processed")?,
                loadavg: read.opt_loadavg()?,
                freq: read.f64("freq")?,
            }),
            other => {
                return Err(CelersError::Deserialization(format!(
                    "wire event '{other}': not an event type CeleRS knows"
                )))
            }
        };

        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("valid RFC3339 fixture")
            .with_timezone(&Utc)
    }

    /// The one hand-rolled formatting decision in this file. If `chrono` ever
    /// changes how it serializes a `DateTime<Utc>`, this test fails here rather
    /// than silently changing the `eta`/`expires` bytes on the wire.
    #[test]
    fn rfc3339_matches_chrono_serde() {
        for sample in [
            "2026-03-04T05:10:00Z",        // whole seconds
            "2026-03-04T05:06:07.123Z",    // milliseconds
            "2026-03-04T05:06:07.123456Z", // microseconds
            "1969-07-20T20:17:40.000001Z", // pre-epoch
            "1970-01-01T00:00:00Z",
        ] {
            let parsed = at(sample);
            assert_eq!(
                rfc3339(parsed),
                serde_json::to_value(parsed).expect("chrono serializes"),
                "hand-rolled RFC3339 diverged from chrono's serde impl for {sample}"
            );
        }
    }

    #[test]
    fn from_event_leaves_the_envelope_empty() {
        // A typed event knows nothing about who published it: the transport's
        // EventEnvelope supplies clock/utcoffset, and pid/hostname only where
        // the event itself does not carry them.
        let event = Event::Task(TaskEvent::Sent {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            queue: "celery".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            args: None,
            kwargs: None,
            eta: None,
            expires: None,
            retries: None,
        });

        let message = EventMessage::from(&event);
        assert_eq!(message.event_type, "task-sent");
        assert_eq!(message.clock, None);
        assert_eq!(message.utcoffset, None);
        assert_eq!(message.pid, None, "task-sent carries no pid of its own");
        assert_eq!(message.hostname, None);

        // Absent optionals are omitted, never written as null: a Python monitor
        // reads a present-but-null `eta` as a scheduled task.
        for absent in ["args", "kwargs", "eta", "expires", "retries"] {
            assert!(
                !message.fields.contains_key(absent),
                "{absent} should be omitted, not null"
            );
        }
    }

    #[test]
    fn events_that_carry_their_own_host_and_pid_report_them() {
        let event = Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            pid: 4242,
        });

        let message = EventMessage::from(&event);
        assert_eq!(message.hostname.as_deref(), Some("celery@worker-1"));
        assert_eq!(message.pid, Some(4242));
    }

    #[test]
    fn typed_round_trip_is_lossless() {
        for event in super::super::wire::tests::every_event() {
            let message = EventMessage::from(&event);
            let back = Event::try_from(&message)
                .unwrap_or_else(|e| panic!("{} should convert back: {e}", event.event_type()));
            assert_eq!(
                back,
                event,
                "typed round trip changed {}",
                event.event_type()
            );
        }
    }

    #[test]
    fn unknown_event_type_is_named_in_the_error() {
        let mut message = EventMessage::from(&Event::Worker(WorkerEvent::Offline {
            hostname: "celery@w".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
        }));
        message.event_type = "task-teleported".to_string();

        let err = Event::try_from(&message).expect_err("unknown type must be refused");
        assert!(err.to_string().contains("task-teleported"), "{err}");
    }

    #[test]
    fn a_missing_required_field_names_the_event_and_the_field() {
        let mut message = EventMessage::from(&Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            pid: 7,
        }));

        message.hostname = None;
        let err = Event::try_from(&message).expect_err("hostname is required");
        assert!(err.to_string().contains("task-started"), "{err}");
        assert!(err.to_string().contains("hostname"), "{err}");

        // A malformed uuid is refused too, rather than defaulting to nil.
        let mut message = EventMessage::from(&Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            pid: 7,
        }));
        message
            .fields
            .insert(WIRE_UUID.to_string(), Value::from("not-a-uuid"));
        let err = Event::try_from(&message).expect_err("uuid must parse");
        assert!(err.to_string().contains("not a UUID"), "{err}");
    }

    #[test]
    fn a_wrongly_typed_field_is_refused_rather_than_coerced() {
        let mut message = EventMessage::from(&Event::Task(TaskEvent::Succeeded {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            runtime: 1.5,
            result: Some("3".to_string()),
        }));

        message
            .fields
            .insert("runtime".to_string(), Value::from("very fast"));
        let err = Event::try_from(&message).expect_err("runtime must be a number");
        assert!(err.to_string().contains("runtime"), "{err}");
        assert!(err.to_string().contains("not a number"), "{err}");
    }

    #[test]
    fn a_malformed_loadavg_is_refused() {
        let good = Event::Worker(WorkerEvent::Heartbeat {
            hostname: "celery@w".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            active: 3,
            processed: 100,
            loadavg: Some([1.0, 0.8, 0.5]),
            freq: 2.0,
        });

        // The good case first, so the negatives below are meaningful.
        assert_eq!(
            Event::try_from(&EventMessage::from(&good)).expect("round trips"),
            good
        );

        for bad in [
            Value::from(vec![1.0, 0.8]),
            Value::from(vec![1.0, 0.8, 0.5, 0.2]),
            Value::from("1.0 0.8 0.5"),
        ] {
            let mut message = EventMessage::from(&good);
            message.fields.insert("loadavg".to_string(), bad.clone());
            let err = Event::try_from(&message)
                .expect_err("a malformed loadavg must be refused, not silently dropped");
            assert!(err.to_string().contains("loadavg"), "{bad}: {err}");
        }

        // An absent loadavg is legitimately None, not an error.
        let mut message = EventMessage::from(&good);
        message.fields.remove("loadavg");
        let Event::Worker(WorkerEvent::Heartbeat { loadavg, .. }) =
            Event::try_from(&message).expect("absent loadavg is fine")
        else {
            panic!("worker-heartbeat maps onto WorkerEvent::Heartbeat");
        };
        assert_eq!(loadavg, None);
    }

    /// Python Celery omits `name` after `task-received` and the retry count on
    /// `task-retried`. The typed conversion has to fill those the same way the
    /// serde defaults did, or a mixed cluster stops being readable.
    #[test]
    fn celery_omissions_come_back_at_their_documented_defaults() {
        let mut message = EventMessage::from(&Event::Task(TaskEvent::Retried {
            task_id: Uuid::new_v4(),
            task_name: "tasks.add".to_string(),
            hostname: "celery@worker-1".to_string(),
            timestamp: at("2026-03-04T05:06:07.123456Z"),
            exception: "Timeout".to_string(),
            retries: 2,
        }));
        message.fields.remove(WIRE_NAME);
        message.fields.remove("retries");

        let Event::Task(TaskEvent::Retried {
            task_name, retries, ..
        }) = Event::try_from(&message).expect("a Celery task-retried parses")
        else {
            panic!("task-retried maps onto TaskEvent::Retried");
        };
        assert_eq!(task_name, "");
        assert_eq!(retries, 0);
    }
}
