//! Python Celery protocol v2 wire-format checks
//!
//! This module checks a [`Message`] against the concrete shape Python Celery
//! puts on the wire for protocol v2, and provides the canonical Celery envelope
//! as a fixture for deserialization tests.
//!
//! # Scope
//!
//! [`verify_message_format`] is a *structural* check: it validates the envelope
//! layout, the required header keys, and that the body really is a base64
//! `[args, kwargs, embed]` tuple with the canonical embed dict. It cannot prove
//! interoperability with a particular Celery release -- only an end-to-end test
//! against a running Python worker can do that -- but, unlike a bare
//! key-presence check, it *can* fail: a message with an opaque body, a missing
//! `body_encoding`, or a malformed embed dict is rejected.
//!
//! The reference for every rule below is `celery.app.amqp.AMQP.as_task_v2` and
//! `kombu.transport.virtual.base`.

use crate::embed::EmbeddedBody;
use crate::{Message, BODY_ENCODING_BASE64};
use base64::Engine;
use serde_json::json;
use uuid::Uuid;

/// Header keys Python Celery always writes for protocol v2.
///
/// Celery emits these unconditionally (with `null` when unused), so a consumer
/// may index them directly. CeleRS omits the null-valued optional ones, which is
/// safe because Celery's own worker reads headers with `.get()`; only the three
/// listed in [`REQUIRED_V2_HEADERS`] are load-bearing.
pub const CELERY_V2_HEADERS: &[&str] = &[
    "task",
    "id",
    "lang",
    "root_id",
    "parent_id",
    "group",
    "retries",
    "eta",
    "expires",
    "timelimit",
    "argsrepr",
    "kwargsrepr",
    "origin",
    "shadow",
    "ignore_result",
];

/// Header keys without which a Celery worker cannot dispatch the task.
pub const REQUIRED_V2_HEADERS: &[&str] = &["task", "id", "lang"];

/// The queue Celery uses when a task names none (`task_default_queue`).
///
/// Re-exported from the crate root, where [`crate::DeliveryInfo`] uses the same
/// constant as its default routing key -- the two must never drift apart.
pub use crate::DEFAULT_CELERY_QUEUE;

/// How deep `celery.utils.saferepr` descends before eliding a container.
///
/// Celery renders `argsrepr` / `kwargsrepr` with `saferepr(..., maxlevels=3)`,
/// which replaces the *contents* of any container opened at nesting level 3 or
/// deeper with `...` while keeping its brackets: `{'a': {'b': {'c': [...]}}}`.
pub const SAFEREPR_MAXLEVELS: usize = 3;

/// Above this magnitude (and below `1e-4`) Python's `repr` switches a float to
/// exponent notation. `repr(1e16)` is `'1e+16'`; `repr(1e15)` is
/// `'1000000000000000.0'`.
const FLOAT_EXPONENT_THRESHOLD: i32 = 16;

/// Below this exponent Python's `repr` switches a float to exponent notation.
/// `repr(1e-4)` is `'0.0001'`; `repr(1e-5)` is `'1e-05'`.
const FLOAT_EXPONENT_FLOOR: i32 = -4;

/// Render a JSON value the way `celery.utils.saferepr` renders its Python
/// equivalent.
///
/// This is what the `argsrepr` / `kwargsrepr` observability headers carry, and
/// it is what Flower, `celery events` and `celery inspect` print. It is *not*
/// `repr` from the standard library: Celery's `saferepr` always delimits a
/// string with single quotes, escaping an embedded `'` as `\'`, where the
/// builtin would switch to double quotes. It also leaves backslashes,
/// newlines, tabs and control characters unescaped, and passes non-ASCII
/// through verbatim.
///
/// The mapping:
///
/// | JSON        | Python              |
/// |-------------|---------------------|
/// | `null`      | `None`              |
/// | `true`      | `True`              |
/// | `false`     | `False`             |
/// | `4`         | `4`                 |
/// | `2.0`       | `2.0`               |
/// | `"it's"`    | `'it\'s'`           |
/// | `[1, 2]`    | `[1, 2]`            |
/// | `{"a": 1}`  | `{'a': 1}`          |
///
/// # Ordering
///
/// A JSON object has no insertion order once parsed -- [`serde_json::Map`] is
/// a `BTreeMap` here -- so a dict is rendered with its keys **sorted**. Python
/// reprs a dict in insertion order, so a caller who built `kwargs` as
/// `{'z': 1, 'a': 2}` sees `{'z': 1, 'a': 2}` from Celery and
/// `{'a': 2, 'z': 1}` from this function. The difference is confined to a
/// display header; no dispatch decision reads it.
pub fn python_repr(value: &serde_json::Value) -> String {
    let mut out = String::new();
    write_python_repr(&mut out, value, 0);
    out
}

/// Render a positional-argument list as the Python **tuple** literal Celery's
/// `argsrepr` carries: `()`, `(1,)`, `(4, 5)`.
///
/// The tuple form is deliberate. `task.delay(4, 5)` -- the canonical Celery
/// call -- passes `args` as a tuple, so `repr(args)` is `'(4, 5)'`. Calling
/// `apply_async(args=[4, 5])` with a *list* instead yields `'[4, 5]'`; both
/// are real Celery output, and CeleRS follows `.delay()`. Do not "fix" this to
/// the list form.
pub fn python_args_repr(args: &[serde_json::Value]) -> String {
    let mut out = String::from("(");
    for (index, arg) in args.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        write_python_repr(&mut out, arg, 1);
    }
    // Python disambiguates a one-element tuple from a parenthesised value.
    if args.len() == 1 {
        out.push(',');
    }
    out.push(')');
    out
}

/// Render keyword arguments as the Python dict literal Celery's `kwargsrepr`
/// carries: `{}`, `{'loud': True}`.
///
/// A non-object `kwargs` cannot occur in a well-formed protocol v2 body, but
/// rather than panic on one this falls back to [`python_repr`], which renders
/// whatever it was given.
pub fn python_kwargs_repr(kwargs: &serde_json::Value) -> String {
    match kwargs {
        serde_json::Value::Object(_) => python_repr(kwargs),
        other => python_repr(other),
    }
}

/// Render `value` into `out`, where `level` counts the containers already open.
fn write_python_repr(out: &mut String, value: &serde_json::Value, level: usize) {
    use serde_json::Value;
    match value {
        Value::Null => out.push_str("None"),
        Value::Bool(true) => out.push_str("True"),
        Value::Bool(false) => out.push_str("False"),
        Value::Number(number) => out.push_str(&python_number_repr(number)),
        Value::String(text) => write_python_str(out, text),
        Value::Array(items) => {
            if level >= SAFEREPR_MAXLEVELS {
                out.push_str("[...]");
                return;
            }
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_python_repr(out, item, level + 1);
            }
            out.push(']');
        }
        Value::Object(entries) => {
            if level >= SAFEREPR_MAXLEVELS {
                out.push_str("{...}");
                return;
            }
            out.push('{');
            for (index, (key, item)) in entries.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_python_str(out, key);
                out.push_str(": ");
                write_python_repr(out, item, level + 1);
            }
            out.push('}');
        }
    }
}

/// Write a Python string literal: single quotes, `'` escaped as `\'`.
///
/// Everything else -- backslashes, newlines, tabs, control and non-ASCII
/// characters -- is passed through verbatim, because that is what
/// `celery.utils.saferepr` does. Reproducing the builtin `repr`'s escaping
/// here would *diverge* from the header Celery actually writes.
fn write_python_str(out: &mut String, text: &str) {
    out.push('\'');
    for ch in text.chars() {
        if ch == '\'' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('\'');
}

/// Render a JSON number as Python's `repr` of the corresponding int or float.
///
/// Integers print as-is. Floats follow Python: a `.0` suffix keeps an integral
/// float distinguishable from an int, and magnitudes outside
/// `[1e-4, 1e16)` switch to exponent notation with a signed, zero-padded
/// two-digit exponent (`1e+20`, `1e-07`).
fn python_number_repr(number: &serde_json::Number) -> String {
    if number.is_i64() || number.is_u64() {
        return number.to_string();
    }
    let Some(value) = number.as_f64() else {
        // Unreachable for a `serde_json::Number`, which is always one of the
        // three arms; falling back to the JSON text beats panicking.
        return number.to_string();
    };
    python_float_repr(value)
}

/// Python's `repr` for a finite `f64`.
fn python_float_repr(value: f64) -> String {
    if !value.is_finite() {
        // JSON cannot carry these, but a hand-built `Number` could not either,
        // so this arm exists only so the function is total.
        return match (value.is_nan(), value.is_sign_negative()) {
            (true, _) => "nan".to_string(),
            (false, true) => "-inf".to_string(),
            (false, false) => "inf".to_string(),
        };
    }

    // Rust's `LowerExp` is shortest-round-trip, the same guarantee Python's
    // `repr` gives, so the mantissa needs no reformatting -- only the exponent
    // spelling differs (`1e20` vs `1e+20`).
    let exponential = format!("{:e}", value);
    let (mantissa, exponent_text) = match exponential.split_once('e') {
        Some(parts) => parts,
        // `LowerExp` always emits an `e`; if that ever changed, the decimal
        // rendering below is still correct.
        None => return with_float_point(&format!("{}", value)),
    };
    let exponent: i32 = exponent_text.parse().unwrap_or(0);

    if !(FLOAT_EXPONENT_FLOOR..FLOAT_EXPONENT_THRESHOLD).contains(&exponent) {
        let sign = if exponent < 0 { '-' } else { '+' };
        return format!("{}e{}{:02}", mantissa, sign, exponent.abs());
    }

    with_float_point(&format!("{}", value))
}

/// Give a decimal float rendering the `.0` Python uses to mark it as a float.
fn with_float_point(text: &str) -> String {
    if text.contains('.') || text.contains('e') || text.contains("inf") || text.contains("nan") {
        text.to_string()
    } else {
        format!("{}.0", text)
    }
}

/// Verify that a CeleRS message serializes to Celery-compatible JSON
///
/// Checks, in order:
///
/// 1. The envelope carries `headers`, `properties`, `body`, `content-type` and
///    `content-encoding` (kombu's hyphenated spellings).
/// 2. Every key in [`REQUIRED_V2_HEADERS`] is present in `headers`.
/// 3. `properties.delivery_mode` is 1 or 2, `properties.body_encoding` is
///    `"base64"` -- without which a kombu consumer never base64-decodes the
///    body and hands the encoded text to the content-type deserializer -- and
///    `properties.delivery_tag` / `properties.delivery_info` are both present,
///    the latter with `exchange` and `routing_key`. kombu's
///    `Message.__init__` indexes those two without a default, so omitting
///    either raises `KeyError` inside the consumer callback and takes the
///    worker's event loop down with it.
/// 4. `body` is a base64 string that decodes to the protocol v2 tuple
///    `[args, kwargs, embed]`: a list, an object, and an embed object. This
///    step applies only when `content-type` is `application/json`; other
///    serializations frame the same tuple in their own encoding, which this
///    function does not decode.
pub fn verify_message_format(msg: &Message) -> Result<(), String> {
    // Serialize to JSON
    let json_str = serde_json::to_string(msg).map_err(|e| format!("Serialization error: {}", e))?;

    let value: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| format!("Parse error: {}", e))?;

    // 1. Envelope layout.
    for field in [
        "headers",
        "properties",
        "body",
        "content-type",
        "content-encoding",
    ] {
        if value.get(field).is_none() {
            return Err(format!("Missing '{}' field", field));
        }
    }

    // 2. Required headers.
    let headers = value
        .get("headers")
        .ok_or_else(|| "Missing 'headers' field".to_string())?;
    for header in REQUIRED_V2_HEADERS {
        if headers.get(header).is_none() {
            return Err(format!("Missing 'headers.{}' field", header));
        }
    }

    // 3. Properties that govern how the body is read.
    let properties = value
        .get("properties")
        .ok_or_else(|| "Missing 'properties' field".to_string())?;
    match properties.get("delivery_mode").and_then(|v| v.as_u64()) {
        Some(1) | Some(2) => {}
        other => {
            return Err(format!(
                "Invalid 'properties.delivery_mode': expected 1 or 2, got {:?}",
                other
            ))
        }
    }
    match properties.get("body_encoding").and_then(|v| v.as_str()) {
        Some(BODY_ENCODING_BASE64) => {}
        other => {
            return Err(format!(
                "Invalid 'properties.body_encoding': expected {:?}, got {:?}. \
                 kombu only base64-decodes the body when this property says so.",
                BODY_ENCODING_BASE64, other
            ))
        }
    }

    // The two properties a kombu consumer indexes without a default.
    match properties.get("delivery_tag").and_then(|v| v.as_str()) {
        Some(tag) if !tag.is_empty() => {}
        other => {
            return Err(format!(
                "Invalid 'properties.delivery_tag': expected a non-empty string, got {:?}. \
                 kombu.transport.virtual.base.Message.__init__ indexes it directly, so a \
                 missing one raises KeyError inside the consumer callback.",
                other
            ))
        }
    }
    let delivery_info = properties
        .get("delivery_info")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            "Missing 'properties.delivery_info': kombu indexes \
             delivery_info['exchange'] directly, which raises KeyError and kills \
             the consumer loop when the property is absent"
                .to_string()
        })?;
    for key in ["exchange", "routing_key"] {
        if !delivery_info
            .get(key)
            .is_some_and(serde_json::Value::is_string)
        {
            return Err(format!(
                "Invalid 'properties.delivery_info.{}': expected a string, got {:?}",
                key,
                delivery_info.get(key)
            ));
        }
    }

    // 4. The body must be the protocol v2 [args, kwargs, embed] tuple.
    let body = value
        .get("body")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "'body' must be a base64 string".to_string())?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| format!("'body' is not valid base64: {}", e))?;

    // The tuple check only applies to a JSON-serialized body; msgpack and other
    // content types encode the same three-element tuple in their own framing,
    // which this function does not decode.
    if msg.content_type != crate::CONTENT_TYPE_JSON {
        return Ok(());
    }

    let tuple: serde_json::Value = serde_json::from_slice(&decoded)
        .map_err(|e| format!("Body is not valid protocol v2 JSON: {}", e))?;
    let elements = tuple
        .as_array()
        .ok_or_else(|| "Body must be the [args, kwargs, embed] tuple".to_string())?;
    if elements.len() != 3 {
        return Err(format!(
            "Body must have exactly 3 elements [args, kwargs, embed], got {}",
            elements.len()
        ));
    }
    if !elements[0].is_array() {
        return Err("Body element 0 (args) must be a list".to_string());
    }
    if !elements[1].is_object() {
        return Err("Body element 1 (kwargs) must be an object".to_string());
    }
    if !elements[2].is_object() && !elements[2].is_null() {
        return Err("Body element 2 (embed) must be an object".to_string());
    }

    EmbeddedBody::decode(&decoded).map_err(|e| format!("Body embed dict is malformed: {}", e))?;

    Ok(())
}

/// Build the canonical Python Celery protocol v2 envelope (for testing
/// deserialization).
///
/// This mirrors `celery.app.amqp.AMQP.as_task_v2` plus the kombu
/// virtual-transport envelope, including the parts CeleRS itself omits:
///
/// * every v2 header, with explicit `null` for the unused ones, and
///   `timelimit` as the `[soft, hard]` pair;
/// * `properties.body_encoding`, which tells kombu to base64-decode the body;
/// * the embed dict with all four workflow keys present
///   (`{'callbacks': None, 'errbacks': None, 'chain': None, 'chord': None}`),
///   which is what Python emits even when no workflow is attached;
/// * `argsrepr` / `kwargsrepr` as *Python* literals -- see
///   [`python_args_repr`] and [`python_kwargs_repr`]. These are what Flower
///   and `celery events` display, so a Rust `Debug` rendering there
///   (`[Number(4), Number(5)]`) is visible to every operator watching the
///   cluster.
///
/// The envelope is a strict subset of Celery 5.6's headers: that release also
/// writes `group_index`, `replaced_task_nesting`, `stamped_headers` and
/// `stamps`. A real worker reads headers with `.get()`, and the interop suite
/// under `tests/python-compat` proves an envelope built here executes on an
/// unmodified Celery worker.
///
/// # Deterministic by design
///
/// Every field of the result is a function of the arguments: `reply_to` and
/// `delivery_tag` are the **nil UUID** and `origin` is a fixed string, so the
/// bytes are reproducible and can be compared against a committed capture
/// (`celers_envelope_accepted_by_celery.json`). That is what makes this a
/// fixture builder rather than a producer: kombu mints a *fresh* delivery tag
/// per publish, and a consumer keys its unacknowledged-message table by it, so
/// two of these envelopes in flight on one channel share a tag. Publish real
/// work through [`crate::builder::MessageBuilder`], whose
/// [`crate::MessageProperties`] carry a unique tag and a routing key naming the
/// queue.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the `[args, kwargs, embed]` body tuple
/// cannot be serialized to JSON. In practice this cannot happen for a body
/// built from already-valid [`serde_json::Value`]s (a `Value` can never
/// hold a non-finite float or a non-string map key -- the only two things
/// that make `serde_json` serialization fail), but the possibility is
/// surfaced through the return type rather than papered over with a panic.
pub fn create_python_celery_message(
    task_name: &str,
    task_id: Uuid,
    args: Vec<serde_json::Value>,
    kwargs: serde_json::Value,
) -> Result<serde_json::Value, serde_json::Error> {
    create_python_celery_message_on_queue(task_name, task_id, args, kwargs, DEFAULT_CELERY_QUEUE)
}

/// Build the canonical envelope for a task that lives on `queue`.
///
/// Identical to [`create_python_celery_message`] except that
/// `properties.delivery_info.routing_key` names `queue` instead of the default.
///
/// # Why the routing key matters
///
/// It is not decoration. A Celery worker re-publishes with
/// `self.request.delivery_info` when a task calls `task.retry()`, and uses the
/// same information to route `link` callbacks. A message that sits on
/// `payments` while claiming `routing_key: "celery"` therefore sends its own
/// retries to the *default* queue -- where, if nothing consumes it, the retry
/// is never executed and the task stays `RETRY` forever. The interop suite
/// reproduces exactly that (`tests/python-compat/test_celers_to_python.py`).
///
/// # Errors
///
/// As [`create_python_celery_message`].
pub fn create_python_celery_message_on_queue(
    task_name: &str,
    task_id: Uuid,
    args: Vec<serde_json::Value>,
    kwargs: serde_json::Value,
    queue: &str,
) -> Result<serde_json::Value, serde_json::Error> {
    let embed = json!({
        "callbacks": null,
        "errbacks": null,
        "chain": null,
        "chord": null
    });

    let body = serde_json::to_vec(&json!([args, kwargs, embed]))?;

    Ok(json!({
        "headers": {
            "task": task_name,
            "id": task_id.to_string(),
            "lang": "py",
            "root_id": task_id.to_string(),
            "parent_id": null,
            "group": null,
            "retries": 0,
            "eta": null,
            "expires": null,
            "timelimit": [null, null],
            "argsrepr": python_args_repr(&args),
            "kwargsrepr": python_kwargs_repr(&kwargs),
            "origin": "1234@celers-test",
            "shadow": null,
            "ignore_result": false
        },
        "properties": {
            "correlation_id": task_id.to_string(),
            "reply_to": Uuid::nil().to_string(),
            "delivery_mode": 2,
            "priority": 0,
            "body_encoding": BODY_ENCODING_BASE64,
            "delivery_tag": Uuid::nil().to_string(),
            // The empty exchange is kombu's direct-to-queue routing; the
            // routing key names the queue the message is being put on.
            "delivery_info": {"exchange": "", "routing_key": queue}
        },
        "content-type": "application/json",
        "content-encoding": "utf-8",
        "body": base64::engine::general_purpose::STANDARD.encode(body)
    }))
}

/// Parse a Python Celery message into CeleRS Message
pub fn parse_python_message(json_value: serde_json::Value) -> Result<Message, String> {
    serde_json::from_value(json_value).map_err(|e| format!("Parse error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContentEncoding, ContentType};
    use chrono::Utc;

    #[test]
    fn test_celers_message_format_compatibility() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&json!([[1, 2], {}, {}])).unwrap();

        let msg = Message::new("tasks.add".to_string(), task_id, body);

        // Verify it produces valid Celery format
        verify_message_format(&msg).expect("Message format should be compatible");
    }

    /// Regression: `verify_message_format` used to check nothing but the
    /// presence of a handful of keys, so every message this crate could
    /// produce passed and no real incompatibility could ever be detected.
    #[test]
    fn test_verify_message_format_rejects_incompatible_messages() {
        let task_id = Uuid::new_v4();

        // An opaque body is not a protocol v2 [args, kwargs, embed] tuple.
        let opaque = Message::new("tasks.add".to_string(), task_id, b"not json".to_vec());
        let err = verify_message_format(&opaque)
            .expect_err("an opaque body must be rejected as protocol v2");
        assert!(
            err.contains("protocol v2 JSON"),
            "unexpected error: {}",
            err
        );

        // A JSON body that is not the 3-tuple is rejected too.
        let two_tuple = Message::new(
            "tasks.add".to_string(),
            task_id,
            serde_json::to_vec(&json!([[1, 2], {}])).expect("encode"),
        );
        let err = verify_message_format(&two_tuple).expect_err("a 2-tuple body must be rejected");
        assert!(
            err.contains("exactly 3 elements"),
            "unexpected error: {}",
            err
        );

        // args must be a list, kwargs an object -- the Python calling
        // convention, not a free-form pair.
        let swapped = Message::new(
            "tasks.add".to_string(),
            task_id,
            serde_json::to_vec(&json!([{}, {}, {}])).expect("encode"),
        );
        let err = verify_message_format(&swapped).expect_err("args must be a list");
        assert!(
            err.contains("(args) must be a list"),
            "unexpected error: {}",
            err
        );

        // An invalid delivery mode is rejected.
        let mut bad_mode = Message::new(
            "tasks.add".to_string(),
            task_id,
            serde_json::to_vec(&json!([[], {}, {}])).expect("encode"),
        );
        bad_mode.properties.delivery_mode = 7;
        let err = verify_message_format(&bad_mode).expect_err("delivery_mode 7 must be rejected");
        assert!(err.contains("delivery_mode"), "unexpected error: {}", err);
    }

    /// A message built through the crate's own v5 path must also satisfy the
    /// protocol v2 envelope rules (v5 is the same envelope plus header stamps).
    #[test]
    fn test_v5_message_satisfies_v2_envelope_rules() {
        let msg = crate::v5::V5MessageSpec::new("tasks.add", Uuid::new_v4())
            .with_args(vec![json!(1), json!(2)])
            .with_kwarg("debug", json!(true))
            .build()
            .expect("v5 build must succeed")
            .into_message();

        verify_message_format(&msg).expect("a v5 message is a valid v2 envelope");
    }

    /// A protocol v2 envelope captured verbatim from Python Celery 5.6.3.
    ///
    /// Recorded by `tests/python-compat/capture_fixtures.py`, which published
    /// `tasks.add.apply_async(args=(4, 5))` through a real Celery to a real
    /// Redis and wrote back the bytes kombu put on the queue. See
    /// `tests/fixtures/README.md` for the versions and the exact command.
    ///
    /// Regression: the tests below used to build their "canonical Celery
    /// envelope" by calling [`create_python_celery_message`] -- the function in
    /// this very module -- and then assert over its output. Such a test cannot
    /// detect divergence from Celery; it can only fail if the function
    /// contradicts itself.
    const CELERY_CAPTURE: &str =
        include_str!("../tests/fixtures/celery_task_v2_positional_tuple.json");

    /// The envelope [`create_python_celery_message`] produces, recorded only
    /// after an unmodified Celery worker executed it and returned `SUCCESS`.
    const CELERS_ACCEPTED: &str =
        include_str!("../tests/fixtures/celers_envelope_accepted_by_celery.json");

    #[test]
    fn test_parse_python_celery_message() {
        let python_msg: serde_json::Value =
            serde_json::from_str(CELERY_CAPTURE).expect("the capture is valid JSON");

        let msg = parse_python_message(python_msg).expect("a real Celery message must parse");

        assert_eq!(msg.headers.task, "tasks.add");
        assert_eq!(
            msg.headers.id.to_string(),
            "7b1a0d1e-0000-4000-8000-000000000002"
        );
        assert_eq!(msg.headers.lang, "py");
        assert_eq!(msg.content_type, "application/json");

        let decoded = EmbeddedBody::decode(&msg.body).expect("body must decode");
        assert_eq!(decoded.args, vec![json!(4), json!(5)]);
    }

    /// What Python actually emits, asserted against Python's own bytes: every
    /// v2 header present (null when unused), `timelimit` as the `[soft, hard]`
    /// pair, `body_encoding` so kombu decodes the body, and an embed dict with
    /// all four workflow keys spelled out.
    #[test]
    fn test_the_capture_is_the_canonical_celery_envelope() {
        let python_msg: serde_json::Value =
            serde_json::from_str(CELERY_CAPTURE).expect("the capture is valid JSON");

        for header in CELERY_V2_HEADERS {
            assert!(
                python_msg["headers"].get(header).is_some(),
                "Celery's own message is missing the v2 header '{}'",
                header
            );
        }
        assert_eq!(python_msg["headers"]["timelimit"], json!([null, null]));
        assert_eq!(python_msg["properties"]["body_encoding"], json!("base64"));
        assert!(python_msg["properties"]["delivery_info"].is_object());

        let body = base64::engine::general_purpose::STANDARD
            .decode(python_msg["body"].as_str().expect("body is a string"))
            .expect("body is base64");
        let tuple: serde_json::Value = serde_json::from_slice(&body).expect("body is json");
        for key in ["callbacks", "errbacks", "chain", "chord"] {
            assert_eq!(
                tuple[2][key],
                json!(null),
                "Celery's embed dict is missing the '{}' key",
                key
            );
        }

        let msg = parse_python_message(python_msg).expect("the capture must parse");
        let decoded = EmbeddedBody::decode(&msg.body).expect("body must decode");
        assert_eq!(decoded.args, vec![json!(4), json!(5)]);
        assert!(!decoded.embed.has_workflow());
        verify_message_format(&msg).expect("the capture is a valid v2 envelope");
    }

    /// A task published to a named queue must say so in its routing key, or a
    /// Celery worker sends the task's own retries to the default queue -- where
    /// nothing is listening, so the task stays `RETRY` forever.
    #[test]
    fn test_routing_key_names_the_queue_the_message_is_on() {
        let task_id = Uuid::nil();
        let default = create_python_celery_message("tasks.add", task_id, vec![json!(1)], json!({}))
            .expect("serialize");
        assert_eq!(
            default["properties"]["delivery_info"]["routing_key"],
            json!(DEFAULT_CELERY_QUEUE),
            "the default must stay `celery`; the accepted golden fixture pins it"
        );

        let routed = create_python_celery_message_on_queue(
            "tasks.add",
            task_id,
            vec![json!(1)],
            json!({}),
            "payments",
        )
        .expect("serialize");
        assert_eq!(
            routed["properties"]["delivery_info"]["routing_key"],
            json!("payments")
        );
        assert_eq!(
            routed["properties"]["delivery_info"]["exchange"],
            json!(""),
            "kombu routes direct-to-queue through the empty exchange"
        );

        // The queue must not leak anywhere else in the envelope.
        assert_eq!(routed["headers"], default["headers"]);
        assert_eq!(routed["body"], default["body"]);
    }

    /// The other half: what CeleRS emits must equal what Celery was proven to
    /// accept. Together with the test above, drift in *either* direction fails.
    #[test]
    fn test_created_envelope_equals_the_one_celery_accepted() {
        let task_id: Uuid = "7b1a0d1e-0000-4000-8000-000000000012"
            .parse()
            .expect("fixture task id");
        let produced =
            create_python_celery_message("tasks.add", task_id, vec![json!(4), json!(5)], json!({}))
                .expect("the canonical envelope must serialize");
        let accepted: serde_json::Value =
            serde_json::from_str(CELERS_ACCEPTED).expect("the capture is valid JSON");

        assert_eq!(
            produced, accepted,
            "create_python_celery_message drifted from the envelope a real \
             Celery worker executed"
        );
    }

    #[test]
    fn test_round_trip_serialization() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&json!([[10, 20], {"debug": true}, {}])).unwrap();

        let msg1 = Message::new("tasks.process".to_string(), task_id, body.clone());

        // Serialize to JSON
        let json_str = serde_json::to_string(&msg1).expect("Should serialize");

        // Deserialize back
        let msg2: Message = serde_json::from_str(&json_str).expect("Should deserialize");

        // Verify fields match
        assert_eq!(msg1.headers.task, msg2.headers.task);
        assert_eq!(msg1.headers.id, msg2.headers.id);
        assert_eq!(msg1.body, msg2.body);
        assert_eq!(msg1.content_type, msg2.content_type);
    }

    #[test]
    fn test_message_with_workflow_fields() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let group_id = Uuid::new_v4();

        let body = serde_json::to_vec(&json!([[], {}, {}])).unwrap();

        let msg = Message::new("tasks.chord_callback".to_string(), task_id, body)
            .with_parent(parent_id)
            .with_root(root_id)
            .with_group(group_id)
            .with_priority(5);

        // Verify format
        verify_message_format(&msg).expect("Should be compatible");

        // Serialize and check JSON structure
        let json_str = serde_json::to_string(&msg).expect("Should serialize");
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(value["headers"]["parent_id"], json!(parent_id.to_string()));
        assert_eq!(value["headers"]["root_id"], json!(root_id.to_string()));
        assert_eq!(value["headers"]["group"], json!(group_id.to_string()));
        assert_eq!(value["properties"]["priority"], json!(5));
    }

    #[test]
    fn test_message_with_eta_and_expires() {
        let task_id = Uuid::new_v4();
        let eta = Utc::now() + chrono::Duration::hours(2);
        let expires = Utc::now() + chrono::Duration::days(1);

        let body = serde_json::to_vec(&json!([[], {}, {}])).unwrap();

        let msg = Message::new("tasks.scheduled".to_string(), task_id, body)
            .with_eta(eta)
            .with_expires(expires);

        verify_message_format(&msg).expect("Should be compatible");

        // Serialize and verify timestamp format
        let json_str = serde_json::to_string(&msg).expect("Should serialize");
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Celery uses ISO 8601 format for timestamps
        assert!(value["headers"]["eta"].is_string());
        assert!(value["headers"]["expires"].is_string());
    }

    #[test]
    fn test_body_base64_encoding() {
        let task_id = Uuid::new_v4();
        let raw_body = b"test data";

        let msg = Message::new("tasks.test".to_string(), task_id, raw_body.to_vec());

        let json_str = serde_json::to_string(&msg).expect("Should serialize");
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Body should be base64-encoded string
        assert!(value["body"].is_string());

        // Decode and verify
        let encoded = value["body"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("Should decode");
        assert_eq!(decoded, raw_body);
    }

    #[test]
    fn test_content_type_values() {
        assert_eq!(ContentType::Json.as_str(), "application/json");
        #[cfg(feature = "msgpack")]
        assert_eq!(ContentType::MessagePack.as_str(), "application/x-msgpack");
        #[cfg(feature = "binary")]
        assert_eq!(ContentType::Binary.as_str(), "application/octet-stream");
    }

    #[test]
    fn test_content_encoding_values() {
        assert_eq!(ContentEncoding::Utf8.as_str(), "utf-8");
        assert_eq!(ContentEncoding::Binary.as_str(), "binary");
    }

    #[test]
    fn test_delivery_mode_persistent() {
        let task_id = Uuid::new_v4();
        let body = vec![];

        let msg = Message::new("tasks.test".to_string(), task_id, body);

        // Default should be persistent (delivery_mode = 2)
        assert_eq!(msg.properties.delivery_mode, 2);

        let json_str = serde_json::to_string(&msg).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(value["properties"]["delivery_mode"], json!(2));
    }
}
