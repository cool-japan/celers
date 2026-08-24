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

/// Verify that a CeleRS message serializes to Celery-compatible JSON
///
/// Checks, in order:
///
/// 1. The envelope carries `headers`, `properties`, `body`, `content-type` and
///    `content-encoding` (kombu's hyphenated spellings).
/// 2. Every key in [`REQUIRED_V2_HEADERS`] is present in `headers`.
/// 3. `properties.delivery_mode` is 1 or 2, and `properties.body_encoding` is
///    `"base64"` -- without which a kombu consumer never base64-decodes the
///    body and hands the encoded text to the content-type deserializer.
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
///   which is what Python emits even when no workflow is attached.
pub fn create_python_celery_message(
    task_name: &str,
    task_id: Uuid,
    args: Vec<serde_json::Value>,
    kwargs: serde_json::Value,
) -> serde_json::Value {
    let embed = json!({
        "callbacks": null,
        "errbacks": null,
        "chain": null,
        "chord": null
    });

    json!({
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
            "argsrepr": format!("{:?}", args),
            "kwargsrepr": kwargs.to_string(),
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
            "delivery_info": {"exchange": "", "routing_key": "celery"}
        },
        "content-type": "application/json",
        "content-encoding": "utf-8",
        "body": base64::engine::general_purpose::STANDARD.encode(
            serde_json::to_vec(&json!([args, kwargs, embed])).expect("serialization should not fail")
        )
    })
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

    #[test]
    fn test_parse_python_celery_message() {
        let task_id = Uuid::new_v4();
        let python_msg = create_python_celery_message(
            "tasks.multiply",
            task_id,
            vec![json!(4), json!(5)],
            json!({}),
        );

        // Should parse without errors
        let msg = parse_python_message(python_msg).expect("Should parse Python message");

        assert_eq!(msg.headers.task, "tasks.multiply");
        assert_eq!(msg.headers.id, task_id);
        assert_eq!(msg.headers.lang, "py");
        assert_eq!(msg.content_type, "application/json");
    }

    /// The fixture must carry the parts a real Celery producer emits and CeleRS
    /// previously ignored: the null-valued embed keys, `body_encoding`, and the
    /// full v2 header set. Regression: the fixture claimed to be "the exact
    /// format Python Celery uses" while emitting an empty embed dict `{}` and
    /// omitting every one of those keys.
    #[test]
    fn test_python_fixture_is_the_canonical_celery_envelope() {
        let task_id = Uuid::new_v4();
        let python_msg = create_python_celery_message(
            "tasks.multiply",
            task_id,
            vec![json!(4), json!(5)],
            json!({}),
        );

        // Every protocol v2 header key is present (null when unused).
        for header in CELERY_V2_HEADERS {
            assert!(
                python_msg["headers"].get(header).is_some(),
                "fixture is missing the Celery v2 header '{}'",
                header
            );
        }
        assert_eq!(python_msg["headers"]["timelimit"], json!([null, null]));

        // kombu's body codec selector.
        assert_eq!(python_msg["properties"]["body_encoding"], json!("base64"));
        assert!(python_msg["properties"]["delivery_info"].is_object());

        // The embed dict carries all four workflow keys with explicit nulls,
        // which is what `as_task_v2` writes.
        let body = base64::engine::general_purpose::STANDARD
            .decode(python_msg["body"].as_str().expect("body is a string"))
            .expect("body is base64");
        let tuple: serde_json::Value = serde_json::from_slice(&body).expect("body is json");
        for key in ["callbacks", "errbacks", "chain", "chord"] {
            assert_eq!(
                tuple[2][key],
                json!(null),
                "embed dict is missing the '{}' key",
                key
            );
        }

        // And the whole thing round-trips into a `Message` whose body decodes.
        let msg = parse_python_message(python_msg).expect("fixture must parse");
        let decoded = EmbeddedBody::decode(&msg.body).expect("body must decode");
        assert_eq!(decoded.args, vec![json!(4), json!(5)]);
        assert!(!decoded.embed.has_workflow());
        verify_message_format(&msg).expect("the fixture is a valid v2 envelope");
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
