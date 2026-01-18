//! Python Celery Protocol Compatibility Tests
//!
//! This module provides verification that CeleRS messages are wire-compatible
//! with Python Celery's protocol v2/v5 format.

use crate::Message;
use base64::Engine;
use serde_json::json;
use uuid::Uuid;

/// Verify that a CeleRS message serializes to Celery-compatible JSON
pub fn verify_message_format(msg: &Message) -> Result<(), String> {
    // Serialize to JSON
    let json_str = serde_json::to_string(msg).map_err(|e| format!("Serialization error: {}", e))?;

    let value: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| format!("Parse error: {}", e))?;

    // Verify required fields exist
    if value.get("headers").is_none() {
        return Err("Missing 'headers' field".to_string());
    }

    if value.get("properties").is_none() {
        return Err("Missing 'properties' field".to_string());
    }

    if value.get("body").is_none() {
        return Err("Missing 'body' field".to_string());
    }

    if value.get("content-type").is_none() {
        return Err("Missing 'content-type' field".to_string());
    }

    if value.get("content-encoding").is_none() {
        return Err("Missing 'content-encoding' field".to_string());
    }

    // Verify headers structure
    let headers = value.get("headers").expect("headers field should exist");
    if headers.get("task").is_none() {
        return Err("Missing 'headers.task' field".to_string());
    }

    if headers.get("id").is_none() {
        return Err("Missing 'headers.id' field".to_string());
    }

    if headers.get("lang").is_none() {
        return Err("Missing 'headers.lang' field".to_string());
    }

    Ok(())
}

/// Create a Python Celery-compatible message (for testing deserialization)
pub fn create_python_celery_message(
    task_name: &str,
    task_id: Uuid,
    args: Vec<serde_json::Value>,
    kwargs: serde_json::Value,
) -> serde_json::Value {
    // This is the exact format Python Celery uses for Protocol v2
    json!({
        "headers": {
            "task": task_name,
            "id": task_id.to_string(),
            "lang": "py",
            "root_id": null,
            "parent_id": null,
            "group": null
        },
        "properties": {
            "correlation_id": task_id.to_string(),
            "reply_to": null,
            "delivery_mode": 2,
            "priority": null
        },
        "content-type": "application/json",
        "content-encoding": "utf-8",
        "body": base64::engine::general_purpose::STANDARD.encode(
            serde_json::to_vec(&json!([args, kwargs, {}])).expect("serialization should not fail")
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
