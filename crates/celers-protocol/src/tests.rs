#![cfg(test)]

use super::*;
use chrono::Utc;
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn test_message_creation() {
    let task_id = Uuid::new_v4();
    let body = serde_json::to_vec(&TaskArgs::new()).expect("Failed to serialize TaskArgs");
    let msg = Message::new("tasks.add".to_string(), task_id, body);

    assert_eq!(msg.headers.task, "tasks.add");
    assert_eq!(msg.headers.id, task_id);
    assert_eq!(msg.headers.lang, "rust");
    assert_eq!(msg.content_type, "application/json");
}

#[test]
fn test_message_with_priority() {
    let task_id = Uuid::new_v4();
    let body = vec![];
    let msg = Message::new("tasks.test".to_string(), task_id, body).with_priority(9);

    assert_eq!(msg.properties.priority, Some(9));
}

#[test]
fn test_task_args() {
    let args = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);

    assert_eq!(args.args.len(), 2);
    assert_eq!(args.kwargs.len(), 0);
}

#[test]
fn test_protocol_version_default() {
    let version = ProtocolVersion::default();
    assert_eq!(version, ProtocolVersion::V2);
}

#[test]
fn test_protocol_version_display() {
    assert_eq!(ProtocolVersion::V2.to_string(), "v2");
    assert_eq!(ProtocolVersion::V5.to_string(), "v5");
}

#[test]
fn test_content_type_as_str() {
    assert_eq!(ContentType::Json.as_str(), "application/json");
    assert_eq!(
        ContentType::Custom("text/plain".to_string()).as_str(),
        "text/plain"
    );
}

#[test]
fn test_content_type_default() {
    let ct = ContentType::default();
    assert_eq!(ct, ContentType::Json);
}

#[test]
fn test_content_type_display() {
    assert_eq!(ContentType::Json.to_string(), "application/json");
    assert_eq!(
        ContentType::Custom("text/xml".to_string()).to_string(),
        "text/xml"
    );
}

#[test]
fn test_content_encoding_as_str() {
    assert_eq!(ContentEncoding::Utf8.as_str(), "utf-8");
    assert_eq!(ContentEncoding::Binary.as_str(), "binary");
    assert_eq!(ContentEncoding::Custom("gzip".to_string()).as_str(), "gzip");
}

#[test]
fn test_content_encoding_default() {
    let ce = ContentEncoding::default();
    assert_eq!(ce, ContentEncoding::Utf8);
}

#[test]
fn test_content_encoding_display() {
    assert_eq!(ContentEncoding::Utf8.to_string(), "utf-8");
    assert_eq!(ContentEncoding::Binary.to_string(), "binary");
}

#[test]
fn test_message_headers_validate_empty_task() {
    let headers = MessageHeaders::new("".to_string(), Uuid::new_v4());
    let result = headers.validate();
    assert!(result.is_err());
    match result {
        Err(ValidationError::EmptyTaskName) => {}
        other => panic!("Expected EmptyTaskName, got: {:?}", other),
    }
}

#[test]
fn test_message_headers_validate_retries_limit() {
    let mut headers = MessageHeaders::new("test".to_string(), Uuid::new_v4());
    headers.retries = Some(1001);
    let result = headers.validate();
    assert!(result.is_err());
    match result {
        Err(ValidationError::RetryLimitExceeded {
            retries: 1001,
            max: 1000,
        }) => {}
        other => panic!("Expected RetryLimitExceeded, got: {:?}", other),
    }
}

#[test]
fn test_message_headers_validate_eta_expires() {
    let mut headers = MessageHeaders::new("test".to_string(), Uuid::new_v4());
    headers.eta = Some(Utc::now() + chrono::Duration::hours(2));
    headers.expires = Some(Utc::now() + chrono::Duration::hours(1));
    let result = headers.validate();
    assert!(result.is_err());
    match result {
        Err(ValidationError::EtaAfterExpiration) => {}
        other => panic!("Expected EtaAfterExpiration, got: {:?}", other),
    }
}

#[test]
fn test_message_properties_validate_delivery_mode() {
    let props = MessageProperties {
        delivery_mode: 3,
        ..MessageProperties::default()
    };
    let result = props.validate();
    assert!(result.is_err());
    match result {
        Err(ValidationError::InvalidDeliveryMode { mode: 3 }) => {}
        other => panic!("Expected InvalidDeliveryMode, got: {:?}", other),
    }
}

#[test]
fn test_message_properties_validate_priority() {
    let props = MessageProperties {
        delivery_mode: 2, // Set valid delivery mode
        priority: Some(10),
        ..MessageProperties::default()
    };
    let result = props.validate();
    assert!(result.is_err());
    match result {
        Err(ValidationError::InvalidPriority { priority: 10 }) => {}
        other => panic!("Expected InvalidPriority, got: {:?}", other),
    }
}

#[test]
fn test_message_predicates() {
    let task_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();
    let root_id = Uuid::new_v4();
    let group_id = Uuid::new_v4();

    let mut msg = Message::new("test".to_string(), task_id, vec![1, 2, 3])
        .with_parent(parent_id)
        .with_root(root_id)
        .with_group(group_id)
        .with_eta(Utc::now() + chrono::Duration::hours(1))
        .with_expires(Utc::now() + chrono::Duration::days(1));

    // Set delivery_mode to 2 for persistence check
    msg.properties.delivery_mode = 2;

    assert!(msg.has_parent());
    assert!(msg.has_root());
    assert!(msg.has_group());
    assert!(msg.has_eta());
    assert!(msg.has_expires());
    assert!(msg.is_persistent());
}

#[test]
fn test_message_accessors() {
    let task_id = Uuid::new_v4();
    let msg = Message::new("my_task".to_string(), task_id, vec![1, 2, 3]);

    assert_eq!(msg.task_id(), task_id);
    assert_eq!(msg.task_name(), "my_task");
}

#[test]
fn test_task_args_add_methods() {
    let mut args = TaskArgs::new();
    assert!(args.is_empty());

    args.add_arg(serde_json::json!(1));
    args.add_arg(serde_json::json!(2));
    assert_eq!(args.len(), 2);
    assert!(args.has_args());
    assert!(!args.has_kwargs());

    args.add_kwarg("key1".to_string(), serde_json::json!("value1"));
    assert_eq!(args.len(), 3);
    assert!(args.has_kwargs());
}

#[test]
fn test_task_args_get_methods() {
    let mut args = TaskArgs::new();
    args.add_arg(serde_json::json!(42));
    args.add_kwarg("name".to_string(), serde_json::json!("test"));

    assert_eq!(args.get_arg(0), Some(&serde_json::json!(42)));
    assert_eq!(args.get_arg(1), None);
    assert_eq!(args.get_kwarg("name"), Some(&serde_json::json!("test")));
    assert_eq!(args.get_kwarg("missing"), None);
}

#[test]
fn test_task_args_clear() {
    let mut args = TaskArgs::new()
        .with_args(vec![serde_json::json!(1)])
        .with_kwargs({
            let mut map = HashMap::new();
            map.insert("key".to_string(), serde_json::json!("value"));
            map
        });

    assert!(!args.is_empty());
    args.clear();
    assert!(args.is_empty());
    assert_eq!(args.len(), 0);
}

#[test]
fn test_task_args_partial_eq() {
    let args1 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    let args2 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    let args3 = TaskArgs::new().with_args(vec![serde_json::json!(1)]);

    assert_eq!(args1, args2);
    assert_ne!(args1, args3);
}

#[test]
fn test_message_body_methods() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3, 4, 5]);
    assert_eq!(msg.body_size(), 5);
    assert!(!msg.has_empty_body());

    let empty_msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
    assert_eq!(empty_msg.body_size(), 0);
    assert!(empty_msg.has_empty_body());
}

#[test]
fn test_message_content_accessors() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
    assert_eq!(msg.content_type_str(), "application/json");
    assert_eq!(msg.content_encoding_str(), "utf-8");
}

#[test]
fn test_message_retry_and_priority() {
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
    assert_eq!(msg.retry_count(), 0);
    assert_eq!(msg.priority(), None);

    msg.headers.retries = Some(3);
    msg.properties.priority = Some(5);
    assert_eq!(msg.retry_count(), 3);
    assert_eq!(msg.priority(), Some(5));
}

#[test]
fn test_message_correlation_and_reply() {
    let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
    assert!(!msg.has_correlation_id());
    assert_eq!(msg.correlation_id(), None);
    assert_eq!(msg.reply_to(), None);

    msg.properties.correlation_id = Some("corr-123".to_string());
    msg.properties.reply_to = Some("reply-queue".to_string());
    assert!(msg.has_correlation_id());
    assert_eq!(msg.correlation_id(), Some("corr-123"));
    assert_eq!(msg.reply_to(), Some("reply-queue"));
}

#[test]
fn test_message_workflow_check() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
    assert!(!msg.is_workflow_message());

    let workflow_msg = msg.with_parent(Uuid::new_v4());
    assert!(workflow_msg.is_workflow_message());
}

#[test]
fn test_message_with_new_id() {
    let task_id = Uuid::new_v4();
    let msg = Message::new("test".to_string(), task_id, vec![1, 2, 3]);
    let new_msg = msg.with_new_id();

    assert_ne!(msg.task_id(), new_msg.task_id());
    assert_eq!(msg.task_name(), new_msg.task_name());
    assert_eq!(msg.body, new_msg.body);
}

#[test]
fn test_message_to_builder() {
    let task_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();
    let msg = Message::new("test.task".to_string(), task_id, vec![1, 2, 3])
        .with_priority(5)
        .with_parent(parent_id);

    let builder = msg.to_builder();
    // Need to add args since builder doesn't copy body automatically
    let rebuilt = builder
        .args(vec![serde_json::json!(1)])
        .build()
        .expect("Failed to build message");

    assert_eq!(rebuilt.task_id(), msg.task_id());
    assert_eq!(rebuilt.task_name(), msg.task_name());
    assert_eq!(rebuilt.priority(), msg.priority());
    assert_eq!(rebuilt.headers.parent_id, msg.headers.parent_id);
}

#[test]
fn test_protocol_version_from_str() {
    use std::str::FromStr;
    assert_eq!(
        ProtocolVersion::from_str("v2").expect("parse v2"),
        ProtocolVersion::V2
    );
    assert_eq!(
        ProtocolVersion::from_str("V2").expect("parse V2"),
        ProtocolVersion::V2
    );
    assert_eq!(
        ProtocolVersion::from_str("2").expect("parse 2"),
        ProtocolVersion::V2
    );
    assert_eq!(
        ProtocolVersion::from_str("v5").expect("parse v5"),
        ProtocolVersion::V5
    );
    assert_eq!(
        ProtocolVersion::from_str("V5").expect("parse V5"),
        ProtocolVersion::V5
    );
    assert_eq!(
        ProtocolVersion::from_str("5").expect("parse 5"),
        ProtocolVersion::V5
    );
    assert!(ProtocolVersion::from_str("v3").is_err());
    assert!(ProtocolVersion::from_str("invalid").is_err());
}

#[test]
fn test_protocol_version_ordering() {
    assert!(ProtocolVersion::V2 < ProtocolVersion::V5);
    assert!(ProtocolVersion::V5 > ProtocolVersion::V2);
    assert_eq!(ProtocolVersion::V2, ProtocolVersion::V2);
}

#[test]
fn test_content_type_from_str() {
    use std::str::FromStr;
    assert_eq!(
        ContentType::from_str("application/json").expect("parse json"),
        ContentType::Json
    );
    assert_eq!(
        ContentType::from_str("text/plain").expect("parse text/plain"),
        ContentType::Custom("text/plain".to_string())
    );
}

#[test]
fn test_content_encoding_from_str() {
    use std::str::FromStr;
    assert_eq!(
        ContentEncoding::from_str("utf-8").expect("parse utf-8"),
        ContentEncoding::Utf8
    );
    assert_eq!(
        ContentEncoding::from_str("binary").expect("parse binary"),
        ContentEncoding::Binary
    );
    assert_eq!(
        ContentEncoding::from_str("gzip").expect("parse gzip"),
        ContentEncoding::Custom("gzip".to_string())
    );
}

#[test]
fn test_message_headers_equality() {
    let id = Uuid::new_v4();
    let headers1 = MessageHeaders::new("tasks.add".to_string(), id);
    let headers2 = MessageHeaders::new("tasks.add".to_string(), id);
    let headers3 = MessageHeaders::new("tasks.sub".to_string(), id);

    assert_eq!(headers1, headers2);
    assert_ne!(headers1, headers3);
}

#[test]
fn test_message_properties_equality() {
    let props1 = MessageProperties::default();
    let props2 = MessageProperties::default();
    let props3 = MessageProperties {
        priority: Some(5),
        ..Default::default()
    };

    assert_eq!(props1, props2);
    assert_ne!(props1, props3);
}

#[test]
fn test_message_equality() {
    let id = Uuid::new_v4();
    let body = vec![1, 2, 3];
    let msg1 = Message::new("tasks.add".to_string(), id, body.clone());
    let msg2 = Message::new("tasks.add".to_string(), id, body.clone());
    let msg3 = Message::new("tasks.add".to_string(), id, vec![4, 5, 6]);

    assert_eq!(msg1, msg2);
    assert_ne!(msg1, msg3);
}

#[test]
fn test_message_equality_with_options() {
    let id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();
    let body = vec![1, 2, 3];

    let msg1 = Message::new("tasks.add".to_string(), id, body.clone())
        .with_priority(5)
        .with_parent(parent_id);
    let msg2 = Message::new("tasks.add".to_string(), id, body.clone())
        .with_priority(5)
        .with_parent(parent_id);
    let msg3 = Message::new("tasks.add".to_string(), id, body.clone())
        .with_priority(3)
        .with_parent(parent_id);

    assert_eq!(msg1, msg2);
    assert_ne!(msg1, msg3);
}

#[test]
fn test_task_args_equality() {
    let args1 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    let args2 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    let args3 = TaskArgs::new().with_args(vec![serde_json::json!(3), serde_json::json!(4)]);

    assert_eq!(args1, args2);
    assert_ne!(args1, args3);
}

#[test]
fn test_task_args_equality_with_kwargs() {
    let mut kwargs1 = std::collections::HashMap::new();
    kwargs1.insert("key".to_string(), serde_json::json!("value"));

    let mut kwargs2 = std::collections::HashMap::new();
    kwargs2.insert("key".to_string(), serde_json::json!("value"));

    let args1 = TaskArgs::new().with_kwargs(kwargs1);
    let args2 = TaskArgs::new().with_kwargs(kwargs2);
    let args3 = TaskArgs::new();

    assert_eq!(args1, args2);
    assert_ne!(args1, args3);
}

#[test]
fn test_content_type_from_str_trait() {
    let ct1: ContentType = "application/json".into();
    let ct2: ContentType = "text/plain".into();

    assert_eq!(ct1, ContentType::Json);
    assert_eq!(ct2, ContentType::Custom("text/plain".to_string()));
}

#[test]
fn test_content_encoding_from_str_trait() {
    let ce1: ContentEncoding = "utf-8".into();
    let ce2: ContentEncoding = "binary".into();
    let ce3: ContentEncoding = "gzip".into();

    assert_eq!(ce1, ContentEncoding::Utf8);
    assert_eq!(ce2, ContentEncoding::Binary);
    assert_eq!(ce3, ContentEncoding::Custom("gzip".to_string()));
}

#[test]
fn test_content_type_as_ref() {
    let ct = ContentType::Json;
    let s: &str = ct.as_ref();
    assert_eq!(s, "application/json");

    let ct_custom = ContentType::Custom("text/plain".to_string());
    let s_custom: &str = ct_custom.as_ref();
    assert_eq!(s_custom, "text/plain");
}

#[test]
fn test_content_encoding_as_ref() {
    let ce = ContentEncoding::Utf8;
    let s: &str = ce.as_ref();
    assert_eq!(s, "utf-8");

    let ce_binary = ContentEncoding::Binary;
    let s_binary: &str = ce_binary.as_ref();
    assert_eq!(s_binary, "binary");
}

#[test]
fn test_content_type_hash() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(ContentType::Json);
    set.insert(ContentType::Json); // Duplicate
    #[cfg(feature = "msgpack")]
    set.insert(ContentType::MessagePack);
    set.insert(ContentType::Custom("text/plain".to_string()));

    #[cfg(feature = "msgpack")]
    assert_eq!(set.len(), 3);
    #[cfg(not(feature = "msgpack"))]
    assert_eq!(set.len(), 2);

    assert!(set.contains(&ContentType::Json));
}

#[test]
fn test_content_encoding_hash() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(ContentEncoding::Utf8);
    set.insert(ContentEncoding::Utf8); // Duplicate
    set.insert(ContentEncoding::Binary);
    set.insert(ContentEncoding::Custom("base64".to_string()));

    assert_eq!(set.len(), 3);
    assert!(set.contains(&ContentEncoding::Utf8));
    assert!(set.contains(&ContentEncoding::Binary));
}

#[test]
fn test_message_with_retries() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]).with_retries(5);

    assert_eq!(msg.headers.retries, Some(5));
    assert_eq!(msg.retry_count(), 5);
}

#[test]
fn test_message_with_correlation_id() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1])
        .with_correlation_id("corr-123".to_string());

    assert_eq!(msg.properties.correlation_id, Some("corr-123".to_string()));
    assert_eq!(msg.correlation_id(), Some("corr-123"));
}

#[test]
fn test_message_with_reply_to() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1])
        .with_reply_to("reply-queue".to_string());

    assert_eq!(msg.properties.reply_to, Some("reply-queue".to_string()));
    assert_eq!(msg.reply_to(), Some("reply-queue"));
}

#[test]
fn test_message_with_delivery_mode() {
    let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]).with_delivery_mode(1);

    assert_eq!(msg.properties.delivery_mode, 1);
    assert!(!msg.is_persistent());

    let persistent_msg =
        Message::new("test".to_string(), Uuid::new_v4(), vec![1]).with_delivery_mode(2);

    assert_eq!(persistent_msg.properties.delivery_mode, 2);
    assert!(persistent_msg.is_persistent());
}

#[test]
fn test_message_builder_chaining() {
    let parent_id = Uuid::new_v4();
    let msg = Message::new("test.task".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_priority(7)
        .with_retries(3)
        .with_correlation_id("corr-456".to_string())
        .with_reply_to("result-queue".to_string())
        .with_parent(parent_id)
        .with_delivery_mode(1);

    assert_eq!(msg.priority(), Some(7));
    assert_eq!(msg.retry_count(), 3);
    assert_eq!(msg.correlation_id(), Some("corr-456"));
    assert_eq!(msg.reply_to(), Some("result-queue"));
    assert_eq!(msg.headers.parent_id, Some(parent_id));
    assert_eq!(msg.properties.delivery_mode, 1);
    assert!(!msg.is_persistent());
}

#[test]
fn test_protocol_version_is_v2() {
    assert!(ProtocolVersion::V2.is_v2());
    assert!(!ProtocolVersion::V5.is_v2());
}

#[test]
fn test_protocol_version_is_v5() {
    assert!(ProtocolVersion::V5.is_v5());
    assert!(!ProtocolVersion::V2.is_v5());
}

#[test]
fn test_protocol_version_as_u8() {
    assert_eq!(ProtocolVersion::V2.as_u8(), 2);
    assert_eq!(ProtocolVersion::V5.as_u8(), 5);
}

#[test]
fn test_protocol_version_as_number_str() {
    assert_eq!(ProtocolVersion::V2.as_number_str(), "2");
    assert_eq!(ProtocolVersion::V5.as_number_str(), "5");
}

#[test]
fn test_task_args_from_json() {
    let json = r#"{"args":[1,2,3],"kwargs":{"key":"value"}}"#;
    let args = TaskArgs::from_json(json).expect("Failed to parse TaskArgs from JSON");

    assert_eq!(args.args.len(), 3);
    assert_eq!(args.kwargs.len(), 1);
    assert_eq!(args.get_kwarg("key"), Some(&serde_json::json!("value")));
}

#[test]
fn test_task_args_to_json() {
    let mut args = TaskArgs::new();
    args.add_arg(serde_json::json!(1));
    args.add_arg(serde_json::json!(2));
    args.add_kwarg("key".to_string(), serde_json::json!("value"));

    let json = args.to_json().expect("Failed to serialize TaskArgs");
    assert!(json.contains("\"args\""));
    assert!(json.contains("\"kwargs\""));
    assert!(json.contains("\"key\""));
}

#[test]
fn test_task_args_to_json_pretty() {
    let args = TaskArgs::new()
        .with_args(vec![serde_json::json!(1)])
        .with_kwargs({
            let mut map = std::collections::HashMap::new();
            map.insert("test".to_string(), serde_json::json!("value"));
            map
        });

    let json_pretty = args
        .to_json_pretty()
        .expect("Failed to serialize TaskArgs pretty");
    assert!(json_pretty.contains('\n')); // Should have newlines
}

#[test]
fn test_message_is_ready_for_execution() {
    let msg1 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
    assert!(msg1.is_ready_for_execution()); // No ETA

    let msg2 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_eta(chrono::Utc::now() - chrono::Duration::hours(1));
    assert!(msg2.is_ready_for_execution()); // Past ETA

    let msg3 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_eta(chrono::Utc::now() + chrono::Duration::hours(1));
    assert!(!msg3.is_ready_for_execution()); // Future ETA
}

#[test]
fn test_message_is_not_expired() {
    let msg1 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
    assert!(msg1.is_not_expired()); // No expiration

    let msg2 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_expires(chrono::Utc::now() + chrono::Duration::hours(1));
    assert!(msg2.is_not_expired()); // Future expiration

    let msg3 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
    assert!(!msg3.is_not_expired()); // Past expiration
}

#[test]
fn test_message_should_process() {
    // Ready and not expired
    let msg1 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
    assert!(msg1.should_process());

    // Not ready (future ETA)
    let msg2 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_eta(chrono::Utc::now() + chrono::Duration::hours(1));
    assert!(!msg2.should_process());

    // Expired
    let msg3 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
    assert!(!msg3.should_process());

    // Ready but expired
    let msg4 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
        .with_eta(chrono::Utc::now() - chrono::Duration::hours(2))
        .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
    assert!(!msg4.should_process());
}

#[test]
fn test_message_headers_builder() {
    let task_id = Uuid::new_v4();
    let root_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();
    let group_id = Uuid::new_v4();

    let headers = MessageHeaders::new("task".to_string(), task_id)
        .with_lang("python".to_string())
        .with_root_id(root_id)
        .with_parent_id(parent_id)
        .with_group(group_id)
        .with_retries(3)
        .with_eta(chrono::Utc::now() + chrono::Duration::minutes(5))
        .with_expires(chrono::Utc::now() + chrono::Duration::hours(1));

    assert_eq!(headers.lang, "python");
    assert_eq!(headers.root_id, Some(root_id));
    assert_eq!(headers.parent_id, Some(parent_id));
    assert_eq!(headers.group, Some(group_id));
    assert_eq!(headers.retries, Some(3));
    assert!(headers.eta.is_some());
    assert!(headers.expires.is_some());
}

#[test]
fn test_message_properties_builder() {
    let props = MessageProperties::new()
        .with_correlation_id("corr-123".to_string())
        .with_reply_to("reply.queue".to_string())
        .with_delivery_mode(1)
        .with_priority(5);

    assert_eq!(props.correlation_id, Some("corr-123".to_string()));
    assert_eq!(props.reply_to, Some("reply.queue".to_string()));
    assert_eq!(props.delivery_mode, 1);
    assert_eq!(props.priority, Some(5));
}

#[test]
fn test_message_with_eta_delay() {
    let before = chrono::Utc::now();
    let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
        .with_eta_delay(chrono::Duration::minutes(10));
    let after = chrono::Utc::now();

    assert!(msg.has_eta());
    if let Some(eta) = msg.headers.eta {
        // ETA should be roughly 10 minutes from now
        assert!(eta > before + chrono::Duration::minutes(9));
        assert!(eta < after + chrono::Duration::minutes(11));
    } else {
        panic!("Expected ETA to be set");
    }
}

#[test]
fn test_message_with_expires_in() {
    let before = chrono::Utc::now();
    let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
        .with_expires_in(chrono::Duration::hours(2));
    let after = chrono::Utc::now();

    assert!(msg.has_expires());
    if let Some(expires) = msg.headers.expires {
        // Expiration should be roughly 2 hours from now
        assert!(expires > before + chrono::Duration::hours(2) - chrono::Duration::seconds(1));
        assert!(expires < after + chrono::Duration::hours(2) + chrono::Duration::seconds(1));
    } else {
        panic!("Expected expires to be set");
    }
}

#[test]
fn test_message_time_until_eta() {
    // No ETA
    let msg1 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
    assert!(msg1.time_until_eta().is_none());

    // Future ETA
    let msg2 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
        .with_eta(chrono::Utc::now() + chrono::Duration::minutes(30));
    let time_left = msg2.time_until_eta();
    assert!(time_left.is_some());
    if let Some(tl) = time_left {
        assert!(tl > chrono::Duration::minutes(29));
        assert!(tl < chrono::Duration::minutes(31));
    }

    // Past ETA
    let msg3 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
        .with_eta(chrono::Utc::now() - chrono::Duration::minutes(30));
    assert!(msg3.time_until_eta().is_none());
}

#[test]
fn test_message_time_until_expiration() {
    // No expiration
    let msg1 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
    assert!(msg1.time_until_expiration().is_none());

    // Future expiration
    let msg2 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
        .with_expires(chrono::Utc::now() + chrono::Duration::hours(1));
    let time_left = msg2.time_until_expiration();
    assert!(time_left.is_some());
    if let Some(tl) = time_left {
        assert!(tl > chrono::Duration::minutes(59));
        assert!(tl < chrono::Duration::minutes(61));
    }

    // Past expiration
    let msg3 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
        .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
    assert!(msg3.time_until_expiration().is_none());
}

#[test]
fn test_message_increment_retry() {
    let mut msg = Message::new("task".to_string(), Uuid::new_v4(), vec![]);

    // Initial retry count is 0
    assert_eq!(msg.retry_count(), 0);

    // Increment to 1
    let count1 = msg.increment_retry();
    assert_eq!(count1, 1);
    assert_eq!(msg.retry_count(), 1);

    // Increment to 2
    let count2 = msg.increment_retry();
    assert_eq!(count2, 2);
    assert_eq!(msg.retry_count(), 2);
}

#[test]
fn test_task_args_index_usize() {
    let args = TaskArgs::new().with_args(vec![
        serde_json::json!(1),
        serde_json::json!("hello"),
        serde_json::json!(true),
    ]);

    // Test Index trait
    assert_eq!(args[0], serde_json::json!(1));
    assert_eq!(args[1], serde_json::json!("hello"));
    assert_eq!(args[2], serde_json::json!(true));
}

#[test]
fn test_task_args_index_mut_usize() {
    let mut args = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);

    // Test IndexMut trait
    args[0] = serde_json::json!(100);
    args[1] = serde_json::json!(200);

    assert_eq!(args[0], serde_json::json!(100));
    assert_eq!(args[1], serde_json::json!(200));
}

#[test]
fn test_task_args_index_str() {
    let mut kwargs = HashMap::new();
    kwargs.insert("name".to_string(), serde_json::json!("Alice"));
    kwargs.insert("age".to_string(), serde_json::json!(30));

    let args = TaskArgs::new().with_kwargs(kwargs);

    // Test Index trait with string keys
    assert_eq!(args["name"], serde_json::json!("Alice"));
    assert_eq!(args["age"], serde_json::json!(30));
}

#[test]
#[should_panic(expected = "no entry found for key")]
fn test_task_args_index_str_panic() {
    let args = TaskArgs::new();
    let _ = &args["nonexistent"]; // Should panic
}

#[test]
fn test_task_args_into_iterator() {
    let args = TaskArgs::new().with_args(vec![
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!(3),
    ]);

    // Test IntoIterator
    let values: Vec<_> = args.into_iter().collect();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0], serde_json::json!(1));
    assert_eq!(values[1], serde_json::json!(2));
    assert_eq!(values[2], serde_json::json!(3));
}

#[test]
fn test_task_args_into_iterator_ref() {
    let args = TaskArgs::new().with_args(vec![serde_json::json!(10), serde_json::json!(20)]);

    // Test IntoIterator for &TaskArgs
    let sum: i64 = (&args).into_iter().filter_map(|v| v.as_i64()).sum();

    assert_eq!(sum, 30);
    // args is still usable
    assert_eq!(args.args.len(), 2);
}

#[test]
fn test_task_args_extend() {
    let mut args = TaskArgs::new().with_args(vec![serde_json::json!(1)]);

    // Test Extend trait with positional args
    args.extend(vec![serde_json::json!(2), serde_json::json!(3)]);

    assert_eq!(args.args.len(), 3);
    assert_eq!(args[0], serde_json::json!(1));
    assert_eq!(args[1], serde_json::json!(2));
    assert_eq!(args[2], serde_json::json!(3));
}

#[test]
fn test_task_args_extend_kwargs() {
    let mut args = TaskArgs::new();

    // Test Extend trait with key-value pairs
    args.extend(vec![
        ("key1".to_string(), serde_json::json!("value1")),
        ("key2".to_string(), serde_json::json!(42)),
    ]);

    assert_eq!(args.kwargs.len(), 2);
    assert_eq!(args["key1"], serde_json::json!("value1"));
    assert_eq!(args["key2"], serde_json::json!(42));
}

#[test]
fn test_task_args_from_iterator() {
    // Test FromIterator trait
    let args: TaskArgs = vec![
        serde_json::json!(1),
        serde_json::json!("hello"),
        serde_json::json!(true),
    ]
    .into_iter()
    .collect();

    assert_eq!(args.args.len(), 3);
    assert_eq!(args.kwargs.len(), 0);
    assert_eq!(args[0], serde_json::json!(1));
    assert_eq!(args[1], serde_json::json!("hello"));
    assert_eq!(args[2], serde_json::json!(true));
}

#[test]
fn test_task_args_from_iterator_range() {
    // Build TaskArgs from range using FromIterator
    let args: TaskArgs = (1..=5).map(|i| serde_json::json!(i)).collect();

    assert_eq!(args.args.len(), 5);
    assert_eq!(args[0], serde_json::json!(1));
    assert_eq!(args[4], serde_json::json!(5));
}

#[test]
fn test_task_args_iterator_chain() {
    // Test combining traits: FromIterator, IntoIterator, Extend
    let args1: TaskArgs = vec![serde_json::json!(1), serde_json::json!(2)]
        .into_iter()
        .collect();

    let mut args2 = TaskArgs::new();
    args2.extend(vec![serde_json::json!(3), serde_json::json!(4)]);

    // Extend args2 with args1's values
    args2.extend(args1);

    assert_eq!(args2.args.len(), 4);
    assert_eq!(args2[0], serde_json::json!(3));
    assert_eq!(args2[3], serde_json::json!(2));
}
