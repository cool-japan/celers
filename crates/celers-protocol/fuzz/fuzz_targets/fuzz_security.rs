#![no_main]

use libfuzzer_sys::fuzz_target;
use celers_protocol::security::{SecurityPolicy, ContentTypeWhitelist};
use celers_protocol::Message;

fuzz_target!(|data: &[u8]| {
    // Test security validation with arbitrary messages
    if let Ok(msg) = serde_json::from_slice::<Message>(data) {
        let policy = SecurityPolicy::new(
            ContentTypeWhitelist::safe(),
            10 * 1024 * 1024, // 10MB
            false,
        );

        let _ = policy.validate_message(&msg);
        let _ = policy.validate_content_type(&msg.content_type);
        let _ = policy.validate_body_size(msg.body.len());
    }

    // Test content type whitelist
    if let Ok(s) = std::str::from_utf8(data) {
        let whitelist = ContentTypeWhitelist::safe();
        let _ = whitelist.is_allowed(s);
    }
});
