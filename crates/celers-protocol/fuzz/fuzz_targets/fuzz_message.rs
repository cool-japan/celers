#![no_main]

use libfuzzer_sys::fuzz_target;
use celers_protocol::Message;

fuzz_target!(|data: &[u8]| {
    // Try to deserialize arbitrary bytes as a Message
    let _result: Result<Message, _> = serde_json::from_slice(data);

    // If it succeeds, try to validate and serialize back
    if let Ok(msg) = serde_json::from_slice::<Message>(data) {
        let _ = msg.validate();
        let _ = serde_json::to_vec(&msg);

        // Test predicates
        let _ = msg.has_eta();
        let _ = msg.has_expires();
        let _ = msg.has_parent();
        let _ = msg.has_root();
        let _ = msg.has_group();
        let _ = msg.is_persistent();
        let _ = msg.task_id();
        let _ = msg.task_name();
    }
});
