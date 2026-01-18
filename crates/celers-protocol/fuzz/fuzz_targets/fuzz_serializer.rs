#![no_main]

use libfuzzer_sys::fuzz_target;
use celers_protocol::serializer::{JsonSerializer, Serializer};
use serde_json::Value;

fuzz_target!(|data: &[u8]| {
    let serializer = JsonSerializer;

    // Try to deserialize arbitrary bytes
    if let Ok(value) = serializer.deserialize::<Value>(data) {
        // If successful, try to serialize back
        if let Ok(serialized) = serializer.serialize(&value) {
            // Try to deserialize again (round-trip test)
            let _ = serializer.deserialize::<Value>(&serialized);
        }
    }
});
