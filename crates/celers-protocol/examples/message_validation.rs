//! Message validation example
//!
//! Demonstrates message validation, security policies, and content-type whitelists.
//!
//! Run with: cargo run --example message_validation

use celers_protocol::{
    builder::MessageBuilder,
    security::{ContentTypeWhitelist, SecurityPolicy},
};
use serde_json::json;

fn main() {
    println!("=== Message Validation Example ===\n");

    // Example 1: Basic validation
    println!("1. Basic message validation:");
    let msg = MessageBuilder::new("tasks.validate")
        .args(vec![json!("data")])
        .build()
        .unwrap();

    match msg.validate() {
        Ok(_) => println!("✓ Message is valid\n"),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    // Example 2: Invalid message (empty task name)
    println!("2. Invalid message (empty task name):");
    let mut invalid_msg = MessageBuilder::new("tasks.test").build().unwrap();
    invalid_msg.headers.task = "".to_string();

    match invalid_msg.validate() {
        Ok(_) => println!("✓ Message is valid\n"),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    // Example 3: Invalid priority
    println!("3. Invalid priority:");
    let mut priority_msg = MessageBuilder::new("tasks.test").build().unwrap();
    priority_msg.properties.priority = Some(15); // Max is 9

    match priority_msg.validate() {
        Ok(_) => println!("✓ Message is valid\n"),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    // Example 4: Content-type whitelist (safe mode)
    println!("4. Content-type whitelist (safe mode):");
    let whitelist = ContentTypeWhitelist::safe();

    let json_valid = whitelist.is_allowed("application/json");
    let pickle_valid = whitelist.is_allowed("application/x-python-serialize");

    println!("  JSON allowed: {}", json_valid);
    println!("  Pickle allowed: {} (security risk!)\n", pickle_valid);

    // Example 5: Strict content-type whitelist
    println!("5. Strict whitelist (JSON only):");
    let strict = ContentTypeWhitelist::strict();

    println!("  JSON allowed: {}", strict.is_allowed("application/json"));
    println!(
        "  MessagePack allowed: {}\n",
        strict.is_allowed("application/x-msgpack")
    );

    // Example 6: Security policy
    println!("6. Security policy validation:");
    let policy = SecurityPolicy::standard();

    let msg = MessageBuilder::new("tasks.secure")
        .args(vec![json!("data")])
        .build()
        .unwrap();

    match policy.validate_message(&msg.content_type, msg.body.len(), &msg.headers.task) {
        Ok(_) => println!("✓ Message passed security policy\n"),
        Err(e) => println!("✗ Security error: {}\n", e),
    }

    // Example 7: Size limit validation
    println!("7. Message size limit:");
    let large_data = "x".repeat(1_000_000); // 1MB
    let large_msg = MessageBuilder::new("tasks.large")
        .args(vec![json!(large_data)])
        .build()
        .unwrap();

    match large_msg.validate() {
        Ok(_) => println!(
            "✓ Large message is valid (size: {} bytes)\n",
            large_msg.body.len()
        ),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    // Example 8: Custom size limit
    println!("8. Custom size limit (1KB max):");
    let msg = MessageBuilder::new("tasks.test")
        .args(vec![json!("x".repeat(5000))])
        .build()
        .unwrap();

    match msg.validate_with_limit(1024) {
        Ok(_) => println!("✓ Message within limit\n"),
        Err(e) => println!("✗ Size error: {}\n", e),
    }

    // Example 9: ETA and expiration validation
    println!("9. ETA/Expiration validation:");
    use chrono::{Duration, Utc};

    let valid_msg = MessageBuilder::new("tasks.scheduled")
        .eta(Utc::now() + Duration::hours(1))
        .expires_in(Duration::hours(2))
        .build()
        .unwrap();

    match valid_msg.validate() {
        Ok(_) => println!("✓ ETA before expiration\n"),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    // Example 10: Invalid ETA/expiration
    println!("10. Invalid ETA/Expiration (ETA after expiration):");
    let mut invalid_eta = MessageBuilder::new("tasks.invalid").build().unwrap();

    invalid_eta.headers.eta = Some(Utc::now() + Duration::hours(2));
    invalid_eta.headers.expires = Some(Utc::now() + Duration::hours(1));

    match invalid_eta.validate() {
        Ok(_) => println!("✓ Valid\n"),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    // Example 11: Retry limit validation
    println!("11. Retry limit validation:");
    let mut high_retries = MessageBuilder::new("tasks.retry").build().unwrap();
    high_retries.headers.retries = Some(1001); // Max is 1000

    match high_retries.validate() {
        Ok(_) => println!("✓ Valid\n"),
        Err(e) => println!("✗ Validation error: {}\n", e),
    }

    println!("=== Validation Complete ===");
    println!("\nBest practices:");
    println!("- Always validate messages before sending");
    println!("- Use content-type whitelists to block unsafe formats (pickle)");
    println!("- Enforce size limits to prevent memory issues");
    println!("- Use security policies for additional checks");
}
