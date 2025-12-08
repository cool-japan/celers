//! Advanced protocol features example
//!
//! Demonstrates serializers, compression, signing, encryption, and more.
//!
//! Run with: cargo run --example advanced_features --features crypto,compression

use celers_protocol::{
    builder::MessageBuilder,
    negotiation::{detect_protocol, negotiate_protocol},
    result::{ResultMessage, TaskStatus},
    ProtocolVersion,
};
use serde_json::json;
use uuid::Uuid;

fn main() {
    println!("=== Advanced Protocol Features ===\n");

    // Example 1: Protocol version detection
    println!("1. Protocol version detection:");
    let msg = MessageBuilder::new("tasks.test")
        .args(vec![json!(1)])
        .build()
        .unwrap();

    let json_value = serde_json::to_value(&msg).unwrap();
    let detection = detect_protocol(&json_value);
    println!(
        "   Detected protocol: {} (confidence: {:.0}%)\n",
        detection.version,
        detection.confidence * 100.0
    );

    // Example 2: Protocol negotiation
    println!("2. Protocol negotiation:");
    let supported = vec![ProtocolVersion::V2, ProtocolVersion::V5];
    let requested = vec![ProtocolVersion::V5, ProtocolVersion::V2];

    match negotiate_protocol(&supported, &requested) {
        Ok(version) => println!("   Negotiated: {}\n", version),
        Err(e) => println!("   Negotiation error: {}\n", e),
    }

    // Example 3: Result message (success)
    println!("3. Task result message (success):");
    let result = ResultMessage::success(
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        json!({"sum": 42}),
    );

    let json = serde_json::to_string_pretty(&result).unwrap();
    println!("{}\n", json);

    // Example 4: Result message (failure)
    println!("4. Task result message (failure):");
    let failure = ResultMessage::failure_with_traceback(
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap(),
        "ValueError",
        "Invalid input",
        "Traceback:\n  File main.py, line 42\n    raise ValueError()",
    );

    let json = serde_json::to_string_pretty(&failure).unwrap();
    println!("{}\n", json);

    // Example 5: Result message with children (workflow)
    println!("5. Result message with children (workflow):");
    let child1 = Uuid::new_v4();
    let child2 = Uuid::new_v4();

    let workflow_result = ResultMessage::success(
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap(),
        json!("workflow done"),
    )
    .with_children(vec![child1, child2]);

    let json = serde_json::to_string_pretty(&workflow_result).unwrap();
    println!("{}\n", json);

    // Example 6: Task status checking
    println!("6. Task status checking:");
    let statuses = vec![
        TaskStatus::Pending,
        TaskStatus::Started,
        TaskStatus::Success,
        TaskStatus::Failure,
        TaskStatus::Retry,
    ];

    for status in statuses {
        println!(
            "   {}: terminal={}, ready={}",
            status,
            status.is_terminal(),
            status.is_ready()
        );
    }
    println!();

    // Example 7: Message with custom headers
    println!("7. Message with custom headers:");
    let msg = MessageBuilder::new("tasks.custom")
        .args(vec![json!("data")])
        .build()
        .unwrap();

    let mut custom_msg = msg;
    custom_msg
        .headers
        .extra
        .insert("custom_field".to_string(), json!("custom_value"));

    let json = serde_json::to_string_pretty(&custom_msg).unwrap();
    println!("{}\n", json);

    // Example 8: Compression (if enabled)
    #[cfg(feature = "gzip")]
    {
        println!("8. Message compression:");
        use celers_protocol::compression::{CompressionType, Compressor};

        let data = b"Hello, Celery! ".repeat(100);
        let compressor = Compressor::new(CompressionType::Gzip, 6);

        match compressor.compress(data) {
            Ok(compressed) => {
                println!("   Original size: {} bytes", data.len());
                println!("   Compressed size: {} bytes", compressed.len());
                println!(
                    "   Compression ratio: {:.2}%\n",
                    (compressed.len() as f64 / data.len() as f64) * 100.0
                );
            }
            Err(e) => println!("   Compression error: {}\n", e),
        }
    }

    // Example 9: Message signing (if enabled)
    #[cfg(feature = "signing")]
    {
        println!("9. Message signing (HMAC-SHA256):");
        use celers_protocol::auth::MessageSigner;

        let signer = MessageSigner::new(b"secret-key");
        let message = b"important message";

        let signature = signer.sign_hex(message);
        println!("   Message: {:?}", String::from_utf8_lossy(message));
        println!("   Signature: {}", signature);

        let valid = signer.verify_hex(message, &signature);
        println!(
            "   Verification: {}\n",
            if valid { "✓ Valid" } else { "✗ Invalid" }
        );
    }

    // Example 10: Message encryption (if enabled)
    #[cfg(feature = "encryption")]
    {
        println!("10. Message encryption (AES-256-GCM):");
        use celers_protocol::crypto::MessageEncryptor;

        let key = b"12345678901234567890123456789012"; // 32 bytes for AES-256
        let encryptor = MessageEncryptor::new(key);

        let plaintext = b"sensitive data";
        match encryptor.encrypt_hex(plaintext) {
            Ok(encrypted) => {
                println!("   Plaintext: {:?}", String::from_utf8_lossy(plaintext));
                println!("   Encrypted: {}...", &encrypted[..40]);

                match encryptor.decrypt_hex(&encrypted) {
                    Ok(decrypted) => {
                        println!("   Decrypted: {:?}", String::from_utf8_lossy(&decrypted));
                        println!("   Match: {}\n", decrypted == plaintext);
                    }
                    Err(e) => println!("   Decryption error: {}\n", e),
                }
            }
            Err(e) => println!("   Encryption error: {}\n", e),
        }
    }

    println!("=== Advanced Features Complete ===");
    println!("\nFeatures demonstrated:");
    println!("✓ Protocol version detection and negotiation");
    println!("✓ Task result messages (success/failure)");
    println!("✓ Workflow tracking with children");
    println!("✓ Task status checking");
    println!("✓ Custom headers");

    #[cfg(feature = "gzip")]
    println!("✓ Gzip compression");

    #[cfg(feature = "signing")]
    println!("✓ HMAC-SHA256 message signing");

    #[cfg(feature = "encryption")]
    println!("✓ AES-256-GCM encryption");

    println!("\nRun with all features:");
    println!("  cargo run --example advanced_features --all-features");
}
