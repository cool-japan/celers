//! Serialization formats example
//!
//! Demonstrates different serialization formats: JSON, MessagePack, BSON, YAML.
//!
//! Run with: cargo run --example serialization_formats --all-features

#[cfg(feature = "msgpack")]
use celers_protocol::builder::MessageBuilder;
use celers_protocol::{Message, TaskArgs};
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

fn main() {
    println!("=== Serialization Formats Example ===\n");

    // Create test data
    let task_id = Uuid::new_v4();
    let mut kwargs = HashMap::new();
    kwargs.insert("name".to_string(), json!("test"));
    kwargs.insert("value".to_string(), json!(42));

    let args = TaskArgs::new()
        .with_args(vec![json!(1), json!(2), json!(3)])
        .with_kwargs(kwargs);

    // Example 1: JSON serialization (default)
    println!("1. JSON Serialization (default):");
    let body = serde_json::to_vec(&args).unwrap();
    let msg = Message::new("tasks.test".to_string(), task_id, body.clone());
    println!("   Content-Type: {}", msg.content_type);
    println!("   Body size: {} bytes", msg.body.len());
    println!(
        "   Body: {}",
        String::from_utf8_lossy(&msg.body[..msg.body.len().min(100)])
    );

    // Deserialize back
    let deserialized: TaskArgs = serde_json::from_slice(&msg.body).unwrap();
    println!("   Args count: {}", deserialized.args.len());
    println!("   Kwargs count: {}\n", deserialized.kwargs.len());

    // Example 2: MessagePack serialization (optional)
    #[cfg(feature = "msgpack")]
    {
        println!("2. MessagePack Serialization:");
        use celers_protocol::serializer::{MessagePackSerializer, Serializer};

        let serializer = MessagePackSerializer;
        match serializer.serialize(&args) {
            Ok(msgpack_body) => {
                println!("   Content-Type: {}", serializer.content_type());
                println!("   Body size: {} bytes", msgpack_body.len());
                println!(
                    "   Compression ratio: {:.1}%",
                    (msgpack_body.len() as f64 / body.len() as f64) * 100.0
                );

                // Deserialize back
                match serializer.deserialize::<TaskArgs>(&msgpack_body) {
                    Ok(deserialized) => {
                        println!("   ✓ Deserialization successful");
                        println!("   Args count: {}", deserialized.args.len());
                        println!("   Kwargs count: {}\n", deserialized.kwargs.len());
                    }
                    Err(e) => println!("   ✗ Deserialization error: {}\n", e),
                }
            }
            Err(e) => println!("   Serialization error: {}\n", e),
        }
    }
    #[cfg(not(feature = "msgpack"))]
    println!("2. MessagePack: Not enabled (use --features msgpack)\n");

    // Example 3: BSON serialization (optional)
    #[cfg(feature = "bson-format")]
    {
        println!("3. BSON Serialization:");
        use celers_protocol::serializer::{BsonSerializer, Serializer};

        let serializer = BsonSerializer;
        match serializer.serialize(&args) {
            Ok(bson_body) => {
                println!("   Content-Type: {}", serializer.content_type());
                println!("   Body size: {} bytes", bson_body.len());
                println!(
                    "   Size vs JSON: {:.1}%",
                    (bson_body.len() as f64 / body.len() as f64) * 100.0
                );

                // Deserialize back
                match serializer.deserialize::<TaskArgs>(&bson_body) {
                    Ok(deserialized) => {
                        println!("   ✓ Deserialization successful");
                        println!("   Args count: {}", deserialized.args.len());
                        println!("   Kwargs count: {}\n", deserialized.kwargs.len());
                    }
                    Err(e) => println!("   ✗ Deserialization error: {}\n", e),
                }
            }
            Err(e) => println!("   Serialization error: {}\n", e),
        }
    }
    #[cfg(not(feature = "bson-format"))]
    println!("3. BSON: Not enabled (use --features bson-format)\n");

    // Example 4: YAML serialization (optional)
    #[cfg(feature = "yaml")]
    {
        println!("4. YAML Serialization:");
        use celers_protocol::serializer::{Serializer, YamlSerializer};

        let serializer = YamlSerializer;
        match serializer.serialize(&args) {
            Ok(yaml_body) => {
                println!("   Content-Type: {}", serializer.content_type());
                println!("   Body size: {} bytes", yaml_body.len());
                println!("   Body:\n{}\n", String::from_utf8_lossy(&yaml_body));

                // Deserialize back
                match serializer.deserialize::<TaskArgs>(&yaml_body) {
                    Ok(deserialized) => {
                        println!("   ✓ Deserialization successful");
                        println!("   Args count: {}", deserialized.args.len());
                        println!("   Kwargs count: {}\n", deserialized.kwargs.len());
                    }
                    Err(e) => println!("   ✗ Deserialization error: {}\n", e),
                }
            }
            Err(e) => println!("   Serialization error: {}\n", e),
        }
    }
    #[cfg(not(feature = "yaml"))]
    println!("4. YAML: Not enabled (use --features yaml)\n");

    // Example 5: Using MessageBuilder with different formats
    println!("5. MessageBuilder with custom serialization:");
    #[cfg(feature = "msgpack")]
    {
        let msg = MessageBuilder::new("tasks.msgpack")
            .args(vec![json!(1), json!(2)])
            .build()
            .unwrap();

        println!("   Task: {}", msg.task_name());
        println!("   Content-Type: {}\n", msg.content_type);
    }
    #[cfg(not(feature = "msgpack"))]
    println!("   (Requires msgpack feature)\n");

    // Example 6: Format comparison
    println!("6. Format Comparison:");
    println!("   Format       | Size  | Binary | Human-readable | Celery Compat");
    println!("   -------------+-------+--------+----------------+--------------");
    println!(
        "   JSON         | {}    | No     | Yes            | ✓ Default",
        body.len()
    );

    #[cfg(feature = "msgpack")]
    {
        use celers_protocol::serializer::{MessagePackSerializer, Serializer};
        let mp = MessagePackSerializer.serialize(&args).unwrap();
        println!(
            "   MessagePack  | {}    | Yes    | No             | ✓ Optional",
            mp.len()
        );
    }
    #[cfg(not(feature = "msgpack"))]
    println!("   MessagePack  | N/A   | Yes    | No             | ✓ Optional");

    #[cfg(feature = "bson-format")]
    {
        use celers_protocol::serializer::{BsonSerializer, Serializer};
        let bs = BsonSerializer.serialize(&args).unwrap();
        println!(
            "   BSON         | {}   | Yes    | No             | ✗ Custom",
            bs.len()
        );
    }
    #[cfg(not(feature = "bson-format"))]
    println!("   BSON         | N/A   | Yes    | No             | ✗ Custom");

    #[cfg(feature = "yaml")]
    {
        use celers_protocol::serializer::{Serializer, YamlSerializer};
        let ym = YamlSerializer.serialize(&args).unwrap();
        println!(
            "   YAML         | {}   | No     | Yes            | ✗ Custom",
            ym.len()
        );
    }
    #[cfg(not(feature = "yaml"))]
    println!("   YAML         | N/A   | No     | Yes            | ✗ Custom");

    println!();

    println!("=== Recommendations ===");
    println!("- Use JSON for maximum compatibility (default)");
    println!("- Use MessagePack for smaller message sizes (Celery compatible)");
    println!("- Use BSON for MongoDB integration");
    println!("- Use YAML for human-readable debugging");
    println!("\nRun with --all-features to see all formats");
}
