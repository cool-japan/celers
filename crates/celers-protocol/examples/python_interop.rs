//! Python Celery interoperability example
//!
//! This example demonstrates how to create messages compatible with Python Celery.
//!
//! Run with: cargo run --example python_interop

use celers_protocol::{builder::MessageBuilder, Message, TaskArgs};
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

fn main() {
    println!("=== Celery Protocol Python Interop Example ===\n");

    // Example 1: Simple task
    println!("1. Simple task message:");
    let simple_msg = MessageBuilder::new("tasks.add")
        .args(vec![json!(2), json!(3)])
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&simple_msg).unwrap();
    println!("{}\n", json);

    // Example 2: Task with kwargs
    println!("2. Task with keyword arguments:");
    let mut kwargs = HashMap::new();
    kwargs.insert("x".to_string(), json!(10));
    kwargs.insert("y".to_string(), json!(20));

    let kwargs_msg = MessageBuilder::new("tasks.multiply")
        .kwargs(kwargs)
        .queue("celery")
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&kwargs_msg).unwrap();
    println!("{}\n", json);

    // Example 3: Delayed task (ETA)
    println!("3. Delayed task (execute in 60 seconds):");
    let delayed_msg = MessageBuilder::new("tasks.cleanup")
        .countdown(60)
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&delayed_msg).unwrap();
    println!("{}\n", json);

    // Example 4: Task with expiration
    println!("4. Task with expiration:");
    use chrono::Duration;
    let expiring_msg = MessageBuilder::new("tasks.process")
        .args(vec![json!("data")])
        .expires_in(Duration::minutes(5))
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&expiring_msg).unwrap();
    println!("{}\n", json);

    // Example 5: Task chain
    println!("5. Task chain (link callbacks):");
    let chain_msg = MessageBuilder::new("tasks.step1")
        .args(vec![json!(100)])
        .link("tasks.step2")
        .link("tasks.step3")
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&chain_msg).unwrap();
    println!("{}\n", json);

    // Example 6: Task with error callback
    println!("6. Task with error callback:");
    let error_msg = MessageBuilder::new("tasks.risky")
        .args(vec![json!("data")])
        .link_error("tasks.handle_error")
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&error_msg).unwrap();
    println!("{}\n", json);

    // Example 7: High priority task
    println!("7. High priority task:");
    let priority_msg = MessageBuilder::new("tasks.urgent")
        .priority(9)
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&priority_msg).unwrap();
    println!("{}\n", json);

    // Example 8: Workflow with parent/root tracking
    println!("8. Workflow task (with parent and root):");
    let root_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();

    let workflow_msg = MessageBuilder::new("tasks.workflow_step")
        .args(vec![json!("step_data")])
        .parent(parent_id)
        .root(root_id)
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&workflow_msg).unwrap();
    println!("{}\n", json);

    // Example 9: Group task
    println!("9. Group task:");
    let group_id = Uuid::new_v4();

    let group_msg = MessageBuilder::new("tasks.parallel")
        .args(vec![json!("data1")])
        .group(group_id)
        .build()
        .unwrap();

    let json = serde_json::to_string_pretty(&group_msg).unwrap();
    println!("{}\n", json);

    // Example 10: Manual message creation (low-level API)
    println!("10. Manual message creation:");
    let task_id = Uuid::new_v4();
    let args = TaskArgs::new().with_args(vec![json!(42)]);
    let body = serde_json::to_vec(&args).unwrap();
    let manual_msg = Message::new("tasks.custom".to_string(), task_id, body).with_priority(5);

    let json = serde_json::to_string_pretty(&manual_msg).unwrap();
    println!("{}\n", json);

    println!("=== Protocol Compatibility ===");
    println!("All messages above are 100% wire-format compatible with Python Celery.");
    println!("They can be sent to RabbitMQ/Redis and consumed by Python Celery workers.");
    println!("\nSee the Python example in examples/python_consumer.py");
}
