//! Performance optimization examples
//!
//! Demonstrates zero-copy deserialization, lazy loading, and message pooling.
//!
//! Run with: cargo run --example performance --release

use celers_protocol::{
    lazy::LazyMessage,
    pool::{MessagePool, TaskArgsPool},
    zerocopy::MessageRef,
    Message, TaskArgs,
};
use serde_json::json;
use std::time::Instant;
use uuid::Uuid;

fn main() {
    println!("=== Performance Optimization Examples ===\n");

    // Create test data
    let task_id = Uuid::new_v4();
    let args = TaskArgs::new().with_args(vec![json!(1), json!(2), json!(3)]);
    let body = serde_json::to_vec(&args).unwrap();
    let msg = Message::new("tasks.benchmark".to_string(), task_id, body);
    let json_data = serde_json::to_vec(&msg).unwrap();

    println!("Test message size: {} bytes\n", json_data.len());

    // Example 1: Standard deserialization
    println!("1. Standard deserialization:");
    let start = Instant::now();
    for _ in 0..10000 {
        let _msg: Message = serde_json::from_slice(&json_data).unwrap();
    }
    let duration = start.elapsed();
    println!("   10,000 deserializations: {:?}\n", duration);

    // Example 2: Zero-copy deserialization
    println!("2. Zero-copy deserialization:");
    let start = Instant::now();
    for _ in 0..10000 {
        let _msg: MessageRef = serde_json::from_slice(&json_data).unwrap();
    }
    let duration = start.elapsed();
    println!("   10,000 zero-copy deserializations: {:?}", duration);
    println!("   (Borrows strings instead of copying)\n");

    // Example 3: Lazy deserialization
    println!("3. Lazy deserialization (headers only):");
    let start = Instant::now();
    for _ in 0..10000 {
        let msg = LazyMessage::from_json(&json_data).unwrap();
        let _task_name = msg.task_name();
    }
    let duration = start.elapsed();
    println!("   10,000 lazy loads (no body parsing): {:?}", duration);
    println!("   (Body deserialized on-demand)\n");

    // Example 4: Message pooling
    println!("4. Message pooling:");
    let pool = MessagePool::new();

    let start = Instant::now();
    for _ in 0..10000 {
        let mut msg = pool.acquire();
        msg.headers.task = "tasks.test".to_string();
        msg.headers.id = Uuid::new_v4();
        msg.body = vec![1, 2, 3];
        // Automatically returned to pool on drop
    }
    let duration = start.elapsed();
    println!("   10,000 pooled allocations: {:?}", duration);
    println!("   (Reuses memory allocations)\n");

    // Example 5: TaskArgs pooling
    println!("5. TaskArgs pooling:");
    let args_pool = TaskArgsPool::new();

    let start = Instant::now();
    for _ in 0..10000 {
        let mut args = args_pool.acquire();
        args.get_mut().args.push(json!(42));
        // Automatically returned to pool on drop
    }
    let duration = start.elapsed();
    println!("   10,000 pooled TaskArgs: {:?}\n", duration);

    // Example 6: Zero-copy with conversion
    println!("6. Zero-copy with owned conversion:");
    let msg_ref: MessageRef = serde_json::from_slice(&json_data).unwrap();
    println!("   Borrowed task name: {}", msg_ref.task_name());

    let owned_msg = msg_ref.into_owned();
    println!("   Converted to owned: {}", owned_msg.task_name());
    println!("   (Convert to owned when needed)\n");

    // Example 7: Lazy with body access
    println!("7. Lazy deserialization with body:");
    let lazy_msg = LazyMessage::from_json(&json_data).unwrap();
    println!("   Task name (no body parse): {}", lazy_msg.task_name());

    match lazy_msg.body() {
        Ok(body) => println!("   Body deserialized: {} bytes", body.len()),
        Err(e) => println!("   Body error: {}", e),
    }
    println!("   (Body only parsed when accessed)\n");

    // Example 8: Comparing message sizes
    println!("8. Memory efficiency comparison:");
    let small_msg = Message::new("tasks.small".to_string(), Uuid::new_v4(), vec![1]);
    let large_args = TaskArgs::new().with_args(vec![json!("x".repeat(10000))]);
    let large_body = serde_json::to_vec(&large_args).unwrap();
    let large_msg = Message::new("tasks.large".to_string(), Uuid::new_v4(), large_body);

    println!(
        "   Small message: {} bytes",
        std::mem::size_of_val(&small_msg)
    );
    println!(
        "   Large message: {} bytes",
        std::mem::size_of_val(&large_msg)
    );
    println!("   (Zero-copy avoids allocations for strings)\n");

    // Example 9: Pool statistics
    println!("9. Pool reuse demonstration:");
    let pool = MessagePool::with_capacity(10);

    {
        let mut messages = Vec::new();
        for _ in 0..5 {
            messages.push(pool.acquire());
        }
        println!("   Acquired 5 messages from pool");
        // Messages dropped here, returned to pool
    }

    {
        let _msg1 = pool.acquire();
        let _msg2 = pool.acquire();
        println!("   Acquired 2 more (reused from pool)");
    }
    println!();

    // Example 10: Best practices
    println!("10. Performance best practices:");
    println!("    - Use zero-copy (MessageRef) when you don't need owned data");
    println!("    - Use lazy loading (LazyMessage) for large messages");
    println!("    - Use pooling for high-throughput scenarios");
    println!("    - Access body only when needed in lazy mode");
    println!("    - Convert to owned only when data outlives the buffer");
    println!();

    println!("=== Performance Comparison ===");
    println!("Standard deserialization: Full parse + allocations");
    println!("Zero-copy: Minimal allocations (borrows strings)");
    println!("Lazy: Deferred body parsing (best for metadata-only)");
    println!("Pooling: Memory reuse (best for high volume)");
    println!("\nRun with --release for accurate benchmarks!");
}
