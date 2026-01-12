//! Basic workflow examples demonstrating Chain, Group, Chord, Map, and Starmap

use celers_canvas::{Chain, Chord, Group, Map, Signature, Starmap};
use serde_json::json;

fn main() {
    println!("=== Basic Workflow Examples ===\n");

    // Example 1: Simple Chain
    println!("1. Chain - Sequential Execution:");
    let chain = Chain::new()
        .then("process_data", vec![json!("input.csv")])
        .then("transform", vec![])
        .then("save_results", vec![]);
    println!("   {}", chain);
    println!();

    // Example 2: Group - Parallel Execution
    println!("2. Group - Parallel Execution:");
    let group = Group::new()
        .add("fetch_user_data", vec![json!(1)])
        .add("fetch_user_data", vec![json!(2)])
        .add("fetch_user_data", vec![json!(3)]);
    println!("   {}", group);
    println!();

    // Example 3: Chord - Map-Reduce Pattern
    println!("3. Chord - Map-Reduce Pattern:");
    let header = Group::new()
        .add("process_chunk", vec![json!(0)])
        .add("process_chunk", vec![json!(1)])
        .add("process_chunk", vec![json!(2)]);
    let callback = Signature::new("aggregate_results".to_string());
    let chord = Chord::new(header, callback);
    println!("   {}", chord);
    println!();

    // Example 4: Map - Apply task to multiple arguments
    println!("4. Map - Apply Task to Multiple Arguments:");
    let map = Map::new(
        Signature::new("send_email".to_string()),
        vec![
            vec![json!("user1@example.com")],
            vec![json!("user2@example.com")],
            vec![json!("user3@example.com")],
        ],
    );
    println!("   {} (size: {})", map, map.len());
    println!();

    // Example 5: Starmap - Map with unpacked arguments
    println!("5. Starmap - Map with Unpacked Arguments:");
    let starmap = Starmap::new(
        Signature::new("send_notification".to_string()),
        vec![
            vec![json!("user1@example.com"), json!("Welcome!")],
            vec![json!("user2@example.com"), json!("New message")],
            vec![json!("user3@example.com"), json!("Alert")],
        ],
    );
    println!("   {} (size: {})", starmap, starmap.len());
    println!();

    // Example 6: Signature with options
    println!("6. Signature with Priority and Queue:");
    let sig = Signature::new("high_priority_task".to_string())
        .with_args(vec![json!({"data": "important"})])
        .with_priority(9)
        .with_queue("urgent".to_string())
        .with_time_limit(300);
    println!("   Task: {}", sig.task);
    println!(
        "   Priority: {:?}",
        sig.options.priority.unwrap_or_default()
    );
    println!("   Queue: {:?}", sig.options.queue.unwrap_or_default());
    println!();

    // Example 7: Immutable signature
    println!("7. Immutable Signature (.si()):");
    let immutable = Signature::new("constant_task".to_string())
        .with_args(vec![json!(42)])
        .si();
    println!(
        "   Is immutable: {}",
        if immutable.is_immutable() {
            "Yes"
        } else {
            "No"
        }
    );
    println!();

    // Example 8: Chain using Signature builder
    println!("8. Chain using Signature builder:");
    let nested = Chain::new()
        .then_signature(Signature::new("prepare".to_string()))
        .then_signature(Signature::new("process".to_string()))
        .then_signature(Signature::new("finalize".to_string()));
    println!("   {} (length: {})", nested, nested.len());
    println!();

    println!("=== All examples completed successfully ===");
}
