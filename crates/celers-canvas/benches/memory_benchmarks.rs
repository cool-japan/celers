//! Memory usage benchmarks for workflow primitives

use celers_canvas::{Chain, Chord, Group, Map, Signature};
use serde_json::json;

fn get_memory_usage() -> usize {
    // Note: This is a simplified memory estimation
    // In production, use tools like valgrind or heaptrack for accurate measurements
    std::mem::size_of::<Chain>()
        + std::mem::size_of::<Group>()
        + std::mem::size_of::<Chord>()
        + std::mem::size_of::<Map>()
        + std::mem::size_of::<Signature>()
}

fn estimate_chain_memory(tasks: usize) -> usize {
    let base = std::mem::size_of::<Chain>();
    let task_size = std::mem::size_of::<Signature>();
    base + (task_size * tasks)
}

fn estimate_group_memory(tasks: usize) -> usize {
    let base = std::mem::size_of::<Group>();
    let task_size = std::mem::size_of::<Signature>();
    base + (task_size * tasks)
}

fn estimate_chord_memory(tasks: usize) -> usize {
    let base = std::mem::size_of::<Chord>();
    let task_size = std::mem::size_of::<Signature>();
    let callback_size = std::mem::size_of::<Signature>();
    base + (task_size * tasks) + callback_size
}

fn main() {
    println!("=== Memory Usage Benchmarks ===\n");

    println!("Base Sizes:");
    println!("  Chain:     {:>6} bytes", std::mem::size_of::<Chain>());
    println!("  Group:     {:>6} bytes", std::mem::size_of::<Group>());
    println!("  Chord:     {:>6} bytes", std::mem::size_of::<Chord>());
    println!("  Map:       {:>6} bytes", std::mem::size_of::<Map>());
    println!("  Signature: {:>6} bytes", std::mem::size_of::<Signature>());
    println!();

    println!("Estimated Memory for Large Workflows:");
    for size in [10, 100, 1000, 10000] {
        println!("\n  {} tasks:", size);
        println!(
            "    Chain: {:>8} bytes ({:>6.2} KB)",
            estimate_chain_memory(size),
            estimate_chain_memory(size) as f64 / 1024.0
        );
        println!(
            "    Group: {:>8} bytes ({:>6.2} KB)",
            estimate_group_memory(size),
            estimate_group_memory(size) as f64 / 1024.0
        );
        println!(
            "    Chord: {:>8} bytes ({:>6.2} KB)",
            estimate_chord_memory(size),
            estimate_chord_memory(size) as f64 / 1024.0
        );
    }
    println!();

    println!("Actual Workflow Creation:");

    // Small workflow
    let small_chain = Chain::new()
        .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .then_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]))
        .then_signature(Signature::new("task3".to_string()).with_args(vec![json!(3)]));
    println!(
        "  Small chain (3 tasks): {} bytes (estimated)",
        estimate_chain_memory(small_chain.len())
    );

    // Medium workflow
    let mut medium_group = Group::new();
    for i in 0..50 {
        medium_group = medium_group
            .add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    println!(
        "  Medium group (50 tasks): {} bytes (estimated)",
        estimate_group_memory(medium_group.len())
    );

    // Large workflow
    let mut large_group = Group::new();
    for i in 0..500 {
        large_group = large_group
            .add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    let _large_chord = Chord::new(large_group, Signature::new("callback".to_string()));
    println!(
        "  Large chord (500 tasks + callback): {} bytes (estimated)",
        estimate_chord_memory(500)
    );
    println!();

    println!("Memory Efficiency:");
    let base_usage = get_memory_usage();
    println!("  Combined base types: {} bytes", base_usage);
    println!(
        "  Per-task overhead: ~{} bytes",
        std::mem::size_of::<Signature>()
    );
    println!();

    println!("=== Memory Benchmarks Complete ===");
    println!("\nNote: These are conservative estimates based on struct sizes.");
    println!("Actual runtime memory usage may vary due to heap allocations,");
    println!("JSON values, strings, and other dynamic data structures.");
}
