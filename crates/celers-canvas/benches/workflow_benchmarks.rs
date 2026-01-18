//! Performance benchmarks for workflow primitives

use celers_canvas::{Chain, Chord, Group, Map, Signature, Starmap};
use serde_json::json;
use std::time::Instant;

fn benchmark_chain_creation(size: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut chain = Chain::new();
    for i in 0..size {
        chain =
            chain.then_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    start.elapsed()
}

fn benchmark_group_creation(size: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut group = Group::new();
    for i in 0..size {
        group =
            group.add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    start.elapsed()
}

fn benchmark_chord_creation(size: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut group = Group::new();
    for i in 0..size {
        group =
            group.add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    let _chord = Chord::new(group, Signature::new("callback".to_string()));
    start.elapsed()
}

fn benchmark_map_creation(size: usize) -> std::time::Duration {
    let start = Instant::now();
    let args: Vec<Vec<serde_json::Value>> = (0..size).map(|i| vec![json!(i)]).collect();
    let _map = Map::new(Signature::new("task".to_string()), args);
    start.elapsed()
}

fn benchmark_starmap_creation(size: usize) -> std::time::Duration {
    let start = Instant::now();
    let args: Vec<Vec<serde_json::Value>> =
        (0..size).map(|i| vec![json!(i), json!(i * 2)]).collect();
    let _starmap = Starmap::new(Signature::new("task".to_string()), args);
    start.elapsed()
}

fn benchmark_nested_workflows(depth: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut chain = Chain::new();
    for i in 0..depth {
        chain = chain.then_signature(Signature::new(format!("task_{}", i)));
    }
    start.elapsed()
}

fn benchmark_signature_with_options(count: usize) -> std::time::Duration {
    let start = Instant::now();
    for i in 0..count {
        let _sig = Signature::new(format!("task_{}", i))
            .with_args(vec![json!(i)])
            .with_priority(5)
            .with_queue("default".to_string())
            .with_time_limit(300)
            .with_retry_delay(1000);
    }
    start.elapsed()
}

fn run_benchmark(name: &str, f: impl Fn() -> std::time::Duration) {
    // Warmup
    for _ in 0..3 {
        f();
    }

    // Actual benchmark
    let iterations = 100;
    let mut total = std::time::Duration::ZERO;
    for _ in 0..iterations {
        total += f();
    }
    let avg = total / iterations as u32;

    println!("  {} avg: {:>8.2} µs", name, avg.as_micros() as f64);
}

fn main() {
    println!("=== Workflow Creation Benchmarks ===\n");

    println!("Chain Creation:");
    run_benchmark("  10 tasks ", || benchmark_chain_creation(10));
    run_benchmark("  50 tasks ", || benchmark_chain_creation(50));
    run_benchmark(" 100 tasks ", || benchmark_chain_creation(100));
    run_benchmark(" 500 tasks ", || benchmark_chain_creation(500));
    println!();

    println!("Group Creation:");
    run_benchmark("  10 tasks ", || benchmark_group_creation(10));
    run_benchmark("  50 tasks ", || benchmark_group_creation(50));
    run_benchmark(" 100 tasks ", || benchmark_group_creation(100));
    run_benchmark(" 500 tasks ", || benchmark_group_creation(500));
    println!();

    println!("Chord Creation:");
    run_benchmark("  10 tasks ", || benchmark_chord_creation(10));
    run_benchmark("  50 tasks ", || benchmark_chord_creation(50));
    run_benchmark(" 100 tasks ", || benchmark_chord_creation(100));
    run_benchmark(" 500 tasks ", || benchmark_chord_creation(500));
    println!();

    println!("Map Creation:");
    run_benchmark("  10 items ", || benchmark_map_creation(10));
    run_benchmark("  50 items ", || benchmark_map_creation(50));
    run_benchmark(" 100 items ", || benchmark_map_creation(100));
    run_benchmark(" 500 items ", || benchmark_map_creation(500));
    println!();

    println!("Starmap Creation:");
    run_benchmark("  10 items ", || benchmark_starmap_creation(10));
    run_benchmark("  50 items ", || benchmark_starmap_creation(50));
    run_benchmark(" 100 items ", || benchmark_starmap_creation(100));
    run_benchmark(" 500 items ", || benchmark_starmap_creation(500));
    println!();

    println!("Nested Workflows:");
    run_benchmark("  5 levels ", || benchmark_nested_workflows(5));
    run_benchmark(" 10 levels ", || benchmark_nested_workflows(10));
    run_benchmark(" 20 levels ", || benchmark_nested_workflows(20));
    println!();

    println!("Signature with Options:");
    run_benchmark("  10 sigs  ", || benchmark_signature_with_options(10));
    run_benchmark("  50 sigs  ", || benchmark_signature_with_options(50));
    run_benchmark(" 100 sigs  ", || benchmark_signature_with_options(100));
    println!();

    println!("=== Benchmarks Complete ===");
}
