//! Workflow Optimization Example
//!
//! This example demonstrates how to use the WorkflowCompiler to optimize workflows
//! using various optimization passes:
//! - Common Subexpression Elimination (CSE)
//! - Dead Code Elimination (DCE)
//! - Task Fusion
//! - Parallel Scheduling
//! - Resource Optimization

use celers_canvas::{Chain, Chord, Group, OptimizationPass, Signature, WorkflowCompiler};
use serde_json::json;

fn main() {
    println!("=== Workflow Optimization Examples ===\n");

    // Example 1: Common Subexpression Elimination (CSE)
    example_cse();

    // Example 2: Dead Code Elimination (DCE)
    example_dce();

    // Example 3: Task Fusion
    example_task_fusion();

    // Example 4: Parallel Scheduling
    example_parallel_scheduling();

    // Example 5: Resource Optimization
    example_resource_optimization();

    // Example 6: Combined Optimizations
    example_combined_optimizations();

    // Example 7: Chord Optimization
    example_chord_optimization();
}

fn example_cse() {
    println!("--- Example 1: Common Subexpression Elimination ---");

    // Create a chain with duplicate tasks
    let chain = Chain::new()
        .then_signature(Signature::new("process_data".to_string()).with_args(vec![json!(1)]))
        .then_signature(Signature::new("validate".to_string()).with_args(vec![json!(2)]))
        .then_signature(Signature::new("process_data".to_string()).with_args(vec![json!(1)])); // Duplicate!

    println!("Original chain: {} tasks", chain.tasks.len());

    // Apply CSE optimization in aggressive mode
    let compiler = WorkflowCompiler::new().aggressive();
    let optimized = compiler.optimize_chain(&chain);

    println!("Optimized chain: {} tasks", optimized.tasks.len());
    println!(
        "CSE removed {} duplicate task(s)\n",
        chain.tasks.len() - optimized.tasks.len()
    );
}

fn example_dce() {
    println!("--- Example 2: Dead Code Elimination ---");

    // Create a group with some invalid tasks
    let group = Group::new()
        .add_signature(Signature::new("valid_task".to_string()))
        .add_signature(Signature::new("".to_string())) // Empty name - dead code
        .add_signature(Signature::new("another_valid_task".to_string()));

    println!("Original group: {} tasks", group.tasks.len());

    // Apply DCE optimization
    let compiler = WorkflowCompiler::new();
    let optimized = compiler.optimize_group(&group);

    println!("Optimized group: {} tasks", optimized.tasks.len());
    println!(
        "DCE removed {} invalid task(s)\n",
        group.tasks.len() - optimized.tasks.len()
    );
}

fn example_task_fusion() {
    println!("--- Example 3: Task Fusion ---");

    // Create a chain with fusible tasks
    let chain = Chain::new()
        .then_signature(
            Signature::new("batch_process".to_string())
                .with_args(vec![json!(1)])
                .immutable(), // Must be immutable to fuse
        )
        .then_signature(
            Signature::new("batch_process".to_string())
                .with_args(vec![json!(2)])
                .immutable(),
        )
        .then_signature(Signature::new("finalize".to_string()));

    println!("Original chain: {} tasks", chain.tasks.len());
    println!(
        "Tasks: {:?}",
        chain.tasks.iter().map(|t| &t.task).collect::<Vec<_>>()
    );

    // Apply task fusion in aggressive mode
    let compiler = WorkflowCompiler::new().aggressive();
    let optimized = compiler.optimize_chain(&chain);

    println!("Optimized chain: {} tasks", optimized.tasks.len());
    println!(
        "Tasks: {:?}",
        optimized.tasks.iter().map(|t| &t.task).collect::<Vec<_>>()
    );
    if !optimized.tasks.is_empty() {
        println!(
            "Fused task has {} args (combined from 2 tasks)\n",
            optimized.tasks[0].args.len()
        );
    }
}

fn example_parallel_scheduling() {
    println!("--- Example 4: Parallel Scheduling ---");

    // Create a group with different priorities
    let group = Group::new()
        .add_signature(Signature::new("low_priority".to_string()).with_priority(1))
        .add_signature(Signature::new("high_priority".to_string()).with_priority(9))
        .add_signature(Signature::new("medium_priority".to_string()).with_priority(5));

    println!("Original task order:");
    for task in &group.tasks {
        println!(
            "  {} (priority: {})",
            task.task,
            task.options.priority.unwrap_or(0)
        );
    }

    // Apply parallel scheduling optimization
    let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ParallelScheduling);
    let optimized = compiler.optimize_group(&group);

    println!("\nOptimized task order (sorted by priority):");
    for task in &optimized.tasks {
        println!(
            "  {} (priority: {})",
            task.task,
            task.options.priority.unwrap_or(0)
        );
    }
    println!();
}

fn example_resource_optimization() {
    println!("--- Example 5: Resource Optimization ---");

    // Create a group with tasks on different queues
    let group = Group::new()
        .add_signature(Signature::new("task_a".to_string()).with_queue("queue_2".to_string()))
        .add_signature(Signature::new("task_b".to_string()).with_queue("queue_1".to_string()))
        .add_signature(Signature::new("task_c".to_string()).with_queue("queue_1".to_string()))
        .add_signature(Signature::new("task_d".to_string()).with_queue("queue_2".to_string()));

    println!("Original task order:");
    for task in &group.tasks {
        println!(
            "  {} (queue: {})",
            task.task,
            task.options
                .queue
                .as_ref()
                .unwrap_or(&"default".to_string())
        );
    }

    // Apply resource optimization
    let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ResourceOptimization);
    let optimized = compiler.optimize_group(&group);

    println!("\nOptimized task order (grouped by queue):");
    for task in &optimized.tasks {
        println!(
            "  {} (queue: {})",
            task.task,
            task.options
                .queue
                .as_ref()
                .unwrap_or(&"default".to_string())
        );
    }
    println!();
}

fn example_combined_optimizations() {
    println!("--- Example 6: Combined Optimizations ---");

    // Create a complex workflow with multiple optimization opportunities
    let group = Group::new()
        .add_signature(
            Signature::new("process".to_string())
                .with_priority(1)
                .with_args(vec![json!(1)]),
        )
        .add_signature(Signature::new("".to_string())) // Dead code
        .add_signature(
            Signature::new("process".to_string())
                .with_priority(1)
                .with_args(vec![json!(1)]),
        ) // Duplicate
        .add_signature(
            Signature::new("analyze".to_string())
                .with_priority(9)
                .with_args(vec![json!(2)]),
        );

    println!("Original group: {} tasks", group.tasks.len());

    // Apply all optimizations in aggressive mode
    let compiler = WorkflowCompiler::new()
        .aggressive()
        .add_pass(OptimizationPass::ParallelScheduling);
    let optimized = compiler.optimize_group(&group);

    println!("Optimized group: {} tasks", optimized.tasks.len());
    println!("Applied optimizations:");
    println!("  - Dead Code Elimination: removed empty tasks");
    println!("  - Common Subexpression Elimination: removed duplicates");
    println!("  - Parallel Scheduling: sorted by priority");

    println!("\nFinal task order:");
    for task in &optimized.tasks {
        println!(
            "  {} (priority: {})",
            task.task,
            task.options.priority.unwrap_or(0)
        );
    }
    println!();
}

fn example_chord_optimization() {
    println!("--- Example 7: Chord Optimization ---");

    // Create a chord with duplicate tasks in the header
    let group = Group::new()
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]))
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])); // Duplicate

    let chord = Chord::new(group, Signature::new("aggregate".to_string()));

    println!("Original chord header: {} tasks", chord.header.tasks.len());

    // Optimize the chord
    let compiler = WorkflowCompiler::new().aggressive();
    let optimized = compiler.optimize_chord(&chord);

    println!(
        "Optimized chord header: {} tasks",
        optimized.header.tasks.len()
    );
    println!("Callback: {}", optimized.body.task);
    println!(
        "Removed {} duplicate task(s) from chord header\n",
        chord.header.tasks.len() - optimized.header.tasks.len()
    );
}
