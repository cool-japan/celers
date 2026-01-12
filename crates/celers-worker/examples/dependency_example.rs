//! Task Dependencies Example
//!
//! This example demonstrates how to use the DependencyGraph to manage
//! task dependencies and ensure tasks execute in the correct order.

use celers_worker::dependencies::{DependencyGraph, DependencyStatus};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Task Dependencies Example ===\n");

    // Create a dependency graph
    let graph = DependencyGraph::new();

    // Create task IDs for a data pipeline:
    // 1. fetch_data (no dependencies)
    // 2. process_data (depends on fetch_data)
    // 3. validate_data (depends on process_data)
    // 4. save_results (depends on validate_data)
    let fetch_id = Uuid::new_v4();
    let process_id = Uuid::new_v4();
    let validate_id = Uuid::new_v4();
    let save_id = Uuid::new_v4();

    println!("Task IDs:");
    println!("  fetch_data: {}", fetch_id);
    println!("  process_data: {}", process_id);
    println!("  validate_data: {}", validate_id);
    println!("  save_results: {}\n", save_id);

    // Define dependencies
    graph.add_dependency(process_id, fetch_id).await?;
    graph.add_dependency(validate_id, process_id).await?;
    graph.add_dependency(save_id, validate_id).await?;

    println!("Dependencies configured:");
    println!("  process_data depends on fetch_data");
    println!("  validate_data depends on process_data");
    println!("  save_results depends on validate_data\n");

    // Get topological sort (execution order)
    let execution_order = graph.topological_sort().await?;
    println!("Optimal execution order:");
    for (i, task_id) in execution_order.iter().enumerate() {
        let name = if *task_id == fetch_id {
            "fetch_data"
        } else if *task_id == process_id {
            "process_data"
        } else if *task_id == validate_id {
            "validate_data"
        } else {
            "save_results"
        };
        println!("  {}. {}", i + 1, name);
    }
    println!();

    // Simulate task execution
    println!("Simulating task execution:\n");

    // Task 1: fetch_data completes
    println!("1. fetch_data: Starting...");
    graph.set_status(fetch_id, DependencyStatus::Running).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    graph
        .set_status(fetch_id, DependencyStatus::Completed)
        .await;
    println!("   fetch_data: Completed");
    graph.update_dependent_statuses(fetch_id).await;

    // Check if process_data is ready
    if graph.get_status(process_id).await == Some(DependencyStatus::Ready) {
        println!("   process_data: Now ready to execute\n");
    }

    // Task 2: process_data completes
    println!("2. process_data: Starting...");
    graph
        .set_status(process_id, DependencyStatus::Running)
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    graph
        .set_status(process_id, DependencyStatus::Completed)
        .await;
    println!("   process_data: Completed");
    graph.update_dependent_statuses(process_id).await;

    // Check if validate_data is ready
    if graph.get_status(validate_id).await == Some(DependencyStatus::Ready) {
        println!("   validate_data: Now ready to execute\n");
    }

    // Task 3: validate_data completes
    println!("3. validate_data: Starting...");
    graph
        .set_status(validate_id, DependencyStatus::Running)
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    graph
        .set_status(validate_id, DependencyStatus::Completed)
        .await;
    println!("   validate_data: Completed");
    graph.update_dependent_statuses(validate_id).await;

    // Check if save_results is ready
    if graph.get_status(save_id).await == Some(DependencyStatus::Ready) {
        println!("   save_results: Now ready to execute\n");
    }

    // Task 4: save_results completes
    println!("4. save_results: Starting...");
    graph.set_status(save_id, DependencyStatus::Running).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    graph.set_status(save_id, DependencyStatus::Completed).await;
    println!("   save_results: Completed\n");

    // Get final statistics
    let stats = graph.get_stats().await;
    println!("Final Statistics:");
    println!("  Total tasks: {}", stats.total_tasks);
    println!("  Total dependencies: {}", stats.total_dependencies);
    println!("  Completed tasks: {}", stats.completed_count);
    println!("  Completion rate: {:.1}%", stats.completion_percentage());

    println!("\n=== Example with Optional Dependencies ===\n");

    // Create a new graph with optional dependencies
    let graph2 = DependencyGraph::new();

    let main_task = Uuid::new_v4();
    let required_dep = Uuid::new_v4();
    let optional_dep = Uuid::new_v4();

    println!("Task IDs:");
    println!("  main_task: {}", main_task);
    println!("  required_dep: {}", required_dep);
    println!("  optional_dep: {}\n", optional_dep);

    // Add required and optional dependencies
    graph2.add_dependency(main_task, required_dep).await?;
    graph2
        .add_optional_dependency(main_task, optional_dep)
        .await?;

    println!("Dependencies:");
    println!("  main_task requires required_dep");
    println!("  main_task optionally depends on optional_dep\n");

    // Simulate required dependency succeeding
    graph2
        .set_status(required_dep, DependencyStatus::Completed)
        .await;
    graph2.update_dependent_statuses(required_dep).await;

    // Simulate optional dependency failing
    graph2
        .set_status(optional_dep, DependencyStatus::Failed)
        .await;
    graph2.update_dependent_statuses(optional_dep).await;

    // Main task should still be ready because optional dependency can fail
    let main_status = graph2.get_status(main_task).await;
    println!("Main task status: {:?}", main_status);
    println!(
        "Main task can execute: {}",
        main_status == Some(DependencyStatus::Ready)
    );

    Ok(())
}
