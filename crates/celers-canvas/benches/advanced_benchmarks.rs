//! Advanced performance benchmarks for complex workflow patterns

use celers_canvas::{
    Chain, Chord, CompensationWorkflow, FanIn, FanOut, Group, OptimizationPass, Pipeline, Saga,
    SagaIsolation, ScatterGather, Signature, StreamingMapReduce, WorkflowCompiler,
    WorkflowConcurrencyControl, WorkflowMetricsCollector, WorkflowRateLimiter, WorkflowState,
};
use serde_json::json;
use std::time::Instant;
use uuid::Uuid;

fn benchmark_saga_creation(steps: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut compensation = CompensationWorkflow::new();
    for i in 0..steps {
        compensation = compensation.step(
            Signature::new(format!("forward_{}", i)),
            Signature::new(format!("compensate_{}", i)),
        );
    }
    let _saga = Saga::new(compensation).with_isolation(SagaIsolation::Serializable);
    start.elapsed()
}

fn benchmark_pipeline_creation(stages: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut pipeline = Pipeline::new();
    for i in 0..stages {
        pipeline = pipeline.stage(Signature::new(format!("stage_{}", i)));
    }
    let _pipeline = pipeline.with_buffer_size(1000);
    start.elapsed()
}

fn benchmark_fanout_creation(consumers: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut fanout = FanOut::new(Signature::new("source".to_string()));
    for i in 0..consumers {
        fanout = fanout.consumer(Signature::new(format!("consumer_{}", i)));
    }
    start.elapsed()
}

fn benchmark_fanin_creation(sources: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut fanin = FanIn::new(Signature::new("aggregator".to_string()));
    for i in 0..sources {
        fanin = fanin.source(Signature::new(format!("source_{}", i)));
    }
    start.elapsed()
}

fn benchmark_scattergather_creation(workers: usize) -> std::time::Duration {
    let start = Instant::now();
    let workers_vec: Vec<Signature> = (0..workers)
        .map(|i| Signature::new(format!("worker_{}", i)))
        .collect();
    let _scatter = ScatterGather::new(
        Signature::new("scatter".to_string()),
        workers_vec,
        Signature::new("gather".to_string()),
    );
    start.elapsed()
}

fn benchmark_complex_nested_workflow(depth: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut chain = Chain::new();
    for i in 0..depth {
        let _group = Group::new()
            .add(format!("task_{}_1", i).as_str(), vec![json!(i)])
            .add(format!("task_{}_2", i).as_str(), vec![json!(i)])
            .add(format!("task_{}_3", i).as_str(), vec![json!(i)]);

        chain = chain.then_signature(Signature::new(format!("pre_{}", i)));
        // Simulate nested structure
        chain = chain.then_signature(Signature::new(format!("post_{}", i)));
    }
    start.elapsed()
}

fn benchmark_workflow_state_updates(updates: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut state = WorkflowState::new(Uuid::new_v4(), updates);
    for _ in 0..updates {
        state.mark_completed();
    }
    let _ = state.progress();
    start.elapsed()
}

fn benchmark_metrics_collection(tasks: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut metrics = WorkflowMetricsCollector::new(Uuid::new_v4());
    for i in 0..tasks {
        let task_id = Uuid::new_v4();
        metrics.record_task_start(task_id);
        metrics.record_task_complete(task_id, 100 + i as u64);
    }
    metrics.finalize();
    start.elapsed()
}

fn benchmark_rate_limiter(requests: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut limiter = WorkflowRateLimiter::new(100, 1000);
    for _ in 0..requests {
        limiter.allow_workflow();
    }
    start.elapsed()
}

fn benchmark_concurrency_control(workflows: usize) -> std::time::Duration {
    let start = Instant::now();
    let mut control = WorkflowConcurrencyControl::new(50);
    let mut workflow_ids = Vec::new();

    for _ in 0..workflows.min(50) {
        let wf_id = Uuid::new_v4();
        control.try_start(wf_id);
        workflow_ids.push(wf_id);
    }

    for wf_id in workflow_ids {
        control.complete(wf_id);
    }
    start.elapsed()
}

fn benchmark_streaming_mapreduce_setup() -> std::time::Duration {
    let start = Instant::now();
    let _streaming = StreamingMapReduce::new(
        Signature::new("map".to_string()),
        Signature::new("reduce".to_string()),
    )
    .with_chunk_size(100)
    .with_buffer_size(1000)
    .with_backpressure(true);
    start.elapsed()
}

// Workflow Optimization Benchmarks

fn benchmark_cse_chain(tasks: usize, duplicates: usize) -> std::time::Duration {
    let mut chain = Chain::new();
    // Add unique tasks
    for i in 0..tasks {
        chain =
            chain.then_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    // Add duplicate tasks
    for i in 0..duplicates {
        chain =
            chain.then_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }

    let compiler = WorkflowCompiler::new().aggressive();
    let start = Instant::now();
    let _optimized = compiler.optimize_chain(&chain);
    start.elapsed()
}

fn benchmark_cse_group(tasks: usize, duplicates: usize) -> std::time::Duration {
    let mut group = Group::new();
    // Add unique tasks
    for i in 0..tasks {
        group =
            group.add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    // Add duplicate tasks
    for i in 0..duplicates {
        group =
            group.add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }

    let compiler = WorkflowCompiler::new().aggressive();
    let start = Instant::now();
    let _optimized = compiler.optimize_group(&group);
    start.elapsed()
}

fn benchmark_dce_chain(valid_tasks: usize, dead_tasks: usize) -> std::time::Duration {
    let mut chain = Chain::new();
    // Add valid tasks
    for i in 0..valid_tasks {
        chain = chain.then_signature(Signature::new(format!("task_{}", i)));
    }
    // Add dead code (empty task names)
    for _ in 0..dead_tasks {
        chain = chain.then_signature(Signature::new("".to_string()));
    }

    let compiler = WorkflowCompiler::new();
    let start = Instant::now();
    let _optimized = compiler.optimize_chain(&chain);
    start.elapsed()
}

fn benchmark_task_fusion(fusible_pairs: usize) -> std::time::Duration {
    let mut chain = Chain::new();
    for i in 0..fusible_pairs {
        chain = chain
            .then_signature(
                Signature::new("batch_process".to_string())
                    .with_args(vec![json!(i)])
                    .immutable(),
            )
            .then_signature(
                Signature::new("batch_process".to_string())
                    .with_args(vec![json!(i + 1000)])
                    .immutable(),
            );
    }

    let compiler = WorkflowCompiler::new().aggressive();
    let start = Instant::now();
    let _optimized = compiler.optimize_chain(&chain);
    start.elapsed()
}

fn benchmark_parallel_scheduling(tasks: usize) -> std::time::Duration {
    let mut group = Group::new();
    for i in 0..tasks {
        let priority = (i % 10) as u8;
        group = group.add_signature(Signature::new(format!("task_{}", i)).with_priority(priority));
    }

    let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ParallelScheduling);
    let start = Instant::now();
    let _optimized = compiler.optimize_group(&group);
    start.elapsed()
}

fn benchmark_resource_optimization(tasks: usize, queues: usize) -> std::time::Duration {
    let mut group = Group::new();
    for i in 0..tasks {
        let queue = format!("queue_{}", i % queues);
        group = group.add_signature(Signature::new(format!("task_{}", i)).with_queue(queue));
    }

    let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ResourceOptimization);
    let start = Instant::now();
    let _optimized = compiler.optimize_group(&group);
    start.elapsed()
}

fn benchmark_chord_optimization(header_tasks: usize, duplicates: usize) -> std::time::Duration {
    let mut group = Group::new();
    // Add unique tasks
    for i in 0..header_tasks {
        group =
            group.add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }
    // Add duplicates
    for i in 0..duplicates {
        group =
            group.add_signature(Signature::new(format!("task_{}", i)).with_args(vec![json!(i)]));
    }

    let chord = Chord::new(group, Signature::new("callback".to_string()));
    let compiler = WorkflowCompiler::new().aggressive();
    let start = Instant::now();
    let _optimized = compiler.optimize_chord(&chord);
    start.elapsed()
}

fn benchmark_combined_optimizations(tasks: usize) -> std::time::Duration {
    let mut group = Group::new();
    // Mix of valid tasks, duplicates, dead code, different priorities
    for i in 0..tasks / 2 {
        group = group.add_signature(
            Signature::new(format!("task_{}", i))
                .with_priority((i % 10) as u8)
                .with_args(vec![json!(i)]),
        );
    }
    // Add some duplicates
    for i in 0..tasks / 4 {
        group = group.add_signature(
            Signature::new(format!("task_{}", i))
                .with_priority((i % 10) as u8)
                .with_args(vec![json!(i)]),
        );
    }
    // Add some dead code
    for _ in 0..tasks / 8 {
        group = group.add_signature(Signature::new("".to_string()));
    }

    let compiler = WorkflowCompiler::new()
        .aggressive()
        .add_pass(OptimizationPass::ParallelScheduling)
        .add_pass(OptimizationPass::ResourceOptimization);
    let start = Instant::now();
    let _optimized = compiler.optimize_group(&group);
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
    println!("=== Advanced Pattern Benchmarks ===\n");

    println!("Saga Pattern:");
    run_benchmark("   5 steps   ", || benchmark_saga_creation(5));
    run_benchmark("  10 steps   ", || benchmark_saga_creation(10));
    run_benchmark("  25 steps   ", || benchmark_saga_creation(25));
    println!();

    println!("Pipeline Pattern:");
    run_benchmark("   5 stages  ", || benchmark_pipeline_creation(5));
    run_benchmark("  10 stages  ", || benchmark_pipeline_creation(10));
    run_benchmark("  20 stages  ", || benchmark_pipeline_creation(20));
    println!();

    println!("FanOut Pattern:");
    run_benchmark("  10 consumers", || benchmark_fanout_creation(10));
    run_benchmark("  50 consumers", || benchmark_fanout_creation(50));
    run_benchmark(" 100 consumers", || benchmark_fanout_creation(100));
    println!();

    println!("FanIn Pattern:");
    run_benchmark("  10 sources ", || benchmark_fanin_creation(10));
    run_benchmark("  50 sources ", || benchmark_fanin_creation(50));
    run_benchmark(" 100 sources ", || benchmark_fanin_creation(100));
    println!();

    println!("ScatterGather Pattern:");
    run_benchmark("  10 workers ", || benchmark_scattergather_creation(10));
    run_benchmark("  50 workers ", || benchmark_scattergather_creation(50));
    run_benchmark(" 100 workers ", || benchmark_scattergather_creation(100));
    println!();

    println!("Complex Nested Workflows:");
    run_benchmark("  Depth  5   ", || benchmark_complex_nested_workflow(5));
    run_benchmark("  Depth 10   ", || benchmark_complex_nested_workflow(10));
    run_benchmark("  Depth 20   ", || benchmark_complex_nested_workflow(20));
    println!();

    println!("Workflow State Updates:");
    run_benchmark("  100 updates", || benchmark_workflow_state_updates(100));
    run_benchmark("  500 updates", || benchmark_workflow_state_updates(500));
    run_benchmark(" 1000 updates", || benchmark_workflow_state_updates(1000));
    println!();

    println!("Metrics Collection:");
    run_benchmark("  100 tasks  ", || benchmark_metrics_collection(100));
    run_benchmark("  500 tasks  ", || benchmark_metrics_collection(500));
    run_benchmark(" 1000 tasks  ", || benchmark_metrics_collection(1000));
    println!();

    println!("Rate Limiter:");
    run_benchmark(" 100 requests", || benchmark_rate_limiter(100));
    run_benchmark(" 500 requests", || benchmark_rate_limiter(500));
    run_benchmark("1000 requests", || benchmark_rate_limiter(1000));
    println!();

    println!("Concurrency Control:");
    run_benchmark("  50 workflows", || benchmark_concurrency_control(50));
    run_benchmark(" 100 workflows", || benchmark_concurrency_control(100));
    println!();

    println!("Streaming MapReduce Setup:");
    run_benchmark(" Setup       ", benchmark_streaming_mapreduce_setup);
    println!();

    println!("=== Workflow Optimization Benchmarks ===\n");

    println!("Common Subexpression Elimination (CSE) - Chain:");
    run_benchmark(" 50 tasks, 25 dups ", || benchmark_cse_chain(50, 25));
    run_benchmark("100 tasks, 50 dups ", || benchmark_cse_chain(100, 50));
    run_benchmark("200 tasks, 100 dups", || benchmark_cse_chain(200, 100));
    println!();

    println!("Common Subexpression Elimination (CSE) - Group:");
    run_benchmark(" 50 tasks, 25 dups ", || benchmark_cse_group(50, 25));
    run_benchmark("100 tasks, 50 dups ", || benchmark_cse_group(100, 50));
    run_benchmark("200 tasks, 100 dups", || benchmark_cse_group(200, 100));
    println!();

    println!("Dead Code Elimination (DCE) - Chain:");
    run_benchmark(" 50 valid, 25 dead ", || benchmark_dce_chain(50, 25));
    run_benchmark("100 valid, 50 dead ", || benchmark_dce_chain(100, 50));
    run_benchmark("200 valid, 100 dead", || benchmark_dce_chain(200, 100));
    println!();

    println!("Task Fusion:");
    run_benchmark(" 25 pairs       ", || benchmark_task_fusion(25));
    run_benchmark(" 50 pairs       ", || benchmark_task_fusion(50));
    run_benchmark("100 pairs       ", || benchmark_task_fusion(100));
    println!();

    println!("Parallel Scheduling:");
    run_benchmark(" 50 tasks       ", || benchmark_parallel_scheduling(50));
    run_benchmark("100 tasks       ", || benchmark_parallel_scheduling(100));
    run_benchmark("200 tasks       ", || benchmark_parallel_scheduling(200));
    println!();

    println!("Resource Optimization:");
    run_benchmark(" 50 tasks, 5 queues ", || {
        benchmark_resource_optimization(50, 5)
    });
    run_benchmark("100 tasks, 10 queues", || {
        benchmark_resource_optimization(100, 10)
    });
    run_benchmark("200 tasks, 20 queues", || {
        benchmark_resource_optimization(200, 20)
    });
    println!();

    println!("Chord Optimization:");
    run_benchmark(" 50 tasks, 25 dups ", || {
        benchmark_chord_optimization(50, 25)
    });
    run_benchmark("100 tasks, 50 dups ", || {
        benchmark_chord_optimization(100, 50)
    });
    run_benchmark("200 tasks, 100 dups", || {
        benchmark_chord_optimization(200, 100)
    });
    println!();

    println!("Combined Optimizations:");
    run_benchmark(" 100 tasks      ", || benchmark_combined_optimizations(100));
    run_benchmark(" 200 tasks      ", || benchmark_combined_optimizations(200));
    run_benchmark(" 500 tasks      ", || benchmark_combined_optimizations(500));
    println!();

    println!("=== Advanced Benchmarks Complete ===");
}
