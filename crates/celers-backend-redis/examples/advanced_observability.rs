//! Advanced Observability and Performance Features
//!
//! This example demonstrates the advanced observability and performance features:
//! - Telemetry hooks for monitoring and tracing
//! - Retry strategies with exponential backoff
//! - Batch streaming for efficient large-scale operations
//! - Pipeline optimization for performance tuning
//! - Performance profiling for bottleneck identification

use celers_backend_redis::{
    batch_stream::{BatchStreamConfig, BatchStreamStats},
    pipeline::{PipelineMetrics, PipelineOptimizer, PipelineStrategy},
    profiler::{Profiler, ThroughputAnalyzer},
    retry::RetryStrategy,
    telemetry::{LoggingHook, MetricsHook, MultiHook, NoOpHook, OperationType},
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Advanced Observability and Performance Features ===\n");

    // Redis URL
    let redis_url = "redis://127.0.0.1:6379";

    // Example 1: Telemetry Hooks
    println!("1. Telemetry Hooks");
    println!("------------------");
    demonstrate_telemetry_hooks(redis_url).await?;

    // Example 2: Retry Strategies
    println!("\n2. Retry Strategies with Exponential Backoff");
    println!("--------------------------------------------");
    demonstrate_retry_strategies().await?;

    // Example 3: Batch Streaming
    println!("\n3. Batch Streaming for Large Result Sets");
    println!("----------------------------------------");
    demonstrate_batch_streaming(redis_url).await?;

    // Example 4: Pipeline Optimization
    println!("\n4. Pipeline Optimization");
    println!("------------------------");
    demonstrate_pipeline_optimization().await?;

    // Example 5: Performance Profiling
    println!("\n5. Performance Profiling");
    println!("------------------------");
    demonstrate_performance_profiling(redis_url).await?;

    println!("\n=== All examples completed successfully ===");
    Ok(())
}

/// Demonstrate telemetry hooks for observability
async fn demonstrate_telemetry_hooks(_redis_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 1. No-op hook (default, zero overhead)
    let _noop_hook = NoOpHook;
    println!("Using NoOpHook (zero overhead):");
    println!("  ✓ NoOpHook provides zero-overhead monitoring placeholder\n");

    // 2. Logging hook with tracing integration
    println!("Using LoggingHook:");
    let _logging_hook = LoggingHook::new();
    println!("  ✓ Created LoggingHook (logs operations with tracing)");
    println!("  ✓ Can configure with .with_min_duration() and .errors_only()\n");

    // 3. Metrics hook for operation statistics
    println!("Using MetricsHook:");
    let metrics_hook = MetricsHook::new();
    println!("  ✓ Created MetricsHook for operation statistics");
    println!(
        "  ✓ Store operations: {}",
        metrics_hook.operation_count(OperationType::Store)
    );
    println!(
        "  ✓ Get operations: {}",
        metrics_hook.operation_count(OperationType::Get)
    );
    println!(
        "  ✓ Delete operations: {}",
        metrics_hook.operation_count(OperationType::Delete)
    );
    println!(
        "  ✓ Errors: {}",
        metrics_hook.error_count(OperationType::Store)
    );
    println!(
        "  ✓ Avg duration: {:?}\n",
        metrics_hook.average_duration(OperationType::Store)
    );

    // 4. Multi-hook (combine multiple telemetry backends)
    println!("Using MultiHook (combines multiple hooks):");
    let _multi_hook = MultiHook::new()
        .add_hook(Arc::new(LoggingHook::new()))
        .add_hook(Arc::new(MetricsHook::new()));
    println!("  ✓ Created MultiHook with logging and metrics\n");

    Ok(())
}

/// Demonstrate retry strategies with exponential backoff
async fn demonstrate_retry_strategies() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Basic retry strategy
    println!("Basic retry strategy:");
    let basic_strategy = RetryStrategy::default();
    println!("  Max attempts: {}", basic_strategy.max_attempts);
    println!("  Initial backoff: {:?}", basic_strategy.initial_backoff);
    println!("  Max backoff: {:?}", basic_strategy.max_backoff);
    println!("  Multiplier: {}", basic_strategy.multiplier);
    println!("  Jitter: {}\n", basic_strategy.jitter);

    // 2. Custom retry strategy
    println!("Custom retry strategy:");
    let _custom_strategy = RetryStrategy {
        max_attempts: 5,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        multiplier: 2.0,
        jitter: true,
    };
    println!("  Configured for aggressive retries with jitter\n");

    // 3. Retry executor example
    println!("Retry executor:");
    println!("  ✓ RetryExecutor provides automatic retry with exponential backoff");
    println!("  ✓ Automatically detects retryable errors");
    println!("  ✓ Supports custom retry predicates");
    println!("  ✓ Includes detailed logging\n");

    // 4. Connection retry helper
    println!("Connection retry helper:");
    println!("  ✓ Available via celers_backend_redis::retry::retry_connection");
    println!("  ✓ Specialized for Redis connection retries\n");

    Ok(())
}

/// Demonstrate batch streaming for large result sets
async fn demonstrate_batch_streaming(_redis_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Batch streaming configuration
    println!("Batch streaming configuration:");
    let config = BatchStreamConfig {
        chunk_size: 10,
        max_concurrent: 3,
        skip_errors: true,
    };
    println!("  Chunk size: {}", config.chunk_size);
    println!("  Max concurrent: {}", config.max_concurrent);
    println!("  Skip errors: {}\n", config.skip_errors);

    // 2. Builder pattern
    println!("Using builder pattern:");
    let config2 = BatchStreamConfig::new()
        .with_chunk_size(50)
        .with_max_concurrent(8)
        .with_skip_errors(false);
    println!("  ✓ Created config with chunk_size={}", config2.chunk_size);
    println!("  ✓ max_concurrent={}", config2.max_concurrent);
    println!("  ✓ skip_errors={}\n", config2.skip_errors);

    // 3. Batch statistics tracking
    println!("Batch statistics tracking:");
    let stats = BatchStreamStats::new();
    println!("  Initial state:");
    println!("  - Total: {}", stats.total);
    println!("  - Success: {}", stats.success);
    println!("  - Errors: {}", stats.errors);
    println!("  - Not found: {}", stats.not_found);
    println!("  ✓ Stats can track batch operation results\n");

    println!("  Success rate: {:.1}%", stats.success_rate() * 100.0);
    println!("  Error rate: {:.1}%", stats.error_rate() * 100.0);

    Ok(())
}

/// Demonstrate pipeline optimization
async fn demonstrate_pipeline_optimization() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Pipeline strategies
    println!("Pipeline strategies:");

    let _always_strategy = PipelineStrategy::Always;
    println!("  Always: Use pipelining for all batch operations");

    let _adaptive_strategy = PipelineStrategy::Adaptive { threshold: 5 };
    println!("  Adaptive: threshold=5 (use pipelining when batch >= 5)");

    let _never_strategy = PipelineStrategy::Never;
    println!("  Never: disabled\n");

    // 2. Pipeline metrics
    println!("Pipeline metrics tracking:");
    let mut metrics = PipelineMetrics::new();

    // Simulate operations
    metrics.record_pipelined(10);
    metrics.record_pipelined(15);
    metrics.record_sequential();
    metrics.record_sequential();

    println!("  Pipelined operations: {}", metrics.pipelined_ops);
    println!("  Sequential operations: {}", metrics.sequential_ops);
    println!("  Total commands: {}", metrics.total_commands);
    println!(
        "  Average commands per pipeline: {:.2}",
        metrics.avg_commands_per_pipeline()
    );
    println!(
        "  Pipeline efficiency: {:.2}%\n",
        metrics.pipeline_efficiency() * 100.0
    );

    // 3. Pipeline optimizer
    println!("Pipeline optimizer analysis:");
    let mut optimizer = PipelineOptimizer::new();

    // Simulate operation patterns
    for _ in 0..20 {
        optimizer.record_pipelined(8);
    }
    for _ in 0..5 {
        optimizer.record_pipelined(3);
    }

    let recommendation = optimizer.recommend_config();
    println!("  Recommended configuration based on patterns:");
    println!("    Strategy: {:?}", recommendation.strategy);
    println!("    Max batch size: {}", recommendation.max_batch_size);
    println!("    Use transactions: {}", recommendation.use_transactions);

    Ok(())
}

/// Demonstrate performance profiling
async fn demonstrate_performance_profiling(
    _redis_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Operation profiler
    println!("Operation profiler:");
    let mut profiler = Profiler::new();
    profiler.enable();
    println!("  ✓ Profiler enabled");
    println!("  ✓ Can track operation timing, data size, and errors\n");

    // Simulate some operations
    profiler.record("store".to_string(), Duration::from_millis(5), 1024, true);
    profiler.record("store".to_string(), Duration::from_millis(3), 2048, true);
    profiler.record("get".to_string(), Duration::from_millis(2), 512, true);
    profiler.record("get".to_string(), Duration::from_millis(1), 512, true);

    println!("  ✓ Recorded sample operations\n");

    // 2. Profiling report
    println!("Profiling report:");
    let report = profiler.summary();
    println!("{}", report);

    // 3. Slowest operations
    println!("Slowest operations:");
    let slowest = profiler.slowest_operations();
    for (i, profile) in slowest.iter().take(3).enumerate() {
        println!(
            "  {}. {} - avg: {:?}, max: {:?}",
            i + 1,
            profile.name,
            profile.avg_duration(),
            profile.max_duration
        );
    }
    println!();

    // 4. Check for errors
    println!("Error analysis:");
    let error_prone = profiler.most_errors();
    if error_prone.is_empty() || error_prone.iter().all(|p| p.error_count == 0) {
        println!("  ✓ No errors detected\n");
    }

    // 5. Throughput analyzer
    println!("Throughput analyzer:");
    let mut analyzer = ThroughputAnalyzer::new(Duration::from_secs(10));

    // Simulate operations over time
    for i in 0..20 {
        analyzer.record(1);
        if i % 2 == 0 {
            analyzer.record(1);
        }
    }

    println!("  Throughput: {:.2} ops/sec", analyzer.throughput());
    println!("  Sample count: {}", analyzer.sample_count());
    println!("  ✓ Can track operations per second within time windows");

    Ok(())
}
