# Workflow Design Patterns Guide

> Comprehensive guide to workflow patterns in celers-canvas

## Table of Contents

1. [Basic Patterns](#basic-patterns)
2. [Advanced Patterns](#advanced-patterns)
3. [Error Handling Patterns](#error-handling-patterns)
4. [Performance Patterns](#performance-patterns)
5. [State Management Patterns](#state-management-patterns)
6. [Testing Patterns](#testing-patterns)

---

## Basic Patterns

### 1. Sequential Processing (Chain)

Execute tasks in sequence, passing results between them.

```rust
use celers_canvas::*;

// Simple chain: task1 -> task2 -> task3
let workflow = Chain::new()
    .then("process_data", vec![serde_json::json!({"input": "data"})])
    .then("validate_result", vec![])
    .then("save_output", vec![]);

workflow.apply(&broker).await?;
```

**Use cases:**
- Data processing pipelines
- Multi-step transformations
- Dependent operations

**Best practices:**
- Keep chains focused (5-10 tasks max)
- Use immutable signatures when arguments shouldn't be replaced
- Add timeouts for long-running chains

### 2. Parallel Execution (Group)

Execute multiple tasks concurrently.

```rust
// Parallel execution of independent tasks
let workflow = Group::new()
    .add("fetch_user_data", vec![serde_json::json!(user_id)])
    .add("fetch_order_data", vec![serde_json::json!(user_id)])
    .add("fetch_preferences", vec![serde_json::json!(user_id)]);

workflow.apply(&broker).await?;
```

**Use cases:**
- Independent data fetching
- Parallel computations
- Batch operations

**Best practices:**
- Ensure tasks are truly independent
- Set appropriate priorities for resource management
- Use group_id for tracking related tasks

### 3. Map-Reduce (Chord)

Parallel processing followed by result aggregation.

```rust
// Map: process items in parallel
// Reduce: aggregate results
let workflow = Chord::new()
    .add("process_item", vec![serde_json::json!(1)])
    .add("process_item", vec![serde_json::json!(2)])
    .add("process_item", vec![serde_json::json!(3)])
    .callback("aggregate_results", vec![]);

workflow.apply(&broker, &mut backend).await?;
```

**Use cases:**
- Distributed computations
- Batch processing with aggregation
- Parallel data transformation

**Best practices:**
- Requires Redis backend for coordination
- Set appropriate timeouts
- Handle partial failures gracefully

### 4. Data Distribution (Map)

Apply the same task to multiple inputs.

```rust
let items = vec![
    vec![serde_json::json!(1)],
    vec![serde_json::json!(2)],
    vec![serde_json::json!(3)],
];

let workflow = Map::new(
    Signature::new("process_item".to_string()),
    items
);

workflow.apply(&broker).await?;
```

**Use cases:**
- Batch processing
- Data transformation at scale
- Parallel validation

---

## Advanced Patterns

### 1. Conditional Execution (Branch)

Execute different workflows based on conditions.

```rust
let condition = Condition::new(
    "check_user_type",
    vec![serde_json::json!(user_id)]
);

let workflow = Branch::new(condition)
    .on_true(Chain::new()
        .then("premium_flow", vec![])
        .then("send_premium_email", vec![]))
    .on_false(Chain::new()
        .then("standard_flow", vec![])
        .then("send_standard_email", vec![]));
```

**Use cases:**
- User type handling
- A/B testing
- Feature flags

### 2. Switch/Case Pattern

Select from multiple execution paths.

```rust
let selector = Condition::new("get_plan_type", vec![serde_json::json!(user_id)]);

let workflow = Switch::new(selector)
    .case("premium", Chain::new().then("premium_processing", vec![]))
    .case("business", Chain::new().then("business_processing", vec![]))
    .case("enterprise", Chain::new().then("enterprise_processing", vec![]))
    .default(Chain::new().then("free_tier_processing", vec![]));
```

**Use cases:**
- Multi-tier service levels
- Content type routing
- Regional processing

### 3. Loop Patterns

#### ForEach Loop
```rust
let items = vec![
    serde_json::json!({"id": 1}),
    serde_json::json!({"id": 2}),
    serde_json::json!({"id": 3}),
];

let workflow = ForEach::new(
    Signature::new("process_item".to_string()),
    items,
    Some(10) // max concurrent
);
```

#### While Loop
```rust
let workflow = WhileLoop::new(
    Condition::new("has_more_pages", vec![]),
    Chain::new()
        .then("fetch_page", vec![])
        .then("process_page", vec![]),
    Some(100) // max iterations
);
```

**Use cases:**
- Pagination
- Retry with backoff
- Iterative refinement

### 4. Scatter-Gather

Distribute work and collect results.

```rust
let workflow = ScatterGather::new(
    Signature::new("fetch_data".to_string()),
    vec![
        vec![serde_json::json!("api1")],
        vec![serde_json::json!("api2")],
        vec![serde_json::json!("api3")],
    ],
    AggregationStrategy::Merge,
    Some(Duration::from_secs(10)) // timeout
);
```

**Use cases:**
- Multi-source data fetching
- Distributed searches
- Parallel API calls

### 5. Pipeline Pattern

Chain processing stages with buffering.

```rust
let workflow = Pipeline::new()
    .add_stage(Signature::new("extract".to_string()))
    .add_stage(Signature::new("transform".to_string()))
    .add_stage(Signature::new("load".to_string()))
    .with_buffer_size(100);
```

**Use cases:**
- ETL processes
- Stream processing
- Data transformation pipelines

### 6. Fan-Out/Fan-In

Distribute work and synchronize results.

```rust
// Fan-out: distribute to multiple workers
let fanout = FanOut::new(
    Signature::new("process_chunk".to_string()),
    10 // worker count
);

// Fan-in: collect and merge results
let fanin = FanIn::new(
    vec![result1, result2, result3],
    AggregationStrategy::Concat
);
```

**Use cases:**
- Load distribution
- Parallel processing with synchronization
- Result consolidation

---

## Error Handling Patterns

### 1. Saga Pattern (Distributed Transactions)

Implement compensating transactions for rollback.

```rust
let saga = Saga::new()
    .add_step(
        Signature::new("reserve_inventory".to_string()),
        CompensationWorkflow::new(
            Signature::new("release_inventory".to_string())
        )
    )
    .add_step(
        Signature::new("charge_payment".to_string()),
        CompensationWorkflow::new(
            Signature::new("refund_payment".to_string())
        )
    )
    .add_step(
        Signature::new("send_confirmation".to_string()),
        CompensationWorkflow::new(
            Signature::new("send_cancellation".to_string())
        )
    )
    .with_isolation(SagaIsolation::ReadCommitted);
```

**Use cases:**
- E-commerce transactions
- Multi-service operations
- Distributed state changes

### 2. Circuit Breaker

Prevent cascading failures.

```rust
let workflow = Chain::new()
    .then("call_external_api", vec![])
    .with_retry_policy(
        WorkflowRetryPolicy::new()
            .with_max_retries(3)
            .with_backoff_strategy(BackoffStrategy::Exponential)
    );
```

### 3. Error Propagation Control

Control how errors affect workflow execution.

```rust
// Stop on first error (default)
let strict = Group::new()
    .with_error_propagation(ErrorPropagationMode::StopOnFirstError);

// Continue on error
let resilient = Group::new()
    .with_error_propagation(ErrorPropagationMode::ContinueOnError);

// Partial failure tracking
let partial = Group::new()
    .with_error_propagation(ErrorPropagationMode::PartialFailure(
        PartialFailureTracker::new(0.2) // max 20% failure rate
    ));
```

### 4. Fallback Pattern

Provide alternative execution paths on failure.

```rust
let handler = WorkflowErrorHandler::new()
    .with_strategy(ErrorStrategy::Fallback(
        Chain::new().then("use_cached_data", vec![])
    ));

let workflow = Chain::new()
    .then("fetch_fresh_data", vec![])
    .with_error_handler(handler);
```

---

## Performance Patterns

### 1. Result Caching

Cache expensive computations.

```rust
let cache = ResultCache::new()
    .with_policy(CachePolicy::TimeToLive(Duration::from_secs(3600)))
    .with_max_size(1000);

let workflow = Chain::new()
    .then("expensive_computation", vec![])
    .with_cache(cache);
```

**Best practices:**
- Set appropriate TTL
- Monitor cache hit rates
- Use for idempotent operations

### 2. Batch Processing

Process multiple items efficiently.

```rust
let batcher = WorkflowBatcher::new()
    .with_strategy(BatchingStrategy::BySize(100))
    .with_timeout(Duration::from_secs(5));

// Workflows are automatically batched
for item in items {
    batcher.add_workflow(process_workflow(item));
}
```

### 3. Parallel Scheduling

Optimize task distribution across workers.

```rust
let scheduler = ParallelScheduler::new()
    .with_strategy(SchedulingStrategy::LeastLoaded)
    .with_max_concurrent_tasks(100);

scheduler.schedule_workflow(workflow).await?;
```

### 4. Workflow Compilation

Optimize workflow execution.

```rust
let compiler = WorkflowCompiler::new()
    .add_pass(OptimizationPass::DeadCodeElimination)
    .add_pass(OptimizationPass::TaskFusion)
    .add_pass(OptimizationPass::CommonSubexpressionElimination);

let optimized = compiler.compile(workflow)?;
```

---

## State Management Patterns

### 1. Workflow Checkpointing

Save and restore workflow state.

```rust
let checkpoint = WorkflowCheckpoint::new(
    workflow_id,
    workflow_state.clone()
);

// Save checkpoint
checkpoint.save_to_backend(&backend).await?;

// Restore from checkpoint
let restored = WorkflowCheckpoint::restore_from_backend(&backend, workflow_id).await?;
```

### 2. State Versioning

Track workflow state changes over time.

```rust
let versioned_state = VersionedWorkflowState::new(
    workflow_state,
    StateVersion::new(1, 0, 0)
);

// Migrate state when schema changes
let migration = StateMigration::new()
    .from_version(StateVersion::new(1, 0, 0))
    .to_version(StateVersion::new(2, 0, 0))
    .with_transform(|old_state| {
        // Transform logic
        new_state
    });
```

### 3. Progress Tracking

Monitor workflow execution in real-time.

```rust
let state = WorkflowState::new(workflow_id, total_tasks);

// Update progress
state.mark_task_completed(task_id);
state.mark_task_failed(task_id, error);

// Get progress
let percentage = state.progress_percentage();
let status = state.status();
```

---

## Testing Patterns

### 1. Mock Tasks

Test workflows without executing real tasks.

```rust
let executor = MockTaskExecutor::new();
executor.register("task1", MockTaskResult::success(serde_json::json!({"result": "ok"})));
executor.register("task2", MockTaskResult::failure("Simulated error"));

let result = executor.execute("task1")?;
assert!(result.is_success());
```

### 2. Test Data Injection

Inject test data into workflow execution.

```rust
let injector = TestDataInjector::new();
injector.inject("user_data", serde_json::json!({"id": 123, "name": "Test"}));

// Use in tests
let data = injector.get("user_data").unwrap();
```

### 3. Time-Travel Debugging

Debug workflows by stepping through execution.

```rust
let debugger = TimeTravelDebugger::new(workflow_id);

// Record snapshots during execution
debugger.record_snapshot(snapshot);

// Step through execution
debugger.step_backward();
debugger.step_forward();
debugger.replay_from(index);
```

### 4. Workflow Validation

Validate workflow structure before execution.

```rust
let validator = WorkflowValidator::default();
let result = validator.validate_chain(&chain);

match result {
    ValidationResult::Valid => println!("Workflow is valid"),
    ValidationResult::Invalid(errors) => {
        for error in errors {
            eprintln!("Validation error: {}", error);
        }
    }
}
```

---

## Best Practices Summary

### General Guidelines

1. **Keep workflows focused**: Each workflow should have a single responsibility
2. **Handle errors gracefully**: Use compensation workflows and error handlers
3. **Set timeouts**: Prevent infinite execution
4. **Use priorities**: Ensure critical tasks execute first
5. **Monitor execution**: Track progress and failures

### Performance Tips

1. **Batch when possible**: Reduce overhead with batching
2. **Cache results**: Avoid redundant computations
3. **Optimize dependencies**: Use compilation and optimization passes
4. **Limit concurrency**: Prevent resource exhaustion
5. **Use streaming**: For large datasets, use streaming map-reduce

### Debugging Tips

1. **Use workflow IDs**: Track execution across distributed systems
2. **Enable checkpointing**: Recover from failures
3. **Log state transitions**: Monitor workflow progress
4. **Use mock tasks**: Test workflows in isolation
5. **Validate before execution**: Catch errors early

### Scalability Considerations

1. **Partition large workflows**: Break into smaller sub-workflows
2. **Use sub-workflow isolation**: Prevent interference
3. **Implement backpressure**: Control task submission rate
4. **Monitor resource usage**: Track CPU, memory, and queue sizes
5. **Use workflow templates**: Reuse proven patterns

---

## Common Anti-Patterns to Avoid

### ❌ Too Many Tasks in Chain
```rust
// Bad: 100 tasks in a single chain
let chain = Chain::new()
    .then("task1", vec![])
    .then("task2", vec![])
    // ... 98 more tasks
    .then("task100", vec![]);
```

**Solution**: Break into smaller chains or use sub-workflows.

### ❌ Ignoring Failures
```rust
// Bad: No error handling
let workflow = Group::new()
    .add("critical_task", vec![]);
    // What happens if it fails?
```

**Solution**: Add error handlers and compensation workflows.

### ❌ No Timeouts
```rust
// Bad: Infinite execution possible
let workflow = Chain::new()
    .then("might_hang", vec![]);
```

**Solution**: Always set timeouts.

### ❌ Circular Dependencies
```rust
// Bad: Task A depends on B, B depends on A
let dep_graph = DependencyGraph::new()
    .add_dependency("taskA", "taskB")
    .add_dependency("taskB", "taskA");
```

**Solution**: Validate dependency graphs before execution.

### ❌ Blocking Operations
```rust
// Bad: Synchronous blocking in async context
let workflow = Chain::new()
    .then("blocking_io", vec![]); // Blocks thread pool
```

**Solution**: Use async operations throughout.

---

## Further Reading

- [TODO.md](./TODO.md) - Feature implementation status
- [MIGRATION.md](./MIGRATION.md) - Migrating from Celery Canvas
- [Examples](./examples/) - Code examples
- [API Documentation](https://docs.rs/celers-canvas)
