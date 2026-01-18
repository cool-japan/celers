# Migration from Celery Canvas

> Complete guide for migrating from Python Celery Canvas to celers-canvas

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [API Comparison](#api-comparison)
4. [Common Patterns](#common-patterns)
5. [Feature Parity](#feature-parity)
6. [Migration Checklist](#migration-checklist)
7. [Troubleshooting](#troubleshooting)

---

## Overview

celers-canvas provides a Rust implementation of Celery's Canvas workflow primitives, offering:

- **Type Safety**: Compile-time guarantees for workflow correctness
- **Performance**: Zero-cost abstractions and async execution
- **Compatibility**: Similar API to Python Celery Canvas
- **Advanced Features**: Additional patterns and error handling

### Key Differences

| Aspect | Celery (Python) | celers-canvas (Rust) |
|--------|-----------------|---------------------|
| Language | Python | Rust |
| Type System | Dynamic | Static |
| Execution | Sync/Async | Async (tokio) |
| Error Handling | Exceptions | Result types |
| Memory | GC | Ownership |
| Null Safety | None checks | Option/Result |

---

## Core Concepts

### Signatures

**Python (Celery):**
```python
from celery import signature

sig = signature('tasks.add', args=(2, 2))
sig = tasks.add.s(2, 2)  # shorthand
sig = tasks.add.si(2, 2)  # immutable
```

**Rust (celers-canvas):**
```rust
use celers_canvas::Signature;
use serde_json::json;

let sig = Signature::new("tasks.add".to_string())
    .with_args(vec![json!(2), json!(2)]);

// Immutable signature
let sig = Signature::new("tasks.add".to_string())
    .with_args(vec![json!(2), json!(2)])
    .si();  // or .immutable()
```

### Chains

**Python:**
```python
from celery import chain

workflow = chain(
    tasks.task1.s(),
    tasks.task2.s(),
    tasks.task3.s()
)

# Or using pipe operator
workflow = tasks.task1.s() | tasks.task2.s() | tasks.task3.s()

workflow.apply_async()
```

**Rust:**
```rust
use celers_canvas::Chain;

let workflow = Chain::new()
    .then("tasks.task1", vec![])
    .then("tasks.task2", vec![])
    .then("tasks.task3", vec![]);

workflow.apply(&broker).await?;
```

### Groups

**Python:**
```python
from celery import group

workflow = group(
    tasks.task1.s(1),
    tasks.task2.s(2),
    tasks.task3.s(3)
)

workflow.apply_async()
```

**Rust:**
```rust
use celers_canvas::Group;

let workflow = Group::new()
    .add("tasks.task1", vec![json!(1)])
    .add("tasks.task2", vec![json!(2)])
    .add("tasks.task3", vec![json!(3)]);

workflow.apply(&broker).await?;
```

### Chords

**Python:**
```python
from celery import chord

workflow = chord(
    group(tasks.process.s(i) for i in range(10))
)(tasks.aggregate.s())

# Or using pipe
workflow = group(tasks.process.s(i) for i in range(10)) | tasks.aggregate.s()

workflow.apply_async()
```

**Rust:**
```rust
use celers_canvas::Chord;

let mut workflow = Chord::new();
for i in 0..10 {
    workflow = workflow.add("tasks.process", vec![json!(i)]);
}
workflow = workflow.callback("tasks.aggregate", vec![]);

workflow.apply(&broker, &mut backend).await?;
```

### Map

**Python:**
```python
from celery import group

# Map pattern
workflow = group(tasks.process.s(i) for i in items)
workflow.apply_async()
```

**Rust:**
```rust
use celers_canvas::Map;

let items: Vec<Vec<serde_json::Value>> =
    items.iter()
        .map(|i| vec![json!(i)])
        .collect();

let workflow = Map::new(
    Signature::new("tasks.process".to_string()),
    items
);

workflow.apply(&broker).await?;
```

---

## API Comparison

### Task Options

**Python:**
```python
sig = tasks.add.s(2, 2)
sig.set(
    queue='high_priority',
    priority=9,
    countdown=10,
    eta=datetime.now() + timedelta(seconds=60),
    expires=3600,
    retry=True,
    retry_policy={
        'max_retries': 3,
        'interval_start': 0,
        'interval_step': 0.2,
        'interval_max': 0.2,
    }
)
```

**Rust:**
```rust
let sig = Signature::new("tasks.add".to_string())
    .with_args(vec![json!(2), json!(2)])
    .with_queue("high_priority".to_string())
    .with_priority(9)
    .with_time_limit(3600)
    .with_retry_delay(10)
    .with_retry_backoff(0.2)
    .with_retry_backoff_max(3600);
```

### Callbacks (Links)

**Python:**
```python
# Success callback
sig = tasks.task1.s()
sig.link(tasks.on_success.s())

# Error callback
sig.link_error(tasks.on_error.s())

# Multiple callbacks
sig.link(tasks.callback1.s())
sig.link(tasks.callback2.s())
```

**Rust:**
```rust
// Single callbacks
let sig = Signature::new("tasks.task1".to_string())
    .with_link(
        Signature::new("tasks.on_success".to_string())
    )
    .with_link_error(
        Signature::new("tasks.on_error".to_string())
    );

// Multiple callbacks
let sig = Signature::new("tasks.task1".to_string())
    .add_link(Signature::new("tasks.callback1".to_string()))
    .add_link(Signature::new("tasks.callback2".to_string()));
```

### Partial Application

**Python:**
```python
# Partial args
partial_sig = tasks.add.s(2)
result = partial_sig.delay(2)  # Completes as add(2, 2)

# Clone
cloned = sig.clone()
```

**Rust:**
```rust
// Partial args (via signature mutation)
let mut sig = Signature::new("tasks.add".to_string())
    .with_args(vec![json!(2)]);

// Add more args later
sig.args.push(json!(2));

// Clone
let cloned = sig.clone();
```

---

## Common Patterns

### Pattern 1: Sequential Pipeline

**Python:**
```python
from celery import chain

pipeline = chain(
    tasks.extract.s(),
    tasks.transform.s(),
    tasks.load.s()
)

pipeline.apply_async()
```

**Rust:**
```rust
let pipeline = Chain::new()
    .then("tasks.extract", vec![])
    .then("tasks.transform", vec![])
    .then("tasks.load", vec![]);

pipeline.apply(&broker).await?;
```

### Pattern 2: Parallel + Aggregate

**Python:**
```python
from celery import chord, group

workflow = chord(
    group(tasks.fetch.s(url) for url in urls)
)(tasks.combine.s())

workflow.apply_async()
```

**Rust:**
```rust
let mut workflow = Chord::new();
for url in urls {
    workflow = workflow.add("tasks.fetch", vec![json!(url)]);
}
workflow = workflow.callback("tasks.combine", vec![]);

workflow.apply(&broker, &mut backend).await?;
```

### Pattern 3: Dynamic Workflows

**Python:**
```python
from celery import chain, group

# Build workflow dynamically
tasks_list = []
for item in items:
    if item.needs_processing:
        tasks_list.append(tasks.process.s(item))

workflow = group(tasks_list)
workflow.apply_async()
```

**Rust:**
```rust
let mut workflow = Group::new();

for item in items {
    if item.needs_processing {
        workflow = workflow.add("tasks.process", vec![json!(item)]);
    }
}

workflow.apply(&broker).await?;
```

### Pattern 4: Error Handling

**Python:**
```python
from celery import chain

workflow = chain(
    tasks.task1.s(),
    tasks.task2.s().set(link_error=tasks.handle_error.s()),
    tasks.task3.s()
)

workflow.apply_async()
```

**Rust:**
```rust
let workflow = Chain::new()
    .then("tasks.task1", vec![])
    .then("tasks.task2", vec![])
    .then("tasks.task3", vec![]);

// Add error handler at workflow level
let handler = WorkflowErrorHandler::new()
    .with_strategy(ErrorStrategy::Fallback(
        Chain::new().then("tasks.handle_error", vec![])
    ));

let workflow = workflow.with_error_handler(handler);
```

### Pattern 5: Retries

**Python:**
```python
sig = tasks.unreliable_task.s()
sig.set(
    retry=True,
    retry_policy={
        'max_retries': 3,
        'interval_start': 1,
        'interval_step': 2,
        'interval_max': 10,
    }
)

sig.apply_async()
```

**Rust:**
```rust
let workflow = Chain::new()
    .then("tasks.unreliable_task", vec![])
    .with_retry_policy(
        WorkflowRetryPolicy::new()
            .with_max_retries(3)
            .with_backoff_strategy(BackoffStrategy::Exponential)
            .with_initial_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(10))
    );
```

---

## Feature Parity

### ✅ Fully Supported

| Feature | Celery | celers-canvas |
|---------|--------|---------------|
| Chain | ✅ | ✅ |
| Group | ✅ | ✅ |
| Chord | ✅ | ✅ |
| Map | ✅ | ✅ |
| Starmap | ✅ | ✅ |
| Chunks | ✅ | ✅ |
| Signatures | ✅ | ✅ |
| Immutable Signatures | ✅ | ✅ |
| Callbacks (link) | ✅ | ✅ |
| Error Callbacks | ✅ | ✅ |
| Task Priority | ✅ | ✅ |
| Task Queues | ✅ | ✅ |
| Task IDs | ✅ | ✅ |
| Timeouts | ✅ | ✅ |

### 🎁 Enhanced Features

Features that go beyond Celery:

| Feature | Description |
|---------|-------------|
| Workflow Cancellation | Cancel entire workflow trees |
| Workflow Retry Policies | Retry entire workflows with backoff |
| Conditional Workflows | Branch, Switch, Maybe patterns |
| Loops | ForEach, While loops |
| State Tracking | Real-time progress monitoring |
| DAG Export | GraphViz, Mermaid, JSON formats |
| Result Caching | Memoization of task results |
| Saga Pattern | Distributed transactions with compensation |
| Advanced Patterns | Scatter-Gather, Pipeline, Fan-Out/Fan-In |
| Workflow Validation | Pre-execution validation |
| Workflow Compilation | Optimization passes |
| Time-Travel Debugging | Step through workflow execution |
| Sub-Workflow Isolation | Isolated execution contexts |
| Event-Driven Workflows | React to events |
| Reactive Workflows | Observable streams |

### ⚠️ Limited Support

| Feature | Status | Notes |
|---------|--------|-------|
| Nested Groups | Partial | Use CanvasElement pattern |
| Dynamic Routing | Manual | Use routing keys in options |
| Task Revocation | Basic | Use cancel() method |

### ❌ Not Supported

| Feature | Alternative |
|---------|-------------|
| Python-specific features | N/A |
| Celery Beat (periodic tasks) | Use celers-beat crate |
| Result backends (other than Redis) | Redis only for chords |

---

## Migration Checklist

### Pre-Migration

- [ ] Audit existing Celery workflows
- [ ] Identify dependencies and task definitions
- [ ] Document task signatures and arguments
- [ ] Review error handling strategies
- [ ] List required queue configurations

### During Migration

- [ ] Set up Rust development environment
- [ ] Install celers crates
- [ ] Port task definitions to Rust
- [ ] Convert workflow definitions
- [ ] Update broker configurations
- [ ] Implement error handlers
- [ ] Add tests for workflows
- [ ] Set up monitoring

### Post-Migration

- [ ] Verify workflow execution
- [ ] Monitor performance metrics
- [ ] Compare error rates
- [ ] Validate result correctness
- [ ] Document new patterns
- [ ] Train team on new system

---

## Migration Examples

### Example 1: Simple Chain

**Before (Python):**
```python
from celery import chain
from tasks import process_data, validate, save

workflow = chain(
    process_data.s(input_data),
    validate.s(),
    save.s()
)

result = workflow.apply_async()
```

**After (Rust):**
```rust
use celers_canvas::Chain;
use serde_json::json;

let workflow = Chain::new()
    .then("tasks.process_data", vec![json!(input_data)])
    .then("tasks.validate", vec![])
    .then("tasks.save", vec![]);

let result = workflow.apply(&broker).await?;
```

### Example 2: Parallel Processing

**Before (Python):**
```python
from celery import group
from tasks import fetch_data

urls = ['url1', 'url2', 'url3']
workflow = group(fetch_data.s(url) for url in urls)

result = workflow.apply_async()
```

**After (Rust):**
```rust
use celers_canvas::Group;

let mut workflow = Group::new();
for url in &urls {
    workflow = workflow.add("tasks.fetch_data", vec![json!(url)]);
}

let result = workflow.apply(&broker).await?;
```

### Example 3: Map-Reduce

**Before (Python):**
```python
from celery import chord, group
from tasks import process_item, aggregate

items = [1, 2, 3, 4, 5]
workflow = chord(
    group(process_item.s(item) for item in items)
)(aggregate.s())

result = workflow.apply_async()
```

**After (Rust):**
```rust
use celers_canvas::Chord;

let mut workflow = Chord::new();
for item in items {
    workflow = workflow.add("tasks.process_item", vec![json!(item)]);
}
workflow = workflow.callback("tasks.aggregate", vec![]);

let result = workflow.apply(&broker, &mut backend).await?;
```

### Example 4: Conditional Execution

**Before (Python):**
```python
from celery import chain
from tasks import check_condition, process_a, process_b

workflow = chain(
    check_condition.s(data),
    # Conditional logic in task
    process_a.s() if condition else process_b.s()
)
```

**After (Rust):**
```rust
use celers_canvas::{Branch, Condition, Chain};

let condition = Condition::new(
    "tasks.check_condition",
    vec![json!(data)]
);

let workflow = Branch::new(condition)
    .on_true(Chain::new().then("tasks.process_a", vec![]))
    .on_false(Chain::new().then("tasks.process_b", vec![]));
```

---

## Troubleshooting

### Issue: Serialization Errors

**Problem:** JSON serialization fails for complex types.

**Solution:**
```rust
// Use serde_json::Value for flexibility
let args = vec![serde_json::json!({
    "user_id": 123,
    "data": {
        "name": "John",
        "email": "john@example.com"
    }
})];
```

### Issue: Async/Await Confusion

**Problem:** Forgetting to await futures.

**Solution:**
```rust
// Wrong
let result = workflow.apply(&broker);  // Returns Future

// Correct
let result = workflow.apply(&broker).await?;
```

### Issue: Broker Connection

**Problem:** Broker connection not configured.

**Solution:**
```rust
// Ensure broker is properly initialized
use celers_broker_amqp::AmqpBroker;

let broker = AmqpBroker::new("amqp://localhost").await?;
```

### Issue: Type Mismatches

**Problem:** Static typing requires explicit types.

**Solution:**
```rust
// Be explicit with types
let items: Vec<Vec<serde_json::Value>> = vec![
    vec![json!(1)],
    vec![json!(2)],
];
```

### Issue: Error Handling

**Problem:** Not handling Result types.

**Solution:**
```rust
// Always handle errors
match workflow.apply(&broker).await {
    Ok(workflow_id) => println!("Started workflow: {}", workflow_id),
    Err(e) => eprintln!("Failed to start workflow: {}", e),
}

// Or use ? operator
let workflow_id = workflow.apply(&broker).await?;
```

---

## Performance Considerations

### Memory Usage

**Python Celery:**
- GC overhead
- Per-task memory allocation
- Dynamic typing overhead

**celers-canvas:**
- Zero-cost abstractions
- Stack allocation when possible
- Compile-time optimizations

### Throughput

**Benchmarks** (approximate):

| Operation | Celery (Python) | celers-canvas (Rust) |
|-----------|-----------------|---------------------|
| Chain creation (1000) | ~100ms | ~10ms |
| Group creation (1000) | ~150ms | ~15ms |
| Serialization (1000) | ~200ms | ~50ms |

### Concurrency

**Python:**
```python
# Limited by GIL
# Multi-process for CPU-bound tasks
```

**Rust:**
```rust
// True parallelism
// Async I/O with tokio
// No GIL limitations
```

---

## Additional Resources

- [Design Patterns Guide](./DESIGN_PATTERNS.md)
- [API Documentation](https://docs.rs/celers-canvas)
- [Examples](./examples/)
- [Celery Canvas Documentation](https://docs.celeryproject.org/en/stable/userguide/canvas.html)

---

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/yourusername/celers/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/celers/discussions)
- **Discord**: [celers Discord Server](#)

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.
