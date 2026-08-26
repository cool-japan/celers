# celers-canvas

**Version: 0.3.1 | Status: [Stable] | Tests: 388 (`--all-features`) + 85 doctests | Updated: 2026-08-26**

Distributed workflow primitives for CeleRS task orchestration. Build complex task dependencies with Chain, Group, Chord, Map, and Starmap patterns.

## Overview

Production-ready workflow patterns inspired by Celery's Canvas:

- ✅ **Chain**: Sequential task execution with result passing
- ✅ **Group**: Parallel task execution
- ✅ **Chord**: Map-reduce pattern (parallel tasks + callback)
- ✅ **Map**: Apply task to multiple argument sets
- ✅ **Starmap**: Like Map but unpacks arguments
- ✅ **Signature**: Reusable task definitions with arguments
- ✅ **Priority Support**: Task prioritization in workflows
- ✅ **Immutability**: Prevent argument replacement in chains
- ✅ **Nested Workflows**: Real nested chain/group/chord execution (`NestedChain`, `NestedGroup`) — chord callbacks are enqueued even when nested inside a chain or group
- ✅ **DAG Visualization**: Export any workflow to Mermaid or GraphViz DOT (`DagExport`/`DagVisualize`)
- ✅ **Loops, Templates & Sub-Workflows**: Bounded loops, map-style iteration, parameterized templates, sub-workflow composition
- ✅ **Versioning**: Schema-versioned serialization with a migration registry for upgrading older payloads
- ✅ **Rate-Limit-Aware Dispatch**: Derive per-member staggered countdowns from a token-bucket `RateLimitConfig`

## Quick Start

```rust
use celers_canvas::{Chain, Group, Chord, Map, Signature};
use celers_broker_redis::RedisBroker;

// Create broker
let broker = RedisBroker::new("redis://localhost:6379", "celery")?;

// Chain: Sequential execution
let workflow = Chain::new()
    .then("download_data", vec![serde_json::json!("https://example.com")])
    .then("process_data", vec![])
    .then("upload_result", vec![]);

let task_id = workflow.apply(&broker).await?;
println!("Chain started: {}", task_id);
```

## Workflow Primitives

### Chain - Sequential Execution

Execute tasks one after another, passing results as arguments:

```rust
use celers_canvas::Chain;

// task1(args1) -> task2(result1) -> task3(result2)
let chain = Chain::new()
    .then("fetch_user", vec![serde_json::json!(user_id)])
    .then("validate_user", vec![])
    .then("save_user", vec![]);

// Start the chain
let first_task_id = chain.apply(&broker).await?;
```

**How it works:**
1. First task executes with provided arguments
2. Result passed to next task
3. Continues until all tasks complete
4. Final task returns the result

**Use cases:**
- Data pipelines (ETL workflows)
- Multi-stage processing
- Sequential transformations
- Dependent operations

### Group - Parallel Execution

Execute multiple tasks in parallel:

```rust
use celers_canvas::Group;

// (task1 | task2 | task3)
let group = Group::new()
    .add("process_chunk_1", vec![serde_json::json!(data1)])
    .add("process_chunk_2", vec![serde_json::json!(data2)])
    .add("process_chunk_3", vec![serde_json::json!(data3)]);

// Start all tasks in parallel
let group_id = group.apply(&broker).await?;
```

**Features:**
- Tasks execute independently
- No result aggregation
- Group ID for tracking
- Individual task priorities

**Use cases:**
- Parallel data processing
- Bulk operations
- Independent computations
- Fan-out patterns

### Chord - Map-Reduce Pattern

Execute tasks in parallel, then run callback with aggregated results:

```rust
use celers_canvas::{Chord, Group, Signature};
use celers_backend_redis::RedisResultBackend;

// (task1 | task2 | task3) -> callback([result1, result2, result3])
let header = Group::new()
    .add("compute_partial", vec![serde_json::json!(1)])
    .add("compute_partial", vec![serde_json::json!(2)])
    .add("compute_partial", vec![serde_json::json!(3)]);

let callback = Signature::new("aggregate_results".to_string());
let chord = Chord::new(header, callback);

// Requires result backend for coordination
let mut backend = RedisResultBackend::new("redis://localhost:6379")?;
let chord_id = chord.apply(&broker, &mut backend).await?;
```

**How it works:**
1. Header tasks execute in parallel
2. Each completion increments atomic counter (Redis INCR)
3. When all tasks complete, callback enqueued
4. Callback receives array of all results

**Requirements:**
- `backend-redis` feature enabled
- Redis result backend for coordination
- Atomic counter for barrier synchronization

**Use cases:**
- Map-reduce operations
- Aggregating parallel results
- Distributed computations
- Parallel data collection + summarization

### Map - Batch Processing

Apply the same task to multiple argument sets:

```rust
use celers_canvas::{Map, Signature};

// map(task, [args1, args2, args3])
let task = Signature::new("process_image".to_string());
let argsets = vec![
    vec![serde_json::json!("image1.jpg")],
    vec![serde_json::json!("image2.jpg")],
    vec![serde_json::json!("image3.jpg")],
];

let map = Map::new(task, argsets);
let group_id = map.apply(&broker).await?;
```

**Equivalent to:**
```rust
Group::new()
    .add("process_image", vec![serde_json::json!("image1.jpg")])
    .add("process_image", vec![serde_json::json!("image2.jpg")])
    .add("process_image", vec![serde_json::json!("image3.jpg")])
```

**Use cases:**
- Batch image/video processing
- Bulk data transformations
- Same operation on many inputs
- Parallel file operations

### Starmap - Unpacked Arguments

Like Map but unpacks argument tuples:

```rust
use celers_canvas::{Starmap, Signature};

// starmap(task, [(a1, b1), (a2, b2)])
let task = Signature::new("add".to_string());
let argsets = vec![
    vec![serde_json::json!(10), serde_json::json!(20)],  // add(10, 20)
    vec![serde_json::json!(30), serde_json::json!(40)],  // add(30, 40)
];

let starmap = Starmap::new(task, argsets);
let group_id = starmap.apply(&broker).await?;
```

**Difference from Map:**
- Map: Each argset is a single argument
- Starmap: Each argset is unpacked as multiple arguments

**Use cases:**
- Functions with multiple parameters
- Coordinate processing (x, y pairs)
- Key-value operations
- Tuple-based data processing

## Signature - Reusable Task Definitions

```rust
use celers_canvas::Signature;
use std::collections::HashMap;

// Basic signature
let sig = Signature::new("my_task".to_string());

// With positional arguments
let sig = Signature::new("process".to_string())
    .with_args(vec![
        serde_json::json!("input.txt"),
        serde_json::json!(100),
    ]);

// With keyword arguments
let mut kwargs = HashMap::new();
kwargs.insert("timeout".to_string(), serde_json::json!(300));
kwargs.insert("retries".to_string(), serde_json::json!(3));

let sig = Signature::new("process".to_string())
    .with_kwargs(kwargs);

// With priority
let sig = Signature::new("urgent_task".to_string())
    .with_priority(9);  // Higher = more urgent

// Immutable (args cannot be replaced in chain)
let sig = Signature::new("critical_task".to_string())
    .with_args(vec![serde_json::json!("data")])
    .immutable();
```

## Advanced Patterns

### Nested Workflows

`NestedChain` and `NestedGroup` (`celers_canvas::{NestedChain, NestedGroup, CanvasElement}`)
combine Chain/Group/Chord/Map/Branch/Switch elements into one workflow tree and execute it for
real — including a chord nested inside a chain, whose header fans out and whose callback is
enqueued once the header completes (previously this was silently dropped):

```rust
use celers_canvas::{Group, NestedChain, Signature};

let workflow = NestedChain::new()
    .then("step1", vec![])
    .then_group(
        Group::new()
            .add("parallel_a", vec![])
            .add("parallel_b", vec![]),
    )
    .then_chord(
        Group::new()
            .add("compute_partial", vec![serde_json::json!(1)])
            .add("compute_partial", vec![serde_json::json!(2)]),
        Signature::new("aggregate_results".to_string()),
    )
    .then("step2", vec![]);

let task_id = workflow.apply(&broker).await?;
```

`NestedGroup` is the parallel equivalent, running its members (which may themselves be
chains/groups/chords) concurrently via the same `apply_chord_element` execution path.

### Priority Workflows

Assign priorities to workflow tasks:

```rust
let high_priority_chain = Chain::new()
    .then_signature(
        Signature::new("urgent_task".to_string())
            .with_priority(9)  // Highest priority
    )
    .then_signature(
        Signature::new("followup_task".to_string())
            .with_priority(8)
    );
```

### Partial Chord Results

Handle partial results with error handling:

```rust
// In callback task, check which tasks completed successfully
// Results array may contain errors for failed tasks
async fn aggregate_results(results: Vec<Option<String>>) -> Result<String, String> {
    let successful: Vec<_> = results.into_iter()
        .filter_map(|r| r)
        .collect();

    if successful.len() >= 2 {
        Ok(format!("Processed {} items", successful.len()))
    } else {
        Err("Too many failures".to_string())
    }
}
```

### Workflow Optimization

Optimize workflows before execution using the `WorkflowCompiler`:

```rust
use celers_canvas::{Chain, Group, WorkflowCompiler, OptimizationPass};

// Create a workflow with redundant tasks
let chain = Chain::new()
    .then_signature(Signature::new("process".to_string()).with_args(vec![json!(1)]))
    .then_signature(Signature::new("validate".to_string()))
    .then_signature(Signature::new("process".to_string()).with_args(vec![json!(1)])); // Duplicate

// Optimize the workflow
let compiler = WorkflowCompiler::new().aggressive();
let optimized = compiler.optimize_chain(&chain);

// The optimized chain has duplicates removed
assert_eq!(optimized.tasks.len(), 2); // Was 3, now 2
```

**Available Optimization Passes:**

- **Common Subexpression Elimination (CSE)**: Removes duplicate task signatures
  ```rust
  let compiler = WorkflowCompiler::new().aggressive();
  // Deduplicates identical tasks (same name, args, kwargs)
  ```

- **Dead Code Elimination (DCE)**: Removes unreachable or invalid tasks
  ```rust
  let compiler = WorkflowCompiler::new();
  // Removes tasks with empty names or no effect
  ```

- **Task Fusion**: Combines sequential tasks with the same name
  ```rust
  let compiler = WorkflowCompiler::new().aggressive();
  // Combines immutable tasks with same name and priority
  ```

- **Parallel Scheduling**: Optimizes task execution order
  ```rust
  let compiler = WorkflowCompiler::new()
      .add_pass(OptimizationPass::ParallelScheduling);
  // Sorts group tasks by priority (highest first)
  ```

- **Resource Optimization**: Improves resource utilization
  ```rust
  let compiler = WorkflowCompiler::new()
      .add_pass(OptimizationPass::ResourceOptimization);
  // Groups tasks by queue for better locality
  ```

**Example: Combined Optimizations**

```rust
use celers_canvas::{Group, WorkflowCompiler, OptimizationPass};

let group = Group::new()
    .add_signature(Signature::new("task1".to_string()).with_priority(1).with_args(vec![json!(1)]))
    .add_signature(Signature::new("".to_string())) // Dead code
    .add_signature(Signature::new("task2".to_string()).with_priority(9).with_args(vec![json!(2)]))
    .add_signature(Signature::new("task1".to_string()).with_priority(1).with_args(vec![json!(1)])); // Duplicate

let compiler = WorkflowCompiler::new()
    .aggressive()
    .add_pass(OptimizationPass::ParallelScheduling);

let optimized = compiler.optimize_group(&group);
// Result: 2 tasks (dead code removed, duplicate removed, sorted by priority)
// Task order: task2 (priority 9), task1 (priority 1)
```

**Performance Benefits:**
- Reduced task count (faster workflow setup)
- Better queue locality (improved throughput)
- Optimized scheduling (higher priority tasks first)
- See `examples/workflow_optimization.rs` for detailed examples

## Chord Barrier Synchronization

The Chord primitive uses Redis atomic operations for barrier synchronization:

### Implementation

```rust
// 1. Initialize chord state
chord_init(ChordState {
    chord_id,
    total: 3,           // Number of header tasks
    completed: 0,       // Counter (Redis INCR)
    callback: Some("aggregate_results"),
    task_ids: vec![],
});

// 2. Each worker completion increments counter
let count = chord_complete_task(chord_id).await?;  // Atomic INCR

// 3. When count == total, enqueue callback
if count >= state.total {
    broker.enqueue(callback_task).await?;
}
```

### Thread Safety

- **Atomic Counter**: Redis INCR operation (atomic)
- **Race Condition Free**: Multiple workers can complete simultaneously
- **Exactly Once**: Callback enqueued exactly once
- **No Lost Updates**: Atomic operations prevent race conditions

## Workflow Visualization

Export any workflow as a Mermaid flowchart or GraphViz DOT graph for inspection or documentation.

`DagExport` covers the base primitives (`Chain`, `Group`, `Chord`) and additionally offers
`to_json()` / `to_svg()` (the latter shells out to a local `dot` binary):

```rust
use celers_canvas::{Chain, DagExport};

let chain = Chain::new()
    .then("download_data", vec![])
    .then("process_data", vec![])
    .then("upload_result", vec![]);

println!("{}", chain.to_mermaid());
println!("{}", chain.to_dot());
```

`DagVisualize` (`to_mermaid()` / `to_dot()`) covers `CanvasElement`, `NestedChain`, `NestedGroup`,
and the advanced primitives (`Map`, `Starmap`, `Chunks`, `Branch`, `Switch`), with deterministic
node ids and labelled fan-out/fan-in/chord/decision edges — including chords nested inside a
chain or group:

```rust
use celers_canvas::{CanvasElement, DagVisualize, NestedChain};

let workflow = NestedChain::new()
    .then("step1", vec![])
    .then("step2", vec![]);

println!("{}", workflow.to_mermaid());
```

## Feature Flags

```toml
[dependencies]
celers-canvas = { version = "0.3", features = ["backend-redis"] }
```

**Available features:**
- `backend-redis`: Enable Chord support with Redis backend (required for barrier synchronization)

**Without backend-redis:**
- Chain, Group, Map, Starmap work normally
- Chord falls back to Group (no callback coordination)

## Error Handling

```rust
use celers_canvas::{Chain, CanvasError};

match Chain::new().then("task", vec![]).apply(&broker).await {
    Ok(task_id) => println!("Started: {}", task_id),
    Err(CanvasError::Invalid(msg)) => eprintln!("Invalid workflow: {}", msg),
    Err(CanvasError::Broker(msg)) => eprintln!("Broker error: {}", msg),
    Err(CanvasError::Serialization(msg)) => eprintln!("Serialization error: {}", msg),
}
```

**Error Types:**
- `Invalid`: Empty workflow, missing callback, etc.
- `Broker`: Enqueue failures, connection errors
- `Serialization`: JSON encoding errors

## Comparison with Celery

| Feature | CeleRS Canvas | Celery Canvas |
|---------|---------------|---------------|
| Chain | ✅ | ✅ |
| Group | ✅ | ✅ |
| Chord | ✅ | ✅ |
| Map | ✅ | ✅ |
| Starmap | ✅ | ✅ |
| Immutability | ✅ | ✅ |
| Priority | ✅ | ✅ |
| Nested Workflows | ✅ `NestedChain`/`NestedGroup` | ✅ Automatic |
| Result Backend Required | Chord only | All (optional) |

**Compatibility:**
- API design matches Celery Canvas patterns
- Task format compatible with Celery workers
- Can interoperate with Python Celery deployments

## Performance Characteristics

| Workflow | Time Complexity | Space Complexity | Notes |
|----------|----------------|------------------|-------|
| Chain | O(n) sequential | O(1) per task | Executes serially |
| Group | O(1) enqueue | O(n) tasks | Parallel execution |
| Chord | O(1) + callback | O(n) + state | Atomic counter overhead |
| Map | O(1) enqueue | O(n) tasks | Same as Group |
| Starmap | O(1) enqueue | O(n) tasks | Same as Group |

**Throughput:**
- Chain: Limited by sequential execution
- Group/Map/Starmap: Limited by broker throughput (50K+ tasks/sec with batch)
- Chord: Same as Group + Redis INCR overhead (<1ms)

## Examples

### Data Pipeline

```rust
// ETL workflow
let pipeline = Chain::new()
    .then("extract_data", vec![serde_json::json!("source.db")])
    .then("transform_data", vec![])
    .then("load_data", vec![serde_json::json!("dest.db")]);

pipeline.apply(&broker).await?;
```

### Image Processing

```rust
// Batch resize images
let task = Signature::new("resize_image".to_string());
let images = vec![
    vec![serde_json::json!("img1.jpg"), serde_json::json!("800x600")],
    vec![serde_json::json!("img2.jpg"), serde_json::json!("800x600")],
    vec![serde_json::json!("img3.jpg"), serde_json::json!("800x600")],
];

let workflow = Starmap::new(task, images);
workflow.apply(&broker).await?;
```

### Map-Reduce Analytics

```rust
// Process log files in parallel, then aggregate
let header = Group::new()
    .add("analyze_log", vec![serde_json::json!("log1.txt")])
    .add("analyze_log", vec![serde_json::json!("log2.txt")])
    .add("analyze_log", vec![serde_json::json!("log3.txt")]);

let callback = Signature::new("summarize_stats".to_string());
let workflow = Chord::new(header, callback);

let mut backend = RedisResultBackend::new("redis://localhost:6379")?;
workflow.apply(&broker, &mut backend).await?;
```

### Priority Processing

```rust
// High-priority urgent tasks
let urgent = Group::new()
    .add_signature(
        Signature::new("process_alert".to_string())
            .with_args(vec![serde_json::json!("alert1")])
            .with_priority(9)
    )
    .add_signature(
        Signature::new("process_alert".to_string())
            .with_args(vec![serde_json::json!("alert2")])
            .with_priority(9)
    );

urgent.apply(&broker).await?;
```

## Requirements

- **Rust**: 1.70+ (async/await, trait bounds)
- **Broker**: Any CeleRS broker (Redis, PostgreSQL, etc.)
- **Backend**: Redis backend required for Chord (feature: `backend-redis`)
- **Serialization**: serde_json for argument encoding

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Canvas Workflow                          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ├─ Chain ──> Sequential execution
                            ├─ Group ──> Parallel execution
                            ├─ Chord ──> Map-reduce (Group + callback)
                            ├─ Map ───> Batch same task
                            └─ Starmap > Batch with unpacking
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Broker (Queue)                          │
│                   (Redis, PostgreSQL)                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     Workers (Executors)                      │
│              (Process tasks + update state)                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Result Backend (Chord coordination)             │
│                    (Redis atomic INCR)                       │
└─────────────────────────────────────────────────────────────┘
```

## Testing

**388 tests passing** (`cargo nextest run --all-features`) plus **85 doc tests passing** — 2 further
unit tests and 3 doc tests are `ignore`d

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_chain_workflow() {
        let broker = MockBroker::new();
        let chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![]);

        let _task_id = chain.apply(&broker).await.unwrap();
        assert_eq!(broker.task_count(), 1); // first chain step enqueued
    }
}
```

## See Also

- **Examples**: `examples/basic_workflows.rs`, `examples/advanced_patterns.rs`, `examples/integration_example.rs` - Workflow examples
- **Core**: `celers-core` - Task registry and execution
- **Worker**: `celers-worker` - Worker runtime with workflow support
- **Backend**: `celers-backend-redis` - Result backend for Chord

## License

Apache-2.0
