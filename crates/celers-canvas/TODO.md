# celers-canvas TODO

> Canvas workflow primitives for distributed task orchestration

## Status: ✅ ENHANCED - Production-Ready with Advanced Features

All Canvas workflow primitives implemented and production-ready.
Major enhancements added: cancellation, retry policies, timeouts, loops, state tracking, DAG export, error propagation control, sub-workflow isolation, workflow recovery/checkpointing, and workflow compilation/optimization framework.

## Completed Features

### Workflow Primitives ✅
- [x] Chain - Sequential execution with result passing
  - [x] `is_empty()`, `len()` - Chain size checks
  - [x] `Display` implementation
- [x] Group - Parallel task execution
  - [x] `is_empty()`, `len()`, `has_group_id()` - Group utility methods
  - [x] `Display` implementation
- [x] Chord - Map-reduce pattern (parallel + callback)
  - [x] `Display` implementation
- [x] Map - Apply task to multiple argument sets
  - [x] `is_empty()`, `len()` - Map size checks
  - [x] `Display` implementation
- [x] Starmap - Map with unpacked arguments
  - [x] `is_empty()`, `len()` - Starmap size checks
  - [x] `Display` implementation
- [x] Signature - Reusable task definitions
  - [x] `has_args()`, `has_kwargs()`, `is_immutable()` - Signature utility methods
  - [x] `Display` implementation

### Features ✅
- [x] Priority support for workflow tasks
- [x] Immutability flag for chain tasks
- [x] Task options (queue, priority, timeout)
  - [x] `has_priority()`, `has_queue()`, `has_task_id()`, `has_link()`, `has_link_error()` - TaskOptions utility methods
  - [x] `Display` implementation for TaskOptions
- [x] Group ID tracking
- [x] Chord barrier synchronization
- [x] Result backend integration
- [x] Fluent builder API for Signature
  - [x] `with_queue()` - Set target queue
  - [x] `with_task_id()` - Set custom task ID
  - [x] `with_link()` - Success callback
  - [x] `with_link_error()` - Failure callback
- [x] CanvasError utility methods
  - [x] `is_invalid()`, `is_broker()`, `is_serialization()` - Error type checks
  - [x] `is_cancelled()`, `is_timeout()` - New error type checks
  - [x] `is_retryable()` - Retry decision logic
  - [x] `category()` - Error categorization for logging/metrics
- [x] Workflow cancellation support (CancellationToken)
- [x] Workflow-level retry policies (WorkflowRetryPolicy)
- [x] Workflow-level timeout support (WorkflowTimeout, TimeoutEscalation)
- [x] Workflow loops (ForEach, WhileLoop)
- [x] Workflow state tracking (WorkflowState, WorkflowStatus)
- [x] DAG export (DagExport trait for Chain, Group, Chord)
- [x] Advanced result passing (NamedOutput, ResultTransform, AggregationStrategy)
- [x] Result caching (ResultCache, CachePolicy)
- [x] Workflow-level error handlers (WorkflowErrorHandler)
- [x] Compensation workflows and Saga pattern (CompensationWorkflow, Saga, SagaIsolation)
- [x] Advanced patterns (ScatterGather, Pipeline, FanOut, FanIn)
- [x] Workflow validation (WorkflowValidator trait, ValidationResult)
- [x] Loop control (LoopControl for break/continue)
- [x] Error propagation control (ErrorPropagationMode, PartialFailureTracker)
- [x] Sub-workflow isolation (IsolationLevel, SubWorkflowIsolation)
- [x] Workflow checkpointing and recovery (WorkflowCheckpoint, WorkflowRecoveryPolicy)
- [x] Workflow compilation/optimization framework (WorkflowCompiler, OptimizationPass)
- [x] Type-safe result passing (TypedResult, TypeValidator)
- [x] Data dependencies (TaskDependency, DependencyGraph with circular detection and topological sort)
- [x] Parallel reduce (ParallelReduce for map-reduce)
- [x] Workflow templates and parameterization (WorkflowTemplate, TemplateParameter)
- [x] Event-driven workflows (WorkflowEvent, EventHandler, EventDrivenWorkflow)

### Integration ✅
- [x] Broker integration (enqueue workflows)
- [x] Backend integration (chord state management)
- [x] Worker integration (via celers-worker)

## Future Enhancements

### Advanced Workflows
- [x] Nested workflows (Chain of Groups, etc.)
  - [x] Deep nesting support (unlimited depth)
  - [x] Workflow composition patterns (CanvasElement)
  - [x] Sub-workflow isolation (IsolationLevel, SubWorkflowIsolation)
- [x] Workflow cancellation
  - [x] Cancel entire workflow tree
  - [x] Cancel individual branches
  - [x] Cancellation propagation (CancellationToken)
- [x] Workflow retry policies
  - [x] Retry entire workflow
  - [x] Retry failed branches only
  - [x] Exponential backoff for workflows
- [x] Workflow timeout support
  - [x] Global workflow timeout
  - [x] Per-stage timeout
  - [x] Timeout escalation
- [x] Conditional workflows (if/else)
  - [x] Conditional branches based on results (Branch)
  - [x] Switch/case patterns (Switch)
  - [x] Dynamic path selection (Condition)
- [x] Workflow loops and iteration
  - [x] For-each loops over collections (ForEach)
  - [x] While loops with conditions (WhileLoop)
  - [x] Break and continue support (LoopControl)
- [x] Workflow templates and macros
  - [x] Reusable workflow patterns (WorkflowTemplate)
  - [x] Template parameterization (TemplateParameter)

### State Management
- [x] Workflow progress tracking
  - [x] Real-time progress updates (WorkflowState)
  - [x] Progress percentage calculation
  - [x] Stage-level progress
- [x] Partial result retrieval
  - [x] Get intermediate results
  - [x] Stream results as they complete
  - [x] Result aggregation strategies (AggregationStrategy - already implemented)
- [x] Workflow persistence
  - [x] Serialize workflow state (via Serde)
  - [x] Checkpoint/snapshot support (WorkflowCheckpoint)
  - [ ] State versioning
- [x] Workflow recovery after crashes
  - [x] Resume from last checkpoint (WorkflowCheckpoint)
  - [x] Replay failed stages (WorkflowRecoveryPolicy)
  - [x] Automatic recovery policies (WorkflowRecoveryPolicy)

### Optimizations
- [x] Workflow compilation/optimization
  - [x] Static analysis and optimization (WorkflowCompiler framework)
  - [x] Common subexpression elimination (OptimizationPass)
  - [x] Dead code elimination (OptimizationPass)
  - [x] Task fusion (OptimizationPass)
  - [x] Parallel scheduling optimization (OptimizationPass)
  - [x] Resource optimization (OptimizationPass)
- [ ] Parallel workflow scheduling (implementation)
  - [ ] Intelligent task distribution
  - [ ] Load balancing across workers
  - [ ] Resource-aware scheduling
- [ ] Workflow batching
  - [ ] Batch similar workflows
  - [ ] Shared resource optimization

### Monitoring
- [x] Workflow metrics
  - [x] Execution time per stage (WorkflowState timestamps)
  - [x] Success/failure rates (WorkflowState counters)
  - [ ] Resource utilization
- [ ] Workflow visualization
  - [ ] Real-time workflow graphs
  - [ ] Interactive DAG viewer
  - [ ] Execution animation
- [x] Workflow DAG export
  - [x] GraphViz format (.dot) - DagExport trait
  - [x] Mermaid format (.mmd) - DagExport trait
  - [x] JSON representation - DagExport trait
  - [ ] PNG/SVG rendering

### Data Flow & Passing
- [x] Advanced result passing
  - [x] Named result outputs (NamedOutput)
  - [x] Result transformation pipelines (ResultTransform)
  - [x] Aggregation strategies (AggregationStrategy)
  - [x] Type-safe result passing (TypedResult, TypeValidator)
- [x] Data dependencies
  - [x] Explicit dependency declaration (TaskDependency)
  - [x] Automatic dependency resolution (DependencyGraph::topological_sort)
  - [x] Circular dependency detection (DependencyGraph::has_circular_dependency)
- [x] Result caching
  - [x] Memoization of expensive tasks (ResultCache)
  - [x] Cache invalidation strategies (CachePolicy, TTL)

### Error Handling
- [x] Workflow-level error handlers
  - [x] Catch and handle errors (WorkflowErrorHandler)
  - [x] Error recovery strategies
  - [x] Fallback workflows (via ErrorStrategy)
- [x] Compensation workflows
  - [x] Saga pattern support (Saga, CompensationWorkflow)
  - [x] Undo/rollback operations
  - [x] Transaction-like semantics (SagaIsolation)
- [x] Error propagation control
  - [x] Stop on first error (ErrorPropagationMode::StopOnFirstError)
  - [x] Continue on error (ErrorPropagationMode::ContinueOnError)
  - [x] Partial failure handling (ErrorPropagationMode::PartialFailure, PartialFailureTracker)

### Advanced Patterns
- [x] Map-reduce improvements
  - [x] Custom reduce functions (AggregationStrategy)
  - [x] Parallel reduce (ParallelReduce)
  - [ ] Streaming map-reduce
- [x] Scatter-gather pattern (ScatterGather)
- [x] Pipeline pattern (Pipeline with buffering)
- [x] Fan-out/fan-in pattern (FanOut, FanIn)
- [x] Event-driven workflows (WorkflowEvent, EventHandler, EventDrivenWorkflow)
- [ ] Reactive workflows

### Debugging & Testing
- [x] Workflow dry-run mode
  - [x] Simulate without execution (WorkflowValidator trait)
  - [x] Validate workflow structure (ValidationResult)
- [ ] Workflow testing framework
  - [ ] Mock task implementations
  - [ ] Test data injection
- [ ] Time-travel debugging
  - [ ] Replay workflow from point
  - [ ] Step-by-step execution

## Testing

- [x] Unit tests for each primitive (68 comprehensive tests)
  - [x] Basic workflow primitives
  - [x] Cancellation, retry, timeout features
  - [x] Loop constructs
  - [x] State tracking
  - [x] DAG export
  - [x] Advanced result passing
  - [x] Result caching
  - [x] Error handlers and compensation
  - [x] Advanced patterns
  - [x] Workflow validation
  - [x] Error propagation control
  - [x] Sub-workflow isolation
  - [x] Workflow checkpointing and recovery
  - [x] Workflow compilation/optimization
  - [x] Type-safe result passing
  - [x] Data dependencies and circular detection
  - [x] Parallel reduce
  - [x] Workflow templates
  - [x] Event-driven workflows
- [ ] Integration tests with broker
- [ ] Integration tests with backend
- [ ] Chord barrier race condition tests
- [ ] Performance tests

## Documentation

- [x] Comprehensive README
- [x] Module-level documentation
- [x] Example code
- [ ] Workflow design patterns guide
- [ ] Migration from Celery Canvas

## Known Limitations

- Nested workflows require manual implementation
- Chord requires Redis backend (atomic INCR)
- No automatic workflow retry on partial failure
- No workflow checkpointing

## Dependencies

- `celers-core` - Task types
- `celers-backend-redis` - Chord state management (optional)
- `uuid` - Workflow IDs
- `serde` - Serialization

## Notes

- Chord implementation uses Redis atomic INCR for thread-safe counter
- All workflows are Celery-compatible
- Workflow IDs are UUIDs for global uniqueness
