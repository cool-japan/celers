# celers-canvas TODO

> Canvas workflow primitives for distributed task orchestration

## Status: ✅ FEATURE COMPLETE

All Canvas workflow primitives implemented and production-ready.

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
  - [x] `is_retryable()` - Retry decision logic
  - [x] `category()` - Error categorization for logging/metrics

### Integration ✅
- [x] Broker integration (enqueue workflows)
- [x] Backend integration (chord state management)
- [x] Worker integration (via celers-worker)

## Future Enhancements

### Advanced Workflows
- [ ] Nested workflows (Chain of Groups, etc.)
  - [ ] Deep nesting support (unlimited depth)
  - [ ] Workflow composition patterns
  - [ ] Sub-workflow isolation
- [ ] Workflow cancellation
  - [ ] Cancel entire workflow tree
  - [ ] Cancel individual branches
  - [ ] Cancellation propagation
- [ ] Workflow retry policies
  - [ ] Retry entire workflow
  - [ ] Retry failed branches only
  - [ ] Exponential backoff for workflows
- [ ] Workflow timeout support
  - [ ] Global workflow timeout
  - [ ] Per-stage timeout
  - [ ] Timeout escalation
- [ ] Conditional workflows (if/else)
  - [ ] Conditional branches based on results
  - [ ] Switch/case patterns
  - [ ] Dynamic path selection
- [ ] Workflow loops and iteration
  - [ ] For-each loops over collections
  - [ ] While loops with conditions
  - [ ] Break and continue support
- [ ] Workflow templates and macros
  - [ ] Reusable workflow patterns
  - [ ] Template parameterization

### State Management
- [ ] Workflow progress tracking
  - [ ] Real-time progress updates
  - [ ] Progress percentage calculation
  - [ ] Stage-level progress
- [ ] Partial result retrieval
  - [ ] Get intermediate results
  - [ ] Stream results as they complete
  - [ ] Result aggregation strategies
- [ ] Workflow persistence
  - [ ] Serialize workflow state
  - [ ] Checkpoint/snapshot support
  - [ ] State versioning
- [ ] Workflow recovery after crashes
  - [ ] Resume from last checkpoint
  - [ ] Replay failed stages
  - [ ] Automatic recovery policies

### Optimizations
- [ ] Workflow compilation/optimization
  - [ ] Static analysis and optimization
  - [ ] Common subexpression elimination
  - [ ] Dead code elimination
- [ ] Parallel workflow scheduling
  - [ ] Intelligent task distribution
  - [ ] Load balancing across workers
  - [ ] Resource-aware scheduling
- [ ] Workflow batching
  - [ ] Batch similar workflows
  - [ ] Shared resource optimization

### Monitoring
- [ ] Workflow metrics
  - [ ] Execution time per stage
  - [ ] Success/failure rates
  - [ ] Resource utilization
- [ ] Workflow visualization
  - [ ] Real-time workflow graphs
  - [ ] Interactive DAG viewer
  - [ ] Execution animation
- [ ] Workflow DAG export
  - [ ] GraphViz format (.dot)
  - [ ] Mermaid format (.mmd)
  - [ ] JSON representation
  - [ ] PNG/SVG rendering

### Data Flow & Passing
- [ ] Advanced result passing
  - [ ] Named result outputs
  - [ ] Result transformation pipelines
  - [ ] Type-safe result passing
- [ ] Data dependencies
  - [ ] Explicit dependency declaration
  - [ ] Automatic dependency resolution
  - [ ] Circular dependency detection
- [ ] Result caching
  - [ ] Memoization of expensive tasks
  - [ ] Cache invalidation strategies

### Error Handling
- [ ] Workflow-level error handlers
  - [ ] Catch and handle errors
  - [ ] Error recovery strategies
  - [ ] Fallback workflows
- [ ] Compensation workflows
  - [ ] Saga pattern support
  - [ ] Undo/rollback operations
  - [ ] Transaction-like semantics
- [ ] Error propagation control
  - [ ] Stop on first error
  - [ ] Continue on error
  - [ ] Partial failure handling

### Advanced Patterns
- [ ] Map-reduce improvements
  - [ ] Custom reduce functions
  - [ ] Parallel reduce
  - [ ] Streaming map-reduce
- [ ] Scatter-gather pattern
- [ ] Pipeline pattern
- [ ] Fan-out/fan-in pattern
- [ ] Event-driven workflows
- [ ] Reactive workflows

### Debugging & Testing
- [ ] Workflow dry-run mode
  - [ ] Simulate without execution
  - [ ] Validate workflow structure
- [ ] Workflow testing framework
  - [ ] Mock task implementations
  - [ ] Test data injection
- [ ] Time-travel debugging
  - [ ] Replay workflow from point
  - [ ] Step-by-step execution

## Testing

- [x] Unit tests for each primitive
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
