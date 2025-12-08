# celers-macros TODO

> Procedural macros for simplified CeleRS task definition

## Status: ✅ FEATURE COMPLETE

Provides ergonomic macros for defining tasks without boilerplate.

## Recent Enhancements (2025)

### December 5, 2025 - Part 2: Derive Macro, Examples, Edge Cases
- ✅ Enhanced #[derive(Task)] macro (previously stub)
  - Parse custom attributes (input, output, name)
  - Automatic CamelCase to snake_case name conversion
  - Support for generic structs
  - Expects execute_impl method implementation
  - Unit tests for name conversion logic
- ✅ Added runnable examples directory
  - basic_task.rs: Demonstrates #[task] macro usage
  - derive_task.rs: Demonstrates #[derive(Task)] usage
  - Both examples run successfully with comprehensive output
- ✅ Expanded edge case test coverage (18 total integration tests now)
  - No parameters task
  - Many parameters (5+) task
  - Unit return type task
  - Nested types (Vec<Vec<T>>) task
  - Tuple return types
  - All new tests pass ✅
- ✅ Updated dependencies for examples
  - Added rt-multi-thread feature to tokio

### December 5, 2025 - Part 1: Integration Tests, Generics, Documentation
- ✅ Added comprehensive integration test suite (13 tests)
  - Tests cover basic execution, configuration, optionals, errors, serialization
  - All tests pass successfully
- ✅ Implemented generic parameter support
  - Generics are now parsed from function signatures
  - Generic parameters propagated to generated structs
  - Default implementation handles both generic and non-generic tasks
  - Basic generics work correctly (HRTBs need careful handling)
- ✅ Added validation infrastructure (foundation)
  - FieldValidation struct for range, length, and pattern validation
  - Code generation methods ready for validation logic
  - Full implementation deferred (requires parameter attribute parsing)
- ✅ Enhanced module documentation
  - Added comprehensive overview with multiple examples
  - Documented generated code structure
  - Added usage examples for basic, configured, optional, and generic tasks
  - Documented limitations and attribute parameters
- ✅ Updated test dependencies (async-trait, serde, serde_json, tokio)

### Earlier 2025 Enhancements

#### New Attribute Parameters
- ✅ `timeout`: Configure task timeout in seconds
- ✅ `priority`: Set task priority (higher = more important)
- ✅ `max_retries`: Specify maximum retry attempts

#### Code Generation Improvements
- ✅ Automatic documentation for generated structs
- ✅ Optional field support with smart serde attributes
- ✅ Configuration accessor methods on Task structs
- ✅ Default derive for Input structs with all optional fields
- ✅ Generic parameter support in generated code

#### Error Handling & Validation
- ✅ Enhanced error messages with detailed context
- ✅ Duplicate attribute detection
- ✅ Empty name validation
- ✅ Zero timeout validation
- ✅ Value range validation (u64, i32, u32)
- ✅ Async function validation with helpful error messages

#### Testing Infrastructure
- ✅ 21 comprehensive unit tests (added name conversion test)
- ✅ 18 integration tests (expanded with edge cases)
- ✅ Attribute parsing tests (14 tests)
- ✅ Type detection edge case tests (6 tests)
- ✅ Tests for duplicate attributes
- ✅ Tests for invalid values
- ✅ Complex generic type tests
- ✅ Edge case tests (no params, many params, unit return, nested types, tuples)

## Completed Features

### #[task] Attribute Macro ✅
- [x] Automatic Task trait implementation
- [x] Input struct generation from function parameters
- [x] Result type extraction from function signature
- [x] Async function support
- [x] Name inference from function name
- [x] Serde derive for input structs
- [x] Attribute-based configuration (timeout, priority, max_retries)
- [x] Optional field support with automatic serde attributes
- [x] Documentation generation for generated structs

### Code Generation ✅
- [x] Generate `{FunctionName}Task` struct
- [x] Generate `{FunctionName}TaskInput` struct
- [x] Implement Task::execute() method
- [x] Implement Task::name() method
- [x] Preserve function visibility
- [x] Handle generic lifetimes

### Type System ✅
- [x] Extract Result<T> inner type for Output
- [x] Support celers_core::Result
- [x] Handle complex return types
- [x] Proper error propagation

## Usage Example

```rust
use celers_macros::task;

#[task]
async fn process_data(id: u64, name: String) -> celers_core::Result<String> {
    // Task logic here
    Ok(format!("Processed {} - {}", id, name))
}

// Generated:
// - ProcessDataTask: impl Task
// - ProcessDataTaskInput { id: u64, name: String }
```

## Future Enhancements

### Macro Features
- [x] Custom task names via attribute parameters ✅
  - [x] `#[task(name = "custom.name")]` support
- [x] Timeout configuration via attributes ✅
  - [x] `#[task(timeout = 60)]` support
- [x] Priority configuration via attributes ✅
  - [x] `#[task(priority = 10)]` support
- [x] Max retries configuration via attributes ✅
  - [x] `#[task(max_retries = 3)]` support
- [x] Default values for parameters ✅
  - [x] Automatic Default derive for all-optional Input structs
  - [x] Serde default support for optional fields
- [ ] Validation attributes (future)
- [ ] Custom serialization formats (future)

### Code Generation Improvements
- [x] Documentation generation ✅
- [x] Optional field support ✅
  - [x] Automatic serde attributes for Option<T> fields
  - [x] Default derive for all-optional Input structs
- [x] Better error messages ✅
- [x] Span preservation for errors ✅
- [x] Generic parameter support ✅ (COMPLETED 2025-12-05)
  - [x] Parse generic parameters from function signatures
  - [x] Propagate generics to generated structs
  - [x] Handle where clauses
  - [x] Support both generic and non-generic tasks
  - [ ] Full HRTB support with serde (future enhancement)

### Developer Experience
- [ ] IDE autocomplete support
- [ ] Better compile error messages
- [ ] Macro expansion examples in docs
- [ ] cargo-expand integration guide

## Testing Status

- [x] Doc tests (8 tests, currently ignored - normal for proc macros)
- [x] Unit tests for macro expansion ✅ (21 tests total)
  - [x] is_option_type helper function tests (7 tests)
  - [x] TaskAttr parsing tests (13 tests total)
  - [x] Invalid input tests
  - [x] Duplicate attribute tests
  - [x] Value validation tests
  - [x] Name conversion test (CamelCase to snake_case)
- [x] Edge case testing ✅
  - [x] Nested Option types
  - [x] Complex generic types
  - [x] Various container types
- [x] Integration tests ✅ (18 tests - EXPANDED 2025-12-05)
  - [x] Basic task execution tests
  - [x] Custom configuration tests (name, timeout, priority, max_retries)
  - [x] Optional parameter tests
  - [x] Mixed type tests
  - [x] Error handling tests
  - [x] Serialization tests
  - [x] Default derive tests
  - [x] Complex return type tests
  - [x] Generic parameter syntax tests
  - [x] No parameters test ✅ (NEW)
  - [x] Many parameters test ✅ (NEW)
  - [x] Unit return type test ✅ (NEW)
  - [x] Nested types test ✅ (NEW)
  - [x] Tuple return types test ✅ (NEW)
- [x] Examples ✅ (NEW - 2025-12-05)
  - [x] basic_task.rs - Runnable example for #[task] macro
  - [x] derive_task.rs - Runnable example for #[derive(Task)]

## Documentation

- [x] Module-level documentation ✅ (ENHANCED - 2025-12-05)
  - [x] Comprehensive overview section
  - [x] Multiple code examples (basic, configuration, optional params, generics)
  - [x] Generated code structure explanation
  - [x] Attribute parameters reference
  - [x] Limitations section
- [x] #[task] macro documentation with example
- [x] Detailed attribute parameter docs ✅
  - [x] name, timeout, priority, max_retries
- [x] Generated struct documentation ✅
- [x] Macro expansion guide ✅
  - [x] Complete examples with before/after code
  - [x] Configuration usage patterns
  - [x] Optional parameter handling
  - [x] Error message examples
  - [x] Best practices guide
- [ ] Common patterns and recipes (in expansion guide)
- [ ] Troubleshooting guide (error messages cover this)

## Dependencies

- `proc-macro2`: Proc macro utilities
- `quote`: Code generation
- `syn`: Rust parsing

## Known Limitations

- Generic parameters: Basic support added ✅ (2025-12-05)
  - Generic parameters are now parsed and propagated to generated structs
  - Simple generics work correctly
  - HRTBs (Higher-Ranked Trait Bounds) with serde may require careful handling
- Lifetime parameters require specific handling
- Complex return types may not parse correctly
- No support for `impl Trait` return types
- Validation attributes infrastructure in place but not yet fully implemented

## Notes

- Macros are optional (can use manual Task implementation)
- Generated code is clean and readable
- Preserves function visibility
- Supports async functions only
- Result type must wrap the actual return type
