# celers-macros TODO

> Procedural macros for simplified CeleRS task definition

## Status: ✅ FEATURE COMPLETE

Provides ergonomic macros for defining tasks without boilerplate.

## Completed Features

### #[task] Attribute Macro ✅
- [x] Automatic Task trait implementation
- [x] Input struct generation from function parameters
- [x] Result type extraction from function signature
- [x] Async function support
- [x] Name inference from function name
- [x] Serde derive for input structs

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
- [ ] Default values for parameters
- [ ] Validation attributes
- [ ] Custom serialization formats
- [ ] Timeout configuration via attributes
- [ ] Priority configuration via attributes

### Code Generation Improvements
- [ ] Better error messages
- [ ] Span preservation for errors
- [ ] Documentation generation
- [ ] Optional field support
- [ ] Generic parameter support

### Developer Experience
- [ ] IDE autocomplete support
- [ ] Better compile error messages
- [ ] Macro expansion examples in docs
- [ ] cargo-expand integration guide

## Testing Status

- [x] Doc tests (2 tests, currently ignored)
- [ ] Unit tests for macro expansion
- [ ] Integration tests with task registry
- [ ] Error case testing
- [ ] Edge case testing (lifetimes, generics)

## Documentation

- [x] Module-level documentation
- [x] #[task] macro documentation with example
- [ ] Detailed attribute parameter docs
- [ ] Macro expansion guide
- [ ] Common patterns and recipes
- [ ] Troubleshooting guide

## Dependencies

- `proc-macro2`: Proc macro utilities
- `quote`: Code generation
- `syn`: Rust parsing

## Known Limitations

- Generic parameters not yet supported
- Lifetime parameters require specific handling
- Complex return types may not parse correctly
- No support for `impl Trait` return types

## Notes

- Macros are optional (can use manual Task implementation)
- Generated code is clean and readable
- Preserves function visibility
- Supports async functions only
- Result type must wrap the actual return type
