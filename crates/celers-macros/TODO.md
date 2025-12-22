# celers-macros TODO

> Procedural macros for simplified CeleRS task definition

## Status: ✅ FEATURE COMPLETE

Provides ergonomic macros for defining tasks without boilerplate.

## Recent Enhancements (2025)

### December 20, 2025: Predefined Validation Shortcuts (COMPLETED)
- ✅ Implemented predefined validation shortcuts for common patterns
  - `email` - Email address validation (regex-based)
  - `url` - HTTP/HTTPS URL validation
  - `phone` - Phone number validation (10-15 digits, optional + prefix)
  - `not_empty` - String must not be empty
  - `positive` - Number must be greater than 0
  - `negative` - Number must be less than 0
  - `alphabetic` - String must contain only alphabetic characters
  - `alphanumeric` - String must contain only alphanumeric characters
- ✅ Added 21 new integration tests for predefined validators
  - Email, URL, phone shorthand tests (success/failure)
  - not_empty, positive, negative tests
  - alphabetic, alphanumeric tests
  - Combined predefined validators with custom messages
  - Total: 71 integration tests (up from 50)
- ✅ Enhanced documentation with predefined validator examples
  - Added "Predefined Validation Shortcuts" section
  - Comprehensive example showing all predefined validators
  - Usage examples in overview section
- ✅ All tests passing: 21 unit + 71 integration + 20 doc tests
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work

## Recent Enhancements (2025)

### December 14, 2025 - Part 2: Custom Validation Error Messages (COMPLETED)
- ✅ Implemented custom error message support for all validation types
  - Full support for `message` parameter in `#[validate(...)]` attribute
  - Works with min/max, min_length/max_length, and pattern validation
  - Provides clear, domain-specific error messages
  - Falls back to descriptive default messages if not specified
- ✅ Added comprehensive custom message tests (12 new tests)
  - Custom message for min validation
  - Custom message for max validation
  - Custom message for length validation
  - Custom message for pattern validation
  - Custom message for range (min+max) validation
  - All tests verify exact custom message content
- ✅ Updated validation.rs example with custom message demonstration
  - Example 8: Account creation with custom error messages
  - Shows all validation types with user-friendly messages
  - Demonstrates proper error messaging for better UX
- ✅ Enhanced documentation with custom message examples
  - Added Custom Error Messages section to parameter-level attributes
  - Full example showing custom messages for age, username, email validation
  - Clear examples of message parameter usage
- ✅ All tests passing: 21 unit + 50 integration + 20 doc tests
- ✅ Zero compiler warnings, zero clippy warnings

### December 14, 2025 - Part 1: Pattern Validation Feature (COMPLETED)
- ✅ Implemented regex pattern validation for string fields
  - Full support for `#[validate(pattern = "regex")]` attribute
  - Email validation patterns
  - Phone number validation patterns
  - URL validation patterns
  - Custom regex pattern support
- ✅ Added comprehensive pattern validation tests (7 new tests)
  - Email pattern validation success/failure tests
  - Phone number pattern validation tests
  - Combined validation (length + pattern) tests
  - Multiple validation types on single field
- ✅ Created validation.rs example demonstrating all validation features
  - 7 comprehensive examples showing all validation types
  - Email, phone, URL, SKU pattern validation examples
  - Combined validation with multiple fields
  - Runs successfully with clear output
- ✅ Updated documentation with pattern validation
  - Added pattern validation section with examples
  - Updated limitations to remove "pattern validation not implemented"
  - Added note about regex crate dependency requirement
- ✅ All tests passing: 21 unit + 38 integration + 19 doc tests
- ✅ Zero compiler warnings, zero clippy warnings

### December 9, 2025 - Part 2: Parameter Validation Feature
- ✅ Implemented parameter validation attributes
  - #[validate(...)] attribute support on function parameters
  - Numeric validation (min, max) for integer types
  - String/collection length validation (min_length, max_length)
  - Pattern validation infrastructure (regex support deferred)
  - Validation happens before task execution
  - Clear, descriptive error messages
- ✅ Comprehensive validation tests (13 new tests)
  - Min validation success/failure tests
  - Max validation success/failure tests
  - Range validation (min + max) tests
  - String length validation tests
  - Multiple validated parameters test
- ✅ Updated documentation with validation examples
  - Full usage example in module docs
  - Parameter-level attributes section
  - Validation expansion example
  - Added to best practices
- ✅ All tests passing: 21 unit + 31 integration + 19 doc tests
- ✅ Zero compiler warnings, zero clippy warnings

### December 9, 2025 - Part 1: Documentation & Developer Experience
- ✅ Added comprehensive Macro Expansion Guide
  - cargo-expand usage instructions
  - 4 detailed expansion examples (basic, configured, optional params, generics)
  - Shows actual generated code with detailed explanations
  - Best practices guide for using the macros
  - Troubleshooting section with common errors and solutions
- ✅ Enhanced module documentation with 180+ lines of examples
- ✅ All 21 unit tests + 18 integration tests passing
- ✅ 16 doc tests (comprehensive examples in documentation)
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Both examples running successfully

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
- [x] Validation attributes ✅ (COMPLETED 2025-12-09, ENHANCED 2025-12-14)
  - [x] `#[validate(min = N, max = N)]` for numeric validation
  - [x] `#[validate(min_length = N, max_length = N)]` for string/collection validation
  - [x] `#[validate(pattern = "regex")]` for pattern validation ✅ (NEW 2025-12-14)
  - [x] `#[validate(message = "...")]` for custom error messages ✅ (NEW 2025-12-14)
  - [x] Validation runs before task execution
  - [x] Clear error messages for validation failures
  - [x] 32 comprehensive validation tests (13 base + 7 pattern + 12 custom message)
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
- [x] Validation code generation ✅ (COMPLETED 2025-12-09)
  - [x] Parse parameter-level validation attributes
  - [x] Generate validation checks in execute method
  - [x] Support for min, max, min_length, max_length
  - [x] Clear error messages with field names and values

### Developer Experience
- [ ] IDE autocomplete support (requires IDE-specific plugins)
- [x] Better compile error messages ✅ (enhanced error context and validation)
- [x] Macro expansion examples in docs ✅ (COMPLETED 2025-12-09)
  - [x] 4 comprehensive expansion examples showing generated code
  - [x] Basic task, configured task, optional params, and generics
  - [x] Before/after comparison for each pattern
- [x] cargo-expand integration guide ✅ (COMPLETED 2025-12-09)
  - [x] Installation instructions
  - [x] Usage examples for library and examples
  - [x] Integrated into module documentation

## Testing Status

- [x] Doc tests (19 tests, currently ignored - normal for proc macros) ✅ (UPDATED 2025-12-09 Part 2)
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
- [x] Integration tests ✅ (71 tests - EXPANDED 2025-12-20)
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
  - [x] Min validation test ✅ (NEW - 2025-12-09)
  - [x] Max validation test ✅ (NEW - 2025-12-09)
  - [x] Range validation test ✅ (NEW - 2025-12-09)
  - [x] String length validation test ✅ (NEW - 2025-12-09)
  - [x] Multiple validated parameters test ✅ (NEW - 2025-12-09)
  - [x] Email pattern validation test ✅ (NEW - 2025-12-14)
  - [x] Phone pattern validation test ✅ (NEW - 2025-12-14)
  - [x] Combined length + pattern validation test ✅ (NEW - 2025-12-14)
  - [x] Custom message min validation test ✅ (NEW - 2025-12-14)
  - [x] Custom message max validation test ✅ (NEW - 2025-12-14)
  - [x] Custom message length validation test ✅ (NEW - 2025-12-14)
  - [x] Custom message pattern validation test ✅ (NEW - 2025-12-14)
  - [x] Custom message range validation test ✅ (NEW - 2025-12-14)
  - [x] Predefined email validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined url validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined phone validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined not_empty validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined positive validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined negative validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined alphabetic validator test ✅ (NEW - 2025-12-20)
  - [x] Predefined alphanumeric validator test ✅ (NEW - 2025-12-20)
  - [x] Combined predefined validators test ✅ (NEW - 2025-12-20)
- [x] Examples ✅ (NEW - 2025-12-05, EXPANDED - 2025-12-14)
  - [x] basic_task.rs - Runnable example for #[task] macro
  - [x] derive_task.rs - Runnable example for #[derive(Task)]
  - [x] validation.rs - Comprehensive validation examples ✅ (NEW - 2025-12-14, ENHANCED 2025-12-14)
    - [x] Numeric range validation (min, max)
    - [x] String length validation (min_length, max_length)
    - [x] Pattern validation (email, phone, URL, SKU)
    - [x] Combined validation (multiple rules on same field)
    - [x] Multiple field validation
    - [x] Custom error messages for better UX ✅ (NEW - 2025-12-14)

## Documentation

- [x] Module-level documentation ✅ (ENHANCED - 2025-12-09)
  - [x] Comprehensive overview section
  - [x] Multiple code examples (basic, configuration, optional params, generics)
  - [x] Generated code structure explanation
  - [x] Attribute parameters reference
  - [x] Limitations section
  - [x] 180+ lines of detailed macro expansion examples
- [x] #[task] macro documentation with example
- [x] Detailed attribute parameter docs ✅
  - [x] name, timeout, priority, max_retries
- [x] Generated struct documentation ✅
- [x] Macro expansion guide ✅ (ENHANCED - 2025-12-09)
  - [x] Complete examples with before/after code
  - [x] Configuration usage patterns
  - [x] Optional parameter handling
  - [x] Generic task examples with full expansion
  - [x] cargo-expand installation and usage guide
  - [x] Best practices guide (5 key recommendations)
- [x] Common patterns and recipes ✅ (COMPLETED - 2025-12-09)
  - [x] Descriptive function naming conventions
  - [x] Optional parameter patterns
  - [x] Configuration best practices
  - [x] Focus on single responsibility
- [x] Troubleshooting guide ✅ (COMPLETED - 2025-12-09)
  - [x] Common error messages with solutions
  - [x] async function requirement
  - [x] Timeout validation
  - [x] Duplicate attributes
  - [x] Empty names
  - [x] Serde serialization issues

## Dependencies

**Build Dependencies:**
- `proc-macro2`: Proc macro utilities
- `quote`: Code generation
- `syn`: Rust parsing

**Dev Dependencies (for testing):**
- `regex`: Pattern matching for validation tests
- `async-trait`: Async trait support for tests
- `serde` & `serde_json`: Serialization for tests
- `tokio`: Async runtime for tests

**User Dependencies (for pattern validation):**
- `regex`: Required when using `#[validate(pattern = "...")]`

## Known Limitations

- Generic parameters: Basic support added ✅ (2025-12-05)
  - Generic parameters are now parsed and propagated to generated structs
  - Simple generics work correctly
  - HRTBs (Higher-Ranked Trait Bounds) with serde may require careful handling
- Lifetime parameters require specific handling
- Complex return types may not parse correctly
- No support for `impl Trait` return types
- Pattern validation requires the `regex` crate in user's dependencies ✅ (IMPLEMENTED 2025-12-14)

## Notes

- Macros are optional (can use manual Task implementation)
- Generated code is clean and readable
- Preserves function visibility
- Supports async functions only
- Result type must wrap the actual return type
