# celers-macros TODO

> Procedural macros for simplified CeleRS task definition

**Version: 0.2.0 | Status: [Stable] | Updated: 2026-03-27 | Tests: 221**

## Status: ✅ FEATURE COMPLETE

Provides ergonomic macros for defining tasks without boilerplate.

## Completed Features

### #[task] Attribute Macro
- Automatic Task trait implementation
- Input struct generation from function parameters
- Result type extraction from function signature
- Async function support
- Name inference from function name
- Serde derive for input structs
- Attribute-based configuration (timeout, priority, max_retries)
- Optional field support with automatic serde attributes
- Documentation generation for generated structs

### #[derive(Task)] Macro
- Parse custom attributes (input, output, name)
- Automatic CamelCase to snake_case name conversion
- Support for generic structs
- Expects execute_impl method implementation

### Parameter Validation
- `#[validate(min = N, max = N)]` for numeric validation
- `#[validate(min_length = N, max_length = N)]` for string/collection validation
- `#[validate(pattern = "regex")]` for pattern validation
- `#[validate(message = "...")]` for custom error messages
- `#[validate(custom = "fn_name")]` for custom validator functions
- Validation runs before task execution
- Clear error messages for validation failures

### Predefined Validators (37 shortcuts)
- **Basic**: email, url, phone, not_empty, positive, negative, alphabetic, alphanumeric
- **Numeric**: numeric, hexadecimal, uuid, ipv4, ipv6
- **Network**: slug, mac_address, domain
- **Format**: semver, time_24h, date_iso8601, json, base64, color_hex
- **Text**: ascii, lowercase, uppercase
- **Geographic**: latitude, longitude, iso_country, iso_language, us_zip, ca_postal
- **Financial**: credit_card, iban, bitcoin_address, ethereum_address, isbn, password_strength

### Code Generation
- Generate `{FunctionName}Task` struct
- Generate `{FunctionName}TaskInput` struct
- Implement Task::execute() method
- Implement Task::name() method
- Preserve function visibility
- Handle generic lifetimes
- LazyLock-based regex pattern caching for performance

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

- Custom serialization formats
- IDE autocomplete support (requires IDE-specific plugins)
- Full HRTB support with serde

## Testing

- 221 total tests passing
- 21 unit tests
- 200 integration tests (task_attr, task_macro, derive_macro, validation modules)
- 23 doc tests
- 3 runnable examples (basic_task, derive_task, validation)

## Dependencies

**Build Dependencies:**
- `proc-macro2`: Proc macro utilities
- `quote`: Code generation
- `syn`: Rust parsing

**User Dependencies (for pattern validation):**
- `regex`: Required when using `#[validate(pattern = "...")]` or regex-based predefined validators

## Known Limitations

- Generic parameters: Basic support (HRTBs with serde may require careful handling)
- Lifetime parameters require specific handling
- Complex return types may not parse correctly
- No support for `impl Trait` return types
- Pattern validation requires the `regex` crate in user's dependencies

## Notes

- Macros are optional (can use manual Task implementation)
- Generated code is clean and readable
- Preserves function visibility
- Supports async functions only
- Result type must wrap the actual return type
