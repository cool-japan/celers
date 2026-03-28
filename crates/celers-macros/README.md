# celers-macros

**Version: 0.2.0 | Status: [Stable] | Updated: 2026-03-27**

Procedural macros for simplified CeleRS task definitions.

## Status: ✅ FEATURE COMPLETE + OPTIMIZED

This crate provides ergonomic procedural macros for defining CeleRS tasks without boilerplate:
- `#[task]` - Attribute macro for converting async functions into tasks
- `#[derive(Task)]` - Derive macro for implementing the Task trait

## Features

### Automatic Code Generation
- ✅ Task struct generation with `Task` trait implementation
- ✅ Input struct generation from function parameters with serde support
- ✅ Output type extraction from function return type
- ✅ Configuration methods (timeout, priority, max_retries)
- ✅ Optional parameter support with automatic serde attributes
- ✅ Generic parameter support

### Parameter Validation (37 Predefined Validators + Custom Functions)
- ✅ Numeric validation (min, max, positive, negative)
- ✅ String/collection length validation (min_length, max_length, not_empty)
- ✅ Pattern validation with custom regex
- ✅ **Custom validator functions** for complex validation logic
- ✅ Custom error messages for better UX
- ✅ **String Validators**: email, url, phone, alphabetic, alphanumeric, numeric, hexadecimal, uuid, slug, json, base64, semver, domain, ascii, lowercase, uppercase
- ✅ **Network Validators**: ipv4, ipv6, mac_address
- ✅ **Web Data Validators**: color_hex, time_24h, date_iso8601
- ✅ **Geographic/Locale Validators**: latitude, longitude, iso_country, iso_language, us_zip, ca_postal
- ✅ **Financial/Crypto Validators**: iban, bitcoin_address, ethereum_address, isbn, credit_card, password_strength
- ✅ **Performance Optimization**: LazyLock pattern caching for all regex-based validators

### Testing & Quality
- ✅ 221 total tests (21 unit + 200 integration + 23 doc tests; see TODO for full count)
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Comprehensive validation examples
- ✅ Runnable examples demonstrating all features

## Quick Start

### Basic Task

```rust
use celers_macros::task;
use celers_core::Result;

#[task]
async fn add_numbers(a: i32, b: i32) -> Result<i32> {
    Ok(a + b)
}

// Generated automatically:
// - AddNumbersTask: impl Task
// - AddNumbersTaskInput { a: i32, b: i32 }
// - AddNumbersTaskOutput = i32
```

### Task with Configuration

```rust
#[task(
    name = "tasks.process_data",
    timeout = 60,
    priority = 10,
    max_retries = 3
)]
async fn process_data(data: String) -> Result<String> {
    Ok(format!("Processed: {}", data))
}

// Access configuration:
let task = ProcessDataTask;
assert_eq!(task.name(), "tasks.process_data");
assert_eq!(task.timeout(), Some(60));
```

### Task with Validation

```rust
#[task]
async fn register_user(
    #[validate(min = 18, max = 120, message = "Age must be between 18 and 120")]
    age: i32,
    #[validate(email, message = "Invalid email address")]
    email: String,
    #[validate(password_strength)]
    password: String,
) -> Result<String> {
    Ok(format!("User registered with email {}", email))
}
```

### Predefined Validators

Use convenient shortcuts for common validation patterns:

```rust
#[task]
async fn create_profile(
    #[validate(email)] email: String,
    #[validate(url)] website: String,
    #[validate(phone)] phone: String,
    #[validate(positive)] age: i32,
    #[validate(alphanumeric)] username: String,
    #[validate(uuid)] user_id: String,
    #[validate(ipv4)] server_ip: String,
    #[validate(credit_card)] card: String,
    #[validate(bitcoin_address)] btc_wallet: String,
) -> Result<String> {
    Ok("Profile created".to_string())
}
```

### Optional Parameters

```rust
#[task]
async fn send_notification(
    user_id: u64,
    message: String,
    email: Option<String>,
    sms: Option<String>,
) -> Result<bool> {
    // Optional fields automatically get serde support
    // with skip_serializing_if and default
    Ok(true)
}
```

### Generic Tasks

```rust
#[task]
async fn process_items<T>(items: Vec<T>) -> Result<usize>
where
    T: Send + Clone,
{
    Ok(items.len())
}

// Use with specific type:
let task = ProcessItemsTask::<String>;
```

## Examples

See the [examples directory](./examples/) for comprehensive demonstrations:
- `basic_task.rs` - Basic #[task] macro usage
- `derive_task.rs` - #[derive(Task)] usage
- `validation.rs` - All 37 predefined validators with 13 examples

Run examples:
```bash
cargo run --example basic_task
cargo run --example validation
```

## Validation Reference

### Parameter-Level Attributes

Use `#[validate(...)]` on function parameters:

**Numeric Validation:**
- `min = N` - Minimum value (inclusive)
- `max = N` - Maximum value (inclusive)
- `positive` - Must be > 0
- `negative` - Must be < 0

**String/Collection Length:**
- `min_length = N` - Minimum length
- `max_length = N` - Maximum length
- `not_empty` - Must not be empty

**Pattern Validation:**
- `pattern = "regex"` - Custom regex pattern
- Or use any of 37 predefined validators (see above)

**Custom Validator Functions:**
- `custom = "function_name"` - Call a custom validation function

For complex validation logic, define a function with signature `fn(&T) -> Result<(), String>`:

```rust
fn validate_even(value: &i32) -> Result<(), String> {
    if value % 2 == 0 {
        Ok(())
    } else {
        Err("Value must be an even number".to_string())
    }
}

#[task]
async fn process(
    #[validate(custom = "validate_even")]
    count: i32,
) -> Result<String> {
    Ok(format!("Processing {} items", count))
}
```

**Custom Error Messages:**
- `message = "error text"` - Custom error message for validation failures

### Dependencies for Validation

Add to your `Cargo.toml`:

```toml
[dependencies]
regex = "1.11"        # For pattern validators
serde_json = "1.0"    # For json validator (usually already present)
```

## Documentation

For detailed documentation including macro expansion examples, troubleshooting, and best practices, see the [module documentation](./src/lib.rs) or run:

```bash
cargo doc --open
```

### Macro Expansion

To see generated code, use `cargo-expand`:

```bash
cargo install cargo-expand
cargo expand --lib
cargo expand --example basic_task
```

## Current Limitations

- Functions must be `async`
- Return type must be wrapped in `Result<T>`
- Generic parameters with complex trait bounds (HRTBs) may require careful handling
- No support for `impl Trait` return types

## Performance

- **LazyLock Pattern Caching**: All regex-based validators compile patterns once and cache them (Rust 1.80+)
- **Zero Runtime Overhead**: Validation happens at task execution time, not during macro expansion
- **Efficient Code Generation**: Generated code is clean, readable, and optimized

## Testing Status

- ✅ 21 unit tests (attribute parsing, type detection, name conversion)
- ✅ 200 integration tests (all features, validators, and custom validators)
- ✅ 23 doc tests
- ✅ Zero compiler warnings
- ✅ Zero clippy warnings
- ✅ All examples run successfully

## See Also

- [`celers-core`](../celers-core): Core Task trait definition
- [`celers`](../celers): Main CeleRS library

## License

See workspace license.
