# celers-macros TODO

> Procedural macros for simplified CeleRS task definition

## Status: ✅ FEATURE COMPLETE + OPTIMIZED

Provides ergonomic macros for defining tasks without boilerplate.

## Recent Enhancements (2025)

### January 6, 2026: Code Quality Enhancement Session (COMPLETED)
- ✅ Fixed clippy::ptr_arg warning in integration tests
  - Changed `validate_username_format` parameter from `&String` to `&str`
  - More idiomatic Rust code that accepts both `&String` and `&str` arguments
  - Follows Rust best practices for string parameter types
- ✅ Updated documentation for custom validators
  - Added note that `&str` is more idiomatic than `&String` for String field validators
  - Updated example in lib.rs to use `&str` instead of `&String`
  - Clarified that both `&String` and `&str` work for String fields
- ✅ Updated README.md with accurate test counts
  - Corrected test count from 243 to 244 (21 unit + 200 integration + 23 doc tests)
  - Ensured all documentation is consistent and up-to-date
- ✅ Comprehensive verification completed
  - All 244 tests passing
  - Zero compiler warnings, zero clippy warnings (strict mode: -D warnings)
  - Release build succeeds with no warnings
  - All 3 examples running successfully (basic_task, derive_task, validation)
  - Documentation builds successfully with no warnings
- ✅ Code quality metrics
  - 2,689 lines in lib.rs (production code)
  - 3,067 lines in integration_test.rs (comprehensive test coverage)
  - 197 test functions covering all features and edge cases

### January 5, 2026: Custom Validator Functions (COMPLETED)
- ✅ Added custom validator function support
  - `custom = "function_name"` parameter for calling user-defined validation functions
  - Function signature: `fn(&T) -> Result<(), String>` where T is the field type
  - Integrates seamlessly with existing predefined validators
  - Custom validators run after predefined validators (min, max, pattern, etc.)
- ✅ Added 10 new integration tests for custom validators
  - Custom validator success/failure tests
  - Username validation with multiple failure scenarios
  - Combined custom and predefined validators
  - Even number validation test
  - Percentage validation with dynamic error messages
  - Total: 200 integration tests (up from 190, +10 new tests)
- ✅ Enhanced documentation
  - Added "Custom Validator Functions" section to module docs
  - Comprehensive example showing custom validator definition and usage
  - Updated README.md with custom validator feature
  - Documented function signature requirements
  - Noted that custom validators run after predefined validators
- ✅ All tests passing: 21 unit + 200 integration + 23 doc tests (244 total)
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work
- ✅ Code quality: Clean implementation with proper error propagation

## Recent Enhancements (2025)

### December 31, 2025: Financial and Specialized Validators (COMPLETED)
- ✅ Added 5 new financial and specialized validators
  - `iban` - Validates International Bank Account Number (IBAN) format
  - `bitcoin_address` - Validates Bitcoin addresses (P2PKH, P2SH, Bech32 formats)
  - `ethereum_address` - Validates Ethereum addresses (0x + 40 hex characters)
  - `isbn` - Validates ISBN-10 or ISBN-13 with checksum validation (Luhn algorithm)
  - `password_strength` - Validates strong passwords (8+ chars with uppercase, lowercase, digit, special char)
- ✅ Added 26 new integration tests for financial/specialized validators
  - IBAN tests (success, failure, too short) - 3 tests
  - Bitcoin address tests (P2PKH, P2SH, Bech32, failure) - 4 tests
  - Ethereum address tests (success, lowercase, no prefix, too short) - 4 tests
  - ISBN tests (ISBN-10, ISBN-13, with hyphens, invalid checksum, wrong length, with X) - 6 tests
  - Password strength tests (success, too short, no uppercase, no lowercase, no digit, no special) - 6 tests
  - Combined financial/crypto test (success, IBAN failure, password failure) - 3 tests
  - Total: 190 integration tests (up from 164, +26 new tests)
- ✅ Enhanced documentation
  - Added "Financial and Specialized Validators" section
  - Updated examples showing all 37 predefined validators
  - Updated dependency notes (IBAN, Bitcoin, Ethereum use regex; ISBN and password use inline validation)
  - Added Example 13 to validation.rs demonstrating all new validators
- ✅ Comprehensive validation example
  - Added Example 13 with 6 test cases (success + 5 failure scenarios)
  - Demonstrates IBAN, Bitcoin, Ethereum, ISBN, and password strength validation
  - Shows proper error messages for each validator
- ✅ All tests passing: 21 unit + 190 integration + 22 doc tests (233 total)
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work
- ✅ Code quality: 2,595 lines in lib.rs, 2,894 lines in integration tests, 803 lines in validation example
- ✅ Extended LazyLock optimization to new regex-based validators (IBAN, Bitcoin, Ethereum)

### December 30, 2025 - Part 3: Performance Optimization + Geographic/Locale Validators (COMPLETED)
- ✅ Major Performance Optimization: Regex Pattern Caching with LazyLock
  - Implemented `LazyLock` (Rust 1.80+) for all regex-based validators
  - Patterns are now compiled once and cached, eliminating repeated regex compilation overhead
  - Affects 18 validators: email, url, phone, uuid, ipv4, ipv6, slug, mac_address, base64, color_hex, semver, domain, time_24h, date_iso8601, iso_country, iso_language, us_zip, ca_postal
  - Significant performance improvement for validation-heavy workloads
- ✅ Added 6 new geographic and locale validators
  - `latitude` - Validates latitude coordinates (-90 to 90)
  - `longitude` - Validates longitude coordinates (-180 to 180)
  - `iso_country` - Validates ISO 3166-1 alpha-2 country codes (e.g., US, CA, GB)
  - `iso_language` - Validates ISO 639-1 language codes (e.g., en, es, fr)
  - `us_zip` - Validates US ZIP codes (12345 or 12345-6789)
  - `ca_postal` - Validates Canadian postal codes (A1A 1A1 or A1A1A1)
- ✅ Added 24 new integration tests for geographic/locale validators
  - Latitude tests (success, edge cases, failure) - 3 tests
  - Longitude tests (success, edge cases, failure) - 3 tests
  - ISO country tests (success with US/CA/GB, lowercase failure, 3-letter failure) - 2 tests
  - ISO language tests (success with en/es/fr, uppercase failure, 3-letter failure) - 2 tests
  - US ZIP tests (5-digit success, 9-digit success, failures) - 3 tests
  - Canadian postal tests (with space, without space, lowercase/digit failures) - 3 tests
  - Combined location data tests (success, lat failure, country failure) - 3 tests
  - Additional combined and edge case tests - 5 tests
  - Total: 164 integration tests (up from 145, +19 new tests)
- ✅ Enhanced documentation
  - Added "Geographic and Locale Validators" section
  - Updated examples showing all predefined validators (32 at this point)
  - Added performance note about LazyLock optimization
  - Updated dependency notes for new validators
- ✅ All tests passing: 21 unit + 164 integration + 22 doc tests (207 total, now 233 total after Dec 31 update)
- ✅ Zero compiler warnings, zero clippy warnings (fixed manual range check warnings)
- ✅ Backwards compatible - all existing validation features still work
- ✅ Code quality: 2,437 lines in lib.rs, 2,551 lines in integration tests
- ✅ Used `RangeInclusive::contains` for latitude/longitude validation (clippy-compliant)

### December 30, 2025 - Part 2: Additional Practical Validators + Code Quality (COMPLETED)
- ✅ Fixed clippy warning in integration tests (useless format! usage)
- ✅ Added 8 new practical predefined validators
  - `semver` - Validates semantic versioning format (e.g., 1.2.3, 2.0.0-alpha)
  - `domain` - Validates domain name format (e.g., example.com, subdomain.example.co.uk)
  - `ascii` - Ensures string contains only ASCII characters
  - `lowercase` - Ensures string contains only lowercase characters
  - `uppercase` - Ensures string contains only uppercase characters
  - `time_24h` - Validates 24-hour time format (HH:MM or HH:MM:SS)
  - `date_iso8601` - Validates ISO 8601 date format (YYYY-MM-DD)
  - `credit_card` - Validates credit card number using Luhn algorithm
- ✅ Added 27 new integration tests for additional validators
  - Semver validator tests (success, prerelease, failure) - 3 tests
  - Domain validator tests (success, subdomain, failure) - 3 tests
  - ASCII validator tests (success, failure, empty) - 3 tests
  - Lowercase validator tests (success, failure, with numbers) - 3 tests
  - Uppercase validator tests (success, failure, with numbers) - 3 tests
  - Time 24h validator tests (success, with seconds, failure) - 3 tests
  - Date ISO 8601 validator tests (success, invalid month, wrong format) - 3 tests
  - Credit card validator tests (success, with spaces, failure) - 3 tests
  - Combined test with all 8 new validators (success, version failure, card failure) - 3 tests
  - Total: 145 integration tests (up from 118)
- ✅ Enhanced documentation with new validators
  - Added 8 new validators to predefined validation shortcuts section
  - Updated examples showing all 26 predefined validators
  - Updated dependency notes (regex for most, credit_card uses Luhn algorithm)
- ✅ Added Example 12 to validation.rs
  - Demonstrates all 8 new validators with custom messages
  - Shows success and failure cases for each validator
  - Comprehensive test coverage in runnable example
  - Software release validation scenario with all validators
- ✅ All tests passing: 21 unit + 145 integration + 22 doc tests
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work

### December 29, 2025 - Part 2: Web Data Validators (COMPLETED)
- ✅ Added 3 new web-related predefined validators
  - `json` - Validates JSON string format using serde_json
  - `base64` - Validates base64-encoded strings with proper padding
  - `color_hex` - Validates hex color codes (#RGB or #RRGGBB format)
- ✅ Added 15 new integration tests for web validators
  - JSON validator tests (object success, array success, failure)
  - Base64 validator tests (success, with/without padding, invalid chars, length check)
  - Color hex tests (6-digit, 3-digit, no hash failure, wrong length failure)
  - Combined web data tests with custom messages (3 validators together)
  - Total: 118 integration tests (up from 103)
- ✅ Enhanced documentation with web validators
  - Added 3 new validators to predefined validation shortcuts section
  - Updated examples showing all 18 predefined validators
  - Updated dependency notes (regex for most, serde_json for JSON)
- ✅ Added Example 11 to validation.rs
  - Demonstrates all 3 new web data validators with custom messages
  - Shows success and failure cases for JSON, base64, and color_hex
  - Comprehensive test coverage in runnable example
- ✅ All tests passing: 21 unit + 118 integration + 22 doc tests
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work

### December 29, 2025 - Part 1: Network Validators (COMPLETED)
- ✅ Added 3 new network-related predefined validators
  - `ipv6` - Validates IPv6 address format (e.g., 2001:0db8:85a3:0000:0000:8a2e:0370:7334)
  - `slug` - Validates URL-friendly slugs (lowercase letters, numbers, hyphens)
  - `mac_address` - Validates MAC address format (e.g., 00:1A:2B:3C:4D:5E)
- ✅ Added 15 new integration tests for network validators
  - IPv6 validator tests (full format, compressed format, failure)
  - Slug validator tests (success with numbers, uppercase failure, underscore failure)
  - MAC address tests (colon format, hyphen format, invalid format)
  - Combined network config tests with custom messages (3 validators together)
  - Total: 103 integration tests (up from 88)
- ✅ Enhanced documentation with network validators
  - Added 3 new validators to predefined validation shortcuts section
  - Updated examples showing all 15 predefined validators
  - Updated regex requirement note to include new validators
- ✅ Added Example 10 to validation.rs
  - Demonstrates all 3 new network validators with custom messages
  - Shows success and failure cases for IPv6, slug, and MAC address
  - Comprehensive test coverage in runnable example
- ✅ All tests passing: 21 unit + 103 integration + 22 doc tests
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work

### December 28, 2025: Extended Predefined Validators (COMPLETED)
- ✅ Added 4 new predefined validation shortcuts for common data formats
  - `numeric` - String contains only numeric characters (0-9)
  - `hexadecimal` - String contains only hexadecimal characters (0-9, a-f, A-F)
  - `uuid` - Validates UUID format (8-4-4-4-12 hex digits with hyphens)
  - `ipv4` - Validates IPv4 address format (e.g., 192.168.1.1)
- ✅ Added 17 new integration tests for extended validators
  - Numeric validator tests (success/failure)
  - UUID validator tests (success/failure/wrong format)
  - IPv4 validator tests (success/failure/out of range/edge cases)
  - Hexadecimal validator tests (success with lowercase/uppercase/mixed case, failure)
  - Combined new validators test with custom messages
  - Total: 88 integration tests (up from 71)
- ✅ Enhanced documentation with new validators
  - Added 4 new validators to predefined validation shortcuts section
  - Updated examples showing all 12 predefined validators
  - Updated regex requirement note to include new validators
- ✅ Added Example 9 to validation.rs
  - Demonstrates all 4 new validators with custom messages
  - Shows success and failure cases for each validator
  - Comprehensive test coverage in runnable example
- ✅ All tests passing: 21 unit + 88 integration + 22 doc tests
- ✅ Zero compiler warnings, zero clippy warnings
- ✅ Backwards compatible - all existing validation features still work

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
- [x] Integration tests ✅ (118 tests - EXPANDED 2025-12-29 Part 2)
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
  - [x] Predefined numeric validator test ✅ (NEW - 2025-12-28)
  - [x] Predefined uuid validator test ✅ (NEW - 2025-12-28)
  - [x] Predefined ipv4 validator test ✅ (NEW - 2025-12-28)
  - [x] Predefined hexadecimal validator test ✅ (NEW - 2025-12-28)
  - [x] Combined new validators test ✅ (NEW - 2025-12-28)
  - [x] IPv6 validator tests (full format, compressed, failure) ✅ (NEW - 2025-12-29 Part 1)
  - [x] Slug validator tests (success, numbers, uppercase failure, underscore failure) ✅ (NEW - 2025-12-29 Part 1)
  - [x] MAC address validator tests (colon, hyphen, invalid format) ✅ (NEW - 2025-12-29 Part 1)
  - [x] Network config combined test (ipv6+mac+slug with custom messages) ✅ (NEW - 2025-12-29 Part 1)
  - [x] JSON validator tests (object, array, failure) ✅ (NEW - 2025-12-29 Part 2)
  - [x] Base64 validator tests (success, padding, invalid chars, length) ✅ (NEW - 2025-12-29 Part 2)
  - [x] Color hex tests (6-digit, 3-digit, no hash, wrong length) ✅ (NEW - 2025-12-29 Part 2)
  - [x] Web data combined test (json+base64+color_hex with custom messages) ✅ (NEW - 2025-12-29 Part 2)
- [x] Examples ✅ (NEW - 2025-12-05, EXPANDED - 2025-12-14)
  - [x] basic_task.rs - Runnable example for #[task] macro
  - [x] derive_task.rs - Runnable example for #[derive(Task)]
  - [x] validation.rs - Comprehensive validation examples ✅ (NEW - 2025-12-14, ENHANCED 2025-12-29 Part 2)
    - [x] Numeric range validation (min, max)
    - [x] String length validation (min_length, max_length)
    - [x] Pattern validation (email, phone, URL, SKU)
    - [x] Combined validation (multiple rules on same field)
    - [x] Multiple field validation
    - [x] Custom error messages for better UX ✅ (NEW - 2025-12-14)
    - [x] New predefined validators (numeric, uuid, ipv4, hexadecimal) ✅ (NEW - 2025-12-28)
    - [x] Network validators (ipv6, slug, mac_address) ✅ (NEW - 2025-12-29 Part 1)
    - [x] Web data validators (json, base64, color_hex) ✅ (NEW - 2025-12-29 Part 2)

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
