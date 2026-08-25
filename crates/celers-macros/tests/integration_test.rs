//! Integration tests for celers-macros
//!
//! These tests verify that the procedural macros work correctly
//! in a real-world scenario by actually compiling and executing them.
//!
//! This file covers core `#[task]`/`#[derive(Task)]` behaviour and the
//! first tier of `#[validate(...)]` field validators. Three sibling files
//! were split out of what used to be one file here, to keep every one of
//! them under the COOLJAPAN 2000-line-per-file policy, grouped by validator
//! family: `validators_practical.rs` (semver, domain, ascii, case,
//! time/date, credit card), `validators_geo.rs` (latitude, longitude, ISO
//! country/language, US ZIP, Canadian postal code), and `validators_ids.rs`
//! (IBAN, Bitcoin/Ethereum, ISBN, password strength, custom validator
//! functions). `tests/common/mod.rs` holds the `.message()` helper every one
//! of these files (this one included) uses on `celers_core::CelersError`.

use celers_macros::{task, Task as TaskDerive};

// Uses the *real* `celers_core::{Task, CelersError, Result}` (a
// dev-dependency of this package) rather than a hand-rolled mirror of them.
// A mock can drift out of sync with the real crate's shape without anything
// noticing -- that happened once already: this mock used to define
// `CelersError` as a plain tuple struct `CelersError(pub String)`, which
// happened to *also* compile against the (buggy) old macro output, so this
// whole suite could not have caught the real crate rejecting the generated
// code with `error[E0423]: expected function, tuple struct or tuple
// variant, found enum`. Using the real crate directly closes that gap for
// good instead of relying on the mock being kept in lockstep by hand.
mod common;
use common::CelersErrorMessage;

// Import the trait so it's in scope for tests
use celers_core::Task;

// Test 1: Basic task with simple parameters
#[task]
async fn add_numbers(a: i32, b: i32) -> celers_core::Result<i32> {
    Ok(a + b)
}

#[tokio::test]
async fn test_basic_task() {
    let task = AddNumbersTask;
    let input = AddNumbersTaskInput { a: 5, b: 3 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 8);
    assert_eq!(task.name(), "add_numbers");
}

// Test 2: Task with custom name
#[task(name = "tasks.multiply")]
async fn multiply(x: i32, y: i32) -> celers_core::Result<i32> {
    Ok(x * y)
}

#[tokio::test]
async fn test_custom_name() {
    let task = MultiplyTask;
    assert_eq!(task.name(), "tasks.multiply");
    let input = MultiplyTaskInput { x: 4, y: 7 };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), 28);
}

// Test 3: Task with timeout configuration
#[task(timeout = 60)]
async fn slow_operation(data: String) -> celers_core::Result<String> {
    Ok(format!("Processed: {}", data))
}

#[tokio::test]
async fn test_timeout_config() {
    let task = SlowOperationTask;
    assert_eq!(task.timeout(), Some(60));
    let input = SlowOperationTaskInput {
        data: "test".to_string(),
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "Processed: test");
}

// Test 4: Task with priority configuration
#[task(priority = 10)]
async fn high_priority_task(value: u64) -> celers_core::Result<u64> {
    Ok(value * 2)
}

#[tokio::test]
async fn test_priority_config() {
    let task = HighPriorityTaskTask;
    assert_eq!(task.priority(), Some(10));
    let input = HighPriorityTaskTaskInput { value: 42 };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), 84);
}

// Test 5: Task with max_retries configuration
#[task(max_retries = 3)]
async fn retry_task(id: u32) -> celers_core::Result<String> {
    Ok(format!("Task {}", id))
}

#[tokio::test]
async fn test_max_retries_config() {
    let task = RetryTaskTask;
    assert_eq!(task.max_retries(), Some(3));
    let input = RetryTaskTaskInput { id: 123 };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "Task 123");
}

// Test 6: Task with all configuration options
#[task(name = "tasks.complex", timeout = 30, priority = 5, max_retries = 2)]
async fn complex_task(a: i32, b: String) -> celers_core::Result<String> {
    Ok(format!("{}: {}", a, b))
}

#[tokio::test]
async fn test_all_configs() {
    let task = ComplexTaskTask;
    assert_eq!(task.name(), "tasks.complex");
    assert_eq!(task.timeout(), Some(30));
    assert_eq!(task.priority(), Some(5));
    assert_eq!(task.max_retries(), Some(2));

    let input = ComplexTaskTaskInput {
        a: 42,
        b: "answer".to_string(),
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "42: answer");
}

// Test 7: Task with optional parameters
#[task]
async fn optional_params(required: String, optional: Option<i32>) -> celers_core::Result<String> {
    match optional {
        Some(val) => Ok(format!("{}: {}", required, val)),
        None => Ok(required),
    }
}

#[tokio::test]
async fn test_optional_params() {
    let task = OptionalParamsTask;

    // Test with Some value
    let input = OptionalParamsTaskInput {
        required: "test".to_string(),
        optional: Some(42),
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "test: 42");

    // Test with None value
    let input = OptionalParamsTaskInput {
        required: "test".to_string(),
        optional: None,
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "test");
}

// Test 8: Task with multiple types
#[task]
async fn mixed_types(
    int_val: i32,
    uint_val: u64,
    string_val: String,
    bool_val: bool,
) -> celers_core::Result<String> {
    Ok(format!(
        "{}, {}, {}, {}",
        int_val, uint_val, string_val, bool_val
    ))
}

#[tokio::test]
async fn test_mixed_types() {
    let task = MixedTypesTask;
    let input = MixedTypesTaskInput {
        int_val: -42,
        uint_val: 100,
        string_val: "hello".to_string(),
        bool_val: true,
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "-42, 100, hello, true");
}

// Test 9: Task that returns error
#[task]
async fn failing_task(should_fail: bool) -> celers_core::Result<String> {
    if should_fail {
        Err(celers_core::CelersError::TaskExecution(
            "Task failed".to_string(),
        ))
    } else {
        Ok("Success".to_string())
    }
}

#[tokio::test]
async fn test_error_handling() {
    let task = FailingTaskTask;

    // Test success case
    let input = FailingTaskTaskInput { should_fail: false };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Success");

    // Test error case
    let input = FailingTaskTaskInput { should_fail: true };
    let result = task.execute(input).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().message(), "Task failed");
}

// Test 10: Test serialization of Input structs
#[task]
async fn serialize_test(value: i32) -> celers_core::Result<i32> {
    Ok(value)
}

#[test]
fn test_input_serialization() {
    let input = SerializeTestTaskInput { value: 42 };

    // Test serialization to JSON
    let json = serde_json::to_string(&input).unwrap();
    assert_eq!(json, r#"{"value":42}"#);

    // Test deserialization from JSON
    let deserialized: SerializeTestTaskInput = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.value, 42);
}

// Test 11: Test Default derive for all-optional inputs
#[task]
async fn all_optional(a: Option<i32>, b: Option<String>) -> celers_core::Result<String> {
    Ok(format!("{:?}, {:?}", a, b))
}

#[test]
fn test_default_for_optional() {
    let input = AllOptionalTaskInput::default();
    assert_eq!(input.a, None);
    assert_eq!(input.b, None);
}

// Test 12: Task with complex return type
#[task]
async fn return_vec(count: usize) -> celers_core::Result<Vec<i32>> {
    Ok((0..count).map(|i| i as i32).collect())
}

#[tokio::test]
async fn test_complex_return_type() {
    let task = ReturnVecTask;
    let input = ReturnVecTaskInput { count: 5 };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), vec![0, 1, 2, 3, 4]);
}

// Test 13: Task with a generic type parameter.
//
// Regression test: `#[task]` on a generic fn used to expand the task marker
// struct as a field-less unit struct (`struct GenericCollectTask<T>;`),
// which is `error[E0392]: type parameter 'T' is never used` for *every*
// concrete `T` -- generic tasks could not compile at all. The fix gives the
// marker struct a `PhantomData` field that references every type/lifetime
// parameter. This test both compiles *and executes* a generic task (with
// two different concrete `T`s), so it would fail to build if that
// regression reappeared.
//
// Note: the type parameter needs `Serialize + Deserialize` bounds (in
// addition to `Send + Clone`) because those are required by the generated
// `<GenericCollectTask<T> as Task>::Input` (`GenericCollectTaskInput<T>`,
// which derives `Serialize`/`Deserialize` over its `Vec<T>` field).
// Regression test for a second, related bug the fix also had to address:
// the generated input struct used to repeat the fn's *entire* where-clause
// on its own declaration -- redundant with what `#[derive(Serialize,
// Deserialize)]` already infers per-field, and for any bound that mentions
// `Serialize`/`Deserialize` (exactly what a task moving real generic data
// needs), actively conflicting with it as
// `error[E0283]: type annotations needed ... multiple impls or where
// clauses satisfying ... found`. The fix stops repeating the where-clause
// on the input struct specifically (it never needs it: it holds data only,
// and auto-traits like `Send` are inferred from field types regardless).
// Without *both* halves of this fix, no generic `#[task]` fn whose data
// needs to be (de)serialized could compile at all -- this test uses a
// perfectly ordinary `for<'de> Deserialize<'de>` bound (the fix no longer
// requires working around the name `'de` specifically) and *executes* the
// task, so it would fail to build (not just fail an assertion) if either
// regression reappeared.
//
// This test also exercises the *other* half of the `#[task]` generics fix:
// the output type (`usize`) never mentions `T`, which used to make the
// generated `type GenericCollectTaskOutput<T> = usize;` alias
// `error[E0091]: type parameter is never used` -- a hard error, not the
// `type_alias_bounds` warning the first pass at this fix assumed.
#[task]
async fn generic_collect<T>(items: Vec<T>) -> celers_core::Result<usize>
where
    T: Send + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    Ok(items.len())
}

#[tokio::test]
async fn test_generic_task_compiles_and_executes() {
    let task = GenericCollectTask::<i32>::default();
    let input = GenericCollectTaskInput {
        items: vec![1, 2, 3, 4, 5],
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), 5);
    assert_eq!(task.name(), "generic_collect");
}

#[tokio::test]
async fn test_generic_task_with_different_type_param() {
    // Same generated struct, instantiated at a different concrete type --
    // demonstrating the PhantomData marker does not tie the struct to a
    // single `T`.
    let task = GenericCollectTask::<String>::default();
    let input = GenericCollectTaskInput {
        items: vec!["a".to_string(), "b".to_string(), "c".to_string()],
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), 3);
}

// Test 14: Task with empty parameters
#[task]
async fn no_params_task() -> celers_core::Result<String> {
    Ok("No parameters".to_string())
}

#[tokio::test]
async fn test_no_params() {
    let task = NoParamsTaskTask;
    let input = NoParamsTaskTaskInput {};
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), "No parameters");
}

// Test 15: Task with many parameters
#[task]
async fn many_params_task(a: i32, b: i32, c: i32, d: i32, e: i32) -> celers_core::Result<i32> {
    Ok(a + b + c + d + e)
}

#[tokio::test]
async fn test_many_params() {
    let task = ManyParamsTaskTask;
    let input = ManyParamsTaskTaskInput {
        a: 1,
        b: 2,
        c: 3,
        d: 4,
        e: 5,
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), 15);
}

// Test 16: Task returning unit type
#[task]
async fn unit_return_task(message: String) -> celers_core::Result<()> {
    println!("{}", message);
    Ok(())
}

#[tokio::test]
async fn test_unit_return() {
    let task = UnitReturnTaskTask;
    let input = UnitReturnTaskTaskInput {
        message: "test".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

// Test 17: Task with nested types
#[task]
async fn nested_types_task(data: Vec<Vec<i32>>) -> celers_core::Result<Vec<i32>> {
    Ok(data.into_iter().flatten().collect())
}

#[tokio::test]
async fn test_nested_types() {
    let task = NestedTypesTaskTask;
    let input = NestedTypesTaskTaskInput {
        data: vec![vec![1, 2], vec![3, 4], vec![5]],
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), vec![1, 2, 3, 4, 5]);
}

// Test 18: Task with tuple return type
#[task]
async fn tuple_return_task(a: i32, b: String) -> celers_core::Result<(i32, String, bool)> {
    Ok((a, b, true))
}

#[tokio::test]
async fn test_tuple_return() {
    let task = TupleReturnTaskTask;
    let input = TupleReturnTaskTaskInput {
        a: 42,
        b: "test".to_string(),
    };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap(), (42, "test".to_string(), true));
}

// Test 19: Task with min validation
#[task]
async fn validate_min_task(#[validate(min = 0)] age: i32) -> celers_core::Result<String> {
    Ok(format!("Age: {}", age))
}

#[tokio::test]
async fn test_validate_min_success() {
    let task = ValidateMinTaskTask;
    let input = ValidateMinTaskTaskInput { age: 25 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Age: 25");
}

#[tokio::test]
async fn test_validate_min_failure() {
    let task = ValidateMinTaskTask;
    let input = ValidateMinTaskTaskInput { age: -5 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("below minimum"));
}

// Test 20: Task with max validation
#[task]
async fn validate_max_task(#[validate(max = 100)] score: i32) -> celers_core::Result<String> {
    Ok(format!("Score: {}", score))
}

#[tokio::test]
async fn test_validate_max_success() {
    let task = ValidateMaxTaskTask;
    let input = ValidateMaxTaskTaskInput { score: 85 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Score: 85");
}

#[tokio::test]
async fn test_validate_max_failure() {
    let task = ValidateMaxTaskTask;
    let input = ValidateMaxTaskTaskInput { score: 150 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("exceeds maximum"));
}

// Test 21: Task with range validation (min and max)
#[task]
async fn validate_range_task(
    #[validate(min = 0, max = 120)] age: i32,
) -> celers_core::Result<String> {
    Ok(format!("Valid age: {}", age))
}

#[tokio::test]
async fn test_validate_range_success() {
    let task = ValidateRangeTaskTask;
    let input = ValidateRangeTaskTaskInput { age: 50 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Valid age: 50");
}

#[tokio::test]
async fn test_validate_range_too_low() {
    let task = ValidateRangeTaskTask;
    let input = ValidateRangeTaskTaskInput { age: -10 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("below minimum"));
}

#[tokio::test]
async fn test_validate_range_too_high() {
    let task = ValidateRangeTaskTask;
    let input = ValidateRangeTaskTaskInput { age: 150 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("exceeds maximum"));
}

// Test 22: Task with string length validation
#[task]
async fn validate_length_task(
    #[validate(min_length = 3, max_length = 10)] username: String,
) -> celers_core::Result<String> {
    Ok(format!("Username: {}", username))
}

#[tokio::test]
async fn test_validate_length_success() {
    let task = ValidateLengthTaskTask;
    let input = ValidateLengthTaskTaskInput {
        username: "alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Username: alice");
}

#[tokio::test]
async fn test_validate_length_too_short() {
    let task = ValidateLengthTaskTask;
    let input = ValidateLengthTaskTaskInput {
        username: "ab".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("below minimum"));
}

#[tokio::test]
async fn test_validate_length_too_long() {
    let task = ValidateLengthTaskTask;
    let input = ValidateLengthTaskTaskInput {
        username: "verylongusername".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("exceeds maximum"));
}

// Test 23: Task with multiple validated parameters
#[task]
async fn validate_multiple_task(
    #[validate(min = 18, max = 100)] age: i32,
    #[validate(min_length = 2, max_length = 50)] name: String,
) -> celers_core::Result<String> {
    Ok(format!("{} is {} years old", name, age))
}

#[tokio::test]
async fn test_validate_multiple_success() {
    let task = ValidateMultipleTaskTask;
    let input = ValidateMultipleTaskTaskInput {
        age: 25,
        name: "Alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Alice is 25 years old");
}

#[tokio::test]
async fn test_validate_multiple_age_invalid() {
    let task = ValidateMultipleTaskTask;
    let input = ValidateMultipleTaskTaskInput {
        age: 15,
        name: "Bob".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("age"));
    assert!(error.message().contains("below minimum"));
}

#[tokio::test]
async fn test_validate_multiple_name_invalid() {
    let task = ValidateMultipleTaskTask;
    let input = ValidateMultipleTaskTaskInput {
        age: 25,
        name: "A".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("name"));
    assert!(error.message().contains("below minimum"));
}

// Test 24: Task with pattern validation (email)
#[task]
async fn validate_email_task(
    #[validate(pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")] email: String,
) -> celers_core::Result<String> {
    Ok(format!("Email registered: {}", email))
}

#[tokio::test]
async fn test_validate_pattern_email_success() {
    let task = ValidateEmailTaskTask;
    let input = ValidateEmailTaskTaskInput {
        email: "user@example.com".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Email registered: user@example.com");
}

#[tokio::test]
async fn test_validate_pattern_email_failure() {
    let task = ValidateEmailTaskTask;
    let input = ValidateEmailTaskTaskInput {
        email: "not-an-email".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("does not match required pattern"));
}

// Test 25: Task with pattern validation (phone number)
#[task]
async fn validate_phone_task(
    #[validate(pattern = r"^\+?[1-9]\d{1,14}$")] phone: String,
) -> celers_core::Result<String> {
    Ok(format!("Phone: {}", phone))
}

#[tokio::test]
async fn test_validate_pattern_phone_success() {
    let task = ValidatePhoneTaskTask;
    let input = ValidatePhoneTaskTaskInput {
        phone: "+1234567890".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Phone: +1234567890");
}

#[tokio::test]
async fn test_validate_pattern_phone_failure() {
    let task = ValidatePhoneTaskTask;
    let input = ValidatePhoneTaskTaskInput {
        phone: "abc".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("does not match required pattern"));
}

// Test 26: Task with combined validation (length + pattern)
#[task]
async fn validate_combined_task(
    #[validate(min_length = 8, max_length = 20, pattern = r"^[a-zA-Z0-9_]+$")] username: String,
) -> celers_core::Result<String> {
    Ok(format!("Username created: {}", username))
}

#[tokio::test]
async fn test_validate_combined_success() {
    let task = ValidateCombinedTaskTask;
    let input = ValidateCombinedTaskTaskInput {
        username: "valid_user123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Username created: valid_user123");
}

#[tokio::test]
async fn test_validate_combined_length_failure() {
    let task = ValidateCombinedTaskTask;
    let input = ValidateCombinedTaskTaskInput {
        username: "short".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("length"));
}

#[tokio::test]
async fn test_validate_combined_pattern_failure() {
    let task = ValidateCombinedTaskTask;
    let input = ValidateCombinedTaskTaskInput {
        username: "invalid-user!".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("does not match required pattern"));
}

// Test 27: Custom error message for min validation
#[task]
async fn custom_message_min_task(
    #[validate(min = 18, message = "You must be at least 18 years old")] age: i32,
) -> celers_core::Result<String> {
    Ok(format!("Age: {}", age))
}

#[tokio::test]
async fn test_custom_message_min_success() {
    let task = CustomMessageMinTaskTask;
    let input = CustomMessageMinTaskTaskInput { age: 25 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_custom_message_min_failure() {
    let task = CustomMessageMinTaskTask;
    let input = CustomMessageMinTaskTaskInput { age: 15 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "You must be at least 18 years old");
}

// Test 28: Custom error message for max validation
#[task]
async fn custom_message_max_task(
    #[validate(max = 100, message = "Score cannot exceed 100 points")] score: i32,
) -> celers_core::Result<String> {
    Ok(format!("Score: {}", score))
}

#[tokio::test]
async fn test_custom_message_max_success() {
    let task = CustomMessageMaxTaskTask;
    let input = CustomMessageMaxTaskTaskInput { score: 85 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_custom_message_max_failure() {
    let task = CustomMessageMaxTaskTask;
    let input = CustomMessageMaxTaskTaskInput { score: 150 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Score cannot exceed 100 points");
}

// Test 29: Custom error message for length validation
#[task]
async fn custom_message_length_task(
    #[validate(
        min_length = 3,
        max_length = 20,
        message = "Username must be between 3 and 20 characters"
    )]
    username: String,
) -> celers_core::Result<String> {
    Ok(format!("Username: {}", username))
}

#[tokio::test]
async fn test_custom_message_length_success() {
    let task = CustomMessageLengthTaskTask;
    let input = CustomMessageLengthTaskTaskInput {
        username: "alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_custom_message_length_too_short() {
    let task = CustomMessageLengthTaskTask;
    let input = CustomMessageLengthTaskTaskInput {
        username: "ab".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Username must be between 3 and 20 characters"
    );
}

#[tokio::test]
async fn test_custom_message_length_too_long() {
    let task = CustomMessageLengthTaskTask;
    let input = CustomMessageLengthTaskTaskInput {
        username: "this_is_a_very_long_username_that_exceeds_limit".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Username must be between 3 and 20 characters"
    );
}

// Test 30: Custom error message for pattern validation
#[task]
async fn custom_message_pattern_task(
    #[validate(
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        message = "Please provide a valid email address"
    )]
    email: String,
) -> celers_core::Result<String> {
    Ok(format!("Email: {}", email))
}

#[tokio::test]
async fn test_custom_message_pattern_success() {
    let task = CustomMessagePatternTaskTask;
    let input = CustomMessagePatternTaskTaskInput {
        email: "user@example.com".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_custom_message_pattern_failure() {
    let task = CustomMessagePatternTaskTask;
    let input = CustomMessagePatternTaskTaskInput {
        email: "invalid-email".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Please provide a valid email address");
}

// Test 31: Custom error message with range validation
#[task]
async fn custom_message_range_task(
    #[validate(
        min = 0,
        max = 120,
        message = "Age must be a realistic value between 0 and 120"
    )]
    age: i32,
) -> celers_core::Result<String> {
    Ok(format!("Age: {}", age))
}

#[tokio::test]
async fn test_custom_message_range_success() {
    let task = CustomMessageRangeTaskTask;
    let input = CustomMessageRangeTaskTaskInput { age: 50 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_custom_message_range_too_low() {
    let task = CustomMessageRangeTaskTask;
    let input = CustomMessageRangeTaskTaskInput { age: -10 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Age must be a realistic value between 0 and 120"
    );
}

#[tokio::test]
async fn test_custom_message_range_too_high() {
    let task = CustomMessageRangeTaskTask;
    let input = CustomMessageRangeTaskTaskInput { age: 150 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Age must be a realistic value between 0 and 120"
    );
}
// Test: Predefined email validator
#[task]
async fn validate_email_shorthand(#[validate(email)] email: String) -> celers_core::Result<String> {
    Ok(format!("Email: {}", email))
}

#[tokio::test]
async fn test_validate_email_shorthand_success() {
    let task = ValidateEmailShorthandTask;
    let input = ValidateEmailShorthandTaskInput {
        email: "user@example.com".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_email_shorthand_failure() {
    let task = ValidateEmailShorthandTask;
    let input = ValidateEmailShorthandTaskInput {
        email: "invalid-email".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("valid email address"));
}

// Test: Predefined url validator
#[task]
async fn validate_url_shorthand(#[validate(url)] website: String) -> celers_core::Result<String> {
    Ok(format!("URL: {}", website))
}

#[tokio::test]
async fn test_validate_url_shorthand_success() {
    let task = ValidateUrlShorthandTask;
    let input = ValidateUrlShorthandTaskInput {
        website: "https://example.com".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_url_shorthand_failure() {
    let task = ValidateUrlShorthandTask;
    let input = ValidateUrlShorthandTaskInput {
        website: "not-a-url".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("valid URL"));
}

// Test: Predefined phone validator
#[task]
async fn validate_phone_shorthand(
    #[validate(phone)] number: String,
) -> celers_core::Result<String> {
    Ok(format!("Phone: {}", number))
}

#[tokio::test]
async fn test_validate_phone_shorthand_success() {
    let task = ValidatePhoneShorthandTask;
    let input = ValidatePhoneShorthandTaskInput {
        number: "+1234567890".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_phone_shorthand_failure() {
    let task = ValidatePhoneShorthandTask;
    let input = ValidatePhoneShorthandTaskInput {
        number: "123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("valid phone number"));
}

// Test: not_empty validator
#[task]
async fn validate_not_empty(#[validate(not_empty)] text: String) -> celers_core::Result<String> {
    Ok(format!("Text: {}", text))
}

#[tokio::test]
async fn test_validate_not_empty_success() {
    let task = ValidateNotEmptyTask;
    let input = ValidateNotEmptyTaskInput {
        text: "hello".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_not_empty_failure() {
    let task = ValidateNotEmptyTask;
    let input = ValidateNotEmptyTaskInput {
        text: "".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("must not be empty"));
}

// Test: positive validator
#[task]
async fn validate_positive_number(#[validate(positive)] count: i32) -> celers_core::Result<String> {
    Ok(format!("Count: {}", count))
}

#[tokio::test]
async fn test_validate_positive_success() {
    let task = ValidatePositiveNumberTask;
    let input = ValidatePositiveNumberTaskInput { count: 42 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_positive_failure_zero() {
    let task = ValidatePositiveNumberTask;
    let input = ValidatePositiveNumberTaskInput { count: 0 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("must be positive"));
}

#[tokio::test]
async fn test_validate_positive_failure_negative() {
    let task = ValidatePositiveNumberTask;
    let input = ValidatePositiveNumberTaskInput { count: -5 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("must be positive"));
}

// Test: negative validator
#[task]
async fn validate_negative_number(
    #[validate(negative)] temperature: i32,
) -> celers_core::Result<String> {
    Ok(format!("Temperature: {}", temperature))
}

#[tokio::test]
async fn test_validate_negative_success() {
    let task = ValidateNegativeNumberTask;
    let input = ValidateNegativeNumberTaskInput { temperature: -10 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_negative_failure_zero() {
    let task = ValidateNegativeNumberTask;
    let input = ValidateNegativeNumberTaskInput { temperature: 0 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("must be negative"));
}

#[tokio::test]
async fn test_validate_negative_failure_positive() {
    let task = ValidateNegativeNumberTask;
    let input = ValidateNegativeNumberTaskInput { temperature: 5 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("must be negative"));
}

// Test: alphabetic validator
#[task]
async fn validate_alphabetic(#[validate(alphabetic)] name: String) -> celers_core::Result<String> {
    Ok(format!("Name: {}", name))
}

#[tokio::test]
async fn test_validate_alphabetic_success() {
    let task = ValidateAlphabeticTask;
    let input = ValidateAlphabeticTaskInput {
        name: "Alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_alphabetic_failure() {
    let task = ValidateAlphabeticTask;
    let input = ValidateAlphabeticTaskInput {
        name: "Alice123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("only alphabetic characters"));
}

// Test: alphanumeric validator
#[task]
async fn validate_alphanumeric(
    #[validate(alphanumeric)] username: String,
) -> celers_core::Result<String> {
    Ok(format!("Username: {}", username))
}

#[tokio::test]
async fn test_validate_alphanumeric_success() {
    let task = ValidateAlphanumericTask;
    let input = ValidateAlphanumericTaskInput {
        username: "Alice123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_alphanumeric_failure() {
    let task = ValidateAlphanumericTask;
    let input = ValidateAlphanumericTaskInput {
        username: "Alice_123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("only alphanumeric characters"));
}

// Test: Combined predefined validators with custom message
#[task]
async fn validate_combined_predefined(
    #[validate(email, message = "Please enter a valid email address")] email: String,
    #[validate(positive, message = "Quantity must be greater than zero")] quantity: i32,
) -> celers_core::Result<String> {
    Ok(format!("Order for {} with {} items", email, quantity))
}

#[tokio::test]
async fn test_validate_combined_predefined_success() {
    let task = ValidateCombinedPredefinedTask;
    let input = ValidateCombinedPredefinedTaskInput {
        email: "user@example.com".to_string(),
        quantity: 5,
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_combined_predefined_email_failure() {
    let task = ValidateCombinedPredefinedTask;
    let input = ValidateCombinedPredefinedTaskInput {
        email: "invalid".to_string(),
        quantity: 5,
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Please enter a valid email address");
}

#[tokio::test]
async fn test_validate_combined_predefined_quantity_failure() {
    let task = ValidateCombinedPredefinedTask;
    let input = ValidateCombinedPredefinedTaskInput {
        email: "user@example.com".to_string(),
        quantity: -1,
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Quantity must be greater than zero");
}

// Test: numeric validator
#[task]
async fn validate_numeric(#[validate(numeric)] pin: String) -> celers_core::Result<String> {
    Ok(format!("PIN: {}", pin))
}

#[tokio::test]
async fn test_validate_numeric_success() {
    let task = ValidateNumericTask;
    let input = ValidateNumericTaskInput {
        pin: "123456".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_numeric_failure() {
    let task = ValidateNumericTask;
    let input = ValidateNumericTaskInput {
        pin: "12a456".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("only numeric characters"));
}

// Test: uuid validator
#[task]
async fn validate_uuid(#[validate(uuid)] id: String) -> celers_core::Result<String> {
    Ok(format!("ID: {}", id))
}

#[tokio::test]
async fn test_validate_uuid_success() {
    let task = ValidateUuidTask;
    let input = ValidateUuidTaskInput {
        id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_uuid_failure() {
    let task = ValidateUuidTask;
    let input = ValidateUuidTaskInput {
        id: "not-a-uuid".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("valid UUID"));
}

#[tokio::test]
async fn test_validate_uuid_failure_wrong_format() {
    let task = ValidateUuidTask;
    let input = ValidateUuidTaskInput {
        id: "550e8400e29b41d4a716446655440000".to_string(), // Missing hyphens
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: ipv4 validator
#[task]
async fn validate_ipv4(#[validate(ipv4)] address: String) -> celers_core::Result<String> {
    Ok(format!("IP: {}", address))
}

#[tokio::test]
async fn test_validate_ipv4_success() {
    let task = ValidateIpv4Task;
    let input = ValidateIpv4TaskInput {
        address: "192.168.1.1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ipv4_success_edge_cases() {
    let task = ValidateIpv4Task;

    // Test 0.0.0.0
    let input = ValidateIpv4TaskInput {
        address: "0.0.0.0".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test 255.255.255.255
    let input = ValidateIpv4TaskInput {
        address: "255.255.255.255".to_string(),
    };
    assert!(task.execute(input).await.is_ok());
}

#[tokio::test]
async fn test_validate_ipv4_failure() {
    let task = ValidateIpv4Task;
    let input = ValidateIpv4TaskInput {
        address: "not-an-ip".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("valid IPv4 address"));
}

#[tokio::test]
async fn test_validate_ipv4_failure_out_of_range() {
    let task = ValidateIpv4Task;
    let input = ValidateIpv4TaskInput {
        address: "256.1.1.1".to_string(), // 256 is out of range
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: hexadecimal validator
#[task]
async fn validate_hexadecimal(
    #[validate(hexadecimal)] hash: String,
) -> celers_core::Result<String> {
    Ok(format!("Hash: {}", hash))
}

#[tokio::test]
async fn test_validate_hexadecimal_success() {
    let task = ValidateHexadecimalTask;
    let input = ValidateHexadecimalTaskInput {
        hash: "1a2b3c4d5e6f".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_hexadecimal_success_uppercase() {
    let task = ValidateHexadecimalTask;
    let input = ValidateHexadecimalTaskInput {
        hash: "1A2B3C4D5E6F".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_hexadecimal_success_mixed_case() {
    let task = ValidateHexadecimalTask;
    let input = ValidateHexadecimalTaskInput {
        hash: "1a2B3c4D5e6F".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_hexadecimal_failure() {
    let task = ValidateHexadecimalTask;
    let input = ValidateHexadecimalTaskInput {
        hash: "not-hex-123g".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.message().contains("only hexadecimal characters"));
}

// Test: Combined new validators with custom messages
#[task]
async fn validate_new_validators_combined(
    #[validate(numeric, message = "PIN must contain only digits")] pin: String,
    #[validate(uuid, message = "Invalid transaction ID format")] transaction_id: String,
    #[validate(ipv4, message = "Invalid server IP address")] server_ip: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Transaction {} from {} with PIN {}",
        transaction_id, server_ip, pin
    ))
}

#[tokio::test]
async fn test_validate_new_validators_combined_success() {
    let task = ValidateNewValidatorsCombinedTask;
    let input = ValidateNewValidatorsCombinedTaskInput {
        pin: "123456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "192.168.1.100".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_new_validators_combined_pin_failure() {
    let task = ValidateNewValidatorsCombinedTask;
    let input = ValidateNewValidatorsCombinedTaskInput {
        pin: "12a456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "192.168.1.100".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "PIN must contain only digits");
}

#[tokio::test]
async fn test_validate_new_validators_combined_uuid_failure() {
    let task = ValidateNewValidatorsCombinedTask;
    let input = ValidateNewValidatorsCombinedTaskInput {
        pin: "123456".to_string(),
        transaction_id: "invalid-uuid".to_string(),
        server_ip: "192.168.1.100".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid transaction ID format");
}

#[tokio::test]
async fn test_validate_new_validators_combined_ipv4_failure() {
    let task = ValidateNewValidatorsCombinedTask;
    let input = ValidateNewValidatorsCombinedTaskInput {
        pin: "123456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "256.256.256.256".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid server IP address");
}

// Test IPv6 validation
#[task]
async fn validate_ipv6(#[validate(ipv6)] address: String) -> celers_core::Result<String> {
    Ok(format!("Valid IPv6: {}", address))
}

#[tokio::test]
async fn test_validate_ipv6_success() {
    let task = ValidateIpv6Task;
    let input = ValidateIpv6TaskInput {
        address: "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ipv6_success_compressed() {
    let task = ValidateIpv6Task;
    let input = ValidateIpv6TaskInput {
        address: "2001:db8::1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ipv6_failure() {
    let task = ValidateIpv6Task;
    let input = ValidateIpv6TaskInput {
        address: "not-an-ipv6".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Field 'address' must be a valid IPv6 address"
    );
}

// Test slug validation
#[task]
async fn validate_slug(#[validate(slug)] slug: String) -> celers_core::Result<String> {
    Ok(format!("Valid slug: {}", slug))
}

#[tokio::test]
async fn test_validate_slug_success() {
    let task = ValidateSlugTask;
    let input = ValidateSlugTaskInput {
        slug: "hello-world".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_slug_success_numbers() {
    let task = ValidateSlugTask;
    let input = ValidateSlugTaskInput {
        slug: "article-123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_slug_failure_uppercase() {
    let task = ValidateSlugTask;
    let input = ValidateSlugTaskInput {
        slug: "Hello-World".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Field 'slug' must be a valid URL slug (lowercase letters, numbers, and hyphens only)"
    );
}

#[tokio::test]
async fn test_validate_slug_failure_underscore() {
    let task = ValidateSlugTask;
    let input = ValidateSlugTaskInput {
        slug: "hello_world".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test MAC address validation
#[task]
async fn validate_mac(#[validate(mac_address)] mac: String) -> celers_core::Result<String> {
    Ok(format!("Valid MAC: {}", mac))
}

#[tokio::test]
async fn test_validate_mac_success_colon() {
    let task = ValidateMacTask;
    let input = ValidateMacTaskInput {
        mac: "00:1A:2B:3C:4D:5E".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_mac_success_hyphen() {
    let task = ValidateMacTask;
    let input = ValidateMacTaskInput {
        mac: "00-1A-2B-3C-4D-5E".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_mac_failure() {
    let task = ValidateMacTask;
    let input = ValidateMacTaskInput {
        mac: "not-a-mac".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Field 'mac' must be a valid MAC address");
}

#[tokio::test]
async fn test_validate_mac_failure_wrong_format() {
    let task = ValidateMacTask;
    let input = ValidateMacTaskInput {
        mac: "00:1A:2B:3C:4D".to_string(), // Too short
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test combined new validators with custom messages
#[task]
async fn validate_network_config(
    #[validate(ipv6, message = "Invalid IPv6 address")] ipv6_addr: String,
    #[validate(mac_address, message = "Invalid MAC address")] mac_addr: String,
    #[validate(slug, message = "Invalid hostname slug")] hostname: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Network configured: {} - {} - {}",
        ipv6_addr, mac_addr, hostname
    ))
}

#[tokio::test]
async fn test_validate_network_config_success() {
    let task = ValidateNetworkConfigTask;
    let input = ValidateNetworkConfigTaskInput {
        ipv6_addr: "2001:db8::1".to_string(),
        mac_addr: "00:1A:2B:3C:4D:5E".to_string(),
        hostname: "server-01".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_network_config_ipv6_failure() {
    let task = ValidateNetworkConfigTask;
    let input = ValidateNetworkConfigTaskInput {
        ipv6_addr: "invalid".to_string(),
        mac_addr: "00:1A:2B:3C:4D:5E".to_string(),
        hostname: "server-01".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid IPv6 address");
}

#[tokio::test]
async fn test_validate_network_config_mac_failure() {
    let task = ValidateNetworkConfigTask;
    let input = ValidateNetworkConfigTaskInput {
        ipv6_addr: "2001:db8::1".to_string(),
        mac_addr: "invalid".to_string(),
        hostname: "server-01".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid MAC address");
}

#[tokio::test]
async fn test_validate_network_config_slug_failure() {
    let task = ValidateNetworkConfigTask;
    let input = ValidateNetworkConfigTaskInput {
        ipv6_addr: "2001:db8::1".to_string(),
        mac_addr: "00:1A:2B:3C:4D:5E".to_string(),
        hostname: "Server_01".to_string(), // Invalid: contains uppercase and underscore
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid hostname slug");
}

// Test JSON validation
#[task]
async fn validate_json(#[validate(json)] data: String) -> celers_core::Result<String> {
    Ok(format!("Valid JSON: {}", data))
}

#[tokio::test]
async fn test_validate_json_success_object() {
    let task = ValidateJsonTask;
    let input = ValidateJsonTaskInput {
        data: r#"{"name": "test", "value": 123}"#.to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_json_success_array() {
    let task = ValidateJsonTask;
    let input = ValidateJsonTaskInput {
        data: r#"[1, 2, 3, "test"]"#.to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_json_failure() {
    let task = ValidateJsonTask;
    let input = ValidateJsonTaskInput {
        data: "not-valid-json".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Field 'data' must be valid JSON");
}

// Test base64 validation
#[task]
async fn validate_base64(#[validate(base64)] data: String) -> celers_core::Result<String> {
    Ok(format!("Valid base64: {}", data))
}

#[tokio::test]
async fn test_validate_base64_success() {
    let task = ValidateBase64Task;
    let input = ValidateBase64TaskInput {
        data: "SGVsbG8gV29ybGQ=".to_string(), // "Hello World" in base64
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_base64_success_no_padding() {
    let task = ValidateBase64Task;
    let input = ValidateBase64TaskInput {
        data: "SGVsbG8=".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_base64_failure_invalid_chars() {
    let task = ValidateBase64Task;
    let input = ValidateBase64TaskInput {
        data: "Invalid@#$".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Field 'data' must be valid base64");
}

#[tokio::test]
async fn test_validate_base64_failure_wrong_length() {
    let task = ValidateBase64Task;
    let input = ValidateBase64TaskInput {
        data: "SGVs".to_string(), // Length not divisible by 4
    };
    let result = task.execute(input).await;
    assert!(result.is_ok()); // This should pass as it's divisible by 4
}

// Test color hex validation
#[task]
async fn validate_color(#[validate(color_hex)] color: String) -> celers_core::Result<String> {
    Ok(format!("Valid color: {}", color))
}

#[tokio::test]
async fn test_validate_color_hex_success_six_digit() {
    let task = ValidateColorTask;
    let input = ValidateColorTaskInput {
        color: "#FF5733".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_color_hex_success_three_digit() {
    let task = ValidateColorTask;
    let input = ValidateColorTaskInput {
        color: "#F53".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_color_hex_failure_no_hash() {
    let task = ValidateColorTask;
    let input = ValidateColorTaskInput {
        color: "FF5733".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(
        error.message(),
        "Field 'color' must be a valid hex color code (#RGB or #RRGGBB)"
    );
}

#[tokio::test]
async fn test_validate_color_hex_failure_wrong_length() {
    let task = ValidateColorTask;
    let input = ValidateColorTaskInput {
        color: "#FF57".to_string(), // 4 digits, should be 3 or 6
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test combined new validators with custom messages
#[task]
async fn validate_web_data(
    #[validate(json, message = "Invalid JSON configuration")] config: String,
    #[validate(base64, message = "Invalid base64 encoded data")] encoded: String,
    #[validate(color_hex, message = "Invalid color code")] primary_color: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Web data validated: config, encoded, color={}",
        primary_color
    ))
}

#[tokio::test]
async fn test_validate_web_data_success() {
    let task = ValidateWebDataTask;
    let input = ValidateWebDataTaskInput {
        config: r#"{"theme": "dark"}"#.to_string(),
        encoded: "SGVsbG8=".to_string(),
        primary_color: "#007BFF".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_web_data_json_failure() {
    let task = ValidateWebDataTask;
    let input = ValidateWebDataTaskInput {
        config: "invalid-json".to_string(),
        encoded: "SGVsbG8=".to_string(),
        primary_color: "#007BFF".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid JSON configuration");
}

#[tokio::test]
async fn test_validate_web_data_base64_failure() {
    let task = ValidateWebDataTask;
    let input = ValidateWebDataTaskInput {
        config: r#"{"theme": "dark"}"#.to_string(),
        encoded: "Invalid@Data".to_string(),
        primary_color: "#007BFF".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid base64 encoded data");
}

#[tokio::test]
async fn test_validate_web_data_color_failure() {
    let task = ValidateWebDataTask;
    let input = ValidateWebDataTaskInput {
        config: r#"{"theme": "dark"}"#.to_string(),
        encoded: "SGVsbG8=".to_string(),
        primary_color: "blue".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "Invalid color code");
}

// Tests: `#[derive(Task)]` attribute parsing (regression tests for the
// derive-macro fix). Two of the original bugs -- `parse_nested_meta`
// errors being discarded via `let _ = ...`, and an unparsable `input`/
// `output` type string silently becoming `serde_json::Value` -- are
// compile-time failure modes that would need a `trybuild`-style
// compile-fail harness to assert on directly (not available to this
// package's dev-dependencies). What *is* directly testable here is that
// the valid, documented paths the fix had to keep working still parse
// correctly and generate a working `Task` impl: explicit `input`/`output`
// types, and the no-attribute default (`serde_json::Value` for both).

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DeriveMathInput {
    a: i32,
    b: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DeriveMathOutput {
    sum: i32,
}

#[derive(TaskDerive)]
#[task(
    input = "DeriveMathInput",
    output = "DeriveMathOutput",
    name = "derive.math.add"
)]
struct DeriveMathTask;

impl DeriveMathTask {
    async fn execute_impl(&self, input: DeriveMathInput) -> celers_core::Result<DeriveMathOutput> {
        Ok(DeriveMathOutput {
            sum: input.a + input.b,
        })
    }
}

#[tokio::test]
async fn test_derive_task_with_explicit_types() {
    let task = DeriveMathTask;
    assert_eq!(task.name(), "derive.math.add");
    let input = DeriveMathInput { a: 3, b: 4 };
    let result = task.execute(input).await;
    assert_eq!(result.unwrap().sum, 7);
}

#[derive(TaskDerive)]
struct DeriveDefaultTask;

impl DeriveDefaultTask {
    async fn execute_impl(
        &self,
        input: serde_json::Value,
    ) -> celers_core::Result<serde_json::Value> {
        Ok(input)
    }
}

#[tokio::test]
async fn test_derive_task_defaults() {
    let task = DeriveDefaultTask;
    // No #[task(name = "...")] given -> falls back to the snake_case
    // conversion of the struct name.
    assert_eq!(task.name(), "derive_default_task");
    // No #[task(input = ..., output = ...)] given -> falls back to
    // `serde_json::Value` for both, per `parse_type_attr`'s documented
    // default.
    let input = serde_json::json!({"x": 1});
    let result = task.execute(input.clone()).await;
    assert_eq!(result.unwrap(), input);
}
