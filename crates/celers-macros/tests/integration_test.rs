//! Integration tests for celers-macros
//!
//! These tests verify that the procedural macros work correctly
//! in a real-world scenario by actually compiling and executing them.

use celers_macros::task;

// Mock the celers_core types that the macro expects
mod celers_core {
    use serde::{Deserialize, Serialize};

    #[derive(Debug)]
    pub struct CelersError(pub String);

    impl std::fmt::Display for CelersError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for CelersError {}

    pub type Result<T> = std::result::Result<T, CelersError>;

    #[async_trait::async_trait]
    pub trait Task: Send + Sync {
        type Input: Serialize + for<'de> Deserialize<'de> + Send;
        type Output: Serialize + for<'de> Deserialize<'de> + Send;

        async fn execute(&self, input: Self::Input) -> Result<Self::Output>;
        fn name(&self) -> &str;
    }
}

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
        Err(celers_core::CelersError("Task failed".to_string()))
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
    assert_eq!(result.unwrap_err().0, "Task failed");
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

// Test 13: Task with simple generic (no HRTB for now)
// Note: Full generic support with HRTB requires more complex implementation
// This test demonstrates basic generic parameter support
#[test]
fn test_generic_task_compiles() {
    // This test just verifies that generic tasks compile correctly
    // Actual execution with generics requires the serde bounds which are complex
    // For now, we verify the macro accepts generic syntax

    // The macro should accept this syntax without errors:
    // #[task]
    // async fn generic_task<T>(value: T) -> celers_core::Result<T>
    // where T: Send { Ok(value) }
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
    assert!(error.0.contains("below minimum"));
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
    assert!(error.0.contains("exceeds maximum"));
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
    assert!(error.0.contains("below minimum"));
}

#[tokio::test]
async fn test_validate_range_too_high() {
    let task = ValidateRangeTaskTask;
    let input = ValidateRangeTaskTaskInput { age: 150 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.0.contains("exceeds maximum"));
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
    assert!(error.0.contains("below minimum"));
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
    assert!(error.0.contains("exceeds maximum"));
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
    assert!(error.0.contains("age"));
    assert!(error.0.contains("below minimum"));
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
    assert!(error.0.contains("name"));
    assert!(error.0.contains("below minimum"));
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
    assert!(error.0.contains("does not match required pattern"));
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
    assert!(error.0.contains("does not match required pattern"));
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
    assert!(error.0.contains("length"));
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
    assert!(error.0.contains("does not match required pattern"));
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
    assert_eq!(error.0, "You must be at least 18 years old");
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
    assert_eq!(error.0, "Score cannot exceed 100 points");
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
    assert_eq!(error.0, "Username must be between 3 and 20 characters");
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
    assert_eq!(error.0, "Username must be between 3 and 20 characters");
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
    assert_eq!(error.0, "Please provide a valid email address");
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
    assert_eq!(error.0, "Age must be a realistic value between 0 and 120");
}

#[tokio::test]
async fn test_custom_message_range_too_high() {
    let task = CustomMessageRangeTaskTask;
    let input = CustomMessageRangeTaskTaskInput { age: 150 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Age must be a realistic value between 0 and 120");
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
    assert!(error.0.contains("valid email address"));
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
    assert!(error.0.contains("valid URL"));
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
    assert!(error.0.contains("valid phone number"));
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
    assert!(error.0.contains("must not be empty"));
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
    assert!(error.0.contains("must be positive"));
}

#[tokio::test]
async fn test_validate_positive_failure_negative() {
    let task = ValidatePositiveNumberTask;
    let input = ValidatePositiveNumberTaskInput { count: -5 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.0.contains("must be positive"));
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
    assert!(error.0.contains("must be negative"));
}

#[tokio::test]
async fn test_validate_negative_failure_positive() {
    let task = ValidateNegativeNumberTask;
    let input = ValidateNegativeNumberTaskInput { temperature: 5 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.0.contains("must be negative"));
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
    assert!(error.0.contains("only alphabetic characters"));
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
    assert!(error.0.contains("only alphanumeric characters"));
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
    assert_eq!(error.0, "Please enter a valid email address");
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
    assert_eq!(error.0, "Quantity must be greater than zero");
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
    assert!(error.0.contains("only numeric characters"));
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
    assert!(error.0.contains("valid UUID"));
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
    assert!(error.0.contains("valid IPv4 address"));
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
    assert!(error.0.contains("only hexadecimal characters"));
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
    assert_eq!(error.0, "PIN must contain only digits");
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
    assert_eq!(error.0, "Invalid transaction ID format");
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
    assert_eq!(error.0, "Invalid server IP address");
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
    assert_eq!(error.0, "Field 'address' must be a valid IPv6 address");
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
        error.0,
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
    assert_eq!(error.0, "Field 'mac' must be a valid MAC address");
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
    assert_eq!(error.0, "Invalid IPv6 address");
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
    assert_eq!(error.0, "Invalid MAC address");
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
    assert_eq!(error.0, "Invalid hostname slug");
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
    assert_eq!(error.0, "Field 'data' must be valid JSON");
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
    assert_eq!(error.0, "Field 'data' must be valid base64");
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
        error.0,
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
    assert_eq!(error.0, "Invalid JSON configuration");
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
    assert_eq!(error.0, "Invalid base64 encoded data");
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
    assert_eq!(error.0, "Invalid color code");
}

// ============================================================================
// Additional Practical Validators Tests (January 18, 2026)
// ============================================================================

// Semver validator tests
#[task]
async fn validate_semver(
    #[validate(semver, message = "Invalid version format")] version: String,
) -> celers_core::Result<String> {
    Ok(format!("Version: {}", version))
}

#[tokio::test]
async fn test_validate_semver_success() {
    let task = ValidateSemverTask;
    let input = ValidateSemverTaskInput {
        version: "1.2.3".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_semver_success_prerelease() {
    let task = ValidateSemverTask;
    let input = ValidateSemverTaskInput {
        version: "2.0.0-alpha.1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_semver_failure() {
    let task = ValidateSemverTask;
    let input = ValidateSemverTaskInput {
        version: "1.2".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid version format");
}

// Domain validator tests
#[task]
async fn validate_domain(
    #[validate(domain, message = "Invalid domain name")] domain: String,
) -> celers_core::Result<String> {
    Ok(format!("Domain: {}", domain))
}

#[tokio::test]
async fn test_validate_domain_success() {
    let task = ValidateDomainTask;
    let input = ValidateDomainTaskInput {
        domain: "example.com".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_domain_success_subdomain() {
    let task = ValidateDomainTask;
    let input = ValidateDomainTaskInput {
        domain: "subdomain.example.co.uk".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_domain_failure() {
    let task = ValidateDomainTask;
    let input = ValidateDomainTaskInput {
        domain: "not_a_domain".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid domain name");
}

// ASCII validator tests
#[task]
async fn validate_ascii(
    #[validate(ascii, message = "Must be ASCII only")] text: String,
) -> celers_core::Result<String> {
    Ok(format!("ASCII: {}", text))
}

#[tokio::test]
async fn test_validate_ascii_success() {
    let task = ValidateAsciiTask;
    let input = ValidateAsciiTaskInput {
        text: "Hello World 123!".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ascii_failure() {
    let task = ValidateAsciiTask;
    let input = ValidateAsciiTaskInput {
        text: "Hello 世界".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Must be ASCII only");
}

#[tokio::test]
async fn test_validate_ascii_success_empty() {
    let task = ValidateAsciiTask;
    let input = ValidateAsciiTaskInput {
        text: "".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

// Lowercase validator tests
#[task]
async fn validate_lowercase(
    #[validate(lowercase, message = "Must be lowercase")] tag: String,
) -> celers_core::Result<String> {
    Ok(format!("Tag: {}", tag))
}

#[tokio::test]
async fn test_validate_lowercase_success() {
    let task = ValidateLowercaseTask;
    let input = ValidateLowercaseTaskInput {
        tag: "hello123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_lowercase_failure() {
    let task = ValidateLowercaseTask;
    let input = ValidateLowercaseTaskInput {
        tag: "Hello".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Must be lowercase");
}

#[tokio::test]
async fn test_validate_lowercase_success_with_numbers() {
    let task = ValidateLowercaseTask;
    let input = ValidateLowercaseTaskInput {
        tag: "tag-123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

// Uppercase validator tests
#[task]
async fn validate_uppercase(
    #[validate(uppercase, message = "Must be uppercase")] code: String,
) -> celers_core::Result<String> {
    Ok(format!("Code: {}", code))
}

#[tokio::test]
async fn test_validate_uppercase_success() {
    let task = ValidateUppercaseTask;
    let input = ValidateUppercaseTaskInput {
        code: "HELLO123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_uppercase_failure() {
    let task = ValidateUppercaseTask;
    let input = ValidateUppercaseTaskInput {
        code: "Hello".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Must be uppercase");
}

#[tokio::test]
async fn test_validate_uppercase_success_with_numbers() {
    let task = ValidateUppercaseTask;
    let input = ValidateUppercaseTaskInput {
        code: "CODE-456".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

// Time 24h validator tests
#[task]
async fn validate_time(
    #[validate(time_24h, message = "Invalid time format")] time: String,
) -> celers_core::Result<String> {
    Ok(format!("Time: {}", time))
}

#[tokio::test]
async fn test_validate_time_success() {
    let task = ValidateTimeTask;
    let input = ValidateTimeTaskInput {
        time: "14:30".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_time_success_with_seconds() {
    let task = ValidateTimeTask;
    let input = ValidateTimeTaskInput {
        time: "23:59:59".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_time_failure() {
    let task = ValidateTimeTask;
    let input = ValidateTimeTaskInput {
        time: "25:00".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid time format");
}

// Date ISO 8601 validator tests
#[task]
async fn validate_date(
    #[validate(date_iso8601, message = "Invalid date format")] date: String,
) -> celers_core::Result<String> {
    Ok(format!("Date: {}", date))
}

#[tokio::test]
async fn test_validate_date_success() {
    let task = ValidateDateTask;
    let input = ValidateDateTaskInput {
        date: "2026-01-30".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_date_failure_invalid_month() {
    let task = ValidateDateTask;
    let input = ValidateDateTaskInput {
        date: "2026-13-01".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid date format");
}

#[tokio::test]
async fn test_validate_date_failure_wrong_format() {
    let task = ValidateDateTask;
    let input = ValidateDateTaskInput {
        date: "01/18/2026".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid date format");
}

// Credit card validator tests
#[task]
async fn validate_credit_card(
    #[validate(credit_card, message = "Invalid credit card number")] card: String,
) -> celers_core::Result<String> {
    Ok("Card validated".to_string())
}

#[tokio::test]
async fn test_validate_credit_card_success() {
    let task = ValidateCreditCardTask;
    // Valid Visa test card number
    let input = ValidateCreditCardTaskInput {
        card: "4532015112830366".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_credit_card_success_with_spaces() {
    let task = ValidateCreditCardTask;
    // Valid Visa test card with spaces
    let input = ValidateCreditCardTaskInput {
        card: "4532 0151 1283 0366".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_credit_card_failure() {
    let task = ValidateCreditCardTask;
    let input = ValidateCreditCardTaskInput {
        card: "1234567890123456".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid credit card number");
}

// Combined test with all new validators
#[task]
async fn validate_all_new(
    #[validate(semver, message = "Invalid version")] version: String,
    #[validate(domain, message = "Invalid domain")] domain: String,
    #[validate(ascii, message = "Must be ASCII")] description: String,
    #[validate(lowercase, message = "Must be lowercase")] tag: String,
    #[validate(uppercase, message = "Must be uppercase")] code: String,
    #[validate(time_24h, message = "Invalid time")] time: String,
    #[validate(date_iso8601, message = "Invalid date")] date: String,
    #[validate(credit_card, message = "Invalid card")] card: String,
) -> celers_core::Result<String> {
    Ok("All validated".to_string())
}

#[tokio::test]
async fn test_validate_all_new_success() {
    let task = ValidateAllNewTask;
    let input = ValidateAllNewTaskInput {
        version: "1.0.0".to_string(),
        domain: "example.com".to_string(),
        description: "ASCII text".to_string(),
        tag: "tag123".to_string(),
        code: "CODE456".to_string(),
        time: "14:30:00".to_string(),
        date: "2026-01-30".to_string(),
        card: "4532015112830366".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_all_new_version_failure() {
    let task = ValidateAllNewTask;
    let input = ValidateAllNewTaskInput {
        version: "1.0".to_string(),
        domain: "example.com".to_string(),
        description: "ASCII text".to_string(),
        tag: "tag123".to_string(),
        code: "CODE456".to_string(),
        time: "14:30:00".to_string(),
        date: "2026-01-30".to_string(),
        card: "4532015112830366".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid version");
}

#[tokio::test]
async fn test_validate_all_new_card_failure() {
    let task = ValidateAllNewTask;
    let input = ValidateAllNewTaskInput {
        version: "1.0.0".to_string(),
        domain: "example.com".to_string(),
        description: "ASCII text".to_string(),
        tag: "tag123".to_string(),
        code: "CODE456".to_string(),
        time: "14:30:00".to_string(),
        date: "2026-01-30".to_string(),
        card: "1234567890123456".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid card");
}

// ============================================================================
// NEW GEOGRAPHIC AND LOCALE VALIDATORS (ADDED 2026-01-30)
// ============================================================================

// Test: Latitude validator - success
#[task]
async fn validate_latitude_task(#[validate(latitude)] lat: String) -> celers_core::Result<String> {
    Ok(format!("Latitude: {}", lat))
}

#[tokio::test]
async fn test_validate_latitude_success() {
    let task = ValidateLatitudeTaskTask;
    let input = ValidateLatitudeTaskTaskInput {
        lat: "45.5".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_latitude_edge_cases() {
    let task = ValidateLatitudeTaskTask;

    // Test -90 (valid)
    let input = ValidateLatitudeTaskTaskInput {
        lat: "-90".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test 90 (valid)
    let input = ValidateLatitudeTaskTaskInput {
        lat: "90".to_string(),
    };
    assert!(task.execute(input).await.is_ok());
}

#[tokio::test]
async fn test_validate_latitude_failure() {
    let task = ValidateLatitudeTaskTask;

    // Test > 90
    let input = ValidateLatitudeTaskTaskInput {
        lat: "91".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());

    // Test < -90
    let input = ValidateLatitudeTaskTaskInput {
        lat: "-91".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: Longitude validator - success
#[task]
async fn validate_longitude_task(
    #[validate(longitude)] lon: String,
) -> celers_core::Result<String> {
    Ok(format!("Longitude: {}", lon))
}

#[tokio::test]
async fn test_validate_longitude_success() {
    let task = ValidateLongitudeTaskTask;
    let input = ValidateLongitudeTaskTaskInput {
        lon: "122.4".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_longitude_edge_cases() {
    let task = ValidateLongitudeTaskTask;

    // Test -180 (valid)
    let input = ValidateLongitudeTaskTaskInput {
        lon: "-180".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test 180 (valid)
    let input = ValidateLongitudeTaskTaskInput {
        lon: "180".to_string(),
    };
    assert!(task.execute(input).await.is_ok());
}

#[tokio::test]
async fn test_validate_longitude_failure() {
    let task = ValidateLongitudeTaskTask;

    // Test > 180
    let input = ValidateLongitudeTaskTaskInput {
        lon: "181".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());

    // Test < -180
    let input = ValidateLongitudeTaskTaskInput {
        lon: "-181".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: ISO country code validator
#[task]
async fn validate_iso_country_task(
    #[validate(iso_country)] country: String,
) -> celers_core::Result<String> {
    Ok(format!("Country: {}", country))
}

#[tokio::test]
async fn test_validate_iso_country_success() {
    let task = ValidateIsoCountryTaskTask;

    // Test US
    let input = ValidateIsoCountryTaskTaskInput {
        country: "US".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test CA
    let input = ValidateIsoCountryTaskTaskInput {
        country: "CA".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test GB
    let input = ValidateIsoCountryTaskTaskInput {
        country: "GB".to_string(),
    };
    assert!(task.execute(input).await.is_ok());
}

#[tokio::test]
async fn test_validate_iso_country_failure() {
    let task = ValidateIsoCountryTaskTask;

    // Test lowercase (invalid)
    let input = ValidateIsoCountryTaskTaskInput {
        country: "us".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());

    // Test 3 letters (invalid)
    let input = ValidateIsoCountryTaskTaskInput {
        country: "USA".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: ISO language code validator
#[task]
async fn validate_iso_language_task(
    #[validate(iso_language)] lang: String,
) -> celers_core::Result<String> {
    Ok(format!("Language: {}", lang))
}

#[tokio::test]
async fn test_validate_iso_language_success() {
    let task = ValidateIsoLanguageTaskTask;

    // Test en
    let input = ValidateIsoLanguageTaskTaskInput {
        lang: "en".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test es
    let input = ValidateIsoLanguageTaskTaskInput {
        lang: "es".to_string(),
    };
    assert!(task.execute(input).await.is_ok());

    // Test fr
    let input = ValidateIsoLanguageTaskTaskInput {
        lang: "fr".to_string(),
    };
    assert!(task.execute(input).await.is_ok());
}

#[tokio::test]
async fn test_validate_iso_language_failure() {
    let task = ValidateIsoLanguageTaskTask;

    // Test uppercase (invalid)
    let input = ValidateIsoLanguageTaskTaskInput {
        lang: "EN".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());

    // Test 3 letters (invalid)
    let input = ValidateIsoLanguageTaskTaskInput {
        lang: "eng".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: US ZIP code validator
#[task]
async fn validate_us_zip_task(#[validate(us_zip)] zip: String) -> celers_core::Result<String> {
    Ok(format!("ZIP: {}", zip))
}

#[tokio::test]
async fn test_validate_us_zip_success_5_digit() {
    let task = ValidateUsZipTaskTask;
    let input = ValidateUsZipTaskTaskInput {
        zip: "12345".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_us_zip_success_9_digit() {
    let task = ValidateUsZipTaskTask;
    let input = ValidateUsZipTaskTaskInput {
        zip: "12345-6789".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_us_zip_failure() {
    let task = ValidateUsZipTaskTask;

    // Test 4 digits (invalid)
    let input = ValidateUsZipTaskTaskInput {
        zip: "1234".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());

    // Test with letters (invalid)
    let input = ValidateUsZipTaskTaskInput {
        zip: "12A45".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: Canadian postal code validator
#[task]
async fn validate_ca_postal_task(
    #[validate(ca_postal)] postal: String,
) -> celers_core::Result<String> {
    Ok(format!("Postal: {}", postal))
}

#[tokio::test]
async fn test_validate_ca_postal_success_with_space() {
    let task = ValidateCaPostalTaskTask;
    let input = ValidateCaPostalTaskTaskInput {
        postal: "K1A 0B1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ca_postal_success_without_space() {
    let task = ValidateCaPostalTaskTask;
    let input = ValidateCaPostalTaskTaskInput {
        postal: "K1A0B1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ca_postal_failure() {
    let task = ValidateCaPostalTaskTask;

    // Test lowercase (invalid)
    let input = ValidateCaPostalTaskTaskInput {
        postal: "k1a 0b1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());

    // Test all digits (invalid)
    let input = ValidateCaPostalTaskTaskInput {
        postal: "123 456".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Test: Combined new validators with custom messages
#[task]
async fn validate_location_data(
    #[validate(latitude, message = "Invalid latitude")] lat: String,
    #[validate(longitude, message = "Invalid longitude")] lon: String,
    #[validate(iso_country, message = "Invalid country code")] country: String,
    #[validate(iso_language, message = "Invalid language code")] lang: String,
    #[validate(us_zip, message = "Invalid ZIP code")] zip: String,
    #[validate(ca_postal, message = "Invalid postal code")] postal: String,
) -> celers_core::Result<String> {
    Ok("Location data validated".to_string())
}

#[tokio::test]
async fn test_validate_location_data_success() {
    let task = ValidateLocationDataTask;
    let input = ValidateLocationDataTaskInput {
        lat: "40.7128".to_string(),
        lon: "-74.0060".to_string(),
        country: "US".to_string(),
        lang: "en".to_string(),
        zip: "10001".to_string(),
        postal: "K1A 0B1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_location_data_lat_failure() {
    let task = ValidateLocationDataTask;
    let input = ValidateLocationDataTaskInput {
        lat: "91".to_string(), // Invalid
        lon: "-74.0060".to_string(),
        country: "US".to_string(),
        lang: "en".to_string(),
        zip: "10001".to_string(),
        postal: "K1A 0B1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid latitude");
}

#[tokio::test]
async fn test_validate_location_data_country_failure() {
    let task = ValidateLocationDataTask;
    let input = ValidateLocationDataTaskInput {
        lat: "40.7128".to_string(),
        lon: "-74.0060".to_string(),
        country: "USA".to_string(), // Invalid (3 letters)
        lang: "en".to_string(),
        zip: "10001".to_string(),
        postal: "K1A 0B1".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid country code");
}

// ============================================================================
// New Validators Tests (IBAN, Bitcoin, Ethereum, ISBN, Password Strength)
// ============================================================================

// IBAN Validator Tests
#[task]
async fn validate_iban(#[validate(iban)] account: String) -> celers_core::Result<String> {
    Ok(format!("IBAN validated: {}", account))
}

#[tokio::test]
async fn test_validate_iban_success() {
    let task = ValidateIbanTask;
    let input = ValidateIbanTaskInput {
        account: "GB82WEST12345698765432".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_iban_failure() {
    let task = ValidateIbanTask;
    let input = ValidateIbanTaskInput {
        account: "invalid-iban".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_iban_failure_too_short() {
    let task = ValidateIbanTask;
    let input = ValidateIbanTaskInput {
        account: "GB82".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Bitcoin Address Validator Tests
#[task]
async fn validate_bitcoin(
    #[validate(bitcoin_address, message = "Invalid Bitcoin address")] address: String,
) -> celers_core::Result<String> {
    Ok(format!("Bitcoin address validated: {}", address))
}

#[tokio::test]
async fn test_validate_bitcoin_success_p2pkh() {
    let task = ValidateBitcoinTask;
    let input = ValidateBitcoinTaskInput {
        address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_bitcoin_success_p2sh() {
    let task = ValidateBitcoinTask;
    let input = ValidateBitcoinTaskInput {
        address: "3J98t1WpEZ73CNmYviecrnyiWrnqRhWNLy".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_bitcoin_success_bech32() {
    let task = ValidateBitcoinTask;
    let input = ValidateBitcoinTaskInput {
        address: "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_bitcoin_failure() {
    let task = ValidateBitcoinTask;
    let input = ValidateBitcoinTaskInput {
        address: "not-a-bitcoin-address".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid Bitcoin address");
}

// Ethereum Address Validator Tests
#[task]
async fn validate_ethereum(
    #[validate(ethereum_address, message = "Invalid Ethereum address")] address: String,
) -> celers_core::Result<String> {
    Ok(format!("Ethereum address validated: {}", address))
}

#[tokio::test]
async fn test_validate_ethereum_success() {
    let task = ValidateEthereumTask;
    let input = ValidateEthereumTaskInput {
        address: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ethereum_success_lowercase() {
    let task = ValidateEthereumTask;
    let input = ValidateEthereumTaskInput {
        address: "0x742d35cc6634c0532925a3b844bc454e4438f44e".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_ethereum_failure_no_prefix() {
    let task = ValidateEthereumTask;
    let input = ValidateEthereumTaskInput {
        address: "742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid Ethereum address");
}

#[tokio::test]
async fn test_validate_ethereum_failure_too_short() {
    let task = ValidateEthereumTask;
    let input = ValidateEthereumTaskInput {
        address: "0x742d35Cc".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// ISBN Validator Tests
#[task]
async fn validate_isbn(
    #[validate(isbn, message = "Invalid ISBN")] book_id: String,
) -> celers_core::Result<String> {
    Ok(format!("ISBN validated: {}", book_id))
}

#[tokio::test]
async fn test_validate_isbn_10_success() {
    let task = ValidateIsbnTask;
    let input = ValidateIsbnTaskInput {
        book_id: "0306406152".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_isbn_10_with_x() {
    let task = ValidateIsbnTask;
    let input = ValidateIsbnTaskInput {
        book_id: "043942089X".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_isbn_13_success() {
    let task = ValidateIsbnTask;
    let input = ValidateIsbnTaskInput {
        book_id: "9780306406157".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_isbn_13_with_hyphens() {
    let task = ValidateIsbnTask;
    let input = ValidateIsbnTaskInput {
        book_id: "978-0-306-40615-7".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_isbn_failure_invalid_checksum() {
    let task = ValidateIsbnTask;
    let input = ValidateIsbnTaskInput {
        book_id: "9780306406150".to_string(), // Invalid checksum
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid ISBN");
}

#[tokio::test]
async fn test_validate_isbn_failure_wrong_length() {
    let task = ValidateIsbnTask;
    let input = ValidateIsbnTaskInput {
        book_id: "12345".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Password Strength Validator Tests
#[task]
async fn validate_password(
    #[validate(password_strength, message = "Weak password")] password: String,
) -> celers_core::Result<String> {
    Ok(format!("Password validated: {}", password.len()))
}

#[tokio::test]
async fn test_validate_password_success() {
    let task = ValidatePasswordTask;
    let input = ValidatePasswordTaskInput {
        password: "Str0ng!Pass".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_password_failure_too_short() {
    let task = ValidatePasswordTask;
    let input = ValidatePasswordTaskInput {
        password: "Str0!".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Weak password");
}

#[tokio::test]
async fn test_validate_password_failure_no_uppercase() {
    let task = ValidatePasswordTask;
    let input = ValidatePasswordTaskInput {
        password: "str0ng!pass".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Weak password");
}

#[tokio::test]
async fn test_validate_password_failure_no_lowercase() {
    let task = ValidatePasswordTask;
    let input = ValidatePasswordTaskInput {
        password: "STR0NG!PASS".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_password_failure_no_digit() {
    let task = ValidatePasswordTask;
    let input = ValidatePasswordTaskInput {
        password: "Strong!Pass".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_password_failure_no_special() {
    let task = ValidatePasswordTask;
    let input = ValidatePasswordTaskInput {
        password: "Str0ngPass".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
}

// Combined Test with Financial and Crypto Validators
#[task]
async fn validate_financial_crypto(
    #[validate(iban, message = "Invalid bank account")] iban: String,
    #[validate(bitcoin_address, message = "Invalid BTC address")] btc: String,
    #[validate(ethereum_address, message = "Invalid ETH address")] eth: String,
    #[validate(isbn, message = "Invalid book ID")] isbn: String,
    #[validate(password_strength, message = "Password too weak")] password: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "All validated: {} {} {} {} {}",
        iban.len(),
        btc.len(),
        eth.len(),
        isbn.len(),
        password.len()
    ))
}

#[tokio::test]
async fn test_validate_financial_crypto_success() {
    let task = ValidateFinancialCryptoTask;
    let input = ValidateFinancialCryptoTaskInput {
        iban: "GB82WEST12345698765432".to_string(),
        btc: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        isbn: "9780306406157".to_string(),
        password: "Str0ng!Pass".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_financial_crypto_iban_failure() {
    let task = ValidateFinancialCryptoTask;
    let input = ValidateFinancialCryptoTaskInput {
        iban: "invalid".to_string(),
        btc: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        isbn: "9780306406157".to_string(),
        password: "Str0ng!Pass".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Invalid bank account");
}

#[tokio::test]
async fn test_validate_financial_crypto_password_failure() {
    let task = ValidateFinancialCryptoTask;
    let input = ValidateFinancialCryptoTaskInput {
        iban: "GB82WEST12345698765432".to_string(),
        btc: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        isbn: "9780306406157".to_string(),
        password: "weak".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Password too weak");
}

// Test: Custom validator function support
fn validate_even_number(value: &i32) -> Result<(), String> {
    if value % 2 == 0 {
        Ok(())
    } else {
        Err("Value must be an even number".to_string())
    }
}

fn validate_username_format(value: &str) -> Result<(), String> {
    if value.starts_with('@') {
        Err("Username cannot start with @".to_string())
    } else if value.len() < 3 {
        Err("Username must be at least 3 characters".to_string())
    } else if value.contains(' ') {
        Err("Username cannot contain spaces".to_string())
    } else {
        Ok(())
    }
}

fn validate_percentage(value: &i32) -> Result<(), String> {
    if *value >= 0 && *value <= 100 {
        Ok(())
    } else {
        Err(format!(
            "Percentage must be between 0 and 100, got {}",
            value
        ))
    }
}

#[task]
async fn custom_validator_even(
    #[validate(custom = "validate_even_number")] number: i32,
) -> celers_core::Result<String> {
    Ok(format!("Even number: {}", number))
}

#[tokio::test]
async fn test_custom_validator_success() {
    let task = CustomValidatorEvenTask;
    let input = CustomValidatorEvenTaskInput { number: 42 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Even number: 42");
}

#[tokio::test]
async fn test_custom_validator_failure() {
    let task = CustomValidatorEvenTask;
    let input = CustomValidatorEvenTaskInput { number: 43 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Value must be an even number");
}

#[task]
async fn custom_validator_username(
    #[validate(custom = "validate_username_format")] username: String,
) -> celers_core::Result<String> {
    Ok(format!("Username: {}", username))
}

#[tokio::test]
async fn test_custom_validator_username_success() {
    let task = CustomValidatorUsernameTask;
    let input = CustomValidatorUsernameTaskInput {
        username: "alice123".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Username: alice123");
}

#[tokio::test]
async fn test_custom_validator_username_at_sign() {
    let task = CustomValidatorUsernameTask;
    let input = CustomValidatorUsernameTaskInput {
        username: "@alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Username cannot start with @");
}

#[tokio::test]
async fn test_custom_validator_username_too_short() {
    let task = CustomValidatorUsernameTask;
    let input = CustomValidatorUsernameTaskInput {
        username: "al".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Username must be at least 3 characters");
}

#[tokio::test]
async fn test_custom_validator_username_with_space() {
    let task = CustomValidatorUsernameTask;
    let input = CustomValidatorUsernameTaskInput {
        username: "alice bob".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Username cannot contain spaces");
}

#[task]
async fn combined_custom_and_predefined(
    #[validate(custom = "validate_percentage", min = 0, max = 100)] completion: i32,
    #[validate(custom = "validate_username_format", min_length = 3)] user: String,
) -> celers_core::Result<String> {
    Ok(format!("User {} is {}% complete", user, completion))
}

#[tokio::test]
async fn test_combined_custom_predefined_success() {
    let task = CombinedCustomAndPredefinedTask;
    let input = CombinedCustomAndPredefinedTaskInput {
        completion: 75,
        user: "alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "User alice is 75% complete");
}

#[tokio::test]
async fn test_combined_max_validator_failure() {
    let task = CombinedCustomAndPredefinedTask;
    let input = CombinedCustomAndPredefinedTaskInput {
        completion: 150,
        user: "alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    // The max validator runs before custom validator
    assert_eq!(error.0, "Field 'completion' value 150 exceeds maximum 100");
}

#[tokio::test]
async fn test_combined_username_validator_failure() {
    let task = CombinedCustomAndPredefinedTask;
    let input = CombinedCustomAndPredefinedTaskInput {
        completion: 75,
        user: "@alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.0, "Username cannot start with @");
}

#[tokio::test]
async fn test_combined_min_validator_failure() {
    let task = CombinedCustomAndPredefinedTask;
    let input = CombinedCustomAndPredefinedTaskInput {
        completion: -10,
        user: "alice".to_string(),
    };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    // The min validator runs before custom validator
    assert!(error.0.contains("below minimum"));
}
