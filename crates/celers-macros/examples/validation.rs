//! Advanced validation example for celers-macros
//!
//! This example demonstrates all available validation features:
//! - Numeric range validation (min, max)
//! - String/collection length validation (min_length, max_length)
//! - Pattern validation with regex (pattern)
//! - Combined validations on a single field
//!
//! To run this example:
//! ```bash
//! cargo run --example validation
//! ```

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
        #[allow(dead_code)]
        fn name(&self) -> &str;
    }
}

use celers_core::Task;

// Example 1: Numeric range validation
#[task]
async fn validate_age(#[validate(min = 0, max = 120)] age: i32) -> celers_core::Result<String> {
    Ok(format!("Valid age: {}", age))
}

// Example 2: String length validation
#[task]
async fn validate_username(
    #[validate(min_length = 3, max_length = 20)] username: String,
) -> celers_core::Result<String> {
    Ok(format!("Username: {}", username))
}

// Example 3: Email validation with regex pattern
#[task]
async fn validate_email(
    #[validate(pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")] email: String,
) -> celers_core::Result<String> {
    Ok(format!("Email registered: {}", email))
}

// Example 4: Phone number validation with regex pattern
#[task]
async fn validate_phone(
    #[validate(pattern = r"^\+?[1-9]\d{1,14}$")] phone: String,
) -> celers_core::Result<String> {
    Ok(format!("Phone number: {}", phone))
}

// Example 5: URL validation with regex pattern
#[task]
async fn validate_url(
    #[validate(pattern = r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$")] url: String,
) -> celers_core::Result<String> {
    Ok(format!("Valid URL: {}", url))
}

// Example 6: Combined validation (length + pattern)
#[task]
async fn create_user(
    #[validate(min_length = 3, max_length = 20, pattern = r"^[a-zA-Z0-9_]+$")] username: String,
    #[validate(min = 13, max = 120)] age: i32,
    #[validate(pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")] email: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "User created: {} (age: {}, email: {})",
        username, age, email
    ))
}

// Example 7: Multiple fields with different validations
#[task]
async fn register_product(
    #[validate(min_length = 5, max_length = 100)] name: String,
    #[validate(min = 0)] price: i32,
    #[validate(min_length = 10, max_length = 500)] description: String,
    #[validate(pattern = r"^[A-Z]{3}-\d{4}$")] sku: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Product registered: {} (SKU: {}, Price: ${}, Desc: {})",
        name, sku, price, description
    ))
}

// Example 8: Custom error messages for better UX
#[task]
async fn create_account(
    #[validate(
        min = 18,
        message = "You must be at least 18 years old to create an account"
    )]
    age: i32,
    #[validate(
        min_length = 3,
        max_length = 20,
        message = "Username must be between 3 and 20 characters"
    )]
    username: String,
    #[validate(
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        message = "Please provide a valid email address"
    )]
    email: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Account created for {} (age: {}, email: {})",
        username, age, email
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CeleRS Validation Examples ===\n");

    // Example 1: Age validation
    println!("1. Age Validation:");
    let task = ValidateAgeTask;
    let input = ValidateAgeTaskInput { age: 25 };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test invalid age
    let input = ValidateAgeTaskInput { age: 150 };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 2: Username validation
    println!("2. Username Validation:");
    let task = ValidateUsernameTask;
    let input = ValidateUsernameTaskInput {
        username: "alice".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test invalid username (too short)
    let input = ValidateUsernameTaskInput {
        username: "ab".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 3: Email validation
    println!("3. Email Validation:");
    let task = ValidateEmailTask;
    let input = ValidateEmailTaskInput {
        email: "user@example.com".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test invalid email
    let input = ValidateEmailTaskInput {
        email: "not-an-email".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 4: Phone validation
    println!("4. Phone Number Validation:");
    let task = ValidatePhoneTask;
    let input = ValidatePhoneTaskInput {
        phone: "+1234567890".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test invalid phone
    let input = ValidatePhoneTaskInput {
        phone: "abc123".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 5: URL validation
    println!("5. URL Validation:");
    let task = ValidateUrlTask;
    let input = ValidateUrlTaskInput {
        url: "https://example.com/path".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test invalid URL
    let input = ValidateUrlTaskInput {
        url: "not-a-url".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 6: Combined validation
    println!("6. User Registration (Combined Validation):");
    let task = CreateUserTask;
    let input = CreateUserTaskInput {
        username: "alice_2024".to_string(),
        age: 25,
        email: "alice@example.com".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid username (contains special character)
    let input = CreateUserTaskInput {
        username: "alice-2024".to_string(),
        age: 25,
        email: "alice@example.com".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 7: Product registration with multiple validations
    println!("7. Product Registration (Multiple Field Validation):");
    let task = RegisterProductTask;
    let input = RegisterProductTaskInput {
        name: "Awesome Product".to_string(),
        price: 2999,
        description: "This is a great product that does amazing things!".to_string(),
        sku: "PRD-1234".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid SKU format
    let input = RegisterProductTaskInput {
        name: "Another Product".to_string(),
        price: 1999,
        description: "Another great product description here".to_string(),
        sku: "INVALID".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 8: Custom error messages
    println!("8. Custom Error Messages:");
    let task = CreateAccountTask;
    let input = CreateAccountTaskInput {
        age: 25,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid age (custom message)
    let input = CreateAccountTaskInput {
        age: 15,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid username (custom message)
    let input = CreateAccountTaskInput {
        age: 25,
        username: "ab".to_string(),
        email: "alice@example.com".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid email (custom message)
    let input = CreateAccountTaskInput {
        age: 25,
        username: "alice".to_string(),
        email: "not-an-email".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    println!("=== All examples completed ===");

    Ok(())
}
