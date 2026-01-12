//! Advanced validation example for celers-macros
//!
//! This example demonstrates all available validation features:
//! - Numeric range validation (min, max)
//! - String/collection length validation (min_length, max_length)
//! - Pattern validation with regex (pattern)
//! - Predefined validators (email, url, phone, uuid, ipv4, ipv6, slug, mac_address, etc.)
//! - Financial validators (iban, bitcoin_address, ethereum_address, isbn, password_strength)
//! - Combined validations on a single field
//! - Custom error messages
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

// Example 9: New predefined validators (numeric, uuid, ipv4, hexadecimal)
#[task]
async fn validate_new_types(
    #[validate(numeric, message = "PIN must contain only digits")] pin: String,
    #[validate(uuid, message = "Invalid transaction ID format")] transaction_id: String,
    #[validate(ipv4, message = "Invalid server IP address")] server_ip: String,
    #[validate(hexadecimal, message = "Hash must contain only hexadecimal characters")]
    hash: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Transaction {} from {} with PIN {}, hash: {}",
        transaction_id, server_ip, pin, hash
    ))
}

// Example 10: Network validators (ipv6, slug, mac_address)
#[task]
async fn validate_network(
    #[validate(ipv6, message = "Invalid IPv6 address")] ipv6_addr: String,
    #[validate(slug, message = "Hostname must be a valid slug")] hostname: String,
    #[validate(mac_address, message = "Invalid MAC address format")] mac_addr: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Network config: IPv6={}, Hostname={}, MAC={}",
        ipv6_addr, hostname, mac_addr
    ))
}

// Example 11: Web data validators (json, base64, color_hex)
#[task]
async fn validate_web_config(
    #[validate(json, message = "Configuration must be valid JSON")] config: String,
    #[validate(base64, message = "Image data must be valid base64")] image_data: String,
    #[validate(color_hex, message = "Theme color must be a valid hex color")] theme_color: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Web config validated: config={}, image size={}, color={}",
        config.len(),
        image_data.len(),
        theme_color
    ))
}

// Example 12: Additional practical validators (December 30, 2025)
#[task]
async fn validate_software_release(
    #[validate(semver, message = "Invalid version format")] version: String,
    #[validate(domain, message = "Invalid website domain")] website: String,
    #[validate(ascii, message = "Description must be ASCII only")] description: String,
    #[validate(lowercase, message = "Tag must be lowercase")] tag: String,
    #[validate(uppercase, message = "Build ID must be uppercase")] build_id: String,
    #[validate(time_24h, message = "Invalid time format (use HH:MM:SS)")] start_time: String,
    #[validate(date_iso8601, message = "Invalid date format (use YYYY-MM-DD)")]
    release_date: String,
    #[validate(credit_card, message = "Invalid payment card number")] payment_card: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Software release validated: {} v{} on {} at {} from {}",
        tag, version, release_date, start_time, website
    ))
}

// Example 13: Financial and Specialized Validators
#[task]
async fn validate_financial_crypto_data(
    #[validate(iban, message = "Invalid IBAN format")] bank_account: String,
    #[validate(bitcoin_address, message = "Invalid Bitcoin address")] btc_wallet: String,
    #[validate(ethereum_address, message = "Invalid Ethereum address")] eth_wallet: String,
    #[validate(isbn, message = "Invalid ISBN")] book_isbn: String,
    #[validate(
        password_strength,
        message = "Password must be at least 8 characters with uppercase, lowercase, digit, and special character"
    )]
    user_password: String,
) -> celers_core::Result<String> {
    Ok(format!(
        "Financial data validated: IBAN={}, BTC={}, ETH={}, ISBN={}, Password length={}",
        bank_account.len(),
        btc_wallet.len(),
        eth_wallet.len(),
        book_isbn,
        user_password.len()
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

    // Example 9: New predefined validators (numeric, uuid, ipv4, hexadecimal)
    println!("9. New Predefined Validators:");
    let task = ValidateNewTypesTask;
    let input = ValidateNewTypesTaskInput {
        pin: "123456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "192.168.1.100".to_string(),
        hash: "1a2b3c4d5e6f".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid PIN (contains letters)
    let input = ValidateNewTypesTaskInput {
        pin: "12a456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "192.168.1.100".to_string(),
        hash: "1a2b3c4d5e6f".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid UUID format
    let input = ValidateNewTypesTaskInput {
        pin: "123456".to_string(),
        transaction_id: "invalid-uuid".to_string(),
        server_ip: "192.168.1.100".to_string(),
        hash: "1a2b3c4d5e6f".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid IP address
    let input = ValidateNewTypesTaskInput {
        pin: "123456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "256.256.256.256".to_string(),
        hash: "1a2b3c4d5e6f".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid hexadecimal (contains 'g')
    let input = ValidateNewTypesTaskInput {
        pin: "123456".to_string(),
        transaction_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        server_ip: "192.168.1.100".to_string(),
        hash: "1a2b3g4d5e6f".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 10: Network validators (ipv6, slug, mac_address)
    println!("10. Network Validators:");
    let task = ValidateNetworkTask;
    let input = ValidateNetworkTaskInput {
        ipv6_addr: "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
        hostname: "my-server-01".to_string(),
        mac_addr: "00:1A:2B:3C:4D:5E".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid IPv6
    let input = ValidateNetworkTaskInput {
        ipv6_addr: "not-an-ipv6".to_string(),
        hostname: "my-server-01".to_string(),
        mac_addr: "00:1A:2B:3C:4D:5E".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid slug (contains uppercase)
    let input = ValidateNetworkTaskInput {
        ipv6_addr: "2001:db8::1".to_string(),
        hostname: "My-Server-01".to_string(),
        mac_addr: "00:1A:2B:3C:4D:5E".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid MAC address
    let input = ValidateNetworkTaskInput {
        ipv6_addr: "2001:db8::1".to_string(),
        hostname: "my-server-01".to_string(),
        mac_addr: "invalid-mac".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 11: Web data validators (json, base64, color_hex)
    println!("11. Web Data Validators:");
    let task = ValidateWebConfigTask;
    let input = ValidateWebConfigTaskInput {
        config: r#"{"theme": "dark", "language": "en"}"#.to_string(),
        image_data: "SGVsbG8gV29ybGQ=".to_string(), // "Hello World" in base64
        theme_color: "#007BFF".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid JSON
    let input = ValidateWebConfigTaskInput {
        config: "not-json".to_string(),
        image_data: "SGVsbG8gV29ybGQ=".to_string(),
        theme_color: "#007BFF".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid base64
    let input = ValidateWebConfigTaskInput {
        config: r#"{"theme": "dark"}"#.to_string(),
        image_data: "Invalid@Data!".to_string(),
        theme_color: "#007BFF".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid color (no # prefix)
    let input = ValidateWebConfigTaskInput {
        config: r#"{"theme": "dark"}"#.to_string(),
        image_data: "SGVsbG8gV29ybGQ=".to_string(),
        theme_color: "FF5733".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 12: Additional practical validators (December 30, 2025)
    println!("12. Additional Practical Validators:");
    let task = ValidateSoftwareReleaseTask;
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "example.com".to_string(),
        description: "Major release with new features".to_string(),
        tag: "release-candidate".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid semver
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1".to_string(),
        website: "example.com".to_string(),
        description: "Major release".to_string(),
        tag: "release".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid domain
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "not_a_domain".to_string(),
        description: "Major release".to_string(),
        tag: "release".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with non-ASCII description
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "example.com".to_string(),
        description: "Major release with 日本語".to_string(),
        tag: "release".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with uppercase in tag (should be lowercase)
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "example.com".to_string(),
        description: "Major release".to_string(),
        tag: "RELEASE".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid time
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "example.com".to_string(),
        description: "Major release".to_string(),
        tag: "release".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "25:00:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid date
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "example.com".to_string(),
        description: "Major release".to_string(),
        tag: "release".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "12/30/2025".to_string(),
        payment_card: "4532015112830366".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid credit card
    let input = ValidateSoftwareReleaseTaskInput {
        version: "2.1.3".to_string(),
        website: "example.com".to_string(),
        description: "Major release".to_string(),
        tag: "release".to_string(),
        build_id: "BUILD123".to_string(),
        start_time: "09:30:00".to_string(),
        release_date: "2025-12-30".to_string(),
        payment_card: "1234567890123456".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    // Example 13: Financial and Specialized Validators
    println!("13. Financial and Crypto Validators:");
    let task = ValidateFinancialCryptoDataTask;
    let input = ValidateFinancialCryptoDataTaskInput {
        bank_account: "GB82WEST12345698765432".to_string(),
        btc_wallet: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth_wallet: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        book_isbn: "978-0-306-40615-7".to_string(),
        user_password: "Str0ng!Pass".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid IBAN
    let input = ValidateFinancialCryptoDataTaskInput {
        bank_account: "invalid-iban".to_string(),
        btc_wallet: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth_wallet: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        book_isbn: "9780306406157".to_string(),
        user_password: "Str0ng!Pass".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid Bitcoin address
    let input = ValidateFinancialCryptoDataTaskInput {
        bank_account: "GB82WEST12345698765432".to_string(),
        btc_wallet: "not-a-bitcoin-address".to_string(),
        eth_wallet: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        book_isbn: "9780306406157".to_string(),
        user_password: "Str0ng!Pass".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid Ethereum address
    let input = ValidateFinancialCryptoDataTaskInput {
        bank_account: "GB82WEST12345698765432".to_string(),
        btc_wallet: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth_wallet: "invalid-eth-address".to_string(),
        book_isbn: "9780306406157".to_string(),
        user_password: "Str0ng!Pass".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with invalid ISBN
    let input = ValidateFinancialCryptoDataTaskInput {
        bank_account: "GB82WEST12345698765432".to_string(),
        btc_wallet: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth_wallet: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        book_isbn: "123456".to_string(),
        user_password: "Str0ng!Pass".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }

    // Test with weak password
    let input = ValidateFinancialCryptoDataTaskInput {
        bank_account: "GB82WEST12345698765432".to_string(),
        btc_wallet: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(),
        eth_wallet: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
        book_isbn: "9780306406157".to_string(),
        user_password: "weak".to_string(),
    };
    match task.execute(input).await {
        Ok(result) => println!("   ✓ {}", result),
        Err(e) => println!("   ✗ Error: {}", e),
    }
    println!();

    println!("=== All examples completed ===");

    Ok(())
}
