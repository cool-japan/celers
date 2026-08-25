//! Identifier and financial validator integration tests for celers-macros:
//! IBAN, Bitcoin/Ethereum addresses, ISBN, password strength, custom
//! validator functions (including the module-qualified custom-validator-path
//! regression test).
//!
//! Split out of `tests/integration_test.rs` to keep both files under the
//! COOLJAPAN 2000-line-per-file policy (see that file's module doc for why
//! this suite uses the real `celers_core` crate rather than a hand-rolled
//! mock -- the same rationale applies here).

use celers_macros::task;

mod common;
use celers_core::Task;
use common::CelersErrorMessage;

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
    assert_eq!(error.message(), "Invalid Bitcoin address");
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
    assert_eq!(error.message(), "Invalid Ethereum address");
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
    assert_eq!(error.message(), "Invalid ISBN");
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
    assert_eq!(error.message(), "Weak password");
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
    assert_eq!(error.message(), "Weak password");
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
    assert_eq!(error.message(), "Invalid bank account");
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
    assert_eq!(error.message(), "Password too weak");
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
    assert_eq!(error.message(), "Value must be an even number");
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
    assert_eq!(error.message(), "Username cannot start with @");
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
    assert_eq!(error.message(), "Username must be at least 3 characters");
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
    assert_eq!(error.message(), "Username cannot contain spaces");
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
    assert_eq!(
        error.message(),
        "Field 'completion' value 150 exceeds maximum 100"
    );
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
    assert_eq!(error.message(), "Username cannot start with @");
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
    assert!(error.message().contains("below minimum"));
}

// Test: Module-qualified custom validator path (regression test).
//
// The custom-validator codegen used to build the call target with
// `syn::Ident::new(custom_fn, Span::call_site())`, which *panics*
// ("... is not a valid Ident") for any value containing `::` -- so
// `#[validate(custom = "some_mod::check")]` could not be used at all; the
// whole proc-macro invocation aborted with "proc macro panicked" rather
// than compiling, or even producing an ordinary compile error. The fix
// parses `custom` as a `syn::Path` while parsing the attribute, which
// accepts module-qualified paths (and turns a genuinely invalid value into
// a normal compile error instead of a panic).
mod validators {
    pub fn check_even(value: &i32) -> Result<(), String> {
        if value % 2 == 0 {
            Ok(())
        } else {
            Err("value must be even".to_string())
        }
    }
}

#[task]
async fn module_qualified_custom_validator(
    #[validate(custom = "validators::check_even")] value: i32,
) -> celers_core::Result<String> {
    Ok(format!("Value: {}", value))
}

#[tokio::test]
async fn test_module_qualified_custom_validator_success() {
    let task = ModuleQualifiedCustomValidatorTask;
    let input = ModuleQualifiedCustomValidatorTaskInput { value: 4 };
    let result = task.execute(input).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Value: 4");
}

#[tokio::test]
async fn test_module_qualified_custom_validator_failure() {
    let task = ModuleQualifiedCustomValidatorTask;
    let input = ModuleQualifiedCustomValidatorTaskInput { value: 3 };
    let result = task.execute(input).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.message(), "value must be even");
}
