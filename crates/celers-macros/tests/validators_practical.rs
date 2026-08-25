//! "Additional Practical Validators" integration tests for celers-macros:
//! semver, domain, ascii, case, time/date, credit card, and combined
//! validators.
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
    assert_eq!(error.message(), "Invalid version format");
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
    assert_eq!(error.message(), "Invalid domain name");
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
    assert_eq!(error.message(), "Must be ASCII only");
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
    assert_eq!(error.message(), "Must be lowercase");
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
    assert_eq!(error.message(), "Must be uppercase");
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
    assert_eq!(error.message(), "Invalid time format");
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
    assert_eq!(error.message(), "Invalid date format");
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
    assert_eq!(error.message(), "Invalid date format");
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
    assert_eq!(error.message(), "Invalid credit card number");
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
    assert_eq!(error.message(), "Invalid version");
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
    assert_eq!(error.message(), "Invalid card");
}
