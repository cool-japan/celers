//! Geographic and locale validator integration tests for celers-macros:
//! latitude, longitude, ISO country/language codes, US ZIP, Canadian postal
//! code, and combined validators.
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
    assert_eq!(error.message(), "Invalid latitude");
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
    assert_eq!(error.message(), "Invalid country code");
}
