//! Procedural macros for CeleRS
//!
//! This crate provides procedural macros to simplify task definition:
//! - `#[task]` - Attribute macro for converting async functions into tasks
//! - `#[derive(Task)]` - Derive macro for implementing the Task trait
//!
//! # Overview
//!
//! The `#[task]` macro eliminates boilerplate when defining tasks by automatically:
//! - Generating a Task struct that implements the `Task` trait
//! - Creating an Input struct from function parameters with serde support
//! - Extracting the Output type from the function's return type
//! - Adding configuration methods for timeout, priority, and max_retries
//!
//! # Examples
//!
//! ## Basic Task
//!
//! ```ignore
//! use celers_macros::task;
//! use celers_core::Result;
//!
//! #[task]
//! async fn add_numbers(a: i32, b: i32) -> Result<i32> {
//!     Ok(a + b)
//! }
//!
//! // Generated:
//! // - AddNumbersTask: impl Task
//! // - AddNumbersTaskInput { a: i32, b: i32 }
//! // - AddNumbersTaskOutput = i32
//!
//! // Usage:
//! let task = AddNumbersTask;
//! let input = AddNumbersTaskInput { a: 5, b: 3 };
//! let result = task.execute(input).await?;
//! assert_eq!(result, 8);
//! ```
//!
//! ## Task with Configuration
//!
//! ```ignore
//! #[task(
//!     name = "tasks.process_data",
//!     timeout = 60,
//!     priority = 10,
//!     max_retries = 3
//! )]
//! async fn process_data(data: String) -> Result<String> {
//!     // Process the data
//!     Ok(format!("Processed: {}", data))
//! }
//!
//! // Access configuration:
//! let task = ProcessDataTask;
//! assert_eq!(task.name(), "tasks.process_data");
//! assert_eq!(task.timeout(), Some(60));
//! assert_eq!(task.priority(), Some(10));
//! assert_eq!(task.max_retries(), Some(3));
//! ```
//!
//! ## Task with Optional Parameters
//!
//! ```ignore
//! #[task]
//! async fn send_notification(
//!     user_id: u64,
//!     message: String,
//!     email: Option<String>,
//!     sms: Option<String>,
//! ) -> Result<bool> {
//!     // Optional fields get automatic serde support
//!     // with skip_serializing_if and default
//!     Ok(true)
//! }
//!
//! // Input struct automatically derives Default if all fields are optional
//! let input = SendNotificationTaskInput {
//!     user_id: 123,
//!     message: "Hello".to_string(),
//!     email: Some("user@example.com".to_string()),
//!     sms: None,  // Optional fields can be omitted
//! };
//! ```
//!
//! ## Task with Generic Parameters
//!
//! ```ignore
//! #[task]
//! async fn process_items<T>(items: Vec<T>) -> Result<usize>
//! where
//!     T: Send + Clone,
//! {
//!     Ok(items.len())
//! }
//!
//! // Use with specific type:
//! let task = ProcessItemsTask::<String>;
//! let input = ProcessItemsTaskInput {
//!     items: vec!["a".to_string(), "b".to_string()],
//! };
//! ```
//!
//! ## Task with Parameter Validation
//!
//! ```ignore
//! #[task]
//! async fn register_user(
//!     #[validate(min = 18, max = 120)]
//!     age: i32,
//!     #[validate(min_length = 3, max_length = 50)]
//!     username: String,
//!     #[validate(pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")]
//!     email: String,
//! ) -> Result<String> {
//!     // Validation happens automatically before execution
//!     Ok(format!("User {} registered with email {}", username, email))
//! }
//!
//! // Valid input succeeds:
//! let input = RegisterUserTaskInput {
//!     age: 25,
//!     username: "alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! let result = task.execute(input).await?; // Ok
//!
//! // Invalid input returns error:
//! let input = RegisterUserTaskInput {
//!     age: 15, // Below minimum
//!     username: "alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! let result = task.execute(input).await; // Err: "Field 'age' value 15 is below minimum 18"
//! ```
//!
//! ## Task with Custom Validation Messages
//!
//! ```ignore
//! #[task]
//! async fn create_account(
//!     #[validate(min = 18, message = "You must be at least 18 years old to create an account")]
//!     age: i32,
//!     #[validate(min_length = 3, max_length = 20, message = "Username must be between 3 and 20 characters")]
//!     username: String,
//!     #[validate(pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", message = "Please provide a valid email address")]
//!     email: String,
//! ) -> Result<String> {
//!     Ok(format!("Account created for {}", username))
//! }
//!
//! // Invalid input returns custom error message:
//! let input = CreateAccountTaskInput {
//!     age: 15,
//!     username: "alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! let result = task.execute(input).await; // Err: "You must be at least 18 years old to create an account"
//! ```
//!
//! ## Task with Predefined Validators
//!
//! ```ignore
//! #[task]
//! async fn register_user(
//!     #[validate(email, message = "Please enter a valid email")]
//!     email: String,
//!     #[validate(phone)]
//!     phone: String,
//!     #[validate(url)]
//!     website: String,
//!     #[validate(positive)]
//!     age: i32,
//!     #[validate(alphanumeric)]
//!     username: String,
//! ) -> Result<String> {
//!     Ok(format!("User {} registered", username))
//! }
//!
//! // Predefined validators provide convenient shortcuts:
//! let input = RegisterUserTaskInput {
//!     email: "user@example.com".to_string(),  // Must be valid email
//!     phone: "+1234567890".to_string(),        // Must be valid phone (10-15 digits)
//!     website: "https://example.com".to_string(), // Must be valid URL
//!     age: 25,                                  // Must be positive (> 0)
//!     username: "user123".to_string(),         // Must be alphanumeric
//! };
//! ```
//!
//! # Generated Code Structure
//!
//! For a function named `my_task`, the macro generates:
//!
//! ```ignore
//! // Input struct with serde support
//! #[derive(Serialize, Deserialize, Debug, Clone)]
//! pub struct MyTaskTaskInput {
//!     // Function parameters become struct fields
//! }
//!
//! // Output type alias
//! pub type MyTaskTaskOutput = /* extracted from Result<T> */;
//!
//! // Task implementation
//! #[derive(Default)]
//! pub struct MyTaskTask;
//!
//! #[async_trait::async_trait]
//! impl Task for MyTaskTask {
//!     type Input = MyTaskTaskInput;
//!     type Output = MyTaskTaskOutput;
//!
//!     async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
//!         // Original function body
//!     }
//!
//!     fn name(&self) -> &str {
//!         "my_task"  // or custom name from attribute
//!     }
//! }
//!
//! // Configuration methods (if specified)
//! impl MyTaskTask {
//!     pub fn timeout(&self) -> Option<u64> { /* ... */ }
//!     pub fn priority(&self) -> Option<i32> { /* ... */ }
//!     pub fn max_retries(&self) -> Option<u32> { /* ... */ }
//! }
//! ```
//!
//! # Attribute Parameters
//!
//! ## Task-level Attributes (on `#[task(...)]`)
//!
//! - `name` - Custom task name (default: function name)
//! - `timeout` - Task timeout in seconds (must be > 0)
//! - `priority` - Task priority as i32 (higher = more important)
//! - `max_retries` - Maximum retry attempts as u32
//!
//! ## Parameter-level Attributes (on function parameters)
//!
//! Use `#[validate(...)]` to add validation rules to parameters:
//!
//! ### Numeric Validation
//! - `min` - Minimum value (inclusive) for numeric types
//! - `max` - Maximum value (inclusive) for numeric types
//!
//! Example: `#[validate(min = 0, max = 100)] score: i32`
//!
//! ### String/Collection Length Validation
//! - `min_length` - Minimum length for strings or collections
//! - `max_length` - Maximum length for strings or collections
//!
//! Example: `#[validate(min_length = 3, max_length = 50)] username: String`
//!
//! ### Pattern Validation
//! - `pattern` - Regex pattern for string validation
//!
//! Example: `#[validate(pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")] email: String`
//!
//! **Note:** Pattern validation requires the `regex` crate to be added to your dependencies:
//! ```toml
//! [dependencies]
//! regex = "1.11"
//! ```
//!
//! ### Predefined Validation Shortcuts
//!
//! For common validation patterns, you can use convenient predefined validators:
//!
//! #### String Validators
//! - `email` - Validates email addresses (shorthand for email regex pattern)
//! - `url` - Validates HTTP/HTTPS URLs
//! - `phone` - Validates phone numbers (10-15 digits, optional + prefix)
//! - `not_empty` - Ensures string is not empty
//! - `alphabetic` - Ensures string contains only alphabetic characters
//! - `alphanumeric` - Ensures string contains only alphanumeric characters
//! - `numeric` - Ensures string contains only numeric characters (0-9)
//! - `hexadecimal` - Ensures string contains only hexadecimal characters (0-9, a-f, A-F)
//! - `uuid` - Validates UUID format (8-4-4-4-12 hex digits with hyphens)
//! - `ipv4` - Validates IPv4 address format (e.g., 192.168.1.1)
//! - `ipv6` - Validates IPv6 address format (e.g., 2001:0db8:85a3:0000:0000:8a2e:0370:7334)
//! - `slug` - Validates URL-friendly slug (lowercase letters, numbers, hyphens)
//! - `mac_address` - Validates MAC address format (e.g., 00:1A:2B:3C:4D:5E)
//! - `json` - Validates JSON string format
//! - `base64` - Validates base64-encoded strings
//! - `color_hex` - Validates hex color codes (#RGB or #RRGGBB)
//! - `semver` - Validates semantic versioning format (e.g., 1.2.3, 2.0.0-alpha)
//! - `domain` - Validates domain name format (e.g., example.com, subdomain.example.co.uk)
//! - `ascii` - Ensures string contains only ASCII characters
//! - `lowercase` - Ensures string contains only lowercase characters
//! - `uppercase` - Ensures string contains only uppercase characters
//! - `time_24h` - Validates 24-hour time format (HH:MM or HH:MM:SS)
//! - `date_iso8601` - Validates ISO 8601 date format (YYYY-MM-DD)
//! - `credit_card` - Validates credit card number using Luhn algorithm
//!
//! #### Geographic and Locale Validators
//! - `latitude` - Validates latitude coordinates (-90 to 90)
//! - `longitude` - Validates longitude coordinates (-180 to 180)
//! - `iso_country` - Validates ISO 3166-1 alpha-2 country codes (e.g., US, CA, GB)
//! - `iso_language` - Validates ISO 639-1 language codes (e.g., en, es, fr)
//! - `us_zip` - Validates US ZIP codes (12345 or 12345-6789)
//! - `ca_postal` - Validates Canadian postal codes (A1A 1A1 or A1A1A1)
//!
//! #### Numeric Validators
//! - `positive` - Ensures number is greater than 0
//! - `negative` - Ensures number is less than 0
//!
//! #### Financial and Specialized Validators
//! - `iban` - Validates International Bank Account Number (IBAN) format
//! - `bitcoin_address` - Validates Bitcoin addresses (P2PKH, P2SH, Bech32 formats)
//! - `ethereum_address` - Validates Ethereum addresses (0x + 40 hex characters)
//! - `isbn` - Validates ISBN-10 or ISBN-13 with checksum validation
//! - `password_strength` - Validates strong passwords (8+ chars with uppercase, lowercase, digit, special char)
//!
//! Examples:
//! ```ignore
//! #[task]
//! async fn register(
//!     #[validate(email)] email: String,
//!     #[validate(url)] website: String,
//!     #[validate(phone)] contact: String,
//!     #[validate(not_empty)] name: String,
//!     #[validate(positive)] quantity: i32,
//!     #[validate(alphabetic)] first_name: String,
//!     #[validate(alphanumeric)] username: String,
//!     #[validate(numeric)] pin: String,
//!     #[validate(hexadecimal)] hash: String,
//!     #[validate(uuid)] transaction_id: String,
//!     #[validate(ipv4)] server_ip: String,
//!     #[validate(ipv6)] ipv6_address: String,
//!     #[validate(slug)] article_slug: String,
//!     #[validate(mac_address)] device_mac: String,
//!     #[validate(json)] config_json: String,
//!     #[validate(base64)] encoded_data: String,
//!     #[validate(color_hex)] theme_color: String,
//!     #[validate(semver)] version: String,
//!     #[validate(domain)] website_domain: String,
//!     #[validate(ascii)] ascii_code: String,
//!     #[validate(lowercase)] lowercase_tag: String,
//!     #[validate(uppercase)] uppercase_code: String,
//!     #[validate(time_24h)] meeting_time: String,
//!     #[validate(date_iso8601)] birth_date: String,
//!     #[validate(credit_card)] card_number: String,
//!     #[validate(latitude)] location_lat: String,
//!     #[validate(longitude)] location_lon: String,
//!     #[validate(iso_country)] country_code: String,
//!     #[validate(iso_language)] language_code: String,
//!     #[validate(us_zip)] zip_code: String,
//!     #[validate(ca_postal)] postal_code: String,
//!     #[validate(iban)] bank_account: String,
//!     #[validate(bitcoin_address)] btc_address: String,
//!     #[validate(ethereum_address)] eth_address: String,
//!     #[validate(isbn)] book_id: String,
//!     #[validate(password_strength)] password: String,
//! ) -> Result<String> {
//!     Ok(format!("Registered {}", email))
//! }
//! ```
//!
//! **Note:** Predefined validators require dependencies:
//! - `email`, `url`, `phone`, `uuid`, `ipv4`, `ipv6`, `slug`, `mac_address`, `base64`, `color_hex`, `semver`, `domain`, `time_24h`, `date_iso8601`, `iso_country`, `iso_language`, `us_zip`, `ca_postal`, `iban`, `bitcoin_address`, `ethereum_address` require the `regex` crate
//! - `json` requires the `serde_json` crate (usually already in dependencies)
//! - `credit_card`, `latitude`, `longitude`, `isbn`, `password_strength` use inline validation (no extra dependencies)
//!
//! **Performance Note:** All regex-based validators use `LazyLock` (Rust 1.80+) to compile patterns once and cache them,
//! providing significant performance improvements over repeated regex compilation.
//!
//! ### Custom Error Messages
//! - `message` - Custom error message for validation failures
//!
//! All validation rules can include a custom `message` parameter to provide clearer, domain-specific error messages:
//!
//! Example: `#[validate(min = 18, max = 120, message = "Age must be between 18 and 120")] age: i32`
//!
//! ### Custom Validator Functions
//! - `custom = "function_name"` - Call a custom validation function
//!
//! For complex validation logic not covered by predefined validators, you can specify a custom function.
//! The function must have the signature `fn(&T) -> Result<(), String>` where `T` is the field type.
//! For `String` fields, you can use either `&String` or the more idiomatic `&str`.
//!
//! ```ignore
//! // Define a custom validator function
//! fn validate_even_number(value: &i32) -> Result<(), String> {
//!     if value % 2 == 0 {
//!         Ok(())
//!     } else {
//!         Err("Value must be an even number".to_string())
//!     }
//! }
//!
//! fn validate_username(value: &str) -> Result<(), String> {
//!     if value.starts_with('@') {
//!         Err("Username cannot start with @".to_string())
//!     } else if value.len() < 3 {
//!         Err("Username must be at least 3 characters".to_string())
//!     } else {
//!         Ok(())
//!     }
//! }
//!
//! // Use in task parameter
//! #[task]
//! async fn process_data(
//!     #[validate(custom = "validate_even_number")]
//!     count: i32,
//!     #[validate(custom = "validate_username", min_length = 3)]
//!     username: String,
//! ) -> Result<String> {
//!     Ok(format!("Processing {} items for {}", count, username))
//! }
//! ```
//!
//! **Note:** Custom validators can be combined with predefined validators. Predefined validators
//! (like `min`, `max`, `min_length`) run first, followed by custom validators.
//!
//! # Limitations
//!
//! - Functions must be `async`
//! - Return type must be wrapped in `Result<T>`
//! - Generic parameters with complex trait bounds (HRTBs) may require careful handling
//!
//! # Macro Expansion Guide
//!
//! Understanding what code the macros generate can help with debugging and IDE support.
//!
//! ## Using cargo-expand
//!
//! To see the expanded macro output, install and use `cargo-expand`:
//!
//! ```bash
//! cargo install cargo-expand
//! cargo expand --lib  # Expand library code
//! cargo expand --example basic_task  # Expand example code
//! ```
//!
//! ## Expansion Examples
//!
//! ### Basic Task Expansion
//!
//! **Input:**
//! ```ignore
//! #[task]
//! async fn add(a: i32, b: i32) -> Result<i32> {
//!     Ok(a + b)
//! }
//! ```
//!
//! **Expands to (simplified):**
//! ```ignore
//! #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
//! pub struct AddTaskInput {
//!     pub a: i32,
//!     pub b: i32,
//! }
//!
//! pub type AddTaskOutput = i32;
//!
//! #[derive(Default)]
//! pub struct AddTask;
//!
//! #[async_trait::async_trait]
//! impl celers_core::Task for AddTask {
//!     type Input = AddTaskInput;
//!     type Output = i32;
//!
//!     async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
//!         let AddTaskInput { a, b } = input;
//!         { Ok(a + b) }
//!     }
//!
//!     fn name(&self) -> &str {
//!         "add"
//!     }
//! }
//!
//! impl AddTask {}
//! ```
//!
//! ### Task with Configuration
//!
//! **Input:**
//! ```ignore
//! #[task(name = "math.multiply", timeout = 60, priority = 10)]
//! async fn multiply(x: i32, y: i32) -> Result<i32> {
//!     Ok(x * y)
//! }
//! ```
//!
//! **Expands to (configuration methods added):**
//! ```ignore
//! impl MultiplyTask {
//!     pub fn timeout(&self) -> Option<u64> {
//!         Some(60)
//!     }
//!     pub fn priority(&self) -> Option<i32> {
//!         Some(10)
//!     }
//! }
//! ```
//!
//! ### Task with Optional Parameters
//!
//! **Input:**
//! ```ignore
//! #[task]
//! async fn notify(user: String, email: Option<String>) -> Result<bool> {
//!     Ok(true)
//! }
//! ```
//!
//! **Expands to (with serde attributes):**
//! ```ignore
//! #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
//! pub struct NotifyTaskInput {
//!     pub user: String,
//!     #[serde(skip_serializing_if = "Option::is_none", default)]
//!     pub email: Option<String>,
//! }
//! ```
//!
//! ### Generic Task Expansion
//!
//! **Input:**
//! ```ignore
//! #[task]
//! async fn process<T>(items: Vec<T>) -> Result<usize>
//! where
//!     T: Send + Clone,
//! {
//!     Ok(items.len())
//! }
//! ```
//!
//! **Expands to (with generics):**
//! ```ignore
//! #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
//! pub struct ProcessTaskInput<T>
//! where
//!     T: Send + Clone,
//! {
//!     pub items: Vec<T>,
//! }
//!
//! pub type ProcessTaskOutput<T> = usize;
//!
//! pub struct ProcessTask<T>
//! where
//!     T: Send + Clone;
//!
//! impl<T> Default for ProcessTask<T>
//! where
//!     T: Send + Clone,
//! {
//!     fn default() -> Self {
//!         ProcessTask
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl<T> celers_core::Task for ProcessTask<T>
//! where
//!     T: Send + Clone,
//! {
//!     type Input = ProcessTaskInput<T>;
//!     type Output = usize;
//!
//!     async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
//!         let ProcessTaskInput { items } = input;
//!         { Ok(items.len()) }
//!     }
//!
//!     fn name(&self) -> &str {
//!         "process"
//!     }
//! }
//! ```
//!
//! ### Validation Expansion
//!
//! **Input:**
//! ```ignore
//! #[task]
//! async fn validate_age(#[validate(min = 0, max = 120)] age: i32) -> Result<String> {
//!     Ok(format!("Age: {}", age))
//! }
//! ```
//!
//! **Expands to (validation checks added):**
//! ```ignore
//! impl celers_core::Task for ValidateAgeTask {
//!     async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
//!         let ValidateAgeTaskInput { age } = input;
//!
//!         // Validation checks are inserted here
//!         if (age as i64) < 0 {
//!             return Err(celers_core::CelersError(format!(
//!                 "Field 'age' value {} is below minimum 0",
//!                 age
//!             )));
//!         }
//!         if (age as i64) > 120 {
//!             return Err(celers_core::CelersError(format!(
//!                 "Field 'age' value {} exceeds maximum 120",
//!                 age
//!             )));
//!         }
//!
//!         // Original function body executes after validation
//!         { Ok(format!("Age: {}", age)) }
//!     }
//! }
//! ```
//!
//! ## Best Practices
//!
//! 1. **Use descriptive function names** - They become task names (e.g., `process_payment` → `"process_payment"`)
//! 2. **Leverage optional parameters** - Use `Option<T>` for fields that may be omitted
//! 3. **Configure timeouts and retries** - Use attributes to set task-specific configurations
//! 4. **Keep functions focused** - Each task should do one thing well
//! 5. **Add validation to parameters** - Use `#[validate(...)]` to enforce constraints early
//! 6. **Use cargo-expand** - Inspect generated code when debugging macro issues
//!
//! ## Troubleshooting
//!
//! ### "the #[task] attribute can only be applied to async functions"
//! **Solution:** Add the `async` keyword before `fn`
//!
//! ### "timeout must be greater than 0 seconds"
//! **Solution:** Use a positive timeout value: `#[task(timeout = 30)]`
//!
//! ### "duplicate 'name' attribute"
//! **Solution:** Each attribute can only be specified once
//!
//! ### "task name cannot be empty"
//! **Solution:** Provide a non-empty string: `#[task(name = "my_task")]`
//!
//! ### Serde serialization errors
//! **Solution:** Ensure all types in the function signature implement `Serialize` and `Deserialize`

mod derive_macro;
mod task_attr;
mod task_macro;
mod validation;

use proc_macro::TokenStream;

/// Attribute macro for marking async functions as tasks
///
/// # Attributes
///
/// - `name`: Custom task name (default: function name)
/// - `timeout`: Task timeout in seconds
/// - `priority`: Task priority (higher value = higher priority)
/// - `max_retries`: Maximum number of retry attempts
///
/// # Examples
///
/// Basic usage:
/// ```ignore
/// #[task]
/// async fn add_numbers(a: i32, b: i32) -> Result<i32> {
///     Ok(a + b)
/// }
/// ```
///
/// With custom configuration:
/// ```ignore
/// #[task(name = "tasks.add", timeout = 30, priority = 10, max_retries = 3)]
/// async fn add_numbers(a: i32, b: i32) -> Result<i32> {
///     Ok(a + b)
/// }
/// ```
///
/// This generates:
/// - `AddNumbersTask`: struct implementing `Task` trait
/// - `AddNumbersTaskInput`: input struct with task parameters
/// - `AddNumbersTaskOutput`: type alias for the return type
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task_macro::task_macro_impl(attr, item)
}

/// Derive macro for Task trait
///
/// Generates a Task implementation for a struct with configurable types.
///
/// # Attributes
///
/// - `#[task(input = "TypeName")]` - Specify the Input type
/// - `#[task(output = "TypeName")]` - Specify the Output type
/// - `#[task(name = "task.name")]` - Specify the task name
///
/// # Examples
///
/// ```ignore
/// #[derive(Task)]
/// #[task(input = "MyInput", output = "MyOutput", name = "my_task")]
/// struct MyTask {
///     // Task fields
/// }
///
/// // Implement the execute method separately:
/// #[async_trait::async_trait]
/// impl MyTask {
///     async fn execute_impl(&self, input: MyInput) -> celers_core::Result<MyOutput> {
///         // Task logic
///         Ok(MyOutput { /* ... */ })
///     }
/// }
/// ```
#[proc_macro_derive(Task, attributes(task))]
pub fn derive_task(input: TokenStream) -> TokenStream {
    derive_macro::derive_task_impl(input)
}

#[cfg(test)]
mod tests {
    use crate::task_attr::TaskAttr;
    use crate::validation::is_option_type;
    use syn::Type;

    #[test]
    fn test_is_option_type() {
        use syn::parse_quote;

        let option_type: Type = parse_quote!(Option<i32>);
        assert!(is_option_type(&option_type));

        let non_option_type: Type = parse_quote!(i32);
        assert!(!is_option_type(&non_option_type));

        let result_type: Type = parse_quote!(Result<i32, String>);
        assert!(!is_option_type(&result_type));
    }

    #[test]
    fn test_task_attr_parsing_empty() {
        let empty_tokens = proc_macro2::TokenStream::new();
        let attr: Result<TaskAttr, _> = syn::parse2(empty_tokens);
        assert!(attr.is_ok());
        let attr = match attr {
            Ok(a) => a,
            Err(e) => panic!("Failed to parse empty task attr: {e}"),
        };
        assert!(attr.name.is_none());
        assert!(attr.timeout.is_none());
        assert!(attr.priority.is_none());
        assert!(attr.max_retries.is_none());
    }

    #[test]
    fn test_task_attr_parsing_name() {
        use syn::parse_quote;

        let tokens = parse_quote!(name = "custom.task");
        let attr: TaskAttr = match syn::parse2(tokens) {
            Ok(a) => a,
            Err(e) => panic!("Failed to parse task attr: {e}"),
        };
        assert_eq!(attr.name, Some("custom.task".to_string()));
    }

    #[test]
    fn test_task_attr_parsing_timeout() {
        use syn::parse_quote;

        let tokens = parse_quote!(timeout = 60);
        let attr: TaskAttr = match syn::parse2(tokens) {
            Ok(a) => a,
            Err(e) => panic!("Failed to parse task attr: {e}"),
        };
        assert_eq!(attr.timeout, Some(60));
    }

    #[test]
    fn test_task_attr_parsing_priority() {
        use syn::parse_quote;

        let tokens = parse_quote!(priority = 10);
        let attr: TaskAttr = match syn::parse2(tokens) {
            Ok(a) => a,
            Err(e) => panic!("Failed to parse task attr: {e}"),
        };
        assert_eq!(attr.priority, Some(10));
    }

    #[test]
    fn test_task_attr_parsing_max_retries() {
        use syn::parse_quote;

        let tokens = parse_quote!(max_retries = 3);
        let attr: TaskAttr = match syn::parse2(tokens) {
            Ok(a) => a,
            Err(e) => panic!("Failed to parse task attr: {e}"),
        };
        assert_eq!(attr.max_retries, Some(3));
    }

    #[test]
    fn test_task_attr_parsing_multiple() {
        use syn::parse_quote;

        let tokens = parse_quote!(
            name = "task.name",
            timeout = 30,
            priority = 5,
            max_retries = 2
        );
        let attr: TaskAttr = match syn::parse2(tokens) {
            Ok(a) => a,
            Err(e) => panic!("Failed to parse task attr: {e}"),
        };
        assert_eq!(attr.name, Some("task.name".to_string()));
        assert_eq!(attr.timeout, Some(30));
        assert_eq!(attr.priority, Some(5));
        assert_eq!(attr.max_retries, Some(2));
    }

    #[test]
    fn test_task_attr_parsing_invalid_attribute() {
        use syn::parse_quote;

        let tokens = parse_quote!(invalid_attr = "value");
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_attr_parsing_invalid_name_type() {
        use syn::parse_quote;

        let tokens = parse_quote!(name = 123);
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_attr_parsing_invalid_timeout_type() {
        use syn::parse_quote;

        let tokens = parse_quote!(timeout = "not_a_number");
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_attr_parsing_duplicate_name() {
        use syn::parse_quote;

        let tokens = parse_quote!(name = "first", name = "second");
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("duplicate"));
        }
    }

    #[test]
    fn test_task_attr_parsing_empty_name() {
        use syn::parse_quote;

        let tokens = parse_quote!(name = "");
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("empty"));
        }
    }

    #[test]
    fn test_task_attr_parsing_zero_timeout() {
        use syn::parse_quote;

        let tokens = parse_quote!(timeout = 0);
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("greater than 0"));
        }
    }

    #[test]
    fn test_task_attr_parsing_duplicate_timeout() {
        use syn::parse_quote;

        let tokens = parse_quote!(timeout = 30, timeout = 60);
        let result: Result<TaskAttr, _> = syn::parse2(tokens);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("duplicate"));
        }
    }

    // Edge case tests for type detection
    #[test]
    fn test_is_option_type_nested() {
        use syn::parse_quote;

        let nested_option: Type = parse_quote!(Option<Option<i32>>);
        assert!(is_option_type(&nested_option));
    }

    #[test]
    fn test_is_option_type_with_path() {
        use syn::parse_quote;

        let std_option: Type = parse_quote!(std::option::Option<String>);
        assert!(is_option_type(&std_option));
    }

    #[test]
    fn test_is_option_type_complex_generic() {
        use syn::parse_quote;

        let complex: Type = parse_quote!(Option<Vec<HashMap<String, i32>>>);
        assert!(is_option_type(&complex));
    }

    #[test]
    fn test_is_option_type_box() {
        use syn::parse_quote;

        let boxed: Type = parse_quote!(Box<i32>);
        assert!(!is_option_type(&boxed));
    }

    #[test]
    fn test_is_option_type_vec() {
        use syn::parse_quote;

        let vec_type: Type = parse_quote!(Vec<i32>);
        assert!(!is_option_type(&vec_type));
    }

    #[test]
    fn test_is_option_type_reference() {
        use syn::parse_quote;

        let ref_type: Type = parse_quote!(&str);
        assert!(!is_option_type(&ref_type));
    }

    // Tests for name conversion (CamelCase to snake_case)
    #[test]
    fn test_camel_to_snake_conversion() {
        // Helper function to convert CamelCase to snake_case (same logic as derive macro)
        fn convert_to_snake(name: &str) -> String {
            let mut result = String::new();
            for (i, ch) in name.chars().enumerate() {
                if ch.is_uppercase() {
                    if i > 0 {
                        result.push('_');
                    }
                    if let Some(lower) = ch.to_lowercase().next() {
                        result.push(lower);
                    }
                } else {
                    result.push(ch);
                }
            }
            result
        }

        assert_eq!(convert_to_snake("MyTask"), "my_task");
        assert_eq!(convert_to_snake("MyLongTaskName"), "my_long_task_name");
        assert_eq!(convert_to_snake("SimpleTask"), "simple_task");
        assert_eq!(convert_to_snake("A"), "a");
        assert_eq!(convert_to_snake("AB"), "a_b");
    }
}
