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
//!
//! #### Numeric Validators
//! - `positive` - Ensures number is greater than 0
//! - `negative` - Ensures number is less than 0
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
//! ) -> Result<String> {
//!     Ok(format!("Registered {}", email))
//! }
//! ```
//!
//! **Note:** Predefined validators (`email`, `url`, `phone`) require the `regex` crate.
//!
//! ### Custom Error Messages
//! - `message` - Custom error message for validation failures
//!
//! All validation rules can include a custom `message` parameter to provide clearer, domain-specific error messages:
//!
//! Example: `#[validate(min = 18, max = 120, message = "Age must be between 18 and 120")] age: i32`
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

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, Attribute, DeriveInput, FnArg,
    GenericArgument, ItemFn, Lit, Pat, PathArguments, ReturnType, Token, Type, TypePath,
};

/// Helper function to check if a type is Option<T>
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(segment) = path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

/// Task attribute parameters
struct TaskAttr {
    name: Option<String>,
    timeout: Option<u64>,
    priority: Option<i32>,
    max_retries: Option<u32>,
}

/// Field validation configuration
///
/// Infrastructure for validation attribute support with custom error messages
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FieldValidation {
    min_value: Option<i64>,
    max_value: Option<i64>,
    min_length: Option<usize>,
    max_length: Option<usize>,
    pattern: Option<String>,
    message: Option<String>,
    // Predefined validation shortcuts
    email: bool,
    url: bool,
    phone: bool,
    not_empty: bool,
    positive: bool,
    negative: bool,
    alphabetic: bool,
    alphanumeric: bool,
}

#[allow(dead_code)]
impl FieldValidation {
    fn new() -> Self {
        Self {
            min_value: None,
            max_value: None,
            min_length: None,
            max_length: None,
            pattern: None,
            message: None,
            email: false,
            url: false,
            phone: false,
            not_empty: false,
            positive: false,
            negative: false,
            alphabetic: false,
            alphanumeric: false,
        }
    }

    fn from_attributes(attrs: &[Attribute]) -> syn::Result<Option<Self>> {
        let mut validation = FieldValidation::new();
        let mut has_validation = false;

        for attr in attrs {
            if attr.path().is_ident("validate") {
                has_validation = true;
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("min") {
                        let value: syn::LitInt = meta.value()?.parse()?;
                        validation.min_value = Some(value.base10_parse()?);
                    } else if meta.path.is_ident("max") {
                        let value: syn::LitInt = meta.value()?.parse()?;
                        validation.max_value = Some(value.base10_parse()?);
                    } else if meta.path.is_ident("min_length") {
                        let value: syn::LitInt = meta.value()?.parse()?;
                        validation.min_length = Some(value.base10_parse()?);
                    } else if meta.path.is_ident("max_length") {
                        let value: syn::LitInt = meta.value()?.parse()?;
                        validation.max_length = Some(value.base10_parse()?);
                    } else if meta.path.is_ident("pattern") {
                        let value: syn::LitStr = meta.value()?.parse()?;
                        validation.pattern = Some(value.value());
                    } else if meta.path.is_ident("message") {
                        let value: syn::LitStr = meta.value()?.parse()?;
                        validation.message = Some(value.value());
                    } else if meta.path.is_ident("email") {
                        validation.email = true;
                    } else if meta.path.is_ident("url") {
                        validation.url = true;
                    } else if meta.path.is_ident("phone") {
                        validation.phone = true;
                    } else if meta.path.is_ident("not_empty") {
                        validation.not_empty = true;
                    } else if meta.path.is_ident("positive") {
                        validation.positive = true;
                    } else if meta.path.is_ident("negative") {
                        validation.negative = true;
                    } else if meta.path.is_ident("alphabetic") {
                        validation.alphabetic = true;
                    } else if meta.path.is_ident("alphanumeric") {
                        validation.alphanumeric = true;
                    } else {
                        return Err(meta.error(format!(
                            "unknown validation parameter '{}'. Valid parameters: min, max, min_length, max_length, pattern, message, email, url, phone, not_empty, positive, negative, alphabetic, alphanumeric",
                            meta.path.get_ident().map(|i| i.to_string()).unwrap_or_default()
                        )));
                    }
                    Ok(())
                })?;
            }
        }

        if has_validation {
            Ok(Some(validation))
        } else {
            Ok(None)
        }
    }

    fn generate_validation_code(
        &self,
        field_name: &syn::Ident,
        _field_type: &Type,
    ) -> proc_macro2::TokenStream {
        let mut validations = Vec::new();

        // Range validation for numeric types
        if let Some(min) = self.min_value {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' value {} is below minimum {}",
                        stringify!(#field_name),
                        #field_name,
                        #min
                    )
                }
            };

            validations.push(quote! {
                if (#field_name as i64) < #min {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        if let Some(max) = self.max_value {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' value {} exceeds maximum {}",
                        stringify!(#field_name),
                        #field_name,
                        #max
                    )
                }
            };

            validations.push(quote! {
                if (#field_name as i64) > #max {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Length validation for strings and collections
        if self.min_length.is_some() || self.max_length.is_some() {
            if let Some(min) = self.min_length {
                let error_msg = if let Some(msg) = &self.message {
                    quote! { #msg.to_string() }
                } else {
                    quote! {
                        format!(
                            "Field '{}' length {} is below minimum {}",
                            stringify!(#field_name),
                            #field_name.len(),
                            #min
                        )
                    }
                };

                validations.push(quote! {
                    if #field_name.len() < #min {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                });
            }

            if let Some(max) = self.max_length {
                let error_msg = if let Some(msg) = &self.message {
                    quote! { #msg.to_string() }
                } else {
                    quote! {
                        format!(
                            "Field '{}' length {} exceeds maximum {}",
                            stringify!(#field_name),
                            #field_name.len(),
                            #max
                        )
                    }
                };

                validations.push(quote! {
                    if #field_name.len() > #max {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                });
            }
        }

        // Pattern validation for strings
        if let Some(pattern) = &self.pattern {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' does not match required pattern",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    let regex = regex::Regex::new(#pattern).map_err(|e| {
                        celers_core::CelersError(format!(
                            "Invalid regex pattern for field '{}': {}",
                            stringify!(#field_name),
                            e
                        ))
                    })?;
                    if !regex.is_match(&#field_name) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: email
        if self.email {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid email address",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    let email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$";
                    let regex = regex::Regex::new(email_pattern).unwrap();
                    if !regex.is_match(&#field_name) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: url
        if self.url {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid URL",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    let url_pattern = r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$";
                    let regex = regex::Regex::new(url_pattern).unwrap();
                    if !regex.is_match(&#field_name) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: phone
        if self.phone {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid phone number",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    let phone_pattern = r"^\+?[0-9]{10,15}$";
                    let regex = regex::Regex::new(phone_pattern).unwrap();
                    if !regex.is_match(&#field_name) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: not_empty
        if self.not_empty {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must not be empty",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if #field_name.is_empty() {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: positive
        if self.positive {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be positive (greater than 0)",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if #field_name <= 0 {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: negative
        if self.negative {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be negative (less than 0)",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if #field_name >= 0 {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: alphabetic
        if self.alphabetic {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only alphabetic characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.chars().all(|c| c.is_alphabetic()) {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: alphanumeric
        if self.alphanumeric {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only alphanumeric characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.chars().all(|c| c.is_alphanumeric()) {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        quote! { #(#validations)* }
    }
}

impl Parse for TaskAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut timeout = None;
        let mut priority = None;
        let mut max_retries = None;

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;
                let ident_str = ident.to_string();
                match ident_str.as_str() {
                    "name" => {
                        if name.is_some() {
                            return Err(syn::Error::new_spanned(
                                ident,
                                "duplicate 'name' attribute - this attribute can only be specified once",
                            ));
                        }
                        input.parse::<Token![=]>()?;
                        let lit: Lit = input.parse()?;
                        if let Lit::Str(lit_str) = lit {
                            let value = lit_str.value();
                            if value.is_empty() {
                                return Err(syn::Error::new_spanned(
                                    lit_str,
                                    "task name cannot be empty",
                                ));
                            }
                            name = Some(value);
                        } else {
                            return Err(syn::Error::new_spanned(
                                lit,
                                "name must be a string literal, e.g., name = \"tasks.my_task\"",
                            ));
                        }
                    }
                    "timeout" => {
                        if timeout.is_some() {
                            return Err(syn::Error::new_spanned(
                                ident,
                                "duplicate 'timeout' attribute - this attribute can only be specified once",
                            ));
                        }
                        input.parse::<Token![=]>()?;
                        let lit: Lit = input.parse()?;
                        if let Lit::Int(lit_int) = lit {
                            let value: u64 = lit_int.base10_parse().map_err(|_| {
                                syn::Error::new_spanned(
                                    &lit_int,
                                    "timeout value must be a valid u64 (0 to 18446744073709551615)",
                                )
                            })?;
                            if value == 0 {
                                return Err(syn::Error::new_spanned(
                                    lit_int,
                                    "timeout must be greater than 0 seconds",
                                ));
                            }
                            timeout = Some(value);
                        } else {
                            return Err(syn::Error::new_spanned(
                                lit,
                                "timeout must be an integer literal (seconds), e.g., timeout = 60",
                            ));
                        }
                    }
                    "priority" => {
                        if priority.is_some() {
                            return Err(syn::Error::new_spanned(
                                ident,
                                "duplicate 'priority' attribute - this attribute can only be specified once",
                            ));
                        }
                        input.parse::<Token![=]>()?;
                        let lit: Lit = input.parse()?;
                        if let Lit::Int(lit_int) = lit {
                            let value: i32 = lit_int.base10_parse().map_err(|_| {
                                syn::Error::new_spanned(
                                    &lit_int,
                                    "priority value must be a valid i32 (-2147483648 to 2147483647)",
                                )
                            })?;
                            priority = Some(value);
                        } else {
                            return Err(syn::Error::new_spanned(
                                lit,
                                "priority must be an integer literal, e.g., priority = 10",
                            ));
                        }
                    }
                    "max_retries" => {
                        if max_retries.is_some() {
                            return Err(syn::Error::new_spanned(
                                ident,
                                "duplicate 'max_retries' attribute - this attribute can only be specified once",
                            ));
                        }
                        input.parse::<Token![=]>()?;
                        let lit: Lit = input.parse()?;
                        if let Lit::Int(lit_int) = lit {
                            let value: u32 = lit_int.base10_parse().map_err(|_| {
                                syn::Error::new_spanned(
                                    &lit_int,
                                    "max_retries value must be a valid u32 (0 to 4294967295)",
                                )
                            })?;
                            max_retries = Some(value);
                        } else {
                            return Err(syn::Error::new_spanned(
                                lit,
                                "max_retries must be an integer literal, e.g., max_retries = 3",
                            ));
                        }
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            ident,
                            format!(
                                "unknown attribute '{}'. Valid attributes are: name, timeout, priority, max_retries",
                                ident_str
                            ),
                        ));
                    }
                }
            } else {
                return Err(lookahead.error());
            }

            // Try to parse comma if there are more attributes
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(TaskAttr {
            name,
            timeout,
            priority,
            max_retries,
        })
    }
}

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
    let task_attr = parse_macro_input!(attr as TaskAttr);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Validate that the function is async
    if input_fn.sig.asyncness.is_none() {
        let error = syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "the #[task] attribute can only be applied to async functions. Add 'async' before 'fn'",
        );
        return error.to_compile_error().into();
    }

    // Extract function details
    let fn_name = &input_fn.sig.ident;
    let fn_vis = &input_fn.vis;
    let fn_block = &input_fn.block;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_attrs = &input_fn.attrs;
    let fn_generics = &input_fn.sig.generics;
    let where_clause = &fn_generics.where_clause;

    // Generate task struct name (e.g., add_numbers -> AddNumbersTask)
    let struct_name = syn::Ident::new(
        &format!(
            "{}Task",
            fn_name
                .to_string()
                .split('_')
                .map(|s| {
                    let mut c = s.chars();
                    match c.next() {
                        None => String::new(),
                        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                    }
                })
                .collect::<String>()
        ),
        fn_name.span(),
    );

    // Extract input parameters (skip self) and parse validation attributes
    let mut input_fields = Vec::new();
    let mut input_field_names = Vec::new();
    let mut field_validations = Vec::new();
    let mut all_fields_optional = true;

    for arg in fn_inputs.iter() {
        if let FnArg::Typed(pat_type) = arg {
            if let Pat::Ident(pat_ident) = &*pat_type.pat {
                let field_name = &pat_ident.ident;
                let field_type = &pat_type.ty;

                // Check if the field is Option<T> to add serde skip_serializing_if
                let is_option = is_option_type(field_type);
                if !is_option {
                    all_fields_optional = false;
                }

                // Parse validation attributes from the parameter
                let validation = match FieldValidation::from_attributes(&pat_type.attrs) {
                    Ok(v) => v,
                    Err(e) => return e.to_compile_error().into(),
                };

                let field_def = if is_option {
                    quote! {
                        #[serde(skip_serializing_if = "Option::is_none", default)]
                        pub #field_name: #field_type
                    }
                } else {
                    quote! { pub #field_name: #field_type }
                };

                input_fields.push(field_def);
                input_field_names.push(field_name.clone());
                field_validations.push((field_name.clone(), field_type.clone(), validation));
            }
        }
    }

    // Add Default derive if all fields are optional
    let input_derives = if all_fields_optional && !input_fields.is_empty() {
        quote! { #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)] }
    } else {
        quote! { #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)] }
    };

    // Extract return type and handle Result wrapper
    let output_type = match &input_fn.sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => {
            // Try to extract inner type from Result<T, E>
            let mut extracted_type = None;
            if let Type::Path(type_path) = &**ty {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Result" {
                        if let PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(GenericArgument::Type(inner_ty)) = args.args.first() {
                                // Found Result<T>, use T as output type
                                extracted_type = Some(quote! { #inner_ty });
                            }
                        }
                    }
                }
            }
            extracted_type.unwrap_or_else(|| quote! { #ty })
        }
    };

    // Generate input and output structs
    let input_struct_name = syn::Ident::new(&format!("{}Input", struct_name), fn_name.span());
    let output_struct_name = syn::Ident::new(&format!("{}Output", struct_name), fn_name.span());

    // Determine task name (custom or function name)
    let task_name = task_attr.name.unwrap_or_else(|| fn_name.to_string());

    // Generate optional configuration methods
    let timeout_impl = task_attr.timeout.map(|timeout| {
        quote! {
            /// Get the configured timeout in seconds
            pub fn timeout(&self) -> Option<u64> {
                Some(#timeout)
            }
        }
    });

    let priority_impl = task_attr.priority.map(|priority| {
        quote! {
            /// Get the configured priority
            pub fn priority(&self) -> Option<i32> {
                Some(#priority)
            }
        }
    });

    let max_retries_impl = task_attr.max_retries.map(|max_retries| {
        quote! {
            /// Get the configured maximum retry attempts
            pub fn max_retries(&self) -> Option<u32> {
                Some(#max_retries)
            }
        }
    });

    // Split generics for impl blocks
    let (impl_generics, ty_generics, _) = fn_generics.split_for_impl();

    // Only add Default derive if there are no generic parameters
    let has_generics = !fn_generics.params.is_empty();
    let task_struct_derives = if has_generics {
        quote! {}
    } else {
        quote! { #[derive(Default)] }
    };

    // Add Default implementation for generic structs
    let default_impl = if has_generics {
        quote! {
            impl #impl_generics Default for #struct_name #ty_generics #where_clause {
                fn default() -> Self {
                    #struct_name
                }
            }
        }
    } else {
        quote! {}
    };

    // Generate validation code for fields that have validation rules
    let validation_code: Vec<_> = field_validations
        .iter()
        .filter_map(|(name, ty, validation)| {
            validation
                .as_ref()
                .map(|v| v.generate_validation_code(name, ty))
        })
        .collect();

    let expanded = quote! {
        /// Input struct for the task
        ///
        /// Generated by the `#[task]` macro
        #input_derives
        #fn_vis struct #input_struct_name #impl_generics #where_clause {
            #(#input_fields),*
        }

        /// Output type for the task
        ///
        /// Generated by the `#[task]` macro
        #fn_vis type #output_struct_name #impl_generics = #output_type;

        /// Task struct
        ///
        /// Generated by the `#[task]` macro from the function definition
        #(#fn_attrs)*
        #task_struct_derives
        #fn_vis struct #struct_name #impl_generics #where_clause;

        #default_impl

        // Task implementation
        #[async_trait::async_trait]
        impl #impl_generics celers_core::Task for #struct_name #ty_generics #where_clause {
            type Input = #input_struct_name #ty_generics;
            type Output = #output_type;

            async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
                // Destructure input
                let #input_struct_name { #(#input_field_names),* } = input;

                // Validate input fields
                #(#validation_code)*

                // Execute the original function body inline
                #fn_block
            }

            fn name(&self) -> &str {
                #task_name
            }
        }

        // Additional configuration methods
        impl #impl_generics #struct_name #ty_generics #where_clause {
            #timeout_impl
            #priority_impl
            #max_retries_impl
        }
    };

    TokenStream::from(expanded)
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
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Parse task attributes
    let mut task_name = None;
    let mut input_type = None;
    let mut output_type = None;

    for attr in &input.attrs {
        if attr.path().is_ident("task") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    task_name = Some(value.value());
                } else if meta.path.is_ident("input") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    input_type = Some(value.value());
                } else if meta.path.is_ident("output") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    output_type = Some(value.value());
                }
                Ok(())
            });
        }
    }

    // Use parsed values or defaults
    let task_name_str = task_name.unwrap_or_else(|| {
        // Convert CamelCase to snake_case for task name
        let name_str = name.to_string();
        let mut result = String::new();
        for (i, ch) in name_str.chars().enumerate() {
            if ch.is_uppercase() {
                if i > 0 {
                    result.push('_');
                }
                result.push(ch.to_lowercase().next().unwrap());
            } else {
                result.push(ch);
            }
        }
        result
    });

    let input_ty: syn::Type = if let Some(input_str) = input_type {
        syn::parse_str(&input_str).unwrap_or_else(|_| syn::parse_quote!(serde_json::Value))
    } else {
        syn::parse_quote!(serde_json::Value)
    };

    let output_ty: syn::Type = if let Some(output_str) = output_type {
        syn::parse_str(&output_str).unwrap_or_else(|_| syn::parse_quote!(serde_json::Value))
    } else {
        syn::parse_quote!(serde_json::Value)
    };

    let expanded = quote! {
        #[async_trait::async_trait]
        impl #impl_generics celers_core::Task for #name #ty_generics #where_clause {
            type Input = #input_ty;
            type Output = #output_ty;

            async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
                // Call the execute_impl method if it exists, otherwise unimplemented
                self.execute_impl(input).await
            }

            fn name(&self) -> &str {
                #task_name_str
            }
        }
    };

    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let attr = attr.unwrap();
        assert!(attr.name.is_none());
        assert!(attr.timeout.is_none());
        assert!(attr.priority.is_none());
        assert!(attr.max_retries.is_none());
    }

    #[test]
    fn test_task_attr_parsing_name() {
        use syn::parse_quote;

        let tokens = parse_quote!(name = "custom.task");
        let attr: TaskAttr = syn::parse2(tokens).unwrap();
        assert_eq!(attr.name, Some("custom.task".to_string()));
    }

    #[test]
    fn test_task_attr_parsing_timeout() {
        use syn::parse_quote;

        let tokens = parse_quote!(timeout = 60);
        let attr: TaskAttr = syn::parse2(tokens).unwrap();
        assert_eq!(attr.timeout, Some(60));
    }

    #[test]
    fn test_task_attr_parsing_priority() {
        use syn::parse_quote;

        let tokens = parse_quote!(priority = 10);
        let attr: TaskAttr = syn::parse2(tokens).unwrap();
        assert_eq!(attr.priority, Some(10));
    }

    #[test]
    fn test_task_attr_parsing_max_retries() {
        use syn::parse_quote;

        let tokens = parse_quote!(max_retries = 3);
        let attr: TaskAttr = syn::parse2(tokens).unwrap();
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
        let attr: TaskAttr = syn::parse2(tokens).unwrap();
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
                    result.push(ch.to_lowercase().next().unwrap());
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
