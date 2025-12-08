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
//! - `name` - Custom task name (default: function name)
//! - `timeout` - Task timeout in seconds (must be > 0)
//! - `priority` - Task priority as i32 (higher = more important)
//! - `max_retries` - Maximum retry attempts as u32
//!
//! # Limitations
//!
//! - Functions must be `async`
//! - Return type must be wrapped in `Result<T>`
//! - Generic parameters with complex trait bounds (HRTBs) may require careful handling
//! - Validation attributes are not yet fully implemented

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, Attribute, DeriveInput, FnArg,
    GenericArgument, ItemFn, Lit, Meta, Pat, PathArguments, ReturnType, Token, Type, TypePath,
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
/// Infrastructure for future validation attribute support
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FieldValidation {
    min_value: Option<i64>,
    max_value: Option<i64>,
    min_length: Option<usize>,
    max_length: Option<usize>,
    pattern: Option<String>,
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
        }
    }

    fn from_attributes(attrs: &[Attribute]) -> syn::Result<Option<Self>> {
        let mut validation = FieldValidation::new();
        let mut has_validation = false;

        for attr in attrs {
            if let Meta::List(meta_list) = &attr.meta {
                let path_str = meta_list
                    .path
                    .segments
                    .last()
                    .map(|s| s.ident.to_string())
                    .unwrap_or_default();

                match path_str.as_str() {
                    "validate_range" => {
                        has_validation = true;
                        attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident("min") {
                                let value: syn::LitInt = meta.value()?.parse()?;
                                validation.min_value = Some(value.base10_parse()?);
                            } else if meta.path.is_ident("max") {
                                let value: syn::LitInt = meta.value()?.parse()?;
                                validation.max_value = Some(value.base10_parse()?);
                            }
                            Ok(())
                        })?;
                    }
                    "validate_length" => {
                        has_validation = true;
                        attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident("min") {
                                let value: syn::LitInt = meta.value()?.parse()?;
                                validation.min_length = Some(value.base10_parse()?);
                            } else if meta.path.is_ident("max") {
                                let value: syn::LitInt = meta.value()?.parse()?;
                                validation.max_length = Some(value.base10_parse()?);
                            }
                            Ok(())
                        })?;
                    }
                    "validate_pattern" => {
                        has_validation = true;
                        attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident("regex") {
                                let value: syn::LitStr = meta.value()?.parse()?;
                                validation.pattern = Some(value.value());
                            }
                            Ok(())
                        })?;
                    }
                    _ => {}
                }
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
            validations.push(quote! {
                if (#field_name as i64) < #min {
                    return Err(celers_core::CelersError::from(format!(
                        "Field '{}' value {} is below minimum {}",
                        stringify!(#field_name),
                        #field_name,
                        #min
                    )));
                }
            });
        }

        if let Some(max) = self.max_value {
            validations.push(quote! {
                if (#field_name as i64) > #max {
                    return Err(celers_core::CelersError::from(format!(
                        "Field '{}' value {} exceeds maximum {}",
                        stringify!(#field_name),
                        #field_name,
                        #max
                    )));
                }
            });
        }

        // Length validation for strings and collections
        if self.min_length.is_some() || self.max_length.is_some() {
            if let Some(min) = self.min_length {
                validations.push(quote! {
                    if #field_name.len() < #min {
                        return Err(celers_core::CelersError::from(format!(
                            "Field '{}' length {} is below minimum {}",
                            stringify!(#field_name),
                            #field_name.len(),
                            #min
                        )));
                    }
                });
            }

            if let Some(max) = self.max_length {
                validations.push(quote! {
                    if #field_name.len() > #max {
                        return Err(celers_core::CelersError::from(format!(
                            "Field '{}' length {} exceeds maximum {}",
                            stringify!(#field_name),
                            #field_name.len(),
                            #max
                        )));
                    }
                });
            }
        }

        // Pattern validation for strings
        if let Some(pattern) = &self.pattern {
            validations.push(quote! {
                {
                    let regex = regex::Regex::new(#pattern).map_err(|e| {
                        celers_core::CelersError::from(format!(
                            "Invalid regex pattern for field '{}': {}",
                            stringify!(#field_name),
                            e
                        ))
                    })?;
                    if !regex.is_match(#field_name) {
                        return Err(celers_core::CelersError::from(format!(
                            "Field '{}' does not match required pattern",
                            stringify!(#field_name)
                        )));
                    }
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

    // Extract input parameters (skip self)
    let mut input_fields = Vec::new();
    let mut input_field_names = Vec::new();
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

                let field_def = if is_option {
                    quote! {
                        #[serde(skip_serializing_if = "Option::is_none", default)]
                        pub #field_name: #field_type
                    }
                } else {
                    quote! { pub #field_name: #field_type }
                };

                input_fields.push(field_def);
                input_field_names.push(field_name);
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
