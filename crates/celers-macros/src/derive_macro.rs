//! Implementation of the `#[derive(Task)]` derive macro.
//!
//! This module contains the code generation logic for the Task derive macro,
//! which generates a `Task` trait implementation for annotated structs.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Implementation of the `#[derive(Task)]` macro.
///
/// This function is called from the proc_macro entry point in lib.rs.
pub(crate) fn derive_task_impl(input: TokenStream) -> TokenStream {
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
                if let Some(lower) = ch.to_lowercase().next() {
                    result.push(lower);
                }
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
