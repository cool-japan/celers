//! Implementation of the `#[derive(Task)]` derive macro.
//!
//! This module contains the code generation logic for the Task derive macro,
//! which generates a `Task` trait implementation for annotated structs.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Parse the `input`/`output` string literal from `#[task(input = "...")]` /
/// `#[task(output = "...")]` as a `syn::Type`.
///
/// Returns `serde_json::Value` when the attribute was not given at all (the
/// documented default). When it *was* given but does not parse as a type,
/// this returns a spanned `syn::Error` instead of silently substituting
/// `serde_json::Value` — swallowing the mistake here used to turn a typo'd
/// type name into a confusing trait-mismatch error far away from the
/// attribute that actually caused it.
fn parse_type_attr(lit: Option<syn::LitStr>, attr_name: &str) -> syn::Result<syn::Type> {
    match lit {
        Some(lit) => lit.parse().map_err(|e: syn::Error| {
            syn::Error::new(
                lit.span(),
                format!(
                    "invalid `{attr_name}` type '{}': {e}. Expected a valid Rust type, e.g. #[task({attr_name} = \"MyType\")]",
                    lit.value()
                ),
            )
        }),
        None => Ok(syn::parse_quote!(serde_json::Value)),
    }
}

/// Implementation of the `#[derive(Task)]` macro.
///
/// This function is called from the proc_macro entry point in lib.rs.
pub(crate) fn derive_task_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Parse task attributes. Every parse failure (a typo like `inpt = "..."`,
    // a wrong literal type like `name = 42`, or an unknown key) must surface
    // as a real compile error instead of being silently ignored, so keep the
    // `syn::Result` from `parse_nested_meta` and propagate it below rather
    // than discarding it with `let _ = ...`.
    let mut task_name = None;
    let mut input_type: Option<syn::LitStr> = None;
    let mut output_type: Option<syn::LitStr> = None;

    for attr in &input.attrs {
        if attr.path().is_ident("task") {
            let parse_result = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    task_name = Some(value.value());
                } else if meta.path.is_ident("input") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    input_type = Some(value);
                } else if meta.path.is_ident("output") {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    output_type = Some(value);
                } else {
                    return Err(meta.error(format!(
                        "unknown #[task(...)] attribute '{}'. Valid attributes are: name, input, output",
                        meta.path
                            .get_ident()
                            .map(|i| i.to_string())
                            .unwrap_or_default()
                    )));
                }
                Ok(())
            });
            if let Err(e) = parse_result {
                return e.to_compile_error().into();
            }
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

    let input_ty: syn::Type = match parse_type_attr(input_type, "input") {
        Ok(ty) => ty,
        Err(e) => return e.to_compile_error().into(),
    };

    let output_ty: syn::Type = match parse_type_attr(output_type, "output") {
        Ok(ty) => ty,
        Err(e) => return e.to_compile_error().into(),
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
