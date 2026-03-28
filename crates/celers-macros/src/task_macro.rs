//! Implementation of the `#[task]` attribute macro.
//!
//! This module contains the core code generation logic for transforming
//! async functions annotated with `#[task]` into task structs that
//! implement the `Task` trait.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, FnArg, GenericArgument, ItemFn, Pat, PathArguments, ReturnType, Type,
};

use crate::task_attr::TaskAttr;
use crate::validation::{is_option_type, FieldValidation};

/// Implementation of the `#[task]` attribute macro.
///
/// This function is called from the proc_macro entry point in lib.rs.
pub(crate) fn task_macro_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
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
