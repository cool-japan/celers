//! Procedural macros for CeleRS
//!
//! This crate provides procedural macros to simplify task definition:
//! - `#[task]` - Attribute macro for converting async functions into tasks
//! - `#[derive(Task)]` - Derive macro for implementing the Task trait

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, DeriveInput, FnArg, GenericArgument,
    ItemFn, Lit, Pat, PathArguments, ReturnType, Token, Type,
};

/// Task attribute parameters
struct TaskAttr {
    name: Option<String>,
}

impl Parse for TaskAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;
                let ident_str = ident.to_string();
                if ident_str == "name" {
                    input.parse::<Token![=]>()?;
                    let lit: Lit = input.parse()?;
                    if let Lit::Str(lit_str) = lit {
                        name = Some(lit_str.value());
                    } else {
                        return Err(syn::Error::new_spanned(
                            lit,
                            "name must be a string literal",
                        ));
                    }
                } else {
                    return Err(syn::Error::new_spanned(
                        ident,
                        format!("unknown attribute: {}", ident_str),
                    ));
                }
            }

            // Try to parse comma if there are more attributes
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(TaskAttr { name })
    }
}

/// Attribute macro for marking async functions as tasks
///
/// # Example
///
/// ```ignore
/// #[task]
/// async fn add_numbers(a: i32, b: i32) -> Result<i32> {
///     Ok(a + b)
/// }
///
/// // With custom name:
/// #[task(name = "tasks.add")]
/// async fn add_numbers(a: i32, b: i32) -> Result<i32> {
///     Ok(a + b)
/// }
/// ```
///
/// This will generate a struct `AddNumbersTask` that implements the `Task` trait.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let task_attr = parse_macro_input!(attr as TaskAttr);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Extract function details
    let fn_name = &input_fn.sig.ident;
    let fn_vis = &input_fn.vis;
    let fn_block = &input_fn.block;
    let fn_inputs = &input_fn.sig.inputs;

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

    for arg in fn_inputs.iter() {
        if let FnArg::Typed(pat_type) = arg {
            if let Pat::Ident(pat_ident) = &*pat_type.pat {
                let field_name = &pat_ident.ident;
                let field_type = &pat_type.ty;
                input_fields.push(quote! { pub #field_name: #field_type });
                input_field_names.push(field_name);
            }
        }
    }

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

    let expanded = quote! {
        // Input struct
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        #fn_vis struct #input_struct_name {
            #(#input_fields),*
        }

        // Output type alias
        #fn_vis type #output_struct_name = #output_type;

        // Task struct
        #[derive(Default)]
        #fn_vis struct #struct_name;

        // Task implementation
        #[async_trait::async_trait]
        impl celers_core::Task for #struct_name {
            type Input = #input_struct_name;
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
    };

    TokenStream::from(expanded)
}

/// Derive macro for Task trait
///
/// # Example
///
/// ```ignore
/// #[derive(Task)]
/// struct MyTask {
///     // Task configuration
/// }
/// ```
#[proc_macro_derive(Task, attributes(task))]
pub fn derive_task(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // For now, generate a basic implementation
    // This would need more sophisticated parsing for real use
    let expanded = quote! {
        #[async_trait::async_trait]
        impl celers_core::Task for #name {
            type Input = serde_json::Value;
            type Output = serde_json::Value;

            async fn execute(&self, _input: Self::Input) -> celers_core::Result<Self::Output> {
                unimplemented!("Task execution not implemented")
            }

            fn name(&self) -> &str {
                stringify!(#name)
            }
        }
    };

    TokenStream::from(expanded)
}
