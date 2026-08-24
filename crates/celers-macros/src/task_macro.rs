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

/// True if `tokens` contains an identifier exactly equal to `name` anywhere,
/// including inside nested delimited groups (`<...>`, `(...)`, `[...]`,
/// `{...}`). Used to work out which of a task fn's generic parameters are
/// actually referenced by its extracted output type; see the output type
/// alias generation in [`task_macro_impl`] for why this matters.
fn token_stream_mentions_ident(tokens: &proc_macro2::TokenStream, name: &str) -> bool {
    tokens.clone().into_iter().any(|tt| match tt {
        proc_macro2::TokenTree::Ident(ident) => ident == name,
        proc_macro2::TokenTree::Group(group) => token_stream_mentions_ident(&group.stream(), name),
        proc_macro2::TokenTree::Punct(_) | proc_macro2::TokenTree::Literal(_) => false,
    })
}

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

    // A generic `#[task]` fn (e.g. `async fn process<T>(items: Vec<T>) -> ...`)
    // generates a marker `#struct_name<T>` task struct that carries no real
    // data of its own (the function's parameters become fields of the
    // *input* struct, not this one). Declaring it as a plain unit struct
    // `struct ProcessTask<T>;` would leave every type/lifetime parameter
    // completely unused, which is a hard error (E0392 "parameter `T` is
    // never used"). Give it a `PhantomData` marker field that references
    // every parameter instead, so the struct compiles for any `T` without
    // implying any auto-trait bound on `T` itself: `fn() -> T` is `Send +
    // Sync + Copy` regardless of what `T` is, since the field never
    // actually stores a `T` value. Const generics can't be woven into a
    // marker type this way in general, so surface a clear compile error
    // for that case instead of emitting code that will not build.
    let mut marker_members: Vec<proc_macro2::TokenStream> = Vec::new();
    if has_generics {
        for param in &fn_generics.params {
            match param {
                syn::GenericParam::Type(type_param) => {
                    let ident = &type_param.ident;
                    marker_members.push(quote! { fn() -> #ident });
                }
                syn::GenericParam::Lifetime(lifetime_param) => {
                    let lifetime = &lifetime_param.lifetime;
                    marker_members.push(quote! { & #lifetime () });
                }
                syn::GenericParam::Const(const_param) => {
                    let error = syn::Error::new_spanned(
                        const_param,
                        "#[task] does not support const generic parameters yet",
                    );
                    return error.to_compile_error().into();
                }
            }
        }
    }

    // Struct definition: a plain unit struct when there are no generics
    // (unchanged from before), or a struct with a hidden PhantomData marker
    // field when there are.
    let struct_def = if has_generics {
        quote! {
            #fn_vis struct #struct_name #impl_generics #where_clause {
                #[doc(hidden)]
                _marker: ::core::marker::PhantomData<(#(#marker_members,)*)>,
            }
        }
    } else {
        quote! {
            #fn_vis struct #struct_name #impl_generics #where_clause;
        }
    };

    // Add Default implementation for generic structs (this is *not*
    // `#[derive(Default)]`-able in general, since deriving would require
    // `T: Default` even though the marker field never holds a `T`).
    let default_impl = if has_generics {
        quote! {
            impl #impl_generics Default for #struct_name #ty_generics #where_clause {
                fn default() -> Self {
                    #struct_name {
                        _marker: ::core::marker::PhantomData,
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    // Only give the output type alias the generic parameters its own
    // definition actually needs (e.g. `Vec<T>` needs `T`; a fixed `usize`
    // needs none) -- this is *not* just a style choice. A type alias
    // parameter that is not referenced anywhere in its definition is
    // `error[E0091]: type parameter is never used`, a hard error, not a
    // lint. Restating every one of the fn's bounds here (as `impl_generics`
    // would) makes a would-be-unused parameter "used" for E0091's purposes,
    // but reintroduces the `type_alias_bounds` warning this fix is also
    // meant to avoid; using `ty_generics` unconditionally instead trades
    // that warning straight back into E0091 whenever the output type
    // happens not to mention a parameter -- exactly the case for the
    // documented `async fn process<T>(items: Vec<T>) -> Result<usize>`
    // example, whose `usize` output never references `T`. Filtering down to
    // only the parameters actually mentioned in `output_type` avoids both.
    let output_alias_params: Vec<proc_macro2::TokenStream> = if has_generics {
        fn_generics
            .params
            .iter()
            .filter_map(|param| match param {
                syn::GenericParam::Type(type_param) => {
                    let ident = &type_param.ident;
                    token_stream_mentions_ident(&output_type, &ident.to_string())
                        .then(|| quote! { #ident })
                }
                syn::GenericParam::Lifetime(lifetime_param) => {
                    let lifetime = &lifetime_param.lifetime;
                    token_stream_mentions_ident(&output_type, &lifetime.ident.to_string())
                        .then(|| quote! { #lifetime })
                }
                // Const generics are rejected earlier (see `marker_members`
                // above), so this arm is unreachable in practice -- kept
                // only for match exhaustiveness.
                syn::GenericParam::Const(_) => None,
            })
            .collect()
    } else {
        Vec::new()
    };
    let output_alias_generics = if output_alias_params.is_empty() {
        quote! {}
    } else {
        quote! { < #(#output_alias_params),* > }
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
        //
        // Deliberately does *not* repeat `#where_clause` here (unlike the
        // marker struct and impl blocks below, which do need it). This
        // struct only ever holds data and is (de)serialized -- it never
        // calls any of the original fn's bounded methods -- so it does not
        // itself need the fn's where-clause, and `#[derive(Serialize,
        // Deserialize)]` already infers its own per-field bounds
        // automatically. Repeating a bound here that also mentions
        // `Serialize`/`Deserialize` (as any task moving real generic data
        // through validation or storage would need on the `Task` impl
        // below) collides with that auto-inferred bound and produces
        // `error[E0283]: type annotations needed ... multiple impls or
        // where clauses satisfying ... found` -- an unhelpful, hard-to-
        // diagnose ambiguity rather than a normal compile error.
        #input_derives
        #fn_vis struct #input_struct_name #impl_generics {
            #(#input_fields),*
        }

        /// Output type for the task
        ///
        /// Generated by the `#[task]` macro
        //
        // `output_alias_generics` carries only the parameters `output_type`
        // actually mentions, with no bounds -- see the comment where it is
        // computed above for why (avoids both `error[E0091]` and the
        // `type_alias_bounds` warning).
        #fn_vis type #output_struct_name #output_alias_generics = #output_type;

        /// Task struct
        ///
        /// Generated by the `#[task]` macro from the function definition
        #(#fn_attrs)*
        #task_struct_derives
        #struct_def

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
