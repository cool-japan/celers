//! Task attribute parsing for `#[task(...)]` macro parameters.
//!
//! This module handles parsing of task-level configuration attributes
//! such as `name`, `timeout`, `priority`, and `max_retries`.

use syn::{parse::Parse, parse::ParseStream, Lit, Token};

/// Task attribute parameters
pub(crate) struct TaskAttr {
    pub(crate) name: Option<String>,
    pub(crate) timeout: Option<u64>,
    pub(crate) priority: Option<i32>,
    pub(crate) max_retries: Option<u32>,
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
