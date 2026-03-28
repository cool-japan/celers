//! Field validation configuration and code generation for task parameters.
//!
//! This module contains the `FieldValidation` struct and its implementation,
//! which handles parsing of `#[validate(...)]` attributes and generating
//! the corresponding validation code at compile time.

use quote::quote;
use syn::{Attribute, Type, TypePath};

/// Regex patterns are pre-compiled using LazyLock for optimal performance.
/// This avoids recompiling patterns on every validation call.
pub(crate) fn generate_lazy_regex_validation(
    field_name: &syn::Ident,
    pattern: &str,
    pattern_name: &str,
    error_msg_tokens: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let static_name = syn::Ident::new(
        &format!("REGEX_{}", pattern_name.to_uppercase()),
        field_name.span(),
    );

    quote! {
        {
            use std::sync::LazyLock;
            static #static_name: LazyLock<regex::Regex> = LazyLock::new(|| {
                regex::Regex::new(#pattern).expect("Invalid regex pattern")
            });
            if !#static_name.is_match(&#field_name) {
                return Err(celers_core::CelersError(#error_msg_tokens));
            }
        }
    }
}

/// Helper function to check if a type is Option<T>
pub(crate) fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(segment) = path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

/// Field validation configuration
///
/// Infrastructure for validation attribute support with custom error messages
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FieldValidation {
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
    numeric: bool,
    uuid: bool,
    ipv4: bool,
    hexadecimal: bool,
    ipv6: bool,
    slug: bool,
    mac_address: bool,
    json: bool,
    base64: bool,
    color_hex: bool,
    // Additional practical validators
    semver: bool,
    domain: bool,
    ascii: bool,
    lowercase: bool,
    uppercase: bool,
    time_24h: bool,
    date_iso8601: bool,
    credit_card: bool,
    // Geographic and locale validators
    latitude: bool,
    longitude: bool,
    iso_country: bool,
    iso_language: bool,
    us_zip: bool,
    ca_postal: bool,
    // Financial and specialized validators
    iban: bool,
    bitcoin_address: bool,
    ethereum_address: bool,
    isbn: bool,
    password_strength: bool,
    // Custom validator function
    custom: Option<String>,
}

#[allow(dead_code)]
impl FieldValidation {
    pub(crate) fn new() -> Self {
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
            numeric: false,
            uuid: false,
            ipv4: false,
            hexadecimal: false,
            ipv6: false,
            slug: false,
            mac_address: false,
            json: false,
            base64: false,
            color_hex: false,
            semver: false,
            domain: false,
            ascii: false,
            lowercase: false,
            uppercase: false,
            time_24h: false,
            date_iso8601: false,
            credit_card: false,
            latitude: false,
            longitude: false,
            iso_country: false,
            iso_language: false,
            us_zip: false,
            ca_postal: false,
            iban: false,
            bitcoin_address: false,
            ethereum_address: false,
            isbn: false,
            password_strength: false,
            custom: None,
        }
    }

    pub(crate) fn from_attributes(attrs: &[Attribute]) -> syn::Result<Option<Self>> {
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
                    } else if meta.path.is_ident("numeric") {
                        validation.numeric = true;
                    } else if meta.path.is_ident("uuid") {
                        validation.uuid = true;
                    } else if meta.path.is_ident("ipv4") {
                        validation.ipv4 = true;
                    } else if meta.path.is_ident("hexadecimal") {
                        validation.hexadecimal = true;
                    } else if meta.path.is_ident("ipv6") {
                        validation.ipv6 = true;
                    } else if meta.path.is_ident("slug") {
                        validation.slug = true;
                    } else if meta.path.is_ident("mac_address") {
                        validation.mac_address = true;
                    } else if meta.path.is_ident("json") {
                        validation.json = true;
                    } else if meta.path.is_ident("base64") {
                        validation.base64 = true;
                    } else if meta.path.is_ident("color_hex") {
                        validation.color_hex = true;
                    } else if meta.path.is_ident("semver") {
                        validation.semver = true;
                    } else if meta.path.is_ident("domain") {
                        validation.domain = true;
                    } else if meta.path.is_ident("ascii") {
                        validation.ascii = true;
                    } else if meta.path.is_ident("lowercase") {
                        validation.lowercase = true;
                    } else if meta.path.is_ident("uppercase") {
                        validation.uppercase = true;
                    } else if meta.path.is_ident("time_24h") {
                        validation.time_24h = true;
                    } else if meta.path.is_ident("date_iso8601") {
                        validation.date_iso8601 = true;
                    } else if meta.path.is_ident("credit_card") {
                        validation.credit_card = true;
                    } else if meta.path.is_ident("latitude") {
                        validation.latitude = true;
                    } else if meta.path.is_ident("longitude") {
                        validation.longitude = true;
                    } else if meta.path.is_ident("iso_country") {
                        validation.iso_country = true;
                    } else if meta.path.is_ident("iso_language") {
                        validation.iso_language = true;
                    } else if meta.path.is_ident("us_zip") {
                        validation.us_zip = true;
                    } else if meta.path.is_ident("ca_postal") {
                        validation.ca_postal = true;
                    } else if meta.path.is_ident("iban") {
                        validation.iban = true;
                    } else if meta.path.is_ident("bitcoin_address") {
                        validation.bitcoin_address = true;
                    } else if meta.path.is_ident("ethereum_address") {
                        validation.ethereum_address = true;
                    } else if meta.path.is_ident("isbn") {
                        validation.isbn = true;
                    } else if meta.path.is_ident("password_strength") {
                        validation.password_strength = true;
                    } else if meta.path.is_ident("custom") {
                        let value: syn::LitStr = meta.value()?.parse()?;
                        validation.custom = Some(value.value());
                    } else {
                        return Err(meta.error(format!(
                            "unknown validation parameter '{}'. Valid parameters: min, max, min_length, max_length, pattern, message, email, url, phone, not_empty, positive, negative, alphabetic, alphanumeric, numeric, uuid, ipv4, hexadecimal, ipv6, slug, mac_address, json, base64, color_hex, semver, domain, ascii, lowercase, uppercase, time_24h, date_iso8601, credit_card, latitude, longitude, iso_country, iso_language, us_zip, ca_postal, iban, bitcoin_address, ethereum_address, isbn, password_strength, custom",
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

    pub(crate) fn generate_validation_code(
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

        // Pattern validation for strings (custom patterns cannot use LazyLock due to dynamic pattern)
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

        // Predefined validation: email (optimized with LazyLock)
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

            let pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "EMAIL", error_msg,
            ));
        }

        // Predefined validation: url (optimized with LazyLock)
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

            let pattern = r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "URL", error_msg,
            ));
        }

        // Predefined validation: phone (optimized with LazyLock)
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

            let pattern = r"^\+?[0-9]{10,15}$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "PHONE", error_msg,
            ));
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

        // Predefined validation: numeric
        if self.numeric {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only numeric characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.chars().all(|c| c.is_numeric()) {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: uuid (optimized with LazyLock)
        if self.uuid {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid UUID",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern =
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "UUID", error_msg,
            ));
        }

        // Predefined validation: ipv4 (optimized with LazyLock)
        if self.ipv4 {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid IPv4 address",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "IPV4", error_msg,
            ));
        }

        // Predefined validation: hexadecimal
        if self.hexadecimal {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only hexadecimal characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: ipv6 (optimized with LazyLock)
        if self.ipv6 {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid IPv6 address",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "IPV6", error_msg,
            ));
        }

        // Predefined validation: slug (optimized with LazyLock)
        if self.slug {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid URL slug (lowercase letters, numbers, and hyphens only)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^[a-z0-9]+(?:-[a-z0-9]+)*$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "SLUG", error_msg,
            ));
        }

        // Predefined validation: mac_address (optimized with LazyLock)
        if self.mac_address {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid MAC address",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "MAC_ADDRESS",
                error_msg,
            ));
        }

        // Predefined validation: json
        if self.json {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be valid JSON",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    if serde_json::from_str::<serde_json::Value>(&#field_name).is_err() {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: base64 (optimized with LazyLock)
        if self.base64 {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be valid base64",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    use std::sync::LazyLock;
                    static REGEX_BASE64: LazyLock<regex::Regex> = LazyLock::new(|| {
                        regex::Regex::new(r"^[A-Za-z0-9+/]*={0,2}$").expect("Invalid regex pattern")
                    });
                    if !REGEX_BASE64.is_match(&#field_name) || (#field_name.len() % 4 != 0) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: color_hex (optimized with LazyLock)
        if self.color_hex {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid hex color code (#RGB or #RRGGBB)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "COLOR_HEX",
                error_msg,
            ));
        }

        // Predefined validation: semver (optimized with LazyLock)
        if self.semver {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid semantic version (e.g., 1.2.3)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "SEMVER", error_msg,
            ));
        }

        // Predefined validation: domain (optimized with LazyLock)
        if self.domain {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid domain name",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "DOMAIN", error_msg,
            ));
        }

        // Predefined validation: ascii
        if self.ascii {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only ASCII characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.is_ascii() {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: lowercase
        if self.lowercase {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only lowercase characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.chars().all(|c| c.is_lowercase() || !c.is_alphabetic()) {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: uppercase
        if self.uppercase {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must contain only uppercase characters",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                if !#field_name.chars().all(|c| c.is_uppercase() || !c.is_alphabetic()) {
                    return Err(celers_core::CelersError(#error_msg));
                }
            });
        }

        // Predefined validation: time_24h (optimized with LazyLock)
        if self.time_24h {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid 24-hour time (HH:MM or HH:MM:SS)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "TIME_24H", error_msg,
            ));
        }

        // Predefined validation: date_iso8601 (optimized with LazyLock)
        if self.date_iso8601 {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid ISO 8601 date (YYYY-MM-DD)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "DATE_ISO8601",
                error_msg,
            ));
        }

        // Predefined validation: credit_card (Luhn algorithm)
        if self.credit_card {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid credit card number",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    // Remove spaces and hyphens
                    let cleaned: String = #field_name.chars().filter(|c| c.is_numeric()).collect();

                    // Check length (13-19 digits for most cards)
                    if cleaned.len() < 13 || cleaned.len() > 19 {
                        return Err(celers_core::CelersError(#error_msg));
                    }

                    // Luhn algorithm
                    let mut sum = 0;
                    let mut double = false;
                    for ch in cleaned.chars().rev() {
                        let mut digit = ch.to_digit(10).unwrap_or(0) as i32;
                        if double {
                            digit *= 2;
                            if digit > 9 {
                                digit -= 9;
                            }
                        }
                        sum += digit;
                        double = !double;
                    }

                    if sum % 10 != 0 {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: latitude (geographic coordinate)
        if self.latitude {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid latitude (-90 to 90)",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    let lat: f64 = #field_name.parse().map_err(|_| {
                        celers_core::CelersError(#error_msg.clone())
                    })?;
                    if !(-90.0..=90.0).contains(&lat) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: longitude (geographic coordinate)
        if self.longitude {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid longitude (-180 to 180)",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    let lon: f64 = #field_name.parse().map_err(|_| {
                        celers_core::CelersError(#error_msg.clone())
                    })?;
                    if !(-180.0..=180.0).contains(&lon) {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: iso_country (ISO 3166-1 alpha-2 country code)
        if self.iso_country {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid ISO 3166-1 alpha-2 country code (e.g., US, CA, GB)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^[A-Z]{2}$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "ISO_COUNTRY",
                error_msg,
            ));
        }

        // Predefined validation: iso_language (ISO 639-1 language code)
        if self.iso_language {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid ISO 639-1 language code (e.g., en, es, fr)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^[a-z]{2}$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "ISO_LANGUAGE",
                error_msg,
            ));
        }

        // Predefined validation: us_zip (US ZIP code)
        if self.us_zip {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid US ZIP code (12345 or 12345-6789)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^\d{5}(?:-\d{4})?$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "US_ZIP", error_msg,
            ));
        }

        // Predefined validation: ca_postal (Canadian postal code)
        if self.ca_postal {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid Canadian postal code (A1A 1A1 or A1A1A1)",
                        stringify!(#field_name)
                    )
                }
            };

            let pattern = r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "CA_POSTAL",
                error_msg,
            ));
        }

        // Predefined validation: iban (International Bank Account Number)
        if self.iban {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid IBAN (International Bank Account Number)",
                        stringify!(#field_name)
                    )
                }
            };

            // IBAN format: 2 letter country code + 2 check digits + up to 30 alphanumeric
            let pattern = r"^[A-Z]{2}[0-9]{2}[A-Z0-9]{1,30}$";
            validations.push(generate_lazy_regex_validation(
                field_name, pattern, "IBAN", error_msg,
            ));
        }

        // Predefined validation: bitcoin_address
        if self.bitcoin_address {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid Bitcoin address",
                        stringify!(#field_name)
                    )
                }
            };

            // Bitcoin address formats: P2PKH (1...), P2SH (3...), Bech32 (bc1...)
            let pattern =
                r"^(1[a-km-zA-HJ-NP-Z1-9]{25,34}|3[a-km-zA-HJ-NP-Z1-9]{25,34}|bc1[a-z0-9]{39,59})$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "BITCOIN_ADDRESS",
                error_msg,
            ));
        }

        // Predefined validation: ethereum_address
        if self.ethereum_address {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid Ethereum address",
                        stringify!(#field_name)
                    )
                }
            };

            // Ethereum address: 0x followed by 40 hexadecimal characters
            let pattern = r"^0x[a-fA-F0-9]{40}$";
            validations.push(generate_lazy_regex_validation(
                field_name,
                pattern,
                "ETHEREUM_ADDRESS",
                error_msg,
            ));
        }

        // Predefined validation: isbn
        if self.isbn {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a valid ISBN (10 or 13 digits)",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    // Remove hyphens and spaces
                    let cleaned: String = #field_name.chars().filter(|c| c.is_alphanumeric()).collect();

                    if cleaned.len() == 10 {
                        // ISBN-10 validation with checksum
                        if !cleaned.chars().take(9).all(|c| c.is_numeric()) {
                            return Err(celers_core::CelersError(#error_msg));
                        }

                        let mut sum = 0;
                        for (i, ch) in cleaned.chars().take(9).enumerate() {
                            sum += (10 - i) * ch.to_digit(10).unwrap_or(0) as usize;
                        }

                        let check_char = cleaned.chars().nth(9).unwrap_or('?');
                        let check_digit = if check_char == 'X' || check_char == 'x' {
                            10
                        } else {
                            check_char.to_digit(10).unwrap_or(11) as usize
                        };

                        if (sum + check_digit) % 11 != 0 {
                            return Err(celers_core::CelersError(#error_msg));
                        }
                    } else if cleaned.len() == 13 {
                        // ISBN-13 validation with checksum
                        if !cleaned.chars().all(|c| c.is_numeric()) {
                            return Err(celers_core::CelersError(#error_msg));
                        }

                        let mut sum = 0;
                        for (i, ch) in cleaned.chars().enumerate() {
                            let digit = ch.to_digit(10).unwrap_or(0) as usize;
                            sum += if i % 2 == 0 { digit } else { digit * 3 };
                        }

                        if sum % 10 != 0 {
                            return Err(celers_core::CelersError(#error_msg));
                        }
                    } else {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Predefined validation: password_strength
        if self.password_strength {
            let error_msg = if let Some(msg) = &self.message {
                quote! { #msg.to_string() }
            } else {
                quote! {
                    format!(
                        "Field '{}' must be a strong password (at least 8 characters with uppercase, lowercase, digit, and special character)",
                        stringify!(#field_name)
                    )
                }
            };

            validations.push(quote! {
                {
                    if #field_name.len() < 8 {
                        return Err(celers_core::CelersError(#error_msg));
                    }

                    let has_uppercase = #field_name.chars().any(|c| c.is_uppercase());
                    let has_lowercase = #field_name.chars().any(|c| c.is_lowercase());
                    let has_digit = #field_name.chars().any(|c| c.is_numeric());
                    let has_special = #field_name.chars().any(|c| !c.is_alphanumeric());

                    if !has_uppercase || !has_lowercase || !has_digit || !has_special {
                        return Err(celers_core::CelersError(#error_msg));
                    }
                }
            });
        }

        // Custom validator function
        if let Some(custom_fn) = &self.custom {
            let custom_fn_ident = syn::Ident::new(custom_fn, proc_macro2::Span::call_site());

            validations.push(quote! {
                if let Err(e) = #custom_fn_ident(&#field_name) {
                    return Err(celers_core::CelersError(e));
                }
            });
        }

        quote! { #(#validations)* }
    }
}
