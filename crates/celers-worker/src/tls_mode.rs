//! URL-driven TLS mode selection for OxiSQL-backed PostgreSQL connections.
//!
//! # Background
//!
//! The previous `sqlx`-based connection code (`sqlx::PgPool::connect`)
//! respected the standard `sslmode=` query parameter embedded in a database
//! connection URL implicitly: a caller who wrote `sslmode=require` in their
//! connection string got a TLS-encrypted connection, and a caller who wrote
//! nothing (or `sslmode=disable`) got plain-text.
//!
//! The `sqlx` → `oxisql` migration of `dlq_storage.rs`'s `PostgresDlqStorage`
//! replaces the `sqlx::PgPool::connect` call with
//! `oxisql_postgres::PgConnection::connect`, which takes an explicit
//! [`oxisql_postgres::TlsMode`] value as an argument rather than inferring
//! one from the URL. Hardcoding `TlsMode::Disabled` there would silently
//! downgrade any connection to plain-text even when the URL explicitly asked
//! for TLS via `sslmode=require` — a MEDIUM-severity regression: a caller who
//! believes they are encrypting traffic (because their connection string
//! says so) would unknowingly send credentials and query data in the clear.
//!
//! This module avoids that regression by parsing the caller's connection URL
//! for the `sslmode` query parameter and building the matching
//! [`oxisql_postgres::TlsMode`] value. `TlsMode::Disabled` remains a
//! legitimate outcome — it is simply now an explicit, URL-driven choice
//! rather than a blind default hardcoded at the `connect()` call site.
//!
//! This mirrors `celers-broker-postgres`'s `src/tls_mode.rs`, which already
//! established this pattern for that crate's own `oxisql-postgres` /
//! `oxisql-mysql` call sites. Only the PostgreSQL half is needed here — this
//! crate's `postgres` feature has no MySQL connections.
//!
//! # Rules
//!
//! **PostgreSQL** — the `sslmode` query parameter:
//! - absent, or `sslmode=disable` → [`PgTlsWanted::No`]
//! - any other value (`require`, `verify-ca`, `verify-full`, `prefer`,
//!   `allow`, ...) → [`PgTlsWanted::Yes`]
//!
//! This intentionally treats `prefer`/`allow` as "TLS wanted" rather than
//! trying to implement libpq's opportunistic negotiation/fallback
//! semantics: this crate's connection helper has exactly one shot at
//! establishing a connection (no automatic downgrade-and-retry), so the
//! safe reading of "the caller mentioned a non-`disable` `sslmode`" is
//! "attempt TLS."
//!
//! # TLS config construction
//!
//! `oxisql-postgres` 0.3.2 does not expose a "standard TLS with webpki
//! roots" convenience constructor on `TlsMode` (confirmed by reading the
//! crate's `builder.rs`/`connection.rs`: the only helpers are
//! `TlsMode::skip_verify()` and `TlsMode::with_ca_pem()`, both
//! insecure-by-design or requiring caller-supplied CA material). The
//! crate's own doc comments document the standard path as building the
//! `rustls::ClientConfig` via the `oxitls` facade crate directly:
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let cfg = oxitls::client_config(oxitls::webpki_root_certs())?;
//! # Ok(())
//! # }
//! ```
//!
//! [`standard_rustls_config`] wraps exactly that call. `oxitls::client_config`
//! already returns `Arc<rustls::ClientConfig>`, which is the exact type
//! `TlsMode::Rustls` wraps — no extra `Arc::new()` needed.

use std::sync::Arc;

/// Whether a PostgreSQL connection URL requests TLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTlsWanted {
    /// No `sslmode` was present, or it was explicitly `disable`.
    No,
    /// `sslmode` requested TLS (`require`, `verify-ca`, `verify-full`,
    /// `prefer`, `allow`, or any other non-`disable` value).
    Yes,
}

/// Extract the raw query-string portion of a connection URL, if any.
///
/// Handles both a well-formed `scheme://...?query` URL (parsed via the
/// `url` crate) and a bare `?query` suffix on strings the `url` crate
/// rejects (e.g. malformed authority sections) by falling back to a plain
/// substring search. Connection strings in the libpq `key=value ...` form
/// (no `?`) yield `None`, which every caller below treats as "TLS not
/// requested" — consistent with the pre-migration `sqlx` behavior, since
/// that form has no query-string component to read a preference from.
fn query_string(url: &str) -> Option<String> {
    if let Ok(parsed) = url::Url::parse(url) {
        return parsed.query().map(str::to_string);
    }
    // Fallback: the `url` crate can reject strings with unusual authority
    // components (e.g. unescaped characters in a password) that are still
    // perfectly parseable for our narrow purpose of reading query params.
    url.split_once('?').map(|(_, query)| query.to_string())
}

/// Parse `key=value` pairs out of a raw query string, tolerating percent
/// encoding the same way the `url` crate's `form_urlencoded` does.
fn query_pairs(query: &str) -> impl Iterator<Item = (String, String)> + '_ {
    url::form_urlencoded::parse(query.as_bytes()).into_owned()
}

/// Look up the last occurrence of `key` (case-insensitive) in a raw query
/// string. Postgres connection URLs are not expected to repeat a TLS
/// parameter, but if a caller does, "last wins" matches how most URL/CLI
/// parsers resolve duplicate keys (and matches `url::Url::query_pairs`
/// iteration order, which yields pairs in source order).
fn find_query_param(query: &str, key: &str) -> Option<String> {
    query_pairs(query)
        .filter(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v)
        .last()
}

/// Determine whether a PostgreSQL connection URL requests TLS via its
/// `sslmode` query parameter.
///
/// See the [module docs](self) for the exact rules.
#[must_use]
pub fn pg_tls_wanted(url: &str) -> PgTlsWanted {
    let Some(query) = query_string(url) else {
        return PgTlsWanted::No;
    };
    match find_query_param(&query, "sslmode") {
        None => PgTlsWanted::No,
        Some(mode) if mode.eq_ignore_ascii_case("disable") => PgTlsWanted::No,
        Some(_) => PgTlsWanted::Yes,
    }
}

/// Build the standard `rustls::ClientConfig` (Mozilla/webpki trust roots via
/// `rustls-rustcrypto`, no FFI) that `oxisql-postgres::TlsMode::Rustls`
/// expects.
///
/// This is the exact construction documented in `oxisql-postgres`'s own doc
/// comments (`TlsMode::Rustls`'s doc example and the crate-level "Quick
/// start (TLS via OxiTLS)" example); the crate does not expose a shorter
/// "standard TLS" convenience constructor as of `oxisql-postgres` 0.3.2.
///
/// # Errors
///
/// Returns [`oxitls::TlsError`] if the `rustls::ClientConfig` cannot be
/// built (e.g. no default `CryptoProvider` available in this build — should
/// not happen with `oxitls`'s default `pure` feature, which always supplies
/// the `rustls-rustcrypto` provider explicitly rather than relying on a
/// process-global default).
pub fn standard_rustls_config() -> Result<Arc<rustls::ClientConfig>, oxitls::TlsError> {
    oxitls::client_config(oxitls::webpki_root_certs())
}

/// Resolve the [`oxisql_postgres::TlsMode`] to use for `url`, based solely
/// on that URL's `sslmode` query parameter.
///
/// # Errors
///
/// Returns an error (wrapped for [`anyhow`] callers) only when TLS *is*
/// requested and the standard `rustls::ClientConfig` fails to build; a URL
/// that does not request TLS never fails here.
pub fn pg_tls_mode_for_url(url: &str) -> anyhow::Result<oxisql_postgres::TlsMode> {
    match pg_tls_wanted(url) {
        PgTlsWanted::No => Ok(oxisql_postgres::TlsMode::Disabled),
        PgTlsWanted::Yes => {
            let cfg = standard_rustls_config()
                .map_err(|e| anyhow::anyhow!("failed to build TLS config for sslmode: {e}"))?;
            Ok(oxisql_postgres::TlsMode::Rustls(cfg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── pg_tls_wanted ────────────────────────────────────────────────────

    #[test]
    fn no_sslmode_param_means_no_tls() {
        assert_eq!(
            pg_tls_wanted("postgres://user:pass@localhost/db"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn no_query_string_at_all_means_no_tls() {
        assert_eq!(pg_tls_wanted("postgresql://localhost/db"), PgTlsWanted::No);
    }

    #[test]
    fn sslmode_disable_means_no_tls() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=disable"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn sslmode_disable_is_case_insensitive() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=DISABLE"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn sslmode_require_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=require"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn sslmode_verify_ca_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=verify-ca"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn sslmode_verify_full_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=verify-full"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn sslmode_prefer_means_tls_wanted() {
        // No downgrade-and-retry path exists at these call sites, so any
        // non-`disable` value is read as "attempt TLS."
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=prefer"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn sslmode_allow_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=allow"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn sslmode_alongside_other_params() {
        assert_eq!(
            pg_tls_wanted(
                "postgres://u:p@host:5432/db?connect_timeout=5&sslmode=require&application_name=celers"
            ),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn last_sslmode_wins_when_duplicated() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=require&sslmode=disable"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn kv_form_connection_string_means_no_tls() {
        // The libpq `key=value ...` form has no query-string component; the
        // caller can still request TLS by using `TlsMode::Rustls` directly
        // via a different code path, but this helper only reads URLs.
        assert_eq!(
            pg_tls_wanted("host=localhost port=5432 dbname=db sslmode=require"),
            PgTlsWanted::No
        );
    }

    // ── pg_tls_mode_for_url ─────────────────────────────────────────────

    #[test]
    fn tls_mode_for_url_disabled_when_not_requested() {
        let mode =
            pg_tls_mode_for_url("postgres://localhost/db").expect("no-TLS resolution never fails");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Disabled));
    }

    #[test]
    fn tls_mode_for_url_disabled_when_sslmode_disable() {
        let mode = pg_tls_mode_for_url("postgres://localhost/db?sslmode=disable")
            .expect("no-TLS resolution never fails");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Disabled));
    }

    #[test]
    fn tls_mode_for_url_rustls_when_require() {
        let mode = pg_tls_mode_for_url("postgres://localhost/db?sslmode=require")
            .expect("standard rustls config must build in test environment");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Rustls(_)));
    }

    #[test]
    fn tls_mode_for_url_rustls_when_verify_ca() {
        let mode = pg_tls_mode_for_url("postgres://localhost/db?sslmode=verify-ca")
            .expect("standard rustls config must build in test environment");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Rustls(_)));
    }

    #[test]
    fn tls_mode_for_url_rustls_when_verify_full() {
        let mode = pg_tls_mode_for_url("postgres://localhost/db?sslmode=verify-full")
            .expect("standard rustls config must build in test environment");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Rustls(_)));
    }

    // ── standard_rustls_config ──────────────────────────────────────────

    #[test]
    fn standard_rustls_config_builds_successfully() {
        standard_rustls_config().expect("standard rustls config must build in test environment");
    }
}
