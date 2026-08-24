//! URL-driven TLS mode selection for OxiSQL-backed database connections.
//!
//! # Background
//!
//! The previous `sqlx`-based connection code respected the standard
//! `sslmode=`/`ssl-mode=` (and MySQL's `tls=`) query parameters embedded in a
//! database connection URL: a caller who wrote `sslmode=require` in their
//! connection string got a TLS-encrypted connection, and a caller who wrote
//! nothing (or `sslmode=disable`) got plain-text.
//!
//! The `sqlx` → `oxisql` migration replaced every `sqlx::PgPoolOptions` /
//! `sqlx::MySqlPoolOptions` call with `oxisql_postgres::PgConnection::connect`
//! / `oxisql_mysql::MyConnection::connect`, both of which take an explicit
//! [`oxisql_postgres::TlsMode`] / [`oxisql_mysql::TlsMode`] value as a second
//! argument rather than inferring one from the URL. Several call sites in
//! this crate were migrated by hardcoding `TlsMode::Disabled` — silently
//! downgrading any connection to plain-text even when the URL explicitly
//! asked for TLS via `sslmode=require`. That is a MEDIUM-severity regression:
//! a caller who believes they are encrypting traffic (because their
//! connection string says so) is unknowingly sending credentials and query
//! data in the clear.
//!
//! This module restores the pre-migration behavior by parsing the caller's
//! connection URL for the TLS-preference query parameter(s) and building the
//! matching [`oxisql_postgres::TlsMode`] / [`oxisql_mysql::TlsMode`] value.
//! `TlsMode::Disabled` remains a legitimate outcome — it is simply now an
//! explicit, URL-driven choice rather than a blind default hardcoded at every
//! `connect()` call site.
//!
//! This mirrors `celers-cli`'s `src/tls_mode.rs` verbatim — this crate needs
//! both the PostgreSQL and MySQL halves (like `celers-cli`), unlike
//! `celers-broker-postgres`'s PG-only variant, since this crate's
//! `PostgresResultBackend` and `MysqlResultBackend` both connect via URL.
//!
//! # Rules
//!
//! **PostgreSQL** — the `sslmode` query parameter:
//! - absent, or `sslmode=disable` → [`PgTlsWanted::No`]
//! - any other value (`require`, `verify-ca`, `verify-full`, `prefer`,
//!   `allow`, ...) → [`PgTlsWanted::Yes`]
//!
//! This intentionally treats `prefer`/`allow` as "TLS wanted" rather than
//! trying to implement libpq's opportunistic negotiation/fallback semantics:
//! this crate's connection helpers have exactly one shot at establishing a
//! connection (no automatic downgrade-and-retry), so the safe reading of
//! "the caller mentioned a non-`disable` `sslmode`" is "attempt TLS."
//!
//! **MySQL** — the `ssl-mode` and `tls` query parameters:
//! - `ssl-mode` absent/empty and `tls` absent/empty/`false` → [`MySqlTlsWanted::No`]
//! - `ssl-mode=disabled` (case-insensitive) and no conflicting `tls=true` →
//!   [`MySqlTlsWanted::No`]
//! - any other `ssl-mode` value, or `tls=true` → [`MySqlTlsWanted::Yes`]
//!
//! # TLS config construction
//!
//! Neither `oxisql-postgres` nor `oxisql-mysql` 0.3.2 expose a "standard TLS
//! with webpki roots" convenience constructor on `TlsMode` (confirmed by
//! reading both crates' `builder.rs`/`connection.rs`: the only helpers are
//! `TlsMode::skip_verify()` and `TlsMode::with_ca_pem()`, both
//! insecure-by-design or requiring caller-supplied CA material). Both
//! crates' own doc comments document the standard path as building the
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
//! `TlsMode::Rustls` wraps for both backends — no extra `Arc::new()` needed.

use std::sync::Arc;

/// Whether a PostgreSQL connection URL requests TLS.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTlsWanted {
    /// No `sslmode` was present, or it was explicitly `disable`.
    No,
    /// `sslmode` requested TLS (`require`, `verify-ca`, `verify-full`,
    /// `prefer`, `allow`, or any other non-`disable` value).
    Yes,
}

/// Whether a MySQL connection URL requests TLS.
#[cfg_attr(not(feature = "mysql"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlTlsWanted {
    /// Neither `ssl-mode` nor `tls` requested TLS.
    No,
    /// `ssl-mode` and/or `tls` requested TLS.
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
/// string. Postgres/MySQL connection URLs are not expected to repeat a TLS
/// parameter, but if a caller does, "last wins" matches how most URL/CLI
/// parsers resolve duplicate keys (and matches `url::Url::query_pairs`
/// iteration order, which yields pairs in source order).
///
/// Uses `.last()` rather than `.next_back()`: the `form_urlencoded::parse`
/// iterator only implements the forward `Iterator` trait (its `impl
/// Iterator` return type here erases the concrete type's `DoubleEndedIterator`
/// bound), so reversing requires consuming the whole sequence forward.
/// Connection-string query strings are a handful of parameters at most, so
/// the full traversal is immaterial.
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
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
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

/// Determine whether a MySQL connection URL requests TLS via its
/// `ssl-mode`/`tls` query parameters.
///
/// See the [module docs](self) for the exact rules.
#[cfg_attr(not(feature = "mysql"), allow(dead_code))]
#[must_use]
pub fn mysql_tls_wanted(url: &str) -> MySqlTlsWanted {
    let Some(query) = query_string(url) else {
        return MySqlTlsWanted::No;
    };

    let tls_flag = find_query_param(&query, "tls");
    let tls_true = tls_flag
        .as_deref()
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    if tls_true {
        return MySqlTlsWanted::Yes;
    }

    match find_query_param(&query, "ssl-mode") {
        None => MySqlTlsWanted::No,
        Some(mode) if mode.eq_ignore_ascii_case("disabled") => MySqlTlsWanted::No,
        Some(_) => MySqlTlsWanted::Yes,
    }
}

/// Build the standard `rustls::ClientConfig` (Mozilla/webpki trust roots via
/// `rustls-rustcrypto`, no FFI) that both `oxisql-postgres::TlsMode::Rustls`
/// and `oxisql-mysql::TlsMode::Rustls` expect.
///
/// This is the exact construction documented in both crates' own doc
/// comments (`oxisql_postgres::TlsMode::Rustls`'s doc example and
/// `oxisql-postgres`'s crate-level "Quick start (TLS via OxiTLS)" example);
/// neither crate exposes a shorter "standard TLS" convenience constructor as
/// of `oxisql-postgres`/`oxisql-mysql` 0.3.2.
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

/// Resolve the [`oxisql_postgres::TlsMode`] to use for `url`, based solely on
/// that URL's `sslmode` query parameter.
///
/// # Errors
///
/// Returns an error (wrapped for [`anyhow`] callers) only when TLS *is*
/// requested and the standard `rustls::ClientConfig` fails to build; a URL
/// that does not request TLS never fails here.
#[cfg(feature = "postgres")]
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

/// Resolve the [`oxisql_mysql::TlsMode`] to use for `url`, based solely on
/// that URL's `ssl-mode`/`tls` query parameters.
///
/// # Errors
///
/// Returns an error (wrapped for [`anyhow`] callers) only when TLS *is*
/// requested and the standard `rustls::ClientConfig` fails to build; a URL
/// that does not request TLS never fails here.
#[cfg(feature = "mysql")]
pub fn mysql_tls_mode_for_url(url: &str) -> anyhow::Result<oxisql_mysql::TlsMode> {
    match mysql_tls_wanted(url) {
        MySqlTlsWanted::No => Ok(oxisql_mysql::TlsMode::Disabled),
        MySqlTlsWanted::Yes => {
            let cfg = standard_rustls_config()
                .map_err(|e| anyhow::anyhow!("failed to build TLS config for ssl-mode/tls: {e}"))?;
            Ok(oxisql_mysql::TlsMode::Rustls(cfg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── PostgreSQL: sslmode ─────────────────────────────────────────────

    #[test]
    fn pg_no_sslmode_param_means_no_tls() {
        assert_eq!(
            pg_tls_wanted("postgres://user:pass@localhost/db"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn pg_no_query_string_at_all_means_no_tls() {
        assert_eq!(pg_tls_wanted("postgresql://localhost/db"), PgTlsWanted::No);
    }

    #[test]
    fn pg_sslmode_disable_means_no_tls() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=disable"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn pg_sslmode_disable_is_case_insensitive() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=DISABLE"),
            PgTlsWanted::No
        );
    }

    #[test]
    fn pg_sslmode_require_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=require"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn pg_sslmode_verify_ca_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=verify-ca"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn pg_sslmode_verify_full_means_tls_wanted() {
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=verify-full"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn pg_sslmode_prefer_means_tls_wanted() {
        // No downgrade-and-retry path exists at these call sites, so any
        // non-`disable` value is read as "attempt TLS."
        assert_eq!(
            pg_tls_wanted("postgres://localhost/db?sslmode=prefer"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn pg_sslmode_alongside_other_params() {
        assert_eq!(
            pg_tls_wanted("postgres://u:p@host:5432/db?connect_timeout=5&sslmode=require&application_name=celers"),
            PgTlsWanted::Yes
        );
    }

    #[test]
    fn pg_kv_form_connection_string_means_no_tls() {
        // The libpq `key=value ...` form has no query-string component; the
        // caller can still request TLS by using `PgTlsMode::Rustls` directly
        // via a different code path, but this helper only reads URLs.
        assert_eq!(
            pg_tls_wanted("host=localhost port=5432 dbname=db sslmode=require"),
            PgTlsWanted::No
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn pg_tls_mode_for_url_disabled_when_not_requested() {
        let mode =
            pg_tls_mode_for_url("postgres://localhost/db").expect("no-TLS resolution never fails");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Disabled));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn pg_tls_mode_for_url_rustls_when_requested() {
        let mode = pg_tls_mode_for_url("postgres://localhost/db?sslmode=require")
            .expect("standard rustls config must build in test environment");
        assert!(matches!(mode, oxisql_postgres::TlsMode::Rustls(_)));
    }

    // ── MySQL: ssl-mode / tls ───────────────────────────────────────────

    #[test]
    fn mysql_no_params_means_no_tls() {
        assert_eq!(
            mysql_tls_wanted("mysql://user:pass@localhost/db"),
            MySqlTlsWanted::No
        );
    }

    #[test]
    fn mysql_ssl_mode_disabled_means_no_tls() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?ssl-mode=disabled"),
            MySqlTlsWanted::No
        );
    }

    #[test]
    fn mysql_ssl_mode_disabled_is_case_insensitive() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?ssl-mode=DISABLED"),
            MySqlTlsWanted::No
        );
    }

    #[test]
    fn mysql_tls_false_means_no_tls() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?tls=false"),
            MySqlTlsWanted::No
        );
    }

    #[test]
    fn mysql_ssl_mode_required_means_tls_wanted() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?ssl-mode=required"),
            MySqlTlsWanted::Yes
        );
    }

    #[test]
    fn mysql_ssl_mode_verify_identity_means_tls_wanted() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?ssl-mode=verify_identity"),
            MySqlTlsWanted::Yes
        );
    }

    #[test]
    fn mysql_tls_true_means_tls_wanted() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?tls=true"),
            MySqlTlsWanted::Yes
        );
    }

    #[test]
    fn mysql_tls_true_is_case_insensitive() {
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?tls=TRUE"),
            MySqlTlsWanted::Yes
        );
    }

    #[test]
    fn mysql_tls_true_wins_over_ssl_mode_disabled() {
        // `tls=true` is an unambiguous request for TLS; do not let a
        // conflicting `ssl-mode=disabled` silently win.
        assert_eq!(
            mysql_tls_wanted("mysql://localhost/db?ssl-mode=disabled&tls=true"),
            MySqlTlsWanted::Yes
        );
    }

    #[test]
    fn mysql_ssl_mode_alongside_other_params() {
        assert_eq!(
            mysql_tls_wanted("mysql://u:p@host:3306/db?connectTimeout=5000&ssl-mode=required"),
            MySqlTlsWanted::Yes
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_tls_mode_for_url_disabled_when_not_requested() {
        let mode =
            mysql_tls_mode_for_url("mysql://localhost/db").expect("no-TLS resolution never fails");
        assert!(matches!(mode, oxisql_mysql::TlsMode::Disabled));
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_tls_mode_for_url_rustls_when_requested() {
        let mode = mysql_tls_mode_for_url("mysql://localhost/db?ssl-mode=required")
            .expect("standard rustls config must build in test environment");
        assert!(matches!(mode, oxisql_mysql::TlsMode::Rustls(_)));
    }

    // ── standard_rustls_config ──────────────────────────────────────────

    #[test]
    fn standard_rustls_config_builds_successfully() {
        standard_rustls_config().expect("standard rustls config must build in test environment");
    }
}
