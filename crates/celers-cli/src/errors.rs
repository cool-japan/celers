//! Structured CLI errors: stable error codes, actionable suggestions, and an
//! upgrade path from the crate's existing `anyhow`-based error handling.
//!
//! The rest of the crate builds errors with `anyhow::anyhow!`/`anyhow::bail!`
//! and `.context(...)`, which is great for propagation but gives the end
//! user very little to act on beyond a free-form message. [`CliError`] adds
//! a small, stable layer on top:
//!
//! - [`CliError::code`] — a short, stable, greppable identifier (e.g.
//!   `"E_CONN_REFUSED"`) suitable for documentation links, support tickets,
//!   and scripting (`if [ "$code" = "E_TIMEOUT" ]; then ...`).
//! - [`CliError::suggestion`] — one actionable sentence describing the most
//!   likely fix (this doubles as the "common fixes" / "debug hint" data for
//!   the variant).
//! - [`CliError::error_code_reference`] — a full table of every known code,
//!   message template, and suggestion, intended to back a future
//!   `--help`-adjacent command (e.g. `celers help errors`).
//! - [`classify_anyhow`] — a best-effort decorator that inspects an existing
//!   [`anyhow::Error`]'s full message chain and classifies it into the
//!   closest matching [`CliError`] variant, so call sites that already
//!   return `anyhow::Result` can be upgraded to structured errors from a
//!   single integration point (e.g. `main`'s top-level error handler)
//!   instead of rewriting every individual call site.
//!
//! # Examples
//!
//! ```
//! use celers_cli::errors::CliError;
//!
//! let err = CliError::InvalidQueue {
//!     queue: "no-such-queue".to_string(),
//! };
//! assert_eq!(err.code(), "E_INVALID_QUEUE");
//!
//! // `Display` already includes the code and the suggestion.
//! let rendered = err.to_string();
//! assert!(rendered.contains(err.code()));
//! assert!(rendered.contains(err.suggestion()));
//! ```

use crate::command_utils::render_table_string;
use thiserror::Error;

// ── Error codes ──────────────────────────────────────────────────────────
//
// One `const` per variant so the code embedded in the `#[error(...)]`
// Display template and the code returned by `CliError::code` are always the
// same string (read from the same constant, so they cannot drift apart).

const CODE_CONN_REFUSED: &str = "E_CONN_REFUSED";
const CODE_BAD_BROKER_URL: &str = "E_BAD_BROKER_URL";
const CODE_INVALID_QUEUE: &str = "E_INVALID_QUEUE";
const CODE_CONFIG_INVALID: &str = "E_CONFIG_INVALID";
const CODE_AUTH_FAILED: &str = "E_AUTH_FAILED";
const CODE_TIMEOUT: &str = "E_TIMEOUT";
const CODE_UNKNOWN: &str = "E_UNKNOWN";

// ── Suggestions ──────────────────────────────────────────────────────────
//
// One actionable sentence per variant ("actionable suggestions" + "common
// fixes" + "debug hints" all live here as plain data). Shared between the
// `#[error(...)]` Display template (so `Display` includes the suggestion)
// and `CliError::suggestion` — same constant, so there is exactly one place
// to update the wording.

const SUG_CONN_REFUSED: &str = "Verify the broker is running and reachable at the configured URL (try `redis-cli -u <url> ping` for Redis or `pg_isready -d <url>` for PostgreSQL), and that no firewall or security group is blocking the port.";
const SUG_BAD_BROKER_URL: &str = "Use a URL of the form `redis://host:port/db`, `postgres://user:pass@host:port/db`, or `amqp://user:pass@host:port/vhost`; check for a missing scheme or typo, or pass --broker explicitly.";
const SUG_INVALID_QUEUE: &str = "Pass --queue <name>, set broker.queue in the config file, or run `celers queue list` to see the queues that currently exist.";
const SUG_CONFIG_INVALID: &str = "Run `celers validate --config <path>` to see the full list of problems, and compare the file against a freshly generated `celers init --output <path>`.";
const SUG_AUTH_FAILED: &str = "Check the username/password or auth token embedded in the broker URL, and confirm the account has permission to access the target broker or database.";
const SUG_TIMEOUT: &str = "Increase --timeout / worker.default_timeout_secs, check network latency to the broker, and confirm the broker is not overloaded or unreachable.";
const SUG_UNKNOWN: &str = "Re-run with --log-level debug for more detail; if the error persists and looks unexpected, please file an issue with the full command and output.";

// ── Message templates ───────────────────────────────────────────────────
//
// The bare, unrendered message shape (field placeholders shown literally,
// e.g. `{detail}`) for `error_code_reference`'s reference table. Kept
// separate from the rendered `#[error(...)]` Display output, which
// additionally carries the code and the suggestion.

const TEMPLATE_CONN_REFUSED: &str = "connection refused: {detail}";
const TEMPLATE_BAD_BROKER_URL: &str = "invalid broker URL: {url}";
const TEMPLATE_INVALID_QUEUE: &str = "missing or invalid queue: {queue}";
const TEMPLATE_CONFIG_INVALID: &str = "configuration validation failed: {detail}";
const TEMPLATE_AUTH_FAILED: &str = "authentication failed: {detail}";
const TEMPLATE_TIMEOUT: &str = "operation timed out: {detail}";
const TEMPLATE_UNKNOWN: &str = "{detail}";

/// A structured CLI error carrying a stable code and an actionable
/// suggestion alongside its human-readable message.
///
/// Every variant's [`Display`](std::fmt::Display) output includes the
/// variant's [`code`](CliError::code) and
/// [`suggestion`](CliError::suggestion), so simply printing a `CliError`
/// (e.g. `eprintln!("{err}")`) already gives the user everything they need:
/// what happened, a stable code to search on, and what to try next.
#[derive(Debug, Error)]
pub enum CliError {
    /// The CLI could not open a connection to the broker (or backend);
    /// typically the target process is not running or is unreachable.
    #[error(
        "[{code}] connection refused: {detail} (suggestion: {sugg})",
        code = CODE_CONN_REFUSED,
        sugg = SUG_CONN_REFUSED
    )]
    ConnectionRefused {
        /// Underlying error text describing what was refused.
        detail: String,
    },

    /// The broker URL supplied via `--broker`, the environment, or the
    /// config file could not be parsed, or is not a recognized scheme.
    #[error(
        "[{code}] invalid broker URL: {url} (suggestion: {sugg})",
        code = CODE_BAD_BROKER_URL,
        sugg = SUG_BAD_BROKER_URL
    )]
    InvalidBrokerUrl {
        /// The offending URL, or the error text describing why it is
        /// invalid when the raw URL itself is not available.
        url: String,
    },

    /// No queue was specified and none could be inferred, or the requested
    /// queue does not exist.
    #[error(
        "[{code}] missing or invalid queue: {queue} (suggestion: {sugg})",
        code = CODE_INVALID_QUEUE,
        sugg = SUG_INVALID_QUEUE
    )]
    InvalidQueue {
        /// The requested queue name, or a description when none was given.
        queue: String,
    },

    /// The resolved configuration failed validation.
    #[error(
        "[{code}] configuration validation failed: {detail} (suggestion: {sugg})",
        code = CODE_CONFIG_INVALID,
        sugg = SUG_CONFIG_INVALID
    )]
    ConfigValidation {
        /// Description of what failed validation.
        detail: String,
    },

    /// The broker or backend rejected the supplied credentials.
    #[error(
        "[{code}] authentication failed: {detail} (suggestion: {sugg})",
        code = CODE_AUTH_FAILED,
        sugg = SUG_AUTH_FAILED
    )]
    AuthFailure {
        /// Underlying authentication failure text.
        detail: String,
    },

    /// An operation exceeded its allotted time budget.
    #[error(
        "[{code}] operation timed out: {detail} (suggestion: {sugg})",
        code = CODE_TIMEOUT,
        sugg = SUG_TIMEOUT
    )]
    Timeout {
        /// Description of which operation timed out.
        detail: String,
    },

    /// A fallback for failures that do not match any of the more specific
    /// variants above. [`classify_anyhow`] never fails to classify; this is
    /// its default when nothing more specific matches.
    #[error(
        "[{code}] {detail} (suggestion: {sugg})",
        code = CODE_UNKNOWN,
        sugg = SUG_UNKNOWN
    )]
    Other {
        /// The original error message, preserved verbatim.
        detail: String,
    },
}

impl CliError {
    /// A stable, short, greppable identifier for this error, e.g.
    /// `"E_CONN_REFUSED"`.
    ///
    /// Codes never change meaning once published; new failure modes get new
    /// codes rather than repurposing existing ones.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ConnectionRefused { .. } => CODE_CONN_REFUSED,
            Self::InvalidBrokerUrl { .. } => CODE_BAD_BROKER_URL,
            Self::InvalidQueue { .. } => CODE_INVALID_QUEUE,
            Self::ConfigValidation { .. } => CODE_CONFIG_INVALID,
            Self::AuthFailure { .. } => CODE_AUTH_FAILED,
            Self::Timeout { .. } => CODE_TIMEOUT,
            Self::Other { .. } => CODE_UNKNOWN,
        }
    }

    /// One actionable sentence describing the most likely fix.
    #[must_use]
    pub fn suggestion(&self) -> &'static str {
        match self {
            Self::ConnectionRefused { .. } => SUG_CONN_REFUSED,
            Self::InvalidBrokerUrl { .. } => SUG_BAD_BROKER_URL,
            Self::InvalidQueue { .. } => SUG_INVALID_QUEUE,
            Self::ConfigValidation { .. } => SUG_CONFIG_INVALID,
            Self::AuthFailure { .. } => SUG_AUTH_FAILED,
            Self::Timeout { .. } => SUG_TIMEOUT,
            Self::Other { .. } => SUG_UNKNOWN,
        }
    }

    /// Every known error code paired with its bare message template (field
    /// placeholders shown literally, e.g. `{detail}`) and its suggestion.
    ///
    /// Intended to back a future `--help`-adjacent command that prints the
    /// full error code reference (e.g. `celers help errors`); this function
    /// only supplies the data — no command is wired up to print it yet.
    #[must_use]
    pub fn error_code_reference() -> Vec<(&'static str, &'static str, &'static str)> {
        vec![
            (CODE_CONN_REFUSED, TEMPLATE_CONN_REFUSED, SUG_CONN_REFUSED),
            (
                CODE_BAD_BROKER_URL,
                TEMPLATE_BAD_BROKER_URL,
                SUG_BAD_BROKER_URL,
            ),
            (
                CODE_INVALID_QUEUE,
                TEMPLATE_INVALID_QUEUE,
                SUG_INVALID_QUEUE,
            ),
            (
                CODE_CONFIG_INVALID,
                TEMPLATE_CONFIG_INVALID,
                SUG_CONFIG_INVALID,
            ),
            (CODE_AUTH_FAILED, TEMPLATE_AUTH_FAILED, SUG_AUTH_FAILED),
            (CODE_TIMEOUT, TEMPLATE_TIMEOUT, SUG_TIMEOUT),
            (CODE_UNKNOWN, TEMPLATE_UNKNOWN, SUG_UNKNOWN),
        ]
    }
}

/// Render [`CliError::error_code_reference`] as a human-readable table
/// string with `Code` / `Message Template` / `Suggestion` columns.
///
/// Pure formatting (no I/O), so it can be exercised directly by unit tests;
/// [`print_error_code_reference`] is a thin `println!` wrapper around this
/// that backs the `celers error-codes` command.
#[must_use]
pub fn render_error_code_reference() -> String {
    let rows: Vec<Vec<String>> = CliError::error_code_reference()
        .into_iter()
        .map(|(code, template, suggestion)| {
            vec![
                code.to_string(),
                template.to_string(),
                suggestion.to_string(),
            ]
        })
        .collect();

    render_table_string(&["Code", "Message Template", "Suggestion"], &rows)
}

/// Print the full error-code reference table to stdout (see
/// [`render_error_code_reference`]).
///
/// Backs the `celers error-codes` command.
pub fn print_error_code_reference() {
    println!("{}", render_error_code_reference());
}

/// Classify an existing [`anyhow::Error`] into the closest matching
/// [`CliError`] variant by inspecting its full display chain (top-level
/// message plus every `.context(...)`/source cause) for common substrings.
///
/// This lets existing call sites that already return `anyhow::Result` be
/// upgraded to structured, actionable errors from a single integration
/// point (e.g. `main`'s top-level error handler) rather than requiring every
/// individual `anyhow::bail!`/`anyhow::anyhow!` call site to be rewritten.
///
/// Falls back to [`CliError::Other`] (code `"E_UNKNOWN"`) when nothing more
/// specific matches; this function never panics and always returns a usable
/// [`CliError`].
#[must_use]
pub fn classify_anyhow(err: &anyhow::Error) -> CliError {
    // The alternate `Display` (`{:#}`) prints the top-level message followed
    // by every cause in the chain, each joined with `": "` — this lets a
    // substring match find e.g. "connection refused" even when it is buried
    // a few `.context(...)` calls deep rather than only in the top message.
    let full_message = format!("{err:#}");
    let haystack = full_message.to_lowercase();

    if contains_any(
        &haystack,
        &[
            "connection refused",
            "refused the connection",
            "econnrefused",
        ],
    ) {
        return CliError::ConnectionRefused {
            detail: full_message,
        };
    }

    if contains_any(
        &haystack,
        &[
            "invalid broker url",
            "invalid url",
            "relative url without a base",
            "url parse error",
            "unsupported broker scheme",
        ],
    ) {
        return CliError::InvalidBrokerUrl { url: full_message };
    }

    if haystack.contains("queue")
        && contains_any(
            &haystack,
            &[
                "invalid",
                "not found",
                "does not exist",
                "unknown queue",
                "cannot be empty",
                "no such queue",
            ],
        )
    {
        return CliError::InvalidQueue {
            queue: full_message,
        };
    }

    if contains_any(
        &haystack,
        &[
            "unauthorized",
            "authentication failed",
            "auth failed",
            "noauth",
            "permission denied",
            "access denied",
            "invalid password",
            "wrong password",
        ],
    ) {
        return CliError::AuthFailure {
            detail: full_message,
        };
    }

    if contains_any(&haystack, &["timed out", "timeout", "deadline exceeded"]) {
        return CliError::Timeout {
            detail: full_message,
        };
    }

    if haystack.contains("config")
        && contains_any(
            &haystack,
            &[
                "invalid",
                "validation failed",
                "failed to parse",
                "missing required",
            ],
        )
    {
        return CliError::ConfigValidation {
            detail: full_message,
        };
    }

    CliError::Other {
        detail: full_message,
    }
}

/// True if `haystack` contains any of `needles` as a substring.
fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One representative instance of every [`CliError`] variant, used to
    /// exercise `code`/`suggestion`/`Display` generically across all
    /// variants instead of duplicating the same assertions per variant.
    fn sample_variants() -> Vec<CliError> {
        vec![
            CliError::ConnectionRefused {
                detail: "os error 61".to_string(),
            },
            CliError::InvalidBrokerUrl {
                url: "not-a-url".to_string(),
            },
            CliError::InvalidQueue {
                queue: "no-such-queue".to_string(),
            },
            CliError::ConfigValidation {
                detail: "broker.url is empty".to_string(),
            },
            CliError::AuthFailure {
                detail: "NOAUTH Authentication required".to_string(),
            },
            CliError::Timeout {
                detail: "30s elapsed".to_string(),
            },
            CliError::Other {
                detail: "totally unexpected".to_string(),
            },
        ]
    }

    #[test]
    fn every_variant_has_a_non_empty_code_and_suggestion() {
        for variant in sample_variants() {
            assert!(!variant.code().is_empty(), "empty code for {variant:?}");
            assert!(
                !variant.suggestion().is_empty(),
                "empty suggestion for {variant:?}"
            );
        }
    }

    #[test]
    fn codes_are_unique() {
        let samples = sample_variants();
        let mut codes: Vec<&str> = samples.iter().map(CliError::code).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), samples.len(), "duplicate error codes found");
    }

    #[test]
    fn display_includes_code_and_suggestion() {
        for variant in sample_variants() {
            let rendered = variant.to_string();
            assert!(
                rendered.contains(variant.code()),
                "Display for {variant:?} does not mention its code: {rendered}"
            );
            assert!(
                rendered.contains(variant.suggestion()),
                "Display for {variant:?} does not mention its suggestion: {rendered}"
            );
        }
    }

    #[test]
    fn error_code_reference_covers_every_variant() {
        let reference = CliError::error_code_reference();
        let samples = sample_variants();
        assert_eq!(reference.len(), samples.len());

        for variant in &samples {
            let entry = reference
                .iter()
                .find(|(code, _, _)| *code == variant.code())
                .unwrap_or_else(|| panic!("error_code_reference() missing entry for {variant:?}"));
            assert!(!entry.1.is_empty(), "empty template for {variant:?}");
            assert_eq!(entry.2, variant.suggestion());
        }
    }

    #[test]
    fn render_error_code_reference_contains_expected_codes_and_headers() {
        let rendered = render_error_code_reference();

        assert!(
            rendered.contains("Code"),
            "table missing 'Code' header:\n{rendered}"
        );
        assert!(
            rendered.contains("Message Template"),
            "table missing 'Message Template' header:\n{rendered}"
        );
        assert!(
            rendered.contains("Suggestion"),
            "table missing 'Suggestion' header:\n{rendered}"
        );
        assert!(
            rendered.contains("E_CONN_REFUSED"),
            "table missing E_CONN_REFUSED:\n{rendered}"
        );
        assert!(
            rendered.contains("E_UNKNOWN"),
            "table missing E_UNKNOWN:\n{rendered}"
        );

        // Every code/suggestion pair from the reference data should be
        // present verbatim in the rendered table.
        for (code, _, suggestion) in CliError::error_code_reference() {
            assert!(
                rendered.contains(code),
                "table missing code {code}:\n{rendered}"
            );
            assert!(
                rendered.contains(suggestion),
                "table missing suggestion for {code}:\n{rendered}"
            );
        }
    }

    #[test]
    fn classify_anyhow_detects_connection_refused() {
        let err = anyhow::anyhow!("Connection refused (os error 61)");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_CONN_REFUSED");
        assert!(matches!(classified, CliError::ConnectionRefused { .. }));
    }

    #[test]
    fn classify_anyhow_detects_invalid_broker_url() {
        let err = anyhow::anyhow!("invalid broker url: relative URL without a base");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_BAD_BROKER_URL");
    }

    #[test]
    fn classify_anyhow_detects_invalid_queue() {
        let err = anyhow::anyhow!("invalid queue: no such queue 'orders-fast'");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_INVALID_QUEUE");
    }

    #[test]
    fn classify_anyhow_detects_auth_failure() {
        let err = anyhow::anyhow!("authentication failed: NOAUTH Authentication required");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_AUTH_FAILED");
    }

    #[test]
    fn classify_anyhow_detects_timeout() {
        let err = anyhow::anyhow!("operation timed out after 30s");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_TIMEOUT");
    }

    #[test]
    fn classify_anyhow_detects_config_validation() {
        let err = anyhow::anyhow!("invalid config: missing required field broker.url");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_CONFIG_INVALID");
    }

    #[test]
    fn classify_anyhow_falls_back_to_other() {
        let err = anyhow::anyhow!("something completely unexpected happened");
        let classified = classify_anyhow(&err);
        assert_eq!(classified.code(), "E_UNKNOWN");
        assert!(matches!(classified, CliError::Other { .. }));
    }

    #[test]
    fn classify_anyhow_inspects_the_full_chain_not_just_the_top_message() {
        let root = anyhow::anyhow!("Connection refused (os error 61)");
        let wrapped = root.context("failed to connect to broker");
        let classified = classify_anyhow(&wrapped);
        assert_eq!(classified.code(), "E_CONN_REFUSED");
    }
}
