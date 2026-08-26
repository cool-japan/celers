//! Broker URL validation shared by [`crate::config::Config::validate`]
//! (`celers validate`) and [`crate::commands::start_worker`] (`celers
//! worker` startup) -- see prod-gaps-11: neither used to inspect
//! `broker.url` at all, so a malformed or type-mismatched URL passed both
//! checks clean and the failure only surfaced later, mid-connection.
//!
//! This mirrors the shape of `celers::config_validation::validate_broker_url`
//! in the `celers` facade crate (same checks: non-empty, has a `scheme://`
//! separator, scheme is one of the known broker schemes) without depending on
//! that crate -- `celers-cli` intentionally carries no dependency on the
//! `celers` facade, so the logic is duplicated here rather than imported.

/// Broker connection schemes this codebase recognizes, across every broker
/// backend (`redis`/`rediss`, `postgres`/`postgresql`, `mysql`,
/// `amqp`/`amqps`, `sqs`) -- not only the ones `celers-cli`'s own `celers
/// worker` can start a worker against (Redis only, today; see
/// `commands::worker`).
pub const KNOWN_BROKER_SCHEMES: &[&str] = &[
    "redis",
    "rediss",
    "postgres",
    "postgresql",
    "mysql",
    "amqp",
    "amqps",
    "sqs",
];

/// Reject a broker URL that is structurally unusable: empty, or missing a
/// `scheme://` separator entirely.
///
/// This is deliberately narrow -- it is the **hard-error** half of
/// validation. An unrecognized-but-well-formed scheme (`"foo://host"`) is a
/// *warning* (see [`scheme_warning`]), not rejected here, because a broker
/// backend this particular build was not compiled with is still a
/// well-formed URL and might be valid for a different `celers-cli` build or
/// a future backend.
///
/// The returned message always contains the literal phrase `"invalid broker
/// url"` so [`crate::errors::classify_anyhow`] classifies the resulting
/// error as `E_BAD_BROKER_URL` rather than falling back to `E_UNKNOWN`.
///
/// # Errors
///
/// Returns `Err(String)` if `url` is empty (or all whitespace) or has no
/// `://`.
pub fn require_well_formed(url: &str) -> Result<(), String> {
    if url.trim().is_empty() {
        return Err("invalid broker url: broker.url cannot be empty".to_string());
    }
    if !url.contains("://") {
        return Err(format!(
            "invalid broker url '{url}': missing a 'scheme://' separator"
        ));
    }
    Ok(())
}

/// The scheme portion of a broker URL: the text before the first `://`, or
/// the whole string if it contains no `://` (callers normally run this only
/// after [`require_well_formed`] has already rejected that case).
#[must_use]
pub fn scheme_of(url: &str) -> &str {
    url.split("://").next().unwrap_or(url)
}

/// A warning message if `url`'s scheme is not one of [`KNOWN_BROKER_SCHEMES`],
/// else `None`.
///
/// Unlike [`require_well_formed`], an unrecognized scheme is advisory, not
/// fatal: `celers validate`/`celers config` must not hard-fail a URL that is
/// merely unfamiliar to this validator.
#[must_use]
pub fn scheme_warning(url: &str) -> Option<String> {
    let scheme = scheme_of(url);
    if KNOWN_BROKER_SCHEMES.contains(&scheme) {
        None
    } else {
        Some(format!(
            "unsupported broker scheme '{scheme}' in broker.url '{url}'; expected one of: {}",
            KNOWN_BROKER_SCHEMES.join(", ")
        ))
    }
}

/// The scheme(s) a `broker.type` value should agree with, or `None` if
/// `broker_type` is not itself one of the recognized types (in which case
/// [`Config::validate`](crate::config::Config::validate) already emits its
/// own "Unknown broker type" warning, and piling a second, redundant warning
/// about the scheme on top would just be noise).
fn expected_schemes_for(broker_type: &str) -> Option<&'static [&'static str]> {
    match broker_type.to_lowercase().as_str() {
        "redis" => Some(&["redis", "rediss"]),
        "postgres" | "postgresql" => Some(&["postgres", "postgresql"]),
        "mysql" => Some(&["mysql"]),
        "amqp" | "rabbitmq" => Some(&["amqp", "amqps"]),
        "sqs" => Some(&["sqs"]),
        _ => None,
    }
}

/// A warning message if `url`'s scheme does not match `broker_type`'s
/// expected scheme family (e.g. `broker_type = "redis"` with `url =
/// "postgres://..."`), else `None`.
///
/// Returns `None` (no warning) when `broker_type` is not itself recognized --
/// see `expected_schemes_for`.
#[must_use]
pub fn scheme_broker_type_mismatch(url: &str, broker_type: &str) -> Option<String> {
    let expected = expected_schemes_for(broker_type)?;
    let scheme = scheme_of(url);
    if expected.contains(&scheme) {
        None
    } else {
        Some(format!(
            "broker.url scheme '{scheme}' does not match broker.type '{broker_type}' \
             (expected {})",
            expected.join(" or ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_well_formed_rejects_empty_url() {
        let err = require_well_formed("").expect_err("empty url must be rejected");
        assert!(err.contains("invalid broker url"));
        assert!(err.contains("empty"));
    }

    #[test]
    fn require_well_formed_rejects_whitespace_only_url() {
        let err = require_well_formed("   ").expect_err("whitespace-only url must be rejected");
        assert!(err.contains("invalid broker url"));
    }

    #[test]
    fn require_well_formed_rejects_url_with_no_scheme_separator() {
        let err = require_well_formed("redis//localhost:6379")
            .expect_err("a url with no '://' must be rejected");
        assert!(err.contains("invalid broker url"));
        assert!(err.contains("redis//localhost:6379"));
    }

    #[test]
    fn require_well_formed_accepts_every_known_scheme() {
        for scheme in KNOWN_BROKER_SCHEMES {
            let url = format!("{scheme}://host:1234/db");
            require_well_formed(&url).unwrap_or_else(|e| panic!("{url} must validate: {e}"));
        }
    }

    #[test]
    fn scheme_of_extracts_the_scheme() {
        assert_eq!(scheme_of("redis://localhost:6379"), "redis");
        assert_eq!(scheme_of("amqps://user:pass@host:5671/vhost"), "amqps");
    }

    #[test]
    fn scheme_of_falls_back_to_the_whole_string_with_no_separator() {
        // require_well_formed would already have rejected this; scheme_of
        // itself must still not panic on it.
        assert_eq!(scheme_of("not-a-url"), "not-a-url");
    }

    #[test]
    fn scheme_warning_is_none_for_every_known_scheme() {
        for scheme in KNOWN_BROKER_SCHEMES {
            let url = format!("{scheme}://host");
            assert_eq!(scheme_warning(&url), None, "{scheme} must be recognized");
        }
    }

    #[test]
    fn scheme_warning_fires_for_an_unknown_scheme() {
        let warning = scheme_warning("ftp://host").expect("unknown scheme must warn");
        assert!(warning.contains("ftp"));
        assert!(warning.contains("unsupported broker scheme"));
    }

    #[test]
    fn scheme_broker_type_mismatch_is_none_when_aligned() {
        assert_eq!(
            scheme_broker_type_mismatch("redis://host:6379", "redis"),
            None
        );
        assert_eq!(
            scheme_broker_type_mismatch("rediss://host:6379", "redis"),
            None,
            "TLS variant must count as aligned"
        );
        assert_eq!(
            scheme_broker_type_mismatch("amqp://host", "rabbitmq"),
            None,
            "amqp scheme must align with the 'rabbitmq' broker_type alias"
        );
        assert_eq!(
            scheme_broker_type_mismatch("postgresql://host/db", "postgres"),
            None,
            "postgres/postgresql must be interchangeable"
        );
    }

    #[test]
    fn scheme_broker_type_mismatch_fires_for_a_real_mismatch() {
        let warning = scheme_broker_type_mismatch("postgres://host/db", "redis")
            .expect("redis broker_type with a postgres:// url must warn");
        assert!(warning.contains("postgres"));
        assert!(warning.contains("redis"));
    }

    #[test]
    fn scheme_broker_type_mismatch_is_none_for_an_unrecognized_broker_type() {
        // `Config::validate` already emits its own "Unknown broker type"
        // warning for this case; a second warning here would be redundant
        // noise about a type nothing else recognizes either.
        assert_eq!(
            scheme_broker_type_mismatch("redis://host", "totally-unknown"),
            None
        );
    }
}
