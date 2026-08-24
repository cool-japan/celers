//! Server capability gating for `FOR UPDATE ... SKIP LOCKED`.
//!
//! The whole claim spine of this broker depends on `SKIP LOCKED`, which
//! exists only on **MySQL 8.0.1+** and **MariaDB 10.6+**. On MySQL 5.7 or any
//! MariaDB below 10.6 the dequeue statement fails with a parse error at
//! runtime, on every single dequeue, with a message that gives no hint that
//! the server is simply too old.
//!
//! [`check_skip_locked_support`] turns that into one clear error at connect
//! time. Version strings that cannot be parsed are *not* treated as failures:
//! proxies (ProxySQL, RDS Proxy, Vitess) and forks report shapes this parser
//! has never seen, and refusing to connect to a perfectly capable server is
//! worse than letting a genuinely unsupported one fail later.

use celers_core::{CelersError, Result};

/// Minimum MySQL version implementing `SKIP LOCKED`.
pub const MIN_MYSQL_VERSION: (u32, u32, u32) = (8, 0, 1);

/// Minimum MariaDB version implementing `SKIP LOCKED`.
pub const MIN_MARIADB_VERSION: (u32, u32, u32) = (10, 6, 0);

/// Which server family reported the version string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerFlavor {
    /// Oracle MySQL (or a fork reporting as such, e.g. Percona Server).
    MySql,
    /// MariaDB.
    MariaDb,
}

impl ServerFlavor {
    /// Minimum version of this flavor that supports `SKIP LOCKED`.
    #[must_use]
    pub const fn min_skip_locked_version(self) -> (u32, u32, u32) {
        match self {
            ServerFlavor::MySql => MIN_MYSQL_VERSION,
            ServerFlavor::MariaDb => MIN_MARIADB_VERSION,
        }
    }

    /// Human-readable name used in diagnostics.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            ServerFlavor::MySql => "MySQL",
            ServerFlavor::MariaDb => "MariaDB",
        }
    }
}

/// A parsed `SELECT VERSION()` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerVersion {
    /// Server family.
    pub flavor: ServerFlavor,
    /// Major version component.
    pub major: u32,
    /// Minor version component.
    pub minor: u32,
    /// Patch version component (`0` when the string omits it).
    pub patch: u32,
}

impl ServerVersion {
    /// Whether this server implements `FOR UPDATE ... SKIP LOCKED`.
    #[must_use]
    pub fn supports_skip_locked(&self) -> bool {
        (self.major, self.minor, self.patch) >= self.flavor.min_skip_locked_version()
    }
}

/// Parse a `SELECT VERSION()` string, or `None` if it has no recognisable
/// `major.minor[.patch]` prefix.
///
/// Handles the MariaDB legacy-compatibility prefix: MariaDB 10.x commonly
/// reports `5.5.5-10.6.12-MariaDB-1:10.6.12+maria~ubu2004` so that old
/// clients, which refused to talk to a server whose version did not start
/// with `5`, keep working. Parsing that naively yields `5.5` and would reject
/// a supported server, so the `5.5.5-` prefix is stripped first.
#[must_use]
pub fn parse_server_version(raw: &str) -> Option<ServerVersion> {
    let trimmed = raw.trim();
    let flavor = if trimmed.to_ascii_lowercase().contains("mariadb") {
        ServerFlavor::MariaDb
    } else {
        ServerFlavor::MySql
    };

    // Strip MariaDB's legacy `5.5.5-` compatibility prefix before parsing.
    let body = trimmed.strip_prefix("5.5.5-").unwrap_or(trimmed);

    // The version proper is everything up to the first `-` (build/vendor
    // suffix) or the first space.
    let head = body
        .split(['-', ' '])
        .next()
        .filter(|segment| !segment.is_empty())?;

    let mut parts = head.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts
        .next()
        .and_then(|p| p.parse::<u32>().ok())
        .unwrap_or(0);
    let patch = parts
        .next()
        .and_then(|p| p.parse::<u32>().ok())
        .unwrap_or(0);

    Some(ServerVersion {
        flavor,
        major,
        minor,
        patch,
    })
}

/// Fail loudly when the connected server is known not to support
/// `SKIP LOCKED`.
///
/// Returns `Ok(())` both for a supported server and for a version string this
/// parser does not recognise (a warning is logged in the latter case).
pub fn check_skip_locked_support(raw: &str) -> Result<()> {
    let Some(version) = parse_server_version(raw) else {
        tracing::warn!(
            server_version = %raw,
            "Could not parse the server version string; assuming \
             FOR UPDATE SKIP LOCKED is supported. CeleRS requires \
             MySQL >= 8.0.1 or MariaDB >= 10.6."
        );
        return Ok(());
    };

    if version.supports_skip_locked() {
        return Ok(());
    }

    let (min_major, min_minor, min_patch) = version.flavor.min_skip_locked_version();
    Err(CelersError::Configuration(format!(
        "CeleRS MySQL broker requires {flavor} >= {min_major}.{min_minor}.{min_patch} for \
         FOR UPDATE SKIP LOCKED, which every dequeue depends on; connected server reports \
         {raw:?} (parsed as {flavor} {major}.{minor}.{patch})",
        flavor = version.flavor.name(),
        major = version.major,
        minor = version.minor,
        patch = version.patch,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(raw: &str) -> ServerVersion {
        parse_server_version(raw).unwrap_or_else(|| panic!("{raw:?} should parse"))
    }

    #[test]
    fn parses_plain_mysql_versions() {
        assert_eq!(
            parsed("8.0.32"),
            ServerVersion {
                flavor: ServerFlavor::MySql,
                major: 8,
                minor: 0,
                patch: 32
            }
        );
        assert_eq!(
            parsed("8.0.32-0ubuntu0.20.04.2"),
            ServerVersion {
                flavor: ServerFlavor::MySql,
                major: 8,
                minor: 0,
                patch: 32
            }
        );
        assert_eq!(parsed("5.7.44-log").major, 5);
        assert_eq!(parsed("9.1.0").major, 9);
    }

    /// MariaDB 10.x reports a `5.5.5-` compatibility prefix; parsing it
    /// naively yields `5.5` and rejects a perfectly supported server.
    #[test]
    fn strips_the_mariadb_legacy_prefix() {
        let version = parsed("5.5.5-10.6.12-MariaDB-1:10.6.12+maria~ubu2004");
        assert_eq!(version.flavor, ServerFlavor::MariaDb);
        assert_eq!((version.major, version.minor, version.patch), (10, 6, 12));
        assert!(version.supports_skip_locked());
    }

    #[test]
    fn parses_mariadb_without_the_legacy_prefix() {
        let version = parsed("10.5.19-MariaDB");
        assert_eq!(version.flavor, ServerFlavor::MariaDb);
        assert_eq!((version.major, version.minor), (10, 5));
        assert!(!version.supports_skip_locked());
    }

    #[test]
    fn skip_locked_support_boundaries() {
        assert!(!parsed("8.0.0").supports_skip_locked());
        assert!(parsed("8.0.1").supports_skip_locked());
        assert!(parsed("8.4.0").supports_skip_locked());
        assert!(!parsed("5.7.44").supports_skip_locked());
        assert!(!parsed("5.5.5-10.5.9-MariaDB").supports_skip_locked());
        assert!(parsed("5.5.5-10.6.0-MariaDB").supports_skip_locked());
    }

    #[test]
    fn unsupported_server_yields_an_actionable_error() {
        let err = check_skip_locked_support("5.7.44-log")
            .expect_err("MySQL 5.7 must be rejected at connect time");
        let message = err.to_string();
        assert!(message.contains("SKIP LOCKED"), "got: {message}");
        assert!(message.contains("8.0.1"), "got: {message}");
        assert!(message.contains("5.7.44-log"), "got: {message}");

        let err = check_skip_locked_support("5.5.5-10.5.9-MariaDB-1:10.5.9")
            .expect_err("MariaDB 10.5 must be rejected at connect time");
        let message = err.to_string();
        assert!(message.contains("MariaDB"), "got: {message}");
        assert!(message.contains("10.6.0"), "got: {message}");
    }

    #[test]
    fn supported_servers_are_accepted() {
        assert!(check_skip_locked_support("8.0.35").is_ok());
        assert!(check_skip_locked_support("8.0.1").is_ok());
        assert!(check_skip_locked_support("5.5.5-10.11.6-MariaDB").is_ok());
    }

    /// Fail-open: an unrecognisable string must not block a capable server.
    #[test]
    fn unparseable_versions_are_not_rejected() {
        assert_eq!(parse_server_version(""), None);
        assert_eq!(parse_server_version("ProxySQL"), None);
        assert!(check_skip_locked_support("ProxySQL").is_ok());
        assert!(check_skip_locked_support("").is_ok());
    }
}
