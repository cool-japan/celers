//! User-defined command aliases: `celers w` expanding to `celers worker
//! start`, backed by a small, serializable config map.
//!
//! This module intentionally covers only *user-defined* aliases loaded from
//! configuration. Built-in short aliases (e.g. `worker` also answering to
//! `w`) are a separate, simpler concern implemented directly as
//! `visible_alias`/`visible_aliases` `clap` attributes on the command
//! structs — see the crate's top-level integration notes for the specific
//! list. The two mechanisms compose: [`AliasConfig::resolve`] runs as a
//! pre-parse expansion pass over the raw argument vector, before `clap` ever
//! sees the arguments, so a user alias can expand to a string that itself
//! uses a `clap` built-in short alias (or vice versa).
//!
//! # Examples
//!
//! ```
//! use celers_cli::aliases::AliasConfig;
//!
//! let reserved = ["worker", "queue", "task"];
//! let mut aliases = AliasConfig::new();
//! aliases.add("w", "worker start", &reserved).expect("valid alias");
//!
//! let args = vec!["w".to_string(), "--queue".to_string(), "fast".to_string()];
//! assert_eq!(
//!     aliases.resolve(&args),
//!     vec!["worker", "start", "--queue", "fast"]
//! );
//!
//! // A user can never redefine a real command name as an alias...
//! assert!(aliases.add("worker", "queue list", &reserved).is_err());
//! // ...so resolving "worker" is untouched and the real command still runs.
//! let real = vec!["worker".to_string()];
//! assert_eq!(aliases.resolve(&real), real);
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A user-defined alias table: alias name -> expansion (a full command line,
/// e.g. `"w"` -> `"worker start"`).
///
/// Backed by a plain `HashMap` so it serializes naturally as a TOML/YAML
/// table (e.g. an `[aliases]` section: `w = "worker start"`). The map is
/// intentionally private; mutation only happens through [`AliasConfig::add`]
/// / [`AliasConfig::remove`] so that the "an alias can never shadow a real
/// command" invariant can be enforced in one place ([`AliasConfig::add`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AliasConfig(HashMap<String, String>);

impl AliasConfig {
    /// An empty alias table.
    ///
    /// Not reachable from this crate's own `bin` target: the live
    /// `expand_aliases` (`src/main.rs`) builds its fallback empty table via
    /// `Option::unwrap_or_default` -- i.e. through the `Default` impl rather
    /// than this associated function directly -- so nothing in the bin's own
    /// call graph names `new` specifically. It is still reachable (and
    /// covered by this module's own tests and the doctest above) via the
    /// public `celers_cli::aliases` library API, which is the surface this
    /// function exists for; also kept as the conventional `new()`
    /// constructor expected alongside a `Default` impl. Kept, not
    /// renamed/removed, per its public API contract.
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build an [`AliasConfig`] directly from a pre-built map, bypassing the
    /// [`AliasConfig::add`] validation.
    ///
    /// Intended for trusted construction paths (deserializing a config file
    /// that was itself produced by a validated [`AliasConfig::add`]/`list`
    /// round-trip, or tests); prefer [`AliasConfig::add`] for anything that
    /// accepts alias definitions from a live user (e.g. `celers alias add`),
    /// since only `add` rejects collisions with real command names.
    ///
    /// Not reachable from this crate's own `bin` target: production alias
    /// tables are always loaded through [`AliasConfig`]'s `Deserialize` impl
    /// (the config file's `[aliases]` section), never assembled from an
    /// in-memory map directly. It is still reachable via the public
    /// `celers_cli::aliases` library API, which is the surface this function
    /// exists for, and is exercised by this module's own tests plus
    /// `tests/proptest_cli.rs`'s `arb_alias_config` property-test generator.
    /// Kept, not renamed/removed, per its public API contract.
    #[allow(dead_code)]
    #[must_use]
    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self(map)
    }

    /// Resolve a raw argument vector (already stripped of the binary name,
    /// e.g. `std::env::args().skip(1)`) against this alias table.
    ///
    /// If `args[0]` matches a user-defined alias, the alias is expanded in
    /// place: the matched token is replaced (spliced) with the alias's
    /// (whitespace-split) expansion, and the remaining arguments are kept
    /// as-is after it. Otherwise `args` is returned unchanged — this
    /// includes the case where `args[0]` is a real top-level command name,
    /// since [`AliasConfig::add`] never allows a real command name to be
    /// stored as an alias in the first place, so real commands always win
    /// without `resolve` needing to know the reserved-name list itself.
    #[must_use]
    pub fn resolve(&self, args: &[String]) -> Vec<String> {
        match args.split_first() {
            Some((first, rest)) => match self.0.get(first) {
                Some(expansion) => {
                    let mut expanded: Vec<String> =
                        expansion.split_whitespace().map(str::to_string).collect();
                    expanded.extend_from_slice(rest);
                    expanded
                }
                None => args.to_vec(),
            },
            None => Vec::new(),
        }
    }

    /// Define a new alias, or replace an existing one with the same name.
    ///
    /// Rejects:
    /// - an empty (or all-whitespace) `name`,
    /// - an empty (or all-whitespace) `expansion`, and
    /// - a `name` that collides with any entry in `reserved_names` (the real
    ///   top-level command list), so a user-defined alias can never shadow a
    ///   real command such as `worker` or `queue`.
    pub fn add(
        &mut self,
        name: &str,
        expansion: &str,
        reserved_names: &[&str],
    ) -> anyhow::Result<()> {
        let trimmed_name = name.trim();
        if trimmed_name.is_empty() {
            anyhow::bail!("alias name cannot be empty");
        }
        if expansion.trim().is_empty() {
            anyhow::bail!("alias expansion cannot be empty");
        }
        if reserved_names.contains(&trimmed_name) {
            anyhow::bail!(
                "'{trimmed_name}' is a reserved command name and cannot be redefined as an alias"
            );
        }

        self.0
            .insert(trimmed_name.to_string(), expansion.trim().to_string());
        Ok(())
    }

    /// Remove an alias by name. Returns `true` if it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.0.remove(name).is_some()
    }

    /// List every defined alias as `(name, expansion)` pairs, sorted by
    /// name for stable/deterministic output (`HashMap` iteration order is
    /// not stable).
    #[must_use]
    pub fn list(&self) -> Vec<(&str, &str)> {
        let mut items: Vec<(&str, &str)> = self
            .0
            .iter()
            .map(|(name, expansion)| (name.as_str(), expansion.as_str()))
            .collect();
        items.sort_unstable_by_key(|(name, _)| *name);
        items
    }

    /// `true` if no aliases are defined.
    ///
    /// Not reachable from this crate's own `bin` target: no CLI command
    /// currently queries an `AliasConfig`'s emptiness directly. It is still
    /// reachable via the public `celers_cli::aliases` library API, which is
    /// the surface this function exists for, and is covered by this
    /// module's own tests; also kept as the conventional `HashMap`/`Vec`-style
    /// companion to [`AliasConfig::len`] (`clippy::len_without_is_empty`
    /// expects the two to travel together). Kept, not renamed/removed, per
    /// its public API contract.
    #[allow(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of defined aliases.
    ///
    /// Not reachable from this crate's own `bin` target: no CLI command
    /// currently queries an `AliasConfig`'s length directly. It is still
    /// reachable via the public `celers_cli::aliases` library API, which is
    /// the surface this function exists for, and is covered by this
    /// module's own tests; also kept as the idiomatic collection-style
    /// counterpart to [`AliasConfig::is_empty`]. Kept, not renamed/removed,
    /// per its public API contract.
    #[allow(dead_code)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESERVED: &[&str] = &[
        "worker",
        "status",
        "dlq",
        "replay",
        "loadtest",
        "queue",
        "task",
        "init",
        "config",
        "metrics",
        "monitor",
        "validate",
        "completions",
        "manpages",
        "health",
        "worker-mgmt",
        "doctor",
        "schedule",
        "debug",
        "report",
        "analyze",
        "autoscale",
        "alert",
        "db",
        "dashboard",
        "interactive",
        "backup",
        "restore",
        "alias",
    ];

    #[test]
    fn add_rejects_empty_name() {
        let mut aliases = AliasConfig::new();
        assert!(aliases.add("", "worker start", RESERVED).is_err());
        assert!(aliases.add("   ", "worker start", RESERVED).is_err());
    }

    #[test]
    fn add_rejects_empty_expansion() {
        let mut aliases = AliasConfig::new();
        assert!(aliases.add("w", "", RESERVED).is_err());
        assert!(aliases.add("w", "   ", RESERVED).is_err());
    }

    #[test]
    fn add_rejects_reserved_name_collision() {
        let mut aliases = AliasConfig::new();
        let result = aliases.add("worker", "worker start --queue fast", RESERVED);
        assert!(result.is_err());
        assert!(aliases.is_empty());
    }

    #[test]
    fn add_accepts_a_valid_alias() {
        let mut aliases = AliasConfig::new();
        aliases
            .add("w", "worker start", RESERVED)
            .expect("valid alias should be accepted");
        assert_eq!(aliases.list(), vec![("w", "worker start")]);
        assert_eq!(aliases.len(), 1);
    }

    #[test]
    fn add_trims_name_and_expansion() {
        let mut aliases = AliasConfig::new();
        aliases
            .add("  w  ", "  worker start  ", RESERVED)
            .expect("valid alias should be accepted");
        assert_eq!(aliases.list(), vec![("w", "worker start")]);
    }

    #[test]
    fn resolve_expands_a_known_alias() {
        let mut aliases = AliasConfig::new();
        aliases
            .add("w", "worker start", RESERVED)
            .expect("valid alias should be accepted");

        let args = vec!["w".to_string(), "--queue".to_string(), "fast".to_string()];
        let resolved = aliases.resolve(&args);

        assert_eq!(resolved, vec!["worker", "start", "--queue", "fast"]);
    }

    #[test]
    fn resolve_leaves_unknown_input_unchanged() {
        let aliases = AliasConfig::new();
        let args = vec!["totally-unknown".to_string(), "--flag".to_string()];
        assert_eq!(aliases.resolve(&args), args);
    }

    #[test]
    fn resolve_leaves_empty_args_unchanged() {
        let aliases = AliasConfig::new();
        let args: Vec<String> = vec![];
        assert_eq!(aliases.resolve(&args), args);
    }

    #[test]
    fn real_commands_always_win_over_a_same_named_alias() {
        let mut aliases = AliasConfig::new();

        // Attempting to shadow a real top-level command is rejected...
        let err = aliases
            .add("worker", "queue list", RESERVED)
            .expect_err("reserved name must be rejected");
        assert!(err.to_string().contains("worker"));

        // ...so the map never learned about it, and resolving "worker" is
        // completely untouched: the real command runs exactly as typed.
        let args = vec![
            "worker".to_string(),
            "--queue".to_string(),
            "fast".to_string(),
        ];
        assert_eq!(aliases.resolve(&args), args);
    }

    #[test]
    fn remove_reports_whether_an_alias_existed() {
        let mut aliases = AliasConfig::new();
        aliases
            .add("w", "worker start", RESERVED)
            .expect("valid alias should be accepted");

        assert!(aliases.remove("w"));
        assert!(!aliases.remove("w"));
        assert!(aliases.is_empty());
    }

    #[test]
    fn list_is_sorted_by_name() {
        let mut aliases = AliasConfig::new();
        aliases
            .add("t", "task inspect", RESERVED)
            .expect("valid alias should be accepted");
        aliases
            .add("q", "queue list", RESERVED)
            .expect("valid alias should be accepted");
        aliases
            .add("w", "worker start", RESERVED)
            .expect("valid alias should be accepted");

        assert_eq!(
            aliases.list(),
            vec![
                ("q", "queue list"),
                ("t", "task inspect"),
                ("w", "worker start"),
            ]
        );
    }

    #[test]
    fn from_map_seeds_aliases_without_validation() {
        let mut map = HashMap::new();
        map.insert("w".to_string(), "worker start".to_string());
        let aliases = AliasConfig::from_map(map);
        assert_eq!(aliases.list(), vec![("w", "worker start")]);
    }

    #[test]
    fn default_config_is_empty() {
        let aliases = AliasConfig::default();
        assert!(aliases.is_empty());
        assert_eq!(aliases.len(), 0);
    }

    #[test]
    fn serde_roundtrip_via_toml() {
        let mut aliases = AliasConfig::new();
        aliases
            .add("w", "worker start", RESERVED)
            .expect("valid alias should be accepted");

        let toml_str = toml::to_string(&aliases).expect("serialize aliases to TOML");
        let roundtripped: AliasConfig = toml::from_str(&toml_str).expect("deserialize aliases");

        assert_eq!(aliases, roundtripped);
    }
}
