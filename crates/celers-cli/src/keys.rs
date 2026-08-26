//! Centralized Redis key-layout helpers matching the key scheme
//! `celers-broker-redis::RedisBroker` actually uses on the wire.
//!
//! `RedisBroker` names every queue-family key directly off the bare queue
//! name passed to `RedisBroker::new`/`with_mode` -- there is no shared
//! prefix at all:
//!
//! ```text
//! <queue>              main queue (LIST in FIFO mode, ZSET in Priority mode)
//! <queue>:processing   in-flight ("currently being processed") tasks (LIST)
//! <queue>:dlq          dead-letter queue (LIST)
//! <queue>:delayed      delayed tasks (ZSET, score = execute_at unix timestamp)
//! ```
//!
//! (see `RedisBroker::with_mode`/`RedisBroker::queue_names` and their
//! `test_queue_names` unit test in `celers-broker-redis::lib`, which this
//! module's own tests mirror byte-for-byte).
//!
//! Every raw-Redis (i.e. not going through a `RedisBroker` method) code path
//! in this CLI historically built these keys independently as
//! `celers:{queue}`, `celers:{queue}:dlq`, and so on -- a namespace the
//! broker never writes to, so those commands silently operated on empty,
//! disconnected keys against a live, non-empty system. This module is the
//! single source of truth from now on: every CLI code path that talks to
//! Redis directly must build its queue-family keys here rather than with an
//! inline `format!("celers:{queue}...")`.
//!
//! Two related key families are deliberately *not* duplicated here:
//!
//! - Queue pause/drain flags (`<queue>:paused` / `<queue>:drain`) are owned
//!   by `celers_broker_redis::queue_control::QueueController`, which also
//!   provides the actual pause/resume/drain *behavior* (clearing the
//!   opposite flag atomically, etc.) -- callers that need these should go
//!   through `RedisBroker::queue_controller()` rather than writing the flag
//!   keys directly (see `commands::queue::pause_queue`/`resume_queue`), so
//!   there is no raw-key constructor for them here to avoid inviting a
//!   second, easily-drifting implementation of that logic.
//! - Cron schedule keys (`celers:schedule:{name}`, written by
//!   `commands::schedule::add_schedule`) are a separate namespace from the
//!   queue-family keys above -- a scheduled-task definition is not a broker
//!   queue at all -- but the scan pattern used to discover them (see
//!   `backup.rs`) is still centralized as `SCHEDULE_SCAN_PATTERN`.

/// The Redis key for `queue`'s main queue: a LIST in FIFO mode, a ZSET in
/// Priority mode. Matches `RedisBroker::queue_name` exactly -- no prefix at
/// all, the bare queue name *is* the key.
#[must_use]
pub(crate) fn main(queue: &str) -> String {
    queue.to_string()
}

/// The Redis key for `queue`'s in-flight/processing LIST. Matches
/// `RedisBroker::processing_queue` / `RedisBroker::processing_queue_name()`.
#[must_use]
pub(crate) fn processing(queue: &str) -> String {
    format!("{queue}:processing")
}

/// The Redis key for `queue`'s dead-letter LIST. Matches
/// `RedisBroker::dlq_name` / `RedisBroker::dlq_name()`.
#[must_use]
pub(crate) fn dlq(queue: &str) -> String {
    format!("{queue}:dlq")
}

/// The Redis key for `queue`'s delayed-tasks ZSET. Matches
/// `RedisBroker::delayed_queue` / `RedisBroker::delayed_queue_name()`.
#[must_use]
pub(crate) fn delayed(queue: &str) -> String {
    format!("{queue}:delayed")
}

/// `SCAN` match pattern for every cron-schedule key
/// `commands::schedule::add_schedule` writes (`celers:schedule:{name}`).
pub(crate) const SCHEDULE_SCAN_PATTERN: &str = "celers:schedule:*";

/// The `celers:`-prefixed sub-namespaces this CLI owns in Redis for
/// bookkeeping that is *never* a queue-family key: worker/task/metrics/
/// schedule records (there is no `celers:alias:` namespace in practice --
/// aliases live in the config file, not Redis -- but it is included here
/// defensively since a bare `commands::monitoring::report::base_queue_name`
/// unit test historically covered it).
///
/// Checked by exact sub-namespace rather than a blanket `celers:` prefix,
/// because a queue's own bare name can itself start with "celers:" -- the
/// configured *default* queue name is literally `"celers"` (see
/// `Config::default_config`), so that queue's own sibling keys
/// (`celers:dlq`, `celers:processing`, `celers:delayed`) must not be
/// confused with these bookkeeping namespaces just because they share the
/// substring. See [`has_queue_family_suffix`], which takes priority over
/// this check for exactly that reason.
const RESERVED_NAMESPACE_PREFIXES: &[&str] = &[
    "celers:worker:",
    "celers:task:",
    "celers:metrics:",
    "celers:schedule:",
    "celers:alias:",
];

/// The queue-family suffixes a scanned key can carry (see the constructors
/// above, plus `celers_broker_redis::queue_control`'s `:paused`/`:drain`
/// flags). A key ending in one of these is always a sibling of some primary
/// queue key, never a primary queue key -- and never bookkeeping -- on its
/// own.
const QUEUE_FAMILY_SUFFIXES: &[&str] = &[":dlq", ":processing", ":delayed", ":paused", ":drain"];

/// `true` when `key` carries one of the [`QUEUE_FAMILY_SUFFIXES`] -- i.e. it
/// is a sibling of some primary queue key (that queue's DLQ/processing/
/// delayed bucket, or its pause/drain control flag) rather than a
/// bookkeeping key or a primary queue key itself.
///
/// Callers that need to tell "primary queue key" apart from "queue-family
/// sibling key" (rather than just "is this queue-family at all") check this
/// *before* [`is_reserved_namespace_key`]: see that function's docs for why
/// the ordering matters.
#[must_use]
pub(crate) fn has_queue_family_suffix(key: &str) -> bool {
    QUEUE_FAMILY_SUFFIXES.iter().any(|s| key.ends_with(s))
}

/// `true` when `key` belongs to one of this CLI's own `celers:`-prefixed
/// bookkeeping namespaces ([`RESERVED_NAMESPACE_PREFIXES`]) rather than the
/// queue-family keyspace `RedisBroker` itself owns.
///
/// Must be checked *after* [`has_queue_family_suffix`], not before: the
/// default queue name is literally `"celers"`, so without that ordering
/// `celers:dlq`/`celers:processing`/`celers:delayed` would be misclassified
/// as bookkeeping instead of as that queue's own sibling keys.
#[must_use]
pub(crate) fn is_reserved_namespace_key(key: &str) -> bool {
    key == "celers:"
        || RESERVED_NAMESPACE_PREFIXES
            .iter()
            .any(|p| key.starts_with(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_family_keys_match_redis_broker_scheme() {
        // Mirrors `celers_broker_redis::RedisBroker`'s own
        // `test_queue_names` unit test (crates/celers-broker-redis/src/lib.rs)
        // byte-for-byte, so a future change to either scheme's format is
        // caught by at least one of the two suites.
        assert_eq!(main("my_queue"), "my_queue");
        assert_eq!(processing("my_queue"), "my_queue:processing");
        assert_eq!(dlq("my_queue"), "my_queue:dlq");
        assert_eq!(delayed("my_queue"), "my_queue:delayed");
    }

    #[test]
    fn schedule_scan_pattern_matches_schedule_command_scheme() {
        let example_key = "celers:schedule:nightly";
        assert!(example_key.starts_with(
            SCHEDULE_SCAN_PATTERN
                .strip_suffix('*')
                .expect("pattern ends with a wildcard")
        ));
    }

    #[test]
    fn keys_are_distinct_for_distinct_queues() {
        assert_ne!(main("a"), main("b"));
        assert_ne!(dlq("a"), delayed("a"));
    }

    #[test]
    fn queue_family_suffix_recognizes_every_sibling_key_kind() {
        assert!(has_queue_family_suffix("default:dlq"));
        assert!(has_queue_family_suffix("default:processing"));
        assert!(has_queue_family_suffix("default:delayed"));
        assert!(has_queue_family_suffix("default:paused"));
        assert!(has_queue_family_suffix("default:drain"));
        assert!(!has_queue_family_suffix("default"));
    }

    #[test]
    fn queue_family_suffix_applies_even_to_the_default_queue_named_celers() {
        // Regression test: `Config::default_config` names the default queue
        // literally "celers", so this queue's own sibling keys collide
        // syntactically with `is_reserved_namespace_key`'s `celers:` prefix
        // -- callers MUST check this function first (see both functions'
        // docs) or these three keys get misclassified as bookkeeping.
        assert!(has_queue_family_suffix("celers:dlq"));
        assert!(has_queue_family_suffix("celers:processing"));
        assert!(has_queue_family_suffix("celers:delayed"));
    }

    #[test]
    fn reserved_namespace_key_matches_every_known_bookkeeping_prefix() {
        assert!(is_reserved_namespace_key("celers:worker:w1:heartbeat"));
        assert!(is_reserved_namespace_key("celers:task:abc:logs"));
        assert!(is_reserved_namespace_key(
            "celers:metrics:default:daily:2026-07-01"
        ));
        assert!(is_reserved_namespace_key("celers:schedule:job1"));
        assert!(is_reserved_namespace_key("celers:alias:foo"));
        assert!(is_reserved_namespace_key("celers:"));
    }

    #[test]
    fn reserved_namespace_key_does_not_match_the_default_queue_or_a_celers_prefixed_queue_name() {
        // The default queue is literally named "celers"; a user-configured
        // queue could also legitimately be named "celers:staging". Neither
        // is one of the specific reserved sub-namespaces above.
        assert!(!is_reserved_namespace_key("celers"));
        assert!(!is_reserved_namespace_key("celers:staging"));
        assert!(!is_reserved_namespace_key("other:default"));
    }
}
