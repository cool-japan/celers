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
//!   `backup.rs`) is still centralized as [`SCHEDULE_SCAN_PATTERN`].

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
}
