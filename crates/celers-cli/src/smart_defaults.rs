//! Smart defaults: environment-based broker detection, intelligent queue
//! selection, and context-aware "did you mean" suggestions.
//!
//! Every function in this module is pure (or, for
//! [`detect_broker_from_env`], reads only process environment variables) —
//! there is no other I/O, no logging, and no dependency on the rest of the
//! crate, so these are easy to unit test and easy to slot into the existing
//! `cli::dispatch` broker/queue resolution idiom
//! (`broker.unwrap_or(cfg.broker.url)`) without pulling in additional
//! machinery.

/// Environment variables consulted by [`detect_broker_from_env`], in the
/// precedence order they are checked — the first one present (and
/// non-empty) wins.
///
/// `REDIS_URL` is checked first because it is the most specific / most
/// common convention for Redis-only deployments; `CELERY_BROKER_URL` is
/// Celery's own long-standing convention (also recognized directly by
/// [`crate::config::Config::apply_env_overrides`]); `AMQP_URL` is the
/// common convention for AMQP/RabbitMQ hosting providers.
const BROKER_ENV_VARS: [&str; 3] = ["REDIS_URL", "CELERY_BROKER_URL", "AMQP_URL"];

/// Auto-detect a broker URL from well-known environment variables.
///
/// Checks, in order, `REDIS_URL`, `CELERY_BROKER_URL`, then `AMQP_URL`, and
/// returns the first one that is set to a non-empty value. Returns `None`
/// when none of them are set, leaving the caller free to fall back to a
/// config file value or a built-in default.
#[must_use]
pub fn detect_broker_from_env() -> Option<String> {
    BROKER_ENV_VARS.iter().find_map(|key| {
        std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

/// Suggest a queue name given the queues that are actually available.
///
/// - `requested` is `None` (the user did not pass `--queue`) and exactly one
///   queue is available: suggest that one queue, since there is no
///   ambiguity.
/// - `requested` is `None` and zero or more-than-one queues are available:
///   no suggestion (either nothing to suggest, or genuinely ambiguous).
/// - `requested` is `Some` and matches an available queue exactly: no
///   suggestion needed, the request is already valid.
/// - `requested` is `Some` and does not match exactly: suggest the closest
///   available queue by edit distance, if one is close enough to be a
///   plausible typo fix.
#[must_use]
pub fn suggest_queue(available: &[String], requested: Option<&str>) -> Option<String> {
    match requested {
        None => {
            if available.len() == 1 {
                Some(available[0].clone())
            } else {
                None
            }
        }
        Some(requested) => {
            if available.iter().any(|queue| queue == requested) {
                None
            } else {
                closest_match(requested, available)
            }
        }
    }
}

/// Suggest the closest known command/argument for an unrecognized input.
///
/// Given the raw text that failed to match anything (e.g. an unrecognized
/// subcommand) and the set of known commands, returns the closest match by
/// edit distance, if one is close enough to be a plausible "did you mean"
/// suggestion. Returns `None` when `available_commands` is empty or nothing
/// is close enough to be a helpful guess.
#[must_use]
pub fn context_suggestion(input_error: &str, available_commands: &[String]) -> Option<String> {
    closest_match(input_error, available_commands)
}

/// Find the candidate closest to `target` by (case-insensitive) Levenshtein
/// distance, provided the closest one is within [`closeness_threshold`] of
/// `target`'s length. Returns `None` for an empty candidate list or when the
/// best candidate is still too far away to be a useful suggestion.
fn closest_match(target: &str, candidates: &[String]) -> Option<String> {
    let target_lower = target.to_lowercase();

    let mut best: Option<(&String, usize)> = None;
    for candidate in candidates {
        let distance = levenshtein_distance(&target_lower, &candidate.to_lowercase());
        let is_better = match best {
            Some((_, best_distance)) => distance < best_distance,
            None => true,
        };
        if is_better {
            best = Some((candidate, distance));
        }
    }

    let (candidate, distance) = best?;
    let threshold = closeness_threshold(target_lower.chars().count(), candidate.chars().count());
    if distance <= threshold {
        Some(candidate.clone())
    } else {
        None
    }
}

/// The maximum edit distance still considered a plausible typo, given the
/// lengths of the two strings being compared: half the longer string's
/// length, floored at `2` so short strings still tolerate a one- or
/// two-character slip.
fn closeness_threshold(a_len: usize, b_len: usize) -> usize {
    (a_len.max(b_len) / 2).max(2)
}

/// Classic Wagner-Fischer Levenshtein edit distance (single-character
/// insert/delete/substitute), computed over `char`s (not bytes, so this is
/// correct for multi-byte UTF-8 input) in O(`n`*`m`) time and O(min(`n`,
/// `m`)) extra space via a rolling two-row table.
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let a_len = a_chars.len();
    let b_len = b_chars.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let mut prev_row: Vec<usize> = (0..=a_len).collect();
    let mut curr_row: Vec<usize> = vec![0; a_len + 1];

    for i in 1..=b_len {
        curr_row[0] = i;
        for j in 1..=a_len {
            let substitution_cost = usize::from(a_chars[j - 1] != b_chars[i - 1]);
            curr_row[j] = (prev_row[j] + 1)
                .min(curr_row[j - 1] + 1)
                .min(prev_row[j - 1] + substitution_cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[a_len]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Serializes tests in this module that mutate the process-wide broker
    /// environment variables, mirroring the pattern used by
    /// `config_layer::tests::env_guard`. Cargo-nextest runs each test in its
    /// own process, so this guard mainly protects the `cargo test`
    /// (single-process, multi-threaded) fallback path from interleaving
    /// `set_var`/`remove_var` calls within *this* file; it cannot protect
    /// against other test files/binaries touching the same variable names
    /// concurrently, which is why cargo-nextest (process-per-test) is the
    /// primary runner for this crate.
    fn env_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn clear_broker_env() {
        for key in BROKER_ENV_VARS {
            std::env::remove_var(key);
        }
    }

    // -- detect_broker_from_env ------------------------------------------

    #[test]
    fn detect_broker_from_env_prefers_redis_url() {
        let _guard = env_guard();
        clear_broker_env();
        std::env::set_var("REDIS_URL", "redis://from-redis-url:6379");
        std::env::set_var("CELERY_BROKER_URL", "redis://from-celery:6379");
        std::env::set_var("AMQP_URL", "amqp://from-amqp:5672");

        let detected = detect_broker_from_env();

        clear_broker_env();
        assert_eq!(detected, Some("redis://from-redis-url:6379".to_string()));
    }

    #[test]
    fn detect_broker_from_env_falls_back_to_celery_broker_url() {
        let _guard = env_guard();
        clear_broker_env();
        std::env::set_var("CELERY_BROKER_URL", "redis://from-celery:6379");
        std::env::set_var("AMQP_URL", "amqp://from-amqp:5672");

        let detected = detect_broker_from_env();

        clear_broker_env();
        assert_eq!(detected, Some("redis://from-celery:6379".to_string()));
    }

    #[test]
    fn detect_broker_from_env_falls_back_to_amqp_url() {
        let _guard = env_guard();
        clear_broker_env();
        std::env::set_var("AMQP_URL", "amqp://from-amqp:5672");

        let detected = detect_broker_from_env();

        clear_broker_env();
        assert_eq!(detected, Some("amqp://from-amqp:5672".to_string()));
    }

    #[test]
    fn detect_broker_from_env_none_when_unset() {
        let _guard = env_guard();
        clear_broker_env();

        let detected = detect_broker_from_env();

        assert_eq!(detected, None);
    }

    #[test]
    fn detect_broker_from_env_ignores_blank_values() {
        let _guard = env_guard();
        clear_broker_env();
        std::env::set_var("REDIS_URL", "   ");
        std::env::set_var("CELERY_BROKER_URL", "redis://from-celery:6379");

        let detected = detect_broker_from_env();

        clear_broker_env();
        assert_eq!(detected, Some("redis://from-celery:6379".to_string()));
    }

    // -- suggest_queue -----------------------------------------------------

    #[test]
    fn suggest_queue_picks_the_only_available_queue_when_none_requested() {
        let available = vec!["celers".to_string()];
        assert_eq!(suggest_queue(&available, None), Some("celers".to_string()));
    }

    #[test]
    fn suggest_queue_none_when_ambiguous_and_none_requested() {
        let available = vec!["a".to_string(), "b".to_string()];
        assert_eq!(suggest_queue(&available, None), None);
    }

    #[test]
    fn suggest_queue_none_when_empty_and_none_requested() {
        let available: Vec<String> = vec![];
        assert_eq!(suggest_queue(&available, None), None);
    }

    #[test]
    fn suggest_queue_none_when_requested_matches_exactly() {
        let available = vec!["orders".to_string(), "payments".to_string()];
        assert_eq!(suggest_queue(&available, Some("orders")), None);
    }

    #[test]
    fn suggest_queue_suggests_closest_match_for_a_typo() {
        let available = vec!["orders".to_string(), "payments".to_string()];
        assert_eq!(
            suggest_queue(&available, Some("order")),
            Some("orders".to_string())
        );
    }

    #[test]
    fn suggest_queue_none_when_nothing_close_enough() {
        let available = vec!["orders".to_string(), "payments".to_string()];
        assert_eq!(suggest_queue(&available, Some("zzzzzzzzzz")), None);
    }

    // -- context_suggestion -------------------------------------------------

    #[test]
    fn context_suggestion_finds_closest_command_for_a_typo() {
        let commands = vec![
            "worker".to_string(),
            "queue".to_string(),
            "task".to_string(),
        ];
        assert_eq!(
            context_suggestion("workre", &commands),
            Some("worker".to_string())
        );
    }

    #[test]
    fn context_suggestion_none_when_no_commands_available() {
        let commands: Vec<String> = vec![];
        assert_eq!(context_suggestion("workre", &commands), None);
    }

    #[test]
    fn context_suggestion_none_when_nothing_close_enough() {
        let commands = vec!["worker".to_string(), "queue".to_string()];
        assert_eq!(context_suggestion("zzzzzzzzzzzzzz", &commands), None);
    }

    // -- levenshtein_distance ------------------------------------------------

    #[test]
    fn levenshtein_distance_matches_known_values() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("a", ""), 1);
        assert_eq!(levenshtein_distance("", "a"), 1);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
        assert_eq!(levenshtein_distance("worker", "workre"), 2);
    }
}
