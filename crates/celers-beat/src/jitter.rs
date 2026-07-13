//! Deterministic schedule jitter (thundering-herd avoidance)
//!
//! When many scheduled entries share the same fire time (for example a fleet of
//! `interval(60)` tasks all created at process start, or a crontab firing on the
//! minute boundary) they would all be dispatched at exactly the same instant.
//! That synchronized burst — the *thundering herd* — can overload the broker or
//! downstream services.
//!
//! Jitter spreads those fires out by nudging each entry's next-run time by a
//! small, bounded offset. Crucially the offset here is **deterministic**: it is
//! derived purely from a stable hash of the entry name together with the target
//! fire time, scaled into the configured `±window`. It never consults the wall
//! clock or a random number generator. The same `(entry_name, fire_time,
//! window)` therefore always yields the same offset, which makes the behaviour
//! reproducible across restarts and across multiple beat instances, and lets the
//! unit tests assert on fixed values.
//!
//! The application is purely additive: a window of `0` produces a zero offset
//! and leaves the fire time unchanged, so enabling jitter never changes the
//! semantics of an entry that opted out.
//!
//! The reusable primitive lives in [`bounded_jitter_offset`]; [`apply_jitter`]
//! wraps it for callers that have a [`DateTime<Utc>`] in hand. The
//! [`ScheduledTask::with_jitter_window`] builder wires a symmetric window into
//! the existing next-run computation (see [`crate::task::ScheduledTask`]).

use crate::history::Jitter;
use crate::task::ScheduledTask;
use chrono::{DateTime, Duration, Utc};

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Compute a stable 64-bit hash of an entry name plus a target fire time.
///
/// This uses the FNV-1a construction over the UTF-8 bytes of `entry_name`
/// followed by the little-endian bytes of the fire time (as whole seconds since
/// the Unix epoch). FNV-1a is chosen deliberately over [`std::collections::
/// hash_map::DefaultHasher`] because its output is specified and stable across
/// Rust releases and platforms, which is exactly the reproducibility guarantee
/// the jitter feature promises. The fire time participates in the hash so that
/// successive fires of the same entry receive different offsets (avoiding a
/// fixed per-entry skew), while a given fire instant is always reproducible.
fn stable_hash(entry_name: &str, fire_time: DateTime<Utc>) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;

    for byte in entry_name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    // Mix a separator so that e.g. ("ab", t) and ("a", "b"||t) cannot collide
    // purely from byte adjacency.
    hash ^= 0xff;
    hash = hash.wrapping_mul(FNV_PRIME);

    let seconds = fire_time.timestamp();
    for byte in seconds.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    hash
}

/// Compute a deterministic, bounded jitter offset in seconds for an entry.
///
/// The returned offset lies in the inclusive range `[-window_secs, +window_secs]`
/// and is a pure function of `(entry_name, fire_time, window_secs)`. A
/// `window_secs` of `0` always returns `0` (no jitter), making the feature
/// additive — opting out reproduces the un-jittered behaviour exactly.
///
/// # Examples
/// ```
/// use celers_beat::jitter::bounded_jitter_offset;
/// use chrono::{TimeZone, Utc};
///
/// let fire = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
///
/// // Zero window is always a no-op.
/// assert_eq!(bounded_jitter_offset("task", fire, 0), 0);
///
/// // Within the bound and reproducible.
/// let off = bounded_jitter_offset("task", fire, 30);
/// assert!((-30..=30).contains(&off));
/// assert_eq!(off, bounded_jitter_offset("task", fire, 30));
/// ```
pub fn bounded_jitter_offset(entry_name: &str, fire_time: DateTime<Utc>, window_secs: u64) -> i64 {
    if window_secs == 0 {
        return 0;
    }

    let hash = stable_hash(entry_name, fire_time);

    // The full span is [-window, +window], i.e. (2 * window + 1) discrete
    // positions. Reducing the hash modulo that span and subtracting `window`
    // maps uniformly onto the symmetric range.
    let span = window_secs
        .saturating_mul(2)
        .saturating_add(1)
        .min(u64::from(u32::MAX));
    let position = (hash % span) as i64;
    position - window_secs as i64
}

/// Apply a deterministic, bounded jitter offset to a fire time.
///
/// Equivalent to adding [`bounded_jitter_offset`] seconds to `fire_time`. A
/// `window_secs` of `0` returns `fire_time` unchanged.
///
/// # Examples
/// ```
/// use celers_beat::jitter::apply_jitter;
/// use chrono::{TimeZone, Utc};
///
/// let fire = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
/// // No window => unchanged.
/// assert_eq!(apply_jitter(fire, "task", 0), fire);
///
/// let jittered = apply_jitter(fire, "task", 45);
/// let delta = (jittered - fire).num_seconds();
/// assert!((-45..=45).contains(&delta));
/// ```
pub fn apply_jitter(fire_time: DateTime<Utc>, entry_name: &str, window_secs: u64) -> DateTime<Utc> {
    let offset = bounded_jitter_offset(entry_name, fire_time, window_secs);
    fire_time + Duration::seconds(offset)
}

impl ScheduledTask {
    /// Add bounded, deterministic jitter to this entry's next-run time.
    ///
    /// This is a convenience over [`ScheduledTask::with_jitter`] that configures
    /// a symmetric `±window_secs` window. The resulting offset is derived from a
    /// stable hash of the entry name and the target fire time (see
    /// [`bounded_jitter_offset`]) and is applied additively wherever the next
    /// run is computed ([`ScheduledTask::next_run_time`],
    /// [`ScheduledTask::is_due`], and the running beat loop).
    ///
    /// A `window_secs` of `0` is a no-op: the next-run time is unchanged, so
    /// callers can configure jitter unconditionally without altering opted-out
    /// behaviour.
    ///
    /// # Examples
    /// ```
    /// use celers_beat::{Schedule, ScheduledTask};
    ///
    /// let task = ScheduledTask::new("report".to_string(), Schedule::interval(60))
    ///     .with_jitter_window(30);
    /// assert!(task.jitter.is_some());
    /// ```
    pub fn with_jitter_window(self, window_secs: u64) -> Self {
        // `i64::try_from` cannot fail for any realistic window; clamp defensively
        // to avoid an overflow panic on absurd inputs (no-unwrap policy).
        let window = i64::try_from(window_secs).unwrap_or(i64::MAX);
        self.with_jitter(Jitter::symmetric(window))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::Schedule;
    use chrono::TimeZone;

    fn fixed_fire() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 13, 12, 0, 0)
            .single()
            .expect("fixed timestamp is valid")
    }

    #[test]
    fn zero_window_is_no_op() {
        let fire = fixed_fire();
        assert_eq!(bounded_jitter_offset("anything", fire, 0), 0);
        assert_eq!(apply_jitter(fire, "anything", 0), fire);
    }

    #[test]
    fn offset_is_within_window() {
        let fire = fixed_fire();
        for name in ["a", "task-1", "really_long_entry_name_for_coverage", ""] {
            for window in [1u64, 5, 30, 60, 3600] {
                let off = bounded_jitter_offset(name, fire, window);
                assert!(
                    off >= -(window as i64) && off <= window as i64,
                    "offset {off} out of +/-{window} for {name:?}"
                );
            }
        }
    }

    #[test]
    fn offset_is_deterministic_across_repeated_calls() {
        let fire = fixed_fire();
        let first = bounded_jitter_offset("stable", fire, 120);
        for _ in 0..1000 {
            assert_eq!(bounded_jitter_offset("stable", fire, 120), first);
        }
        // And via the DateTime wrapper.
        let dt_first = apply_jitter(fire, "stable", 120);
        assert_eq!(apply_jitter(fire, "stable", 120), dt_first);
    }

    #[test]
    fn offset_differs_across_entries() {
        let fire = fixed_fire();
        // A spread of names should not collapse to a single offset.
        let names = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
        ];
        let offsets: std::collections::HashSet<i64> = names
            .iter()
            .map(|n| bounded_jitter_offset(n, fire, 600))
            .collect();
        assert!(
            offsets.len() > 1,
            "expected distinct offsets across entries, got {offsets:?}"
        );
    }

    #[test]
    fn offset_differs_across_fire_times() {
        // The same entry at different fire instants should generally differ,
        // so an entry is not permanently skewed by a constant amount.
        let base = fixed_fire();
        let later = base + Duration::seconds(60);
        let a = bounded_jitter_offset("rotating", base, 600);
        let b = bounded_jitter_offset("rotating", later, 600);
        assert_ne!(a, b);
    }

    #[test]
    fn with_jitter_window_zero_keeps_next_run_unchanged() {
        let schedule = Schedule::interval(60);
        let last = fixed_fire();

        let plain = ScheduledTask::new("t".to_string(), schedule.clone());
        let mut plain_with_last = plain.clone();
        plain_with_last.last_run_at = Some(last);

        let jittered = ScheduledTask::new("t".to_string(), schedule).with_jitter_window(0);
        let mut jittered_with_last = jittered.clone();
        jittered_with_last.last_run_at = Some(last);

        assert_eq!(
            plain_with_last
                .next_run_time()
                .expect("plain next run computes"),
            jittered_with_last
                .next_run_time()
                .expect("jittered next run computes"),
        );
    }

    #[test]
    fn with_jitter_window_applies_in_next_run_computation() {
        let schedule = Schedule::interval(60);
        let last = fixed_fire();

        let mut task = ScheduledTask::new("herd".to_string(), schedule).with_jitter_window(30);
        task.last_run_at = Some(last);

        let base_next = last + Duration::seconds(60);
        let jittered_next = task.next_run_time().expect("next run computes");
        let delta = (jittered_next - base_next).num_seconds();
        assert!(
            (-30..=30).contains(&delta),
            "jittered next-run delta {delta} outside +/-30"
        );

        // Deterministic: recomputing yields the identical instant.
        let again = task.clone();
        assert_eq!(again.next_run_time().expect("recompute"), jittered_next);
    }
}
