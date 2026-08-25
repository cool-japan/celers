//! Task simulation and load-testing command.
//!
//! This module generates synthetic task load against a queue so operators can
//! benchmark throughput, validate autoscaling, and exercise back-pressure
//! handling without writing a bespoke producer. The load is described by a
//! [`LoadTestConfig`] (total count, target rate, duration cap, task name,
//! payload size, and an [`ArrivalPattern`]) plus a `seed`.
//!
//! # Pure, deterministic planning
//!
//! The non-trivial part of the command — turning a configuration into an
//! ordered schedule of *when* each synthetic task fires and *what* it contains
//! — is implemented as the **pure** function [`plan_loadtest`]. Given a
//! [`LoadTestConfig`] it returns a [`LoadPlan`]: an ordered list of
//! [`ScheduledTask`] entries, each carrying an `offset_ms` (milliseconds after
//! start) and a fully-formed [`SyntheticTask`].
//!
//! Determinism is a hard requirement: the same configuration (including the
//! same `seed`) always yields byte-for-byte the same plan. No wall-clock or OS
//! randomness leaks into the planned schedule. Any "randomness" in the
//! [`ArrivalPattern::Jittered`] / [`ArrivalPattern::Poisson`] patterns is
//! derived from the seed via a small [`SplitMix64`] generator, so plans are
//! fully reproducible and unit-testable without a live broker.
//!
//! # Thin enqueue loop
//!
//! The async [`run_loadtest`] wrapper keeps broker interaction minimal: it
//! builds the plan with [`plan_loadtest`], and then either prints it
//! (`--dry-run`) or walks the schedule, sleeping until each entry's `offset_ms`
//! before enqueuing the corresponding task through the broker. The synthetic
//! [`SyntheticTask`] is converted to a real [`SerializedTask`] only at that
//! point, so the random task UUID assigned by `TaskMetadata::new` never
//! participates in the asserted plan math.

use std::time::Duration;

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask};
use colored::Colorize;
use tokio::time::Instant;

/// Hard upper bound on the number of synthetic tasks a single plan may contain.
///
/// This protects against a pathological configuration (e.g. a very high rate
/// over a long duration) accidentally allocating an enormous schedule. Plans
/// are capped at this many entries; [`LoadPlan::capped`] reports when the cap
/// was hit.
pub const MAX_PLAN_ENTRIES: usize = 10_000_000;

/// Maximum synthetic payload size (1 MiB), matching `SerializedTask`'s own
/// validation ceiling so a planned task can always be enqueued.
pub const MAX_PAYLOAD_BYTES: usize = 1_048_576;

/// Hard upper bound on the *total* synthetic payload volume
/// (`entries * effective_payload_size`) a single plan may request.
///
/// This is a distinct guard from [`MAX_PLAN_ENTRIES`]: a plan can stay well
/// under the entry cap yet still request an enormous total payload volume
/// (e.g. `--total 100000 --payload-size 1048576` requests ~104 GB total)
/// that would flood the broker over the life of the run even though
/// [`SyntheticTask`] now generates its payload lazily at enqueue time rather
/// than materializing every task's bytes up front. When the requested
/// volume exceeds this budget, the plan is clamped to the largest count
/// that fits ([`LoadPlan::capped_by_bytes`] reports when this happened).
pub const MAX_PLAN_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default task name used when the caller does not supply one.
pub const DEFAULT_TASK_NAME: &str = "loadtest.noop";

/// Default synthetic payload size in bytes.
pub const DEFAULT_PAYLOAD_BYTES: usize = 64;

/// Default target arrival rate in tasks per second.
pub const DEFAULT_RATE_PER_SEC: f64 = 10.0;

/// Default total number of synthetic tasks to generate.
pub const DEFAULT_TOTAL: usize = 100;

/// A small, fast, fully-deterministic pseudo-random number generator.
///
/// `SplitMix64` is used purely to derive reproducible jitter / inter-arrival
/// spacing from the configuration seed. It is **not** cryptographically secure
/// and is intentionally self-contained so the planner pulls in no `rand`
/// dependency and produces identical output across platforms.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Create a generator seeded with `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Produce the next 64-bit value, advancing the internal state.
    pub fn next_u64(&mut self) -> u64 {
        // Reference SplitMix64 constants (Vigna). All arithmetic wraps.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Produce the next value in the half-open interval `[0, 1)` as `f64`.
    ///
    /// Uses the top 53 bits so the result lands exactly on a representable
    /// double in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        // 53-bit mantissa => divide by 2^53.
        let bits = self.next_u64() >> 11;
        (bits as f64) / ((1u64 << 53) as f64)
    }
}

/// How synthetic tasks are spaced in time over the load test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrivalPattern {
    /// Evenly spaced arrivals: every task is exactly `1000 / rate` ms after the
    /// previous one. Fully deterministic and independent of the seed.
    Constant,

    /// Constant spacing perturbed by bounded, seed-derived jitter.
    ///
    /// Each nominal slot at `i * step` is shifted by up to `±jitter_fraction`
    /// of one `step`. Arrivals remain within `[0, duration]` and are sorted, so
    /// the schedule is monotonically non-decreasing.
    Jittered,

    /// Poisson-like arrivals: exponentially-distributed inter-arrival gaps with
    /// mean `1000 / rate` ms, derived deterministically from the seed.
    ///
    /// This models a memoryless arrival process (the classic open-loop load
    /// generator), where bursts and lulls occur naturally rather than on a
    /// fixed grid.
    Poisson,
}

impl ArrivalPattern {
    /// Parse a pattern from its CLI string form (case-insensitive).
    ///
    /// Accepts `constant`, `jittered` (alias `jitter`), and `poisson`.
    ///
    /// # Errors
    ///
    /// Returns an error string for any unrecognised value.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "constant" | "const" | "uniform" | "fixed" => Ok(Self::Constant),
            "jittered" | "jitter" => Ok(Self::Jittered),
            "poisson" | "exp" | "exponential" => Ok(Self::Poisson),
            other => Err(format!(
                "unknown arrival pattern '{other}' (expected one of: constant, jittered, poisson)"
            )),
        }
    }

    /// Lowercase canonical name, used in plan output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Constant => "constant",
            Self::Jittered => "jittered",
            Self::Poisson => "poisson",
        }
    }
}

/// Configuration describing a synthetic load test.
///
/// This is the sole input to the pure [`plan_loadtest`] planner. Combined with
/// the embedded `seed`, it fully determines the generated [`LoadPlan`].
#[derive(Debug, Clone, PartialEq)]
pub struct LoadTestConfig {
    /// Desired total number of synthetic tasks. The realised count may be lower
    /// when the `duration` cap or [`MAX_PLAN_ENTRIES`] truncates the schedule.
    pub total: usize,

    /// Target arrival rate in tasks per second. Must be strictly positive.
    pub rate_per_sec: f64,

    /// Optional hard cap on the test window. When set, no task is scheduled with
    /// an offset beyond `duration`, which can reduce the realised count below
    /// `total`. `None` means "run until `total` tasks have been scheduled".
    pub duration: Option<Duration>,

    /// Task name stamped on every synthetic task.
    pub task_name: String,

    /// Size, in bytes, of each synthetic task's payload (clamped to
    /// [`MAX_PAYLOAD_BYTES`]; a minimum of 1 byte is enforced because the broker
    /// rejects empty payloads).
    pub payload_size: usize,

    /// Arrival pattern controlling inter-task spacing.
    pub pattern: ArrivalPattern,

    /// Seed for the deterministic generator used by jittered / Poisson spacing.
    pub seed: u64,

    /// For [`ArrivalPattern::Jittered`]: the maximum jitter as a fraction of one
    /// inter-arrival step (e.g. `0.5` => each slot may shift by up to ±50% of a
    /// step). Clamped to `[0.0, 1.0]`. Ignored for other patterns.
    pub jitter_fraction: f64,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            total: DEFAULT_TOTAL,
            rate_per_sec: DEFAULT_RATE_PER_SEC,
            duration: None,
            task_name: DEFAULT_TASK_NAME.to_string(),
            payload_size: DEFAULT_PAYLOAD_BYTES,
            pattern: ArrivalPattern::Constant,
            seed: 0,
            jitter_fraction: 0.5,
        }
    }
}

impl LoadTestConfig {
    /// The nominal inter-arrival step in (fractional) milliseconds: `1000/rate`.
    ///
    /// Returns `None` when the rate is not strictly positive (an invalid
    /// configuration that [`plan_loadtest`] treats as producing an empty plan).
    #[must_use]
    pub fn step_ms(&self) -> Option<f64> {
        if self.rate_per_sec.is_finite() && self.rate_per_sec > 0.0 {
            Some(1000.0 / self.rate_per_sec)
        } else {
            None
        }
    }

    /// Effective payload size after clamping to the broker-safe range
    /// `[1, MAX_PAYLOAD_BYTES]`.
    #[must_use]
    pub fn effective_payload_size(&self) -> usize {
        self.payload_size.clamp(1, MAX_PAYLOAD_BYTES)
    }
}

/// A single synthetic task to be enqueued by the load test.
///
/// The `id` is a deterministic value derived from the seed and the task's
/// index, *not* a real task UUID; it exists so plans are reproducible and
/// inspectable. The actual [`SerializedTask`] (with its random UUID) is built
/// only at enqueue time in [`run_loadtest`].
///
/// Payload bytes are **not** stored on this struct -- only `seed` and
/// `payload_size`, the inputs [`build_payload`] needs to regenerate them
/// deterministically on demand via [`SyntheticTask::payload`]. A plan with a
/// large `total` and/or `payload_size` would otherwise hold every task's
/// full payload in memory simultaneously (`total * payload_size` bytes,
/// e.g. ~100 GB for `--total 100000 --payload-size 1048576`) before a single
/// task is enqueued; generating lazily bounds a plan's memory to
/// `O(total)` small structs regardless of payload size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticTask {
    /// Zero-based position of this task within the plan.
    pub index: usize,

    /// Deterministic synthetic identifier derived from `(seed, index)`.
    pub synthetic_id: u64,

    /// Task name (copied from the configuration).
    pub name: String,

    /// Seed this task's payload is deterministically derived from (see
    /// [`SyntheticTask::payload`]).
    pub seed: u64,

    /// Size, in bytes, of the payload [`SyntheticTask::payload`] generates.
    pub payload_size: usize,
}

impl SyntheticTask {
    /// Deterministically regenerate this task's payload bytes.
    ///
    /// Generated lazily rather than stored, so a [`LoadPlan`] with a large
    /// `total` and/or `payload_size` never holds more than a handful of
    /// payloads in memory at once (only the ones actually being enqueued or
    /// previewed at any given moment). Calling this twice for the same task
    /// always returns identical bytes (same `(seed, index, payload_size)` in,
    /// same [`build_payload`] output out).
    #[must_use]
    pub fn payload(&self) -> Vec<u8> {
        build_payload(self.seed, self.index, self.payload_size)
    }

    /// Convert this synthetic task into a real [`SerializedTask`] ready to
    /// enqueue. A fresh task UUID is assigned by `SerializedTask::new`; the
    /// synthetic id is not reused so the broker sees a genuine unique task.
    #[must_use]
    pub fn to_serialized(&self) -> SerializedTask {
        SerializedTask::new(self.name.clone(), self.payload())
    }
}

/// One entry in a [`LoadPlan`]: a synthetic task and the time, in milliseconds
/// after the test start, at which it should be enqueued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledTask {
    /// Milliseconds after `t0` at which to enqueue [`Self::task`]. Offsets in a
    /// plan are monotonically non-decreasing.
    pub offset_ms: u64,

    /// The synthetic task to enqueue at `offset_ms`.
    pub task: SyntheticTask,
}

/// An ordered, deterministic schedule of synthetic tasks.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LoadPlan {
    /// Scheduled tasks ordered by non-decreasing `offset_ms`.
    pub entries: Vec<ScheduledTask>,

    /// The arrival pattern the plan was generated with.
    pub pattern: Option<ArrivalPattern>,

    /// Effective per-task payload size in bytes (after clamping).
    pub payload_size: usize,

    /// `true` if [`MAX_PLAN_ENTRIES`] or [`MAX_PLAN_TOTAL_BYTES`] capped the
    /// schedule below the requested total (see also
    /// [`LoadPlan::capped_by_bytes`] to distinguish which one).
    pub capped: bool,

    /// `true` specifically when [`MAX_PLAN_TOTAL_BYTES`] (rather than
    /// [`MAX_PLAN_ENTRIES`]) is what capped the schedule -- i.e. the
    /// requested `total * effective_payload_size` exceeded the byte budget.
    pub capped_by_bytes: bool,

    /// The originally requested total, retained so callers can report when the
    /// realised count differs (duration cap / entry cap).
    pub requested_total: usize,
}

impl LoadPlan {
    /// Number of tasks the plan will enqueue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the plan would enqueue nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The largest offset in the plan (`0` for an empty plan).
    #[must_use]
    pub fn span_ms(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.offset_ms)
    }

    /// Whether the realised count is below the requested total (because of a
    /// duration cap or the entry cap).
    #[must_use]
    pub fn truncated(&self) -> bool {
        self.len() < self.requested_total
    }

    /// The effective achieved rate in tasks/second over the plan's span.
    ///
    /// Returns `0.0` for an empty/zero-span plan.
    #[must_use]
    pub fn effective_rate(&self) -> f64 {
        let span = self.span_ms();
        if span == 0 || self.entries.len() < 2 {
            return 0.0;
        }
        // (n - 1) gaps spread over `span` ms.
        ((self.entries.len() - 1) as f64) * 1000.0 / (span as f64)
    }
}

/// Build a deterministic payload of `size` bytes for the task at `index`.
///
/// The content is a function of `(seed, index, size)` only, so a given config
/// always yields identical bytes. A simple counter pattern keeps payloads cheap
/// to construct while still varying per task and per byte.
#[must_use]
fn build_payload(seed: u64, index: usize, size: usize) -> Vec<u8> {
    // Derive a per-task base value deterministically from seed + index so two
    // different tasks (or two different seeds) get visibly different payloads.
    let mut gen = SplitMix64::new(seed ^ (index as u64).wrapping_mul(0x100_0000_01b3));
    let base = gen.next_u64();
    let mut payload = Vec::with_capacity(size);
    for byte_index in 0..size {
        // Mix the base with the byte position; truncating to u8 is intentional.
        let value = base
            .wrapping_add(byte_index as u64)
            .wrapping_mul(0x0100_0000_01B3);
        payload.push((value >> 24) as u8);
    }
    payload
}

/// Compute the ordered arrival offsets (in fractional ms) for `count` tasks.
///
/// This is the heart of the pattern logic and is kept separate so it can be
/// reasoned about independently. The returned offsets are sorted ascending and
/// each lies within `[0, cap_ms]` when a cap is supplied.
fn arrival_offsets(
    count: usize,
    step_ms: f64,
    pattern: ArrivalPattern,
    seed: u64,
    jitter_fraction: f64,
    cap_ms: Option<f64>,
) -> Vec<f64> {
    if count == 0 {
        return Vec::new();
    }

    let mut offsets: Vec<f64> = match pattern {
        ArrivalPattern::Constant => (0..count).map(|i| (i as f64) * step_ms).collect(),

        ArrivalPattern::Jittered => {
            let frac = jitter_fraction.clamp(0.0, 1.0);
            let mut gen = SplitMix64::new(seed);
            (0..count)
                .map(|i| {
                    let nominal = (i as f64) * step_ms;
                    // Map [0,1) -> [-1, 1) then scale by frac * step.
                    let delta = (gen.next_f64() * 2.0 - 1.0) * frac * step_ms;
                    (nominal + delta).max(0.0)
                })
                .collect()
        }

        ArrivalPattern::Poisson => {
            // Exponential inter-arrival gaps with mean `step_ms`. The first task
            // also waits a gap so the process starts "memorylessly" rather than
            // always at t=0; this keeps the mean spacing honest.
            let mut gen = SplitMix64::new(seed);
            let mut cursor = 0.0_f64;
            (0..count)
                .map(|_| {
                    // u in (0,1]; -ln(u) is Exp(1). Guard against u==0.
                    let u = 1.0 - gen.next_f64();
                    let gap = -(u.max(f64::MIN_POSITIVE)).ln() * step_ms;
                    cursor += gap;
                    cursor
                })
                .collect()
        }
    };

    // Patterns other than Constant can perturb ordering; sort to guarantee a
    // monotonically non-decreasing schedule.
    if !matches!(pattern, ArrivalPattern::Constant) {
        offsets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    // Apply the duration cap (drop anything beyond the window).
    if let Some(cap) = cap_ms {
        offsets.retain(|&o| o <= cap + f64::EPSILON);
    }

    offsets
}

/// Build a deterministic [`LoadPlan`] from a [`LoadTestConfig`].
///
/// This is the **pure** core of the load-test command. Given a configuration
/// (including its `seed`), it returns the exact, ordered schedule of synthetic
/// tasks that [`run_loadtest`] will enqueue. It performs no I/O and reads no
/// clock or external randomness, so the same input always yields the same plan.
///
/// Behaviour:
///
/// 1. An invalid rate (`<= 0` or non-finite) or a `total` of zero yields an
///    empty plan.
/// 2. The number of *candidate* tasks is `total`, capped at
///    [`MAX_PLAN_ENTRIES`] and further capped so `candidate_count *
///    effective_payload_size` does not exceed [`MAX_PLAN_TOTAL_BYTES`]
///    (recorded in [`LoadPlan::capped`] / [`LoadPlan::capped_by_bytes`]).
/// 3. Arrival offsets are computed per [`ArrivalPattern`]: constant spacing is
///    exactly `1000 / rate` ms; jittered/Poisson spacing is derived from the
///    seed and sorted ascending.
/// 4. When [`LoadTestConfig::duration`] is set, offsets beyond the window are
///    dropped, which may reduce the realised count below `total`.
/// 5. Each surviving offset is paired with a [`SyntheticTask`] carrying a
///    deterministic id; the payload itself is generated lazily on demand
///    (see [`SyntheticTask::payload`]), so building a plan never allocates
///    `total * payload_size` bytes up front.
#[must_use]
pub fn plan_loadtest(config: &LoadTestConfig) -> LoadPlan {
    let payload_size = config.effective_payload_size();

    // Invalid rate or zero total => nothing to do.
    let Some(step_ms) = config.step_ms() else {
        return LoadPlan {
            entries: Vec::new(),
            pattern: Some(config.pattern),
            payload_size,
            capped: false,
            capped_by_bytes: false,
            requested_total: config.total,
        };
    };

    if config.total == 0 {
        return LoadPlan {
            entries: Vec::new(),
            pattern: Some(config.pattern),
            payload_size,
            capped: false,
            capped_by_bytes: false,
            requested_total: 0,
        };
    }

    // Cap the candidate count to keep the entry count bounded...
    let capped_by_entries = config.total > MAX_PLAN_ENTRIES;
    let mut candidate_count = config.total.min(MAX_PLAN_ENTRIES);

    // ...and separately cap it so the *total* payload volume the plan would
    // enqueue over its run stays under budget. `payload_size` is always in
    // `[1, MAX_PAYLOAD_BYTES]` (see `effective_payload_size`), so this can
    // never divide by zero.
    let byte_budget_count = (MAX_PLAN_TOTAL_BYTES / payload_size as u64) as usize;
    let capped_by_bytes = candidate_count > byte_budget_count;
    if capped_by_bytes {
        candidate_count = byte_budget_count.max(1);
    }

    let cap_ms = config.duration.map(|d| d.as_secs_f64() * 1000.0);

    let offsets = arrival_offsets(
        candidate_count,
        step_ms,
        config.pattern,
        config.seed,
        config.jitter_fraction,
        cap_ms,
    );

    let entries: Vec<ScheduledTask> = offsets
        .into_iter()
        .enumerate()
        .map(|(index, offset)| {
            // Round to whole milliseconds for the schedule; saturate into u64.
            let offset_ms = offset.round().max(0.0);
            let offset_ms = if offset_ms >= (u64::MAX as f64) {
                u64::MAX
            } else {
                offset_ms as u64
            };

            // Deterministic synthetic id from (seed, index).
            let mut id_gen =
                SplitMix64::new(config.seed.wrapping_add((index as u64).wrapping_add(1)));
            let synthetic_id = id_gen.next_u64();

            ScheduledTask {
                offset_ms,
                task: SyntheticTask {
                    index,
                    synthetic_id,
                    name: config.task_name.clone(),
                    seed: config.seed,
                    payload_size,
                },
            }
        })
        .collect();

    LoadPlan {
        entries,
        pattern: Some(config.pattern),
        payload_size,
        capped: capped_by_entries || capped_by_bytes,
        capped_by_bytes,
        requested_total: config.total,
    }
}

/// Render a human-readable summary of a load plan to stdout.
///
/// Shared by the dry-run and live paths so the printed plan is identical. Only
/// the first and last few entries are listed for large plans to keep output
/// manageable; the full count and timing summary are always shown.
fn print_plan(plan: &LoadPlan, dry_run: bool) {
    let header = if dry_run {
        "=== Load Test Plan (dry run) ===".bold().cyan()
    } else {
        "=== Running Load Test ===".bold().cyan()
    };
    println!("{header}");
    println!();

    let pattern_name = plan.pattern.map_or("unknown", ArrivalPattern::as_str);
    println!("  {} {}", "Pattern:".cyan(), pattern_name.yellow());
    println!(
        "  {} {} byte(s)",
        "Payload size:".cyan(),
        plan.payload_size.to_string().yellow()
    );
    println!(
        "  {} {}",
        "Planned tasks:".cyan(),
        plan.len().to_string().green()
    );
    if plan.truncated() {
        println!(
            "  {} {}",
            "Requested:".cyan(),
            plan.requested_total.to_string().yellow()
        );
    }
    if plan.capped_by_bytes {
        println!(
            "{}",
            format!(
                "  Note: capped at {} total payload byte(s) ({} byte(s) x {} entries) -- \
                 requested {} x {} byte(s) would exceed the budget",
                MAX_PLAN_TOTAL_BYTES,
                plan.payload_size,
                plan.len(),
                plan.requested_total,
                plan.payload_size,
            )
            .yellow()
        );
    } else if plan.capped {
        println!(
            "{}",
            format!("  Note: capped at {MAX_PLAN_ENTRIES} entries").yellow()
        );
    }
    let span = plan.span_ms();
    println!("  {} {} ms", "Span:".cyan(), span.to_string().yellow());
    if plan.len() >= 2 {
        println!(
            "  {} {:.2} tasks/s",
            "Effective rate:".cyan(),
            plan.effective_rate()
        );
    }
    println!();

    if plan.is_empty() {
        println!("{}", "No tasks to enqueue.".yellow());
        return;
    }

    // Show at most this many leading and trailing entries.
    const PREVIEW: usize = 5;
    let n = plan.entries.len();
    let show_all = n <= PREVIEW * 2;

    let print_entry = |entry: &ScheduledTask| {
        // `payload_size` alone is enough for the preview -- `build_payload`
        // always produces exactly that many bytes, so there is no need to
        // materialize the payload just to print its length.
        println!(
            "  [{:>8} ms] #{:<6} id={:016x} {} ({} bytes)",
            entry.offset_ms,
            entry.task.index,
            entry.task.synthetic_id,
            entry.task.name,
            entry.task.payload_size,
        );
    };

    if show_all {
        for entry in &plan.entries {
            print_entry(entry);
        }
    } else {
        for entry in &plan.entries[..PREVIEW] {
            print_entry(entry);
        }
        println!("  {}", format!("... {} more ...", n - PREVIEW * 2).dimmed());
        for entry in &plan.entries[n - PREVIEW..] {
            print_entry(entry);
        }
    }
}

/// Generate and (optionally) execute synthetic task load against a queue.
///
/// The schedule is produced by the pure [`plan_loadtest`] function; this
/// wrapper only performs broker I/O:
///
/// 1. build the [`LoadPlan`] from `config`,
/// 2. print the plan,
/// 3. unless `dry_run`, walk the schedule and, for each entry, sleep until its
///    `offset_ms` (relative to a single start instant `t0`) before enqueuing the
///    corresponding [`SerializedTask`] via the broker.
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL.
/// * `queue` - Target queue name; synthetic tasks are enqueued here.
/// * `config` - The load-test configuration (see [`LoadTestConfig`]).
/// * `dry_run` - When `true`, print the plan without enqueuing anything.
/// * `signing_key` - When `Some`, every synthetic task is signed (HMAC-SHA256,
///   [`celers_core::sign_task`] with [`celers_core::SigningOptions::default`])
///   with this shared secret before being enqueued. Without this, a worker
///   configured with signature verification (`CELERS_TASK_SIGNING_KEY`, see
///   `celers_worker::security`) dead-letters every load-test task, since an
///   unsigned message never satisfies a verifying worker's
///   [`celers_core::SignaturePolicy`]. `None` enqueues unsigned tasks, matching
///   the previous, always-unsigned behavior.
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if the broker connection or an
/// enqueue operation fails.
///
/// # Examples
///
/// ```no_run
/// # use std::time::Duration;
/// # use celers_cli::commands::loadtest_cmds::{run_loadtest, ArrivalPattern, LoadTestConfig};
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let config = LoadTestConfig {
///     total: 500,
///     rate_per_sec: 50.0,
///     duration: Some(Duration::from_secs(5)),
///     task_name: "loadtest.noop".to_string(),
///     payload_size: 128,
///     pattern: ArrivalPattern::Poisson,
///     seed: 42,
///     jitter_fraction: 0.5,
/// };
/// // Preview without touching the broker.
/// run_loadtest("redis://localhost:6379", "bench", &config, true, None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn run_loadtest(
    broker_url: &str,
    queue: &str,
    config: &LoadTestConfig,
    dry_run: bool,
    signing_key: Option<&[u8]>,
) -> anyhow::Result<()> {
    if !(config.rate_per_sec.is_finite() && config.rate_per_sec > 0.0) {
        anyhow::bail!(
            "rate must be a positive number of tasks per second (got {})",
            config.rate_per_sec
        );
    }

    let plan = plan_loadtest(config);
    print_plan(&plan, dry_run);

    if dry_run || plan.is_empty() {
        return Ok(());
    }

    // Built once, reused for every task in the run: signing is cheap
    // (HMAC-SHA256 over a small canonical byte string) but there is no
    // reason to re-derive the signer per task. `None` enqueues unsigned
    // tasks, matching the previous, always-unsigned behavior -- see the
    // signing_key argument doc above for why a verifying fleet needs this.
    let signer = signing_key.map(celers_core::TaskSigner::new);

    // Establish the broker connection up front; a single broker handles all
    // enqueues for the run.
    let broker = RedisBroker::new(broker_url, queue)?;

    println!();
    println!("{}", format!("Enqueuing {} task(s)...", plan.len()).cyan());
    if signer.is_some() {
        println!(
            "{}",
            "  (signing every task with the configured key before enqueueing)".dimmed()
        );
    }

    // Single monotonic start instant; every offset is measured from here so the
    // realised arrival times track the plan as closely as the runtime allows.
    let t0 = Instant::now();
    let mut enqueued = 0usize;
    let mut failed = 0usize;

    for entry in &plan.entries {
        // Sleep until this task's scheduled offset relative to t0. If we are
        // already past it (e.g. the broker is slower than the target rate), the
        // sleep is a no-op and we fire immediately.
        let target = t0 + Duration::from_millis(entry.offset_ms);
        let now = Instant::now();
        if target > now {
            tokio::time::sleep(target - now).await;
        }

        let mut task = entry.task.to_serialized();
        if let Some(ref signer) = signer {
            celers_core::sign_task(signer, &mut task, celers_core::SigningOptions::default());
        }
        match broker.enqueue(task).await {
            Ok(_) => {
                enqueued += 1;
                if enqueued.is_multiple_of(100) {
                    print!(
                        "\r{}",
                        format!("Enqueued {} / {} ...", enqueued, plan.len()).cyan()
                    );
                    use std::io::Write;
                    std::io::stdout().flush().ok();
                }
            }
            Err(e) => {
                failed += 1;
                println!(
                    "{}",
                    format!("  ⚠ Failed to enqueue task #{}: {e}", entry.task.index).yellow()
                );
            }
        }
    }

    let elapsed = t0.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        enqueued as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    println!();
    if failed > 0 {
        println!(
            "{}",
            format!(
                "⚠ Enqueued {enqueued} task(s) in {:.2}s ({rate:.2} tasks/s)",
                elapsed.as_secs_f64()
            )
            .yellow()
            .bold()
        );
        println!("{}", format!("  {failed} task(s) failed to enqueue").red());
    } else {
        println!(
            "{}",
            format!(
                "✓ Enqueued {enqueued} task(s) in {:.2}s ({rate:.2} tasks/s)",
                elapsed.as_secs_f64()
            )
            .green()
            .bold()
        );
    }

    // A load test that silently drops enqueue failures on the floor gives
    // no CI/scripted-usage signal that anything went wrong (see `doctor`'s
    // sibling exit-code fix for the same principle). Any failure is real
    // data loss for this run, so it must not be `Ok(())`.
    if failed > 0 {
        anyhow::bail!("{failed} of {} task(s) failed to enqueue", plan.len());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> LoadTestConfig {
        LoadTestConfig {
            total: 10,
            rate_per_sec: 10.0, // step = 100 ms
            duration: None,
            task_name: "loadtest.noop".to_string(),
            payload_size: 16,
            pattern: ArrivalPattern::Constant,
            seed: 12345,
            jitter_fraction: 0.5,
        }
    }

    // ---- SplitMix64 determinism ---------------------------------------

    #[test]
    fn splitmix64_is_deterministic_and_seed_sensitive() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        let mut c = SplitMix64::new(43);

        let seq_a: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        let seq_c: Vec<u64> = (0..8).map(|_| c.next_u64()).collect();

        assert_eq!(seq_a, seq_b, "same seed must reproduce the same sequence");
        assert_ne!(seq_a, seq_c, "different seeds should diverge");
    }

    #[test]
    fn splitmix64_next_f64_in_unit_interval() {
        let mut g = SplitMix64::new(7);
        for _ in 0..1000 {
            let v = g.next_f64();
            assert!((0.0..1.0).contains(&v), "f64 {v} out of [0,1)");
        }
    }

    // ---- ArrivalPattern parsing ---------------------------------------

    #[test]
    fn arrival_pattern_parse_accepts_known_values() {
        assert_eq!(
            ArrivalPattern::parse("constant").unwrap(),
            ArrivalPattern::Constant
        );
        assert_eq!(
            ArrivalPattern::parse("CONST").unwrap(),
            ArrivalPattern::Constant
        );
        assert_eq!(
            ArrivalPattern::parse("Jittered").unwrap(),
            ArrivalPattern::Jittered
        );
        assert_eq!(
            ArrivalPattern::parse(" jitter ").unwrap(),
            ArrivalPattern::Jittered
        );
        assert_eq!(
            ArrivalPattern::parse("poisson").unwrap(),
            ArrivalPattern::Poisson
        );
    }

    #[test]
    fn arrival_pattern_parse_rejects_unknown() {
        assert!(ArrivalPattern::parse("bogus").is_err());
        assert!(ArrivalPattern::parse("").is_err());
    }

    // ---- exact count --------------------------------------------------

    #[test]
    fn plan_produces_exact_count_unbounded() {
        let mut cfg = base_config();
        cfg.total = 250;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.len(), 250);
        assert!(!plan.truncated());
        assert!(!plan.capped);
    }

    #[test]
    fn plan_indices_are_sequential() {
        let cfg = base_config();
        let plan = plan_loadtest(&cfg);
        for (i, entry) in plan.entries.iter().enumerate() {
            assert_eq!(entry.task.index, i);
        }
    }

    #[test]
    fn plan_zero_total_is_empty() {
        let mut cfg = base_config();
        cfg.total = 0;
        let plan = plan_loadtest(&cfg);
        assert!(plan.is_empty());
        assert_eq!(plan.requested_total, 0);
        assert!(!plan.truncated());
    }

    // ---- constant-rate spacing == 1000/rate ---------------------------

    #[test]
    fn constant_spacing_matches_inverse_rate() {
        // rate = 10/s -> step = 100 ms exactly.
        let cfg = base_config();
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.pattern, Some(ArrivalPattern::Constant));

        for (i, entry) in plan.entries.iter().enumerate() {
            assert_eq!(
                entry.offset_ms,
                (i as u64) * 100,
                "entry {i} should sit on the 100 ms grid"
            );
        }
        // First entry fires immediately.
        assert_eq!(plan.entries[0].offset_ms, 0);
    }

    #[test]
    fn constant_spacing_fractional_rate() {
        // rate = 4/s -> step = 250 ms.
        let mut cfg = base_config();
        cfg.rate_per_sec = 4.0;
        cfg.total = 5;
        let plan = plan_loadtest(&cfg);
        let offsets: Vec<u64> = plan.entries.iter().map(|e| e.offset_ms).collect();
        assert_eq!(offsets, vec![0, 250, 500, 750, 1000]);
    }

    #[test]
    fn constant_spacing_is_seed_independent() {
        let mut a = base_config();
        a.seed = 1;
        let mut b = base_config();
        b.seed = 999_999;
        // Constant pattern must ignore the seed for *timing*: the offsets are a
        // pure function of the rate, not the seed. (Synthetic ids/payloads are
        // intentionally seed-derived so distinct plans remain distinguishable.)
        let offsets_a: Vec<u64> = plan_loadtest(&a)
            .entries
            .iter()
            .map(|e| e.offset_ms)
            .collect();
        let offsets_b: Vec<u64> = plan_loadtest(&b)
            .entries
            .iter()
            .map(|e| e.offset_ms)
            .collect();
        assert_eq!(offsets_a, offsets_b);
    }

    // ---- duration bound caps count ------------------------------------

    #[test]
    fn duration_caps_count() {
        // step = 100 ms, duration = 500 ms -> offsets 0,100,200,300,400,500
        // are all <= 500, i.e. 6 tasks survive out of 100.
        let mut cfg = base_config();
        cfg.total = 100;
        cfg.duration = Some(Duration::from_millis(500));
        let plan = plan_loadtest(&cfg);

        assert_eq!(plan.len(), 6);
        assert!(plan.truncated());
        assert_eq!(plan.requested_total, 100);
        assert_eq!(plan.span_ms(), 500);
        assert!(plan.entries.iter().all(|e| e.offset_ms <= 500));
    }

    #[test]
    fn duration_zero_keeps_only_immediate_tasks() {
        // With a 0 ms window only the t=0 task(s) survive for constant spacing.
        let mut cfg = base_config();
        cfg.total = 50;
        cfg.duration = Some(Duration::from_millis(0));
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.entries[0].offset_ms, 0);
    }

    #[test]
    fn duration_larger_than_span_keeps_all() {
        let mut cfg = base_config();
        cfg.total = 5; // span = 400 ms
        cfg.duration = Some(Duration::from_secs(10));
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.len(), 5);
        assert!(!plan.truncated());
    }

    // ---- invalid rate -------------------------------------------------

    #[test]
    fn invalid_rate_yields_empty_plan() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let mut cfg = base_config();
            cfg.rate_per_sec = bad;
            let plan = plan_loadtest(&cfg);
            assert!(plan.is_empty(), "rate {bad} should produce empty plan");
        }
    }

    // ---- seeded jitter reproducibility & bounds -----------------------

    #[test]
    fn jittered_plan_is_reproducible() {
        let mut cfg = base_config();
        cfg.pattern = ArrivalPattern::Jittered;
        cfg.total = 200;
        cfg.seed = 0xDEAD_BEEF;

        let plan_a = plan_loadtest(&cfg);
        let plan_b = plan_loadtest(&cfg);
        assert_eq!(
            plan_a.entries, plan_b.entries,
            "same seed must reproduce identical jittered plan"
        );
    }

    #[test]
    fn jittered_plan_differs_by_seed() {
        let mut a = base_config();
        a.pattern = ArrivalPattern::Jittered;
        a.total = 200;
        a.seed = 1;

        let mut b = a.clone();
        b.seed = 2;

        let offsets_a: Vec<u64> = plan_loadtest(&a)
            .entries
            .iter()
            .map(|e| e.offset_ms)
            .collect();
        let offsets_b: Vec<u64> = plan_loadtest(&b)
            .entries
            .iter()
            .map(|e| e.offset_ms)
            .collect();
        assert_ne!(
            offsets_a, offsets_b,
            "different seeds should yield different jittered offsets"
        );
    }

    #[test]
    fn jittered_offsets_are_sorted_and_within_jitter_bounds() {
        let mut cfg = base_config();
        cfg.pattern = ArrivalPattern::Jittered;
        cfg.total = 500;
        cfg.rate_per_sec = 10.0; // step = 100 ms
        cfg.jitter_fraction = 0.5; // +/- 50 ms around each slot
        cfg.seed = 7;
        let plan = plan_loadtest(&cfg);

        // Monotonic non-decreasing.
        for w in plan.entries.windows(2) {
            assert!(
                w[0].offset_ms <= w[1].offset_ms,
                "offsets must be sorted: {} then {}",
                w[0].offset_ms,
                w[1].offset_ms
            );
        }

        // Each *sorted* offset must remain within the jitter envelope of *some*
        // nominal slot; the strongest portable bound is that no offset exceeds
        // the last slot's upper jitter bound, and none is negative.
        let step = 100.0;
        let max_slot = ((cfg.total - 1) as f64) * step;
        let upper = max_slot + 0.5 * step + 1.0; // +1 for rounding slack
        for entry in &plan.entries {
            assert!(
                (entry.offset_ms as f64) <= upper,
                "offset {} exceeds jitter envelope {upper}",
                entry.offset_ms
            );
        }
    }

    #[test]
    fn jitter_fraction_zero_matches_constant() {
        let mut jit = base_config();
        jit.pattern = ArrivalPattern::Jittered;
        jit.jitter_fraction = 0.0;
        jit.total = 20;

        let mut con = jit.clone();
        con.pattern = ArrivalPattern::Constant;

        let jit_offsets: Vec<u64> = plan_loadtest(&jit)
            .entries
            .iter()
            .map(|e| e.offset_ms)
            .collect();
        let con_offsets: Vec<u64> = plan_loadtest(&con)
            .entries
            .iter()
            .map(|e| e.offset_ms)
            .collect();
        assert_eq!(
            jit_offsets, con_offsets,
            "zero jitter fraction collapses to constant spacing"
        );
    }

    // ---- Poisson reproducibility & properties -------------------------

    #[test]
    fn poisson_plan_is_reproducible() {
        let mut cfg = base_config();
        cfg.pattern = ArrivalPattern::Poisson;
        cfg.total = 300;
        cfg.seed = 0x1234_5678;

        assert_eq!(plan_loadtest(&cfg).entries, plan_loadtest(&cfg).entries);
    }

    #[test]
    fn poisson_offsets_are_monotonic() {
        let mut cfg = base_config();
        cfg.pattern = ArrivalPattern::Poisson;
        cfg.total = 500;
        cfg.seed = 99;
        let plan = plan_loadtest(&cfg);
        for w in plan.entries.windows(2) {
            assert!(w[0].offset_ms <= w[1].offset_ms);
        }
    }

    #[test]
    fn poisson_mean_spacing_is_near_inverse_rate() {
        // Over many samples, the average gap should approach the nominal step.
        let mut cfg = base_config();
        cfg.pattern = ArrivalPattern::Poisson;
        cfg.rate_per_sec = 10.0; // step = 100 ms
        cfg.total = 20_000;
        cfg.seed = 2024;
        let plan = plan_loadtest(&cfg);

        let span = plan.span_ms() as f64;
        let mean_gap = span / ((plan.len() - 1) as f64);
        // Allow a generous tolerance band around 100 ms for the finite sample.
        assert!(
            (80.0..120.0).contains(&mean_gap),
            "mean gap {mean_gap} ms not near 100 ms"
        );
    }

    #[test]
    fn poisson_duration_cap_truncates() {
        let mut cfg = base_config();
        cfg.pattern = ArrivalPattern::Poisson;
        cfg.rate_per_sec = 100.0; // step = 10 ms
        cfg.total = 100_000;
        cfg.duration = Some(Duration::from_millis(1000));
        cfg.seed = 5;
        let plan = plan_loadtest(&cfg);

        assert!(plan.truncated());
        assert!(plan.len() < 100_000);
        assert!(plan.entries.iter().all(|e| e.offset_ms <= 1000));
    }

    // ---- payload size honored -----------------------------------------

    #[test]
    fn payload_size_is_honored() {
        let mut cfg = base_config();
        cfg.payload_size = 256;
        cfg.total = 5;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.payload_size, 256);
        for entry in &plan.entries {
            assert_eq!(entry.task.payload_size, 256);
            // Lazily-generated bytes must actually be that length.
            assert_eq!(entry.task.payload().len(), 256);
        }
    }

    #[test]
    fn payload_size_zero_clamped_to_one() {
        // The broker rejects empty payloads; the planner must clamp up to 1.
        let mut cfg = base_config();
        cfg.payload_size = 0;
        cfg.total = 3;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.payload_size, 1);
        for entry in &plan.entries {
            assert_eq!(entry.task.payload().len(), 1);
        }
    }

    #[test]
    fn payload_size_clamped_to_max() {
        let mut cfg = base_config();
        cfg.payload_size = MAX_PAYLOAD_BYTES + 10_000;
        cfg.total = 1;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.payload_size, MAX_PAYLOAD_BYTES);
        assert_eq!(plan.entries[0].task.payload().len(), MAX_PAYLOAD_BYTES);
    }

    #[test]
    fn payload_content_is_deterministic_and_varies() {
        let mut cfg = base_config();
        cfg.payload_size = 32;
        cfg.total = 4;
        cfg.seed = 555;

        let p1 = plan_loadtest(&cfg);
        let p2 = plan_loadtest(&cfg);
        // Reproducible bytes: same (seed, index, size) always regenerates
        // identical content, even though it is no longer stored on the plan.
        assert_eq!(p1.entries[0].task.payload(), p2.entries[0].task.payload());
        // Distinct tasks have distinct payloads (with overwhelming probability).
        assert_ne!(p1.entries[0].task.payload(), p1.entries[1].task.payload());

        // A different seed changes the payload bytes.
        let mut other = cfg.clone();
        other.seed = 556;
        let p3 = plan_loadtest(&other);
        assert_ne!(p1.entries[0].task.payload(), p3.entries[0].task.payload());
    }

    #[test]
    fn payload_is_not_materialized_eagerly_in_synthetic_task() {
        // Regression guard: `SyntheticTask` must carry the inputs needed to
        // regenerate its payload (`seed`, `payload_size`), not the bytes
        // themselves -- otherwise a large plan is back to holding
        // `total * payload_size` bytes in memory before enqueuing anything.
        let mut cfg = base_config();
        cfg.total = 1;
        cfg.seed = 42;

        cfg.payload_size = 1;
        let tiny = plan_loadtest(&cfg).entries[0].task.clone();

        cfg.payload_size = 1_000_000;
        let huge = plan_loadtest(&cfg).entries[0].task.clone();

        assert_eq!(tiny.payload_size, 1);
        assert_eq!(huge.payload_size, 1_000_000);
        // The struct's own footprint must not scale with the requested
        // payload size -- if it did, `SyntheticTask` would be storing bytes
        // again instead of the (seed, index, size) needed to regenerate them.
        assert_eq!(std::mem::size_of_val(&tiny), std::mem::size_of_val(&huge));
        // And it must stay small regardless -- generously under any
        // plausible size a `String` name plus a few integers would need.
        assert!(std::mem::size_of_val(&tiny) < 128);
    }

    // ---- MAX_PLAN_TOTAL_BYTES budget -----------------------------------

    #[test]
    fn plan_loadtest_bounds_total_payload_bytes() {
        let mut cfg = base_config();
        cfg.total = 100_000;
        cfg.payload_size = MAX_PAYLOAD_BYTES; // 1 MiB/task
        let plan = plan_loadtest(&cfg);

        // Requesting 100,000 x 1 MiB (~104.8 GB) must be clamped to the
        // byte budget rather than producing anywhere close to that many
        // entries.
        let total_bytes = plan.len() as u64 * plan.payload_size as u64;
        assert!(total_bytes <= MAX_PLAN_TOTAL_BYTES);
        assert!(plan.capped_by_bytes);
        assert!(plan.capped);
        assert!(plan.len() < cfg.total);
        assert!(plan.truncated());
    }

    #[test]
    fn plan_loadtest_stays_under_byte_budget_is_not_capped() {
        // At the max payload size, the byte budget allows exactly
        // `MAX_PLAN_TOTAL_BYTES / MAX_PAYLOAD_BYTES` = 2048 entries; staying
        // at or under that must not trigger the byte cap.
        let byte_budget_count = (MAX_PLAN_TOTAL_BYTES / MAX_PAYLOAD_BYTES as u64) as usize;
        let mut cfg = base_config();
        cfg.total = byte_budget_count;
        cfg.payload_size = MAX_PAYLOAD_BYTES;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.len(), byte_budget_count);
        assert!(!plan.capped_by_bytes);
        assert!(!plan.capped);
    }

    #[test]
    fn plan_loadtest_one_past_byte_budget_is_capped() {
        let byte_budget_count = (MAX_PLAN_TOTAL_BYTES / MAX_PAYLOAD_BYTES as u64) as usize;
        let mut cfg = base_config();
        cfg.total = byte_budget_count + 1;
        cfg.payload_size = MAX_PAYLOAD_BYTES;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.len(), byte_budget_count);
        assert!(plan.capped_by_bytes);
        assert!(plan.capped);
    }

    #[test]
    fn plan_loadtest_modest_plan_is_never_byte_capped() {
        let mut cfg = base_config();
        cfg.total = 250;
        cfg.payload_size = 16;
        let plan = plan_loadtest(&cfg);
        assert!(!plan.capped_by_bytes);
        assert_eq!(plan.len(), 250);
    }

    // ---- task name honored --------------------------------------------

    #[test]
    fn task_name_is_stamped_on_every_entry() {
        let mut cfg = base_config();
        cfg.task_name = "billing.charge".to_string();
        cfg.total = 7;
        let plan = plan_loadtest(&cfg);
        assert!(plan.entries.iter().all(|e| e.task.name == "billing.charge"));
    }

    #[test]
    fn synthetic_task_to_serialized_uses_name_and_payload() {
        let mut cfg = base_config();
        cfg.task_name = "x.y".to_string();
        cfg.payload_size = 8;
        cfg.total = 1;
        let plan = plan_loadtest(&cfg);
        let serialized = plan.entries[0].task.to_serialized();
        assert_eq!(serialized.metadata.name, "x.y");
        assert_eq!(serialized.payload.len(), 8);
        // A real (random) UUID is assigned, not the synthetic id.
        assert!(!serialized.metadata.id.is_nil());
    }

    // ---- synthetic ids ------------------------------------------------

    #[test]
    fn synthetic_ids_are_deterministic_and_distinct() {
        let mut cfg = base_config();
        cfg.total = 50;
        cfg.seed = 77;
        let p1 = plan_loadtest(&cfg);
        let p2 = plan_loadtest(&cfg);

        let ids1: Vec<u64> = p1.entries.iter().map(|e| e.task.synthetic_id).collect();
        let ids2: Vec<u64> = p2.entries.iter().map(|e| e.task.synthetic_id).collect();
        assert_eq!(ids1, ids2, "synthetic ids must be reproducible");

        // Expect all-distinct ids for a modest plan.
        let mut sorted = ids1.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids1.len(), "synthetic ids should be unique");
    }

    // ---- entry cap ----------------------------------------------------

    #[test]
    fn requested_total_recorded_even_when_capped_flag_false() {
        let mut cfg = base_config();
        cfg.total = 42;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.requested_total, 42);
        assert!(!plan.capped);
    }

    // ---- LoadPlan helpers ---------------------------------------------

    #[test]
    fn load_plan_default_is_empty() {
        let plan = LoadPlan::default();
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
        assert_eq!(plan.span_ms(), 0);
        assert_eq!(plan.effective_rate(), 0.0);
        assert!(!plan.truncated());
    }

    #[test]
    fn effective_rate_tracks_constant_plan() {
        // 11 tasks at 100 ms spacing => span 1000 ms, 10 gaps => 10 tasks/s.
        let mut cfg = base_config();
        cfg.total = 11;
        let plan = plan_loadtest(&cfg);
        assert_eq!(plan.span_ms(), 1000);
        assert!((plan.effective_rate() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn step_ms_handles_valid_and_invalid_rates() {
        let mut cfg = base_config();
        cfg.rate_per_sec = 8.0;
        assert_eq!(cfg.step_ms(), Some(125.0));
        cfg.rate_per_sec = 0.0;
        assert_eq!(cfg.step_ms(), None);
        cfg.rate_per_sec = -3.0;
        assert_eq!(cfg.step_ms(), None);
    }

    // ---- dry-run plan content (rendering does not panic, plan is stable) --

    #[test]
    fn dry_run_plan_content_matches_planner() {
        // The "dry-run plan content" is exactly what plan_loadtest produced;
        // assert the observable fields a user would see in the dry-run output.
        let mut cfg = base_config();
        cfg.total = 3;
        cfg.rate_per_sec = 2.0; // step = 500 ms
        cfg.payload_size = 10;
        cfg.task_name = "report.daily".to_string();
        let plan = plan_loadtest(&cfg);

        assert_eq!(plan.len(), 3);
        assert_eq!(plan.payload_size, 10);
        assert_eq!(plan.pattern, Some(ArrivalPattern::Constant));
        let offsets: Vec<u64> = plan.entries.iter().map(|e| e.offset_ms).collect();
        assert_eq!(offsets, vec![0, 500, 1000]);
        assert!(plan.entries.iter().all(|e| e.task.name == "report.daily"));
        // Rendering must not panic for either path.
        print_plan(&plan, true);
        print_plan(&plan, false);
    }

    #[test]
    fn print_plan_handles_large_and_empty_plans() {
        // Large plan exercises the head/tail preview branch.
        let mut cfg = base_config();
        cfg.total = 100;
        let plan = plan_loadtest(&cfg);
        print_plan(&plan, true);

        // Empty plan branch.
        let empty = LoadPlan::default();
        print_plan(&empty, true);
    }

    // ---- live-broker regression test (idx 340) --------------------------
    //
    // Local Redis used by this module's live-broker regression test, same
    // convention as `commands::task`/`commands::replay_cmds`. Scopes its
    // own queue name with a fresh UUID so concurrent test runs never
    // collide.
    const TEST_BROKER_URL: &str = "redis://127.0.0.1:6379";

    /// Regression test for idx 340: `run_loadtest` used to always return
    /// `Ok(())` even when every enqueue failed, printing only a dimmed
    /// warning line with no exit-code signal for CI/scripted usage. Forces
    /// every enqueue to fail deterministically (the main queue key is made
    /// a Redis STRING, so the broker's LPUSH/RPUSH hits `WRONGTYPE`) and
    /// confirms `run_loadtest` now returns `Err`.
    #[tokio::test]
    async fn run_loadtest_fails_when_every_enqueue_fails() {
        let queue_name = format!("test-loadtest-fail-{}", uuid::Uuid::new_v4());

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let _: () = redis::cmd("SET")
            .arg(&queue_name)
            .arg("not-a-queue")
            .query_async(&mut conn)
            .await
            .expect("seed wrong-type key");

        let mut cfg = base_config();
        cfg.total = 3;
        cfg.rate_per_sec = 1000.0; // negligible inter-arrival sleep
        cfg.jitter_fraction = 0.0;

        let result = run_loadtest(TEST_BROKER_URL, &queue_name, &cfg, false, None).await;
        assert!(
            result.is_err(),
            "run_loadtest must fail (nonzero exit) when every enqueue fails"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(&queue_name)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
    }

    /// A load test where every task enqueues successfully must still
    /// return `Ok(())` -- the fix must not turn a healthy run into a
    /// false-positive failure.
    #[tokio::test]
    async fn run_loadtest_succeeds_when_all_enqueue() {
        let queue_name = format!("test-loadtest-ok-{}", uuid::Uuid::new_v4());

        let mut cfg = base_config();
        cfg.total = 5;
        cfg.rate_per_sec = 1000.0;
        cfg.jitter_fraction = 0.0;

        let result = run_loadtest(TEST_BROKER_URL, &queue_name, &cfg, false, None).await;
        assert!(result.is_ok(), "healthy run must not fail: {result:?}");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let _: i64 = redis::cmd("DEL")
            .arg(&queue_name)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
    }

    /// Regression test for the security followup: `loadtest` used to mint
    /// unsigned `SerializedTask`s unconditionally, so a verifying worker
    /// dead-lettered every load-test task. With `signing_key: Some(...)`,
    /// every enqueued task must carry a signature that verifies against
    /// that same key.
    #[tokio::test]
    async fn run_loadtest_signs_every_task_when_a_signing_key_is_given() {
        let queue_name = format!("test-loadtest-signed-{}", uuid::Uuid::new_v4());
        let key = b"loadtest-unit-test-signing-key!";

        let mut cfg = base_config();
        cfg.total = 3;
        cfg.rate_per_sec = 1000.0;
        cfg.jitter_fraction = 0.0;

        let result = run_loadtest(TEST_BROKER_URL, &queue_name, &cfg, false, Some(key)).await;
        assert!(result.is_ok(), "a signed run must not fail: {result:?}");

        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");
        let signer = celers_core::TaskSigner::new(key);
        let mut verified = 0;
        while let Ok(Some(msg)) = broker.try_dequeue().await {
            let envelope = msg
                .task
                .metadata
                .signature
                .clone()
                .expect("every enqueued task must carry a signature envelope");
            let fields = celers_core::signed_fields(&msg.task);
            signer
                .verify(&fields, &envelope.signature)
                .expect("the signature must verify against the same key it was signed with");
            verified += 1;
        }
        assert_eq!(
            verified, cfg.total,
            "every task in the run must have been signed and enqueued"
        );
    }

    /// Without a signing key, tasks must stay unsigned -- matching the
    /// previous, always-unsigned behavior for anyone not opting in.
    #[tokio::test]
    async fn run_loadtest_leaves_tasks_unsigned_without_a_signing_key() {
        let queue_name = format!("test-loadtest-unsigned-{}", uuid::Uuid::new_v4());

        let mut cfg = base_config();
        cfg.total = 2;
        cfg.rate_per_sec = 1000.0;
        cfg.jitter_fraction = 0.0;

        let result = run_loadtest(TEST_BROKER_URL, &queue_name, &cfg, false, None).await;
        assert!(result.is_ok(), "an unsigned run must not fail: {result:?}");

        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");
        let mut seen = 0;
        while let Ok(Some(msg)) = broker.try_dequeue().await {
            assert!(
                msg.task.metadata.signature.is_none(),
                "without a signing key, no task must carry a signature"
            );
            seen += 1;
        }
        assert_eq!(seen, cfg.total);
    }
}
