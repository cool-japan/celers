//! Task replay command: re-execute failed / dead-letter-queue tasks.
//!
//! This module re-enqueues failed tasks that have landed in the Dead Letter
//! Queue (DLQ) back onto their target queue. Tasks can be selected by:
//!
//! - a specific task id ([`ReplayFilter::Id`]),
//! - a name/glob pattern ([`ReplayFilter::Pattern`]), or
//! - all DLQ entries ([`ReplayFilter::All`]).
//!
//! The selection / planning logic is intentionally separated from any network
//! or broker interaction. [`plan_replay`] is a **pure** function that, given a
//! list of candidate DLQ entries, a [`ReplayFilter`], and an optional limit,
//! produces an ordered [`ReplayPlan`]. This makes the non-trivial part of the
//! command fully unit-testable without a live broker. The async
//! [`replay_dlq`] wrapper keeps the broker calls thin: it fetches DLQ entries,
//! delegates ordering/filtering to [`plan_replay`], and then either prints the
//! plan (`--dry-run`) or re-enqueues each selected task via the existing
//! [`RedisBroker::replay_from_dlq`] plumbing.

use celers_broker_redis::RedisBroker;
use celers_core::SerializedTask;
use colored::Colorize;
use uuid::Uuid;

/// Default number of DLQ entries scanned when building the replay plan.
///
/// The DLQ is read up to this many entries before filtering; the `--limit`
/// applies to the number of tasks actually replayed, not the scan window.
pub const DEFAULT_SCAN_LIMIT: isize = 1000;

/// How tasks are selected from the pool of DLQ candidates for replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayFilter {
    /// Replay a single task identified by its exact UUID (string form).
    Id(String),

    /// Replay every task whose name matches the given glob pattern.
    ///
    /// The pattern supports `*` (any run of characters, including empty) and
    /// `?` (exactly one character). All other characters match literally.
    Pattern(String),

    /// Replay all candidate tasks (subject to the optional limit).
    All,
}

/// A single candidate DLQ entry considered for replay.
///
/// This is a broker-agnostic projection of the fields needed for planning and
/// display, decoupling the pure planner from `SerializedTask`/broker types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayCandidate {
    /// Unique task identifier.
    pub id: Uuid,

    /// Task name/type identifier (matched against [`ReplayFilter::Pattern`]).
    pub name: String,

    /// Original position of the entry within the scanned DLQ listing.
    ///
    /// Lower values are older entries (head of the DLQ). The plan preserves
    /// this ordering so replays happen oldest-first, mirroring how the broker
    /// drained them in the first place.
    pub dlq_index: usize,
}

impl ReplayCandidate {
    /// Build a candidate from a serialized DLQ task and its position in the
    /// scanned listing.
    #[must_use]
    pub fn from_serialized(task: &SerializedTask, dlq_index: usize) -> Self {
        Self {
            id: task.metadata.id,
            name: task.metadata.name.clone(),
            dlq_index,
        }
    }
}

/// An ordered, ready-to-execute plan describing which tasks will be replayed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplayPlan {
    /// Tasks selected for replay, in the order they should be re-enqueued
    /// (oldest DLQ entry first).
    pub entries: Vec<ReplayCandidate>,

    /// Number of candidates that matched the filter before the limit was
    /// applied. `selected.len()` may be smaller when a limit truncates it.
    pub matched: usize,

    /// The effective limit that was applied (`None` means unlimited).
    pub limit: Option<usize>,
}

impl ReplayPlan {
    /// Number of tasks that will actually be replayed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the plan would replay nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether the limit truncated the matched set (more matched than selected).
    #[must_use]
    pub fn truncated(&self) -> bool {
        self.matched > self.entries.len()
    }
}

/// Match `name` against a glob `pattern` supporting `*` and `?` wildcards.
///
/// Semantics:
/// - `*` matches any sequence of characters (including the empty sequence),
/// - `?` matches exactly one character,
/// - every other character matches itself literally.
///
/// Matching is performed over Unicode scalar values (`char`) and the whole
/// `name` must be consumed (anchored match). This is a pure helper with no
/// allocation beyond the scratch vectors and is exercised directly by tests.
#[must_use]
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();

    // Classic two-pointer wildcard matcher with backtracking on `*`.
    let mut p = 0usize; // index into pattern
    let mut n = 0usize; // index into name
    let mut star_p: Option<usize> = None; // pattern index just after last '*'
    let mut star_n = 0usize; // name index when last '*' was seen

    while n < name.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == name[n]) {
            p += 1;
            n += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            // Record backtracking point and tentatively match zero chars.
            star_p = Some(p);
            star_n = n;
            p += 1;
        } else if let Some(sp) = star_p {
            // Backtrack: let the previous '*' absorb one more character.
            p = sp + 1;
            star_n += 1;
            n = star_n;
        } else {
            return false;
        }
    }

    // Consume any trailing '*' in the pattern.
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }

    p == pattern.len()
}

/// Build an ordered replay plan from candidate DLQ entries.
///
/// This is the **pure** core of the replay command. Given the full pool of
/// `candidates`, a [`ReplayFilter`], and an optional `limit`, it returns a
/// [`ReplayPlan`] whose `selected` entries are:
///
/// 1. filtered according to `filter`
///    - [`ReplayFilter::Id`]: keep the single entry whose id matches the
///      parsed UUID (an unparseable id matches nothing),
///    - [`ReplayFilter::Pattern`]: keep entries whose `name` matches the glob,
///    - [`ReplayFilter::All`]: keep every candidate;
/// 2. ordered oldest-first by `dlq_index` (stable; ties keep input order);
/// 3. truncated to `limit` entries when a limit is supplied.
///
/// `matched` always reflects the count *before* truncation so callers can warn
/// when a limit hid additional matches.
#[must_use]
pub fn plan_replay(
    candidates: &[ReplayCandidate],
    filter: &ReplayFilter,
    limit: Option<usize>,
) -> ReplayPlan {
    // Step 1: filter.
    let mut selected: Vec<ReplayCandidate> = match filter {
        ReplayFilter::Id(raw) => match raw.parse::<Uuid>() {
            Ok(target) => candidates
                .iter()
                .filter(|c| c.id == target)
                .cloned()
                .collect(),
            // An invalid UUID can never match a real task id.
            Err(_) => Vec::new(),
        },
        ReplayFilter::Pattern(pattern) => candidates
            .iter()
            .filter(|c| glob_match(pattern, &c.name))
            .cloned()
            .collect(),
        ReplayFilter::All => candidates.to_vec(),
    };

    // Step 2: deterministic oldest-first ordering. `sort_by_key` is stable, so
    // entries sharing a `dlq_index` retain their relative input order.
    selected.sort_by_key(|c| c.dlq_index);

    let matched = selected.len();

    // Step 3: apply the limit (0 means "replay nothing").
    if let Some(max) = limit {
        selected.truncate(max);
    }

    ReplayPlan {
        entries: selected,
        matched,
        limit,
    }
}

/// Render a human-readable summary of a replay plan to stdout.
///
/// Used by both the dry-run and live paths so the printed plan is identical.
fn print_plan(plan: &ReplayPlan, dry_run: bool) {
    let header = if dry_run {
        "=== Replay Plan (dry run) ===".bold().cyan()
    } else {
        "=== Replaying Tasks from DLQ ===".bold().cyan()
    };
    println!("{header}");
    println!();

    if plan.is_empty() {
        println!("{}", "No matching tasks to replay.".yellow());
        return;
    }

    for (idx, entry) in plan.entries.iter().enumerate() {
        println!("{}", format!("Task #{}", idx + 1).bold());
        println!("  ID: {}", entry.id.to_string().cyan());
        println!("  Name: {}", entry.name.yellow());
        println!("  DLQ position: {}", entry.dlq_index);
    }

    println!();
    println!(
        "Planned replays: {} (matched {})",
        plan.len().to_string().green(),
        plan.matched
    );
    if plan.truncated() {
        if let Some(limit) = plan.limit {
            println!(
                "{}",
                format!(
                    "Note: {} matched but limited to {} (use a larger --limit to replay more)",
                    plan.matched, limit
                )
                .yellow()
            );
        }
    }
}

/// Re-enqueue failed tasks from the DLQ back onto their target queue.
///
/// The selection / ordering is performed by the pure [`plan_replay`] function;
/// this wrapper only performs the broker I/O:
///
/// 1. read up to `DEFAULT_SCAN_LIMIT` DLQ entries,
/// 2. build [`ReplayCandidate`]s and a [`ReplayPlan`],
/// 3. print the plan and, unless `dry_run`, re-enqueue each selected task via
///    [`RedisBroker::replay_from_dlq`].
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL.
/// * `queue` - Queue name (its DLQ is scanned, replays go back to this queue).
/// * `filter` - How to select tasks ([`ReplayFilter`]).
/// * `limit` - Maximum number of tasks to replay (`None` = unlimited).
/// * `dry_run` - When `true`, print the plan without enqueuing anything.
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if the broker connection or a
/// replay operation fails.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::{replay_dlq, ReplayFilter};
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Preview replaying every failed `send_email` task (max 50).
/// replay_dlq(
///     "redis://localhost:6379",
///     "my_queue",
///     &ReplayFilter::Pattern("send_email*".to_string()),
///     Some(50),
///     true,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
pub async fn replay_dlq(
    broker_url: &str,
    queue: &str,
    filter: &ReplayFilter,
    limit: Option<usize>,
    dry_run: bool,
) -> anyhow::Result<()> {
    // Validate an explicit id up front so a typo fails fast with a clear
    // message instead of silently matching nothing.
    if let ReplayFilter::Id(raw) = filter {
        if raw.parse::<Uuid>().is_err() {
            anyhow::bail!("Invalid task ID format: {raw}");
        }
    }

    let broker = RedisBroker::new(broker_url, queue)?;

    let dlq_size = broker.dlq_size().await?;
    if dlq_size == 0 {
        println!("{}", "✓ DLQ is empty; nothing to replay".green());
        return Ok(());
    }

    // Thin broker read: pull DLQ entries, then hand off to the pure planner.
    let tasks = broker.inspect_dlq(DEFAULT_SCAN_LIMIT).await?;
    let candidates: Vec<ReplayCandidate> = tasks
        .iter()
        .enumerate()
        .map(|(idx, task)| ReplayCandidate::from_serialized(task, idx))
        .collect();

    let plan = plan_replay(&candidates, filter, limit);

    print_plan(&plan, dry_run);

    if dry_run || plan.is_empty() {
        return Ok(());
    }

    println!();
    let mut replayed = 0usize;
    let mut missing = 0usize;
    for entry in &plan.entries {
        if broker.replay_from_dlq(&entry.id).await? {
            replayed += 1;
        } else {
            // The task vanished between scan and replay (e.g. a concurrent
            // operation drained it). Report it but keep going.
            missing += 1;
            println!(
                "{}",
                format!("  ⚠ Task {} no longer in DLQ; skipped", entry.id).yellow()
            );
        }
    }

    println!(
        "{}",
        format!("✓ Replayed {replayed} task(s) from DLQ")
            .green()
            .bold()
    );
    if missing > 0 {
        println!(
            "{}",
            format!("  {missing} task(s) were no longer present and were skipped").yellow()
        );
    }
    println!("  Replayed tasks will be processed again by workers");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: Uuid, name: &str, dlq_index: usize) -> ReplayCandidate {
        ReplayCandidate {
            id,
            name: name.to_string(),
            dlq_index,
        }
    }

    fn sample_candidates() -> (Vec<ReplayCandidate>, Vec<Uuid>) {
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let candidates = vec![
            candidate(ids[0], "send_email", 0),
            candidate(ids[1], "generate_report", 1),
            candidate(ids[2], "send_sms", 2),
            candidate(ids[3], "send_email", 3),
        ];
        (candidates, ids)
    }

    // ---- glob_match ----------------------------------------------------

    #[test]
    fn glob_match_literal() {
        assert!(glob_match("send_email", "send_email"));
        assert!(!glob_match("send_email", "send_sms"));
        assert!(!glob_match("send", "send_email"));
    }

    #[test]
    fn glob_match_star() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("send_*", "send_email"));
        assert!(glob_match("send_*", "send_"));
        assert!(glob_match("*_email", "send_email"));
        assert!(glob_match("send*email", "send_my_email"));
        assert!(!glob_match("send_*", "generate_report"));
    }

    #[test]
    fn glob_match_question_mark() {
        assert!(glob_match("send_sm?", "send_sms"));
        assert!(!glob_match("send_sm?", "send_sm"));
        assert!(!glob_match("?", ""));
        assert!(glob_match("?", "x"));
    }

    #[test]
    fn glob_match_mixed_and_backtracking() {
        assert!(glob_match("a*b*c", "axxbxxc"));
        assert!(glob_match("a*b?c", "axxbzc"));
        assert!(!glob_match("a*b*c", "axxbxx"));
        // Trailing stars collapse.
        assert!(glob_match("abc***", "abc"));
    }

    // ---- plan_replay: filter by exact id -------------------------------

    #[test]
    fn plan_filter_by_exact_id() {
        let (candidates, ids) = sample_candidates();
        let plan = plan_replay(&candidates, &ReplayFilter::Id(ids[2].to_string()), None);

        assert_eq!(plan.len(), 1);
        assert_eq!(plan.matched, 1);
        assert_eq!(plan.entries[0].id, ids[2]);
        assert_eq!(plan.entries[0].name, "send_sms");
        assert!(!plan.truncated());
    }

    #[test]
    fn plan_filter_by_id_not_found() {
        let (candidates, _ids) = sample_candidates();
        let other = Uuid::new_v4().to_string();
        let plan = plan_replay(&candidates, &ReplayFilter::Id(other), None);

        assert!(plan.is_empty());
        assert_eq!(plan.matched, 0);
    }

    #[test]
    fn plan_filter_by_invalid_id_matches_nothing() {
        let (candidates, _ids) = sample_candidates();
        let plan = plan_replay(
            &candidates,
            &ReplayFilter::Id("not-a-uuid".to_string()),
            None,
        );

        assert!(plan.is_empty());
        assert_eq!(plan.matched, 0);
    }

    // ---- plan_replay: filter by glob pattern ---------------------------

    #[test]
    fn plan_filter_by_pattern() {
        let (candidates, ids) = sample_candidates();
        let plan = plan_replay(
            &candidates,
            &ReplayFilter::Pattern("send_*".to_string()),
            None,
        );

        // send_email (0), send_sms (2), send_email (3) match; generate_report not.
        assert_eq!(plan.len(), 3);
        assert_eq!(plan.matched, 3);
        let matched_ids: Vec<Uuid> = plan.entries.iter().map(|e| e.id).collect();
        assert_eq!(matched_ids, vec![ids[0], ids[2], ids[3]]);
    }

    #[test]
    fn plan_filter_by_pattern_exact_name() {
        let (candidates, ids) = sample_candidates();
        let plan = plan_replay(
            &candidates,
            &ReplayFilter::Pattern("send_email".to_string()),
            None,
        );

        assert_eq!(plan.len(), 2);
        assert_eq!(plan.entries[0].id, ids[0]);
        assert_eq!(plan.entries[1].id, ids[3]);
    }

    #[test]
    fn plan_filter_by_pattern_no_match() {
        let (candidates, _ids) = sample_candidates();
        let plan = plan_replay(
            &candidates,
            &ReplayFilter::Pattern("nonexistent_*".to_string()),
            None,
        );

        assert!(plan.is_empty());
        assert_eq!(plan.matched, 0);
    }

    // ---- plan_replay: all ----------------------------------------------

    #[test]
    fn plan_filter_all() {
        let (candidates, _ids) = sample_candidates();
        let plan = plan_replay(&candidates, &ReplayFilter::All, None);

        assert_eq!(plan.len(), 4);
        assert_eq!(plan.matched, 4);
        assert!(!plan.truncated());
    }

    // ---- plan_replay: limit --------------------------------------------

    #[test]
    fn plan_respects_limit() {
        let (candidates, ids) = sample_candidates();
        let plan = plan_replay(&candidates, &ReplayFilter::All, Some(2));

        assert_eq!(plan.len(), 2);
        // matched reflects the count *before* truncation.
        assert_eq!(plan.matched, 4);
        assert!(plan.truncated());
        // Oldest two first.
        assert_eq!(plan.entries[0].id, ids[0]);
        assert_eq!(plan.entries[1].id, ids[1]);
        assert_eq!(plan.limit, Some(2));
    }

    #[test]
    fn plan_limit_larger_than_matches_is_not_truncated() {
        let (candidates, _ids) = sample_candidates();
        let plan = plan_replay(&candidates, &ReplayFilter::All, Some(100));

        assert_eq!(plan.len(), 4);
        assert_eq!(plan.matched, 4);
        assert!(!plan.truncated());
    }

    #[test]
    fn plan_limit_zero_replays_nothing() {
        let (candidates, _ids) = sample_candidates();
        let plan = plan_replay(&candidates, &ReplayFilter::All, Some(0));

        assert!(plan.is_empty());
        // Everything matched, but the (zero) limit truncated all of it.
        assert_eq!(plan.matched, 4);
        assert!(plan.truncated());
    }

    #[test]
    fn plan_limit_combined_with_pattern() {
        let (candidates, ids) = sample_candidates();
        let plan = plan_replay(
            &candidates,
            &ReplayFilter::Pattern("send_*".to_string()),
            Some(2),
        );

        assert_eq!(plan.len(), 2);
        assert_eq!(plan.matched, 3);
        assert!(plan.truncated());
        // Oldest two of the matched send_* tasks.
        assert_eq!(plan.entries[0].id, ids[0]);
        assert_eq!(plan.entries[1].id, ids[2]);
    }

    // ---- plan_replay: ordering -----------------------------------------

    #[test]
    fn plan_orders_oldest_first_regardless_of_input_order() {
        // Provide candidates whose dlq_index is shuffled relative to vec order.
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let candidates = vec![
            candidate(ids[0], "task_a", 2),
            candidate(ids[1], "task_b", 0),
            candidate(ids[2], "task_c", 1),
        ];

        let plan = plan_replay(&candidates, &ReplayFilter::All, None);

        // Sorted by dlq_index: b(0), c(1), a(2).
        let ordered: Vec<Uuid> = plan.entries.iter().map(|e| e.id).collect();
        assert_eq!(ordered, vec![ids[1], ids[2], ids[0]]);
    }

    #[test]
    fn plan_stable_on_equal_index() {
        // Two entries share dlq_index; input order must be preserved.
        let ids: Vec<Uuid> = (0..2).map(|_| Uuid::new_v4()).collect();
        let candidates = vec![
            candidate(ids[0], "first", 5),
            candidate(ids[1], "second", 5),
        ];

        let plan = plan_replay(&candidates, &ReplayFilter::All, None);
        let ordered: Vec<Uuid> = plan.entries.iter().map(|e| e.id).collect();
        assert_eq!(ordered, vec![ids[0], ids[1]]);
    }

    // ---- plan_replay: empty candidate set ------------------------------

    #[test]
    fn plan_empty_candidates_all() {
        let plan = plan_replay(&[], &ReplayFilter::All, None);
        assert!(plan.is_empty());
        assert_eq!(plan.matched, 0);
        assert!(!plan.truncated());
    }

    #[test]
    fn plan_empty_candidates_pattern() {
        let plan = plan_replay(&[], &ReplayFilter::Pattern("*".to_string()), Some(10));
        assert!(plan.is_empty());
        assert_eq!(plan.matched, 0);
    }

    #[test]
    fn plan_empty_candidates_id() {
        let plan = plan_replay(&[], &ReplayFilter::Id(Uuid::new_v4().to_string()), None);
        assert!(plan.is_empty());
        assert_eq!(plan.matched, 0);
    }

    // ---- ReplayCandidate::from_serialized ------------------------------

    #[test]
    fn candidate_from_serialized_projects_fields() {
        let task = SerializedTask::new("my_task".to_string(), vec![1, 2, 3]);
        let expected_id = task.metadata.id;
        let candidate = ReplayCandidate::from_serialized(&task, 7);

        assert_eq!(candidate.id, expected_id);
        assert_eq!(candidate.name, "my_task");
        assert_eq!(candidate.dlq_index, 7);
    }

    // ---- ReplayPlan helpers --------------------------------------------

    #[test]
    fn plan_default_is_empty() {
        let plan = ReplayPlan::default();
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
        assert_eq!(plan.matched, 0);
        assert_eq!(plan.limit, None);
        assert!(!plan.truncated());
    }
}
