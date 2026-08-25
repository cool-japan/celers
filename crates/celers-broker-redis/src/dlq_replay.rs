//! Automatic DLQ Replay Policies
//!
//! Provides intelligent replay policies for Dead Letter Queue (DLQ) tasks:
//! - Time-based replay (retry after N hours)
//! - Conditional replay based on error type
//! - Gradual replay with rate limiting
//! - Smart scheduling based on error patterns
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::dlq_replay::{ReplayPolicy, ReplayScheduler, ReplayCondition};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut scheduler = ReplayScheduler::new("redis://localhost:6379", "my_queue").await?;
//!
//! // Time-based replay: retry tasks after 1 hour
//! let policy = ReplayPolicy::time_based(Duration::from_secs(3600));
//! scheduler.add_policy("hourly_retry", policy).await?;
//!
//! // Conditional replay: only retry network errors
//! let policy = ReplayPolicy::conditional(
//!     ReplayCondition::ErrorType("NetworkError".to_string()),
//!     Duration::from_secs(300)
//! );
//! scheduler.add_policy("network_retry", policy).await?;
//!
//! // Gradual replay: 10 tasks per minute
//! let policy = ReplayPolicy::rate_limited(10, Duration::from_secs(60));
//! scheduler.add_policy("gradual", policy).await?;
//!
//! // Start the scheduler. `run`/`stop` both take `&self`, so `stop()` can be
//! // called from another handle to the same (e.g. `Arc`-shared) scheduler
//! // while `run()` is executing on a spawned task.
//! scheduler.run().await?;
//! # Ok(())
//! # }
//! ```

use crate::connection::RedisClientExt;
use crate::QueueMode;
use celers_core::{CelersError, Result, SerializedTask, TaskState};
use redis::{AsyncCommands, Client, Script};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

/// Extract the human-readable failure reason recorded on a task, if any.
///
/// A task that landed in the DLQ carries its failure information in its state:
/// `TaskState::Failed(message)` holds the error message produced by the worker,
/// while `TaskState::Retrying(n)` indicates exhausted retries. This is the real
/// "task result" used for error classification and adaptive replay decisions.
fn failure_reason(task: &SerializedTask) -> Option<String> {
    match &task.metadata.state {
        TaskState::Failed(message) => Some(message.clone()),
        TaskState::Retrying(attempts) => {
            Some(format!("retry attempts exhausted after {attempts} tries"))
        }
        TaskState::Rejected => Some("task rejected".to_string()),
        TaskState::Revoked => Some("task revoked".to_string()),
        TaskState::Custom {
            name,
            metadata: Some(meta),
        } => Some(format!("{name}: {}", String::from_utf8_lossy(meta))),
        TaskState::Custom {
            name,
            metadata: None,
        } => Some(name.clone()),
        _ => None,
    }
}

/// Classification of a DLQ failure used to drive adaptive replay timing.
///
/// Transient failures (network/timeout/resource) are good candidates for an
/// early retry, whereas permanent failures (validation/data corruption) will
/// not succeed on replay and are deprioritized or skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    /// Transient: likely to succeed on retry (network, connection, timeout).
    Transient,
    /// Resource pressure: retry after a longer cooldown (memory, rate limits).
    Resource,
    /// Permanent: replay will not help (validation, corruption, bad input).
    Permanent,
    /// Unknown classification.
    Unknown,
}

impl FailureKind {
    /// Classify a task from its recorded failure reason (falling back to the
    /// task name when no explicit reason is present).
    fn classify(task: &SerializedTask) -> Self {
        let reason = failure_reason(task).unwrap_or_default();
        let haystack = format!("{} {}", task.metadata.name, reason).to_lowercase();

        if haystack.contains("network")
            || haystack.contains("connection")
            || haystack.contains("timeout")
            || haystack.contains("timed out")
            || haystack.contains("unavailable")
            || haystack.contains("temporarily")
        {
            FailureKind::Transient
        } else if haystack.contains("memory")
            || haystack.contains("resource")
            || haystack.contains("rate limit")
            || haystack.contains("too many")
            || haystack.contains("throttle")
        {
            FailureKind::Resource
        } else if haystack.contains("validation")
            || haystack.contains("invalid")
            || haystack.contains("corrupt")
            || haystack.contains("checksum")
            || haystack.contains("parse")
            || haystack.contains("malformed")
            || haystack.contains("deserialize")
        {
            FailureKind::Permanent
        } else {
            FailureKind::Unknown
        }
    }

    /// Ordering priority for replay (lower value = replayed earlier).
    fn replay_priority(self) -> u8 {
        match self {
            FailureKind::Transient => 0,
            FailureKind::Unknown => 1,
            FailureKind::Resource => 2,
            FailureKind::Permanent => 3,
        }
    }

    /// Multiplier applied to the base delay before replaying this kind.
    fn delay_multiplier(self) -> u32 {
        match self {
            FailureKind::Transient => 1,
            FailureKind::Unknown => 2,
            FailureKind::Resource => 4,
            FailureKind::Permanent => 8,
        }
    }
}

/// A named, registrable predicate for [`ReplayCondition::Custom`].
type CustomPredicate = Arc<dyn Fn(&SerializedTask) -> bool + Send + Sync>;

/// DLQ replay policy scheduler
pub struct ReplayScheduler {
    client: Client,
    queue_name: String,
    dlq_key: String,
    /// Must match the mode of the queue being replayed into: a `Priority`
    /// queue is a Redis sorted set, so replay must `ZADD` rather than
    /// `LPUSH` (which fails with `WRONGTYPE` against a sorted set).
    mode: QueueMode,
    policies: HashMap<String, ReplayPolicy>,
    /// Registry backing [`ReplayCondition::Custom`] — see
    /// [`Self::register_predicate`].
    predicates: HashMap<String, CustomPredicate>,
    /// How often [`Self::run`] wakes up to call [`Self::execute_once`].
    poll_interval: Duration,
    running: Arc<AtomicBool>,
}

impl std::fmt::Debug for ReplayScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayScheduler")
            .field("queue_name", &self.queue_name)
            .field("dlq_key", &self.dlq_key)
            .field("mode", &self.mode)
            .field("policies", &self.policies)
            .field(
                "registered_predicates",
                &self.predicates.keys().collect::<Vec<_>>(),
            )
            .field("poll_interval", &self.poll_interval)
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Replay policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayPolicy {
    /// Policy type
    pub policy_type: ReplayPolicyType,
    /// Maximum retry attempts for replayed tasks
    pub max_retries: usize,
    /// Whether this policy is enabled
    pub enabled: bool,
    /// Priority (higher = runs first)
    pub priority: u8,
}

/// Type of replay policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplayPolicyType {
    /// Time-based replay: retry after specified duration
    TimeBased {
        /// Delay before retry
        delay: Duration,
        /// Maximum age of tasks to replay
        max_age: Option<Duration>,
    },
    /// Conditional replay based on error patterns
    Conditional {
        /// Condition to match
        condition: ReplayCondition,
        /// Delay before retry
        delay: Duration,
    },
    /// Rate-limited replay
    RateLimited {
        /// Maximum tasks per window
        max_tasks_per_window: usize,
        /// Time window
        window: Duration,
    },
    /// Smart replay based on error analysis
    Smart {
        /// Analyze patterns and adjust replay timing
        adaptive: bool,
        /// Base delay
        base_delay: Duration,
    },
}

/// Condition for conditional replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplayCondition {
    /// Match specific error type
    ErrorType(String),
    /// Match task name pattern
    TaskName(String),
    /// Match multiple conditions (AND)
    All(Vec<ReplayCondition>),
    /// Match any condition (OR)
    Any(Vec<ReplayCondition>),
    /// Custom predicate, looked up by name in the owning
    /// [`ReplayScheduler`]'s predicate registry — see
    /// [`ReplayScheduler::register_predicate`].
    Custom(String),
}

/// Replay execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    /// Number of tasks replayed
    pub replayed_count: usize,
    /// Number of tasks skipped
    pub skipped_count: usize,
    /// Number of tasks failed to replay
    pub failed_count: usize,
    /// Policy that executed
    pub policy_name: String,
    /// Execution duration
    pub duration: Duration,
}

/// Replay statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayStats {
    /// Total replays executed
    pub total_replays: usize,
    /// Total tasks replayed
    pub total_tasks_replayed: usize,
    /// Success rate (0.0-1.0)
    pub success_rate: f64,
    /// Average tasks per replay
    pub avg_tasks_per_replay: f64,
    /// Last replay timestamp
    pub last_replay: Option<i64>,
}

impl ReplayPolicy {
    /// Create a time-based replay policy
    pub fn time_based(delay: Duration) -> Self {
        Self {
            policy_type: ReplayPolicyType::TimeBased {
                delay,
                max_age: None,
            },
            max_retries: 3,
            enabled: true,
            priority: 50,
        }
    }

    /// Create a conditional replay policy
    pub fn conditional(condition: ReplayCondition, delay: Duration) -> Self {
        Self {
            policy_type: ReplayPolicyType::Conditional { condition, delay },
            max_retries: 3,
            enabled: true,
            priority: 75,
        }
    }

    /// Create a rate-limited replay policy
    pub fn rate_limited(max_tasks_per_window: usize, window: Duration) -> Self {
        Self {
            policy_type: ReplayPolicyType::RateLimited {
                max_tasks_per_window,
                window,
            },
            max_retries: 3,
            enabled: true,
            priority: 25,
        }
    }

    /// Create a smart adaptive replay policy
    pub fn smart(base_delay: Duration) -> Self {
        Self {
            policy_type: ReplayPolicyType::Smart {
                adaptive: true,
                base_delay,
            },
            max_retries: 5,
            enabled: true,
            priority: 100,
        }
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Disable the policy
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

impl ReplayCondition {
    /// Collect every `Custom` predicate name referenced anywhere within this
    /// condition (recursing into `All`/`Any`).
    fn custom_predicate_names(&self, out: &mut Vec<String>) {
        match self {
            ReplayCondition::Custom(name) => out.push(name.clone()),
            ReplayCondition::All(conditions) | ReplayCondition::Any(conditions) => {
                for c in conditions {
                    c.custom_predicate_names(out);
                }
            }
            ReplayCondition::ErrorType(_) | ReplayCondition::TaskName(_) => {}
        }
    }

    /// Check if a task matches this condition.
    ///
    /// `predicates` is the registry of named callables backing
    /// [`ReplayCondition::Custom`] (see
    /// [`ReplayScheduler::register_predicate`]). A `Custom` name with no
    /// registered predicate is treated as non-matching (and logged) rather
    /// than panicking or silently succeeding —
    /// [`ReplayScheduler::add_policy`] is the primary enforcement point and
    /// rejects a policy referencing an unknown name *before* it is ever
    /// evaluated here; this is a defensive fallback for a predicate that was
    /// unregistered after a policy referencing it was already added.
    pub fn matches(
        &self,
        task: &SerializedTask,
        predicates: &HashMap<String, CustomPredicate>,
    ) -> bool {
        match self {
            ReplayCondition::ErrorType(error_type) => {
                // Match against the recorded failure reason (the real error
                // recorded on the task state) and fall back to the task name so
                // callers that classify by task type still work.
                let needle = error_type.to_lowercase();
                if let Some(reason) = failure_reason(task) {
                    if reason.to_lowercase().contains(&needle) {
                        return true;
                    }
                }
                task.metadata.name.to_lowercase().contains(&needle)
            }
            ReplayCondition::TaskName(pattern) => task.metadata.name.contains(pattern),
            ReplayCondition::All(conditions) => {
                conditions.iter().all(|c| c.matches(task, predicates))
            }
            ReplayCondition::Any(conditions) => {
                conditions.iter().any(|c| c.matches(task, predicates))
            }
            ReplayCondition::Custom(name) => match predicates.get(name) {
                Some(predicate) => predicate(task),
                None => {
                    warn!(
                        "ReplayCondition::Custom(\"{name}\") has no registered predicate — \
                         treating as non-match. This should have been caught by \
                         ReplayScheduler::add_policy at registration time."
                    );
                    false
                }
            },
        }
    }
}

/// Outcome of attempting to move one DLQ entry into the live queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayOutcome {
    /// Moved from the DLQ into the live queue.
    Replayed,
    /// The exact entry was no longer present in the DLQ (already moved by a
    /// concurrent replay, or already removed) — not an error.
    NotFound,
    /// The per-policy retry budget (`ReplayPolicy::max_retries`) for this
    /// task has already been spent; left in the DLQ untouched.
    Exhausted,
}

/// Atomically gate on the per-`(policy, task)` retry budget, then move a DLQ
/// entry into the live queue.
///
/// `KEYS[1]` = DLQ key, `KEYS[2]` = destination queue key, `KEYS[3]` =
/// per-policy replay-attempts hash key. `ARGV[1]` = the exact DLQ entry
/// payload, `ARGV[2]` = `"1"` for priority mode (`ZADD`) or `"0"` for FIFO
/// (`LPUSH`), `ARGV[3]` = the ZADD score (ignored in FIFO mode), `ARGV[4]` =
/// the task id (attempts-hash field), `ARGV[5]` = max retries.
///
/// FIFO replay pushes to the **head**, matching `RedisBroker::enqueue`:
/// consumers pop the tail, so a tail push would put a task that has already
/// failed once ahead of every task currently waiting.
///
/// Returns `1` (replayed), `0` (entry not found in the DLQ — a concurrent
/// replay already claimed it), or `-1` (retry budget exhausted). Doing the
/// budget check, the `LREM`, the attempt-count increment, and the
/// destination push in one `EVAL` is what makes this atomic: unlike a
/// separate push followed by a separate `LREM`, there is no window in
/// which a task could be duplicated (present in both the DLQ and the live
/// queue) or double-counted against its retry budget.
const REPLAY_MOVE_SCRIPT: &str = r#"
local attempts = tonumber(redis.call('HGET', KEYS[3], ARGV[4]) or '0')
if attempts >= tonumber(ARGV[5]) then
    return -1
end
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
    return 0
end
redis.call('HINCRBY', KEYS[3], ARGV[4], 1)
if ARGV[2] == '1' then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
else
    redis.call('LPUSH', KEYS[2], ARGV[1])
end
return 1
"#;

impl ReplayScheduler {
    /// Create a new replay scheduler for a FIFO-mode queue.
    ///
    /// Use [`Self::with_mode`] for a queue running in
    /// [`QueueMode::Priority`] — replaying into a priority queue with FIFO
    /// semantics issues `LPUSH` against what is actually a Redis sorted set,
    /// which fails with `WRONGTYPE`.
    pub async fn new(redis_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_mode(redis_url, queue_name, QueueMode::Fifo).await
    }

    /// Create a new replay scheduler for a queue running in `mode`.
    ///
    /// `mode` must match the mode the target queue actually uses (the same
    /// value passed to `RedisBroker::with_mode`), since a `Priority` queue
    /// is a Redis sorted set and replay must `ZADD` into it rather than
    /// `LPUSH`.
    pub async fn with_mode(redis_url: &str, queue_name: &str, mode: QueueMode) -> Result<Self> {
        let client = crate::connection::open_client(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            dlq_key: format!("{}:dlq", queue_name),
            mode,
            policies: HashMap::new(),
            predicates: HashMap::new(),
            poll_interval: Duration::from_secs(60),
            running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Set how often [`Self::run`] wakes up to execute policies (default: 60s).
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Register a named predicate usable by `ReplayCondition::Custom(name)`.
    ///
    /// Must be called before [`Self::add_policy`] for any policy
    /// referencing `name` — `add_policy` validates every `Custom` name
    /// against this registry and returns `Err` for one that is not yet
    /// registered, instead of silently accepting a policy that could never
    /// match anything.
    pub fn register_predicate<F>(&mut self, name: &str, predicate: F)
    where
        F: Fn(&SerializedTask) -> bool + Send + Sync + 'static,
    {
        self.predicates
            .insert(name.to_string(), Arc::new(predicate));
    }

    /// Add a replay policy.
    ///
    /// Returns `Err` if `policy` is a [`ReplayPolicyType::Conditional`]
    /// whose condition references a [`ReplayCondition::Custom`] name that
    /// has not been registered via [`Self::register_predicate`] — this
    /// makes an unknown predicate name fail loudly at registration time
    /// rather than silently evaluating to "never matches" forever.
    pub async fn add_policy(&mut self, name: &str, policy: ReplayPolicy) -> Result<()> {
        if let ReplayPolicyType::Conditional { condition, .. } = &policy.policy_type {
            let mut custom_names = Vec::new();
            condition.custom_predicate_names(&mut custom_names);
            for predicate_name in custom_names {
                if !self.predicates.contains_key(&predicate_name) {
                    return Err(CelersError::Broker(format!(
                        "Policy '{}' references unregistered custom replay predicate '{}': \
                         call register_predicate(\"{}\", ...) before adding this policy",
                        name, predicate_name, predicate_name
                    )));
                }
            }
        }

        self.policies.insert(name.to_string(), policy);
        debug!("Added replay policy: {}", name);
        Ok(())
    }

    /// Remove a replay policy
    pub async fn remove_policy(&mut self, name: &str) -> Result<()> {
        self.policies.remove(name);
        debug!("Removed replay policy: {}", name);
        Ok(())
    }

    /// Enable a policy
    pub async fn enable_policy(&mut self, name: &str) -> Result<()> {
        if let Some(policy) = self.policies.get_mut(name) {
            policy.enabled = true;
            info!("Enabled replay policy: {}", name);
            Ok(())
        } else {
            Err(CelersError::Broker(format!("Policy not found: {}", name)))
        }
    }

    /// Disable a policy
    pub async fn disable_policy(&mut self, name: &str) -> Result<()> {
        if let Some(policy) = self.policies.get_mut(name) {
            policy.enabled = false;
            info!("Disabled replay policy: {}", name);
            Ok(())
        } else {
            Err(CelersError::Broker(format!("Policy not found: {}", name)))
        }
    }

    /// List all policies
    pub fn list_policies(&self) -> Vec<(String, ReplayPolicy)> {
        self.policies
            .iter()
            .map(|(name, policy)| (name.clone(), policy.clone()))
            .collect()
    }

    /// Execute all enabled policies once
    pub async fn execute_once(&self) -> Result<Vec<ReplayResult>> {
        let mut results = Vec::new();

        // Sort policies by priority (highest first)
        let mut policies: Vec<_> = self.policies.iter().filter(|(_, p)| p.enabled).collect();
        policies.sort_by_key(|b| std::cmp::Reverse(b.1.priority));

        for (name, policy) in policies {
            match self.execute_policy(name, policy).await {
                Ok(result) => {
                    info!("Policy {} replayed {} tasks", name, result.replayed_count);
                    results.push(result);
                }
                Err(e) => {
                    error!("Policy {} failed: {}", name, e);
                }
            }
        }

        Ok(results)
    }

    /// Run the scheduler continuously.
    ///
    /// Takes `&self` (not `&mut self`) specifically so [`Self::stop`] can be
    /// called from a different handle to the same scheduler (e.g. wrap it in
    /// an `Arc`, clone the `Arc` before spawning `run()` on a task, and call
    /// `stop()` on the original) while `run()` is executing.
    pub async fn run(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        info!(
            "Starting DLQ replay scheduler for queue: {}",
            self.queue_name
        );

        let mut tick = interval(self.poll_interval);

        loop {
            tick.tick().await;

            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if let Err(e) = self.execute_once().await {
                error!("Scheduler execution error: {}", e);
            }
        }

        Ok(())
    }

    /// Stop the scheduler. See [`Self::run`] for why this takes `&self`.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("Stopping DLQ replay scheduler");
    }

    /// Whether the scheduler is currently (or was most recently) running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Execute a specific policy
    async fn execute_policy(&self, name: &str, policy: &ReplayPolicy) -> Result<ReplayResult> {
        let start = std::time::Instant::now();
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        // Get tasks from DLQ
        let dlq_size: usize = conn
            .llen(&self.dlq_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ size: {}", e)))?;

        if dlq_size == 0 {
            return Ok(ReplayResult {
                replayed_count: 0,
                skipped_count: 0,
                failed_count: 0,
                policy_name: name.to_string(),
                duration: start.elapsed(),
            });
        }

        match &policy.policy_type {
            ReplayPolicyType::TimeBased { delay, max_age } => {
                self.execute_time_based(
                    &mut conn,
                    name,
                    policy.max_retries,
                    *delay,
                    *max_age,
                    start,
                )
                .await
            }
            ReplayPolicyType::Conditional { condition, delay } => {
                self.execute_conditional(
                    &mut conn,
                    name,
                    condition,
                    policy.max_retries,
                    *delay,
                    start,
                )
                .await
            }
            ReplayPolicyType::RateLimited {
                max_tasks_per_window,
                window,
            } => {
                self.execute_rate_limited(
                    &mut conn,
                    name,
                    policy.max_retries,
                    *max_tasks_per_window,
                    *window,
                    start,
                )
                .await
            }
            ReplayPolicyType::Smart {
                adaptive,
                base_delay,
            } => {
                self.execute_smart(
                    &mut conn,
                    name,
                    policy.max_retries,
                    *adaptive,
                    *base_delay,
                    start,
                )
                .await
            }
        }
    }

    /// Atomically move one DLQ entry (`raw`, the exact serialized payload
    /// present in the DLQ list) into the live queue, respecting queue mode,
    /// the per-policy retry budget, and DLQ/queue consistency. See
    /// [`REPLAY_MOVE_SCRIPT`].
    async fn move_dlq_entry_to_queue(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        raw: &str,
        task: &SerializedTask,
        max_retries: usize,
    ) -> Result<ReplayOutcome> {
        let attempts_key = format!("{}:dlq:replay_attempts:{}", self.queue_name, policy_name);
        let is_priority = matches!(self.mode, QueueMode::Priority);
        // Mirrors `RedisBroker::enqueue`'s scoring convention: negate
        // priority so a higher-priority task sorts first (ZSET pops lowest
        // score first).
        let score = -(task.metadata.priority as f64);

        let script = Script::new(REPLAY_MOVE_SCRIPT);
        let result: i64 = script
            .key(&self.dlq_key)
            .key(&self.queue_name)
            .key(&attempts_key)
            .arg(raw)
            .arg(if is_priority { "1" } else { "0" })
            .arg(score)
            .arg(task.metadata.id.to_string())
            .arg(max_retries)
            .invoke_async(conn)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to move DLQ entry to queue: {}", e))
            })?;

        Ok(match result {
            1 => ReplayOutcome::Replayed,
            0 => ReplayOutcome::NotFound,
            _ => ReplayOutcome::Exhausted,
        })
    }

    /// Execute time-based replay.
    ///
    /// Honors `delay` (a task must have been in the DLQ for at least this
    /// long, measured from `task.metadata.updated_at` — the best available
    /// real timestamp; see the `dlq_archival` module for the same caveat)
    /// and `max_age` (a task older than this is left in the DLQ rather than
    /// replayed forever), and enforces `max_retries` via
    /// [`Self::move_dlq_entry_to_queue`].
    #[allow(clippy::too_many_arguments)]
    async fn execute_time_based(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        max_retries: usize,
        delay: Duration,
        max_age: Option<Duration>,
        start: std::time::Instant,
    ) -> Result<ReplayResult> {
        // Get up to 100 tasks from DLQ
        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, 99)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let mut replayed = 0;
        let mut skipped = 0;
        let mut failed = 0;
        let now = chrono::Utc::now().timestamp();
        let delay_secs = delay.as_secs() as i64;
        let max_age_secs = max_age.map(|d| d.as_secs() as i64);

        for task_data in tasks {
            let task = match serde_json::from_str::<SerializedTask>(&task_data) {
                Ok(task) => task,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };

            let age_secs = now - task.metadata.updated_at.timestamp();
            if age_secs < delay_secs {
                skipped += 1; // not old enough yet
                continue;
            }
            if let Some(max_age_secs) = max_age_secs {
                if age_secs > max_age_secs {
                    skipped += 1; // too old: expire rather than replay forever
                    continue;
                }
            }

            match self
                .move_dlq_entry_to_queue(conn, policy_name, &task_data, &task, max_retries)
                .await
            {
                Ok(ReplayOutcome::Replayed) => replayed += 1,
                Ok(ReplayOutcome::NotFound | ReplayOutcome::Exhausted) => skipped += 1,
                Err(e) => {
                    warn!(
                        "Failed to replay DLQ task {} under policy '{}': {}",
                        task.metadata.id, policy_name, e
                    );
                    failed += 1;
                }
            }
        }

        Ok(ReplayResult {
            replayed_count: replayed,
            skipped_count: skipped,
            failed_count: failed,
            policy_name: policy_name.to_string(),
            duration: start.elapsed(),
        })
    }

    /// Execute conditional replay. Honors `delay` the same way as
    /// [`Self::execute_time_based`], and enforces `max_retries`.
    #[allow(clippy::too_many_arguments)]
    async fn execute_conditional(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        condition: &ReplayCondition,
        max_retries: usize,
        delay: Duration,
        start: std::time::Instant,
    ) -> Result<ReplayResult> {
        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, 99)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let mut replayed = 0;
        let mut skipped = 0;
        let mut failed = 0;
        let now = chrono::Utc::now().timestamp();
        let delay_secs = delay.as_secs() as i64;

        for task_data in tasks {
            let task = match serde_json::from_str::<SerializedTask>(&task_data) {
                Ok(task) => task,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };

            if !condition.matches(&task, &self.predicates) {
                skipped += 1;
                continue;
            }

            let age_secs = now - task.metadata.updated_at.timestamp();
            if age_secs < delay_secs {
                skipped += 1; // matches, but not old enough yet
                continue;
            }

            match self
                .move_dlq_entry_to_queue(conn, policy_name, &task_data, &task, max_retries)
                .await
            {
                Ok(ReplayOutcome::Replayed) => replayed += 1,
                Ok(ReplayOutcome::NotFound | ReplayOutcome::Exhausted) => skipped += 1,
                Err(e) => {
                    warn!(
                        "Failed to replay DLQ task {} under policy '{}': {}",
                        task.metadata.id, policy_name, e
                    );
                    failed += 1;
                }
            }
        }

        Ok(ReplayResult {
            replayed_count: replayed,
            skipped_count: skipped,
            failed_count: failed,
            policy_name: policy_name.to_string(),
            duration: start.elapsed(),
        })
    }

    /// Execute rate-limited replay
    async fn execute_rate_limited(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        max_retries: usize,
        max_tasks_per_window: usize,
        window: Duration,
        start: std::time::Instant,
    ) -> Result<ReplayResult> {
        let batch_size = max_tasks_per_window.min(100);
        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, batch_size as isize - 1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let mut replayed = 0;
        let mut skipped = 0;
        let mut failed = 0;

        for task_data in tasks.iter().take(max_tasks_per_window) {
            let task = match serde_json::from_str::<SerializedTask>(task_data) {
                Ok(task) => task,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };

            match self
                .move_dlq_entry_to_queue(conn, policy_name, task_data, &task, max_retries)
                .await
            {
                Ok(ReplayOutcome::Replayed) => {
                    replayed += 1;
                    // Add small delay between tasks
                    if replayed < max_tasks_per_window {
                        let delay_per_task = window.as_millis() / max_tasks_per_window as u128;
                        sleep(Duration::from_millis(delay_per_task as u64)).await;
                    }
                }
                Ok(ReplayOutcome::NotFound | ReplayOutcome::Exhausted) => skipped += 1,
                Err(e) => {
                    warn!(
                        "Failed to replay DLQ task {} under policy '{}': {}",
                        task.metadata.id, policy_name, e
                    );
                    failed += 1;
                }
            }
        }

        Ok(ReplayResult {
            replayed_count: replayed,
            skipped_count: skipped,
            failed_count: failed,
            policy_name: policy_name.to_string(),
            duration: start.elapsed(),
        })
    }

    /// Execute smart adaptive replay.
    ///
    /// This analyses the recorded failure reason of each DLQ task to classify it
    /// (transient / resource / permanent / unknown) and then:
    ///
    /// * replays tasks **most likely to succeed first** (transient errors before
    ///   resource-pressure errors), since freeing up the head of the DLQ early
    ///   maximizes throughput;
    /// * when `adaptive` is enabled, **skips permanent failures** (validation
    ///   errors, data corruption, …) because replaying them will only fail
    ///   again — they are surfaced via the `skipped_count` so operators can
    ///   inspect them;
    /// * applies a per-kind **adaptive backoff** derived from `base_delay`
    ///   (transient errors retry quickly, resource errors wait longer) so the
    ///   downstream system is not immediately overwhelmed by the same load that
    ///   caused the failures.
    ///
    /// When `adaptive` is `false` it degrades to a fixed-order, fixed-delay
    /// replay using `base_delay`. `max_retries` is enforced the same way as
    /// every other execution path.
    #[allow(clippy::too_many_arguments)]
    async fn execute_smart(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        max_retries: usize,
        adaptive: bool,
        base_delay: Duration,
        start: std::time::Instant,
    ) -> Result<ReplayResult> {
        let task_data: Vec<String> = conn
            .lrange(&self.dlq_key, 0, 99)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let mut replayed = 0;
        let mut skipped = 0;
        let mut failed = 0;

        // Classify every task, keeping the parsed task (and its raw payload,
        // needed byte-for-byte to identify the DLQ entry) so we can re-enqueue
        // it. Unparseable payloads are counted as skipped.
        let mut classified: Vec<(String, SerializedTask, FailureKind)> =
            Vec::with_capacity(task_data.len());
        for raw in task_data {
            match serde_json::from_str::<SerializedTask>(&raw) {
                Ok(task) => {
                    let kind = FailureKind::classify(&task);
                    classified.push((raw, task, kind));
                }
                Err(_) => {
                    skipped += 1;
                }
            }
        }

        // Order by likelihood of success when adaptive, so transient failures
        // (most likely to recover) are replayed first.
        if adaptive {
            classified.sort_by_key(|(_, _, kind)| kind.replay_priority());
        }

        for (raw, task, kind) in classified {
            // Permanent failures will not benefit from replay; surface them
            // instead of churning the queue.
            if adaptive && kind == FailureKind::Permanent {
                debug!(
                    policy = policy_name,
                    "skipping permanent failure during smart replay"
                );
                skipped += 1;
                continue;
            }

            match self
                .move_dlq_entry_to_queue(conn, policy_name, &raw, &task, max_retries)
                .await
            {
                Ok(ReplayOutcome::Replayed) => {
                    replayed += 1;

                    // Adaptive backoff: stagger replays according to the failure
                    // kind so we do not immediately re-trigger the same overload.
                    let multiplier = if adaptive { kind.delay_multiplier() } else { 1 };
                    let wait = base_delay.saturating_mul(multiplier);
                    if !wait.is_zero() {
                        sleep(wait).await;
                    }
                }
                Ok(ReplayOutcome::NotFound | ReplayOutcome::Exhausted) => skipped += 1,
                Err(e) => {
                    warn!(
                        "Failed to replay DLQ task {} under policy '{}': {}",
                        task.metadata.id, policy_name, e
                    );
                    failed += 1;
                }
            }
        }

        Ok(ReplayResult {
            replayed_count: replayed,
            skipped_count: skipped,
            failed_count: failed,
            policy_name: policy_name.to_string(),
            duration: start.elapsed(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Redis connection URL used by the integration-style tests below. A
    /// local Redis is expected to be reachable in this crate's test
    /// environment (see the crate's other Redis-backed modules).
    const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

    #[test]
    fn test_replay_policy_time_based() {
        let policy = ReplayPolicy::time_based(Duration::from_secs(3600));
        assert!(policy.enabled);
        assert_eq!(policy.max_retries, 3);
    }

    #[test]
    fn test_replay_policy_conditional() {
        let condition = ReplayCondition::ErrorType("NetworkError".to_string());
        let policy = ReplayPolicy::conditional(condition, Duration::from_secs(300));
        assert!(policy.enabled);
        assert_eq!(policy.priority, 75);
    }

    #[test]
    fn test_replay_policy_rate_limited() {
        let policy = ReplayPolicy::rate_limited(10, Duration::from_secs(60));
        assert!(policy.enabled);
        assert_eq!(policy.priority, 25);
    }

    #[test]
    fn test_replay_policy_builder() {
        let policy = ReplayPolicy::time_based(Duration::from_secs(3600))
            .with_max_retries(5)
            .with_priority(90)
            .disabled();

        assert!(!policy.enabled);
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.priority, 90);
    }

    #[test]
    fn test_replay_condition_matches() {
        let task = SerializedTask::new("process_network_request".to_string(), vec![]);
        let predicates = HashMap::new();

        let condition = ReplayCondition::TaskName("network".to_string());
        assert!(condition.matches(&task, &predicates));

        let condition = ReplayCondition::TaskName("database".to_string());
        assert!(!condition.matches(&task, &predicates));
    }

    #[test]
    fn test_replay_condition_all() {
        let task = SerializedTask::new("process_network_request".to_string(), vec![]);
        let predicates = HashMap::new();

        let condition = ReplayCondition::All(vec![
            ReplayCondition::TaskName("network".to_string()),
            ReplayCondition::TaskName("process".to_string()),
        ]);
        assert!(condition.matches(&task, &predicates));

        let condition = ReplayCondition::All(vec![
            ReplayCondition::TaskName("network".to_string()),
            ReplayCondition::TaskName("database".to_string()),
        ]);
        assert!(!condition.matches(&task, &predicates));
    }

    #[test]
    fn test_replay_condition_any() {
        let task = SerializedTask::new("process_network_request".to_string(), vec![]);
        let predicates = HashMap::new();

        let condition = ReplayCondition::Any(vec![
            ReplayCondition::TaskName("database".to_string()),
            ReplayCondition::TaskName("network".to_string()),
        ]);
        assert!(condition.matches(&task, &predicates));

        let condition = ReplayCondition::Any(vec![
            ReplayCondition::TaskName("database".to_string()),
            ReplayCondition::TaskName("cache".to_string()),
        ]);
        assert!(!condition.matches(&task, &predicates));
    }

    /// Build a failed task whose state carries a specific error message.
    fn failed_task(name: &str, error: &str) -> SerializedTask {
        let mut task = SerializedTask::new(name.to_string(), vec![]);
        task.metadata.state = TaskState::Failed(error.to_string());
        task
    }

    #[test]
    fn test_failure_reason_extraction() {
        let task = failed_task("send_email", "Connection refused (os error 111)");
        assert_eq!(
            failure_reason(&task).as_deref(),
            Some("Connection refused (os error 111)")
        );

        let mut retry = SerializedTask::new("ingest".to_string(), vec![]);
        retry.metadata.state = TaskState::Retrying(5);
        assert!(failure_reason(&retry)
            .unwrap()
            .contains("retry attempts exhausted after 5"));

        // A pending task carries no failure reason.
        let pending = SerializedTask::new("noop".to_string(), vec![]);
        assert!(failure_reason(&pending).is_none());
    }

    #[test]
    fn test_failure_kind_classifies_from_error_message() {
        // Transient: error message wins even though the name is generic.
        let transient = failed_task("do_work", "Connection timed out");
        assert_eq!(FailureKind::classify(&transient), FailureKind::Transient);

        // Permanent: validation/corruption failures should not be replayed.
        let permanent = failed_task("do_work", "ValidationError: field 'email' is invalid");
        assert_eq!(FailureKind::classify(&permanent), FailureKind::Permanent);

        // Resource: pressure-related failures get a longer backoff.
        let resource = failed_task("do_work", "Rate limit exceeded, too many requests");
        assert_eq!(FailureKind::classify(&resource), FailureKind::Resource);

        // Unknown: nothing recognizable in name or error.
        let unknown = failed_task("do_work", "kaboom");
        assert_eq!(FailureKind::classify(&unknown), FailureKind::Unknown);
    }

    #[test]
    fn test_failure_kind_priority_and_backoff_ordering() {
        // Transient errors are replayed first and wait the least.
        assert!(FailureKind::Transient.replay_priority() < FailureKind::Resource.replay_priority());
        assert!(FailureKind::Resource.replay_priority() < FailureKind::Permanent.replay_priority());

        assert!(
            FailureKind::Transient.delay_multiplier() < FailureKind::Resource.delay_multiplier()
        );
        assert!(
            FailureKind::Resource.delay_multiplier() < FailureKind::Permanent.delay_multiplier()
        );
    }

    #[test]
    fn test_error_type_condition_matches_failure_message() {
        // The task name does not mention the network, but the recorded error
        // does — the condition must still match by inspecting the real reason.
        let task = failed_task("process_order", "NetworkError: host unreachable");
        let predicates = HashMap::new();
        let condition = ReplayCondition::ErrorType("network".to_string());
        assert!(condition.matches(&task, &predicates));

        let condition = ReplayCondition::ErrorType("validation".to_string());
        assert!(!condition.matches(&task, &predicates));
    }

    async fn test_conn() -> redis::aio::MultiplexedConnection {
        Client::open(TEST_REDIS_URL)
            .unwrap()
            .celers_multiplexed_connection()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_execute_time_based_honors_delay_and_max_age() {
        let queue_name = format!("test-replay-delay-{}", uuid::Uuid::new_v4());
        let mut scheduler =
            ReplayScheduler::with_mode(TEST_REDIS_URL, &queue_name, QueueMode::Fifo)
                .await
                .unwrap();
        let policy = ReplayPolicy {
            policy_type: ReplayPolicyType::TimeBased {
                delay: Duration::from_secs(3600),             // >= 1h old
                max_age: Some(Duration::from_secs(2 * 3600)), // <= 2h old
            },
            max_retries: 3,
            enabled: true,
            priority: 50,
        };
        scheduler.add_policy("hourly", policy).await.unwrap();

        let mut conn = test_conn().await;
        let dlq_key = format!("{}:dlq", queue_name);

        // Too young: only 10 minutes old, delay requires >= 1 hour.
        let mut young = SerializedTask::new("young_job".to_string(), vec![]);
        young.metadata.updated_at = chrono::Utc::now() - chrono::Duration::minutes(10);
        let young_data = serde_json::to_string(&young).unwrap();

        // Just right: 90 minutes old (within [1h, 2h]).
        let mut ready = SerializedTask::new("ready_job".to_string(), vec![]);
        ready.metadata.updated_at = chrono::Utc::now() - chrono::Duration::minutes(90);
        let ready_data = serde_json::to_string(&ready).unwrap();

        // Too old: 5 hours old, past the 2-hour max_age.
        let mut expired = SerializedTask::new("expired_job".to_string(), vec![]);
        expired.metadata.updated_at = chrono::Utc::now() - chrono::Duration::hours(5);
        let expired_data = serde_json::to_string(&expired).unwrap();

        for data in [&young_data, &ready_data, &expired_data] {
            let _: () = conn.rpush(&dlq_key, data).await.unwrap();
        }

        let results = scheduler.execute_once().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].replayed_count, 1,
            "only the 90-minute-old task falls within [delay, max_age]"
        );
        assert_eq!(results[0].skipped_count, 2);

        let remaining: Vec<String> = conn.lrange(&dlq_key, 0, -1).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining.contains(&young_data),
            "too-young task must stay in the DLQ"
        );
        assert!(
            remaining.contains(&expired_data),
            "expired task must stay in the DLQ"
        );

        // cleanup
        let _: () = conn.del(&dlq_key).await.unwrap();
        let _: () = conn.del(&queue_name).await.unwrap();
    }

    /// A replayed task must land at the *back* of the delivery order, not
    /// the front.
    ///
    /// The broker enqueues with `LPUSH` and consumes from the tail, so a
    /// replay that pushed with `RPUSH` would hand a task that has already
    /// failed at least once straight to the next worker, ahead of every task
    /// that was waiting first — a silent fairness inversion no type check
    /// catches.
    #[tokio::test]
    async fn test_fifo_replay_lands_behind_waiting_tasks() {
        let queue_name = format!("test-replay-order-{}", uuid::Uuid::new_v4());
        let mut scheduler = ReplayScheduler::new(TEST_REDIS_URL, &queue_name)
            .await
            .unwrap();
        scheduler
            .add_policy("immediate", ReplayPolicy::time_based(Duration::ZERO))
            .await
            .unwrap();

        let mut conn = test_conn().await;
        let dlq_key = format!("{}:dlq", queue_name);

        // Already waiting, enqueued the way the broker does it (head push).
        let waiting = SerializedTask::new("waiting".to_string(), vec![]);
        let waiting_data = serde_json::to_string(&waiting).unwrap();
        let _: () = conn.lpush(&queue_name, &waiting_data).await.unwrap();

        let failed = SerializedTask::new("failed".to_string(), vec![]);
        let failed_data = serde_json::to_string(&failed).unwrap();
        let _: () = conn.rpush(&dlq_key, &failed_data).await.unwrap();

        let results = scheduler.execute_once().await.unwrap();
        assert_eq!(results[0].replayed_count, 1);

        // Consumers pop the tail, so the tail is what gets served next.
        let next: Option<String> = conn.rpop(&queue_name, None).await.unwrap();
        assert_eq!(
            next.as_deref(),
            Some(waiting_data.as_str()),
            "the task that was already waiting must be served before the replayed one"
        );
        let then: Option<String> = conn.rpop(&queue_name, None).await.unwrap();
        assert_eq!(then.as_deref(), Some(failed_data.as_str()));

        let _: () = conn.del(&dlq_key).await.unwrap();
        let _: () = conn.del(&queue_name).await.unwrap();
    }

    /// The replay script must agree with the broker on which end of the list
    /// is the back of the queue.
    #[test]
    fn test_replay_script_pushes_to_the_head() {
        assert!(
            REPLAY_MOVE_SCRIPT.contains("'LPUSH'"),
            "replay must push to the head, as `RedisBroker::enqueue` does"
        );
        assert!(
            !REPLAY_MOVE_SCRIPT.contains("'RPUSH'"),
            "a tail push puts the replayed task at the front of the delivery order"
        );
    }

    #[tokio::test]
    async fn test_priority_mode_uses_zadd_not_rpush() {
        let queue_name = format!("test-replay-priority-{}", uuid::Uuid::new_v4());
        let mut scheduler =
            ReplayScheduler::with_mode(TEST_REDIS_URL, &queue_name, QueueMode::Priority)
                .await
                .unwrap();
        scheduler
            .add_policy(
                "immediate",
                ReplayPolicy::time_based(Duration::from_secs(0)),
            )
            .await
            .unwrap();

        let mut conn = test_conn().await;
        let dlq_key = format!("{}:dlq", queue_name);

        let mut task = SerializedTask::new("priority_job".to_string(), vec![]);
        task.metadata.priority = 7;
        task.metadata.updated_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        let data = serde_json::to_string(&task).unwrap();
        let _: () = conn.rpush(&dlq_key, &data).await.unwrap();

        // Before the fix this would RPUSH into what the broker treats as a
        // sorted set, failing with WRONGTYPE (silently counted as `failed`).
        let results = scheduler.execute_once().await.unwrap();
        assert_eq!(
            results[0].failed_count, 0,
            "must not WRONGTYPE-fail in priority mode"
        );
        assert_eq!(results[0].replayed_count, 1);

        // It must land in the queue as a ZSET member with the
        // negated-priority score matching `RedisBroker::enqueue`.
        let score: Option<f64> = conn.zscore(&queue_name, &data).await.unwrap();
        assert_eq!(score, Some(-7.0));

        let dlq_len: i64 = conn.llen(&dlq_key).await.unwrap();
        assert_eq!(dlq_len, 0);

        // cleanup
        let _: () = conn.del(&queue_name).await.unwrap();
    }

    #[tokio::test]
    async fn test_max_retries_enforced_across_replays() {
        let queue_name = format!("test-replay-maxretries-{}", uuid::Uuid::new_v4());
        let mut scheduler =
            ReplayScheduler::with_mode(TEST_REDIS_URL, &queue_name, QueueMode::Fifo)
                .await
                .unwrap();
        let policy = ReplayPolicy::time_based(Duration::from_secs(0)).with_max_retries(2);
        scheduler.add_policy("limited", policy).await.unwrap();

        let mut conn = test_conn().await;
        let dlq_key = format!("{}:dlq", queue_name);

        let mut task = SerializedTask::new("flaky_job".to_string(), vec![]);
        task.metadata.updated_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        let data = serde_json::to_string(&task).unwrap();

        // Simulate the same task id repeatedly failing, landing back in the
        // DLQ, and being reconsidered for replay.
        for attempt in 1..=3 {
            let _: () = conn.rpush(&dlq_key, &data).await.unwrap();
            let results = scheduler.execute_once().await.unwrap();
            let result = &results[0];

            if attempt <= 2 {
                assert_eq!(
                    result.replayed_count, 1,
                    "attempt {attempt} is within max_retries=2 and should replay"
                );
                // Pull it back out of the live queue and pretend it failed
                // again on the next iteration's RPUSH.
                let _: Option<String> = conn.lpop(&queue_name, None).await.unwrap();
            } else {
                assert_eq!(
                    result.replayed_count, 0,
                    "3rd attempt must be blocked: max_retries=2 already spent"
                );
                assert_eq!(result.skipped_count, 1);
            }
        }

        // cleanup
        let _: () = conn.del(&dlq_key).await.unwrap();
        let _: () = conn.del(&queue_name).await.unwrap();
        let _: () = conn
            .del(format!("{}:dlq:replay_attempts:limited", queue_name))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_add_policy_rejects_unregistered_custom_predicate() {
        let queue_name = format!("test-replay-custom-reject-{}", uuid::Uuid::new_v4());
        let mut scheduler = ReplayScheduler::new(TEST_REDIS_URL, &queue_name)
            .await
            .unwrap();

        let policy = ReplayPolicy::conditional(
            ReplayCondition::Custom("transient_network".to_string()),
            Duration::from_secs(0),
        );
        let result = scheduler.add_policy("custom_unregistered", policy).await;
        assert!(
            result.is_err(),
            "add_policy must reject a policy referencing an unregistered custom predicate"
        );

        // Composed inside All/Any must also be caught.
        let composed = ReplayPolicy::conditional(
            ReplayCondition::All(vec![
                ReplayCondition::TaskName("x".to_string()),
                ReplayCondition::Custom("still_unregistered".to_string()),
            ]),
            Duration::from_secs(0),
        );
        assert!(scheduler.add_policy("composed", composed).await.is_err());
    }

    #[tokio::test]
    async fn test_registered_custom_predicate_is_actually_evaluated() {
        let queue_name = format!("test-replay-custom-eval-{}", uuid::Uuid::new_v4());
        let mut scheduler = ReplayScheduler::new(TEST_REDIS_URL, &queue_name)
            .await
            .unwrap();

        scheduler.register_predicate("is_priority_job", |task: &SerializedTask| {
            task.metadata.name.starts_with("priority_")
        });

        let policy = ReplayPolicy::conditional(
            ReplayCondition::Custom("is_priority_job".to_string()),
            Duration::from_secs(3600),
        );
        scheduler
            .add_policy("custom_registered", policy)
            .await
            .unwrap();

        let mut conn = test_conn().await;
        let dlq_key = format!("{}:dlq", queue_name);

        // Matches the predicate AND old enough: should replay.
        let mut ready = SerializedTask::new("priority_job".to_string(), vec![]);
        ready.metadata.updated_at = chrono::Utc::now() - chrono::Duration::hours(2);
        let ready_data = serde_json::to_string(&ready).unwrap();

        // Matches the predicate but too young: delay not satisfied yet.
        let mut young = SerializedTask::new("priority_job_2".to_string(), vec![]);
        young.metadata.updated_at = chrono::Utc::now() - chrono::Duration::minutes(5);
        let young_data = serde_json::to_string(&young).unwrap();

        // Old enough but does not match the predicate at all.
        let mut non_matching = SerializedTask::new("regular_job".to_string(), vec![]);
        non_matching.metadata.updated_at = chrono::Utc::now() - chrono::Duration::hours(2);
        let non_matching_data = serde_json::to_string(&non_matching).unwrap();

        for data in [&ready_data, &young_data, &non_matching_data] {
            let _: () = conn.rpush(&dlq_key, data).await.unwrap();
        }

        let results = scheduler.execute_once().await.unwrap();
        assert_eq!(results[0].replayed_count, 1);
        assert_eq!(results[0].skipped_count, 2);

        let remaining: Vec<String> = conn.lrange(&dlq_key, 0, -1).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.contains(&young_data));
        assert!(remaining.contains(&non_matching_data));

        // cleanup
        let _: () = conn.del(&dlq_key).await.unwrap();
        let _: () = conn.del(&queue_name).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_returns_after_stop_called_concurrently() {
        // `run`/`stop` both take `&self` specifically so this pattern
        // compiles and works: share the scheduler via `Arc`, run it on a
        // spawned task, and stop it from the original handle while it is
        // executing. Before the fix both methods needed `&mut self`, which
        // made this impossible to even write.
        let queue_name = format!("test-replay-runstop-{}", uuid::Uuid::new_v4());
        let scheduler = Arc::new(
            ReplayScheduler::with_mode(TEST_REDIS_URL, &queue_name, QueueMode::Fifo)
                .await
                .unwrap()
                .with_poll_interval(Duration::from_millis(5)),
        );

        let runner = Arc::clone(&scheduler);
        let handle = tokio::spawn(async move { runner.run().await });

        // Let a few ticks fire before stopping (a short real sleep is used
        // only to yield to the spawned task — the pass/fail assertion below
        // is bounded by a generous timeout, not by this sleep's duration).
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(scheduler.is_running());
        scheduler.stop();
        assert!(!scheduler.is_running());

        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            result.is_ok(),
            "run() did not return within 2s of stop() being called"
        );
        assert!(result.unwrap().unwrap().is_ok());
    }
}
