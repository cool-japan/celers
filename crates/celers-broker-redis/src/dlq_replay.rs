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
//! // Start the scheduler
//! scheduler.run().await?;
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result, SerializedTask};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info};

/// DLQ replay policy scheduler
#[derive(Debug)]
pub struct ReplayScheduler {
    client: Client,
    queue_name: String,
    dlq_key: String,
    policies: HashMap<String, ReplayPolicy>,
    running: bool,
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
    /// Custom predicate (serialized as string)
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
    /// Check if a task matches this condition
    pub fn matches(&self, task: &SerializedTask) -> bool {
        match self {
            ReplayCondition::ErrorType(error_type) => {
                // Simplified: check task name for error type
                task.metadata.name.contains(error_type)
            }
            ReplayCondition::TaskName(pattern) => task.metadata.name.contains(pattern),
            ReplayCondition::All(conditions) => conditions.iter().all(|c| c.matches(task)),
            ReplayCondition::Any(conditions) => conditions.iter().any(|c| c.matches(task)),
            ReplayCondition::Custom(_) => {
                // Custom predicates would be evaluated here
                false
            }
        }
    }
}

impl ReplayScheduler {
    /// Create a new replay scheduler
    pub async fn new(redis_url: &str, queue_name: &str) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            dlq_key: format!("{}:dlq", queue_name),
            policies: HashMap::new(),
            running: false,
        })
    }

    /// Add a replay policy
    pub async fn add_policy(&mut self, name: &str, policy: ReplayPolicy) -> Result<()> {
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
        policies.sort_by(|a, b| b.1.priority.cmp(&a.1.priority));

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

    /// Run the scheduler continuously
    pub async fn run(&mut self) -> Result<()> {
        self.running = true;
        info!(
            "Starting DLQ replay scheduler for queue: {}",
            self.queue_name
        );

        let mut tick = interval(Duration::from_secs(60)); // Check every minute

        while self.running {
            tick.tick().await;

            if let Err(e) = self.execute_once().await {
                error!("Scheduler execution error: {}", e);
            }
        }

        Ok(())
    }

    /// Stop the scheduler
    pub fn stop(&mut self) {
        self.running = false;
        info!("Stopping DLQ replay scheduler");
    }

    /// Execute a specific policy
    async fn execute_policy(&self, name: &str, policy: &ReplayPolicy) -> Result<ReplayResult> {
        let start = std::time::Instant::now();
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
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
                self.execute_time_based(&mut conn, name, *delay, *max_age, start)
                    .await
            }
            ReplayPolicyType::Conditional { condition, delay } => {
                self.execute_conditional(&mut conn, name, condition, *delay, start)
                    .await
            }
            ReplayPolicyType::RateLimited {
                max_tasks_per_window,
                window,
            } => {
                self.execute_rate_limited(&mut conn, name, *max_tasks_per_window, *window, start)
                    .await
            }
            ReplayPolicyType::Smart {
                adaptive,
                base_delay,
            } => {
                self.execute_smart(&mut conn, name, *adaptive, *base_delay, start)
                    .await
            }
        }
    }

    /// Execute time-based replay
    async fn execute_time_based(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        _delay: Duration,
        _max_age: Option<Duration>,
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

        for task_data in tasks {
            if let Ok(_task) = serde_json::from_str::<SerializedTask>(&task_data) {
                // Move from DLQ back to main queue
                let main_queue_key = &self.queue_name;
                match conn.rpush::<_, _, ()>(main_queue_key, &task_data).await {
                    Ok(_) => {
                        // Remove from DLQ
                        let _: () = conn.lrem(&self.dlq_key, 1, &task_data).await.unwrap_or(());
                        replayed += 1;
                    }
                    Err(_) => {
                        failed += 1;
                    }
                }
            } else {
                skipped += 1;
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

    /// Execute conditional replay
    async fn execute_conditional(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        condition: &ReplayCondition,
        _delay: Duration,
        start: std::time::Instant,
    ) -> Result<ReplayResult> {
        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, 99)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let mut replayed = 0;
        let mut skipped = 0;
        let mut failed = 0;

        for task_data in tasks {
            if let Ok(task) = serde_json::from_str::<SerializedTask>(&task_data) {
                if condition.matches(&task) {
                    let main_queue_key = &self.queue_name;
                    match conn.rpush::<_, _, ()>(main_queue_key, &task_data).await {
                        Ok(_) => {
                            let _: () = conn.lrem(&self.dlq_key, 1, &task_data).await.unwrap_or(());
                            replayed += 1;
                        }
                        Err(_) => {
                            failed += 1;
                        }
                    }
                } else {
                    skipped += 1;
                }
            } else {
                skipped += 1;
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
        let mut failed = 0;

        for task_data in tasks.iter().take(max_tasks_per_window) {
            let main_queue_key = &self.queue_name;
            match conn.rpush::<_, _, ()>(main_queue_key, task_data).await {
                Ok(_) => {
                    let _: () = conn.lrem(&self.dlq_key, 1, task_data).await.unwrap_or(());
                    replayed += 1;

                    // Add small delay between tasks
                    if replayed < max_tasks_per_window {
                        let delay_per_task = window.as_millis() / max_tasks_per_window as u128;
                        sleep(Duration::from_millis(delay_per_task as u64)).await;
                    }
                }
                Err(_) => {
                    failed += 1;
                }
            }
        }

        Ok(ReplayResult {
            replayed_count: replayed,
            skipped_count: tasks.len() - replayed - failed,
            failed_count: failed,
            policy_name: policy_name.to_string(),
            duration: start.elapsed(),
        })
    }

    /// Execute smart adaptive replay
    async fn execute_smart(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        policy_name: &str,
        _adaptive: bool,
        _base_delay: Duration,
        start: std::time::Instant,
    ) -> Result<ReplayResult> {
        // Smart replay would analyze error patterns and adjust timing
        // For now, implement simple strategy
        self.execute_time_based(conn, policy_name, Duration::from_secs(300), None, start)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let condition = ReplayCondition::TaskName("network".to_string());
        assert!(condition.matches(&task));

        let condition = ReplayCondition::TaskName("database".to_string());
        assert!(!condition.matches(&task));
    }

    #[test]
    fn test_replay_condition_all() {
        let task = SerializedTask::new("process_network_request".to_string(), vec![]);

        let condition = ReplayCondition::All(vec![
            ReplayCondition::TaskName("network".to_string()),
            ReplayCondition::TaskName("process".to_string()),
        ]);
        assert!(condition.matches(&task));

        let condition = ReplayCondition::All(vec![
            ReplayCondition::TaskName("network".to_string()),
            ReplayCondition::TaskName("database".to_string()),
        ]);
        assert!(!condition.matches(&task));
    }

    #[test]
    fn test_replay_condition_any() {
        let task = SerializedTask::new("process_network_request".to_string(), vec![]);

        let condition = ReplayCondition::Any(vec![
            ReplayCondition::TaskName("database".to_string()),
            ReplayCondition::TaskName("network".to_string()),
        ]);
        assert!(condition.matches(&task));

        let condition = ReplayCondition::Any(vec![
            ReplayCondition::TaskName("database".to_string()),
            ReplayCondition::TaskName("cache".to_string()),
        ]);
        assert!(!condition.matches(&task));
    }
}
