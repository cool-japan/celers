//! Weighted Fair Queuing (WFQ) types
//!
//! Provides WFQ configuration, task weights, and state management for
//! fair scheduling based on task importance.

use chrono::{DateTime, Utc};

// ============================================================================
// Weighted Fair Queuing (WFQ)
// ============================================================================

/// Weighted Fair Queuing configuration
///
/// WFQ provides fair scheduling based on task weights, ensuring that higher-weight
/// tasks get proportionally more execution time while preventing starvation of
/// lower-weight tasks.
///
/// # Algorithm
/// - Each task has a weight (default: 1.0, range: 0.1-10.0)
/// - Virtual time tracks fairness: virtual_finish_time = virtual_start_time + (execution_cost / weight)
/// - Tasks with lowest virtual finish time are scheduled first
/// - Prevents starvation while respecting priorities
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WFQConfig {
    /// Enable Weighted Fair Queuing
    pub enabled: bool,

    /// Default weight for tasks without explicit weight
    pub default_weight: f64,

    /// Minimum allowed weight
    pub min_weight: f64,

    /// Maximum allowed weight
    pub max_weight: f64,
}

impl Default for WFQConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_weight: 1.0,
            min_weight: 0.1,
            max_weight: 10.0,
        }
    }
}

/// Task weight for Weighted Fair Queuing
///
/// Higher weights mean higher importance and more execution time allocation.
/// Weight must be between 0.1 and 10.0.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct TaskWeight(f64);

impl TaskWeight {
    /// Create new task weight with validation
    pub fn new(weight: f64) -> Result<Self, String> {
        if !(0.1..=10.0).contains(&weight) {
            return Err(format!(
                "Weight must be between 0.1 and 10.0, got {}",
                weight
            ));
        }
        Ok(Self(weight))
    }

    /// Get the weight value
    pub fn value(&self) -> f64 {
        self.0
    }
}

impl Default for TaskWeight {
    fn default() -> Self {
        Self(1.0)
    }
}

/// Weighted Fair Queuing state for a task
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WFQState {
    /// Task weight (higher = more important)
    #[serde(default)]
    pub weight: TaskWeight,

    /// Virtual start time
    #[serde(default)]
    pub virtual_start_time: f64,

    /// Virtual finish time (determines scheduling order)
    #[serde(default)]
    pub virtual_finish_time: f64,

    /// Total execution time consumed (seconds)
    #[serde(default)]
    pub total_execution_time: f64,
}

impl Default for WFQState {
    fn default() -> Self {
        Self {
            weight: TaskWeight::default(),
            virtual_start_time: 0.0,
            virtual_finish_time: 0.0,
            total_execution_time: 0.0,
        }
    }
}

impl WFQState {
    /// Create new WFQ state with specified weight
    pub fn with_weight(weight: f64) -> Result<Self, String> {
        Ok(Self {
            weight: TaskWeight::new(weight)?,
            ..Default::default()
        })
    }

    /// Update virtual time after task execution
    ///
    /// # Arguments
    /// * `execution_duration_secs` - Actual execution time in seconds
    /// * `global_virtual_time` - Current global virtual time
    pub fn update_after_execution(
        &mut self,
        execution_duration_secs: f64,
        global_virtual_time: f64,
    ) {
        self.total_execution_time += execution_duration_secs;
        self.virtual_start_time = global_virtual_time.max(self.virtual_finish_time);

        // Virtual finish time = virtual start time + (cost / weight)
        // Cost is the execution duration
        self.virtual_finish_time =
            self.virtual_start_time + (execution_duration_secs / self.weight.value());
    }

    /// Get the virtual finish time for scheduling decisions
    pub fn finish_time(&self) -> f64 {
        self.virtual_finish_time
    }
}

/// Task information for WFQ scheduling
#[derive(Debug, Clone)]
pub struct WFQTaskInfo {
    /// Task name
    pub name: String,

    /// Virtual finish time
    pub virtual_finish_time: f64,

    /// Task weight
    pub weight: f64,

    /// Next scheduled run time
    pub next_run_time: DateTime<Utc>,
}
