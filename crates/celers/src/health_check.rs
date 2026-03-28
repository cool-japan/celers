use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Health status of a component
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Component is healthy and operational
    Healthy,
    /// Component is degraded but still operational
    Degraded,
    /// Component is unhealthy and not operational
    Unhealthy,
}

/// Health check result
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Status of the health check
    pub status: HealthStatus,
    /// Human-readable message
    pub message: String,
    /// Timestamp of the check
    pub timestamp: Instant,
    /// Additional metadata
    pub metadata: Vec<(String, String)>,
}

impl HealthCheckResult {
    /// Creates a new healthy result
    pub fn healthy(message: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Healthy,
            message: message.into(),
            timestamp: Instant::now(),
            metadata: Vec::new(),
        }
    }

    /// Creates a new degraded result
    pub fn degraded(message: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Degraded,
            message: message.into(),
            timestamp: Instant::now(),
            metadata: Vec::new(),
        }
    }

    /// Creates a new unhealthy result
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Unhealthy,
            message: message.into(),
            timestamp: Instant::now(),
            metadata: Vec::new(),
        }
    }

    /// Adds metadata to the health check result
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.push((key.into(), value.into()));
        self
    }
}

/// Worker health checker
///
/// Monitors worker health including heartbeat, task processing, and dependencies.
#[derive(Clone)]
pub struct WorkerHealthChecker {
    last_heartbeat: Arc<Mutex<Instant>>,
    last_task_processed: Arc<Mutex<Option<Instant>>>,
    heartbeat_timeout: Duration,
    task_timeout: Duration,
}

impl WorkerHealthChecker {
    /// Creates a new worker health checker
    ///
    /// # Arguments
    ///
    /// * `heartbeat_timeout` - Maximum time between heartbeats before unhealthy
    /// * `task_timeout` - Maximum time since last task processed before degraded
    pub fn new(heartbeat_timeout: Duration, task_timeout: Duration) -> Self {
        Self {
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            last_task_processed: Arc::new(Mutex::new(None)),
            heartbeat_timeout,
            task_timeout,
        }
    }

    /// Records a heartbeat
    pub fn heartbeat(&self) {
        *self
            .last_heartbeat
            .lock()
            .expect("lock should not be poisoned") = Instant::now();
    }

    /// Records task processing
    pub fn task_processed(&self) {
        *self
            .last_task_processed
            .lock()
            .expect("lock should not be poisoned") = Some(Instant::now());
    }

    /// Checks worker health status
    pub fn check_health(&self) -> HealthCheckResult {
        let now = Instant::now();
        let last_heartbeat = *self
            .last_heartbeat
            .lock()
            .expect("lock should not be poisoned");
        let last_task = *self
            .last_task_processed
            .lock()
            .expect("lock should not be poisoned");

        // Check heartbeat
        if now.duration_since(last_heartbeat) > self.heartbeat_timeout {
            return HealthCheckResult::unhealthy("Worker heartbeat timeout").with_metadata(
                "last_heartbeat_seconds_ago",
                format!("{}", now.duration_since(last_heartbeat).as_secs()),
            );
        }

        // Check task processing
        if let Some(last_task_time) = last_task {
            if now.duration_since(last_task_time) > self.task_timeout {
                return HealthCheckResult::degraded("No tasks processed recently").with_metadata(
                    "last_task_seconds_ago",
                    format!("{}", now.duration_since(last_task_time).as_secs()),
                );
            }
        }

        HealthCheckResult::healthy("Worker is operational").with_metadata(
            "uptime_seconds",
            format!("{}", now.duration_since(last_heartbeat).as_secs()),
        )
    }

    /// Checks if worker is ready to accept tasks
    pub fn is_ready(&self) -> bool {
        matches!(
            self.check_health().status,
            HealthStatus::Healthy | HealthStatus::Degraded
        )
    }

    /// Checks if worker is alive
    pub fn is_alive(&self) -> bool {
        let now = Instant::now();
        let last_heartbeat = *self
            .last_heartbeat
            .lock()
            .expect("lock should not be poisoned");
        now.duration_since(last_heartbeat) <= self.heartbeat_timeout
    }
}

impl Default for WorkerHealthChecker {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), Duration::from_secs(300))
    }
}

/// Dependency health checker
///
/// Monitors health of external dependencies (database, cache, etc.)
pub struct DependencyChecker {
    name: String,
    check_fn: Box<dyn Fn() -> HealthCheckResult + Send + Sync>,
}

impl DependencyChecker {
    /// Creates a new dependency checker
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the dependency
    /// * `check_fn` - Function to check dependency health
    pub fn new<F>(name: impl Into<String>, check_fn: F) -> Self
    where
        F: Fn() -> HealthCheckResult + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            check_fn: Box::new(check_fn),
        }
    }

    /// Checks the dependency health
    pub fn check(&self) -> HealthCheckResult {
        (self.check_fn)()
    }

    /// Gets the dependency name
    pub fn name(&self) -> &str {
        &self.name
    }
}
