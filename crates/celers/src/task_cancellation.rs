use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Cancellation token for task control
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<Mutex<bool>>,
    cancellation_reason: Arc<Mutex<Option<String>>>,
}

impl CancellationToken {
    /// Creates a new cancellation token
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(Mutex::new(false)),
            cancellation_reason: Arc::new(Mutex::new(None)),
        }
    }

    /// Cancels the token with an optional reason
    pub fn cancel(&self, reason: Option<String>) {
        *self.cancelled.lock().expect("lock should not be poisoned") = true;
        *self
            .cancellation_reason
            .lock()
            .expect("lock should not be poisoned") = reason;
    }

    /// Checks if the token is cancelled
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.lock().expect("lock should not be poisoned")
    }

    /// Gets the cancellation reason if available
    pub fn cancellation_reason(&self) -> Option<String> {
        self.cancellation_reason
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    /// Throws an error if the token is cancelled
    pub fn check_cancelled(&self) -> Result<(), String> {
        if self.is_cancelled() {
            Err(self
                .cancellation_reason()
                .unwrap_or_else(|| "Task was cancelled".to_string()))
        } else {
            Ok(())
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Timeout manager for task execution
pub struct TimeoutManager {
    timeout: Duration,
    start_time: Instant,
}

impl TimeoutManager {
    /// Creates a new timeout manager
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            start_time: Instant::now(),
        }
    }

    /// Checks if the timeout has been exceeded
    pub fn is_timed_out(&self) -> bool {
        self.start_time.elapsed() > self.timeout
    }

    /// Gets the remaining time
    pub fn remaining(&self) -> Duration {
        let elapsed = self.start_time.elapsed();
        if elapsed >= self.timeout {
            Duration::from_secs(0)
        } else {
            self.timeout - elapsed
        }
    }

    /// Gets the elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Checks timeout and returns error if exceeded
    pub fn check_timeout(&self) -> Result<(), String> {
        if self.is_timed_out() {
            Err(format!(
                "Task timeout exceeded: {}s",
                self.timeout.as_secs()
            ))
        } else {
            Ok(())
        }
    }
}

/// Task execution guard combining cancellation and timeout
pub struct ExecutionGuard {
    cancellation_token: CancellationToken,
    timeout_manager: Option<TimeoutManager>,
}

impl ExecutionGuard {
    /// Creates a new execution guard
    pub fn new(cancellation_token: CancellationToken, timeout: Option<Duration>) -> Self {
        Self {
            cancellation_token,
            timeout_manager: timeout.map(TimeoutManager::new),
        }
    }

    /// Checks if execution should continue
    pub fn should_continue(&self) -> Result<(), String> {
        self.cancellation_token.check_cancelled()?;
        if let Some(timeout_mgr) = &self.timeout_manager {
            timeout_mgr.check_timeout()?;
        }
        Ok(())
    }

    /// Gets the cancellation token
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Gets remaining timeout if configured
    pub fn remaining_timeout(&self) -> Option<Duration> {
        self.timeout_manager.as_ref().map(|t| t.remaining())
    }
}
