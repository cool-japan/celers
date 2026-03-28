use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Resource limits for a task
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory usage in bytes
    pub max_memory_bytes: Option<usize>,
    /// Maximum CPU time in seconds
    pub max_cpu_seconds: Option<u64>,
    /// Maximum wall-clock time in seconds
    pub max_wall_time_seconds: Option<u64>,
    /// Maximum number of file descriptors
    pub max_file_descriptors: Option<usize>,
}

impl ResourceLimits {
    /// Creates new resource limits with no restrictions
    pub fn unlimited() -> Self {
        Self {
            max_memory_bytes: None,
            max_cpu_seconds: None,
            max_wall_time_seconds: None,
            max_file_descriptors: None,
        }
    }

    /// Creates resource limits for memory-constrained tasks
    pub fn memory_constrained(max_memory_mb: usize) -> Self {
        Self {
            max_memory_bytes: Some(max_memory_mb * 1024 * 1024),
            max_cpu_seconds: None,
            max_wall_time_seconds: Some(300), // 5 minutes
            max_file_descriptors: Some(100),
        }
    }

    /// Creates resource limits for CPU-intensive tasks
    pub fn cpu_intensive(max_cpu_seconds: u64) -> Self {
        Self {
            max_memory_bytes: None,
            max_cpu_seconds: Some(max_cpu_seconds),
            max_wall_time_seconds: Some(max_cpu_seconds + 60),
            max_file_descriptors: Some(50),
        }
    }

    /// Creates resource limits for I/O-intensive tasks
    pub fn io_intensive(max_wall_time_seconds: u64) -> Self {
        Self {
            max_memory_bytes: Some(512 * 1024 * 1024), // 512 MB
            max_cpu_seconds: None,
            max_wall_time_seconds: Some(max_wall_time_seconds),
            max_file_descriptors: Some(1000),
        }
    }

    /// Sets maximum memory limit
    pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
        self.max_memory_bytes = Some(mb * 1024 * 1024);
        self
    }

    /// Sets maximum CPU time limit
    pub fn with_max_cpu_seconds(mut self, seconds: u64) -> Self {
        self.max_cpu_seconds = Some(seconds);
        self
    }

    /// Sets maximum wall-clock time limit
    pub fn with_max_wall_time_seconds(mut self, seconds: u64) -> Self {
        self.max_wall_time_seconds = Some(seconds);
        self
    }
}

/// Resource usage tracker
#[derive(Clone)]
pub struct ResourceTracker {
    start_time: Arc<Mutex<Instant>>,
    peak_memory_bytes: Arc<Mutex<usize>>,
    limits: ResourceLimits,
}

impl ResourceTracker {
    /// Creates a new resource tracker with specified limits
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            start_time: Arc::new(Mutex::new(Instant::now())),
            peak_memory_bytes: Arc::new(Mutex::new(0)),
            limits,
        }
    }

    /// Starts tracking resources
    pub fn start(&self) {
        *self.start_time.lock().expect("lock should not be poisoned") = Instant::now();
    }

    /// Records memory usage
    pub fn record_memory_usage(&self, bytes: usize) {
        let mut peak = self
            .peak_memory_bytes
            .lock()
            .expect("lock should not be poisoned");
        if bytes > *peak {
            *peak = bytes;
        }
    }

    /// Checks if resource limits are exceeded
    pub fn check_limits(&self) -> Result<(), String> {
        let elapsed = self
            .start_time
            .lock()
            .expect("lock should not be poisoned")
            .elapsed();

        // Check wall-clock time
        if let Some(max_wall_time) = self.limits.max_wall_time_seconds {
            if elapsed > Duration::from_secs(max_wall_time) {
                return Err(format!(
                    "Wall-clock time limit exceeded: {}s > {}s",
                    elapsed.as_secs(),
                    max_wall_time
                ));
            }
        }

        // Check memory
        if let Some(max_memory) = self.limits.max_memory_bytes {
            let peak_memory = *self
                .peak_memory_bytes
                .lock()
                .expect("lock should not be poisoned");
            if peak_memory > max_memory {
                return Err(format!(
                    "Memory limit exceeded: {} bytes > {} bytes",
                    peak_memory, max_memory
                ));
            }
        }

        Ok(())
    }

    /// Gets elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time
            .lock()
            .expect("lock should not be poisoned")
            .elapsed()
    }

    /// Gets peak memory usage
    pub fn peak_memory_bytes(&self) -> usize {
        *self
            .peak_memory_bytes
            .lock()
            .expect("lock should not be poisoned")
    }

    /// Gets resource limits
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }
}

/// Resource pool for managing shared resources
pub struct ResourcePool<T> {
    resources: Arc<Mutex<Vec<T>>>,
    max_size: usize,
}

impl<T> ResourcePool<T> {
    /// Creates a new resource pool
    pub fn new(max_size: usize) -> Self {
        Self {
            resources: Arc::new(Mutex::new(Vec::with_capacity(max_size))),
            max_size,
        }
    }

    /// Acquires a resource from the pool
    pub fn acquire(&self) -> Option<T> {
        self.resources
            .lock()
            .expect("lock should not be poisoned")
            .pop()
    }

    /// Returns a resource to the pool
    pub fn release(&self, resource: T) -> Result<(), String> {
        let mut resources = self.resources.lock().expect("lock should not be poisoned");
        if resources.len() >= self.max_size {
            return Err("Resource pool is full".to_string());
        }
        resources.push(resource);
        Ok(())
    }

    /// Gets the number of available resources
    pub fn available(&self) -> usize {
        self.resources
            .lock()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Gets the maximum pool size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Checks if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.resources
            .lock()
            .expect("lock should not be poisoned")
            .is_empty()
    }
}

impl<T> Clone for ResourcePool<T> {
    fn clone(&self) -> Self {
        Self {
            resources: Arc::clone(&self.resources),
            max_size: self.max_size,
        }
    }
}
