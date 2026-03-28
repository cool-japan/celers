/// Production worker configuration preset
///
/// Optimized for production workloads with:
/// - Concurrency matching CPU cores
/// - Standard polling interval
/// - Graceful shutdown enabled
pub fn production_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new()
        .concurrency(num_cpus::get())
        .poll_interval_ms(1000)
        .graceful_shutdown(true)
        .build()
}

/// High-throughput worker configuration preset
///
/// Optimized for maximum throughput:
/// - High concurrency (4x CPU cores)
/// - Fast polling interval
/// - Suitable for I/O-bound tasks
pub fn high_throughput_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    let concurrency = num_cpus::get() * 4;

    WorkerConfigBuilder::new()
        .concurrency(concurrency)
        .poll_interval_ms(100)
        .build()
}

/// Low-latency worker configuration preset
///
/// Optimized for low latency:
/// - Moderate concurrency
/// - Very fast polling for quick response
/// - Suitable for real-time tasks
pub fn low_latency_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new()
        .concurrency(num_cpus::get() * 2)
        .poll_interval_ms(50)
        .build()
}

/// Memory-constrained worker configuration preset
///
/// Optimized for low memory usage:
/// - Conservative concurrency
/// - Slower polling to reduce overhead
/// - Suitable for resource-limited environments
pub fn memory_constrained_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new()
        .concurrency(num_cpus::get())
        .poll_interval_ms(2000)
        .build()
}

/// CPU-bound worker configuration preset
///
/// Optimized for CPU-intensive tasks:
/// - Concurrency matches CPU cores (no oversubscription)
/// - Standard polling interval
/// - Suitable for computation-heavy tasks
pub fn cpu_bound_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new()
        .concurrency(num_cpus::get())
        .poll_interval_ms(500)
        .build()
}

/// I/O-bound worker configuration preset
///
/// Optimized for I/O-intensive tasks:
/// - High concurrency (4x CPU cores) for async I/O
/// - Fast polling for quick task pickup
/// - Suitable for network/database operations
pub fn io_bound_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    let concurrency = num_cpus::get() * 4;

    WorkerConfigBuilder::new()
        .concurrency(concurrency)
        .poll_interval_ms(200)
        .build()
}

/// Balanced worker configuration preset
///
/// Optimized for mixed workloads:
/// - Moderate concurrency (2x CPU cores)
/// - Balanced polling interval
/// - Good default for varied task types
pub fn balanced_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    let concurrency = num_cpus::get() * 2;

    WorkerConfigBuilder::new()
        .concurrency(concurrency)
        .poll_interval_ms(500)
        .build()
}

/// Development worker configuration preset
///
/// Optimized for development and testing:
/// - Low concurrency for easier debugging
/// - Slower polling to reduce noise
/// - Suitable for local development
pub fn development_config() -> std::result::Result<crate::WorkerConfig, String> {
    use celers_worker::WorkerConfigBuilder;

    WorkerConfigBuilder::new()
        .concurrency(2)
        .poll_interval_ms(1000)
        .build()
}
