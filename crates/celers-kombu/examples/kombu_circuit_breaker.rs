//! Circuit breaker and connection pooling example
//!
//! This example demonstrates resilience patterns:
//! - Circuit breaker (prevent cascading failures)
//! - Connection pooling (efficient resource management)
//! - Health checks (monitoring and diagnostics)
//!
//! Run with: cargo run --example circuit_breaker

use celers_kombu::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔌 Circuit Breaker and Connection Pooling Example\n");

    // =========================================================================
    // 1. Circuit Breaker Configuration
    // =========================================================================
    println!("⚡ Circuit Breaker");
    println!("   Prevent cascading failures with circuit breaker pattern\n");

    let cb_config = CircuitBreakerConfig::new()
        .with_failure_threshold(5)
        .with_success_threshold(2)
        .with_open_duration(Duration::from_secs(60))
        .with_failure_window(Duration::from_secs(10));

    println!("   Configuration:");
    println!(
        "     Failure threshold: {} failures",
        cb_config.failure_threshold
    );
    println!(
        "     Success threshold: {} successes",
        cb_config.success_threshold
    );
    println!("     Open duration: {:?}", cb_config.open_duration);
    println!("     Failure window: {:?}", cb_config.failure_window);

    // Circuit breaker states
    println!("\n   Circuit States:");
    println!("     Closed: Normal operation, requests pass through");
    println!("     Open: Too many failures, requests rejected immediately");
    println!("     HalfOpen: Testing if system recovered, limited requests");

    // Simulate circuit breaker state transitions
    println!("\n   State Transitions:");

    let states = vec![
        (
            CircuitState::Closed,
            "Normal operation",
            "All requests allowed",
        ),
        (
            CircuitState::Closed,
            "3 failures occurred",
            "Still accepting requests",
        ),
        (
            CircuitState::Open,
            "5 failures reached threshold",
            "Requests rejected",
        ),
        (
            CircuitState::HalfOpen,
            "After 60s timeout",
            "Testing recovery",
        ),
        (
            CircuitState::Closed,
            "2 successful tests",
            "Recovered, back to normal",
        ),
    ];

    for (state, event, action) in states {
        println!("\n     State: {:?}", state);
        println!("       Event: {}", event);
        println!("       Action: {}", action);
        let is_healthy = matches!(state, CircuitState::Closed | CircuitState::HalfOpen);
        let can_execute = matches!(state, CircuitState::Closed | CircuitState::HalfOpen);
        println!("       Healthy: {}", is_healthy);
        println!("       Can execute: {}", can_execute);
    }

    // Circuit breaker statistics
    println!("\n   Statistics:");
    let stats = CircuitBreakerStats {
        state: "Closed".to_string(),
        total_requests: 1000,
        successful_requests: 950,
        failed_requests: 50,
        rejected_requests: 20,
        times_opened: 0,
        consecutive_failures: 0,
        consecutive_successes: 0,
    };

    println!("     State: {}", stats.state);
    println!("     Total requests: {}", stats.total_requests);
    println!("     Successful: {}", stats.successful_requests);
    println!("     Failed: {}", stats.failed_requests);
    println!("     Rejected: {}", stats.rejected_requests);
    println!("     Success rate: {:.1}%", stats.success_rate());

    println!("\n   ✅ Use Case: Fault tolerance");
    println!("      - Prevent cascading failures");
    println!("      - Fast failure when service is down");
    println!("      - Automatic recovery testing");
    println!("      - Improve system resilience");
    println!("      - Reduce resource waste on failing operations");

    // =========================================================================
    // 2. Connection Pooling
    // =========================================================================
    println!("\n🏊 Connection Pooling");
    println!("   Efficient connection management and resource reuse\n");

    let pool_config = PoolConfig::new()
        .with_min_connections(2)
        .with_max_connections(20)
        .with_idle_timeout(Duration::from_secs(300))
        .with_acquire_timeout(Duration::from_secs(5))
        .with_max_lifetime(Duration::from_secs(1800));

    println!("   Configuration:");
    println!("     Min connections: {}", pool_config.min_connections);
    println!("     Max connections: {}", pool_config.max_connections);
    println!("     Idle timeout: {:?}", pool_config.idle_timeout);
    println!("     Acquire timeout: {:?}", pool_config.acquire_timeout);
    println!("     Max lifetime: {:?}", pool_config.max_lifetime);

    // Pool lifecycle
    println!("\n   Connection Lifecycle:");
    println!(
        "     1. Pool starts with {} min connections",
        pool_config.min_connections
    );
    println!(
        "     2. Creates new connections on demand (up to {})",
        pool_config.max_connections
    );
    println!("     3. Returns connections to pool after use");
    println!(
        "     4. Closes idle connections after {:?}",
        pool_config.idle_timeout
    );
    println!(
        "     5. Recycles connections older than {:?}",
        pool_config.max_lifetime
    );

    // Simulate pool statistics over time
    println!("\n   Pool Statistics Over Time:");

    let scenarios = vec![
        ("Application startup", 2, 0, 2, 0, 0, 0),
        ("Normal load", 8, 2, 10, 100, 0, 5),
        ("High load", 20, 0, 20, 500, 5, 10),
        ("Load decreased", 10, 5, 15, 800, 5, 15),
        ("Idle period", 2, 0, 2, 1000, 10, 20),
    ];

    for (phase, active, idle, created, requests, timeouts, closed) in scenarios {
        println!("\n     Phase: {}", phase);
        println!("       Active connections: {}", active);
        println!("       Idle connections: {}", idle);
        println!("       Total created: {}", created);
        println!("       Acquire requests: {}", requests);
        println!("       Timeouts: {}", timeouts);
        println!("       Connections closed: {}", closed);

        let utilization = (active as f32 / pool_config.max_connections as f32) * 100.0;
        println!("       Pool utilization: {:.1}%", utilization);

        if timeouts > 0 {
            println!("       ⚠️  Warning: Connection timeouts detected");
        }
    }

    // Pool health analysis
    println!("\n   Pool Health Indicators:");
    let pool_stats = PoolStats {
        connections_created: 50,
        connections_closed: 30,
        active_connections: 15,
        idle_connections: 5,
        acquire_requests: 1000,
        acquire_timeouts: 10,
    };

    let health_metrics = vec![
        (
            "Connection churn",
            pool_stats.connections_closed as f32 / pool_stats.connections_created as f32 * 100.0,
            "%",
        ),
        (
            "Timeout rate",
            pool_stats.acquire_timeouts as f32 / pool_stats.acquire_requests as f32 * 100.0,
            "%",
        ),
        (
            "Utilization",
            pool_stats.active_connections as f32
                / (pool_stats.active_connections + pool_stats.idle_connections) as f32
                * 100.0,
            "%",
        ),
    ];

    for (metric, value, unit) in health_metrics {
        println!("     {}: {:.2}{}", metric, value, unit);
        if metric == "Timeout rate" && value > 1.0 {
            println!("       ⚠️  Consider increasing max_connections");
        }
    }

    println!("\n   ✅ Use Case: Resource efficiency");
    println!("      - Reuse connections across requests");
    println!("      - Reduce connection overhead");
    println!("      - Limit concurrent connections");
    println!("      - Handle connection failures gracefully");
    println!("      - Improve throughput and latency");

    // =========================================================================
    // 3. Health Checks
    // =========================================================================
    println!("\n🏥 Health Checks");
    println!("   Monitoring and diagnostics\n");

    // Healthy system
    let healthy = HealthCheckResponse::healthy("redis", "redis://localhost:6379");
    println!("   Status: {:?}", healthy.status);
    println!("     Broker: {}", healthy.broker_type);
    println!("     Connection: {}", healthy.connection);
    println!("     Operational: {}", healthy.status.is_operational());
    println!("     Latency: {:?} ms", healthy.latency_ms);

    // Degraded system
    println!("\n   Degraded System:");
    let mut degraded = HealthCheckResponse::healthy("amqp", "amqp://localhost:5672");
    degraded.status = HealthStatus::Degraded;
    degraded
        .details
        .insert("reason".to_string(), "High memory usage".to_string());
    degraded.latency_ms = Some(500);

    println!("     Status: {:?}", degraded.status);
    println!("     Details: {:?}", degraded.details);
    println!("     Latency: {:?} ms", degraded.latency_ms);
    println!("     Operational: {}", degraded.status.is_operational());
    println!("     ⚠️  Warning: System degraded but still operational");

    // Unhealthy system
    println!("\n   Unhealthy System:");
    let unhealthy = HealthCheckResponse::unhealthy(
        "postgres",
        "postgres://localhost:5432",
        "Connection refused",
    );

    println!("     Status: {:?}", unhealthy.status);
    println!("     Details: {:?}", unhealthy.details);
    println!("     Operational: {}", unhealthy.status.is_operational());
    println!("     ❌ System down, circuit breaker should open");

    // Health check workflow
    println!("\n   Health Check Workflow:");
    println!("     1. Periodic health checks (every 30s)");
    println!("     2. Check connection state");
    println!("     3. Measure response latency");
    println!("     4. Test basic operations (ping)");
    println!("     5. Update metrics and logs");
    println!("     6. Alert on status changes");

    println!("\n   ✅ Use Case: Monitoring");
    println!("      - Real-time system status");
    println!("      - Automated health monitoring");
    println!("      - Integration with monitoring tools");
    println!("      - Proactive issue detection");
    println!("      - Operational visibility");

    // =========================================================================
    // 4. Integrated Resilience Example
    // =========================================================================
    println!("\n🔄 Integrated Resilience Example");
    println!("   Combining circuit breaker, pooling, and health checks\n");

    println!("   Scenario: Database broker under stress");
    println!("\n   Timeline:");

    let timeline = vec![
        (
            "00:00",
            "Normal",
            "✓ Circuit: Closed, Pool: 10/20, Health: Healthy",
        ),
        (
            "00:05",
            "Load increases",
            "✓ Circuit: Closed, Pool: 18/20, Health: Healthy",
        ),
        (
            "00:10",
            "DB slow",
            "⚠️  Circuit: Closed, Pool: 20/20, Health: Degraded, Latency: 800ms",
        ),
        (
            "00:12",
            "Timeouts start",
            "⚠️  Circuit: HalfOpen, Pool: 20/20, Health: Degraded, Timeouts: 15",
        ),
        (
            "00:13",
            "Circuit opens",
            "❌ Circuit: Open, Pool: 20/20, Health: Unhealthy, Fast fail",
        ),
        (
            "01:13",
            "Recovery attempt",
            "🔄 Circuit: HalfOpen, Pool: 5/20, Health: Testing",
        ),
        (
            "01:15",
            "Recovery success",
            "✓ Circuit: Closed, Pool: 10/20, Health: Healthy",
        ),
    ];

    for (time, phase, status) in timeline {
        println!("\n     [{}] {}", time, phase);
        println!("       {}", status);
    }

    println!("\n   Benefits:");
    println!("     ✓ Circuit breaker prevented cascading failures");
    println!("     ✓ Connection pool limited resource usage");
    println!("     ✓ Health checks detected issues early");
    println!("     ✓ Automatic recovery without intervention");
    println!("     ✓ Fast failure saved resources during outage");

    println!("\n   Key Metrics:");
    println!("     Total requests: 10,000");
    println!("     Successful: 9,500 (95%)");
    println!("     Failed during outage: 200 (2%)");
    println!("     Fast-failed by circuit breaker: 300 (3%)");
    println!("     Mean time to recovery: 2 minutes");
    println!("     Resources saved: ~70% during outage");

    println!("\n✨ Circuit breaker example completed successfully!");
    Ok(())
}
