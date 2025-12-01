//! Graceful shutdown signal handling

use tokio::signal;
use tracing::info;

/// Wait for a shutdown signal (SIGTERM or SIGINT)
///
/// This function blocks until one of the following signals is received:
/// - SIGTERM (graceful termination)
/// - SIGINT (Ctrl+C)
///
/// # Examples
///
/// ```no_run
/// use celers_worker::wait_for_signal;
///
/// #[tokio::main]
/// async fn main() {
///     // Start your worker in a separate task
///     let worker_task = tokio::spawn(async {
///         // Worker logic here
///     });
///
///     // Wait for shutdown signal
///     wait_for_signal().await;
///
///     // Perform cleanup
///     println!("Shutting down gracefully...");
/// }
/// ```
pub async fn wait_for_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C signal");
        },
        _ = terminate => {
            info!("Received SIGTERM signal");
        },
    }

    info!("Shutdown signal received, initiating graceful shutdown");
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_wait_for_signal_compiles() {
        // This test just ensures the function compiles correctly
        // We can't actually test signal handling in unit tests
        let _signal_task = tokio::spawn(async {
            // This would block forever without a signal
            // wait_for_signal().await;
        });
    }
}
