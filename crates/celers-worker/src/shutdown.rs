//! Graceful shutdown signal handling
//!
//! Regression guard for the fix in idx 190 (`expect()` on production
//! paths, most impactfully in [`wait_for_signal`]): this module denies
//! `clippy::unwrap_used`/`clippy::expect_used` outside its test module, so
//! a future change cannot silently reintroduce a panic on this
//! shutdown-managing path.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use tokio::signal;
use tracing::{error, info};

/// Await a fallible signal-registration result, logging and then waiting
/// forever instead of panicking when registration fails.
///
/// Signal registration can genuinely fail (sandboxed / seccomp-restricted
/// environments, exhausted signalfd resources, being invoked off the main
/// thread on some platforms). [`wait_for_signal`] is the task that is
/// supposed to *manage* graceful shutdown, so it must not itself panic on
/// that failure -- doing so would take down the shutdown coordinator
/// instead of the process shutting down cleanly. Falling back to a future
/// that never resolves simply removes that particular signal source from
/// the `select!` below; the other source (or an external kill) still
/// works.
async fn wait_or_pending_forever<T>(result: std::io::Result<T>, what: &str) -> T {
    match result {
        Ok(value) => value,
        Err(e) => {
            error!("failed to install {what} handler: {e}");
            std::future::pending().await
        }
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT)
///
/// This function blocks until one of the following signals is received:
/// - SIGTERM (graceful termination)
/// - SIGINT (Ctrl+C)
///
/// If installing a signal handler itself fails, that failure is logged and
/// the corresponding source is disabled (it simply never fires) rather
/// than panicking this task; if *both* sources fail to install, this
/// function waits forever and the caller must have another way to trigger
/// shutdown (e.g. an admin endpoint or a forceful kill).
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
        wait_or_pending_forever(signal::ctrl_c().await, "Ctrl+C").await;
    };

    #[cfg(unix)]
    let terminate = async {
        let mut sig = wait_or_pending_forever(
            signal::unix::signal(signal::unix::SignalKind::terminate()),
            "SIGTERM",
        )
        .await;
        sig.recv().await;
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_for_signal_compiles() {
        // This test just ensures the function compiles correctly
        // We can't actually test signal handling in unit tests
        let _signal_task = tokio::spawn(async {
            // This would block forever without a signal
            // wait_for_signal().await;
        });
    }

    /// Regression test: `wait_for_signal()` used to `.expect()` on a
    /// failed signal registration, which would panic the shutdown-managing
    /// task instead of degrading gracefully. `wait_or_pending_forever` is
    /// the extracted fallback logic; a successful registration must
    /// resolve immediately with the wrapped value.
    #[tokio::test]
    async fn test_wait_or_pending_forever_resolves_on_ok() {
        let result: std::io::Result<u32> = Ok(42);
        let value = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            wait_or_pending_forever(result, "test"),
        )
        .await
        .expect("Ok(..) must resolve promptly");
        assert_eq!(value, 42);
    }

    /// Regression test: a failed registration must never panic -- it must
    /// log and then simply never resolve, so the *other* arm of the
    /// `select!` in `wait_for_signal` remains free to fire.
    #[tokio::test]
    async fn test_wait_or_pending_forever_never_resolves_on_err() {
        let result: std::io::Result<u32> =
            Err(std::io::Error::other("simulated registration failure"));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            wait_or_pending_forever(result, "test"),
        )
        .await;
        assert!(
            outcome.is_err(),
            "Err(..) must never resolve (and must not panic)"
        );
    }
}
