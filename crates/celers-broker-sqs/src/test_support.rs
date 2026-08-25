// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test-only construction of `aws_sdk_sqs::Client` values that own no TLS stack.
//!
//! # Why this exists
//!
//! Several unit tests need a `Client` *value* — to pass to
//! [`VisibilityHeartbeat::spawn`](crate::VisibilityHeartbeat::spawn), or to put
//! in [`SqsBroker::client`](crate::SqsBroker) so that
//! [`is_connected`](crate::SqsBroker::is_connected) has something to report —
//! but none of them ever issues a request.
//!
//! Building that value the obvious way is surprisingly expensive and, worse,
//! flaky. `Client::from_conf` calls `HttpClient::validate_base_client_config`
//! on whichever HTTP client the base config selects, and
//! `aws-smithy-http-client` deliberately materialises its TCP connector there:
//!
//! ```text
//! // Initialize the TCP connector at this point so that native certs load
//! // at client initialization time instead of upon first request.
//! ```
//!
//! That path ends in `rustls_native_certs::load_native_certs()`. On macOS the
//! Security-framework walk costs roughly twelve seconds *per process*, and
//! under the concurrency of a full `cargo nextest run --workspace` several
//! processes hit it at once and some come back with **zero** certificates —
//! which trips `debug_assert!(valid > 0, "TrustStore configured to enable
//! native roots but no valid root certificates parsed!")` inside
//! `aws-smithy-http-client`'s rustls provider and fails the test. Passing
//! alone, failing in the full run, for a reason that has nothing to do with the
//! code under test.
//!
//! The comment quoted above also names the escape hatch: the default client's
//! `validate_base_client_config` runs only when it is *the selected* client
//! ("it won't run if this is not the selected HTTP client for the base config
//! ... it was overridden by a later plugin"). Setting an explicit
//! [`http_client`](aws_sdk_sqs::config::Builder::http_client) de-selects it, so
//! no trust store is ever loaded.
//!
//! # What this does not change
//!
//! Nothing about the assertions. The tests that use [`offline_sqs_client`]
//! check exactly what they checked before; only the transport underneath an
//! object they never transmit through is different. A request *would* fail —
//! deliberately, with [`ConnectorError::io`] — but none of them makes one:
//! [`VisibilityHeartbeat`](crate::VisibilityHeartbeat) sleeps a full interval
//! before its first `ChangeMessageVisibility`, and the tests cancel it before
//! then.
//!
//! Live SQS coverage lives in `tests/localstack.rs`, which is gated on a real
//! endpoint and builds its clients normally.

use aws_smithy_runtime_api::client::http::{
    http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::result::ConnectorError;

/// A connector that refuses every request instead of opening a socket.
///
/// Failing loudly is the point: if a test ever starts issuing a request, it
/// should say so rather than silently reach the network (or silently succeed
/// against a stubbed response that nobody wrote).
///
/// The error is deliberately [`ConnectorError::user`], not `io` or `timeout`:
/// those are transient kinds that the SDK's retry policy would attempt three
/// more times with backoff, turning one clear failure into a slow, noisy one.
/// Reaching this connector is a test-authoring mistake, and mistakes are not
/// transient.
#[derive(Debug)]
struct OfflineConnector;

impl HttpConnector for OfflineConnector {
    fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
        HttpConnectorFuture::ready(Err(ConnectorError::user(
            "celers-broker-sqs unit tests use an offline SQS client; \
             a test that needs a real request belongs in tests/localstack.rs"
                .into(),
        )
        .never_connected()))
    }
}

/// An `aws_sdk_sqs::Client` that loads no trust store and opens no sockets.
///
/// Region and behaviour version match what the production builders use, so the
/// client is identical to a real one everywhere except its transport.
///
/// Credentials are static placeholders — not to authenticate anything, but so
/// that a request which *is* made fails at the transport (where
/// [`OfflineConnector`] can say so) rather than earlier, in auth-scheme
/// resolution with `NoIdentityResolver`. Without them the stub would be
/// unreachable and the "no network in unit tests" guarantee untestable. They
/// are AWS's own documentation example key, never a real credential.
pub(crate) fn offline_sqs_client() -> aws_sdk_sqs::Client {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "celers-broker-sqs-offline-tests",
        ))
        .http_client(http_client_fn(|_settings, _components| {
            SharedHttpConnector::new(OfflineConnector)
        }))
        .build();

    aws_sdk_sqs::Client::from_conf(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard: drop the `http_client` override and this fires.
    ///
    /// It is a wall-clock assertion, which normally invites flakiness, but the
    /// two outcomes it separates are three orders of magnitude apart — ~0.03s
    /// with the stub, ~12s when the native trust store is walked. The threshold
    /// is deliberately parked far from *both* so neither a loaded machine nor a
    /// faster keychain can move the verdict; it is not a performance budget.
    #[test]
    fn building_an_offline_client_loads_no_trust_store() {
        let start = std::time::Instant::now();
        let _client = offline_sqs_client();
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "offline client took {:?} to build — something is loading native roots again \
             (has the `http_client` override been dropped?)",
            start.elapsed()
        );
    }

    /// The connector is wired in and refuses traffic rather than dialling out.
    #[tokio::test]
    async fn the_offline_connector_refuses_requests() {
        let client = offline_sqs_client();

        let error = client
            .change_message_visibility()
            .queue_url("https://sqs.us-east-1.amazonaws.com/123456789012/nonexistent")
            .receipt_handle("AQEB")
            .visibility_timeout(30)
            .send()
            .await
            .expect_err("offline connector must not complete a request");

        assert!(
            format!("{error:?}").contains("offline SQS client"),
            "expected the offline connector's error, got: {error:?}"
        );
    }
}
