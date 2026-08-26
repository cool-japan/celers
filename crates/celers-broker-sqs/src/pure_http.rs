// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Pure-Rust HTTPS transport for the AWS SDK.
//!
//! # Why this exists
//!
//! The AWS SDK's stock transport is `aws-smithy-http-client`, and every TLS
//! backend it offers is FFI: `rustls-aws-lc` and `rustls-aws-lc-fips` build
//! `aws-lc-sys` (vendored C/C++/assembly, compiled with cmake + cc),
//! `rustls-ring` builds `ring` (C/assembly), and `s2n-tls` is a C TLS stack.
//! `deny.toml` bans all four, so `aws-config` / `aws-sdk-sqs` /
//! `aws-sdk-cloudwatch` are declared **without** `default-https-client` — which
//! removes `aws-smithy-http-client` from the dependency graph entirely and
//! leaves the SDK with no transport at all.
//!
//! This module supplies the replacement: an
//! [`HttpClient`](aws_smithy_runtime_api::client::http::HttpClient) backed by
//! [`oxihttp-client`](oxihttp_client) (hyper 1.x + `tokio-rustls` + OxiTLS'
//! Pure-Rust `rustls-rustcrypto` provider). Install it with
//! [`pure_http_client`](crate::pure_http::pure_http_client);
//! [`SqsBroker`](crate::SqsBroker) does that for you at
//! every `aws_config::defaults(..)` call.
//!
//! Removing `aws-smithy-http-client` also removes the ~12 s
//! `rustls_native_certs::load_native_certs()` walk it performed at client
//! construction time on macOS (and the `debug_assert!` it tripped when several
//! processes hit the Security framework at once). The trust store here is the
//! Mozilla `webpki-roots` bundle, which is what public AWS endpoints are signed
//! against.
//!
//! # Fidelity to the stock connector
//!
//! * **Timeouts.**
//!   [`HttpConnectorSettings::connect_timeout`](aws_smithy_runtime_api::client::http::HttpConnectorSettings::connect_timeout)
//!   is applied to the TCP connect, and
//!   [`HttpConnectorSettings::read_timeout`](aws_smithy_runtime_api::client::http::HttpConnectorSettings::read_timeout)
//!   bounds the wait
//!   for *response headers* — the same placement `aws-smithy-http-client` uses
//!   (`ConnectTimeout` around the connector, `HttpReadTimeout` around the
//!   response future). The body is read afterwards, unbounded, exactly as
//!   there.
//! * **Connection reuse.** One connector — and therefore one hyper connection
//!   pool — is built per distinct settings pair and cached, because
//!   `HttpClient::http_connector` is called per operation and a fresh pool each
//!   time would mean a fresh TLS handshake per request.
//! * **Error classification.** Transport faults map to
//!   [`ConnectorError::io`](aws_smithy_runtime_api::client::result::ConnectorError::io) /
//!   [`ConnectorError::timeout`](aws_smithy_runtime_api::client::result::ConnectorError::timeout)
//!   so the SDK's retry policy still sees them as transient; only unusable
//!   requests and configurations map to
//!   [`ConnectorError::user`](aws_smithy_runtime_api::client::result::ConnectorError::user).
//!   See `classify_transport_error`.
//!
//! # Known deviation
//!
//! The **request** body is drained into memory before it goes on the wire, and
//! the **response** body is buffered before it is handed back (bounded by
//! oxihttp's 64 MiB `DEFAULT_MAX_RESPONSE_BODY`). Neither SQS nor CloudWatch
//! has a streaming operation — SQS caps `ReceiveMessage` at 10 x 256 KiB and
//! signing requires a known-length payload anyway — so nothing in this crate
//! notices. A future service with streaming I/O would.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use http_body_util::BodyExt;
use oxihttp_client::HttpsClient;
use oxihttp_core::{Body as OxiBody, OxiHttpError};

/// ALPN protocols advertised on every handshake.
///
/// HTTP/1.1 only, deliberately. The AWS query and JSON protocols this crate
/// speaks (SQS, CloudWatch) are request/response with small payloads, where
/// HTTP/2 buys nothing, and announcing only what the client is built to speak
/// keeps protocol selection out of the failure surface.
const ALPN_PROTOCOLS: &[&str] = &["http/1.1"];

/// The process-wide Pure-Rust [`HttpClient`] for the AWS SDK.
///
/// One instance per process, so that every `aws_config` load — the SQS client,
/// the CloudWatch client, and the credential providers underneath them — shares
/// one connector cache and therefore one set of connection pools.
///
/// Pass it to [`aws_config::ConfigLoader::http_client`]:
///
/// ```no_run
/// # async fn example() {
/// use aws_config::BehaviorVersion;
///
/// let config = aws_config::defaults(BehaviorVersion::latest())
///     .http_client(celers_broker_sqs::pure_http::pure_http_client())
///     .load()
///     .await;
/// # let _ = config;
/// # }
/// ```
pub fn pure_http_client() -> SharedHttpClient {
    static CLIENT: OnceLock<SharedHttpClient> = OnceLock::new();

    CLIENT
        .get_or_init(|| SharedHttpClient::new(PureHttpClient::new()))
        .clone()
}

/// Identifies one connector configuration.
///
/// [`HttpConnectorSettings`] is `#[non_exhaustive]` and implements neither
/// `Hash` nor `Eq`, so its two observable fields are copied into a key type of
/// our own. If the SDK ever grows a third setting, this struct is where the
/// compiler will *not* tell you about it — hence the exhaustive construction in
/// [`ConnectorKey::from`], which at least keeps the mapping in one place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ConnectorKey {
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
}

impl From<&HttpConnectorSettings> for ConnectorKey {
    fn from(settings: &HttpConnectorSettings) -> Self {
        Self {
            connect_timeout: settings.connect_timeout(),
            read_timeout: settings.read_timeout(),
        }
    }
}

/// An [`HttpClient`] that hands out `oxihttp-client`-backed connectors.
#[derive(Debug, Default)]
pub struct PureHttpClient {
    connectors: Mutex<HashMap<ConnectorKey, SharedHttpConnector>>,
}

impl PureHttpClient {
    /// Create a client with an empty connector cache.
    ///
    /// Prefer [`pure_http_client`] unless you specifically want an isolated set
    /// of connection pools: two `PureHttpClient` values share nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached connector for one settings pair, building it on first use.
    ///
    /// Split out of [`HttpClient::http_connector`] because the caching contract
    /// is what the tests need to pin down, and constructing the
    /// `RuntimeComponents` that trait method demands would mean enabling
    /// `aws-smithy-runtime-api/test-util` for a value nothing reads.
    fn connector_for(&self, key: ConnectorKey) -> SharedHttpConnector {
        let mut cache = match self.connectors.lock() {
            Ok(guard) => guard,
            // The only work done under this lock is a `HashMap` lookup and
            // insert, so a panic elsewhere cannot have left the map torn.
            // Recovering it beats propagating an unrelated panic into the SDK's
            // request path.
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(existing) = cache.get(&key) {
            return existing.clone();
        }

        let connector = build_connector(key);
        cache.insert(key, connector.clone());
        connector
    }
}

impl HttpClient for PureHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.connector_for(ConnectorKey::from(settings))
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        // Surfaces as `http#oxihttp-client` in the SDK's User-Agent, so an
        // operator reading CloudTrail can tell this transport from the stock
        // one.
        Some(ConnectorMetadata::new("oxihttp-client", None))
    }
}

/// Build the connector for one settings pair.
///
/// Infallible by contract — [`HttpClient::http_connector`] cannot report an
/// error — so a client that fails to build (a broken trust store, an
/// unsupported TLS parameter) becomes an [`UnavailableConnector`] that reports
/// the real reason on every request instead of panicking at construction time.
fn build_connector(key: ConnectorKey) -> SharedHttpConnector {
    let mut builder = oxihttp_client::Client::builder()
        // Public AWS endpoints are signed by public CAs; the Mozilla bundle is
        // both sufficient and free of the OS trust-store walk.
        .with_webpki_roots()
        .with_alpn(ALPN_PROTOCOLS);

    if let Some(connect_timeout) = key.connect_timeout {
        builder = builder.connect_timeout(connect_timeout);
    }

    match builder.build_https() {
        Ok(client) => SharedHttpConnector::new(OxiHttpConnector {
            client,
            read_timeout: key.read_timeout,
        }),
        Err(error) => SharedHttpConnector::new(UnavailableConnector {
            reason: error.to_string(),
        }),
    }
}

/// The working connector: one `oxihttp-client` HTTPS client plus the read
/// timeout that belongs to its settings.
#[derive(Debug)]
struct OxiHttpConnector {
    client: HttpsClient,
    read_timeout: Option<Duration>,
}

impl HttpConnector for OxiHttpConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let client = self.client.clone();
        let read_timeout = self.read_timeout;

        HttpConnectorFuture::new(async move { send(client, read_timeout, request).await })
    }
}

/// A connector that reports why no transport could be built.
///
/// Reaching it means [`oxihttp_client::ClientBuilder::build_https`] failed, so
/// every request through this settings pair will fail the same way; the error
/// is deliberately *not* retryable.
#[derive(Debug)]
struct UnavailableConnector {
    reason: String,
}

impl HttpConnector for UnavailableConnector {
    fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
        HttpConnectorFuture::ready(Err(ConnectorError::user(
            format!(
                "celers-broker-sqs could not build its Pure-Rust HTTPS client: {}",
                self.reason
            )
            .into(),
        )
        .never_connected()))
    }
}

/// The read timeout expired before response headers arrived.
///
/// A dedicated type rather than a string so that the duration survives into the
/// SDK's error chain, where `DisplayErrorContext` will print it.
#[derive(Debug)]
struct ResponseHeadersTimeout(Duration);

impl std::fmt::Display for ResponseHeadersTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no response headers within the configured read timeout of {:?}",
            self.0
        )
    }
}

impl std::error::Error for ResponseHeadersTimeout {}

/// Perform one request/response round trip.
async fn send(
    client: HttpsClient,
    read_timeout: Option<Duration>,
    request: HttpRequest,
) -> Result<HttpResponse, ConnectorError> {
    let request = request
        .try_into_http1x()
        .map_err(|error| ConnectorError::user(error.into()))?;
    let (parts, body) = request.into_parts();

    // See the module docs: the request body is buffered. Every AWS operation
    // this crate issues already carries an in-memory, length-known payload
    // (SigV4 hashes it), so this collect never awaits real I/O.
    let body = body
        .collect()
        .await
        .map_err(|error| ConnectorError::other(error, None))?
        .to_bytes();

    let outgoing = http::Request::from_parts(parts, OxiBody::full(body));

    // `execute_body` is a single round trip: it bypasses oxihttp's redirect
    // policy and retry policy entirely, which is required — the SDK owns
    // retries, and a redirect would invalidate the SigV4 signature.
    let request_future = client.execute_body(outgoing);

    let response = match read_timeout {
        Some(limit) => match tokio::time::timeout(limit, request_future).await {
            Ok(result) => result,
            Err(_elapsed) => {
                return Err(ConnectorError::timeout(Box::new(ResponseHeadersTimeout(
                    limit,
                ))))
            }
        },
        None => request_future.await,
    }
    .map_err(classify_transport_error)?;

    let status = response.status();
    let version = response.version();
    let headers = response.headers().clone();

    let body = response
        .body_bytes()
        .await
        .map_err(classify_transport_error)?;

    let mut builder = http::Response::builder().status(status).version(version);
    if let Some(target) = builder.headers_mut() {
        *target = headers;
    }

    let response = builder
        .body(SdkBody::from(body))
        .map_err(|error| ConnectorError::other(Box::new(error), None))?;

    HttpResponse::try_from(response).map_err(|error| ConnectorError::other(Box::new(error), None))
}

/// Map an `oxihttp-client` failure onto the SDK's error taxonomy.
///
/// The taxonomy is not cosmetic: [`ConnectorError::io`] and
/// [`ConnectorError::timeout`] are *transient* kinds that the SDK's retry
/// policy will attempt again, while [`ConnectorError::user`] is terminal.
/// Misfiling a connection reset as `user` silently disables retries for every
/// transient network fault, so anything that could plausibly be a network
/// condition is classified as I/O.
///
/// `oxihttp-client` flattens hyper's typed error into
/// [`OxiHttpError::Hyper`](OxiHttpError::Hyper) (a `String`), so connect-refused,
/// TLS-handshake failure and reset-by-peer all arrive in that one variant.
/// `aws-smithy-http-client` reaches the same verdict by a different route: it
/// maps `hyper_util`'s connect errors and anything with an `io::Error` source
/// to [`ConnectorError::io`].
fn classify_transport_error(error: OxiHttpError) -> ConnectorError {
    match error {
        OxiHttpError::Hyper(_)
        | OxiHttpError::Io(_)
        | OxiHttpError::Dns(_)
        | OxiHttpError::ConnectionPool(_)
        | OxiHttpError::Body(_) => ConnectorError::io(Box::new(error)),
        OxiHttpError::Timeout(_) => ConnectorError::timeout(Box::new(error)),
        // A malformed URI or header, or a TLS configuration that cannot be
        // built: retrying an identical request cannot change the outcome.
        OxiHttpError::InvalidUri(_)
        | OxiHttpError::Http(_)
        | OxiHttpError::InvalidHeader(_)
        | OxiHttpError::Tls(_) => ConnectorError::user(Box::new(error)),
        // Redirect, JSON, form-encoding and the server-side variants belong to
        // oxihttp's higher-level helpers, none of which `execute_body` goes
        // through. Retrying is not obviously right, so leave the kind unset and
        // let the SDK treat it as non-retryable.
        other => ConnectorError::other(Box::new(other), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(connect: Option<u64>, read: Option<u64>) -> HttpConnectorSettings {
        let mut builder = HttpConnectorSettings::builder();
        builder.set_connect_timeout(connect.map(Duration::from_secs));
        builder.set_read_timeout(read.map(Duration::from_secs));
        builder.build()
    }

    fn cache_len(client: &PureHttpClient) -> usize {
        client
            .connectors
            .lock()
            .map(|guard| guard.len())
            .unwrap_or_else(|poisoned| poisoned.into_inner().len())
    }

    /// The whole point of implementing `HttpClient` rather than passing a bare
    /// closure to `http_client_fn`: asking twice for the same settings must
    /// return the same connector, or every operation opens a new hyper pool and
    /// pays a fresh TLS handshake.
    #[test]
    fn connectors_are_cached_per_settings() {
        let client = PureHttpClient::new();
        let key = ConnectorKey::from(&settings(Some(3), Some(30)));

        let first = client.connector_for(key);
        let second = client.connector_for(key);

        assert_eq!(
            format!("{first:?}"),
            format!("{second:?}"),
            "identical settings must reuse one connector"
        );
        assert_eq!(
            cache_len(&client),
            1,
            "identical settings must not add a second entry"
        );
    }

    /// Different timeouts are genuinely different connectors — the connect
    /// timeout is baked into the TCP connector and the read timeout into the
    /// wrapper, so they cannot share one.
    #[test]
    fn different_settings_get_different_connectors() {
        let client = PureHttpClient::new();

        let _fast = client.connector_for(ConnectorKey::from(&settings(Some(1), Some(5))));
        let _slow = client.connector_for(ConnectorKey::from(&settings(Some(30), Some(90))));
        let _no_timeouts = client.connector_for(ConnectorKey::from(&settings(None, None)));

        assert_eq!(cache_len(&client), 3);
    }

    /// A built connector must be a real one, not the "could not build"
    /// fallback: if the webpki trust store or the OxiTLS provider ever stopped
    /// working, every SQS request would fail with a terminal error and this is
    /// the only place that would notice before production.
    #[test]
    fn the_default_connector_builds_successfully() {
        let connector = build_connector(ConnectorKey::from(&settings(Some(3), Some(30))));
        let rendered = format!("{connector:?}");
        assert!(
            rendered.contains("OxiHttpConnector"),
            "expected a working oxihttp connector, got {rendered}"
        );
    }

    /// `ConnectorKey` must round-trip both settings exactly; a key that dropped
    /// one would silently hand a 90-second-read connector to a 1-second-read
    /// operation.
    #[test]
    fn connector_key_carries_both_timeouts() {
        let key = ConnectorKey::from(&settings(Some(7), Some(11)));
        assert_eq!(key.connect_timeout, Some(Duration::from_secs(7)));
        assert_eq!(key.read_timeout, Some(Duration::from_secs(11)));

        let unset = ConnectorKey::from(&settings(None, None));
        assert_eq!(unset.connect_timeout, None);
        assert_eq!(unset.read_timeout, None);
    }

    /// The process-wide client is a singleton: two calls must not create two
    /// independent connector caches.
    #[test]
    fn pure_http_client_is_one_shared_instance() {
        let first = pure_http_client();
        let second = pure_http_client();
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
    }

    /// Transport faults must stay retryable. This is the assertion that keeps
    /// a future refactor from quietly turning connection resets into terminal
    /// errors.
    #[test]
    fn transport_faults_are_classified_retryable() {
        for error in [
            OxiHttpError::Hyper("connection refused".to_string()),
            OxiHttpError::Io(std::sync::Arc::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset by peer",
            ))),
            OxiHttpError::Dns("no such host".to_string()),
            OxiHttpError::ConnectionPool("exhausted".to_string()),
            OxiHttpError::Body("truncated".to_string()),
        ] {
            let classified = classify_transport_error(error.clone());
            assert!(
                classified.is_io(),
                "{error} must be an I/O error so the SDK retries it, got {classified:?}"
            );
        }
    }

    /// A timeout must arrive as a timeout, not as a generic I/O error: the SDK
    /// distinguishes the two when deciding whether to widen its backoff.
    #[test]
    fn timeouts_are_classified_as_timeouts() {
        let classified = classify_transport_error(OxiHttpError::Timeout("elapsed".to_string()));
        assert!(classified.is_timeout(), "got {classified:?}");
    }

    /// Requests that cannot succeed however often they are repeated must be
    /// terminal, so a bad endpoint fails once instead of three times slowly.
    #[test]
    fn unusable_requests_are_classified_terminal() {
        let classified =
            classify_transport_error(OxiHttpError::Tls("no trust anchors".to_string()));
        assert!(classified.is_user(), "got {classified:?}");
        assert!(!classified.is_io());
    }

    /// The fallback connector reports the build failure instead of hanging or
    /// panicking, and says it never reached the network.
    #[tokio::test]
    async fn unavailable_connector_reports_the_build_failure() {
        let connector = UnavailableConnector {
            reason: "trust store empty".to_string(),
        };

        let request = HttpRequest::try_from(
            http::Request::builder()
                .uri("https://sqs.us-east-1.amazonaws.com/")
                .body(SdkBody::empty())
                .expect("request"),
        )
        .expect("smithy request");

        let error = connector
            .call(request)
            .await
            .expect_err("an unavailable connector must not succeed");

        let rendered = format!("{error:?}");
        assert!(
            rendered.contains("trust store empty"),
            "the real reason must survive: {rendered}"
        );
    }

    /// The timeout error keeps the configured duration, so an operator reading
    /// the log learns which budget was exceeded.
    #[test]
    fn response_headers_timeout_names_the_budget() {
        let rendered = ResponseHeadersTimeout(Duration::from_millis(2500)).to_string();
        assert!(rendered.contains("2.5s"), "got {rendered}");
    }

    /// The connector announces itself so the SDK's User-Agent distinguishes it
    /// from the stock `aws-smithy-http-client`.
    #[test]
    fn connector_metadata_names_oxihttp() {
        let metadata = PureHttpClient::new()
            .connector_metadata()
            .expect("metadata must be reported");
        assert_eq!(metadata.to_string(), "http#oxihttp-client");
    }

    // ---------------------------------------------------------------------
    // End-to-end round trips against a local HTTP/1.1 socket.
    //
    // These are the tests that actually exercise the mapping this module
    // exists for: `HttpRequest` -> wire bytes -> wire bytes -> `HttpResponse`.
    // They speak plain HTTP because the transformation under test is identical
    // on both schemes (`build_https` produces a client that handles `http://`
    // and `https://` through the same code path) and because a TLS listener
    // would need a certificate that no unit test should be minting. TLS itself
    // is oxitls' and rustls' responsibility, covered by their own suites; what
    // is unproven without these tests is *this* crate's request/response
    // translation.
    // ---------------------------------------------------------------------

    use std::sync::Arc as StdArc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// What a stub server should do once a request arrives.
    enum ServerBehaviour {
        /// Send this raw response and close.
        Respond(&'static str),
        /// Read the request and never answer, so the read timeout can fire.
        Stall,
    }

    /// Bind a one-shot HTTP/1.1 server on an ephemeral port.
    ///
    /// Returns the bound address and a handle that yields the raw request bytes
    /// the client actually put on the wire.
    async fn stub_server(
        behaviour: ServerBehaviour,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<String>,
        StdArc<tokio::sync::Notify>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let done = StdArc::new(tokio::sync::Notify::new());
        let shutdown = StdArc::clone(&done);

        let handle = tokio::spawn(async move {
            let (mut socket, _peer) = listener.accept().await.expect("accept");

            // Read headers, then exactly as many body bytes as Content-Length
            // announces. Enough HTTP/1.1 to observe what the client sent; not a
            // general-purpose server.
            let mut raw = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = socket.read(&mut buffer).await.expect("read");
                if read == 0 {
                    break;
                }
                raw.extend_from_slice(&buffer[..read]);

                let text = String::from_utf8_lossy(&raw).to_string();
                let Some(header_end) = text.find("\r\n\r\n") else {
                    continue;
                };
                let body_len = text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if raw.len() >= header_end + 4 + body_len {
                    break;
                }
            }

            match behaviour {
                ServerBehaviour::Respond(response) => {
                    socket.write_all(response.as_bytes()).await.expect("write");
                    socket.flush().await.expect("flush");
                }
                ServerBehaviour::Stall => {
                    // Hold the connection open until the test says it is done,
                    // so the client sees silence rather than a closed socket.
                    shutdown.notified().await;
                }
            }

            String::from_utf8_lossy(&raw).to_string()
        });

        (address, handle, done)
    }

    fn smithy_request(uri: &str, body: &'static [u8]) -> HttpRequest {
        HttpRequest::try_from(
            http::Request::builder()
                .method("POST")
                .uri(uri)
                .header("x-amz-target", "AmazonSQS.ReceiveMessage")
                .header("content-type", "application/x-amz-json-1.0")
                .body(SdkBody::from(body))
                .expect("http request"),
        )
        .expect("smithy request")
    }

    /// A full round trip: method, URI path, headers and body must reach the
    /// wire, and status, response headers and response body must come back on
    /// the `HttpResponse` the SDK will parse.
    #[tokio::test]
    async fn round_trips_a_request_and_response() {
        let (address, server, _done) = stub_server(ServerBehaviour::Respond(
            "HTTP/1.1 200 OK\r\n\
             content-type: application/x-amz-json-1.0\r\n\
             x-amzn-requestid: 11111111-2222-3333-4444-555555555555\r\n\
             content-length: 17\r\n\
             \r\n\
             {\"Messages\":null}",
        ))
        .await;

        let connector = OxiHttpConnector {
            client: oxihttp_client::Client::builder()
                .with_webpki_roots()
                .build_https()
                .expect("https client"),
            read_timeout: Some(Duration::from_secs(10)),
        };

        let response = connector
            .call(smithy_request(
                &format!("http://{address}/queue/celers"),
                b"{\"QueueUrl\":\"x\"}",
            ))
            .await
            .expect("round trip");

        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(
            response.headers().get("x-amzn-requestid"),
            Some("11111111-2222-3333-4444-555555555555"),
            "response headers must survive the mapping"
        );
        assert_eq!(
            response.body().bytes(),
            Some(&b"{\"Messages\":null}"[..]),
            "the response body must reach the SDK intact"
        );

        let sent = server.await.expect("server task");
        assert!(
            sent.starts_with("POST /queue/celers HTTP/1.1\r\n"),
            "{sent}"
        );
        assert!(
            sent.to_ascii_lowercase()
                .contains("x-amz-target: amazonsqs.receivemessage"),
            "signed headers must reach the wire unchanged: {sent}"
        );
        assert!(
            sent.to_ascii_lowercase()
                .contains(&format!("host: {address}")),
            "SigV4 signs the `host` header, so the transport must emit the one the \
             signed URI implies: {sent}"
        );
        assert!(
            sent.ends_with("{\"QueueUrl\":\"x\"}"),
            "the request body must reach the wire: {sent}"
        );
        assert!(
            sent.to_ascii_lowercase().contains("content-length: 16\r\n"),
            "a length-known body must be framed with Content-Length, not chunked \
             (SigV4 signs the payload, and SQS rejects chunked query bodies): {sent}"
        );
    }

    /// A server that never answers must produce a *timeout*, not a hang and not
    /// a generic error — the SDK widens its backoff on timeouts specifically.
    #[tokio::test]
    async fn a_silent_server_trips_the_read_timeout() {
        let (address, server, done) = stub_server(ServerBehaviour::Stall).await;

        let connector = OxiHttpConnector {
            client: oxihttp_client::Client::builder()
                .with_webpki_roots()
                .build_https()
                .expect("https client"),
            read_timeout: Some(Duration::from_millis(150)),
        };

        let error = connector
            .call(smithy_request(
                &format!("http://{address}/queue/celers"),
                b"{}",
            ))
            .await
            .expect_err("a silent server must not produce a response");

        assert!(error.is_timeout(), "expected a timeout, got {error:?}");
        let source = std::error::Error::source(&error)
            .map(ToString::to_string)
            .unwrap_or_default();
        assert!(
            source.contains("read timeout") && source.contains("150ms"),
            "the error must name the budget it exceeded, got {source:?} from {error:?}"
        );

        done.notify_one();
        let _ = server.await;
    }

    /// A refused connection must be classified as I/O so the SDK retries it.
    /// Getting this wrong turns every restart of an endpoint into a hard
    /// failure instead of a retried one.
    #[tokio::test]
    async fn a_refused_connection_is_retryable() {
        // Bind, read the port, drop the listener: the port is now almost
        // certainly closed, and nothing else in the test can have taken it.
        let address = {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            listener.local_addr().expect("local addr")
        };

        let connector = OxiHttpConnector {
            client: oxihttp_client::Client::builder()
                .with_webpki_roots()
                .build_https()
                .expect("https client"),
            read_timeout: Some(Duration::from_secs(5)),
        };

        let error = connector
            .call(smithy_request(&format!("http://{address}/"), b"{}"))
            .await
            .expect_err("a closed port must not produce a response");

        assert!(
            error.is_io(),
            "a refused connection must stay retryable, got {error:?}"
        );
    }

    /// Non-2xx responses are the SDK's business, not the connector's: a 500 has
    /// to come back as a `Response` so the SDK can read the error body and
    /// decide whether to retry, never as a `ConnectorError`.
    #[tokio::test]
    async fn error_statuses_are_returned_not_raised() {
        let (address, server, _done) = stub_server(ServerBehaviour::Respond(
            "HTTP/1.1 500 Internal Server Error\r\n\
             content-length: 30\r\n\
             \r\n\
             {\"__type\":\"InternalFailure\"}\r\n",
        ))
        .await;

        let connector = OxiHttpConnector {
            client: oxihttp_client::Client::builder()
                .with_webpki_roots()
                .build_https()
                .expect("https client"),
            read_timeout: Some(Duration::from_secs(10)),
        };

        let response = connector
            .call(smithy_request(&format!("http://{address}/"), b"{}"))
            .await
            .expect("a 500 is a response, not a connector error");

        assert_eq!(response.status().as_u16(), 500);
        assert!(
            String::from_utf8_lossy(response.body().bytes().expect("buffered body"))
                .contains("InternalFailure")
        );

        let _ = server.await;
    }
}
