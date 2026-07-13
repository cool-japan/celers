//! StatsD metrics backend.
//!
//! A dependency-free StatsD client built on [`std::net::UdpSocket`]. It provides:
//!
//! * Pure line-formatting functions for every StatsD metric kind (counter,
//!   gauge, gauge delta, timer, histogram, set) supporting an optional sample
//!   rate (`@0.1`) and optional DogStatsD-style tags (`|#key:value,...`).
//! * Metric-name sanitization that replaces characters that are illegal in the
//!   StatsD wire protocol (`:`, `|`, `@`, `#`, and whitespace).
//! * A UDP exporter ([`StatsdExporter`]) that batches multiple lines into
//!   newline-separated datagrams while respecting a maximum packet size, with
//!   fallible (non-panicking) construction.
//! * A registry-integrated path that serializes the process-wide CeleRS metrics
//!   ([`crate::CurrentMetrics`]) into StatsD lines.
//!
//! The wire format follows the de-facto StatsD specification and the DogStatsD
//! extensions: `<name>:<value>|<type>[|@<sample_rate>][|#<tag>,<tag>...]`.

use std::fmt;
use std::io;
use std::net::{ToSocketAddrs, UdpSocket};

use crate::aggregation::CustomLabels;

/// Conservative default maximum UDP payload size, in bytes.
///
/// StatsD over UDP works best when datagrams stay below the network MTU to
/// avoid IP fragmentation. 1432 bytes is the value recommended by DataDog for
/// most IPv4 networks (1500 MTU minus IP/UDP headers and a safety margin) and
/// is a safe default for batching multiple metric lines into one packet.
pub const DEFAULT_MAX_PACKET_SIZE: usize = 1432;

/// The kind of a StatsD metric, mapped to its wire-format type suffix.
///
/// Each variant corresponds to one StatsD metric type:
///
/// | Variant       | Suffix | Meaning                                  |
/// |---------------|--------|------------------------------------------|
/// | `Counter`     | `c`    | A value added to a running counter.      |
/// | `Gauge`       | `g`    | An absolute gauge value.                 |
/// | `GaugeDelta`  | `g`    | A signed delta applied to a gauge.       |
/// | `Timer`       | `ms`   | A timing measurement in milliseconds.    |
/// | `Histogram`   | `h`    | A value sampled into a histogram.        |
/// | `Set`         | `s`    | A unique value counted into a set.       |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatsdMetricKind {
    /// Counter (`|c`).
    Counter,
    /// Absolute gauge (`|g`).
    Gauge,
    /// Gauge delta (`|g`, value carries an explicit `+`/`-` sign).
    GaugeDelta,
    /// Timer in milliseconds (`|ms`).
    Timer,
    /// Histogram (`|h`).
    Histogram,
    /// Set (`|s`).
    Set,
}

impl StatsdMetricKind {
    /// The wire-format type suffix for this kind (without the leading `|`).
    #[must_use]
    pub const fn type_suffix(self) -> &'static str {
        match self {
            StatsdMetricKind::Counter => "c",
            StatsdMetricKind::Gauge | StatsdMetricKind::GaugeDelta => "g",
            StatsdMetricKind::Timer => "ms",
            StatsdMetricKind::Histogram => "h",
            StatsdMetricKind::Set => "s",
        }
    }

    /// Whether the value of this kind is rendered with an explicit sign.
    ///
    /// Only [`StatsdMetricKind::GaugeDelta`] forces a leading `+` on
    /// non-negative values so the server interprets it as a relative change
    /// rather than an absolute assignment.
    #[must_use]
    pub const fn is_signed_delta(self) -> bool {
        matches!(self, StatsdMetricKind::GaugeDelta)
    }
}

impl fmt::Display for StatsdMetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.type_suffix())
    }
}

/// Sanitize a metric name for the StatsD wire protocol.
///
/// The StatsD line format uses `:`, `|`, `@`, and `#` as delimiters, so those
/// characters (and any whitespace, including newlines, which would split a
/// datagram into multiple metrics) are replaced with `_`. All other characters
/// — including `.`, which is the conventional StatsD namespace separator — are
/// preserved. An empty name is mapped to `_` so a valid line is always
/// produced.
///
/// # Examples
///
/// ```
/// use celers_metrics::sanitize_statsd_name;
///
/// assert_eq!(sanitize_statsd_name("celers.queue.size"), "celers.queue.size");
/// assert_eq!(sanitize_statsd_name("weird:name|with@stuff"), "weird_name_with_stuff");
/// assert_eq!(sanitize_statsd_name("has space\tand\ntab"), "has_space_and_tab");
/// assert_eq!(sanitize_statsd_name(""), "_");
/// ```
#[must_use]
pub fn sanitize_statsd_name(name: &str) -> String {
    if name.is_empty() {
        return "_".to_string();
    }
    name.chars()
        .map(|c| match c {
            ':' | '|' | '@' | '#' => '_',
            c if c.is_whitespace() => '_',
            c => c,
        })
        .collect()
}

/// Sanitize a single tag component (key or value).
///
/// Tags are appended as `|#key:value,key2:value2`, so `|`, `#`, and `,` would
/// corrupt the tag list and are replaced with `_`. Whitespace and newlines are
/// likewise replaced. `:` is preserved because it is the key/value separator
/// within a DogStatsD tag, so `key:value` round-trips unchanged.
fn sanitize_tag_component(component: &str) -> String {
    component
        .chars()
        .map(|c| match c {
            '|' | '#' | ',' => '_',
            c if c.is_whitespace() => '_',
            c => c,
        })
        .collect()
}

/// Render the optional `|#tag,tag` suffix from a set of labels.
///
/// Returns an empty string when there are no labels. Tag keys and values are
/// sanitized to avoid breaking the wire format, and tags are emitted in
/// lexicographic key order so output is deterministic (which matters for tests
/// and for backends that de-duplicate identical lines).
fn format_tags(labels: &CustomLabels) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = labels
        .as_vec()
        .iter()
        .map(|(k, v)| (sanitize_tag_component(k), sanitize_tag_component(v)))
        .collect();
    pairs.sort();
    let rendered: Vec<String> = pairs
        .into_iter()
        .map(|(k, v)| if v.is_empty() { k } else { format!("{k}:{v}") })
        .collect();
    format!("|#{}", rendered.join(","))
}

/// Format an `f64` metric value for a StatsD line.
///
/// Finite values use the shortest round-tripping decimal representation (the
/// same as Rust's default `f64` `Display`). Non-finite values are not valid in
/// StatsD; `NaN` is emitted as `0` and infinities are clamped to `0` so a
/// malformed value never poisons a packet.
fn format_value(value: f64, signed_delta: bool) -> String {
    if !value.is_finite() {
        return if signed_delta {
            "+0".to_string()
        } else {
            "0".to_string()
        };
    }
    if signed_delta && value >= 0.0 {
        // Gauge deltas must carry an explicit sign so the server treats the
        // value as relative. Negative values already render with `-`.
        format!("+{value}")
    } else {
        format!("{value}")
    }
}

/// Format a sample-rate suffix (`|@<rate>`), or an empty string.
///
/// A suffix is only emitted when `sample_rate` is `Some` and strictly between
/// `0.0` and `1.0` (exclusive of `1.0`, since a rate of 1.0 is the implicit
/// default and adds no information). Out-of-range and non-finite rates are
/// clamped into `(0.0, 1.0]` first.
fn format_sample_rate(sample_rate: Option<f64>) -> String {
    match sample_rate {
        Some(rate) if rate.is_finite() => {
            let clamped = rate.clamp(0.0, 1.0);
            if clamped > 0.0 && clamped < 1.0 {
                format!("|@{clamped}")
            } else {
                String::new()
            }
        }
        _ => String::new(),
    }
}

/// Format a single StatsD line as a pure function.
///
/// Produces `<name>:<value>|<type>[|@<sample_rate>][|#<tags>]` where:
///
/// * `name` is sanitized via [`sanitize_statsd_name`].
/// * `value` is rendered as the shortest decimal; for
///   [`StatsdMetricKind::GaugeDelta`] a non-negative value gets a leading `+`.
/// * the type suffix comes from [`StatsdMetricKind::type_suffix`].
/// * `sample_rate`, when present and in `(0, 1)`, appends `|@<rate>`.
/// * `labels`, when non-empty, append `|#k:v,...` (DogStatsD tags).
///
/// The returned string never contains a newline, so it is safe to join with
/// `\n` when batching.
///
/// # Examples
///
/// ```
/// use celers_metrics::{format_statsd_line, StatsdMetricKind, CustomLabels};
///
/// // Plain counter.
/// assert_eq!(
///     format_statsd_line("api.requests", 5.0, StatsdMetricKind::Counter, None, &CustomLabels::new()),
///     "api.requests:5|c"
/// );
///
/// // Gauge delta with explicit sign.
/// assert_eq!(
///     format_statsd_line("queue.size", 3.0, StatsdMetricKind::GaugeDelta, None, &CustomLabels::new()),
///     "queue.size:+3|g"
/// );
///
/// // Timer with sample rate and tags.
/// let labels = CustomLabels::new().with_label("region", "us");
/// assert_eq!(
///     format_statsd_line("task.duration", 12.5, StatsdMetricKind::Timer, Some(0.1), &labels),
///     "task.duration:12.5|ms|@0.1|#region:us"
/// );
/// ```
#[must_use]
pub fn format_statsd_line(
    name: &str,
    value: f64,
    kind: StatsdMetricKind,
    sample_rate: Option<f64>,
    labels: &CustomLabels,
) -> String {
    let name = sanitize_statsd_name(name);
    let value = format_value(value, kind.is_signed_delta());
    let type_suffix = kind.type_suffix();
    let rate = format_sample_rate(sample_rate);
    let tags = format_tags(labels);
    format!("{name}:{value}|{type_suffix}{rate}{tags}")
}

/// A fully-described StatsD metric ready to be formatted or sent.
///
/// This is the owned, allocation-backed counterpart of the pure
/// [`format_statsd_line`] function. It is useful when collecting a batch of
/// heterogeneous metrics before handing them to a [`StatsdExporter`].
#[derive(Debug, Clone)]
pub struct StatsdMetric {
    /// Metric name (sanitized at format time).
    pub name: String,
    /// Metric value. For [`StatsdMetricKind::GaugeDelta`] the sign is
    /// significant.
    pub value: f64,
    /// Metric kind / wire type.
    pub kind: StatsdMetricKind,
    /// Optional sample rate in `(0, 1)`; `None` or `1.0` omits the suffix.
    pub sample_rate: Option<f64>,
    /// DogStatsD-style tags.
    pub labels: CustomLabels,
}

impl StatsdMetric {
    /// Construct a metric with no sample rate and no tags.
    #[must_use]
    pub fn new(name: impl Into<String>, value: f64, kind: StatsdMetricKind) -> Self {
        Self {
            name: name.into(),
            value,
            kind,
            sample_rate: None,
            labels: CustomLabels::new(),
        }
    }

    /// Shorthand for a counter metric.
    #[must_use]
    pub fn counter(name: impl Into<String>, value: f64) -> Self {
        Self::new(name, value, StatsdMetricKind::Counter)
    }

    /// Shorthand for an absolute gauge metric.
    #[must_use]
    pub fn gauge(name: impl Into<String>, value: f64) -> Self {
        Self::new(name, value, StatsdMetricKind::Gauge)
    }

    /// Shorthand for a gauge-delta metric (value sign is significant).
    #[must_use]
    pub fn gauge_delta(name: impl Into<String>, delta: f64) -> Self {
        Self::new(name, delta, StatsdMetricKind::GaugeDelta)
    }

    /// Shorthand for a timer metric (milliseconds).
    #[must_use]
    pub fn timer(name: impl Into<String>, millis: f64) -> Self {
        Self::new(name, millis, StatsdMetricKind::Timer)
    }

    /// Shorthand for a histogram metric.
    #[must_use]
    pub fn histogram(name: impl Into<String>, value: f64) -> Self {
        Self::new(name, value, StatsdMetricKind::Histogram)
    }

    /// Shorthand for a set metric.
    #[must_use]
    pub fn set(name: impl Into<String>, value: f64) -> Self {
        Self::new(name, value, StatsdMetricKind::Set)
    }

    /// Attach a sample rate, returning `self` for chaining.
    #[must_use]
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = Some(rate);
        self
    }

    /// Attach a single tag, returning `self` for chaining.
    #[must_use]
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels = self.labels.with_label(key, value);
        self
    }

    /// Replace the metric's tags, returning `self` for chaining.
    #[must_use]
    pub fn with_labels(mut self, labels: CustomLabels) -> Self {
        self.labels = labels;
        self
    }

    /// Render this metric to its StatsD wire line.
    #[must_use]
    pub fn to_line(&self) -> String {
        format_statsd_line(
            &self.name,
            self.value,
            self.kind,
            self.sample_rate,
            &self.labels,
        )
    }
}

impl fmt::Display for StatsdMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_line())
    }
}

/// Pack already-formatted StatsD lines into newline-separated UDP payloads.
///
/// Each returned `String` is a datagram body whose length does not exceed
/// `max_packet_size`, formed by joining lines with `\n`. A line longer than
/// `max_packet_size` on its own is emitted as a packet of its own (it cannot be
/// split without corrupting the metric), so callers should keep individual
/// metric names short. Empty input yields an empty `Vec`.
///
/// # Examples
///
/// ```
/// use celers_metrics::pack_statsd_lines;
///
/// let lines = ["a:1|c".to_string(), "b:2|c".to_string(), "c:3|c".to_string()];
/// // Small budget forces one line per packet.
/// let packets = pack_statsd_lines(&lines, 5);
/// assert_eq!(packets.len(), 3);
/// // Generous budget packs everything into one packet.
/// let packets = pack_statsd_lines(&lines, 1432);
/// assert_eq!(packets, vec!["a:1|c\nb:2|c\nc:3|c".to_string()]);
/// ```
#[must_use]
pub fn pack_statsd_lines(lines: &[String], max_packet_size: usize) -> Vec<String> {
    let budget = max_packet_size.max(1);
    let mut packets: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in lines {
        if line.is_empty() {
            continue;
        }
        if current.is_empty() {
            // First line in a fresh packet; it always goes in even if it alone
            // exceeds the budget (an oversize single metric cannot be split).
            current.push_str(line);
            continue;
        }

        // +1 accounts for the `\n` separator that would join this line.
        let projected = current.len() + 1 + line.len();
        if projected <= budget {
            current.push('\n');
            current.push_str(line);
        } else {
            packets.push(std::mem::take(&mut current));
            current.push_str(line);
        }
    }

    if !current.is_empty() {
        packets.push(current);
    }

    packets
}

/// Errors that can occur while constructing or using a [`StatsdExporter`].
#[derive(Debug)]
pub enum StatsdError {
    /// The configured destination did not resolve to any socket address.
    NoAddress(String),
    /// An underlying I/O operation failed (socket bind, connect, or send).
    Io(io::Error),
}

impl fmt::Display for StatsdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatsdError::NoAddress(target) => {
                write!(f, "no socket address resolved for StatsD target `{target}`")
            }
            StatsdError::Io(err) => write!(f, "StatsD I/O error: {err}"),
        }
    }
}

impl std::error::Error for StatsdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StatsdError::Io(err) => Some(err),
            StatsdError::NoAddress(_) => None,
        }
    }
}

impl From<io::Error> for StatsdError {
    fn from(err: io::Error) -> Self {
        StatsdError::Io(err)
    }
}

/// A UDP StatsD exporter.
///
/// Owns a connected [`UdpSocket`] bound to an ephemeral local address and
/// targeting the configured StatsD server. Sending batches multiple metric
/// lines into newline-separated datagrams that respect [`Self::max_packet_size`].
///
/// Construction is fallible and never panics: address resolution, socket bind,
/// and connect failures are returned as [`StatsdError`].
///
/// # Examples
///
/// ```no_run
/// use celers_metrics::{StatsdExporter, StatsdMetric};
///
/// let exporter = StatsdExporter::connect("127.0.0.1:8125")?;
/// exporter.send(&StatsdMetric::counter("api.requests", 1.0))?;
/// # Ok::<(), celers_metrics::StatsdError>(())
/// ```
#[derive(Debug)]
pub struct StatsdExporter {
    socket: UdpSocket,
    max_packet_size: usize,
}

impl StatsdExporter {
    /// Connect to a StatsD server at `addr` (e.g. `"127.0.0.1:8125"`).
    ///
    /// Binds a UDP socket to an ephemeral local address on the matching IP
    /// family and connects it to the resolved server address. Uses
    /// [`DEFAULT_MAX_PACKET_SIZE`] for batching.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::NoAddress`] if `addr` resolves to no socket
    /// address, or [`StatsdError::Io`] if binding or connecting fails.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, StatsdError> {
        Self::connect_with_packet_size(addr, DEFAULT_MAX_PACKET_SIZE)
    }

    /// Connect to a StatsD server with an explicit maximum packet size.
    ///
    /// A `max_packet_size` of `0` is treated as `1` to guarantee progress.
    ///
    /// # Errors
    ///
    /// As [`Self::connect`].
    pub fn connect_with_packet_size<A: ToSocketAddrs>(
        addr: A,
        max_packet_size: usize,
    ) -> Result<Self, StatsdError> {
        let mut last_err: Option<StatsdError> = None;
        let addrs = addr
            .to_socket_addrs()
            .map_err(StatsdError::Io)?
            .collect::<Vec<_>>();

        if addrs.is_empty() {
            return Err(StatsdError::NoAddress(
                "<no resolved addresses>".to_string(),
            ));
        }

        for target in addrs {
            // Bind to the wildcard address of the matching family so the OS
            // chooses an ephemeral port; then connect to fix the destination.
            let bind_addr: &str = if target.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            };
            match UdpSocket::bind(bind_addr) {
                Ok(socket) => match socket.connect(target) {
                    Ok(()) => {
                        return Ok(Self {
                            socket,
                            max_packet_size: max_packet_size.max(1),
                        });
                    }
                    Err(err) => last_err = Some(StatsdError::Io(err)),
                },
                Err(err) => last_err = Some(StatsdError::Io(err)),
            }
        }

        Err(last_err
            .unwrap_or_else(|| StatsdError::NoAddress("<no resolved addresses>".to_string())))
    }

    /// Build an exporter from an already-bound, already-connected socket.
    ///
    /// This is primarily useful for tests and for callers that need full
    /// control over socket options. The socket must already be connected to the
    /// StatsD destination (so [`UdpSocket::send`] can be used).
    #[must_use]
    pub fn from_socket(socket: UdpSocket, max_packet_size: usize) -> Self {
        Self {
            socket,
            max_packet_size: max_packet_size.max(1),
        }
    }

    /// The maximum UDP payload size used when batching, in bytes.
    #[must_use]
    pub fn max_packet_size(&self) -> usize {
        self.max_packet_size
    }

    /// The local address the exporter's socket is bound to.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::Io`] if the local address cannot be retrieved.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, StatsdError> {
        self.socket.local_addr().map_err(StatsdError::Io)
    }

    /// Send a single metric as one datagram.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::Io`] if the send fails.
    pub fn send(&self, metric: &StatsdMetric) -> Result<(), StatsdError> {
        self.send_line(&metric.to_line())?;
        Ok(())
    }

    /// Send a single pre-formatted line as one datagram.
    ///
    /// Empty lines are skipped (sending an empty datagram would be a no-op
    /// metric). Returns the number of bytes written, or `0` for a skipped line.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::Io`] if the send fails.
    pub fn send_line(&self, line: &str) -> Result<usize, StatsdError> {
        if line.is_empty() {
            return Ok(0);
        }
        self.socket.send(line.as_bytes()).map_err(StatsdError::Io)
    }

    /// Send many metrics, batching them into as few datagrams as possible while
    /// respecting [`Self::max_packet_size`].
    ///
    /// Returns the number of datagrams actually written.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::Io`] on the first send that fails. Metrics in
    /// earlier packets may already have been sent.
    pub fn send_batch(&self, metrics: &[StatsdMetric]) -> Result<usize, StatsdError> {
        let lines: Vec<String> = metrics.iter().map(StatsdMetric::to_line).collect();
        self.send_lines(&lines)
    }

    /// Send many pre-formatted lines, batched into datagrams that respect
    /// [`Self::max_packet_size`].
    ///
    /// Returns the number of datagrams written.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::Io`] on the first failing send.
    pub fn send_lines(&self, lines: &[String]) -> Result<usize, StatsdError> {
        let packets = pack_statsd_lines(lines, self.max_packet_size);
        let mut sent = 0;
        for packet in &packets {
            self.socket
                .send(packet.as_bytes())
                .map_err(StatsdError::Io)?;
            sent += 1;
        }
        Ok(sent)
    }
}

// ============================================================================
// Registry-integrated path
// ============================================================================

/// Serialize a [`crate::CurrentMetrics`] snapshot into StatsD lines.
///
/// Counters (cumulative totals) are emitted as gauges rather than `|c`
/// increments, because [`crate::CurrentMetrics`] captures the absolute value of
/// each process-wide counter; sending it as `|c` would make the StatsD server
/// add the running total on every flush. Reporting the absolute value as a
/// gauge is the correct semantics for a periodic scrape of cumulative metrics.
/// Genuine gauges (queue sizes, worker counts) are likewise emitted as `|g`.
///
/// `prefix` is prepended to every metric name (joined with `.`); an empty
/// prefix emits bare names. `labels` are attached to every line as DogStatsD
/// tags, and `sample_rate` is applied to every line.
///
/// # Examples
///
/// ```
/// use celers_metrics::{statsd_lines_from_current, CurrentMetrics, CustomLabels};
///
/// let snapshot = CurrentMetrics {
///     tasks_enqueued: 10.0,
///     tasks_completed: 8.0,
///     tasks_failed: 1.0,
///     tasks_retried: 2.0,
///     tasks_cancelled: 0.0,
///     queue_size: 3.0,
///     processing_queue_size: 1.0,
///     dlq_size: 0.0,
///     active_workers: 4.0,
///     total_payload_bytes: 2048.0,
/// };
/// let lines = statsd_lines_from_current(&snapshot, "celers", None, &CustomLabels::new());
/// assert!(lines.iter().any(|l| l == "celers.queue_size:3|g"));
/// assert!(lines.iter().any(|l| l == "celers.tasks_enqueued_total:10|g"));
/// ```
#[must_use]
pub fn statsd_lines_from_current(
    metrics: &crate::CurrentMetrics,
    prefix: &str,
    sample_rate: Option<f64>,
    labels: &CustomLabels,
) -> Vec<String> {
    // (suffix, value, kind) for every field of the snapshot.
    let entries: [(&str, f64, StatsdMetricKind); 10] = [
        (
            "tasks_enqueued_total",
            metrics.tasks_enqueued,
            StatsdMetricKind::Gauge,
        ),
        (
            "tasks_completed_total",
            metrics.tasks_completed,
            StatsdMetricKind::Gauge,
        ),
        (
            "tasks_failed_total",
            metrics.tasks_failed,
            StatsdMetricKind::Gauge,
        ),
        (
            "tasks_retried_total",
            metrics.tasks_retried,
            StatsdMetricKind::Gauge,
        ),
        (
            "tasks_cancelled_total",
            metrics.tasks_cancelled,
            StatsdMetricKind::Gauge,
        ),
        ("queue_size", metrics.queue_size, StatsdMetricKind::Gauge),
        (
            "processing_queue_size",
            metrics.processing_queue_size,
            StatsdMetricKind::Gauge,
        ),
        ("dlq_size", metrics.dlq_size, StatsdMetricKind::Gauge),
        (
            "active_workers",
            metrics.active_workers,
            StatsdMetricKind::Gauge,
        ),
        (
            "total_payload_bytes",
            metrics.total_payload_bytes,
            StatsdMetricKind::Gauge,
        ),
    ];

    entries
        .iter()
        .map(|(suffix, value, kind)| {
            let name = if prefix.is_empty() {
                (*suffix).to_string()
            } else {
                format!("{prefix}.{suffix}")
            };
            format_statsd_line(&name, *value, *kind, sample_rate, labels)
        })
        .collect()
}

impl StatsdExporter {
    /// Capture the process-wide CeleRS metrics and send them as StatsD lines.
    ///
    /// This is the registry-integrated convenience path: it snapshots
    /// [`crate::CurrentMetrics::capture`], serializes it via
    /// [`statsd_lines_from_current`], and batches the result over UDP.
    ///
    /// Returns the number of datagrams written.
    ///
    /// # Errors
    ///
    /// Returns [`StatsdError::Io`] on the first failing send.
    pub fn send_current_metrics(
        &self,
        prefix: &str,
        sample_rate: Option<f64>,
        labels: &CustomLabels,
    ) -> Result<usize, StatsdError> {
        let snapshot = crate::CurrentMetrics::capture();
        let lines = statsd_lines_from_current(&snapshot, prefix, sample_rate, labels);
        self.send_lines(&lines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::time::Duration;

    fn labels(pairs: &[(&str, &str)]) -> CustomLabels {
        let mut l = CustomLabels::new();
        for (k, v) in pairs {
            l = l.with_label(*k, *v);
        }
        l
    }

    // ---- Exact line formatting per kind -----------------------------------

    #[test]
    fn counter_line() {
        assert_eq!(
            format_statsd_line(
                "api.requests",
                5.0,
                StatsdMetricKind::Counter,
                None,
                &CustomLabels::new()
            ),
            "api.requests:5|c"
        );
    }

    #[test]
    fn gauge_line() {
        assert_eq!(
            format_statsd_line(
                "queue.size",
                12.0,
                StatsdMetricKind::Gauge,
                None,
                &CustomLabels::new()
            ),
            "queue.size:12|g"
        );
    }

    #[test]
    fn gauge_delta_positive_has_plus() {
        assert_eq!(
            format_statsd_line(
                "queue.size",
                3.0,
                StatsdMetricKind::GaugeDelta,
                None,
                &CustomLabels::new()
            ),
            "queue.size:+3|g"
        );
    }

    #[test]
    fn gauge_delta_negative_has_minus() {
        assert_eq!(
            format_statsd_line(
                "queue.size",
                -4.0,
                StatsdMetricKind::GaugeDelta,
                None,
                &CustomLabels::new()
            ),
            "queue.size:-4|g"
        );
    }

    #[test]
    fn gauge_delta_zero_has_plus() {
        assert_eq!(
            format_statsd_line(
                "queue.size",
                0.0,
                StatsdMetricKind::GaugeDelta,
                None,
                &CustomLabels::new()
            ),
            "queue.size:+0|g"
        );
    }

    #[test]
    fn timer_line() {
        assert_eq!(
            format_statsd_line(
                "task.duration",
                250.0,
                StatsdMetricKind::Timer,
                None,
                &CustomLabels::new()
            ),
            "task.duration:250|ms"
        );
    }

    #[test]
    fn histogram_line() {
        assert_eq!(
            format_statsd_line(
                "payload.bytes",
                1024.0,
                StatsdMetricKind::Histogram,
                None,
                &CustomLabels::new()
            ),
            "payload.bytes:1024|h"
        );
    }

    #[test]
    fn set_line() {
        assert_eq!(
            format_statsd_line(
                "unique.users",
                42.0,
                StatsdMetricKind::Set,
                None,
                &CustomLabels::new()
            ),
            "unique.users:42|s"
        );
    }

    #[test]
    fn fractional_value_round_trips_shortest() {
        assert_eq!(
            format_statsd_line(
                "task.duration",
                12.5,
                StatsdMetricKind::Timer,
                None,
                &CustomLabels::new()
            ),
            "task.duration:12.5|ms"
        );
    }

    // ---- Sample rate ------------------------------------------------------

    #[test]
    fn sample_rate_suffix() {
        assert_eq!(
            format_statsd_line(
                "api.requests",
                1.0,
                StatsdMetricKind::Counter,
                Some(0.1),
                &CustomLabels::new()
            ),
            "api.requests:1|c|@0.1"
        );
    }

    #[test]
    fn sample_rate_one_is_omitted() {
        assert_eq!(
            format_statsd_line(
                "api.requests",
                1.0,
                StatsdMetricKind::Counter,
                Some(1.0),
                &CustomLabels::new()
            ),
            "api.requests:1|c"
        );
    }

    #[test]
    fn sample_rate_zero_is_omitted() {
        assert_eq!(
            format_statsd_line(
                "api.requests",
                1.0,
                StatsdMetricKind::Counter,
                Some(0.0),
                &CustomLabels::new()
            ),
            "api.requests:1|c"
        );
    }

    #[test]
    fn sample_rate_out_of_range_is_clamped_and_omitted() {
        // 2.0 clamps to 1.0 -> omitted.
        assert_eq!(
            format_statsd_line(
                "api.requests",
                1.0,
                StatsdMetricKind::Counter,
                Some(2.0),
                &CustomLabels::new()
            ),
            "api.requests:1|c"
        );
        // NaN -> omitted.
        assert_eq!(
            format_statsd_line(
                "api.requests",
                1.0,
                StatsdMetricKind::Counter,
                Some(f64::NAN),
                &CustomLabels::new()
            ),
            "api.requests:1|c"
        );
    }

    // ---- Tags -------------------------------------------------------------

    #[test]
    fn single_tag() {
        assert_eq!(
            format_statsd_line(
                "api.requests",
                1.0,
                StatsdMetricKind::Counter,
                None,
                &labels(&[("region", "us")])
            ),
            "api.requests:1|c|#region:us"
        );
    }

    #[test]
    fn multiple_tags_are_sorted() {
        // HashMap order is non-deterministic; output must be lexicographically sorted by key.
        let line = format_statsd_line(
            "api.requests",
            1.0,
            StatsdMetricKind::Counter,
            None,
            &labels(&[("z", "26"), ("a", "1"), ("m", "13")]),
        );
        assert_eq!(line, "api.requests:1|c|#a:1,m:13,z:26");
    }

    #[test]
    fn sample_rate_and_tags_combined() {
        let line = format_statsd_line(
            "task.duration",
            12.5,
            StatsdMetricKind::Timer,
            Some(0.1),
            &labels(&[("region", "us")]),
        );
        assert_eq!(line, "task.duration:12.5|ms|@0.1|#region:us");
    }

    #[test]
    fn tag_value_only_when_present() {
        // Empty value renders just the key (valid DogStatsD bare tag).
        let line = format_statsd_line(
            "api.requests",
            1.0,
            StatsdMetricKind::Counter,
            None,
            &labels(&[("featureflag", "")]),
        );
        assert_eq!(line, "api.requests:1|c|#featureflag");
    }

    #[test]
    fn illegal_chars_in_tags_are_sanitized() {
        let line = format_statsd_line(
            "api.requests",
            1.0,
            StatsdMetricKind::Counter,
            None,
            &labels(&[("ke,y", "va|lue")]),
        );
        assert_eq!(line, "api.requests:1|c|#ke_y:va_lue");
    }

    // ---- Name sanitization -----------------------------------------------

    #[test]
    fn name_sanitization_replaces_delimiters() {
        assert_eq!(
            format_statsd_line(
                "weird:name|with@stuff#tag",
                1.0,
                StatsdMetricKind::Counter,
                None,
                &CustomLabels::new()
            ),
            "weird_name_with_stuff_tag:1|c"
        );
    }

    #[test]
    fn name_sanitization_replaces_whitespace_and_newlines() {
        assert_eq!(
            format_statsd_line(
                "has space\tand\nnewline",
                1.0,
                StatsdMetricKind::Counter,
                None,
                &CustomLabels::new()
            ),
            "has_space_and_newline:1|c"
        );
    }

    #[test]
    fn name_sanitization_keeps_dots() {
        assert_eq!(
            sanitize_statsd_name("celers.queue.size"),
            "celers.queue.size"
        );
    }

    #[test]
    fn empty_name_becomes_underscore() {
        assert_eq!(sanitize_statsd_name(""), "_");
        assert_eq!(
            format_statsd_line(
                "",
                1.0,
                StatsdMetricKind::Counter,
                None,
                &CustomLabels::new()
            ),
            "_:1|c"
        );
    }

    #[test]
    fn formatted_line_never_contains_newline() {
        let line = format_statsd_line(
            "a\nb",
            1.0,
            StatsdMetricKind::Counter,
            Some(0.5),
            &labels(&[("k\n", "v\n")]),
        );
        assert!(!line.contains('\n'), "line must be newline-free: {line:?}");
    }

    #[test]
    fn non_finite_value_is_zeroed() {
        assert_eq!(
            format_statsd_line(
                "g",
                f64::INFINITY,
                StatsdMetricKind::Gauge,
                None,
                &CustomLabels::new()
            ),
            "g:0|g"
        );
        assert_eq!(
            format_statsd_line(
                "g",
                f64::NAN,
                StatsdMetricKind::GaugeDelta,
                None,
                &CustomLabels::new()
            ),
            "g:+0|g"
        );
    }

    // ---- StatsdMetric builder --------------------------------------------

    #[test]
    fn metric_builder_to_line() {
        let m = StatsdMetric::timer("task.duration", 12.5)
            .with_sample_rate(0.1)
            .with_tag("region", "us");
        assert_eq!(m.to_line(), "task.duration:12.5|ms|@0.1|#region:us");
        assert_eq!(m.to_string(), "task.duration:12.5|ms|@0.1|#region:us");
    }

    #[test]
    fn metric_kind_shorthands() {
        assert_eq!(StatsdMetric::counter("a", 1.0).to_line(), "a:1|c");
        assert_eq!(StatsdMetric::gauge("a", 1.0).to_line(), "a:1|g");
        assert_eq!(StatsdMetric::gauge_delta("a", 1.0).to_line(), "a:+1|g");
        assert_eq!(StatsdMetric::histogram("a", 1.0).to_line(), "a:1|h");
        assert_eq!(StatsdMetric::set("a", 1.0).to_line(), "a:1|s");
    }

    // ---- Batching / packet splitting -------------------------------------

    #[test]
    fn packing_empty_yields_empty() {
        assert!(pack_statsd_lines(&[], 1432).is_empty());
    }

    #[test]
    fn packing_joins_within_budget() {
        let lines = vec![
            "a:1|c".to_string(),
            "b:2|c".to_string(),
            "c:3|c".to_string(),
        ];
        let packets = pack_statsd_lines(&lines, 1432);
        assert_eq!(packets, vec!["a:1|c\nb:2|c\nc:3|c".to_string()]);
    }

    #[test]
    fn packing_splits_at_budget() {
        // "aa:1|c" is 6 bytes. Budget 13 fits two (6 + 1 + 6 = 13) but not three.
        let lines = vec![
            "aa:1|c".to_string(),
            "bb:2|c".to_string(),
            "cc:3|c".to_string(),
        ];
        let packets = pack_statsd_lines(&lines, 13);
        assert_eq!(
            packets,
            vec!["aa:1|c\nbb:2|c".to_string(), "cc:3|c".to_string()]
        );
        for p in &packets {
            assert!(p.len() <= 13, "packet exceeds budget: {p:?}");
        }
    }

    #[test]
    fn packing_one_line_per_tiny_budget() {
        let lines = vec![
            "a:1|c".to_string(),
            "b:2|c".to_string(),
            "c:3|c".to_string(),
        ];
        let packets = pack_statsd_lines(&lines, 1);
        assert_eq!(packets.len(), 3);
    }

    #[test]
    fn packing_oversize_single_line_is_its_own_packet() {
        let big = "x".repeat(50);
        let line = format!("{big}:1|c");
        let lines = vec!["a:1|c".to_string(), line.clone(), "b:2|c".to_string()];
        let packets = pack_statsd_lines(&lines, 10);
        // The oversize line cannot be split; it is emitted alone.
        assert!(packets.contains(&line));
        assert_eq!(packets.len(), 3);
    }

    #[test]
    fn packing_skips_empty_lines() {
        let lines = vec!["a:1|c".to_string(), String::new(), "b:2|c".to_string()];
        let packets = pack_statsd_lines(&lines, 1432);
        assert_eq!(packets, vec!["a:1|c\nb:2|c".to_string()]);
    }

    // ---- Exporter construction -------------------------------------------

    #[test]
    fn connect_to_loopback_succeeds() {
        // A receiver need not exist for an unconnected UDP bind+connect.
        let exporter = StatsdExporter::connect("127.0.0.1:8125");
        assert!(exporter.is_ok(), "connect failed: {:?}", exporter.err());
        let exporter = exporter.expect("checked is_ok");
        assert_eq!(exporter.max_packet_size(), DEFAULT_MAX_PACKET_SIZE);
        assert!(exporter.local_addr().is_ok());
    }

    #[test]
    fn connect_with_custom_packet_size() {
        let exporter = StatsdExporter::connect_with_packet_size("127.0.0.1:8125", 512)
            .expect("connect should succeed");
        assert_eq!(exporter.max_packet_size(), 512);
    }

    #[test]
    fn connect_zero_packet_size_becomes_one() {
        let exporter = StatsdExporter::connect_with_packet_size("127.0.0.1:8125", 0)
            .expect("connect should succeed");
        assert_eq!(exporter.max_packet_size(), 1);
    }

    #[test]
    fn connect_invalid_address_errors_without_panic() {
        let result = StatsdExporter::connect("definitely-not-a-real-host.invalid:0");
        assert!(result.is_err());
        // Error renders without panicking.
        let _ = format!("{}", result.expect_err("checked is_err"));
    }

    // ---- Loopback round-trip ---------------------------------------------

    /// Bind a real receiver on 127.0.0.1:0, send a single metric, and assert the
    /// exact bytes received.
    #[test]
    fn loopback_round_trip_single() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        let receiver_addr = receiver.local_addr().expect("receiver addr");

        let exporter = StatsdExporter::connect(receiver_addr).expect("connect exporter");
        let metric = StatsdMetric::timer("task.duration", 12.5)
            .with_sample_rate(0.5)
            .with_tag("region", "us");
        exporter.send(&metric).expect("send metric");

        let mut buf = [0_u8; 1024];
        let n = receiver.recv(&mut buf).expect("recv datagram");
        let received = std::str::from_utf8(&buf[..n]).expect("utf8");
        assert_eq!(received, "task.duration:12.5|ms|@0.5|#region:us");
    }

    /// Send a batch that fits in one packet and assert the newline-joined body.
    #[test]
    fn loopback_round_trip_batch_one_packet() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        let receiver_addr = receiver.local_addr().expect("receiver addr");

        let exporter = StatsdExporter::connect(receiver_addr).expect("connect exporter");
        let metrics = vec![
            StatsdMetric::counter("a", 1.0),
            StatsdMetric::gauge("b", 2.0),
            StatsdMetric::set("c", 3.0),
        ];
        let packets = exporter.send_batch(&metrics).expect("send batch");
        assert_eq!(packets, 1);

        let mut buf = [0_u8; 1024];
        let n = receiver.recv(&mut buf).expect("recv datagram");
        let received = std::str::from_utf8(&buf[..n]).expect("utf8");
        assert_eq!(received, "a:1|c\nb:2|g\nc:3|s");
    }

    /// Force packet splitting via a tiny max-packet-size and assert each datagram.
    #[test]
    fn loopback_round_trip_batch_split() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        let receiver_addr = receiver.local_addr().expect("receiver addr");

        // Budget large enough for one "x:N|c" (5 bytes) per packet but not two.
        let exporter =
            StatsdExporter::connect_with_packet_size(receiver_addr, 5).expect("connect exporter");
        let metrics = vec![
            StatsdMetric::counter("a", 1.0),
            StatsdMetric::counter("b", 2.0),
        ];
        let packets = exporter.send_batch(&metrics).expect("send batch");
        assert_eq!(packets, 2);

        let mut first = [0_u8; 64];
        let n1 = receiver.recv(&mut first).expect("recv first");
        assert_eq!(std::str::from_utf8(&first[..n1]).expect("utf8"), "a:1|c");

        let mut second = [0_u8; 64];
        let n2 = receiver.recv(&mut second).expect("recv second");
        assert_eq!(std::str::from_utf8(&second[..n2]).expect("utf8"), "b:2|c");
    }

    // ---- Registry-integrated path ----------------------------------------

    #[test]
    fn current_metrics_serialization() {
        let snapshot = crate::CurrentMetrics {
            tasks_enqueued: 10.0,
            tasks_completed: 8.0,
            tasks_failed: 1.0,
            tasks_retried: 2.0,
            tasks_cancelled: 0.0,
            queue_size: 3.0,
            processing_queue_size: 1.0,
            dlq_size: 0.0,
            active_workers: 4.0,
            total_payload_bytes: 2048.0,
        };
        let lines = statsd_lines_from_current(&snapshot, "celers", None, &CustomLabels::new());
        assert_eq!(lines.len(), 10);
        assert!(lines.contains(&"celers.queue_size:3|g".to_string()));
        assert!(lines.contains(&"celers.active_workers:4|g".to_string()));
        assert!(lines.contains(&"celers.tasks_enqueued_total:10|g".to_string()));
        assert!(lines.contains(&"celers.total_payload_bytes:2048|g".to_string()));
    }

    #[test]
    fn current_metrics_empty_prefix() {
        let snapshot = crate::CurrentMetrics {
            tasks_enqueued: 0.0,
            tasks_completed: 0.0,
            tasks_failed: 0.0,
            tasks_retried: 0.0,
            tasks_cancelled: 0.0,
            queue_size: 7.0,
            processing_queue_size: 0.0,
            dlq_size: 0.0,
            active_workers: 0.0,
            total_payload_bytes: 0.0,
        };
        let lines = statsd_lines_from_current(&snapshot, "", None, &CustomLabels::new());
        assert!(lines.contains(&"queue_size:7|g".to_string()));
    }

    #[test]
    fn current_metrics_with_tags_and_sample_rate() {
        let snapshot = crate::CurrentMetrics {
            tasks_enqueued: 0.0,
            tasks_completed: 0.0,
            tasks_failed: 0.0,
            tasks_retried: 0.0,
            tasks_cancelled: 0.0,
            queue_size: 5.0,
            processing_queue_size: 0.0,
            dlq_size: 0.0,
            active_workers: 0.0,
            total_payload_bytes: 0.0,
        };
        let lines =
            statsd_lines_from_current(&snapshot, "celers", Some(0.25), &labels(&[("env", "prod")]));
        assert!(lines.contains(&"celers.queue_size:5|g|@0.25|#env:prod".to_string()));
    }

    /// End-to-end registry-integrated send over loopback.
    #[test]
    fn loopback_send_current_metrics() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        let receiver_addr = receiver.local_addr().expect("receiver addr");

        let exporter = StatsdExporter::connect(receiver_addr).expect("connect exporter");
        let packets = exporter
            .send_current_metrics("celers", None, &CustomLabels::new())
            .expect("send current metrics");
        assert!(packets >= 1);

        let mut buf = [0_u8; 4096];
        let n = receiver.recv(&mut buf).expect("recv datagram");
        let received = std::str::from_utf8(&buf[..n]).expect("utf8");
        // All ten metrics fit in the default packet size, so a single datagram
        // carries every line.
        assert!(received.contains("celers.queue_size:"));
        assert!(received.contains("|g"));
        assert_eq!(received.lines().count(), 10);
    }
}
