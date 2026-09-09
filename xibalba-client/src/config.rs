use std::time::Duration;

/// Default maximum response-head size in bytes.
///
/// API gateways commonly attach tracing, rate-limit, and routing metadata.
/// 64 KiB accepts those normal responses while retaining a bounded default.
/// Select another compile-time limit with `Client<C, MAX_HEAD_SIZE>` or
/// `AsyncClient<MAX_HEAD_SIZE>` when an integration has different needs.
pub const DEFAULT_MAX_HEAD_SIZE: usize = 64 * 1024;

/// Size of each socket read and body-decoding scratch buffer.
///
/// This deliberately does *not* limit a response head: the parsed
/// `HeadData` owns a dynamically sized copy of the complete head. Modern
/// API gateways can legitimately add enough tracing and rate-limit
/// metadata to exceed one read buffer. The `Client` const generic is the
/// explicit memory and abuse limit for response heads.
pub const HEAD_BUF_SIZE: usize = 8192;

pub struct Config {
    /// Per-socket-read ceiling (`SO_RCVTIMEO`). Keep this SHORT: it is
    /// the granularity at which a cancel signal is observed mid-stream,
    /// not a failure threshold. Silence tolerance is governed by the
    /// two budgets below, which retry across read-timeout ticks. Must
    /// be finite: with `None` the reader parks in the kernel until data
    /// arrives and the silence budgets can never trip.
    pub read_timeout: Option<Duration>,
    /// Per-socket-write ceiling (`SO_SNDTIMEO`), the write-side twin of
    /// `read_timeout`. A peer that stops reading fills the socket buffers and
    /// a single write blocks; this is the granularity at which a cancel or
    /// shutdown is observed during a request upload. Like `read_timeout` it is
    /// a granularity and not a failure threshold: an expiry is retried against
    /// `head_silence`, which is what decides the write has stalled.
    ///
    /// Only effective when the connector implements
    /// [`SetWriteTimeout`](crate::connector::SetReadTimeout::set_write_timeout);
    /// the default implementation ignores it, so such a connector is
    /// declaring its writes cannot block indefinitely.
    pub write_timeout: Option<Duration>,
    /// Total wall-clock bound on establishing a connection: resolution, every
    /// address attempted, and any eager TLS handshake together.
    ///
    /// This is the *only* bound on connecting. Every other blocking stage runs
    /// on a stream the client owns and is interrupted between I/O calls;
    /// `Connector::connect` has no stream yet and so no such point, which is
    /// why a cancel or a drop cannot be observed until it returns.
    ///
    /// `None` means no bound, leaving connects to the OS — roughly two
    /// minutes for a blackholed address on Linux, and unbounded for DNS.
    /// Effective only insofar as the connector honours it; see
    /// [`Connector::connect`](crate::connector::Connector::connect).
    pub connect_timeout: Option<Duration>,
    pub max_response_body: usize,
    pub max_redirects: u8,
    /// Whether a redirect may take the request to a different origin than
    /// the one it was sent to.
    ///
    /// A `Location` header is server-controlled input: following it
    /// automatically means the server a caller trusted enough to contact
    /// decides where the request — and any header or body it carries — goes
    /// next. `false` refuses any redirect whose origin differs from the
    /// request's, surfacing
    /// [`CrossOriginRedirectRefused`](xibalba_proto::error::ConnectionError::CrossOriginRedirectRefused)
    /// instead of dialling it; a same-origin redirect is unaffected.
    ///
    /// `true` (the default) preserves the previous behaviour: credential
    /// headers (`Authorization`, `Proxy-Authorization`, `Cookie`, `Cookie2`)
    /// are still stripped crossing an origin, but any other header a caller
    /// attached — including a custom bearer scheme this client does not
    /// recognise as a credential — is forwarded, and a 307/308 still
    /// replays the body. Set this to `false` for a request whose headers or
    /// body must never reach a host the caller did not name.
    pub allow_cross_origin_redirects: bool,
    /// Total wall-clock silence tolerated across dispatching a request and
    /// waiting for its response head.
    ///
    /// This covers the write as well as the wait. A lazily negotiated TLS
    /// session completes its handshake inside the client's first write, which
    /// blocks on the peer's records and expires on the socket's *receive*
    /// timeout; a server merely slow to begin its handshake would otherwise
    /// fail after one `read_timeout`.
    ///
    /// Inference providers legitimately spend a long time queueing,
    /// doing prompt-cache lookup, and initial reasoning before they
    /// emit the SSE head — a budget tied to `read_timeout` retry
    /// *counts* silently changed meaning with the configured timeout
    /// and surfaced raw `EAGAIN` ("os error 11") on healthy-but-slow
    /// starts. This is an explicit duration instead.
    pub head_silence: Duration,
    /// Total wall-clock silence tolerated between body bytes of a
    /// streaming response. Resets on every successful read. Generous
    /// by default: reasoning models can go quiet for minutes between
    /// SSE events; the cap only exists so a peer that half-dies
    /// without FIN/RST (NAT drop) surfaces as an error instead of
    /// wedging the reader forever.
    pub stream_silence: Duration,
    /// Optional total wall-clock bound for one request: dispatch, the
    /// response head, every redirect hop, and a buffered body share it.
    ///
    /// The silence budgets bound a *gap* and reset on every byte, so a peer
    /// delivering one byte just often enough passes them forever; this
    /// bounds the whole exchange and does not reset. It is spent
    /// cooperatively: a hop that used most of it leaves the rest to the
    /// next, and when it passes the request fails with
    /// [`RequestDeadlineExceeded`](xibalba_proto::error::ConnectionError::RequestDeadlineExceeded)
    /// rather than hanging.
    ///
    /// `None` (the default) imposes no total: the silence budgets alone
    /// govern, which is what intentional SSE streams need. A streaming
    /// body is never total-bounded even when this is set — its caller
    /// consumes at its own pace — but the request that produced it is.
    pub request_deadline: Option<Duration>,
}

impl Config {
    /// The deadline for one connect attempt, starting now.
    ///
    /// Built per attempt rather than stored, so a reconnect gets the full
    /// budget instead of the remains of the original connect's.
    pub(crate) fn connect_deadline(&self) -> crate::deadline::Deadline {
        self.connect_timeout.map_or_else(
            crate::deadline::Deadline::never,
            crate::deadline::Deadline::after,
        )
    }

    /// # Errors
    ///
    /// - [`InfiniteReadTimeout`](xibalba_proto::error::ConnectionError::InfiniteReadTimeout)
    ///   when `read_timeout` is `None`: silence budgets can only observe time
    ///   after a socket read returns.
    /// - [`ZeroDuration`](xibalba_proto::error::ConnectionError::ZeroDuration)
    ///   when any timeout or budget is zero. A zero read timeout makes every
    ///   read tick instantly, a zero budget is already spent when the first
    ///   read starts, and a zero connect timeout expires before the first
    ///   address is tried, so every request fails immediately.
    /// - [`TimeoutExceedsBudget`](xibalba_proto::error::ConnectionError::TimeoutExceedsBudget)
    ///   when a per-read timeout is longer than a budget it subdivides. The
    ///   budget is only consulted between reads, so one blocked read would
    ///   overshoot it by the difference.
    pub(crate) const fn validate(&self) -> Result<(), xibalba_proto::error::Error> {
        use xibalba_proto::error::{ConnectionError, Error};

        let Some(read_timeout) = self.read_timeout else {
            return Err(Error::Connection(ConnectionError::InfiniteReadTimeout));
        };
        if read_timeout.is_zero()
            || self.head_silence.is_zero()
            || self.stream_silence.is_zero()
            || matches!(self.write_timeout, Some(w) if w.is_zero())
            || matches!(self.connect_timeout, Some(c) if c.is_zero())
            || matches!(self.request_deadline, Some(r) if r.is_zero())
        {
            return Err(Error::Connection(ConnectionError::ZeroDuration));
        }
        if read_timeout.as_nanos() > self.head_silence.as_nanos()
            || read_timeout.as_nanos() > self.stream_silence.as_nanos()
        {
            return Err(Error::Connection(ConnectionError::TimeoutExceedsBudget));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(30)),
            max_response_body: 10 * 1024 * 1024,
            max_redirects: 10,
            allow_cross_origin_redirects: true,
            head_silence: Duration::from_mins(2),
            stream_silence: Duration::from_mins(5),
            request_deadline: None,
        }
    }
}
