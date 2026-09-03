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
    pub max_response_body: usize,
    pub max_redirects: u8,
    /// Total wall-clock silence tolerated while waiting for a response
    /// head (between flushing the request and the first response byte).
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            read_timeout: Some(Duration::from_secs(30)),
            max_response_body: 10 * 1024 * 1024,
            max_redirects: 10,
            head_silence: Duration::from_mins(2),
            stream_silence: Duration::from_mins(5),
        }
    }
}
