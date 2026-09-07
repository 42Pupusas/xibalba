use std::io::{Read, Write};
use std::time::Duration;

use xibalba_proto::error::Error;
use xibalba_proto::url::Url;

use crate::deadline::Deadline;

/// Abstraction over anything that can set a read timeout.
/// Mirrors `TcpStream::set_read_timeout`.
///
/// # Blocking and error contract
///
/// The client drives a blocking stream and regains control between I/O calls,
/// so an implementor must hold to three things:
///
/// 1. **Reads block up to the configured timeout, then report a tick.** Either
///    `WouldBlock` or `TimedOut` is accepted; both are read as "nothing yet",
///    retried against the silence budget, and never surfaced to the caller.
///    Any other error kind ends the request.
/// 2. **The stream stays in blocking mode.** A non-blocking stream returns a
///    tick immediately and turns the retry loop into a spin. The client paces
///    such a stream rather than burning a core, but the cost is real and the
///    read timeout stops being observed.
/// 3. **`Interrupted` means a signal arrived, nothing more.** The client
///    marks its own cancellation with a private payload, so a connector
///    forwarding a genuine `EINTR` is never mistaken for a cancel.
pub trait SetReadTimeout {
    /// # Errors
    /// Returns `std::io::Error` if the operation fails.
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()>;

    /// Bound how long a single write may block, mirroring
    /// `TcpStream::set_write_timeout`.
    ///
    /// A peer that stops reading fills the socket buffers and a write blocks
    /// inside one call. The client checks for cancellation *between* I/O
    /// calls, so without this bound there is no point at which a cancel or a
    /// shutdown can be observed: `AsyncClient::drop` waits for the peer.
    ///
    /// Like `read_timeout`, this is a per-call ceiling and not a failure
    /// threshold: an expiry is absorbed as a tick and retried against the
    /// head-silence budget, which is what decides that a write has stalled.
    ///
    /// The default does nothing, which keeps existing connectors compiling.
    /// A connector that leaves it unimplemented is declaring that its writes
    /// cannot block indefinitely; a TCP-backed one should forward this to the
    /// socket, or cancellation during an upload is unbounded.
    ///
    /// # Errors
    /// Returns `std::io::Error` if the operation fails.
    fn set_write_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        let _ = dur;
        Ok(())
    }
}

/// A network connector: opens a bidirectional stream to the given URL.
///
/// This trait is the crate's entire TLS boundary. `xibalba-client` links no
/// TLS implementation, so it has no default transport, no default crypto
/// provider, and no cargo feature that selects one. An implementor chooses the
/// stack (rustls, native-tls, plaintext, a test double) and supplies the
/// crypto provider through [`Connector::TlsConfig`].
///
/// Beware that some TLS libraries carry a process-wide default provider of
/// their own; rustls, for instance, resolves one in `ClientConfig::builder()`.
/// An implementor wanting the provider to be explicit must opt out at that
/// library's API, since this crate cannot enforce it.
pub trait Connector {
    /// The synchronous client owns its stream on the caller's thread, so it
    /// need not cross a thread boundary. `AsyncClient::connect` imposes
    /// `Send + 'static` at the only call site that moves it to a reader thread.
    /// This keeps synchronous use available to single-owner `io_uring` streams.
    type Stream: Read + Write + SetReadTimeout;

    /// Whatever the implementor's TLS stack needs to establish a session:
    /// a rustls `ClientConfig` carrying an explicit `CryptoProvider`, a
    /// native-tls connector, or `()` for plaintext transports.
    type TlsConfig;

    /// Open a connection to `url`, using `tls_config` for HTTPS.
    ///
    /// Implementations backed by TCP should enable `TCP_NODELAY` before
    /// returning. The client coalesces small request bodies, but larger
    /// bodies require separate head/body writes and otherwise remain
    /// exposed to delayed-ACK latency.
    ///
    /// # The deadline contract
    ///
    /// **An implementor must return by `deadline`, with a stream or with an
    /// error.** This is the only bound that exists on connecting. Everything
    /// else the client can interrupt happens between I/O calls on a stream it
    /// already owns; during `connect` there is no stream and no such point, so
    /// a cancel or a drop cannot be observed until this returns. An
    /// implementation that overruns is what makes `AsyncClient::drop` wait for
    /// a peer that never answers.
    ///
    /// Each step must be bounded, not just the last:
    ///
    /// - **Resolution.** `ToSocketAddrs` calls `getaddrinfo`, which takes no
    ///   timeout and can hang for the resolver's own retry schedule.
    ///   [`TcpDialer`](crate::dial::TcpDialer) checks the deadline either side
    ///   of it, which bounds *when the result is used* rather than the call
    ///   itself — the honest limit of a blocking resolver.
    /// - **Each connect attempt.** A host resolving to several addresses is
    ///   tried in turn. Giving every attempt the full budget multiplies it by
    ///   the address count, which is why this is an instant and not a
    ///   duration: [`Deadline::clamp`] shortens the last attempts rather than
    ///   restarting the clock.
    /// - **The TLS handshake**, if the implementation performs it eagerly. A
    ///   lazily negotiated session (see the `tcp-rustls` example) instead
    ///   completes inside the client's first read or write, where the silence
    ///   budgets bound it. Note which budget: the handshake blocks waiting for
    ///   the peer's records even when it is a *write* that drives it, so it is
    ///   the head-silence budget that covers it, not `write_timeout` alone.
    ///
    /// [`Deadline::never`] asks for no bound, and the client only sends it
    /// when the caller configured `connect_timeout: None`.
    ///
    /// # Errors
    /// Returns `Error` on DNS failure, TCP connect failure, or TLS handshake
    /// failure, and
    /// [`ConnectDeadlineExceeded`](xibalba_proto::error::ConnectionError::ConnectDeadlineExceeded)
    /// if `deadline` passes first.
    fn connect(
        url: &Url<'_>,
        tls_config: &Self::TlsConfig,
        deadline: Deadline,
    ) -> Result<Self::Stream, Error>;
}
