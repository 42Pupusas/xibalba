use std::io::{Read, Write};
use std::time::Duration;

use xibalba_proto::error::Error;
use xibalba_proto::url::Url;

/// Abstraction over anything that can set a read timeout.
/// Mirrors `TcpStream::set_read_timeout`.
pub trait SetReadTimeout {
    /// # Errors
    /// Returns `std::io::Error` if the operation fails.
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()>;
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
    /// # Errors
    /// Returns `Error` on DNS failure, TCP connect failure, or TLS handshake
    /// failure.
    fn connect(url: &Url<'_>, tls_config: &Self::TlsConfig) -> Result<Self::Stream, Error>;
}
