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
/// The associated `TlsConfig` lets each implementation bring its own TLS
/// strategy (rustls, native-tls, none, etc.) without the client crate knowing
/// anything about a specific library.
pub trait Connector {
    /// The synchronous client owns its stream on the caller's thread, so it
    /// need not cross a thread boundary. `AsyncClient::connect` imposes
    /// `Send + 'static` at the only call site that moves it to a reader thread.
    /// This keeps synchronous use available to single-owner `io_uring` streams.
    type Stream: Read + Write + SetReadTimeout;
    type TlsConfig;

    /// Open a connection to `url`, using `tls_config` for HTTPS.
    ///
    /// # Errors
    /// Returns `Error` on DNS failure, TCP connect failure, or TLS handshake
    /// failure.
    fn connect(url: &Url<'_>, tls_config: &Self::TlsConfig) -> Result<Self::Stream, Error>;
}
