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
    type Stream: Read + Write + SetReadTimeout + Send + 'static;
    type TlsConfig;

    /// Open a connection to `url`, using `tls_config` for HTTPS.
    ///
    /// # Errors
    /// Returns `Error` on DNS failure, TCP connect failure, or TLS handshake
    /// failure.
    fn connect(url: &Url<'_>, tls_config: &Self::TlsConfig) -> Result<Self::Stream, Error>;
}
