use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};

use crate::error::{ConnectionError, Error};
use crate::scheme::Scheme;
use crate::stream::Stream;
use crate::url::Url;

/// # Errors
///
/// Returns `Error` on DNS failure, TCP connect failure, or TLS handshake failure.
pub fn connect(url: &Url<'_>, tls_config: &Arc<ClientConfig>) -> Result<Stream, Error> {
    let host_str = std::str::from_utf8(url.host)
        .map_err(|_| Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into())))?;

    let port = url.effective_port();
    let addr = (host_str, port)
        .to_socket_addrs()
        .map_err(|e| Error::Connection(ConnectionError::Other(format!("DNS resolution failed: {e}"))))?
        .next()
        .ok_or_else(|| Error::Connection(ConnectionError::Other("DNS returned no addresses".into())))?;

    let tcp = TcpStream::connect(addr)?;

    match url.scheme {
        Scheme::Https => {
            let server_name = ServerName::try_from(host_str.to_owned())
                .map_err(|e| Error::Connection(ConnectionError::Other(format!("invalid server name: {e}"))))?;
            let conn = ClientConnection::new(Arc::clone(tls_config), server_name)?;
            let tls_stream = StreamOwned::new(conn, tcp);
            Ok(Stream::Tls(Box::new(tls_stream)))
        }
        Scheme::Http => Ok(Stream::Plain(tcp)),
    }
}
