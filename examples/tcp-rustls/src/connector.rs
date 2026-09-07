use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};

use xibalba_client::Deadline;
use xibalba_client::connector::Connector;
use xibalba_client::dial::TcpDialer;
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::scheme::Scheme;
use xibalba_proto::url::Url;

use crate::stream::Stream;

pub struct TcpConnector;

impl Connector for TcpConnector {
    type Stream = Stream;
    type TlsConfig = Arc<ClientConfig>;

    /// TLS is negotiated lazily: `ClientConnection::new` only prepares the
    /// handshake, so a returned `Stream::Tls` is connected but not yet
    /// verified. The handshake completes inside the first read or write,
    /// which is where a certificate rejection surfaces, and where the read
    /// and write timeouts the client sets are the bound on a stalled peer.
    ///
    /// That laziness is what satisfies the deadline contract here: the only
    /// blocking work this method does is the dial, which takes the deadline
    /// directly. A connector completing the handshake eagerly would have to
    /// bound it too, by setting the socket's read and write timeouts from
    /// [`Deadline::clamp`] before driving the handshake.
    ///
    /// `tests/stalled_handshake.rs` holds the deferred bound to its claim
    /// against a server that accepts the TCP connection and then sends
    /// nothing.
    fn connect(
        url: &Url<'_>,
        tls_config: &Arc<ClientConfig>,
        deadline: Deadline,
    ) -> Result<Stream, Error> {
        let tcp = TcpDialer::default().dial(url, deadline)?;

        match url.scheme {
            Scheme::Https => {
                let host_str = std::str::from_utf8(url.connection_host()).map_err(|_| {
                    Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into()))
                })?;
                let server_name = ServerName::try_from(host_str.to_owned()).map_err(|e| {
                    Error::Connection(ConnectionError::Other(format!("invalid server name: {e}")))
                })?;
                let conn =
                    ClientConnection::new(Arc::clone(tls_config), server_name).map_err(|e| {
                        xibalba_proto::error::TlsError {
                            message: e.to_string(),
                        }
                    })?;
                Ok(Stream::Tls(Box::new(StreamOwned::new(conn, tcp))))
            }
            Scheme::Http => Ok(Stream::Plain(tcp)),
        }
    }
}
