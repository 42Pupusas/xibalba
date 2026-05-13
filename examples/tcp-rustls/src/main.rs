use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};

use xibalba_client::client::Client;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::scheme::Scheme;
use xibalba_proto::url::Url;

// --- Stream wrapper ---

enum Stream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl SetReadTimeout for Stream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(tcp) => tcp.set_read_timeout(dur),
            Self::Tls(tls) => tls.get_ref().set_read_timeout(dur),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(tcp) => tcp.read(buf),
            Self::Tls(tls) => tls.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(tcp) => tcp.write(buf),
            Self::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(tcp) => tcp.flush(),
            Self::Tls(tls) => tls.flush(),
        }
    }
}

// --- Connector impl ---

struct TcpConnector;

impl Connector for TcpConnector {
    type Stream = Stream;
    type TlsConfig = Arc<ClientConfig>;

    fn connect(url: &Url<'_>, tls_config: &Arc<ClientConfig>) -> Result<Stream, Error> {
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
                let conn = ClientConnection::new(Arc::clone(tls_config), server_name)
                    .map_err(|e| xibalba_proto::error::TlsError { message: e.to_string() })?;
                Ok(Stream::Tls(Box::new(StreamOwned::new(conn, tcp))))
            }
            Scheme::Http => Ok(Stream::Plain(tcp)),
        }
    }
}

// --- TLS config builder ---

fn build_tls_config() -> Result<Arc<ClientConfig>, Error> {
    let provider = rustls::crypto::ring::default_provider();
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| xibalba_proto::error::TlsError { message: e.to_string() })?
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

// --- main ---

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tls_config = build_tls_config()?;
    let mut client = Client::<TcpConnector>::connect_default(b"https://httpbin.org/get", tls_config)?;
    let response = client.get(b"/get")?;

    println!("Status: {}", response.status);
    println!("Headers:");
    for (name, value) in response.headers() {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }
    println!("\nBody:\n{}", response.text()?);
    Ok(())
}
