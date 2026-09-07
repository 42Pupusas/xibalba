use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rustls::{ClientConnection, StreamOwned};

use xibalba_client::connector::SetReadTimeout;

/// Either half of what [`TcpConnector`](crate::connector::TcpConnector)
/// returns: a plaintext socket for `http`, a rustls session for `https`.
pub enum Stream {
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

    /// Forwarded to the socket beneath the TLS session, not only the plain
    /// one. A peer that stops reading blocks a write inside a single call,
    /// and for a lazily negotiated session that call may be driving the
    /// handshake rather than sending a request.
    fn set_write_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(tcp) => tcp.set_write_timeout(dur),
            Self::Tls(tls) => tls.get_ref().set_write_timeout(dur),
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
