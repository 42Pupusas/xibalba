//! A cleartext-only [`Connector`], for `http://` URLs.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use xibalba_proto::error::Error;
use xibalba_proto::url::Url;

use crate::connector::{Connector, SetReadTimeout};
use crate::deadline::Deadline;
use crate::dial::TcpDialer;

/// A [`Connector`] that speaks TCP and never TLS.
///
/// Provided so that plaintext use — a loopback test server, a sidecar on
/// localhost, a plain-HTTP service inside a trusted network — does not require
/// writing a connector, and so this crate's own examples and tests share one
/// implementation rather than repeating it.
///
/// It refuses `https://` URLs rather than connecting in the clear; see
/// [`TcpDialer::dial_plaintext`]. For HTTPS, supply a connector wrapping the
/// TLS stack your application chooses — this crate links none.
pub struct PlainConnector;

/// The [`TcpStream`] returned by [`PlainConnector`], carrying the timeout
/// controls the client needs to bound a silent peer.
#[derive(Debug)]
pub struct PlainStream(TcpStream);

impl PlainStream {
    #[must_use]
    pub const fn get_ref(&self) -> &TcpStream {
        &self.0
    }
}

impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        self.0.set_read_timeout(dur)
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        self.0.set_write_timeout(dur)
    }
}

impl Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl Connector for PlainConnector {
    type Stream = PlainStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, _tls_config: &(), deadline: Deadline) -> Result<PlainStream, Error> {
        TcpDialer::default()
            .dial_plaintext(url, deadline)
            .map(PlainStream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xibalba_proto::error::ConnectionError;

    #[test]
    fn an_https_url_is_refused_rather_than_sent_in_the_clear() {
        let url = Url::parse(b"https://example.com/").expect("parse");
        let err = PlainConnector::connect(&url, &(), Deadline::never())
            .expect_err("https must be refused");
        assert!(matches!(
            err,
            Error::Connection(ConnectionError::PlaintextConnectorForHttps)
        ));
    }
}
