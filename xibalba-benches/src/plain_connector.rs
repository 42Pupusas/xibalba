use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::url::Url;

/// A bare-TCP [`Connector`] for benches and examples that never need TLS.
pub struct PlainConnector;

/// The [`PlainConnector`]'s stream: a thin `TcpStream` wrapper satisfying
/// [`Connector::Stream`]'s <code>Read + Write + [SetReadTimeout]</code> bound.
pub struct PlainStream(TcpStream);

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

impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        self.0.set_read_timeout(dur)
    }
}

impl Connector for PlainConnector {
    type Stream = PlainStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, (): &()) -> Result<Self::Stream, Error> {
        let host = std::str::from_utf8(url.host).map_err(|_| {
            Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into()))
        })?;
        let stream = TcpStream::connect(format!("{}:{}", host, url.effective_port()))?;
        Ok(PlainStream(stream))
    }
}
