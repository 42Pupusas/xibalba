use std::io::Read;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

use super::head::HeadData;

/// A fully-buffered response: body already read off the wire.
#[derive(Debug)]
pub struct Response {
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: crate::body::BodyReader,
}

impl Response {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }

    /// # Errors
    ///
    /// Returns `Error::Io` on read failure, or `Error::Connection` if the
    /// body is not valid UTF-8.
    pub fn text(mut self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.body.read_to_end(&mut buf).map_err(Error::from)?;
        String::from_utf8(buf).map_err(|_| Error::from(ConnectionError::InvalidUtf8Body))
    }
}

/// A response whose body is decoded incrementally from the live
/// connection. Produced by [`crate::client::Client::send_streaming`].
///
/// Borrows the client mutably until dropped. Reading the body to
/// completion leaves the connection reusable; dropping early marks it
/// dirty so the next request reconnects.
#[derive(Debug)]
pub struct StreamingResponse<'a, S: Read> {
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: crate::body::StreamingBody<'a, S>,
}

impl<S: Read> StreamingResponse<'_, S> {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }
}
