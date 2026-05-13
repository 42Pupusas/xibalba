use std::io::Write;

use xibalba_proto::error::Error;
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

use crate::body::{BodyReader, HeadData, HEAD_BUF_SIZE, read_body, read_response_head};
use crate::connector::Connector;

pub struct Client<C: Connector> {
    _tls_config: C::TlsConfig,
    stream: C::Stream,
    write_buf: Vec<u8>,
    head_buf: Vec<u8>,
}

pub struct Response {
    pub version: Version,
    pub status: xibalba_proto::status::StatusCode,
    pub head: HeadData,
    pub body: BodyReader,
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
        use std::io::Read;
        let mut buf = Vec::new();
        self.body.read_to_end(&mut buf).map_err(Error::from)?;
        String::from_utf8(buf)
            .map_err(|e| Error::Connection(format!("response body is not valid UTF-8: {e}")))
    }
}

impl<C: Connector> Client<C> {
    /// # Errors
    ///
    /// Returns `Error` on connection failure.
    pub fn connect(url_bytes: &[u8], tls_config: C::TlsConfig) -> Result<Self, Error> {
        let url = Url::parse(url_bytes)?;
        let stream = C::connect(&url, &tls_config)?;
        Ok(Self {
            _tls_config: tls_config,
            stream,
            write_buf: Vec::with_capacity(512),
            head_buf: Vec::with_capacity(HEAD_BUF_SIZE),
        })
    }

    /// # Errors
    ///
    /// Returns `Error` on serialization failure or connection error.
    pub fn request(&mut self, method: Method, path: &[u8], query: Option<&[u8]>) -> Result<Response, Error> {
        let headers = [
            Header { name: HeaderName::Connection, value: b"keep-alive" },
            Header { name: HeaderName::UserAgent, value: b"xibalba/0.1" },
        ];
        let req = Request {
            method,
            path,
            query,
            version: Version::Http11,
            headers: &headers,
        };
        self.write_buf.clear();
        req.serialize_to_writer(&mut self.write_buf)?;

        self.stream.write_all(&self.write_buf)?;
        self.stream.flush()?;

        let (head_data, framing, tail_offset) = read_response_head(&mut self.stream, &mut self.head_buf)?;
        let body_data = read_body(&mut self.stream, &framing, &self.head_buf[tail_offset..])?;

        Ok(Response {
            version: head_data.version,
            status: head_data.status,
            head: head_data,
            body: BodyReader::new(body_data),
        })
    }

    /// # Errors
    ///
    /// See [`request`](Self::request).
    pub fn get(&mut self, path: &[u8]) -> Result<Response, Error> {
        self.request(Method::Get, path, None)
    }
}
