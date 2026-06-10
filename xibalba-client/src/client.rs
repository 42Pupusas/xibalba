use std::io::Write;
use std::time::Duration;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::status::StatusCode;
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

use crate::body::{
    BodyReader, HEAD_BUF_SIZE, HeadData, StreamingBody, read_body, read_response_head,
};
use crate::connector::Connector;

// ── Config ───────────────────────────────────────────────────────────────────

pub struct Config {
    pub read_timeout: Option<Duration>,
    pub max_response_body: usize,
    pub max_head_size: usize,
    pub max_redirects: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            read_timeout: Some(Duration::from_secs(30)),
            max_response_body: 10 * 1024 * 1024,
            max_head_size: 16 * 1024,
            max_redirects: 10,
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Response {
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: BodyReader,
}

/// A response whose body is decoded incrementally from the live
/// connection. Produced by [`RequestBuilder::send_streaming`].
///
/// Borrows the client mutably until dropped. Reading the body to
/// completion leaves the connection reusable; dropping early marks it
/// dirty so the next request reconnects.
#[derive(Debug)]
pub struct StreamingResponse<'a, S: std::io::Read> {
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: StreamingBody<'a, S>,
}

impl<S: std::io::Read> StreamingResponse<'_, S> {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }
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
        String::from_utf8(buf).map_err(|_| Error::from(ConnectionError::InvalidUtf8Body))
    }
}

// ── RequestBuilder ───────────────────────────────────────────────────────────

pub struct RequestBuilder<'a, C: Connector> {
    client: &'a mut Client<C>,
    method: Method,
    path: &'a [u8],
    query: Option<&'a [u8]>,
    body: Option<&'a [u8]>,
    extra_headers: Vec<(&'a [u8], &'a [u8])>,
}

impl<'a, C: Connector> RequestBuilder<'a, C> {
    #[must_use]
    pub fn header(mut self, name: &'a [u8], value: &'a [u8]) -> Self {
        self.extra_headers.push((name, value));
        self
    }

    #[must_use]
    pub const fn body(mut self, data: &'a [u8]) -> Self {
        self.body = Some(data);
        self
    }

    #[must_use]
    pub const fn query(mut self, q: &'a [u8]) -> Self {
        self.query = Some(q);
        self
    }

    /// # Errors
    ///
    /// Returns `Error` on serialization failure, connection error, or if
    /// the redirect limit is exceeded.
    pub fn send(self) -> Result<Response, Error> {
        let extra: Vec<Header<'_>> = self
            .extra_headers
            .iter()
            .map(|(n, v)| Header {
                name: HeaderName::from_bytes(n),
                value: v,
            })
            .collect();
        self.client
            .execute(self.method, self.path, self.query, self.body, &extra)
    }

    /// Like [`send`](Self::send), but the response body is decoded
    /// incrementally as it is read. See [`Client::execute_streaming`].
    ///
    /// # Errors
    ///
    /// Returns `Error` on serialization or connection failure.
    pub fn send_streaming(self) -> Result<StreamingResponse<'a, C::Stream>, Error> {
        let extra: Vec<Header<'_>> = self
            .extra_headers
            .iter()
            .map(|(n, v)| Header {
                name: HeaderName::from_bytes(n),
                value: v,
            })
            .collect();
        self.client
            .execute_streaming(self.method, self.path, self.query, self.body, &extra)
    }
}

// ── Client ───────────────────────────────────────────────────────────────────

pub struct Client<C: Connector> {
    tls_config: C::TlsConfig,
    stream: C::Stream,
    config: Config,
    host: Vec<u8>,
    port: u16,
    scheme: xibalba_proto::scheme::Scheme,
    write_buf: Vec<u8>,
    head_buf: Vec<u8>,
    /// Set while a streaming response is in flight; stays set if the
    /// reader is dropped before the body is fully consumed. The next
    /// request reconnects instead of reading a stale body.
    dirty: bool,
}

impl<C: Connector> Client<C> {
    /// # Errors
    ///
    /// Returns `Error` on connection failure.
    pub fn connect(
        url_bytes: &[u8],
        tls_config: C::TlsConfig,
        config: Config,
    ) -> Result<Self, Error> {
        let url = Url::parse(url_bytes)?;
        let stream = C::connect(&url, &tls_config)?;
        let host = url.host.to_vec();
        let port = url.effective_port();
        let scheme = url.scheme;

        let client = Self {
            tls_config,
            stream,
            config,
            host,
            port,
            scheme,
            write_buf: Vec::with_capacity(512),
            head_buf: Vec::with_capacity(HEAD_BUF_SIZE),
            dirty: false,
        };
        client.apply_timeouts()?;
        Ok(client)
    }

    /// # Errors
    ///
    /// Returns `Error` on connection failure.
    pub fn connect_default(url_bytes: &[u8], tls_config: C::TlsConfig) -> Result<Self, Error> {
        Self::connect(url_bytes, tls_config, Config::default())
    }

    /// Start building a request. Chain `.header()`, `.body()`, `.query()`,
    /// then call `.send()`.
    pub const fn build<'a>(&'a mut self, method: Method, path: &'a [u8]) -> RequestBuilder<'a, C> {
        RequestBuilder {
            client: self,
            method,
            path,
            query: None,
            body: None,
            extra_headers: Vec::new(),
        }
    }

    /// # Errors
    ///
    /// See [`RequestBuilder::send`].
    pub fn request(
        &mut self,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
        body: Option<&[u8]>,
    ) -> Result<Response, Error> {
        self.execute(method, path, query, body, &[])
    }

    /// # Errors
    ///
    /// See [`request`](Self::request).
    pub fn get(&mut self, path: &[u8]) -> Result<Response, Error> {
        self.execute(Method::Get, path, None, None, &[])
    }

    /// # Errors
    ///
    /// See [`request`](Self::request).
    pub fn post(&mut self, path: &[u8], body: &[u8]) -> Result<Response, Error> {
        self.execute(Method::Post, path, None, Some(body), &[])
    }

    // ── internals ────────────────────────────────────────────────────────────

    fn apply_timeouts(&self) -> Result<(), Error> {
        use crate::connector::SetReadTimeout;
        self.stream.set_read_timeout(self.config.read_timeout)?;
        Ok(())
    }

    /// Reconnect to the current host if a previous streaming response
    /// was dropped before its body was fully consumed.
    fn ensure_clean(&mut self) -> Result<(), Error> {
        if !self.dirty {
            return Ok(());
        }
        let host = self.host.clone();
        let url = Url {
            scheme: self.scheme,
            host: &host,
            port: Some(self.port),
            path: b"/",
            query: None,
            fragment: None,
        };
        self.reconnect(&url)?;
        self.dirty = false;
        Ok(())
    }

    fn reconnect(&mut self, url: &Url<'_>) -> Result<(), Error> {
        self.stream = C::connect(url, &self.tls_config)?;
        self.host = url.host.to_vec();
        self.port = url.effective_port();
        self.scheme = url.scheme;
        self.apply_timeouts()?;
        Ok(())
    }

    fn host_header_value(&self) -> Vec<u8> {
        let default_port = self.scheme.default_port();
        if self.port == default_port {
            self.host.clone()
        } else {
            let mut val = self.host.clone();
            val.push(b':');
            val.extend_from_slice(self.port.to_string().as_bytes());
            val
        }
    }

    /// Write one request and read the response head, leaving the body
    /// unread on the stream. Returns the head, its framing, and the
    /// offset of the body's first byte within `self.head_buf`.
    fn send_head(
        &mut self,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
        body: Option<&[u8]>,
        extra_headers: &[Header<'_>],
    ) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
        let host_value = self.host_header_value();
        let content_len_str;
        let mut headers = Vec::with_capacity(1 + extra_headers.len() + 1);

        headers.push(Header {
            name: HeaderName::Host,
            value: &host_value,
        });
        headers.extend_from_slice(extra_headers);

        if let Some(data) = body {
            content_len_str = data.len().to_string();
            headers.push(Header {
                name: HeaderName::ContentLength,
                value: content_len_str.as_bytes(),
            });
        }

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
        if let Some(data) = body {
            self.stream.write_all(data)?;
        }
        self.stream.flush()?;

        let (head_data, framing, tail_offset) = read_response_head(
            &mut self.stream,
            &mut self.head_buf,
            self.config.max_head_size,
        )?;

        Ok((head_data, framing, tail_offset))
    }

    fn send_one(
        &mut self,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
        body: Option<&[u8]>,
        extra_headers: &[Header<'_>],
    ) -> Result<Response, Error> {
        let (head_data, framing, tail_offset) =
            self.send_head(method, path, query, body, extra_headers)?;

        let body_data = read_body(
            &mut self.stream,
            &framing,
            &self.head_buf[tail_offset..],
            self.config.max_response_body,
        )?;

        Ok(Response {
            version: head_data.version,
            status: head_data.status,
            head: head_data,
            body: BodyReader::new(body_data),
        })
    }

    /// Send one request and return a response whose body is decoded
    /// incrementally as the caller reads it. Required for server-sent
    /// events, where the response only ends when the server is done.
    ///
    /// Redirects are NOT followed. The response borrows the client
    /// until dropped; see [`StreamingResponse`] for drop semantics.
    /// `max_response_body` is not enforced — the caller bounds its own
    /// consumption.
    ///
    /// # Errors
    ///
    /// Returns `Error` on serialization or connection failure.
    pub fn execute_streaming(
        &mut self,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
        body: Option<&[u8]>,
        extra_headers: &[Header<'_>],
    ) -> Result<StreamingResponse<'_, C::Stream>, Error> {
        self.ensure_clean()?;
        let (head_data, framing, tail_offset) =
            self.send_head(method, path, query, body, extra_headers)?;

        let tail = self.head_buf[tail_offset..].to_vec();
        self.dirty = true;
        let body = StreamingBody::new(&mut self.stream, &mut self.dirty, &framing, tail);

        Ok(StreamingResponse {
            version: head_data.version,
            status: head_data.status,
            head: head_data,
            body,
        })
    }

    fn execute(
        &mut self,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
        body: Option<&[u8]>,
        extra_headers: &[Header<'_>],
    ) -> Result<Response, Error> {
        self.ensure_clean()?;

        let mut current_method = method;
        let mut current_path: Vec<u8> = path.to_vec();
        let mut current_query: Option<Vec<u8>> = query.map(<[u8]>::to_vec);
        let mut current_body = body.map(<[u8]>::to_vec);

        for _ in 0..=self.config.max_redirects {
            let resp = self.send_one(
                current_method,
                &current_path,
                current_query.as_deref(),
                current_body.as_deref(),
                extra_headers,
            )?;

            if !resp.status.is_redirect() {
                return Ok(resp);
            }

            let location = resp
                .headers()
                .find(|(name, _)| xibalba_proto::header::ascii_eq_ignore_case(name, b"Location"))
                .map(|(_, v)| v);

            let location = match location {
                Some(loc) => loc.to_vec(),
                None => return Ok(resp),
            };

            match resp.status {
                StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER => {
                    current_method = Method::Get;
                    current_body = None;
                }
                StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT => {}
                _ => return Ok(resp),
            }

            if location.starts_with(b"http://") || location.starts_with(b"https://") {
                let url = Url::parse(&location)?;
                let target_port = url.effective_port();
                let same_origin = url.host == &self.host[..]
                    && target_port == self.port
                    && url.scheme == self.scheme;
                if !same_origin {
                    self.reconnect(&url)?;
                }
                current_path = if url.path.is_empty() {
                    b"/".to_vec()
                } else {
                    url.path.to_vec()
                };
                current_query = url.query.map(<[u8]>::to_vec);
            } else {
                let (path_part, query_part) = location
                    .iter()
                    .position(|&b| b == b'?')
                    .map_or((location.as_slice(), None), |pos| {
                        (&location[..pos], Some(location[pos + 1..].to_vec()))
                    });
                current_path = if path_part.is_empty() {
                    b"/".to_vec()
                } else {
                    path_part.to_vec()
                };
                current_query = query_part;
            }
        }

        Err(ConnectionError::TooManyRedirects.into())
    }
}
