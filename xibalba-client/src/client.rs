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

// ── Redirect state ───────────────────────────────────────────────────────────

#[derive(Debug)]
struct RedirectState<'a> {
    method: Method,
    path: Vec<u8>,
    query: Option<Vec<u8>>,
    body: Option<Vec<u8>>,
    extra_headers: &'a [Header<'a>],
}

impl<'a> RedirectState<'a> {
    fn new(params: &'a RequestParams<'a>) -> Self {
        Self {
            method: params.method,
            path: params.path.to_vec(),
            query: params.query.map(<[u8]>::to_vec),
            body: params.body.map(<[u8]>::to_vec),
            extra_headers: params.extra_headers.as_slice(),
        }
    }

    fn to_params(&self) -> RequestParams<'_> {
        RequestParams {
            method: self.method,
            path: &self.path,
            query: self.query.as_deref(),
            body: self.body.as_deref(),
            extra_headers: self.extra_headers.to_vec(),
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
/// connection. Produced by [`Client::send_streaming`].
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

// ── Request params ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct RequestParams<'a> {
    pub(crate) method: Method,
    pub(crate) path: &'a [u8],
    pub(crate) query: Option<&'a [u8]>,
    pub(crate) body: Option<&'a [u8]>,
    pub(crate) extra_headers: Vec<Header<'a>>,
}

// ── RequestBuilder ───────────────────────────────────────────────────────────

/// Collects request parameters for deferred execution.
///
/// Created by [`Client::build`]; consumed by [`Client::send`] or
/// [`Client::send_streaming`].  The builder borrows only the request
/// data (path, headers, body), never the client.
pub struct RequestBuilder<'a> {
    method: Method,
    path: &'a [u8],
    query: Option<&'a [u8]>,
    body: Option<&'a [u8]>,
    extra_headers: Vec<(&'a [u8], &'a [u8])>,
}

impl<'a> RequestBuilder<'a> {
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

    /// Execute this request on `client` and return the full response.
    ///
    /// # Errors
    ///
    /// Returns `Error` on serialization or connection failure.
    pub fn send<C: Connector>(self, client: &mut Client<C>) -> Result<Response, Error> {
        client.execute(&self.into_params())
    }

    /// Materialize the collected parameters into [`RequestParams`].
    pub(crate) fn into_params(self) -> RequestParams<'a> {
        let extra_headers = self
            .extra_headers
            .iter()
            .map(|(n, v)| Header {
                name: HeaderName::from_bytes(n),
                value: v,
            })
            .collect();
        RequestParams {
            method: self.method,
            path: self.path,
            query: self.query,
            body: self.body,
            extra_headers,
        }
    }
}

// ── Client ───────────────────────────────────────────────────────────────────

pub struct Client<C: Connector> {
    tls_config: C::TlsConfig,
    pub(crate) stream: C::Stream,
    pub(crate) config: Config,
    host: Vec<u8>,
    port: u16,
    scheme: xibalba_proto::scheme::Scheme,
    write_buf: Vec<u8>,
    pub(crate) head_buf: Vec<u8>,
    /// Set while a streaming response is in flight; stays set if the
    /// reader is dropped before the body is fully consumed. The next
    /// request reconnects instead of reading a stale body.
    pub(crate) dirty: bool,
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

    /// Start building a request.  Chain `.header()`, `.body()`,
    /// `.query()`, then call [`Client::send`] or
    /// [`Client::send_streaming`].
    #[must_use]
    pub const fn build<'a>(&self, method: Method, path: &'a [u8]) -> RequestBuilder<'a> {
        RequestBuilder {
            method,
            path,
            query: None,
            body: None,
            extra_headers: Vec::new(),
        }
    }

    /// Execute a fully-buffered request built with [`Client::build`].
    /// Follows redirects up to `Config::max_redirects`.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure, serialization error,
    /// or too many redirects.
    pub fn send(&mut self, builder: RequestBuilder<'_>) -> Result<Response, Error> {
        let params = builder.into_params();
        self.execute(&params)
    }

    /// Execute a streaming request built with [`Client::build`].
    /// The response body is decoded incrementally as the caller reads
    /// it. Redirects are NOT followed.
    ///
    /// Borrows the client mutably until the `StreamingResponse` is
    /// dropped; see [`StreamingResponse`] for drop semantics.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure or serialization error.
    pub fn send_streaming(
        &mut self,
        builder: RequestBuilder<'_>,
    ) -> Result<StreamingResponse<'_, C::Stream>, Error> {
        let params = builder.into_params();
        self.execute_streaming(&params)
    }

    /// # Errors
    ///
    /// See [`send`](Self::send).
    pub fn request(
        &mut self,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
        body: Option<&[u8]>,
    ) -> Result<Response, Error> {
        let params = RequestParams {
            method,
            path,
            query,
            body,
            extra_headers: Vec::new(),
        };
        self.execute(&params)
    }

    /// # Errors
    ///
    /// See [`request`](Self::request).
    pub fn get(&mut self, path: &[u8]) -> Result<Response, Error> {
        self.request(Method::Get, path, None, None)
    }

    /// # Errors
    ///
    /// See [`request`](Self::request).
    pub fn post(&mut self, path: &[u8], body: &[u8]) -> Result<Response, Error> {
        self.request(Method::Post, path, None, Some(body))
    }

    // ── internals ────────────────────────────────────────────────────────────

    fn apply_timeouts(&self) -> Result<(), Error> {
        use crate::connector::SetReadTimeout;
        self.stream.set_read_timeout(self.config.read_timeout)?;
        Ok(())
    }

    /// Reconnect to the current host if a previous streaming response
    /// was dropped before its body was fully consumed.
    pub(crate) fn ensure_clean(&mut self) -> Result<(), Error> {
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

    /// Reconnect to the current host (used to recover a stale
    /// keep-alive connection without re-parsing a URL).
    fn reconnect_same_host(&mut self) -> Result<(), Error> {
        let host = self.host.clone();
        let url = Url {
            scheme: self.scheme,
            host: &host,
            port: Some(self.port),
            path: b"/",
            query: None,
            fragment: None,
        };
        self.reconnect(&url)
    }

    /// Whether `err` is the signature of a server-closed idle keep-alive
    /// connection: a transport-level failure (TLS/TCP EOF, reset, broken
    /// pipe) or our own `ConnectionClosed`. Safe to retry only because
    /// we check it *before* any response bytes reached the caller.
    const fn is_stale_connection(err: &Error) -> bool {
        use std::io::ErrorKind;
        match err {
            Error::Connection(ConnectionError::ConnectionClosed) => true,
            Error::Io(io) => matches!(
                io.kind,
                ErrorKind::UnexpectedEof
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
            ),
            _ => false,
        }
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

    /// Write one request and read the response head, reconnecting and
    /// retrying once if the connection fails at the transport level
    /// (the stale-keep-alive case — the server dropped an idle
    /// connection). The connection can go stale before its *first*
    /// request too: hosts that connect at startup and send the first
    /// request much later hit exactly that. Retry is safe here: the
    /// failure is detected before any response byte reaches the caller,
    /// so the request was never processed.
    ///
    /// `pub(crate)` so [`crate::async_client::AsyncClient`]'s reader
    /// thread can submit requests without going through
    /// `RequestBuilder`.
    pub(crate) fn send_head(
        &mut self,
        params: &RequestParams<'_>,
    ) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
        match self.send_head_once(params) {
            Ok(head) => Ok(head),
            Err(e) if Self::is_stale_connection(&e) => {
                self.reconnect_same_host()?;
                self.send_head_once(params)
            }
            Err(e) => Err(e),
        }
    }

    /// Write one request and read the response head, leaving the body
    /// unread on the stream. Returns the head, its framing, and the
    /// offset of the body's first byte within `self.head_buf`.
    pub(crate) fn send_head_once(
        &mut self,
        params: &RequestParams<'_>,
    ) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
        let host_value = self.host_header_value();
        let content_len_str;
        let mut headers = Vec::with_capacity(1 + params.extra_headers.len() + 1);

        headers.push(Header {
            name: HeaderName::Host,
            value: &host_value,
        });
        headers.extend_from_slice(&params.extra_headers);

        if let Some(data) = params.body {
            content_len_str = data.len().to_string();
            headers.push(Header {
                name: HeaderName::ContentLength,
                value: content_len_str.as_bytes(),
            });
        }

        let req = Request {
            method: params.method,
            path: params.path,
            query: params.query,
            version: Version::Http11,
            headers: &headers,
        };
        self.write_buf.clear();
        req.serialize_to_writer(&mut self.write_buf)?;

        self.stream.write_all(&self.write_buf)?;
        if let Some(data) = params.body {
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

    fn send_one(&mut self, params: &RequestParams<'_>) -> Result<Response, Error> {
        let (head_data, framing, tail_offset) = self.send_head(params)?;

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
    pub(crate) fn execute_streaming(
        &mut self,
        params: &RequestParams<'_>,
    ) -> Result<StreamingResponse<'_, C::Stream>, Error> {
        self.ensure_clean()?;
        let (head_data, framing, tail_offset) = self.send_head(params)?;

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

    fn execute(&mut self, params: &RequestParams<'_>) -> Result<Response, Error> {
        self.ensure_clean()?;

        let mut current = RedirectState::new(params);

        for _ in 0..=self.config.max_redirects {
            let resp = self.send_one(&current.to_params())?;

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

            if !Self::is_redirect_method_preserving(resp.status) {
                current.method = Method::Get;
                current.body = None;
            }

            self.apply_redirect_location(&location, &mut current)?;
        }

        Err(ConnectionError::TooManyRedirects.into())
    }

    const fn is_redirect_method_preserving(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
        )
    }

    fn apply_redirect_location(
        &mut self,
        location: &[u8],
        current: &mut RedirectState<'_>,
    ) -> Result<(), Error> {
        if location.starts_with(b"http://") || location.starts_with(b"https://") {
            let url = Url::parse(location)?;
            let target_port = url.effective_port();
            let same_origin =
                url.host == &self.host[..] && target_port == self.port && url.scheme == self.scheme;
            if !same_origin {
                self.reconnect(&url)?;
            }
            current.path = normalize_path(url.path);
            current.query = url.query.map(<[u8]>::to_vec);
        } else {
            let (path_part, query_part) = location
                .iter()
                .position(|&b| b == b'?')
                .map_or((location, None), |pos| {
                    (&location[..pos], Some(location[pos + 1..].to_vec()))
                });
            current.path = normalize_path(path_part);
            current.query = query_part;
        }
        Ok(())
    }
}

fn normalize_path(path: &[u8]) -> Vec<u8> {
    if path.is_empty() {
        b"/".to_vec()
    } else {
        path.to_vec()
    }
}
