use std::io::Write;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

use crate::body::{BodyCollector, BodyReader, StreamingBody};
use crate::config::HEAD_BUF_SIZE;
use crate::connector::{Connector, SetReadTimeout};
use crate::params::RequestParams;
use crate::redirect::RedirectState;
use crate::response::HeadData;

pub use crate::config::{Config, DEFAULT_MAX_HEAD_SIZE};
pub use crate::params::RequestBuilder;
pub use crate::response::{Response, StreamingResponse};

/// A blocking HTTP/1.1 client over one connection.
///
/// Requests go through [`Client::build`] (fluent) or the one-shot
/// [`Client::request`]/[`Client::get`]/[`Client::post`] helpers. A
/// keep-alive connection that dies between requests is reconnected and
/// the request retried once before any response byte is seen.
pub struct Client<C: Connector, const MAX_HEAD_SIZE: usize = DEFAULT_MAX_HEAD_SIZE> {
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

impl<C: Connector, const MAX_HEAD_SIZE: usize> Client<C, MAX_HEAD_SIZE> {
    /// # Errors
    ///
    /// Returns `Error` on connection failure.
    pub fn connect(
        url_bytes: &[u8],
        tls_config: C::TlsConfig,
        config: Config,
    ) -> Result<Self, Error> {
        config.validate()?;
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
    /// `.query()`, then call [`RequestBuilder::send`] or
    /// [`Client::send_streaming`].
    #[must_use]
    pub const fn build<'a>(&self, method: Method, path: &'a [u8]) -> RequestBuilder<'a> {
        RequestBuilder::new(method, path)
    }

    /// Execute a fully-buffered request built with [`Client::build`].
    /// Follows redirects up to the configured `max_redirects` value.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure, serialization error,
    /// or too many redirects.
    pub fn send(&mut self, builder: RequestBuilder<'_>) -> Result<Response, Error> {
        self.execute(&builder.into_params()?)
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
        self.execute_streaming(&builder.into_params()?)
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
        self.execute(&RequestParams {
            method,
            path,
            query,
            body,
            extra_headers: Vec::new(),
        })
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
        self.stream.set_read_timeout(self.config.read_timeout)?;
        Ok(())
    }

    /// Reconnect to the current host if a previous streaming response
    /// was dropped before its body was fully consumed.
    pub(crate) fn ensure_clean(&mut self) -> Result<(), Error> {
        if !self.dirty {
            return Ok(());
        }
        self.reconnect_same_host()
    }

    pub(crate) fn reconnect(&mut self, url: &Url<'_>) -> Result<(), Error> {
        self.stream = C::connect(url, &self.tls_config)?;
        self.host = url.host.to_vec();
        self.port = url.effective_port();
        self.scheme = url.scheme;
        self.apply_timeouts()?;
        self.dirty = false;
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

    /// Mark the live connection unusable after a response head could not be
    /// fully read. The next request must reconnect: unread header/body bytes
    /// would otherwise be interpreted as a new status line.
    pub(crate) const fn discard_partial_response(&mut self) {
        self.dirty = true;
    }

    /// Declare the connection safe to reuse for the next request. Only
    /// valid once this exchange has reached a point where no unread bytes
    /// of it remain addressed to a later request.
    const fn mark_reusable(&mut self) {
        self.dirty = false;
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

    /// Whether `url` points at the origin this client is connected to.
    pub(crate) fn is_same_origin(&self, url: &Url<'_>) -> bool {
        use xibalba_proto::bytes::ByteSliceExt;
        url.host.ascii_eq_ignore_case(&self.host)
            && url.effective_port() == self.port
            && url.scheme == self.scheme
    }

    pub(crate) const fn scheme_bytes(&self) -> &'static [u8] {
        self.scheme.as_bytes()
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
    /// request much later hit exactly that. The retry is
    /// at-least-once: the request may have been fully processed
    /// server-side even though no response byte arrived — treat
    /// non-idempotent requests accordingly.
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
            Err(e) if Self::is_stale_connection(&e) && self.head_buf.is_empty() => {
                self.reconnect_same_host()?;
                self.send_head_once(params)
            }
            Err(e) => Err(e),
        }
    }

    /// Write one request and read the response head, leaving the body
    /// unread on the stream. Returns the head, its framing, and the
    /// offset of the body's first byte within `self.head_buf`.
    fn send_head_once(
        &mut self,
        params: &RequestParams<'_>,
    ) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
        self.head_buf.clear();
        let host_value = self.host_header_value();
        let content_len_str;
        let mut headers = Vec::with_capacity(2 + params.extra_headers.len());

        headers.push(Header {
            name: HeaderName::Host,
            value: &host_value,
        });
        headers.extend(params.extra_headers.iter().map(|(n, v)| Header {
            name: HeaderName::from_bytes(n),
            value: v,
        }));

        if let Some(len) = params.body.map(<[u8]>::len).or_else(|| {
            matches!(params.method, Method::Post | Method::Put | Method::Patch).then_some(0)
        }) {
            content_len_str = len.to_string();
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
        // Serialization runs before anything reaches the peer, so a request
        // rejected here leaves the connection untouched and reusable.
        req.serialize_to_writer(&mut self.write_buf)?;

        // Past this point bytes may have reached the peer, leaving its parser
        // mid-request on any failure. Poison first and clear only once the
        // exchange is known to be recoverable: a write that fails partway
        // through would otherwise let the next request append itself to a
        // truncated one, which the server reads as a single smuggled request.
        self.discard_partial_response();

        match params.body {
            Some(data) if self.inline_body_fits(data.len()) => {
                self.write_buf.extend_from_slice(data);
                self.stream.write_all(&self.write_buf)?;
            }
            Some(data) => {
                self.stream.write_all(&self.write_buf)?;
                self.stream.write_all(data)?;
            }
            None => self.stream.write_all(&self.write_buf)?,
        }
        self.stream.flush()?;

        // A rejected head (notably HeadTooLarge) may already have consumed a
        // prefix of the response, so the poison above stands on every error
        // path: a later request would read the remainder as its own head.
        let parts = HeadData::read_response(
            &mut self.stream,
            &mut self.head_buf,
            MAX_HEAD_SIZE,
            self.config.head_silence,
            params.method == Method::Head,
        )?;
        self.mark_reusable();
        Ok(parts)
    }

    /// Largest request body written as part of the head buffer instead of
    /// a second `write_all`. One write avoids the delayed-ACK stall a
    /// two-write dispatch can hit against servers without `TCP_NODELAY`;
    /// larger bodies are streamed separately to spare the copy.
    const fn inline_body_fits(&self, body_len: usize) -> bool {
        self.write_buf.len() + body_len <= MAX_INLINE_BODY
    }

    /// Read the response body declared by `framing` into memory,
    /// marking the connection dirty on failure (the body may be
    /// partially unread on the socket; never reuse the connection for
    /// the next response).
    ///
    /// `pub(crate)` so the async reader can drain non-2xx bodies with
    /// the same dirty-flag semantics as the blocking path.
    pub(crate) fn read_full_body(
        &mut self,
        framing: &xibalba_proto::response::BodyFraming,
        tail_offset: usize,
    ) -> Result<Vec<u8>, Error> {
        let mut collector =
            BodyCollector::new(self.config.max_response_body, self.config.stream_silence);
        let data = collector.read(&mut self.stream, framing, &self.head_buf[tail_offset..]);
        if data.is_err() || !collector.is_reusable() {
            self.discard_partial_response();
        }
        data
    }

    pub(crate) fn send_one(&mut self, params: &RequestParams<'_>) -> Result<Response, Error> {
        let (head_data, framing, tail_offset) = self.send_head(params)?;
        let body_data = self.read_full_body(&framing, tail_offset)?;
        if head_data.status == xibalba_proto::status::StatusCode::SWITCHING_PROTOCOLS {
            self.discard_partial_response();
        }

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
        let silence = self.config.stream_silence;
        let body = StreamingBody::new(&mut self.stream, &mut self.dirty, &framing, tail, silence);

        Ok(StreamingResponse {
            version: head_data.version,
            status: head_data.status,
            head: head_data,
            body,
        })
    }

    fn execute(&mut self, params: &RequestParams<'_>) -> Result<Response, Error> {
        self.ensure_clean()?;
        RedirectState::follow(self, params)
    }
}

/// See [`Client::inline_body_fits`].
const MAX_INLINE_BODY: usize = 64 * 1024;
