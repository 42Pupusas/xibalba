use std::io::Write;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;

use crate::interrupt::{Interrupt, InterruptibleStream, NeverCancelled};
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

use crate::body::{BodyCollector, BodyReader, StreamingBody};
use crate::config::HEAD_BUF_SIZE;
use crate::connector::{Connector, SetReadTimeout};
use crate::origin::Origin;
use crate::params::RequestParams;
use crate::redirect::{Hop, RedirectState};
use crate::response::HeadData;
use crate::silence::RequestDeadline;

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
    origin: Origin,
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
        let stream = C::connect(&url, &tls_config, config.connect_deadline())?;

        let client = Self {
            tls_config,
            stream,
            config,
            origin: Origin::from_url(&url),
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
            allow_replay: method.is_replay_eligible(),
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
        self.stream.set_write_timeout(self.config.write_timeout)?;
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
        self.stream = C::connect(url, &self.tls_config, self.config.connect_deadline())?;
        self.origin = Origin::from_url(url);
        self.apply_timeouts()?;
        self.dirty = false;
        Ok(())
    }

    /// Reconnect to the current host (used to recover a stale
    /// keep-alive connection without re-parsing a URL).
    fn reconnect_same_host(&mut self) -> Result<(), Error> {
        let origin = self.origin.clone();
        self.reconnect(&origin.root_url())
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
    /// [`send_head`](Self::send_head) with `interrupt` consulted before every
    /// write and every head read, so a caller that can cancel is not left
    /// waiting out the head-silence budget.
    ///
    /// The retry after a stale connection re-checks the interrupt through the
    /// same path, so a cancel arriving during the reconnect is observed
    /// rather than being overtaken by the resent request.
    pub(crate) fn send_head_interruptible<I: Interrupt>(
        &mut self,
        params: &RequestParams<'_>,
        deadline: RequestDeadline,
        mut interrupt: I,
    ) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
        match self.send_head_once(params, deadline, &mut interrupt) {
            Ok(head) => Ok(head),
            Err(e)
                if params.allow_replay
                    && Self::is_stale_connection(&e)
                    && self.head_buf.is_empty() =>
            {
                if interrupt.is_cancelled() {
                    return Err(Error::from(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "request cancelled",
                    )));
                }
                self.reconnect_same_host()?;
                // The retry shares the caller's total rather than earning a
                // fresh one: a deadline spent on the failed attempt is spent.
                self.send_head_once(params, deadline, &mut interrupt)
            }
            Err(e) => Err(e),
        }
    }

    /// Write one request and read the response head, leaving the body
    /// unread on the stream. Returns the head, its framing, and the
    /// offset of the body's first byte within `self.head_buf`.
    fn send_head_once<I: Interrupt>(
        &mut self,
        params: &RequestParams<'_>,
        deadline: RequestDeadline,
        interrupt: &mut I,
    ) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
        // Before the first byte, not only between reads: a hop that spent
        // the total must not write its successor into a doomed exchange.
        deadline.check()?;
        self.head_buf.clear();
        let host_value = self.origin.host_header_value();
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

        // Both the write and the head read run through the interrupt, so a
        // peer that stops reading mid-upload, or never answers, cannot pin the
        // caller until the silence budget expires.
        let inline = params.body.is_some_and(|d| self.inline_body_fits(d.len()));
        let mut wire = InterruptibleStream::new(&mut self.stream, interrupt);
        match params.body {
            Some(data) if inline => {
                self.write_buf.extend_from_slice(data);
                wire.write_all(&self.write_buf)?;
            }
            Some(data) => {
                wire.write_all(&self.write_buf)?;
                wire.write_all(data)?;
            }
            None => wire.write_all(&self.write_buf)?,
        }
        wire.flush()?;

        // A rejected head (notably HeadTooLarge) may already have consumed a
        // prefix of the response, so the poison above stands on every error
        // path: a later request would read the remainder as its own head.
        let parts = HeadData::read_response(
            &mut wire,
            &mut self.head_buf,
            MAX_HEAD_SIZE,
            self.config.head_silence,
            deadline,
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
        deadline: RequestDeadline,
    ) -> Result<Vec<u8>, Error> {
        let mut collector = BodyCollector::with_deadline(
            self.config.max_response_body,
            self.config.stream_silence,
            deadline,
        );
        let data = collector.read(&mut self.stream, framing, &self.head_buf[tail_offset..]);
        if data.is_err() || !collector.is_reusable() {
            self.discard_partial_response();
        }
        data
    }

    fn send_one(
        &mut self,
        params: &RequestParams<'_>,
        deadline: RequestDeadline,
    ) -> Result<Response, Error> {
        // Every dispatch reconnects first if the previous exchange left the
        // connection unusable. Checking only once per caller request would
        // skip the check between redirect hops, where the previous hop's
        // close-delimited or over-long body can have spent the connection.
        self.ensure_clean()?;
        let (head_data, framing, tail_offset) =
            self.send_head_interruptible(params, deadline, NeverCancelled)?;
        let reuse = head_data.connection_reuse();
        deadline.check()?;
        let body_data = self.read_full_body(&framing, tail_offset, deadline)?;
        if !reuse.is_keep() {
            self.discard_partial_response();
        }

        Ok(Response {
            version: head_data.version(),
            status: head_data.status(),
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
        let deadline = RequestDeadline::after(self.config.request_deadline);
        let (head_data, framing, tail_offset) =
            self.send_head_interruptible(params, deadline, NeverCancelled)?;
        let reuse = head_data.connection_reuse();

        let tail = self.head_buf[tail_offset..].to_vec();
        self.dirty = true;
        let silence = self.config.stream_silence;
        let body = StreamingBody::new(
            &mut self.stream,
            &mut self.dirty,
            &framing,
            tail,
            silence,
            reuse.is_keep(),
        );

        Ok(StreamingResponse {
            version: head_data.version(),
            status: head_data.status(),
            head: head_data,
            body,
        })
    }

    /// Execute a request, following redirects up to `max_redirects`.
    ///
    /// The loop lives here because only the client may open a connection.
    /// [`RedirectState`] decides *what* the next hop is and reports whether
    /// it needs a different origin; acting on that is this method's job.
    fn execute(&mut self, params: &RequestParams<'_>) -> Result<Response, Error> {
        let mut state = RedirectState::new(params);
        let mut hops_left = self.config.max_redirects;
        // One total for the whole redirect chain: a hop that spent the
        // budget shortens the next, and the deadline passing anywhere
        // surfaces here rather than restarting per hop.
        let deadline = RequestDeadline::after(self.config.request_deadline);

        loop {
            let response = self.send_one(&state.params(), deadline)?;
            let Some(location) = RedirectState::location_to_follow(&response) else {
                return Ok(response);
            };

            // Check the budget before applying the Location. Applying it first
            // opens a connection to the next hop -- possibly cross-origin --
            // only to discard it, which with max_redirects = 0 contacts a host
            // the caller never agreed to reach.
            if hops_left == 0 {
                return Err(ConnectionError::TooManyRedirects.into());
            }
            hops_left -= 1;

            if let Hop::Reconnect(target) =
                state.advance(&self.origin, response.status, &location)?
            {
                self.reconnect(&Url::parse(&target)?)?;
            }
        }
    }
}

/// See [`Client::inline_body_fits`].
const MAX_INLINE_BODY: usize = 64 * 1024;
