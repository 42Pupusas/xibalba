use std::thread::JoinHandle;

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spsc::{Consumer, Producer, RingBuffer};

use xibalba_proto::error::Error;
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

use crate::body::{BodyReader, HeadData, IoRequest, IoResponse, MAX_REQ_SIZE, io_thread};
use crate::connector::Connector;

pub struct Client<C: Connector> {
    _tls_config: C::TlsConfig,
    tx_req: Producer<IoRequest>,
    _io_thread: JoinHandle<()>,
    rx_resp: Option<Consumer<IoResponse>>,
    write_buf: Vec<u8>,
}

pub struct Response {
    pub version: Version,
    pub status: xibalba_proto::status::StatusCode,
    pub head: HeadData,
    pub body: BodyReader,
}

impl Response {
    /// Iterate over response headers as `(&[u8], &[u8])` pairs.
    /// Borrows directly from the inline head buffer — zero allocations.
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }

    /// Read the entire body as a UTF-8 string.
    ///
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

    pub(crate) fn into_consumer(self) -> Consumer<IoResponse> {
        self.body.into_consumer()
    }
}

impl<C: Connector> Client<C> {
    /// Connect to `url` and start the io thread.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure.
    pub fn connect(url_bytes: &[u8], tls_config: C::TlsConfig) -> Result<Self, Error> {
        let url = Url::parse(url_bytes)?;
        let stream = C::connect(&url, &tls_config)?;

        let (tx_req, mut rx_req) = RingBuffer::<IoRequest>::new(Capacity::exact(16)).split();
        let (tx_resp, rx_resp) = RingBuffer::<IoResponse>::new(Capacity::exact(64)).split();

        let handle = std::thread::spawn(move || {
            io_thread(stream, &mut rx_req, &tx_resp);
        });

        Ok(Self {
            _tls_config: tls_config,
            tx_req,
            rx_resp: Some(rx_resp),
            _io_thread: handle,
            write_buf: Vec::with_capacity(512),
        })
    }

    /// Serialize `method`/`path`/`query` and push the request onto the io-thread
    /// ring without blocking.  Returns immediately after the slot is committed.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the request ring is full, the request is too large to
    /// serialize, or if a previous response has not yet been reclaimed.
    pub fn send_request(&mut self, method: Method, path: &[u8], query: Option<&[u8]>) -> Result<(), Error> {
        if self.rx_resp.is_none() {
            return Err(Error::Connection("previous response not fully consumed".into()));
        }

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
        let len = self.write_buf.len();
        if len > MAX_REQ_SIZE {
            return Err(Error::Connection(format!("request too large: {len} > {MAX_REQ_SIZE}")));
        }

        let mut io_req = IoRequest { buf: [0u8; MAX_REQ_SIZE], len };
        io_req.buf[..len].copy_from_slice(&self.write_buf[..len]);
        self.tx_req
            .push_block(io_req)
            .map_err(|_| Error::Connection("request ring full".into()))
    }

    /// Poll for a response without blocking.
    ///
    /// Returns `Ok(Some(_))` when the response head has arrived, `Ok(None)`
    /// when the io thread has not yet produced one, or `Err` on a connection
    /// error reported by the io thread.
    ///
    /// The returned [`Response`] holds the `rx` consumer; call [`reclaim`](Self::reclaim)
    /// after the body is fully drained to ready the client for the next request.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `send_request` was never called (no pending request),
    /// or if the io thread reported a connection error.
    #[allow(clippy::missing_panics_doc)] // expect() is unreachable: just matched Some above
    pub fn poll_response(&mut self) -> Result<Option<Response>, Error> {
        let rx = self.rx_resp.as_mut()
            .ok_or_else(|| Error::Connection("no pending request".into()))?;

        match rx.pop_block() {
            None => Ok(None),
            Some(IoResponse::Head(head)) => {
                let rx = self.rx_resp.take().expect("just checked");
                Ok(Some(Response {
                    version: head.version,
                    status: head.status,
                    head,
                    body: BodyReader::new(rx),
                }))
            }
            Some(IoResponse::Error(e)) => Err(e),
            Some(_) => Err(Error::Connection("unexpected message from io thread".into())),
        }
    }

    /// Send a request and block until the response head arrives.
    ///
    /// # Errors
    ///
    /// Returns `Error` on serialization failure or if the io thread reports
    /// a connection error.
    pub fn request(&mut self, method: Method, path: &[u8], query: Option<&[u8]>) -> Result<Response, Error> {
        self.send_request(method, path, query)?;
        loop {
            match self.poll_response()? {
                Some(resp) => return Ok(resp),
                None => std::hint::spin_loop(),
            }
        }
    }

    /// Reclaim the consumer from a completed response, readying the client
    /// for the next request.
    pub fn reclaim(&mut self, resp: Response) {
        self.rx_resp = Some(resp.into_consumer());
    }

    /// # Errors
    ///
    /// See [`request`](Self::request).
    pub fn get(&mut self, path: &[u8]) -> Result<Response, Error> {
        self.request(Method::Get, path, None)
    }
}
