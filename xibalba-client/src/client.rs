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

    /// Send a request over the persistent connection.
    ///
    /// # Errors
    ///
    /// Returns `Error` on serialization failure or if the io thread reports
    /// a connection error.
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
        let len = self.write_buf.len();
        if len > MAX_REQ_SIZE {
            return Err(Error::Connection(format!("request too large: {len} > {MAX_REQ_SIZE}")));
        }

        let mut io_req = IoRequest { buf: [0u8; MAX_REQ_SIZE], len };
        io_req.buf[..len].copy_from_slice(&self.write_buf[..len]);
        self.tx_req
            .reserve_block()
            .ok_or_else(|| Error::Connection("io thread closed".into()))?
            .write(io_req)
            .commit();

        let mut rx = self.rx_resp.take()
            .ok_or_else(|| Error::Connection("previous response not fully consumed".into()))?;

        match rx.pop_block() {
            Some(IoResponse::Head(head)) => Ok(Response {
                version: head.version,
                status: head.status,
                head,
                body: BodyReader::new(rx),
            }),
            Some(IoResponse::Error(e)) => {
                self.rx_resp = Some(rx);
                Err(e)
            }
            _ => Err(Error::Connection("io thread closed unexpectedly".into())),
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
