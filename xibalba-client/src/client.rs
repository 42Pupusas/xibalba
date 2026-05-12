use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spsc::RingBuffer;

use xibalba_proto::error::{Error, IoError};
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::response::{determine_body_framing, parse_response_head};
use xibalba_proto::status::StatusCode;
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

use crate::body::{BodyReader, IoBlock, reader_thread};
use crate::connector::{Connector, SetReadTimeout};

pub struct Client<C: Connector> {
    tls_config: C::TlsConfig,
}

pub struct Response {
    pub version: Version,
    pub status: StatusCode,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: BodyReader,
    reader_handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl<C: Connector> Client<C> {
    pub const fn new(tls_config: C::TlsConfig) -> Self {
        Self { tls_config }
    }

    /// Send a request to the given URL.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure, I/O errors, or malformed responses.
    pub fn request(&self, method: Method, url_bytes: &[u8]) -> Result<Response, Error> {
        let url = Url::parse(url_bytes)?;
        let mut stream = C::connect(&url, &self.tls_config)?;

        let host_str = std::str::from_utf8(url.host)
            .map_err(|_| Error::Connection("invalid UTF-8 in host".into()))?;
        let host_value: Vec<u8> = if url.port.is_some() {
            format!("{}:{}", host_str, url.effective_port()).into_bytes()
        } else {
            url.host.to_vec()
        };

        let headers = [
            Header {
                name: HeaderName::Host,
                value: &host_value,
            },
            Header {
                name: HeaderName::UserAgent,
                value: b"xibalba/0.1",
            },
            Header {
                name: HeaderName::Connection,
                value: b"close",
            },
        ];

        let req = Request {
            method,
            path: url.request_path(),
            query: url.query,
            version: Version::Http11,
            headers: &headers,
        };

        req.serialize_to_writer(&mut stream).map_err(Error::from)?;
        stream.flush().map_err(Error::from)?;

        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .map_err(Error::from)?;

        let (producer, mut ringbuf_rx) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();

        let stop = Arc::new(AtomicBool::new(false));
        let error: Arc<Mutex<Option<IoError>>> = Arc::new(Mutex::new(None));

        let stop_for_thread = Arc::clone(&stop);
        let error_for_thread = Arc::clone(&error);
        let handle = std::thread::spawn(move || {
            reader_thread(stream, &producer, &stop_for_thread, &error_for_thread);
        });

        let mut head_buf = Vec::with_capacity(4096);
        let head_end = loop {
            if let Some(pos) = find_header_end(&head_buf) {
                break pos;
            }
            match ringbuf_rx.pop() {
                Some(block) => {
                    if block.len == 0 {
                        if let Ok(guard) = error.lock()
                            && let Some(e) = guard.as_ref()
                        {
                            return Err(Error::Io(e.clone()));
                        }
                        return Err(Error::Connection(
                            "connection closed before headers complete".into(),
                        ));
                    }
                    head_buf.extend_from_slice(&block.data[..block.len]);
                }
                None => {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
        };

        let mut resp_headers = vec![Header::empty(); 64];
        let (head, head_len) = parse_response_head(&head_buf[..head_end + 4], &mut resp_headers)?;

        let is_head = method == Method::Head;
        let framing =
            determine_body_framing(head.status, is_head, &resp_headers, head.header_count);

        let owned_headers: Vec<(Vec<u8>, Vec<u8>)> = resp_headers[..head.header_count]
            .iter()
            .map(|h| (h.name.as_bytes().to_vec(), h.value.to_vec()))
            .collect();

        let leftover = head_buf[head_len..].to_vec();
        let body = BodyReader::new(ringbuf_rx, framing, leftover, error, Arc::clone(&stop));

        Ok(Response {
            version: head.version,
            status: head.status,
            headers: owned_headers,
            body,
            reader_handle: Some(handle),
            stop,
        })
    }

    /// Send a GET request.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure, I/O errors, or malformed responses.
    pub fn get(&self, url: &[u8]) -> Result<Response, Error> {
        self.request(Method::Get, url)
    }
}

impl Response {
    /// Access the body reader.
    #[allow(clippy::missing_const_for_fn)]
    pub fn body(&mut self) -> &mut BodyReader {
        &mut self.body
    }

    /// Read the entire body as a UTF-8 string.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` on read failure, or `Error::Connection` if the body
    /// is not valid UTF-8.
    pub fn text(&mut self) -> Result<String, Error> {
        use std::io::Read;
        let mut buf = Vec::new();
        self.body.read_to_end(&mut buf).map_err(Error::from)?;
        String::from_utf8(buf)
            .map_err(|e| Error::Connection(format!("response body is not valid UTF-8: {e}")))
    }
}

impl Drop for Response {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.reader_handle.take() {
            let _ = handle.join();
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}
