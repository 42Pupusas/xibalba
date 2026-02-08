use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spsc::RingBuffer;
use rustls::ClientConfig;

use crate::body::{BodyReader, IoBlock, reader_thread};
use crate::connection::connect;
use crate::error::{Error, IoError};
use crate::header::{Header, HeaderName};
use crate::method::Method;
use crate::request::Request;
use crate::response::{determine_body_framing, parse_response_head};
use crate::status::StatusCode;
use crate::url::Url;
use crate::version::Version;

pub struct Client {
    tls_config: Arc<ClientConfig>,
}

pub struct Response {
    pub version: Version,
    pub status: StatusCode,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: BodyReader,
    reader_handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl Client {
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new() -> Self {
        let provider = crypto_provider();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .expect("TLS protocol versions")
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Self {
            tls_config: Arc::new(config),
        }
    }

    /// Send a request to the given URL.
    ///
    /// # Errors
    ///
    /// Returns `Error` on connection failure, I/O errors, or malformed responses.
    pub fn request(&self, method: Method, url_bytes: &[u8]) -> Result<Response, Error> {
        let url = Url::parse(url_bytes)?;
        let mut stream = connect(&url, &self.tls_config)?;

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

        // Set read timeout only after the request (and TLS handshake) is complete.
        // The reader thread needs this so it can periodically check the stop flag.
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .map_err(Error::from)?;

        // Create ring buffer
        let (producer, mut ringbuf_rx) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();

        let stop = Arc::new(AtomicBool::new(false));
        let error: Arc<Mutex<Option<IoError>>> = Arc::new(Mutex::new(None));

        let stop_for_thread = Arc::clone(&stop);
        let error_for_thread = Arc::clone(&error);
        let handle = std::thread::spawn(move || {
            reader_thread(stream, &producer, &stop_for_thread, &error_for_thread);
        });

        // Accumulate data until we find \r\n\r\n
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

        // Parse response head
        let mut resp_headers = vec![Header::empty(); 64];
        let (head, head_len) = parse_response_head(&head_buf[..head_end + 4], &mut resp_headers)?;

        let is_head = method == Method::Head;
        let framing =
            determine_body_framing(head.status, is_head, &resp_headers, head.header_count);

        // Owned headers
        let owned_headers: Vec<(Vec<u8>, Vec<u8>)> = resp_headers[..head.header_count]
            .iter()
            .map(|h| (h.name.as_bytes().to_vec(), h.value.to_vec()))
            .collect();

        // Leftover bytes after the head
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

impl Default for Client {
    fn default() -> Self {
        Self::new()
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

#[cfg(feature = "ring")]
fn crypto_provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::ring::default_provider()
}

#[cfg(feature = "aws-lc-rs")]
fn crypto_provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}
