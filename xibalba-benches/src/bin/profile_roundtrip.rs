#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use xibalba_client::client::Client;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_proto::error::Error;
use xibalba_proto::method::Method;
use xibalba_proto::url::Url;

// ── Scenarios ─────────────────────────────────────────────────────────────────
//
// "small"  – 13-byte body, Content-Length framing (no allocations on the hot path ideally)
// "medium" – 1 KiB body, Content-Length
// "large"  – 64 KiB body, chunked transfer-encoding

const ITERATIONS: usize = 10_000;

const SMALL_RESP: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!";

fn medium_resp() -> Vec<u8> {
    let body = vec![b'x'; 1024];
    let mut r = b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: keep-alive\r\n\r\n".to_vec();
    r.extend_from_slice(&body);
    r
}

fn large_resp() -> Vec<u8> {
    const N: usize = 65536;
    let mut r = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"
        .to_vec();
    r.extend_from_slice(b"10000\r\n");
    r.extend(std::iter::repeat_n(b'x', N));
    r.extend_from_slice(b"\r\n0\r\n\r\n");
    r
}

// ── Plain TCP connector ───────────────────────────────────────────────────────

struct PlainConnector;
struct PlainStream(TcpStream);

impl Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}
impl Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        self.0.set_read_timeout(dur)
    }
}
impl Connector for PlainConnector {
    type Stream = PlainStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, _tls_config: &()) -> Result<Self::Stream, Error> {
        let host = std::str::from_utf8(url.host)
            .map_err(|_| Error::Connection("invalid UTF-8 in host".into()))?;
        let stream = TcpStream::connect(format!("{}:{}", host, url.effective_port()))?;
        Ok(PlainStream(stream))
    }
}

// ── Echo server ───────────────────────────────────────────────────────────────

fn spawn_server(response: Vec<u8>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let resp = response.clone();
            std::thread::spawn(move || {
                let mut req_buf = [0u8; 1024];
                while s.read(&mut req_buf).unwrap_or(0) > 0 {
                    if s.write_all(&resp).is_err() {
                        break;
                    }
                }
            });
        }
    });
    port
}

// ── Run helpers ───────────────────────────────────────────────────────────────

fn run(response: Vec<u8>) {
    let port = spawn_server(response);
    let url = format!("http://127.0.0.1:{port}/");
    let mut client = Client::<PlainConnector>::connect(url.as_bytes(), ()).unwrap();

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    for _ in 0..ITERATIONS {
        let mut resp = client.request(black_box(Method::Get), b"/", None).unwrap();
        // Pre-size from Content-Length to avoid read_to_end probe reallocs.
        let content_length: usize = resp
            .headers()
            .find(|(n, _)| n.eq_ignore_ascii_case(b"content-length"))
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        let mut body = Vec::with_capacity(content_length);
        resp.body.read_to_end(&mut body).unwrap();
        black_box(&body);
        client.reclaim(resp);
    }
}

fn main() {
    let scenario = std::env::args().nth(1).unwrap_or_else(|| "small".into());
    match scenario.as_str() {
        "small" => run(SMALL_RESP.to_vec()),
        "medium" => run(medium_resp()),
        "large" => run(large_resp()),
        other => {
            eprintln!("unknown scenario: {other}. use: small | medium | large");
            std::process::exit(1);
        }
    }
}
