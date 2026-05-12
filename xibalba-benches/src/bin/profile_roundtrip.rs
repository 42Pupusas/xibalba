#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use xibalba_iouring::driver::Pool;
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
    let mut r = format!("HTTP/1.1 200 OK\r\nContent-Length: {N}\r\nConnection: keep-alive\r\n\r\n")
        .into_bytes();
    r.extend(std::iter::repeat_n(b'x', N));
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
                let mut hdr_buf = Vec::with_capacity(512);
                let mut raw = [0u8; 4096];
                'conn: loop {
                    let header_end = loop {
                        let n = s.read(&mut raw).unwrap_or(0);
                        if n == 0 { break 'conn; }
                        hdr_buf.extend_from_slice(&raw[..n]);
                        if let Some(pos) = hdr_buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let body_len: usize = std::str::from_utf8(&hdr_buf[..header_end])
                        .ok()
                        .and_then(|s| {
                            s.lines()
                                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                                .and_then(|l| l.split_once(':'))
                                .and_then(|(_, v)| v.trim().parse().ok())
                        })
                        .unwrap_or(0);
                    let already_read = hdr_buf.len() - header_end;
                    let mut remaining = body_len.saturating_sub(already_read);
                    let mut discard = [0u8; 4096];
                    while remaining > 0 {
                        let n = s.read(&mut discard[..remaining.min(4096)]).unwrap_or(0);
                        if n == 0 { break 'conn; }
                        remaining -= n;
                    }
                    if s.write_all(&resp).is_err() { break 'conn; }
                    hdr_buf.clear();
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

    let t0 = std::time::Instant::now();
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
    let elapsed = t0.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let us_per_iter = elapsed.as_secs_f64() * 1e6 / ITERATIONS as f64;
    println!("blocking:  {ITERATIONS} iters in {elapsed:.2?}  ({us_per_iter:.1} µs/iter)");
}

/// Like `run` but uses `send_request` + optional sleep + `poll_response` loop.
///
/// Set `POLL_DELAY_US` to inject a sleep between send and poll (in microseconds).
/// Prints per-iteration timing and total poll-spin count so we can compare
/// against the always-spinning `request()` path.
fn run_decoupled(response: Vec<u8>) {
    let delay = std::env::var("POLL_DELAY_US")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_micros);

    let port = spawn_server(response);
    let url = format!("http://127.0.0.1:{port}/");
    let mut client = Client::<PlainConnector>::connect(url.as_bytes(), ()).unwrap();

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let mut total_polls: u64 = 0;
    let t0 = std::time::Instant::now();

    for _ in 0..ITERATIONS {
        client.send_request(black_box(Method::Get), b"/", None).unwrap();

        if let Some(d) = delay {
            std::thread::sleep(d);
        }

        let mut resp = loop {
            total_polls += 1;
            match client.poll_response().unwrap() {
                Some(r) => break r,
                None => std::hint::spin_loop(),
            }
        };

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

    let elapsed = t0.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let (us_per_iter, avg_polls) = (
        elapsed.as_secs_f64() * 1e6 / ITERATIONS as f64,
        total_polls as f64 / ITERATIONS as f64,
    );
    println!(
        "decoupled: {ITERATIONS} iters in {elapsed:.2?}  ({us_per_iter:.1} µs/iter)  polls={total_polls}  avg={avg_polls:.1} polls/req",
    );
}

fn run_io_uring(response: Vec<u8>) {
    let port = spawn_server(response);

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let url = format!("http://127.0.0.1:{port}");
    let mut pool = Pool::new().unwrap();
    let conn = pool.connect(url.as_bytes()).unwrap();

    let t0 = std::time::Instant::now();
    for _i in 0..ITERATIONS {
        pool.get(conn, black_box(b"/")).unwrap();
        let resp = match pool.recv() {
            xibalba_iouring::driver::ConnResult::Response(r) => r,
            xibalba_iouring::driver::ConnResult::Error { errno, .. } =>
                panic!("io_uring error: errno {errno}"),
            xibalba_iouring::driver::ConnResult::Timeout => panic!("unexpected timeout"),
        };
        black_box(&resp.body);
    }
    let elapsed = t0.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let us_per_iter = elapsed.as_secs_f64() * 1e6 / ITERATIONS as f64;
    println!("io_uring:  {ITERATIONS} iters in {elapsed:.2?}  ({us_per_iter:.1} µs/iter)");
}

fn main() {
    let mut args = std::env::args().skip(1);
    let scenario = args.next().unwrap_or_else(|| "small".into());
    let mode = args.next().unwrap_or_else(|| "blocking".into());

    let response = match scenario.as_str() {
        "small"  => SMALL_RESP.to_vec(),
        "medium" => medium_resp(),
        "large"  => large_resp(),
        other => {
            eprintln!("unknown scenario: {other}. use: small | medium | large");
            std::process::exit(1);
        }
    };

    match mode.as_str() {
        "blocking"  => run(response),
        "decoupled" => run_decoupled(response),
        "uring"     => run_io_uring(response),
        other => {
            eprintln!("unknown mode: {other}. use: blocking | decoupled | uring");
            std::process::exit(1);
        }
    }
}
