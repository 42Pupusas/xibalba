use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;
use std::time::Duration;

use xibalba_iouring::driver::{ConnHandle, ConnResult, Pool};
use xibalba_client::client::Client;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_proto::error::Error;
use xibalba_proto::method::Method;
use xibalba_proto::url::Url;

fn main() {
    divan::main();
}

// ── Plain TCP connector ───────────────────────────────────────────────────────

struct PlainConnector;
struct PlainStream(TcpStream);

impl Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> { self.0.read(buf) }
}
impl Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> { self.0.write(buf) }
    fn flush(&mut self) -> std::io::Result<()> { self.0.flush() }
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
        Ok(PlainStream(TcpStream::connect(format!("{}:{}", host, url.effective_port()))?))
    }
}

// ── Echo server factory ───────────────────────────────────────────────────────
//
// Each scenario gets its own server so responses don't cross-contaminate.
// The server reads until \r\n\r\n (end of request headers), discards the
// request body (reads Content-Length bytes if present), then writes `response`
// for every request on the same keep-alive connection.

fn spawn_echo_server(response: &'static [u8]) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            std::thread::spawn(move || {
                let mut hdr_buf = Vec::with_capacity(512);
                let mut raw = [0u8; 4096];
                'conn: loop {
                    // Read until end of request headers.
                    let header_end = loop {
                        let n = s.read(&mut raw).unwrap_or(0);
                        if n == 0 { break 'conn; }
                        hdr_buf.extend_from_slice(&raw[..n]);
                        if let Some(pos) = hdr_buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    // Drain request body if Content-Length is present.
                    let hdr_str = &hdr_buf[..header_end];
                    let body_len: usize = std::str::from_utf8(hdr_str)
                        .ok()
                        .and_then(|s| {
                            s.lines()
                                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                                .and_then(|l| l.split_once(':'))
                                .and_then(|(_, v)| v.trim().parse().ok())
                        })
                        .unwrap_or(0);
                    // Any bytes past the header end are already in hdr_buf.
                    let already_read = hdr_buf.len() - header_end;
                    let mut remaining = body_len.saturating_sub(already_read);
                    let mut discard = [0u8; 4096];
                    while remaining > 0 {
                        let n = s.read(&mut discard[..remaining.min(4096)]).unwrap_or(0);
                        if n == 0 { break 'conn; }
                        remaining -= n;
                    }
                    if s.write_all(response).is_err() { break 'conn; }
                    hdr_buf.clear();
                }
            });
        }
    });
    port
}

fn spawn_echo_server_owned(response: Vec<u8>) -> u16 {
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
                    let hdr_str = &hdr_buf[..header_end];
                    let body_len: usize = std::str::from_utf8(hdr_str)
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

// ── Per-scenario server ports (lazily started once) ──────────────────────────
//
// Each driver under test gets its own port so their keep-alive connections
// don't interleave.

static PORT_SMALL_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_SMALL_URING: OnceLock<u16> = OnceLock::new();
static PORT_SMALL_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_RESP_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_RESP_URING: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_RESP_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_REQ_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_REQ_URING: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_REQ_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_STRESS_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_STRESS_URING: OnceLock<u16> = OnceLock::new();
static PORT_STRESS_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_CONCURRENT_URING: OnceLock<u16> = OnceLock::new();
static PORT_CONCURRENT_UREQ: OnceLock<u16> = OnceLock::new();

fn port_small_xibalba() -> u16 {
    *PORT_SMALL_XIBALBA.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}
fn port_small_uring() -> u16 {
    *PORT_SMALL_URING.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}
fn port_small_ureq() -> u16 {
    *PORT_SMALL_UREQ.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}

fn make_large_resp() -> Vec<u8> {
    const N: usize = 65536;
    let mut resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {N}\r\nConnection: keep-alive\r\n\r\n"
    ).into_bytes();
    resp.extend(std::iter::repeat_n(b'x', N));
    resp
}
fn port_large_resp_xibalba() -> u16 {
    *PORT_LARGE_RESP_XIBALBA.get_or_init(|| spawn_echo_server_owned(make_large_resp()))
}
fn port_large_resp_uring() -> u16 {
    *PORT_LARGE_RESP_URING.get_or_init(|| spawn_echo_server_owned(make_large_resp()))
}
fn port_large_resp_ureq() -> u16 {
    *PORT_LARGE_RESP_UREQ.get_or_init(|| spawn_echo_server_owned(make_large_resp()))
}

fn port_large_req_xibalba() -> u16 {
    *PORT_LARGE_REQ_XIBALBA.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    ))
}
fn port_large_req_uring() -> u16 {
    *PORT_LARGE_REQ_URING.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    ))
}
fn port_large_req_ureq() -> u16 {
    *PORT_LARGE_REQ_UREQ.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    ))
}

fn port_stress_xibalba() -> u16 {
    *PORT_STRESS_XIBALBA.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}
fn port_stress_uring() -> u16 {
    *PORT_STRESS_URING.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}
fn port_stress_ureq() -> u16 {
    *PORT_STRESS_UREQ.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}
fn port_concurrent_uring() -> u16 {
    *PORT_CONCURRENT_URING.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}
fn port_concurrent_ureq() -> u16 {
    *PORT_CONCURRENT_UREQ.get_or_init(|| spawn_echo_server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
    ))
}

fn io_uring_pool(port: u16) -> (Pool, ConnHandle) {
    let url = format!("http://127.0.0.1:{port}");
    let mut pool = Pool::new().unwrap();
    let conn = pool.connect(url.as_bytes()).unwrap();
    (pool, conn)
}

// 512-byte query string exercising large request serialization.
const LARGE_QUERY: &[u8] = &[b'a'; 512];

fn network_enabled() -> bool {
    std::env::var("BENCH_NETWORK").is_ok()
}

// ── Bench helpers ─────────────────────────────────────────────────────────────

fn xibalba_client(port: u16) -> Client<PlainConnector> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector>::connect(url.as_bytes(), ()).unwrap()
}

fn ureq_agent() -> ureq::Agent {
    ureq::Agent::new_with_defaults()
}

// ── Scenario: small request, small response (13 B) ───────────────────────────

mod small {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, io_uring_pool, network_enabled,
        port_small_xibalba, port_small_uring, port_small_ureq,
        ureq_agent, xibalba_client,
    };

    #[divan::bench(skip_ext_time)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let mut client = xibalba_client(port_small_xibalba());
        bencher.bench_local(|| {
            let mut resp = client.request(black_box(Method::Get), b"/", None).unwrap();
            let mut body = Vec::new();
            resp.body.read_to_end(&mut body).unwrap();
            black_box(&body);
            drop(resp);
        });
    }

    #[divan::bench(skip_ext_time)]
    fn io_uring(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let (mut pool, conn) = io_uring_pool(port_small_uring());
        bencher.bench_local(|| {
            let id = pool.get(conn, black_box(b"/")).unwrap();
            if let Ok(super::ConnResult::Response(r)) = pool.recv(id) { black_box(&r.body); }
        });
    }

    #[divan::bench(skip_ext_time)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let agent = ureq_agent();
        let url = format!("http://127.0.0.1:{}/", port_small_ureq());
        bencher.bench_local(|| {
            let _body = agent.get(black_box(&url)).call().unwrap()
                .body_mut().read_to_string().unwrap();
        });
    }
}

// ── Scenario: large response (64 KiB, Content-Length) ────────────────────────

mod large_resp {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, io_uring_pool, network_enabled,
        port_large_resp_xibalba, port_large_resp_uring, port_large_resp_ureq,
        ureq_agent, xibalba_client,
    };

    #[divan::bench(skip_ext_time)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let mut client = xibalba_client(port_large_resp_xibalba());
        bencher.bench_local(|| {
            let mut resp = client.request(black_box(Method::Get), b"/", None).unwrap();
            let mut body = Vec::new();
            resp.body.read_to_end(&mut body).unwrap();
            black_box(&body);
            drop(resp);
        });
    }

    #[divan::bench(skip_ext_time)]
    fn io_uring(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let (mut pool, conn) = io_uring_pool(port_large_resp_uring());
        bencher.bench_local(|| {
            let id = pool.get(conn, black_box(b"/")).unwrap();
            if let Ok(super::ConnResult::Response(r)) = pool.recv(id) { black_box(&r.body); }
        });
    }

    #[divan::bench(skip_ext_time)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let agent = ureq_agent();
        let url = format!("http://127.0.0.1:{}/", port_large_resp_ureq());
        bencher.bench_local(|| {
            let _body = agent.get(black_box(&url)).call().unwrap()
                .body_mut().read_to_string().unwrap();
        });
    }
}

// ── Scenario: large request (512-byte query string) ──────────────────────────

mod large_req {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, io_uring_pool, network_enabled,
        port_large_req_xibalba, port_large_req_uring, port_large_req_ureq,
        ureq_agent, xibalba_client, LARGE_QUERY,
    };

    #[divan::bench(skip_ext_time)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let mut client = xibalba_client(port_large_req_xibalba());
        bencher.bench_local(|| {
            let mut resp = client
                .request(black_box(Method::Get), b"/search", Some(black_box(LARGE_QUERY)))
                .unwrap();
            let mut body = Vec::new();
            resp.body.read_to_end(&mut body).unwrap();
            black_box(&body);
            drop(resp);
        });
    }

    #[divan::bench(skip_ext_time)]
    fn io_uring(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let (mut pool, conn) = io_uring_pool(port_large_req_uring());
        bencher.bench_local(|| {
            let id = pool.request(conn, black_box(Method::Get), b"/search", Some(black_box(LARGE_QUERY))).unwrap();
            if let Ok(super::ConnResult::Response(r)) = pool.recv(id) { black_box(&r.body); }
        });
    }

    #[divan::bench(skip_ext_time)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let agent = ureq_agent();
        let query = std::str::from_utf8(LARGE_QUERY).unwrap();
        let url = format!("http://127.0.0.1:{}//search?{query}", port_large_req_ureq());
        bencher.bench_local(|| {
            let _body = agent.get(black_box(&url)).call().unwrap()
                .body_mut().read_to_string().unwrap();
        });
    }
}

// ── Scenario: stress (sequential throughput, 1000 iters per sample) ──────────

mod stress {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, io_uring_pool, network_enabled,
        port_stress_xibalba, port_stress_uring, port_stress_ureq,
        ureq_agent, xibalba_client,
    };

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let mut client = xibalba_client(port_stress_xibalba());
        bencher.bench_local(|| {
            for _ in 0..1000 {
                let mut resp = client.request(black_box(Method::Get), b"/", None).unwrap();
                let mut body = Vec::new();
                resp.body.read_to_end(&mut body).unwrap();
                black_box(&body);
                drop(resp);
            }
        });
    }

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn io_uring(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let (mut pool, conn) = io_uring_pool(port_stress_uring());
        bencher.bench_local(|| {
            for _ in 0..1000 {
                let id = pool.get(conn, black_box(b"/")).unwrap();
                if let Ok(super::ConnResult::Response(r)) = pool.recv(id) { black_box(&r.body); }
            }
        });
    }

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let agent = ureq_agent();
        let url = format!("http://127.0.0.1:{}/", port_stress_ureq());
        bencher.bench_local(|| {
            for _ in 0..1000 {
                let _body = agent.get(black_box(&url)).call().unwrap()
                    .body_mut().read_to_string().unwrap();
            }
        });
    }
}

// ── Scenario: concurrent (4 connections × 250 requests = 1000 total) ─────────
//
// io_uring: all 1000 requests submitted across 4 connections before waiting,
//   then all 1000 responses drained — one kernel submission burst.
// ureq:     4 threads each doing 250 sequential requests on their own connection.
//
// This is the scenario where io_uring's batched submission should win:
// the kernel sees all sends in one go and can pipeline them on the wire.

const CONNS: usize = 4;
const REQS_PER_CONN: usize = 250;
#[allow(dead_code)]
const TOTAL_REQS: usize = CONNS * REQS_PER_CONN;

mod concurrent {
    use divan::black_box;

    use super::{
        CONNS, REQS_PER_CONN,
        network_enabled,
        port_concurrent_uring, port_concurrent_ureq,
        ureq_agent,
    };

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn io_uring(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let port = port_concurrent_uring();
        let url = format!("http://127.0.0.1:{port}");
        let mut pool = super::Pool::<256, 64, 8192, 8192>::new().unwrap();
        let conns: Vec<_> = (0..CONNS)
            .map(|_| pool.connect(url.as_bytes()).unwrap())
            .collect();

        bencher.bench_local(|| {
            // One round: submit one request per connection, then drain all
            // CONNS responses.  Keeps one request in-flight per connection
            // at a time while batching all CONNS sends into one submission burst.
            for _ in 0..REQS_PER_CONN {
                let ids: Vec<_> = conns.iter()
                    .map(|&conn| pool.get(conn, black_box(b"/")).unwrap())
                    .collect();
                for id in ids {
                    if let Ok(super::ConnResult::Response(resp)) = pool.recv(id) {
                        black_box(&resp.body);
                    }
                }
            }
        });
    }

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() { return; }
        let port = port_concurrent_ureq();

        bencher.bench_local(|| {
            let url = format!("http://127.0.0.1:{port}/");
            let handles: Vec<_> = (0..CONNS).map(|_| {
                let url = url.clone();
                let agent = ureq_agent();
                std::thread::spawn(move || {
                    for _ in 0..REQS_PER_CONN {
                        let body = agent.get(black_box(&url)).call().unwrap()
                            .body_mut().read_to_string().unwrap();
                        black_box(&body);
                    }
                })
            }).collect();
            for h in handles { h.join().unwrap(); }
        });
    }
}
