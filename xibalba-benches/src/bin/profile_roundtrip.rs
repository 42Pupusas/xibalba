#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::hint::black_box;
use std::io::Read;

use xibalba_benches::{EchoServer, PlainConnector};
use xibalba_client::client::Client;
use xibalba_iouring::driver::Pool;
use xibalba_proto::method::Method;

const ITERATIONS: usize = 10_000;

const SMALL_RESP: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!";

fn medium_resp() -> Vec<u8> {
    let body = vec![b'x'; 1024];
    let mut r =
        b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: keep-alive\r\n\r\n".to_vec();
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

// ── Run helpers ───────────────────────────────────────────────────────────────

fn run_blocking(response: Vec<u8>) {
    let port = EchoServer::spawn_owned(response);
    let url = format!("http://127.0.0.1:{port}/");
    let mut client = Client::<PlainConnector>::connect_default(url.as_bytes(), ()).unwrap();

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let t0 = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let mut resp = client
            .request(black_box(Method::Get), b"/", None, None)
            .unwrap();
        let content_length: usize = resp
            .headers()
            .find(|(n, _)| n.eq_ignore_ascii_case(b"content-length"))
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        let mut body = Vec::with_capacity(content_length);
        resp.body.read_to_end(&mut body).unwrap();
        black_box(&body);
    }
    let elapsed = t0.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let us_per_iter = elapsed.as_secs_f64() * 1e6 / ITERATIONS as f64;
    println!("blocking:  {ITERATIONS} iters in {elapsed:.2?}  ({us_per_iter:.1} µs/iter)");
}

fn run_io_uring(response: Vec<u8>) {
    let port = EchoServer::spawn_owned(response);

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let url = format!("http://127.0.0.1:{port}");
    let mut pool = Pool::<256, 64, 8192, 8192>::new().unwrap();
    let conn = pool.connect(url.as_bytes()).unwrap();

    let t0 = std::time::Instant::now();
    for _i in 0..ITERATIONS {
        let id = pool.get(conn, black_box(b"/")).unwrap();
        let resp = match pool
            .recv(id)
            .unwrap_or_else(|e| panic!("io_uring recv: {e}"))
        {
            xibalba_iouring::driver::ConnResult::Response(r) => r,
            xibalba_iouring::driver::ConnResult::Error { errno, .. } => {
                panic!("io_uring error: errno {errno}")
            }
            xibalba_iouring::driver::ConnResult::ProtocolError { error, .. } => {
                panic!("io_uring protocol error: {error}")
            }
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
        "small" => SMALL_RESP.to_vec(),
        "medium" => medium_resp(),
        "large" => large_resp(),
        other => {
            eprintln!("unknown scenario: {other}. use: small | medium | large");
            std::process::exit(1);
        }
    };

    match mode.as_str() {
        "blocking" => run_blocking(response),
        "uring" => run_io_uring(response),
        other => {
            eprintln!("unknown mode: {other}. use: blocking | uring");
            std::process::exit(1);
        }
    }
}
