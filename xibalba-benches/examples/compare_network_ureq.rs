//! xibalba's synchronous `Client` vs the `ureq` crate, over real sockets.
//!
//! Lives under `examples/` (not `benches/`) so `ureq` — a competing HTTP
//! client, not something xibalba needs to build or ship — only enters the
//! dependency graph when this comparison is run, not for a normal build
//! of the library or its own benches.
//!
//! Benches are gated on `BENCH_NETWORK=1` (spins up real TCP servers).
//!
//! Run: `BENCH_NETWORK=1 cargo run -p xibalba-benches --example compare_network_ureq --release -- --bench`

use std::sync::OnceLock;

use xibalba_benches::{EchoServer, PlainConnector};
use xibalba_client::client::Client;
use xibalba_proto::method::Method;

fn main() {
    divan::main();
}

fn network_enabled() -> bool {
    std::env::var("BENCH_NETWORK").is_ok()
}

fn xibalba_client(port: u16) -> Client<PlainConnector> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector>::connect_default(url.as_bytes(), ()).unwrap()
}

fn ureq_agent() -> ureq::Agent {
    ureq::Agent::new_with_defaults()
}

fn make_large_resp() -> Vec<u8> {
    const N: usize = 65536;
    let mut resp =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {N}\r\nConnection: keep-alive\r\n\r\n")
            .into_bytes();
    resp.extend(std::iter::repeat_n(b'x', N));
    resp
}

// 512-byte query string exercising large request serialization.
const LARGE_QUERY: &[u8] = &[b'a'; 512];

static PORT_SMALL_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_SMALL_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_RESP_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_RESP_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_REQ_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_LARGE_REQ_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_STRESS_XIBALBA: OnceLock<u16> = OnceLock::new();
static PORT_STRESS_UREQ: OnceLock<u16> = OnceLock::new();
static PORT_CONCURRENT_UREQ: OnceLock<u16> = OnceLock::new();

fn port_small_xibalba() -> u16 {
    *PORT_SMALL_XIBALBA.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
        )
    })
}
fn port_small_ureq() -> u16 {
    *PORT_SMALL_UREQ.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
        )
    })
}
fn port_large_resp_xibalba() -> u16 {
    *PORT_LARGE_RESP_XIBALBA.get_or_init(|| EchoServer::spawn_owned(make_large_resp()))
}
fn port_large_resp_ureq() -> u16 {
    *PORT_LARGE_RESP_UREQ.get_or_init(|| EchoServer::spawn_owned(make_large_resp()))
}
fn port_large_req_xibalba() -> u16 {
    *PORT_LARGE_REQ_XIBALBA.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
        )
    })
}
fn port_large_req_ureq() -> u16 {
    *PORT_LARGE_REQ_UREQ.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
        )
    })
}
fn port_stress_xibalba() -> u16 {
    *PORT_STRESS_XIBALBA.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
        )
    })
}
fn port_stress_ureq() -> u16 {
    *PORT_STRESS_UREQ.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
        )
    })
}
fn port_concurrent_ureq() -> u16 {
    *PORT_CONCURRENT_UREQ.get_or_init(|| {
        EchoServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!",
        )
    })
}

// ── Scenario: small request, small response (13 B) ───────────────────────────

mod small {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, network_enabled, port_small_ureq, port_small_xibalba, ureq_agent, xibalba_client,
    };

    #[divan::bench(skip_ext_time)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let mut client = xibalba_client(port_small_xibalba());
        bencher.bench_local(|| {
            let mut resp = client
                .request(black_box(Method::Get), b"/", None, None)
                .unwrap();
            let mut body = Vec::new();
            resp.body.read_to_end(&mut body).unwrap();
            black_box(&body);
            drop(resp);
        });
    }

    #[divan::bench(skip_ext_time)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let agent = ureq_agent();
        let url = format!("http://127.0.0.1:{}/", port_small_ureq());
        bencher.bench_local(|| {
            let _body = agent
                .get(black_box(&url))
                .call()
                .unwrap()
                .body_mut()
                .read_to_string()
                .unwrap();
        });
    }
}

// ── Scenario: large response (64 KiB, Content-Length) ────────────────────────

mod large_resp {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, network_enabled, port_large_resp_ureq, port_large_resp_xibalba, ureq_agent,
        xibalba_client,
    };

    #[divan::bench(skip_ext_time)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let mut client = xibalba_client(port_large_resp_xibalba());
        bencher.bench_local(|| {
            let mut resp = client
                .request(black_box(Method::Get), b"/", None, None)
                .unwrap();
            let mut body = Vec::new();
            resp.body.read_to_end(&mut body).unwrap();
            black_box(&body);
            drop(resp);
        });
    }

    #[divan::bench(skip_ext_time)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let agent = ureq_agent();
        let url = format!("http://127.0.0.1:{}/", port_large_resp_ureq());
        bencher.bench_local(|| {
            let _body = agent
                .get(black_box(&url))
                .call()
                .unwrap()
                .body_mut()
                .read_to_string()
                .unwrap();
        });
    }
}

// ── Scenario: large request (512-byte query string) ──────────────────────────

mod large_req {
    use std::io::Read;

    use divan::black_box;

    use super::{
        LARGE_QUERY, Method, network_enabled, port_large_req_ureq, port_large_req_xibalba,
        ureq_agent, xibalba_client,
    };

    #[divan::bench(skip_ext_time)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let mut client = xibalba_client(port_large_req_xibalba());
        bencher.bench_local(|| {
            let mut resp = client
                .request(
                    black_box(Method::Get),
                    b"/search",
                    Some(black_box(LARGE_QUERY)),
                    None,
                )
                .unwrap();
            let mut body = Vec::new();
            resp.body.read_to_end(&mut body).unwrap();
            black_box(&body);
            drop(resp);
        });
    }

    #[divan::bench(skip_ext_time)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let agent = ureq_agent();
        let query = std::str::from_utf8(LARGE_QUERY).unwrap();
        let url = format!("http://127.0.0.1:{}//search?{query}", port_large_req_ureq());
        bencher.bench_local(|| {
            let _body = agent
                .get(black_box(&url))
                .call()
                .unwrap()
                .body_mut()
                .read_to_string()
                .unwrap();
        });
    }
}

// ── Scenario: stress (sequential throughput, 1000 iters per sample) ──────────

mod stress {
    use std::io::Read;

    use divan::black_box;

    use super::{
        Method, network_enabled, port_stress_ureq, port_stress_xibalba, ureq_agent, xibalba_client,
    };

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn xibalba(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let mut client = xibalba_client(port_stress_xibalba());
        bencher.bench_local(|| {
            for _ in 0..1000 {
                let mut resp = client
                    .request(black_box(Method::Get), b"/", None, None)
                    .unwrap();
                let mut body = Vec::new();
                resp.body.read_to_end(&mut body).unwrap();
                black_box(&body);
                drop(resp);
            }
        });
    }

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let agent = ureq_agent();
        let url = format!("http://127.0.0.1:{}/", port_stress_ureq());
        bencher.bench_local(|| {
            for _ in 0..1000 {
                let _body = agent
                    .get(black_box(&url))
                    .call()
                    .unwrap()
                    .body_mut()
                    .read_to_string()
                    .unwrap();
            }
        });
    }
}

// ── Scenario: concurrent (4 threads x 250 requests = 1000 total) ─────────────

const CONNS: usize = 4;
const REQS_PER_CONN: usize = 250;

mod concurrent {
    use divan::black_box;

    use super::{CONNS, REQS_PER_CONN, network_enabled, port_concurrent_ureq, ureq_agent};

    #[divan::bench(skip_ext_time, sample_count = 20)]
    fn ureq(bencher: divan::Bencher) {
        if !network_enabled() {
            return;
        }
        let port = port_concurrent_ureq();

        bencher.bench_local(|| {
            let url = format!("http://127.0.0.1:{port}/");
            let handles: Vec<_> = (0..CONNS)
                .map(|_| {
                    let url = url.clone();
                    let agent = ureq_agent();
                    std::thread::spawn(move || {
                        for _ in 0..REQS_PER_CONN {
                            let body = agent
                                .get(black_box(&url))
                                .call()
                                .unwrap()
                                .body_mut()
                                .read_to_string()
                                .unwrap();
                            black_box(&body);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    }
}
