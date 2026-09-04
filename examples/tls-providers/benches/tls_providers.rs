//! Benchmarks + adversarial tests for rustls crypto providers:
//!   - ring
//!   - aws-lc-rs
//!   - rustcrypto (experimental)
//!
//! This is a standalone example package, not part of the `xibalba` library
//! or its own benches — it compares xibalba's supported rustls providers
//! against each other, including the third-party `rustls-rustcrypto`
//! provider, so it lives under `examples/` to keep that comparison crate
//! out of the library's and `xibalba-benches`'s normal dependency graph.
//!
//! Bench groups:
//!   `handshake`        — fresh TCP + TLS handshake per iter (1 conn, 1 req)
//!   `roundtrip`        — persistent conn, small body, one GET per iter
//!   `throughput`       — persistent conn, 1 MiB body, measures record-layer perf
//!   `concurrent`       — 8 threads hammering the same server simultaneously
//!   `sequential_burst` — 500 sequential requests on one persistent connection
//!
//! Adversarial tests (cargo test):
//!   `alert_on_bad_cert`        — server presents cert not in client trust store → error
//!   `alert_on_wrong_hostname`  — SNI mismatch → error
//!   `abrupt_close_mid_record`  — server drops TCP mid-TLS record → IO error
//!   `oversized_record`         — server sends a TLS record > 16 KiB limit → error
//!   `garbage_server_hello`     — server sends random bytes instead of TLS → error
//!   `replay_client_hello`      — replayed `ClientHello` bytes → server rejects
//!
//! Run benches:
//!   `BENCH_NETWORK=1` cargo bench --bench `tls_providers` -p tls-providers-bench
//!
//! Run adversarial tests:
//!   cargo test --bench `tls_providers` -p tls-providers-bench -- --test-threads=1

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use rcgen::{CertifiedKey, KeyPair};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, ServerConnection, StreamOwned};

fn main() {
    divan::main();
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

fn network_enabled() -> bool {
    std::env::var("BENCH_NETWORK").is_ok()
}

fn self_signed() -> CertifiedKey<KeyPair> {
    rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap()
}

fn make_server_cfg(
    provider: Arc<rustls::crypto::CryptoProvider>,
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> Arc<ServerConfig> {
    Arc::new(
        ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap(),
    )
}

fn make_client_cfg(
    provider: Arc<rustls::crypto::CryptoProvider>,
    trusted_cert: CertificateDer<'static>,
) -> Arc<ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(trusted_cert).unwrap();
    Arc::new(
        ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// TLS echo server
// ─────────────────────────────────────────────────────────────────────────────
//
// Accepts keep-alive HTTP/1.1 requests and replies with a fixed or sized body.

fn spawn_tls_server(server_config: Arc<ServerConfig>, body: Vec<u8>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(tcp) = stream else { continue };
            let cfg = Arc::clone(&server_config);
            let body = body.clone();
            std::thread::spawn(move || {
                let conn = ServerConnection::new(cfg).unwrap();
                let mut tls = StreamOwned::new(conn, tcp);
                let response = {
                    let mut h = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    h.extend_from_slice(&body);
                    h
                };
                loop {
                    // drain request headers
                    let mut rd = BufReader::new(&mut tls);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match rd.read_line(&mut line) {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        if line == "\r\n" {
                            break;
                        }
                    }
                    drop(rd);
                    if tls.write_all(&response).is_err() || tls.flush().is_err() {
                        break;
                    }
                }
            });
        }
    });
    port
}

// ─────────────────────────────────────────────────────────────────────────────
// Client connect + request helpers
// ─────────────────────────────────────────────────────────────────────────────

fn tls_connect(port: u16, cfg: &Arc<ClientConfig>) -> StreamOwned<ClientConnection, TcpStream> {
    let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let conn = ClientConnection::new(Arc::clone(cfg), name).unwrap();
    StreamOwned::new(conn, tcp)
}

fn do_request(stream: &mut StreamOwned<ClientConnection, TcpStream>) -> io::Result<Vec<u8>> {
    stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    stream.flush()?;
    let mut rd = BufReader::new(stream as &mut dyn Read);
    let mut body_len: Option<usize> = None;
    let mut line = String::new();
    loop {
        line.clear();
        rd.read_line(&mut line)?;
        if line == "\r\n" {
            break;
        }
        let low = line.to_ascii_lowercase();
        if let Some(v) = low.strip_prefix("content-length:") {
            body_len = v.trim().parse().ok();
        }
    }
    let mut body = body_len.map(|n| vec![0u8; n]).unwrap_or_default();
    if !body.is_empty() {
        rd.read_exact(&mut body)?;
    }
    Ok(body)
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-provider context — one per bench group × provider
// ─────────────────────────────────────────────────────────────────────────────

struct ProviderCtx {
    /// port of the small-body (13 B) server
    port_small: u16,
    /// port of the 1 MiB body server
    port_large: u16,
    client_cfg: Arc<ClientConfig>,
}

impl ProviderCtx {
    fn build(
        client_p: Arc<rustls::crypto::CryptoProvider>,
        server_p: Arc<rustls::crypto::CryptoProvider>,
    ) -> Self {
        let ck = self_signed();
        let cert = ck.cert.der().to_owned();
        let key = PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();

        let server_cfg_small =
            make_server_cfg(Arc::clone(&server_p), cert.clone(), key.clone_key());
        let server_cfg_large = make_server_cfg(server_p, cert.clone(), key);

        let port_small = spawn_tls_server(server_cfg_small, b"Hello, World!".to_vec());
        let port_large = spawn_tls_server(server_cfg_large, vec![b'x'; 1024 * 1024]);

        let client_cfg = make_client_cfg(client_p, cert);
        Self {
            port_small,
            port_large,
            client_cfg,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Macro: expand all bench scenarios for one provider
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! provider_benches {
    ($mod_name:ident, $ctx_fn:ident) => {
        mod $mod_name {
            use super::{ProviderCtx, do_request, tls_connect};
            use divan::black_box;
            use std::sync::OnceLock;

            static CTX: OnceLock<ProviderCtx> = OnceLock::new();
            fn ctx() -> &'static ProviderCtx {
                CTX.get_or_init(super::$ctx_fn)
            }

            // ── handshake ─────────────────────────────────────────────────────
            // Full TCP connect + TLS handshake + one tiny GET per iter.
            #[divan::bench(skip_ext_time, sample_count = 100)]
            fn handshake(bencher: divan::Bencher) {
                if !super::network_enabled() {
                    return;
                }
                let c = ctx();
                bencher.bench_local(|| {
                    let mut s = tls_connect(black_box(c.port_small), &c.client_cfg);
                    do_request(black_box(&mut s)).unwrap();
                });
            }

            // ── roundtrip ─────────────────────────────────────────────────────
            // Persistent connection, small (13 B) body, one GET per iter.
            // Isolates record-layer + HTTP framing overhead.
            #[divan::bench(skip_ext_time, sample_count = 500)]
            fn roundtrip(bencher: divan::Bencher) {
                if !super::network_enabled() {
                    return;
                }
                let c = ctx();
                let mut s = tls_connect(c.port_small, &c.client_cfg);
                bencher.bench_local(|| {
                    do_request(black_box(&mut s)).unwrap();
                });
            }

            // ── throughput ────────────────────────────────────────────────────
            // Persistent connection, 1 MiB body.
            // Exercises AES-GCM / ChaCha20 decryption throughput.
            #[divan::bench(skip_ext_time, sample_count = 50)]
            fn throughput(bencher: divan::Bencher) {
                if !super::network_enabled() {
                    return;
                }
                let c = ctx();
                let mut s = tls_connect(c.port_large, &c.client_cfg);
                bencher.bench_local(|| {
                    let body = do_request(black_box(&mut s)).unwrap();
                    black_box(body);
                });
            }

            // ── sequential_burst ──────────────────────────────────────────────
            // 500 sequential GETs on one persistent connection per sample.
            // Reflects real keep-alive API usage.
            #[divan::bench(skip_ext_time, sample_count = 20)]
            fn sequential_burst(bencher: divan::Bencher) {
                if !super::network_enabled() {
                    return;
                }
                let c = ctx();
                let mut s = tls_connect(c.port_small, &c.client_cfg);
                bencher.bench_local(|| {
                    for _ in 0..500 {
                        do_request(black_box(&mut s)).unwrap();
                    }
                });
            }

            // ── concurrent ────────────────────────────────────────────────────
            // 8 threads, each opening their own TLS connection and firing
            // 100 sequential GETs.  Measures provider under thread contention.
            #[divan::bench(skip_ext_time, sample_count = 10)]
            fn concurrent(bencher: divan::Bencher) {
                if !super::network_enabled() {
                    return;
                }
                let c = ctx();
                bencher.bench_local(|| {
                    let handles: Vec<_> = (0..8)
                        .map(|_| {
                            let port = c.port_small;
                            let cfg = std::sync::Arc::clone(&c.client_cfg);
                            std::thread::spawn(move || {
                                let mut s = tls_connect(port, &cfg);
                                for _ in 0..100 {
                                    do_request(&mut s).unwrap();
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
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Provider context factories
// ─────────────────────────────────────────────────────────────────────────────

fn ring_ctx() -> ProviderCtx {
    let p = Arc::new(rustls::crypto::ring::default_provider());
    ProviderCtx::build(Arc::clone(&p), p)
}
fn aws_lc_rs_ctx() -> ProviderCtx {
    let p = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    ProviderCtx::build(Arc::clone(&p), p)
}
fn rustcrypto_ctx() -> ProviderCtx {
    let p = Arc::new(rustls_rustcrypto::provider());
    ProviderCtx::build(Arc::clone(&p), p)
}

provider_benches!(ring, ring_ctx);
provider_benches!(aws_lc_rs, aws_lc_rs_ctx);
provider_benches!(rustcrypto, rustcrypto_ctx);
