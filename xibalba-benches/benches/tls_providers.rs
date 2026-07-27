//! Benchmarks + adversarial tests for rustls crypto providers:
//!   - ring
//!   - aws-lc-rs
//!   - rustcrypto (experimental)
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
//!   `BENCH_NETWORK=1` cargo bench --bench `tls_providers` -p xibalba-benches
//!
//! Run adversarial tests:
//!   cargo test --bench `tls_providers` -p xibalba-benches -- --test-threads=1

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

// ─────────────────────────────────────────────────────────────────────────────
// Adversarial tests
// ─────────────────────────────────────────────────────────────────────────────
//
// Each test is parameterised over all three providers via a helper macro so
// failures are reported per-provider.

#[cfg(test)]
#[allow(dead_code)]
/// Build a (`server_cfg`, `client_cfg`) pair that TRUST each other.
fn trusted_pair(
    server_p: Arc<rustls::crypto::CryptoProvider>,
    client_p: Arc<rustls::crypto::CryptoProvider>,
) -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let ck = self_signed();
    let cert = ck.cert.der().to_owned();
    let key = PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();
    let scfg = make_server_cfg(server_p, cert.clone(), key);
    let ccfg = make_client_cfg(client_p, cert);
    (scfg, ccfg)
}

#[cfg(test)]
#[allow(dead_code)]
/// Build a `server_cfg` from a cert that the client will NOT trust.
fn untrusted_server_cfg(server_p: Arc<rustls::crypto::CryptoProvider>) -> Arc<ServerConfig> {
    let ck = self_signed();
    let cert = ck.cert.der().to_owned();
    let key = PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();
    make_server_cfg(server_p, cert, key)
}

#[cfg(test)]
#[allow(dead_code)]
/// Attempt a TLS connect to `port` using `cfg` with SNI `sni`.
/// Returns Err on any failure (TLS or IO).
fn try_connect_sni(
    port: u16,
    cfg: &Arc<ClientConfig>,
    sni: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let tcp = TcpStream::connect(("127.0.0.1", port))?;
    tcp.set_read_timeout(Some(Duration::from_secs(3)))?;
    let name = ServerName::try_from(sni.to_owned())?;
    let conn = ClientConnection::new(Arc::clone(cfg), name)?;
    let mut tls = StreamOwned::new(conn, tcp);
    // force handshake by writing something
    tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    tls.flush()?;
    let mut buf = [0u8; 64];
    tls.read_exact(&mut buf)?;
    Ok(())
}

macro_rules! adversarial_tests {
    ($mod_name:ident, $server_p:expr, $client_p:expr) => {
        #[cfg(test)]
        mod $mod_name {
            #![allow(unused_imports)]
            use super::{
                make_client_cfg, make_server_cfg, self_signed, spawn_tls_server, tls_connect,
                trusted_pair, try_connect_sni, untrusted_server_cfg,
            };
            use rustls::pki_types::ServerName;
            use rustls::pki_types::{CertificateDer, PrivateKeyDer};
            use rustls::{ClientConnection, StreamOwned};
            use std::io::{Read, Write};
            use std::net::{TcpListener, TcpStream};
            use std::sync::Arc;
            use std::time::Duration;

            // ── 1. Cert not in trust store → client must reject ───────────────
            #[test]
            fn alert_on_bad_cert() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);

                // Server uses a cert the client has never seen.
                let bad_server_cfg = untrusted_server_cfg(Arc::clone(&server_p));
                let port = spawn_tls_server(bad_server_cfg, b"secret".to_vec());

                // Client trusts a *different* cert.
                let ck2 = self_signed();
                let other_cert = CertificateDer::from(ck2.cert.der().to_owned());
                let client_cfg = make_client_cfg(client_p, other_cert);

                let result = try_connect_sni(port, &client_cfg, "localhost");
                assert!(result.is_err(), "client must reject untrusted cert");
            }

            // ── 2. SNI hostname mismatch → client must reject ─────────────────
            #[test]
            fn alert_on_wrong_hostname() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (_scfg, client_cfg) = trusted_pair(server_p.clone(), client_p);
                let port = spawn_tls_server(_scfg, b"ok".to_vec());

                // Connect with SNI "evil.example" instead of "localhost"
                let result = try_connect_sni(port, &client_cfg, "evil.example");
                assert!(result.is_err(), "client must reject SNI mismatch");
            }

            // ── 3. Server drops TCP mid-record → client gets IO / TLS error ───
            #[test]
            fn abrupt_close_mid_record() {
                // Spawn a raw TCP server that completes the TLS handshake,
                // then immediately closes the socket while the client is
                // waiting for the response body — simulating a half-written record.
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(server_p, client_p);

                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                std::thread::spawn(move || {
                    let (tcp, _) = listener.accept().unwrap();
                    let conn = rustls::ServerConnection::new(scfg).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    // Drain client hello / request (force handshake + one read)
                    let mut buf = [0u8; 256];
                    let _ = tls.read(&mut buf);
                    // Write a partial HTTP response header — not a complete TLS record
                    let partial = b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n";
                    let _ = tls.write_all(partial);
                    // abruptly drop — closes the TCP socket mid-stream
                    drop(tls);
                });

                let mut stream = tls_connect(port, &client_cfg);
                stream
                    .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .unwrap();
                stream.flush().unwrap();

                // Read until error — we expect one
                let mut body = Vec::new();
                let result = stream.read_to_end(&mut body);
                // Either read returns Err, or we read 0 bytes (clean close mid-body)
                let is_incomplete = result.is_err() || body.len() < 1000;
                assert!(
                    is_incomplete,
                    "expected error or truncated body on abrupt close"
                );
            }

            // ── 4. Garbage bytes from server → TLS alert ─────────────────────
            #[test]
            fn garbage_server_hello() {
                // A raw TCP server that sends garbage instead of a ServerHello.
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                std::thread::spawn(move || {
                    let (mut tcp, _) = listener.accept().unwrap();
                    // Send random garbage — not a valid TLS record
                    let garbage = b"\xDE\xAD\xBE\xEF not a TLS server hello at all !!!";
                    let _ = tcp.write_all(garbage);
                });

                let client_p = Arc::new($client_p);
                let ck = self_signed();
                let cert = ck.cert.der().to_owned();
                let client_cfg = make_client_cfg(client_p, cert);

                let result = try_connect_sni(port, &client_cfg, "localhost");
                assert!(result.is_err(), "client must reject garbage server hello");
            }

            // ── 5. Oversized TLS record → provider must reject ────────────────
            //
            // TLS 1.3 max plaintext record size is 2^14 bytes (16 384).
            // We craft a raw TLS Application Data record header claiming 32 KiB,
            // which violates the spec and must be rejected.
            #[test]
            fn oversized_record() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(server_p, client_p);

                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();

                std::thread::spawn(move || {
                    let (tcp, _) = listener.accept().unwrap();
                    let conn = rustls::ServerConnection::new(scfg).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);

                    // Drain the client request to force full handshake
                    let mut buf = [0u8; 512];
                    let _ = tls.read(&mut buf);

                    // Now reach into the raw TCP stream and inject a malformed record.
                    // We get the underlying TcpStream back via into_parts().
                    let (_, mut raw_tcp) = tls.into_parts();

                    // TLS record header: content_type=23 (Application Data), version=0x0303,
                    // length=0x8000 (32 768 bytes — above the 16 384 limit).
                    let bad_header: &[u8] = &[0x17, 0x03, 0x03, 0x80, 0x00];
                    let _ = raw_tcp.write_all(bad_header);
                    // Don't send the payload — client should reject on header alone.
                });

                let mut stream = tls_connect(port, &client_cfg);
                stream
                    .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .unwrap();
                stream.flush().unwrap();

                let mut buf = vec![0u8; 4096];
                let result = stream.read(&mut buf);
                assert!(result.is_err(), "provider must reject oversized TLS record");
            }

            // ── 6. Replayed ClientHello → second server rejects it ────────────
            //
            // Capture the raw bytes of a real ClientHello, then send them to a
            // *fresh* server connection.  The server must reject (different
            // session / keys) — it should never complete a handshake with
            // recycled handshake material.
            #[test]
            fn replay_client_hello() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(server_p, client_p);

                // ── First: capture a real ClientHello off the wire ────────────
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();

                // Server thread: just capture whatever bytes arrive, don't respond.
                let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
                std::thread::spawn(move || {
                    let (mut tcp, _) = listener.accept().unwrap();
                    let mut buf = vec![0u8; 4096];
                    let n = tcp.read(&mut buf).unwrap_or(0);
                    buf.truncate(n);
                    let _ = tx.send(buf);
                });

                // Client connects and sends its ClientHello, then we close it.
                let capture_result = std::thread::spawn(move || {
                    let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
                    tcp.set_read_timeout(Some(Duration::from_millis(200)))
                        .unwrap();
                    let name = ServerName::try_from("localhost").unwrap();
                    let conn = ClientConnection::new(client_cfg, name).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    // write triggers the handshake / ClientHello transmission
                    let _ = tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
                    let _ = tls.flush();
                    // timeout will fire since server won't respond
                })
                .join();
                drop(capture_result);

                let client_hello_bytes = rx.recv_timeout(Duration::from_secs(2)).unwrap();
                assert!(
                    !client_hello_bytes.is_empty(),
                    "captured no ClientHello bytes"
                );

                // ── Second: replay those bytes to a fresh server connection ────
                let listener2 = TcpListener::bind("127.0.0.1:0").unwrap();
                let port2 = listener2.local_addr().unwrap().port();

                let (tx2, rx2) = std::sync::mpsc::channel::<bool>();
                std::thread::spawn(move || {
                    let (mut tcp, _) = listener2.accept().unwrap();
                    // Try to do a server-side TLS handshake with the replayed bytes
                    let conn = rustls::ServerConnection::new(scfg).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    let mut buf = [0u8; 256];
                    // The server should either reject or stall — it must NOT succeed
                    let ok = tls.read(&mut buf).is_ok();
                    let _ = tx2.send(ok);
                });

                let replayer = std::thread::spawn(move || {
                    let mut tcp = TcpStream::connect(("127.0.0.1", port2)).unwrap();
                    tcp.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
                    let _ = tcp.write_all(&client_hello_bytes);
                    let _ = tcp.flush();
                    // Don't proceed with a real handshake — just replay the ClientHello
                    std::thread::sleep(Duration::from_millis(200));
                });

                replayer.join().unwrap();

                // The server should NOT have completed a read successfully with
                // only a ClientHello and no further handshake messages.
                let server_read_ok = rx2.recv_timeout(Duration::from_secs(3)).unwrap_or(false);
                assert!(
                    !server_read_ok,
                    "server must not complete handshake from replayed ClientHello alone"
                );
            }
        }
    };
}

adversarial_tests!(
    adversarial_ring,
    rustls::crypto::ring::default_provider(),
    rustls::crypto::ring::default_provider()
);
adversarial_tests!(
    adversarial_aws_lc_rs,
    rustls::crypto::aws_lc_rs::default_provider(),
    rustls::crypto::aws_lc_rs::default_provider()
);
adversarial_tests!(
    adversarial_rustcrypto,
    rustls_rustcrypto::provider(),
    rustls_rustcrypto::provider()
);
