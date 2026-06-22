//! Adversarial / robustness tests for rustls crypto providers.
//!
//! Each test is run against ring, aws-lc-rs, and rustcrypto via a macro so
//! failures are attributed per-provider.
//!
//! Run:
//!   cargo test --test tls_adversarial -p xibalba-benches -- --test-threads=4

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use rcgen::{CertifiedKey, KeyPair};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, ServerConnection, StreamOwned};

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

#[allow(dead_code)]
fn self_signed() -> CertifiedKey<KeyPair> {
    rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap()
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
fn trusted_pair(
    server_p: Arc<rustls::crypto::CryptoProvider>,
    client_p: Arc<rustls::crypto::CryptoProvider>,
) -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let ck = self_signed();
    let cert = CertificateDer::from(ck.cert.der().to_owned());
    let key = PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();
    let scfg = make_server_cfg(server_p, cert.clone(), key);
    let ccfg = make_client_cfg(client_p, cert);
    (scfg, ccfg)
}

#[allow(dead_code)]
fn spawn_tls_server(server_config: Arc<ServerConfig>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(tcp) = stream else { continue };
            let cfg = Arc::clone(&server_config);
            std::thread::spawn(move || {
                let conn = ServerConnection::new(cfg).unwrap();
                let mut tls = StreamOwned::new(conn, tcp);
                let mut buf = [0u8; 512];
                let _ = tls.read(&mut buf);
                let _ = tls.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                );
            });
        }
    });
    port
}

#[allow(dead_code)]
fn tls_connect(port: u16, cfg: &Arc<ClientConfig>) -> StreamOwned<ClientConnection, TcpStream> {
    let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let conn = ClientConnection::new(Arc::clone(cfg), name).unwrap();
    StreamOwned::new(conn, tcp)
}

#[allow(dead_code)]
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
    tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    tls.flush()?;
    let mut buf = [0u8; 64];
    tls.read(&mut buf)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Adversarial test macro — expands once per provider
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! adversarial_suite {
    ($mod_name:ident, $server_p:expr, $client_p:expr) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn alert_on_bad_cert() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let ck_server = super::self_signed();
                let server_cert = CertificateDer::from(ck_server.cert.der().to_owned());
                let server_key =
                    PrivateKeyDer::try_from(ck_server.signing_key.serialize_der()).unwrap();
                let bad_scfg = make_server_cfg(server_p, server_cert, server_key);
                let port = spawn_tls_server(bad_scfg);
                let ck_other = super::self_signed();
                let other_cert = CertificateDer::from(ck_other.cert.der().to_owned());
                let client_cfg = make_client_cfg(client_p, other_cert);
                let result = try_connect_sni(port, &client_cfg, "localhost");
                assert!(result.is_err(), "client must reject untrusted cert");
            }

            #[test]
            fn alert_on_wrong_hostname() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(server_p, client_p);
                let port = spawn_tls_server(scfg);
                let result = try_connect_sni(port, &client_cfg, "evil.example");
                assert!(result.is_err(), "client must reject SNI mismatch");
            }

            #[test]
            fn abrupt_close_mid_record() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(server_p, client_p);
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                std::thread::spawn(move || {
                    let (tcp, _) = listener.accept().unwrap();
                    let conn = ServerConnection::new(scfg).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    let mut buf = [0u8; 256];
                    let _ = tls.read(&mut buf);
                    let _ = tls.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 10000\r\n\r\npartial",
                    );
                    drop(tls);
                });
                let mut stream = tls_connect(port, &client_cfg);
                stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
                stream.flush().unwrap();
                let mut body = Vec::new();
                let result = stream.read_to_end(&mut body);
                assert!(
                    result.is_err() || body.len() < 10000,
                    "expected truncated/error on abrupt close"
                );
            }

            #[test]
            fn garbage_server_hello() {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                std::thread::spawn(move || {
                    let (mut tcp, _) = listener.accept().unwrap();
                    let _ = tcp
                        .write_all(b"\xDE\xAD\xBE\xEF this is NOT a TLS server hello !!!");
                });
                let client_p = Arc::new($client_p);
                let ck = super::self_signed();
                let cert = CertificateDer::from(ck.cert.der().to_owned());
                let client_cfg = make_client_cfg(client_p, cert);
                let result = try_connect_sni(port, &client_cfg, "localhost");
                assert!(result.is_err(), "client must reject garbage server hello");
            }

            #[test]
            fn oversized_record() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(server_p, client_p);
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                std::thread::spawn(move || {
                    let (tcp, _) = listener.accept().unwrap();
                    let conn = ServerConnection::new(scfg).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    let mut buf = [0u8; 512];
                    let _ = tls.read(&mut buf);
                    let (_, mut raw) = tls.into_parts();
                    // AppData record claiming 32 768 bytes — above the 16 384 limit
                    let _ = raw.write_all(&[0x17, 0x03, 0x03, 0x80, 0x00]);
                });
                let mut stream = tls_connect(port, &client_cfg);
                stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
                stream.flush().unwrap();
                let mut buf = vec![0u8; 4096];
                let result = stream.read(&mut buf);
                assert!(result.is_err(), "provider must reject oversized TLS record");
            }

            #[test]
            fn replay_client_hello() {
                let server_p = Arc::new($server_p);
                let client_p = Arc::new($client_p);
                let (scfg, client_cfg) = trusted_pair(Arc::clone(&server_p), client_p);

                // Capture a real ClientHello
                let listener1 = TcpListener::bind("127.0.0.1:0").unwrap();
                let port1 = listener1.local_addr().unwrap().port();
                let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
                std::thread::spawn(move || {
                    let (mut tcp, _) = listener1.accept().unwrap();
                    let mut buf = vec![0u8; 4096];
                    let n = tcp.read(&mut buf).unwrap_or(0);
                    buf.truncate(n);
                    let _ = tx.send(buf);
                });
                std::thread::spawn(move || {
                    let tcp = TcpStream::connect(("127.0.0.1", port1)).unwrap();
                    tcp.set_read_timeout(Some(Duration::from_millis(150))).unwrap();
                    let name = ServerName::try_from("localhost").unwrap();
                    let conn = ClientConnection::new(client_cfg, name).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    let _ = tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
                    let _ = tls.flush();
                    let mut buf = [0u8; 64];
                    let _ = tls.read(&mut buf);
                });
                let hello_bytes = rx.recv_timeout(Duration::from_secs(2)).unwrap();
                assert!(!hello_bytes.is_empty());

                // Replay to a fresh server
                let listener2 = TcpListener::bind("127.0.0.1:0").unwrap();
                let port2 = listener2.local_addr().unwrap().port();
                let (tx2, rx2) = std::sync::mpsc::channel::<bool>();
                std::thread::spawn(move || {
                    let (tcp, _) = listener2.accept().unwrap();
                    let conn = ServerConnection::new(scfg).unwrap();
                    let mut tls = StreamOwned::new(conn, tcp);
                    let mut buf = [0u8; 256];
                    let ok = tls.read(&mut buf).is_ok();
                    let _ = tx2.send(ok);
                });
                std::thread::spawn(move || {
                    let mut tcp = TcpStream::connect(("127.0.0.1", port2)).unwrap();
                    tcp.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
                    let _ = tcp.write_all(&hello_bytes);
                    let _ = tcp.flush();
                    std::thread::sleep(Duration::from_millis(300));
                })
                .join()
                .unwrap();

                let server_succeeded =
                    rx2.recv_timeout(Duration::from_secs(3)).unwrap_or(false);
                assert!(
                    !server_succeeded,
                    "server must not complete handshake from replayed ClientHello alone"
                );
            }
        }
    };
}

adversarial_suite!(
    ring,
    rustls::crypto::ring::default_provider(),
    rustls::crypto::ring::default_provider()
);
adversarial_suite!(
    aws_lc_rs,
    rustls::crypto::aws_lc_rs::default_provider(),
    rustls::crypto::aws_lc_rs::default_provider()
);
adversarial_suite!(
    rustcrypto,
    rustls_rustcrypto::provider(),
    rustls_rustcrypto::provider()
);

fn main() {}
