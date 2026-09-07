//! Holds the deferred TLS handshake to the bound its documentation claims.
//!
//! `TcpConnector::connect` returns before the session is verified, so the
//! deadline contract for connecting is satisfied by a dial that cannot
//! overrun. The handshake it defers still has to be bounded by something, and
//! the connector's doc comment names the client's read and write timeouts.
//!
//! That claim had never been exercised, because the connector lived in a
//! `[[bin]]` and nothing could import it. It was also false: the handshake
//! runs inside the client's first *write*, and the write path had no budget
//! above the per-call socket timeout, so the first tick ended the request.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rustls::pki_types::PrivateKeyDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};

use tcp_rustls::{RustlsConfig, TcpConnector};
use xibalba_client::client::{Client, Config};

/// The per-call socket ceiling. Deliberately far shorter than any budget: it
/// exists for cancel latency, and the point of these tests is that it is not
/// a failure threshold.
const READ_TIMEOUT: Duration = Duration::from_millis(300);

/// A TLS server that accepts the connection, then waits before beginning its
/// handshake — healthy, merely slow to start.
struct SlowServer {
    port: u16,
    roots: rustls::RootCertStore,
}

impl SlowServer {
    fn spawn(delay: Duration) -> Self {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = ck.cert.der().to_owned();
        let key = PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.clone()).unwrap();

        let server_config = Arc::new(
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .unwrap(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let Ok((tcp, _)) = listener.accept() else {
                return;
            };
            std::thread::sleep(delay);
            let conn = ServerConnection::new(server_config).unwrap();
            let mut tls = StreamOwned::new(conn, tcp);
            let mut buf = [0u8; 1024];
            let _ = tls.read(&mut buf);
            let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            let _ = tls.flush();
            std::thread::sleep(Duration::from_millis(200));
        });

        Self { port, roots }
    }
}

/// A TCP peer that accepts the connection and never speaks TLS at all: the
/// shape of a blackholed endpoint, which `connect` cannot distinguish from a
/// healthy one because it never waits for a handshake.
struct SilentPeer {
    port: u16,
    stop: Arc<AtomicBool>,
}

impl SilentPeer {
    fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let held = Arc::clone(&stop);
        std::thread::spawn(move || {
            let Ok((mut tcp, _)) = listener.accept() else {
                return;
            };
            let mut sink = [0u8; 4096];
            while !held.load(Ordering::Relaxed) {
                match tcp.read(&mut sink) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        Self { port, stop }
    }
}

impl Drop for SilentPeer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct Probe;

impl Probe {
    fn config(head_silence: Duration) -> Config {
        Config {
            read_timeout: Some(READ_TIMEOUT),
            write_timeout: Some(Duration::from_secs(3)),
            connect_timeout: Some(Duration::from_secs(2)),
            head_silence,
            stream_silence: Duration::from_secs(5),
            ..Config::default()
        }
    }

    fn client<const N: usize>(
        port: u16,
        roots: rustls::RootCertStore,
        config: Config,
    ) -> Client<TcpConnector, N> {
        let tls =
            RustlsConfig::with_provider_and_roots(rustls::crypto::ring::default_provider(), roots)
                .unwrap();
        let url = format!("https://localhost:{port}/").into_bytes();
        Client::connect(&url, tls, config)
            .expect("connect returns before the handshake, so no peer can block it")
    }
}

/// The regression. A server that takes three read timeouts to begin its
/// handshake is healthy, and the 5s head-silence budget covers it easily.
/// Before the write budget existed this failed after one `read_timeout` with
/// a raw `WouldBlock` — "os error 11" — because rustls drives the handshake
/// inside the first `write` and the write path absorbed no ticks.
#[test]
fn a_server_slow_to_begin_its_handshake_is_waited_for_not_failed() {
    let server = SlowServer::spawn(READ_TIMEOUT * 3);
    let started = Instant::now();
    let mut client = Probe::client::<{ 64 * 1024 }>(
        server.port,
        server.roots,
        Probe::config(Duration::from_secs(5)),
    );

    let response = client
        .get(b"/")
        .expect("a slow handshake is silence to wait out, not a failure");

    assert_eq!(response.status.as_u16(), 200);
    assert!(
        started.elapsed() > READ_TIMEOUT,
        "the test did not actually cross a read-timeout tick, so it proves nothing"
    );
}

/// The other half: the wait is bounded. A peer that accepts the connection
/// and never speaks must end the request at the head-silence budget, and the
/// error must name the budget rather than leaking the socket's `WouldBlock`.
#[test]
fn a_peer_that_never_speaks_tls_ends_at_the_budget_not_the_socket_tick() {
    let peer = SilentPeer::spawn();
    let budget = Duration::from_millis(900);
    let started = Instant::now();
    let mut client = Probe::client::<{ 64 * 1024 }>(
        peer.port,
        rustls::RootCertStore::empty(),
        Probe::config(budget),
    );

    let error = client
        .get(b"/")
        .expect_err("a peer that never handshakes must not hang");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= budget,
        "gave up after {elapsed:?}, before the {budget:?} budget: a socket tick ended the request"
    );
    assert!(
        elapsed < budget * 4,
        "took {elapsed:?} against a {budget:?} budget: the wait is not bounded by it"
    );

    let text = error.to_string();
    assert!(
        text.contains("budget"),
        "the caller was given a raw socket error rather than a named budget: {text}"
    );
}
