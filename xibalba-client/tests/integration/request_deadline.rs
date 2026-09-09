//! The total request deadline firing where the silence budgets cannot.
//!
//! `head_silence` and `stream_silence` bound a *gap* between bytes and reset
//! on every byte received. A peer that delivers one byte every so often —
//! a trickle — passes both forever. [`Config::request_deadline`] bounds the
//! whole exchange instead, and is spent cooperatively: time used by one
//! redirect hop is time the next does not have.

use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::park::StopSignal;
use crate::support::script::Script;
use xibalba_client::client::Config;
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::method::Method;

/// Serves the request head, then one body byte every `gap`.
///
/// The head claims a content-length the body never fulfils, so the client
/// keeps waiting for a body that always has another byte coming. With
/// `total_bytes: None` it trickles until released, which only a total
/// deadline can end; with `Some(n)` it closes after `n` bytes, which the
/// client sees as a mid-body EOF.
struct TrickleServer {
    gap: Duration,
    total_bytes: Option<usize>,
}

impl TrickleServer {
    fn spawn(self) -> (StopSignal, JoinHandle<()>, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (stop, park) = StopSignal::new();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
                .unwrap();
            let byte = [b'.'];
            let mut sent = 0usize;
            loop {
                if self.total_bytes.is_some_and(|total| sent >= total) {
                    break;
                }
                stream.write_all(&byte).unwrap();
                stream.flush().unwrap();
                sent += 1;
                if park.wait_for(self.gap) {
                    break;
                }
            }
            drop(stream);
        });
        (stop, server, port)
    }
}

fn config(request_deadline: Option<Duration>) -> Config {
    Config {
        read_timeout: Some(Duration::from_millis(50)),
        // The gap budgets must be out of the way: the subject is the total,
        // and a silence trip here would pass for the wrong reason.
        head_silence: Duration::from_mins(10),
        stream_silence: Duration::from_mins(10),
        request_deadline,
        ..Config::default()
    }
}

#[test]
fn a_trickling_body_hits_the_total_deadline() {
    let (stop, server, port) = TrickleServer {
        gap: Duration::from_millis(10),
        total_bytes: None,
    }
    .spawn();
    let mut client = TestClient::with_config(port, config(Some(Duration::from_millis(400))));

    let start = std::time::Instant::now();
    let err = client
        .request(Method::Get, b"/", None, None)
        .expect_err("a trickle with no end must hit the total");
    let elapsed = start.elapsed();

    assert_eq!(
        err,
        Error::Connection(ConnectionError::RequestDeadlineExceeded),
        "the total must be typed distinctly from a silence gap, got {err:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(300),
        "the deadline should be honoured, not tripped instantly: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "the total must cut the transfer at its deadline, took {elapsed:?}"
    );

    drop(stop);
    server.join().unwrap();
}

/// Redirect hop 1 burns most of the total before answering; hop 2 stalls
/// longer than the remainder but far less than a whole budget.
///
/// With one shared total the second stall cannot finish and the request
/// fails with [`ConnectionError::RequestDeadlineExceeded`]. If the deadline
/// were rebuilt per hop, hop 2 would start with a fresh 500ms, outlast its
/// 300ms stall, and answer — so a per-hop mutation turns this test's
/// `expect_err` into a panic rather than a slower pass.
#[test]
fn redirect_hops_share_one_total_rather_than_earning_one_each() {
    use crate::support::registry::ScriptedServer;

    let target = ScriptedServer::serving_one(
        Script::new()
            .stall(Duration::from_millis(300))
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi".to_vec()),
    );
    let origin = ScriptedServer::serving_one(
        Script::new()
            .stall(Duration::from_millis(400))
            .send(
                format!(
                    "HTTP/1.1 301 Moved Permanently\r\nContent-Length: 0\r\nLocation: http://127.0.0.1:{}/\r\n\r\n",
                    target.port()
                )
                .into_bytes(),
            ),
    );

    let config = Config {
        read_timeout: Some(Duration::from_millis(20)),
        head_silence: Duration::from_mins(10),
        stream_silence: Duration::from_mins(10),
        request_deadline: Some(Duration::from_millis(500)),
        ..Config::default()
    };
    let mut client = TestClient::scripted_with_config(&origin, config);

    let start = std::time::Instant::now();
    let err = client
        .request(Method::Get, b"/", None, None)
        .expect_err("hop 2's stall must outlive the total hop 1 spent most of");
    let elapsed = start.elapsed();

    assert_eq!(
        err,
        Error::Connection(ConnectionError::RequestDeadlineExceeded)
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "the shared total must stop the chain promptly, took {elapsed:?}"
    );
}

/// A peer that accepts the connection and never reads fills the socket
/// buffers, blocking the request write. Before the total deadline reached
/// the write path, only `write_timeout`/`head_silence` bounded this, and
/// both are configured huge here so the total is the only thing that can
/// end it.
#[test]
fn a_blocked_upload_hits_the_total_deadline_rather_than_hanging_on_the_silence_budgets() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, park) = StopSignal::new();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        // Never read: the kernel buffers fill and the client's write blocks.
        park.wait();
        drop(stream);
    });

    let config = Config {
        write_timeout: Some(Duration::from_millis(20)),
        head_silence: Duration::from_mins(10),
        stream_silence: Duration::from_mins(10),
        request_deadline: Some(Duration::from_millis(300)),
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);

    // Far larger than any socket buffer, so the write must block partway.
    let body = vec![b'x'; 8 * 1024 * 1024];
    let start = std::time::Instant::now();
    let err = client
        .request(Method::Post, b"/upload", None, Some(&body))
        .expect_err("a peer that never reads must not stall past the total deadline");
    let elapsed = start.elapsed();

    assert_eq!(
        err,
        Error::Connection(ConnectionError::RequestDeadlineExceeded),
        "a blocked upload must be typed as the total deadline, got {err:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(200),
        "the deadline should be honoured, not tripped instantly: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the total must cut the upload at its deadline, took {elapsed:?}"
    );

    drop(stop);
    server.join().unwrap();
}

/// A deadline that passes between a fully-read response head and the body
/// it declared must leave the connection poisoned, exactly as a transport
/// failure would.
///
/// The head here parses cleanly and the framing is unambiguous — nothing
/// about the head read itself fails — so a client that derived reuse from
/// head acquisition alone would consider the connection clean while its
/// declared body still sits unread on the wire. That body is scripted to
/// eventually deliver bytes shaped like a whole second response; if the
/// next request reused this connection, it would parse them as its own
/// answer instead of reconnecting.
#[test]
fn a_deadline_crossed_between_the_head_and_its_body_poisons_the_connection() {
    use crate::support::registry::ScriptedServer;

    const LEFTOVER: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nEVIL";
    let server = ScriptedServer::serving(vec![
        Script::new()
            .expect_request()
            .send(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                    LEFTOVER.len()
                )
                .into_bytes(),
            )
            // Long enough that the request deadline below always wins the
            // race: the body must still be unread when the deadline fires.
            .stall(Duration::from_millis(500))
            .send(LEFTOVER.to_vec()),
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi".to_vec()),
    ]);

    let config = Config {
        read_timeout: Some(Duration::from_millis(5)),
        head_silence: Duration::from_mins(10),
        stream_silence: Duration::from_mins(10),
        request_deadline: Some(Duration::from_millis(30)),
        ..Config::default()
    };
    let mut client = TestClient::scripted_with_config(&server, config);

    let err = client
        .request(Method::Get, b"/first", None, None)
        .expect_err("the body never arrives before the total deadline");
    assert_eq!(
        err,
        Error::Connection(ConnectionError::RequestDeadlineExceeded),
        "got {err:?}"
    );

    let second = client
        .get(b"/second")
        .expect("the next request must reconnect rather than wait out the stall");
    assert_eq!(
        second.text().unwrap(),
        "hi",
        "a reused connection would have served the leftover body as this \
         response instead"
    );
    assert!(
        server.connection(1).written().contains("GET /second"),
        "the second request must reach the fresh connection scripted for it"
    );
}

/// The same trickle with no total configured must end in the server's
/// close, never in a deadline: the deadline must not leak into requests
/// that did not opt into one.
#[test]
fn the_same_trickle_ends_in_the_close_without_a_deadline() {
    let (stop, server, port) = TrickleServer {
        gap: Duration::ZERO,
        total_bytes: Some(64),
    }
    .spawn();
    let mut client = TestClient::with_config(port, config(None));

    let err = client
        .request(Method::Get, b"/", None, None)
        .expect_err("the promised body never arrives in full; the close ends it");
    assert_ne!(
        err,
        Error::Connection(ConnectionError::RequestDeadlineExceeded),
        "no total was configured; a deadline must not fire"
    );

    drop(stop);
    server.join().unwrap();
}
