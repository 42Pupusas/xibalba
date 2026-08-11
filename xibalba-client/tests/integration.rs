use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use xibalba_client::async_client::{AsyncClient, Chunk};
use xibalba_client::client::{Client, Config};

const SMALL_HEAD_SIZE: usize = 256;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::method::Method;
use xibalba_client::proto::url::Url;

// ── Plain TCP connector ───────────────────────────────────────────────────────

struct PlainConnector;
struct PlainStream(TcpStream);

impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> std::io::Result<()> {
        self.0.set_read_timeout(dur)
    }
}

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

impl Connector for PlainConnector {
    type Stream = PlainStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, _tls_config: &()) -> Result<PlainStream, Error> {
        let host = std::str::from_utf8(url.host).map_err(|_| {
            Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into()))
        })?;
        let addr = format!("{}:{}", host, url.effective_port());
        TcpStream::connect(&addr)
            .map_err(Error::from)
            .map(PlainStream)
    }
}

// ── Test server helpers ───────────────────────────────────────────────────────

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 4096];
    let mut acc = Vec::new();
    loop {
        let n = stream.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    acc
}

fn one_shot_server(response: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.write_all(response).unwrap();
    });
    (port, handle)
}

fn connect(port: u16) -> Client<PlainConnector> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector>::connect_default(url.as_bytes(), ()).unwrap()
}

fn connect_with_config(port: u16, config: Config) -> Client<PlainConnector> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector>::connect(url.as_bytes(), (), config).unwrap()
}

fn connect_with_small_head_limit(
    port: u16,
    config: Config,
) -> Client<PlainConnector, SMALL_HEAD_SIZE> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector, SMALL_HEAD_SIZE>::connect(url.as_bytes(), (), config).unwrap()
}

fn connect_async(port: u16) -> AsyncClient {
    let url = format!("http://127.0.0.1:{port}/");
    AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), Config::default()).unwrap()
}

// ── AsyncClient tests ────────────────────────────────────────────────────────

#[test]
fn async_silently_dead_stream_surfaces_error_and_recovers() {
    // Regression test for the wedged-reader bug. A server that stops
    // sending mid-SSE without closing the socket (NAT drop, silent
    // middlebox reset) used to park the reader thread in an unbounded
    // WouldBlock retry loop: the caller hung forever in pop_block, and
    // every later request queued behind the dead read — the client
    // could never be restarted. With the stall cap, the stream must
    // yield Chunk::Error within a few read-timeout windows and the next
    // request must be served on a fresh connection.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        // First connection: send head + one chunk, then go silent
        // WITHOUT closing the socket. Hold it open until the test ends.
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Second connection: serve the recovery request.
        let (mut stream2, _) = listener.accept().unwrap();
        read_request(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        stream2.flush().unwrap();
        // Only now let the first (dead) connection drop.
        drop(stream);
    });

    // Short read timeout + short silence budget so the stall trips quickly.
    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        stream_silence: Duration::from_millis(300),
        ..Config::default()
    };
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config).unwrap();

    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle.next_block(), Some(Chunk::Body(b"first".to_vec())));

    // The silent stall must surface as an error, not hang forever.
    let start = std::time::Instant::now();
    let chunk = handle.next_block();
    assert!(
        matches!(chunk, Some(Chunk::Error(_))),
        "expected stall error, got {chunk:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stall detection took too long: {:?}",
        start.elapsed()
    );

    // The client must be restartable: the next request reconnects
    // (dirty connection) and gets a clean, correct response.
    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle2.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle2.next_block(), Some(Chunk::Body(b"fresh".to_vec())));
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn async_stale_cancel_does_not_abort_the_next_request() {
    // Regression test for the cancel-cascade bug. Cancels travel on the
    // control ring shared by every request, and used to carry no
    // identity: the reader treated the next `Cancel` it saw as applying
    // to whatever request was in flight *at that moment*. So a caller
    // that gave up on request A (an application-level head timeout, say)
    // and cancelled it a moment too late would have that cancel land on
    // request B, aborting a perfectly healthy response. One flaky
    // request thereby killed the following turn, and the conversation
    // could not make progress. Cancels are now addressed to a ticket, so
    // a cancel for a finished request is discarded.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        // Request A: a complete, clean response.
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
            .unwrap();
        stream.flush().unwrap();

        // Request B on the same kept-alive connection: send the head and
        // a first chunk, then hold the stream open. B is *mid-body* —
        // exactly when the reader polls the control ring between reads,
        // and so exactly when a stale cancel would be misapplied to it.
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nsecond\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(400));
        stream.write_all(b"5\r\nthird\r\n0\r\n\r\n").unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });

    let client = connect_async(port);

    // Request A: consume it fully, so it is finished and its ticket retired.
    let mut handle_a = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle_a.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle_a.next_block(), Some(Chunk::Body(b"first".to_vec())));
    assert_eq!(handle_a.next_block(), Some(Chunk::Eof));

    // Start B and let it get mid-body, so the reader is inside the
    // streaming loop that polls the control ring between reads.
    let mut handle_b = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle_b.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle_b.next_block(), Some(Chunk::Body(b"second".to_vec())));

    // Only now cancel A — far too late, and while B is streaming. Before
    // tickets, the reader applied this to B and aborted it.
    handle_a.cancel().unwrap();

    // B must run to completion regardless.
    match handle_b.next_block() {
        Some(Chunk::Body(b)) => assert_eq!(b, b"third".to_vec()),
        Some(Chunk::Aborted) => {
            panic!("stale cancel for a finished request aborted the in-flight request")
        }
        other => panic!("expected request B to keep streaming, got {other:?}"),
    }
    assert_eq!(handle_b.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn async_queued_request_reports_when_it_reaches_the_wire() {
    // Regression test for the "timed out waiting for response headers"
    // wedge. Requests serialize through one reader thread, so a request
    // submitted while another is streaming sits in the control ring,
    // unsent. A caller enforcing its own head deadline from submit time
    // therefore timed out against a request that had never been written
    // — reporting a transport failure for bytes that never left the
    // machine, and (with an unscoped cancel) taking the healthy
    // in-flight request down with it. `has_started` lets the caller
    // start its clock when the request actually reaches the socket.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Request A: head, one chunk, then a deliberate pause holding
        // the reader thread busy while B waits in the queue.
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(500));
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();

        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });

    let client = connect_async(port);

    let mut handle_a = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle_a.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle_a.next_block(), Some(Chunk::Body(b"first".to_vec())));

    // B is submitted while A still owns the reader: queued, not sent.
    let mut handle_b = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(
        !handle_b.has_started(),
        "a request queued behind a live stream must not count as started"
    );

    // Once A finishes, B reaches the wire and reports it.
    assert_eq!(handle_a.next_block(), Some(Chunk::Eof));
    assert!(matches!(
        handle_b.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert!(
        handle_b.has_started(),
        "a request that produced a head must report as started"
    );
    assert_eq!(handle_b.next_block(), Some(Chunk::Body(b"second".to_vec())));

    server.join().unwrap();
}

#[test]
fn async_slow_head_beyond_read_timeout_still_succeeds() {
    // Regression test for the "os error 11" leak. A server whose first
    // response byte arrives long after `read_timeout` (inference
    // providers queue + prompt-process before emitting the SSE head)
    // used to exhaust the head path's fixed retry *count* and surface
    // the raw EAGAIN/WouldBlock to the caller. With a wall-clock
    // `head_silence` budget decoupled from `read_timeout`, a slow but
    // healthy head must succeed.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        // Stay silent well past several read-timeout ticks (the old
        // cap was 3 retries × read_timeout = 150ms here).
        thread::sleep(Duration::from_millis(600));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nslow")
            .unwrap();
    });

    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        head_silence: Duration::from_secs(5),
        ..Config::default()
    };
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config).unwrap();

    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(
        matches!(handle.next_block(), Some(Chunk::Head { status: 200, .. })),
        "slow head must not surface a WouldBlock error"
    );
    assert_eq!(handle.next_block(), Some(Chunk::Body(b"slow".to_vec())));
    assert_eq!(handle.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn async_failed_reconnect_surfaces_error_not_desync() {
    // Regression test for the swallowed-reconnect bug. When the
    // connection is dirty (abandoned streaming body) and the reconnect
    // in `process_request` fails, the reader used to ignore the failure
    // (`let _ = client.ensure_clean()`) and send the next request over
    // the still-dirty socket — parsing the stale leftover body as the
    // new response's head and desyncing every response afterwards,
    // permanently. A failed reconnect must surface as Chunk::Error and
    // leave the connection dirty so a later request retries cleanly.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        // One connection only: send a response the client abandons
        // mid-body (chunked, never terminated), leaving it dirty.
        let (mut stream, _) = listener.accept().unwrap();
        // Close the listener immediately — otherwise a reconnect attempt
        // would sit in the kernel accept backlog and "succeed". With it
        // gone, reconnects get ECONNREFUSED, which is the scenario under
        // test.
        drop(listener);
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Hold the socket open while the test runs, then drop.
        thread::sleep(Duration::from_secs(2));
        drop(stream);
        // Listener drops here: all reconnect attempts to this port fail.
    });

    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        ..Config::default()
    };
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config).unwrap();

    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle.next_block(), Some(Chunk::Body(b"first".to_vec())));
    // Abandon the stream mid-body: cancel and drop the handle. The
    // connection is now dirty.
    handle.cancel().unwrap();
    drop(handle);

    // Give the reader time to observe the cancel.
    thread::sleep(Duration::from_millis(200));

    // The server's listener is about to be unreachable for reconnects
    // (single-accept). The next request must fail loudly with a
    // connection error — NOT silently parse the stale "first" chunk
    // remnants as its own response.
    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    match handle2.next_block() {
        Some(Chunk::Error(_)) => {} // reconnect failed loudly — correct
        Some(Chunk::Head { .. }) => {
            // A Head here could only come from stale bytes (the server
            // never serves a second request).
            panic!("desync: stale bytes parsed as a fresh response head");
        }
        other => panic!("expected Chunk::Error, got {other:?}"),
    }

    server.join().unwrap();
}

#[test]
fn async_dropped_handle_before_head_does_not_desync_next_request() {
    // Regression test for the head-push desync bug. If the caller drops
    // its StreamHandle before the reader delivers Chunk::Head (e.g. an
    // application-level header timeout), the reader used to bail out
    // with the response body still unread and the connection NOT marked
    // dirty. The next request then reused the socket and parsed the
    // previous response's leftover body as its own head — silently
    // receiving the wrong response. The reader must mark the connection
    // dirty the moment an unread body exists so the next request
    // reconnects.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        // First connection: a complete buffered response the client
        // will abandon before reading. Its bytes sit unread on the
        // socket — exactly the stale prefix that used to be parsed as
        // the next response's head.
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nstale")
            .unwrap();
        stream.flush().unwrap();

        // Second connection: the follow-up request must land here.
        let (mut stream2, _) = listener.accept().unwrap();
        read_request(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        stream2.flush().unwrap();
        drop(stream);
    });

    let client = connect_async(port);

    // Submit and immediately drop the handle — before the reader can
    // push Chunk::Head. The push then fails and the reader bails with
    // the body unread.
    let handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    drop(handle);

    // Give the reader time to hit the failed head push.
    thread::sleep(Duration::from_millis(200));

    // The follow-up must see the fresh response, not the stale body.
    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle2.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(
        handle2.next_block(),
        Some(Chunk::Body(b"fresh".to_vec())),
        "second response was corrupted by the abandoned response's leftovers"
    );
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn async_cancel_mid_stream_then_next_request_is_clean() {
    // Regression test for the mid-stream cancel corruption bug.
    // Cancelling (dropping) a StreamHandle before the response body is
    // fully consumed must mark the underlying connection dirty so the
    // next request reconnects. Otherwise the next response is parsed
    // from leftover body bytes of the previous response and is corrupted.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        // First request: long chunked body; client will cancel after the first chunk.
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Stall. If the client reuses this connection without reconnecting,
        // it will read this leftover body as the next response head.
        thread::sleep(Duration::from_millis(200));
        let _ = stream.write_all(b"5\r\nstale\r\n0\r\n\r\n");
        let _ = stream.flush();
        drop(stream);

        // Second connection: the follow-up request after the cancel.
        let (mut stream2, _) = listener.accept().unwrap();
        read_request(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        stream2.flush().unwrap();
    });

    let client = connect_async(port);

    // Submit first request, read the head and first chunk, then cancel.
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    let chunk = handle.next_block().unwrap();
    assert!(
        matches!(chunk, Chunk::Head { status: 200, .. }),
        "expected 200 head, got {chunk:?}"
    );
    assert_eq!(handle.next_block(), Some(Chunk::Body(b"first".to_vec())));
    handle.cancel().unwrap(); // cancel mid-stream

    // Submit second request. With the bug, this reads stale body bytes as a head.
    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    let chunk = handle2.next_block().unwrap();
    assert!(
        matches!(chunk, Chunk::Head { status: 200, .. }),
        "expected 200 head on second request, got {chunk:?}"
    );
    assert_eq!(
        handle2.next_block(),
        Some(Chunk::Body(b"fresh".to_vec())),
        "second response body was corrupted by first response leftovers"
    );
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn async_drained_stream_is_reused() {
    // Fully draining a streaming response must leave the connection clean
    // and reusable, exactly like the synchronous path. Dropping the handle
    // does not cancel.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
            .unwrap();
        stream.flush().unwrap();

        // Same connection must be reused.
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond")
            .unwrap();
        stream.flush().unwrap();
    });

    let client = connect_async(port);

    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle.next_block(), Some(Chunk::Body(b"first".to_vec())));
    assert_eq!(handle.next_block(), Some(Chunk::Eof));
    drop(handle);

    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle2.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle2.next_block(), Some(Chunk::Body(b"second".to_vec())));
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn async_request_queued_during_stream_is_not_dropped() {
    // Regression test for the dropped-request bug. While a streaming
    // response is being read, the reader polls the control ring for a
    // cancel before every socket read. That poll used a destructive
    // `pop()`, so a `Control::Request` submitted mid-stream sat at the
    // head of the ring, got popped by the cancel check, and was silently
    // discarded — the second request vanished and its handle never
    // produced a head.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();

        // Request 1: chunked. Send the first chunk, then stall so the
        // client reads it and submits request 2 while the reader is
        // parked in a socket read; then send the rest.
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
        stream.write_all(b"6\r\nsecond\r\n").unwrap();
        stream.flush().unwrap();
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();

        // Request 2 reuses the same keep-alive connection.
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();
        stream.flush().unwrap();
    });

    let client = connect_async(port);

    // Start streaming request 1 and read its head + first chunk.
    let mut handle1 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle1.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle1.next_block(), Some(Chunk::Body(b"first".to_vec())));

    // Submit request 2 while request 1 is still streaming. The reader is
    // parked in a socket read for request 1, so this Request lands on the
    // control ring and is seen by the next cancel poll.
    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();

    // Drain the rest of request 1.
    assert_eq!(handle1.next_block(), Some(Chunk::Body(b"second".to_vec())));
    assert_eq!(handle1.next_block(), Some(Chunk::Eof));

    // Request 2 must still be served, not silently dropped.
    let chunk = handle2.next_block();
    assert!(
        matches!(chunk, Some(Chunk::Head { status: 200, .. })),
        "second request was dropped mid-stream: expected 200 head, got {chunk:?}"
    );
    assert_eq!(handle2.next_block(), Some(Chunk::Body(b"hello".to_vec())));
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

// ── Original tests (updated API) ─────────────────────────────────────────────

#[test]
fn get_content_length_response() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn get_chunked_response() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nworld\r\n0\r\n\r\n";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "world");
    server.join().unwrap();
}

#[test]
fn get_no_body_204() {
    let response = b"HTTP/1.1 204 No Content\r\n\r\n";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(
        resp.status,
        xibalba_client::proto::status::StatusCode::NO_CONTENT
    );
    assert!(resp.text().unwrap().is_empty());
    server.join().unwrap();
}

#[test]
fn get_until_close_response() {
    let response = b"HTTP/1.1 200 OK\r\n\r\nuntil close body";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "until close body");
    server.join().unwrap();
}

#[test]
fn response_headers_accessible() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nhi";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    let ct = resp
        .headers()
        .find(|(name, _)| *name == b"Content-Type")
        .map(|(_, v)| v);
    assert_eq!(ct, Some(b"text/plain" as &[u8]));
    server.join().unwrap();
}

#[test]
fn connection_refused_returns_error() {
    let result = Client::<PlainConnector>::connect_default(b"http://127.0.0.1:1/", ());
    assert!(result.is_err());
}

// ── Adversarial integration tests ────────────────────────────────────────────

fn drip_server(response: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        for &b in response {
            stream.write_all(&[b]).unwrap();
        }
    });
    (port, handle)
}

fn split_server(response: &'static [u8], split_at: usize) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let mid = split_at.min(response.len());
        stream.write_all(&response[..mid]).unwrap();
        stream.flush().unwrap();
        stream.write_all(&response[mid..]).unwrap();
    });
    (port, handle)
}

fn keepalive_server(r1: &'static [u8], r2: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        for response in [r1, r2] {
            let mut acc = Vec::new();
            loop {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    return;
                }
                acc.extend_from_slice(&buf[..n]);
                if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            stream.write_all(response).unwrap();
            stream.flush().unwrap();
        }
    });
    (port, handle)
}

#[test]
fn server_sends_response_byte_at_a_time() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
    let (port, server) = drip_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "abc");
    server.join().unwrap();
}

#[test]
fn server_sends_headers_split_across_reads() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let (port, server) = split_server(response, 20);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn server_sends_empty_chunked_body() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}

#[test]
fn streaming_chunked_delivers_incrementally() {
    // Server sends one chunk, pauses, then sends the rest. A streaming
    // reader must surface the first chunk before the pause ends —
    // proving bytes flow through without waiting for the terminator.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(400));
        stream.write_all(b"6\r\nsecond\r\n0\r\n\r\n").unwrap();
        stream.flush().unwrap();
        // Serve a follow-up request to prove the connection stays
        // reusable after a fully-drained stream.
        let mut acc = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let n = stream.read(&mut buf).unwrap();
            assert!(n > 0, "client closed instead of reusing connection");
            acc.extend_from_slice(&buf[..n]);
            if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });

    let mut client = connect(port);
    let start = std::time::Instant::now();
    let mut resp = client
        .send_streaming(client.build(Method::Get, b"/"))
        .expect("streaming request failed");

    let mut buf = [0u8; 64];
    let n = resp.body.read(&mut buf).unwrap();
    let first_elapsed = start.elapsed();
    assert_eq!(&buf[..n], b"first");
    assert!(
        first_elapsed < Duration::from_millis(300),
        "first chunk should arrive before the server's pause ends, took {first_elapsed:?}"
    );

    let mut remainder = Vec::new();
    resp.body.read_to_end(&mut remainder).unwrap();
    assert_eq!(remainder, b"second");
    assert!(resp.body.is_done());
    drop(resp);

    // Connection must be reusable without reconnecting.
    let resp2 = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp2.text().unwrap(), "ok");
    server.join().unwrap();
}

#[test]
fn streaming_dropped_midway_reconnects() {
    // Drop the streaming response before draining it; the next request
    // must reconnect instead of reading the stale body.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        // First connection: send a body the client will abandon.
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nstale\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Second connection: serve the follow-up request.
        let (mut stream2, _) = listener.accept().unwrap();
        read_request(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        drop(stream);
    });

    let mut client = connect(port);
    let mut resp = client
        .send_streaming(client.build(Method::Get, b"/"))
        .expect("streaming request failed");
    let mut buf = [0u8; 5];
    resp.body.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"stale");
    assert!(!resp.body.is_done());
    drop(resp); // abandon mid-stream

    let resp2 = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp2.text().unwrap(), "fresh");
    server.join().unwrap();
}

#[test]
fn streaming_content_length_body() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nhello world";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let mut resp = client
        .send_streaming(client.build(Method::Get, b"/"))
        .unwrap();
    let mut body = String::new();
    resp.body.read_to_string(&mut body).unwrap();
    assert_eq!(body, "hello world");
    assert!(resp.body.is_done());
    server.join().unwrap();
}

#[test]
fn stale_keepalive_reconnects_and_retries() {
    // Server accepts a first connection, serves one response, then
    // closes it (simulating an idle keep-alive timeout). The second
    // request finds the connection dead on read; the client must
    // reconnect to a fresh connection and succeed transparently.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let r1 = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst";
        let r2 = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond";

        let (mut s1, _) = listener.accept().unwrap();
        read_request(&mut s1);
        s1.write_all(r1).unwrap();
        s1.flush().unwrap();
        // Close the first connection: the client's next request will
        // hit EOF on this socket.
        drop(s1);

        let (mut s2, _) = listener.accept().unwrap();
        read_request(&mut s2);
        s2.write_all(r2).unwrap();
        s2.flush().unwrap();
    });

    let mut client = connect(port);
    let first = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(first.text().unwrap(), "first");

    // Reuses the (now dead) connection, detects EOF, reconnects, retries.
    let second = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(second.text().unwrap(), "second");
    server.join().unwrap();
}

#[test]
fn stale_before_first_request_reconnects_and_retries() {
    // The connection can die before it ever serves a request: a host
    // that connects at startup and sends its first request much later
    // (the server times the idle connection out in between). The retry
    // must cover this case too — there is no response in flight, so
    // resending is safe.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        // Accept the initial connection and close it immediately,
        // without serving anything.
        let (s1, _) = listener.accept().unwrap();
        drop(s1);

        let (mut s2, _) = listener.accept().unwrap();
        read_request(&mut s2);
        s2.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();
        s2.flush().unwrap();
    });

    let mut client = connect(port);
    // First request ever on this client hits the dead socket; the
    // client must reconnect and retry transparently.
    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn chunked_data_and_terminator_in_same_read() {
    // Regression: when one read delivers both chunk data and the
    // terminal "0\r\n\r\n", the decoder reaches Done internally but
    // reports Data (data takes priority). The body reader must notice
    // completion instead of issuing another read that blocks until the
    // server gives up — observed live against CloudFront, where TLS
    // record boundaries decide whether the terminator shares a read
    // with the data.
    let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    let body = b"5\r\nhello\r\n0\r\n\r\n";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.set_nodelay(true).unwrap();
        // Two writes with a pause so the head arrives alone and the
        // entire chunked body (data + terminator) lands in one read.
        stream.write_all(head).unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
        // Hold the connection open: a buggy client blocks here.
        thread::sleep(Duration::from_millis(500));
    });

    let config = Config {
        read_timeout: Some(Duration::from_secs(2)),
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn multiple_requests_same_connection() {
    let r1: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst";
    let r2: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond";
    let (port, server) = keepalive_server(r1, r2);
    let mut client = connect(port);

    let resp1 = client.request(Method::Get, b"/one", None, None).unwrap();
    assert_eq!(resp1.text().unwrap(), "first");

    let resp2 = client.request(Method::Get, b"/two", None, None).unwrap();
    assert_eq!(resp2.text().unwrap(), "second");

    server.join().unwrap();
}

#[test]
fn very_large_header_value() {
    let big_value = "X".repeat(4096);
    let response_str =
        format!("HTTP/1.1 200 OK\r\nX-Big: {big_value}\r\nContent-Length: 2\r\n\r\nok");
    let response_bytes: &'static [u8] = Box::leak(response_str.into_bytes().into_boxed_slice());
    let (port, server) = one_shot_server(response_bytes);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    let big_hdr = resp
        .headers()
        .find(|(name, _)| *name == b"X-Big")
        .map(|(_, v)| v);
    assert_eq!(big_hdr.map(<[u8]>::len), Some(4096));
    assert_eq!(resp.text().unwrap(), "ok");
    server.join().unwrap();
}

#[test]
fn server_closes_connection_before_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        drop(stream);
    });
    let mut client = connect(port);
    let result = client.request(Method::Get, b"/", None, None);
    assert!(result.is_err());
    server.join().unwrap();
}

#[test]
fn response_with_many_headers() {
    let mut response = b"HTTP/1.1 200 OK\r\n".to_vec();
    for i in 0..30 {
        response.extend_from_slice(format!("X-Header-{i}: value-{i}\r\n").as_bytes());
    }
    response.extend_from_slice(b"Content-Length: 4\r\n\r\ndone");
    let response_bytes: &'static [u8] = Box::leak(response.into_boxed_slice());

    let (port, server) = one_shot_server(response_bytes);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    let header_count = resp.headers().count();
    assert!(header_count >= 30);
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}

// ── Host header tests ────────────────────────────────────────────────────────

fn echo_request_server() -> (u16, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let req = read_request(&mut stream);
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        stream.write_all(response).unwrap();
        req
    });
    (port, handle)
}

#[test]
fn host_header_sent() {
    let (port, server) = echo_request_server();
    let mut client = connect(port);
    client.get(b"/test").unwrap();
    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains(&format!("Host: 127.0.0.1:{port}")),
        "expected Host header with port, got:\n{req_str}"
    );
}

#[test]
fn host_header_with_default_port() {
    let (port, server) = echo_request_server();
    // Non-default port so Host should include it
    let mut client = connect(port);
    client.get(b"/").unwrap();
    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains("Host: 127.0.0.1:"),
        "Host header must include non-default port"
    );
}

// ── Timeout tests ────────────────────────────────────────────────────────────

#[test]
fn timeout_fires_on_stalled_server() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        // Never send a response — just sleep
        thread::sleep(Duration::from_secs(10));
        drop(stream);
    });

    // Silence tolerance is the explicit head_silence budget, not a
    // multiple of read_timeout — configure it short so the test is fast.
    let config = Config {
        read_timeout: Some(Duration::from_millis(100)),
        head_silence: Duration::from_millis(400),
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let start = std::time::Instant::now();
    let result = client.request(Method::Get, b"/", None, None);
    assert!(result.is_err(), "expected timeout error");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "should time out quickly"
    );
    // The surfaced error must be a descriptive TimedOut, never the raw
    // EAGAIN/WouldBlock ("os error 11") from the socket.
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("silence budget"),
        "expected the silence-budget error, got: {msg}"
    );
    drop(server);
}

// ── Request body tests ───────────────────────────────────────────────────────

fn echo_body_server() -> (u16, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let req_head = read_request(&mut stream);
        let req_str = String::from_utf8_lossy(&req_head);
        let content_length: usize = req_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);

        // Read request body
        let head_end = req_head.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already_read = req_head.len() - head_end;
        let mut body = req_head[head_end..].to_vec();
        if body.len() < content_length {
            let remaining = content_length - already_read;
            let mut rest = vec![0u8; remaining];
            stream.read_exact(&mut rest).unwrap();
            body.extend_from_slice(&rest);
        }
        body.truncate(content_length);

        // Echo the body back
        let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
        body
    });
    (port, handle)
}

#[test]
fn post_with_body() {
    let (port, server) = echo_body_server();
    let mut client = connect(port);

    let resp = client.post(b"/submit", b"hello world").unwrap();
    assert_eq!(resp.text().unwrap(), "hello world");
    let echoed = server.join().unwrap();
    assert_eq!(echoed, b"hello world");
}

#[test]
fn post_with_empty_body() {
    let (port, server) = echo_body_server();
    let mut client = connect(port);

    let resp = client
        .request(Method::Post, b"/submit", None, Some(b""))
        .unwrap();
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}

// ── Redirect tests ───────────────────────────────────────────────────────────

fn redirect_server(
    redirect_status: u16,
    location: &'static str,
    final_response: &'static [u8],
) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // First request: redirect
        read_request(&mut stream);
        let redirect = format!(
            "HTTP/1.1 {redirect_status} Redirect\r\nContent-Length: 0\r\nLocation: {location}\r\n\r\n"
        );
        stream.write_all(redirect.as_bytes()).unwrap();
        stream.flush().unwrap();

        // Second request: final response
        read_request(&mut stream);
        stream.write_all(final_response).unwrap();
    });
    (port, handle)
}

#[test]
fn redirect_301_followed() {
    let final_resp = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone";
    let (port, server) = redirect_server(301, "/final", final_resp);
    let mut client = connect(port);

    let resp = client.get(b"/start").unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}

#[test]
fn redirect_chain() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Hop 1: 301 → /hop2
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /hop2\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Hop 2: 302 → /final
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /final\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Final
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\narrived")
            .unwrap();
    });
    let mut client = connect(port);
    let resp = client.get(b"/start").unwrap();
    assert_eq!(resp.text().unwrap(), "arrived");
    server.join().unwrap();
}

#[test]
fn redirect_max_exceeded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Infinite redirect loop
        loop {
            let req = read_request(&mut stream);
            if req.is_empty() {
                break;
            }
            let resp = b"HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /loop\r\n\r\n";
            if stream.write_all(resp).is_err() {
                break;
            }
            stream.flush().ok();
        }
    });

    let config = Config {
        max_redirects: 3,
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let result = client.get(b"/start");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::TooManyRedirects)
    );
    drop(server);
}

#[test]
fn redirect_307_preserves_method_and_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // First request: 307 redirect
        let req1 = read_request(&mut stream);
        assert!(String::from_utf8_lossy(&req1).starts_with("POST "));
        stream
            .write_all(b"HTTP/1.1 307 Temporary\r\nContent-Length: 0\r\nLocation: /target\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Read the body from first request
        let req1_str = String::from_utf8_lossy(&req1);
        let cl: usize = req1_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let head_end = req1.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already = req1.len() - head_end;
        if already < cl {
            let mut rest = vec![0u8; cl - already];
            stream.read_exact(&mut rest).ok();
        }

        // Second request: should still be POST with body
        let req2 = read_request(&mut stream);
        let req2_str = String::from_utf8_lossy(&req2);
        assert!(
            req2_str.starts_with("POST "),
            "expected POST after 307, got: {req2_str}"
        );
        // Read the body
        let cl2: usize = req2_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let head_end2 = req2.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already2 = req2.len() - head_end2;
        let mut body2 = req2[head_end2..].to_vec();
        if already2 < cl2 {
            let mut rest = vec![0u8; cl2 - already2];
            stream.read_exact(&mut rest).ok();
            body2.extend_from_slice(&rest);
        }
        body2.truncate(cl2);

        let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body2.len());
        stream.write_all(resp.as_bytes()).unwrap();
        stream.write_all(&body2).unwrap();
    });

    let mut client = connect(port);
    let resp = client.post(b"/original", b"preserved").unwrap();
    assert_eq!(resp.text().unwrap(), "preserved");
    server.join().unwrap();
}

// ── Builder / custom header tests ────────────────────────────────────────────

#[test]
fn builder_sends_custom_headers() {
    let (port, server) = echo_request_server();
    let mut client = connect(port);

    client
        .send(
            client
                .build(Method::Get, b"/api")
                .header(b"Authorization", b"Bearer tok123")
                .header(b"Content-Type", b"application/json"),
        )
        .unwrap();

    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains("Authorization: Bearer tok123"),
        "missing Authorization header:\n{req_str}"
    );
    assert!(
        req_str.contains("Content-Type: application/json"),
        "missing Content-Type header:\n{req_str}"
    );
}

#[test]
fn builder_with_body_and_headers() {
    let (port, server) = echo_body_server();
    let mut client = connect(port);

    let resp = client
        .send(
            client
                .build(Method::Put, b"/upload")
                .header(b"Content-Type", b"text/plain")
                .body(b"file contents"),
        )
        .unwrap();

    assert_eq!(resp.text().unwrap(), "file contents");
    server.join().unwrap();
}

#[test]
fn builder_no_hardcoded_user_agent() {
    let (port, server) = echo_request_server();
    let mut client = connect(port);

    client.send(client.build(Method::Get, b"/check")).unwrap();

    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        !req_str.contains("User-Agent"),
        "User-Agent should not be hardcoded:\n{req_str}"
    );
    assert!(
        req_str.contains("Host:"),
        "Host header must always be present:\n{req_str}"
    );
}

// ── Streaming chunked regression / adversarial tests ─────────────────────────

#[test]
fn streaming_chunked_need_more_is_not_eof() {
    // Regression test: when the chunked decoder consumes a whole chunk but
    // the next chunk has not arrived yet, `StreamingBody::read` used to
    // return `Ok(0)`. The async reader (and any `Read` consumer) interprets
    // a zero-byte read as EOF, cutting the stream short.
    //
    // The server here sends the first chunk, pauses long enough for the
    // client to read it and drain the chunk ring, then sends the second
    // chunk. A buggy reader stops after "first"; the fixed reader yields
    // both chunks.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Pause longer than one socket read timeout window so the client
        // definitely observes `NeedMore` before the second chunk lands.
        thread::sleep(Duration::from_millis(250));
        stream.write_all(b"6\r\nsecond\r\n0\r\n\r\n").unwrap();
        stream.flush().unwrap();
    });

    let client = connect_async(port);
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();

    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));

    let mut chunks = Vec::new();
    while let Some(chunk) = handle.next_block() {
        if matches!(chunk, Chunk::Eof) {
            break;
        }
        chunks.push(chunk);
    }

    assert_eq!(
        chunks,
        vec![
            Chunk::Body(b"first".to_vec()),
            Chunk::Body(b"second".to_vec()),
        ]
    );

    server.join().unwrap();
}

#[test]
fn streaming_chunked_many_short_chunks_with_gaps() {
    // Adversarial: many tiny chunks delivered with small gaps. Each gap is
    // an opportunity for the decoder to return `NeedMore` -> `Ok(0)` -> EOF.
    // The stream must survive all of them and the connection stays clean.
    let chunks: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        for data in &chunks {
            let hex = format!("{:x}\r\n", data.len());
            stream.write_all(hex.as_bytes()).unwrap();
            stream.write_all(data).unwrap();
            stream.write_all(b"\r\n").unwrap();
            stream.flush().unwrap();
            thread::sleep(Duration::from_millis(30));
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();

        // Prove the connection is still reusable.
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });

    let client = connect_async(port);
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();

    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));

    let mut body = String::new();
    while let Some(chunk) = handle.next_block() {
        match chunk {
            Chunk::Body(b) => body.push_str(&String::from_utf8_lossy(&b)),
            Chunk::Eof => break,
            other => panic!("unexpected chunk: {other:?}"),
        }
    }
    assert_eq!(body, "abcde");

    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle2.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle2.next_block(), Some(Chunk::Body(b"ok".to_vec())));
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn streaming_chunked_single_byte_chunks() {
    // Adversarial: one byte per chunk. The decoder crosses `ReadingDataCr`,
    // `ReadingDataLf`, and `ReadingSize` repeatedly; it must not confuse
    // the chunk boundary parsing with EOF.
    let body = b"hello";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        for &b in body {
            stream.write_all(b"1\r\n").unwrap();
            stream.write_all(&[b]).unwrap();
            stream.write_all(b"\r\n").unwrap();
            stream.flush().unwrap();
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();
    });

    let client = connect_async(port);
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));

    let mut got = Vec::new();
    while let Some(chunk) = handle.next_block() {
        match chunk {
            Chunk::Body(b) => got.extend_from_slice(&b),
            Chunk::Eof => break,
            other => panic!("unexpected chunk: {other:?}"),
        }
    }
    assert_eq!(got, body);

    server.join().unwrap();
}

// ── Size limit tests ─────────────────────────────────────────────────────────

#[test]
fn body_too_large_rejected() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.write_all(response).unwrap();
        // Send 100 bytes of body
        stream.write_all(&[b'X'; 100]).unwrap();
    });

    let config = Config {
        max_response_body: 50,
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let result = client.get(b"/big");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::BodyTooLarge)
    );
    drop(server);
}

#[test]
fn default_limit_accepts_large_api_response_head() {
    // Gateways can add substantial tracing and rate-limit metadata. The
    // default must accept a normal 32 KiB response head, and the complete
    // header must remain available after parsing rather than being truncated
    // to the socket read buffer.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nX-Gateway-Metadata: ")
            .unwrap();
        stream.write_all(&vec![b'M'; 32 * 1024]).unwrap();
        stream
            .write_all(b"\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });

    let mut client = connect(port);
    let response = client.get(b"/large-head").unwrap();
    let metadata = response
        .headers()
        .find(|(name, _)| *name == b"X-Gateway-Metadata")
        .map(|(_, value)| value)
        .expect("large gateway header must be preserved");
    assert_eq!(metadata.len(), 32 * 1024);
    assert!(metadata.iter().all(|&byte| byte == b'M'));
    assert_eq!(response.text().unwrap(), "ok");

    server.join().unwrap();
}

#[test]
fn head_at_limit_with_body_tail_is_accepted() {
    // The read that finds `\r\n\r\n` often includes body bytes too. Count
    // only the head toward max_head; rejecting the combined read would make a
    // correctly sized gateway response fail depending on packet boundaries.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let prefix = b"HTTP/1.1 200 OK\r\nX-Fill: ";
        let suffix = b"\r\nContent-Length: 2\r\n\r\n";
        let fill_len = 256 - prefix.len() - suffix.len();
        stream.write_all(prefix).unwrap();
        stream.write_all(&vec![b'F'; fill_len]).unwrap();
        stream.write_all(suffix).unwrap();
        stream.write_all(b"ok").unwrap();
    });

    let mut client = connect_with_small_head_limit(port, Config::default());
    assert_eq!(client.get(b"/at-limit").unwrap().text().unwrap(), "ok");

    server.join().unwrap();
}

#[test]
fn oversized_head_forces_reconnect_before_next_request() {
    // Reading enough bytes to reject a head leaves an indeterminate suffix on
    // the socket. The following request must use a new connection, not parse
    // that suffix as a response head.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        read_request(&mut first);
        first.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        first.write_all(&[b'A'; 2_000]).unwrap();
        first
            .write_all(b"\r\nContent-Length: 5\r\n\r\nstale")
            .unwrap();
        first.flush().unwrap();

        // The client deliberately abandons `first`; a clean retry arrives on
        // a separate socket and gets an unrelated valid answer.
        let (mut second, _) = listener.accept().unwrap();
        read_request(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    let mut client = connect_with_small_head_limit(port, Config::default());
    assert_eq!(
        client.get(b"/too-large").unwrap_err(),
        Error::Connection(ConnectionError::HeadTooLarge)
    );
    assert_eq!(client.get(b"/retry").unwrap().text().unwrap(), "fresh");

    server.join().unwrap();
}

#[test]
fn async_oversized_head_forces_reconnect_before_next_request() {
    // The agent uses AsyncClient. Its reader must carry the dirty marker from
    // a rejected head into the next queued request just like Client does.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        read_request(&mut first);
        first.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        first.write_all(&vec![b'A'; 2_000]).unwrap();
        first
            .write_all(b"\r\nContent-Length: 5\r\n\r\nstale")
            .unwrap();
        first.flush().unwrap();

        let (mut second, _) = listener.accept().unwrap();
        read_request(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    // The async facade carries the same const generic into its reader-owned
    // Client, so deployments can pick a tighter bound without runtime state.
    let url = format!("http://127.0.0.1:{port}/");
    let client = AsyncClient::<SMALL_HEAD_SIZE>::connect::<PlainConnector>(
        url.as_bytes(),
        (),
        Config::default(),
    )
    .unwrap();
    let mut rejected = client
        .submit(Method::Get, b"/too-large".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        rejected.next_block(),
        Some(Chunk::Error(error))
            if *error == Error::Connection(ConnectionError::HeadTooLarge)
    ));
    drop(rejected);

    let mut retry = client
        .submit(Method::Get, b"/retry".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        retry.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(retry.next_block(), Some(Chunk::Body(b"fresh".to_vec())));
    assert_eq!(retry.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn head_too_large_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        // Send a response with a huge header that exceeds the limit
        stream.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        stream.write_all(&[b'A'; 2000]).unwrap();
        stream.write_all(b"\r\nContent-Length: 0\r\n\r\n").unwrap();
    });

    let mut client = connect_with_small_head_limit(port, Config::default());
    let result = client.get(b"/huge-head");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::HeadTooLarge)
    );
    drop(server);
}
