//! [`AsyncClient`] cancellation, queueing, and reader-recovery behaviour.

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::server::RequestReader;
use xibalba_client::PlainConnector;
use xibalba_client::async_client::AsyncClient;
use xibalba_client::async_client::Chunk;
use xibalba_client::client::Config;
use xibalba_client::proto::method::Method;

#[test]
fn async_cancel_interrupts_stalled_error_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 100\r\n\r\npartial")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_secs(2));
    });

    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        stream_silence: Duration::from_mins(5),
        ..Config::default()
    };
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config).unwrap();
    let mut handle = client
        .submit(Method::Get, b"/rate-limit".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 429, .. })
    ));

    handle.cancel().unwrap();
    let started = std::time::Instant::now();
    assert_eq!(handle.next_block(), Some(Chunk::Aborted));
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(client);
    server.join().unwrap();
}

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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Second connection: serve the recovery request.
        let (mut stream2, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream2);
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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
            .unwrap();
        stream.flush().unwrap();

        // Request B on the same kept-alive connection: send the head and
        // a first chunk, then hold the stream open. B is *mid-body* —
        // exactly when the reader polls the control ring between reads,
        // and so exactly when a stale cancel would be misapplied to it.
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nsecond\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(400));
        stream.write_all(b"5\r\nthird\r\n0\r\n\r\n").unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });

    let client = TestClient::connect_async(port);

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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(500));
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();

        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });

    let client = TestClient::connect_async(port);

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
        RequestReader::read_head(&mut stream);
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
        RequestReader::read_head(&mut stream);
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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nstale")
            .unwrap();
        stream.flush().unwrap();

        // Second connection: the follow-up request must land here.
        let (mut stream2, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        stream2.flush().unwrap();
        drop(stream);
    });

    let client = TestClient::connect_async(port);

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
        RequestReader::read_head(&mut stream);
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
        RequestReader::read_head(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        stream2.flush().unwrap();
    });

    let client = TestClient::connect_async(port);

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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
            .unwrap();
        stream.flush().unwrap();

        // Same connection must be reused.
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond")
            .unwrap();
        stream.flush().unwrap();
    });

    let client = TestClient::connect_async(port);

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
        RequestReader::read_head(&mut stream);
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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();
        stream.flush().unwrap();
    });

    let client = TestClient::connect_async(port);

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

#[test]
fn async_cancelled_queued_request_never_reaches_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(400)))
            .unwrap();
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            Ok(0) => false,
            Ok(_) => true,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                false
            }
            Err(error) => panic!("request probe failed: {error}"),
        }
    });

    let client = TestClient::connect_async(port);
    let mut first = client
        .submit(Method::Get, b"/first".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(first.next_block(), Some(Chunk::Head { .. })));
    assert_eq!(first.next_block(), Some(Chunk::Body(b"first".to_vec())));

    let mut queued = client
        .submit(Method::Get, b"/must-not-send".to_vec(), None, None, vec![])
        .unwrap();
    queued.cancel().unwrap();

    assert_eq!(first.next_block(), Some(Chunk::Eof));
    assert_eq!(queued.next_block(), Some(Chunk::Aborted));
    assert!(!queued.has_started());
    drop(client);
    assert!(
        !server.join().unwrap(),
        "cancelled queued request reached the wire"
    );
}
