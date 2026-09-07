//! [`AsyncClient`] cancellation, queueing, and reader-recovery behaviour.
//!
//! Most of these run against a scripted connector rather than a socket: their
//! subject is what the reader thread does with a partly-delivered response, so
//! a real server contributes only the question of when its bytes arrive. Where
//! a test turns on connect-time behaviour — a reconnect that must fail — it
//! keeps a real listener, because refusing a connection is what it asserts.

use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::gate::Gate;
use crate::support::registry::ScriptedServer;
use crate::support::script::Script;
use xibalba_client::async_client::Chunk;
use xibalba_client::client::Config;
use xibalba_client::proto::method::Method;

#[test]
fn async_cancel_interrupts_stalled_error_body() {
    // The body is short of its declared length and never completes. A cancel
    // must be observed on the next read tick rather than waiting out the
    // silence budget, so the server hangs: only the cancel can end this.
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 100\r\n\r\npartial".to_vec(),
            )
            .hang(),
    );

    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        stream_silence: Duration::from_mins(5),
        ..Config::default()
    };
    let client = TestClient::scripted_async_with_config(&server, config);
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
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "cancel waited on the silence budget instead of the next read tick"
    );
    drop(client);
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
    // The first connection goes silent mid-body without closing: a `Hang`,
    // not a `Close`, because EOF would end the read for the wrong reason and
    // the silence budget — the thing under test — would never be consulted.
    let server = ScriptedServer::serving(vec![
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .hang(),
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh".to_vec()),
    ]);

    // Short read timeout + short silence budget so the stall trips quickly.
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        stream_silence: Duration::from_millis(300),
        ..Config::default()
    };
    let client = TestClient::scripted_async_with_config(&server, config);

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

    server.connection(1).assert_script_completed();
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
    //
    // The gate is the cancel itself. B's remaining chunks are withheld until
    // the test has cancelled A, so the stale cancel is guaranteed to arrive
    // while B is mid-body — the window where it used to be misapplied. A
    // sleep only made that likely.
    let cancelled_a = Gate::shut();
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst".to_vec())
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nsecond\r\n".to_vec(),
            )
            .await_gate(&cancelled_a)
            .send(b"5\r\nthird\r\n0\r\n\r\n".to_vec()),
    );

    let client = TestClient::scripted_async(&server);

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
    cancelled_a.open();

    // B must run to completion regardless.
    match handle_b.next_block() {
        Some(Chunk::Body(b)) => assert_eq!(b, b"third".to_vec()),
        Some(Chunk::Aborted) => {
            panic!("stale cancel for a finished request aborted the in-flight request")
        }
        other => panic!("expected request B to keep streaming, got {other:?}"),
    }
    assert_eq!(handle_b.next_block(), Some(Chunk::Eof));

    // The stale cancel must be able to land while B is mid-body; without the
    // gate B could finish first and the cancel would test nothing.
    server.only().assert_gated_on(&cancelled_a);
    server.only().assert_script_completed();
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
    //
    // A holds the reader until the gate opens, which the test does only after
    // submitting B and observing it has *not* started. The queued state is
    // therefore established before A can finish, instead of being inferred
    // from a pause long enough to make that likely.
    let submitted_b = Gate::shut();
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .await_gate(&submitted_b)
            .send(b"0\r\n\r\n".to_vec())
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond".to_vec()),
    );

    let client = TestClient::scripted_async(&server);

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
    submitted_b.open();

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

    // Without this the gate proves nothing: drop the step and A finishes on
    // its own schedule, so B is no longer certain to be observed while queued.
    server.only().assert_gated_on(&submitted_b);
    server.only().assert_script_completed();
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
    //
    // The old cap was three retries, so the silence is stated as a number of
    // ticks well past it rather than as a duration that has to be long enough
    // to imply them. The test no longer waits out a wall-clock delay to prove
    // a count.
    const PAST_THE_OLD_RETRY_CAP: usize = 12;
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .stall_reads(PAST_THE_OLD_RETRY_CAP)
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nslow".to_vec()),
    );

    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        head_silence: Duration::from_secs(5),
        ..Config::default()
    };
    let client = TestClient::scripted_async_with_config(&server, config);

    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(
        matches!(handle.next_block(), Some(Chunk::Head { status: 200, .. })),
        "slow head must not surface a WouldBlock error"
    );
    assert_eq!(handle.next_block(), Some(Chunk::Body(b"slow".to_vec())));
    assert_eq!(handle.next_block(), Some(Chunk::Eof));

    server.only().assert_script_completed();
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
    //
    // Exactly one script is registered, so the reconnect has nothing to
    // connect to and is refused. The real-socket version had to drop its
    // listener at the right moment to stop a reconnect landing in the accept
    // backlog and "succeeding"; here a second connection is unscripted and
    // therefore impossible, which is the condition the test wants rather than
    // an arrangement that produces it.
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .hang(),
    );

    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        ..Config::default()
    };
    let client = TestClient::scripted_async_with_config(&server, config);

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

    // No sleep to let the reader notice the cancel: control messages are
    // ordered, so the cancel is already ahead of the request submitted below
    // and will be seen first.
    //
    // The next request must fail loudly with a connection error — NOT
    // silently parse the stale "first" chunk remnants as its own response.
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
    //
    // Two scripts, so the follow-up request can only be answered on a second
    // connection. If the client wrongly reused the first, it would read the
    // leftover "stale" body instead — and the second script would go
    // unclaimed, which the completion assertion catches.
    let server = ScriptedServer::serving(vec![
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nstale".to_vec())
            .hang(),
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh".to_vec()),
    ]);

    let client = TestClient::scripted_async(&server);

    // Submit and immediately drop the handle — before the reader can
    // push Chunk::Head. The push then fails and the reader bails with
    // the body unread.
    let handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    drop(handle);

    // The follow-up must see the fresh response, not the stale body. No sleep
    // is needed for the reader to notice the dropped handle: this request
    // queues behind the abandoned one, so it cannot be served before the
    // reader has finished dealing with it.
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

    server.connection(1).assert_script_completed();
}

#[test]
fn async_cancel_mid_stream_then_next_request_is_clean() {
    // Regression test for the mid-stream cancel corruption bug.
    // Cancelling (dropping) a StreamHandle before the response body is
    // fully consumed must mark the underlying connection dirty so the
    // next request reconnects. Otherwise the next response is parsed
    // from leftover body bytes of the previous response and is corrupted.
    //
    // The leftover chunks are released only once the test has cancelled, so
    // they are guaranteed to be sitting unread on the first connection when
    // the follow-up request is made — which is the trap being tested. A sleep
    // merely made it likely they had arrived by then.
    let cancelled = Gate::shut();
    let server = ScriptedServer::serving(vec![
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .await_gate(&cancelled)
            .send(b"5\r\nstale\r\n0\r\n\r\n".to_vec())
            .close(),
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh".to_vec()),
    ]);

    let client = TestClient::scripted_async(&server);

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
    cancelled.open();

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

    server.connection(0).assert_gated_on(&cancelled);
    server.connection(1).assert_script_completed();
}

#[test]
fn async_drained_stream_is_reused() {
    // Fully draining a streaming response must leave the connection clean
    // and reusable, exactly like the synchronous path. Dropping the handle
    // does not cancel.
    //
    // One script serves both requests, so reuse is structural: a reconnect
    // has no second script to take and would be refused outright rather than
    // quietly succeeding against a listener that accepts anything.
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst".to_vec())
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond".to_vec()),
    );

    let client = TestClient::scripted_async(&server);

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

    server.only().assert_script_completed();
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
    //
    // Request 1's remaining chunks wait on the gate, which the test opens
    // only after submitting request 2. Request 2 is therefore certain to
    // land on the control ring while the reader is still streaming — the
    // window where it used to be swallowed by the cancel poll.
    let submitted_second = Gate::shut();
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .await_gate(&submitted_second)
            .send(b"6\r\nsecond\r\n0\r\n\r\n".to_vec())
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello".to_vec()),
    );

    let client = TestClient::scripted_async(&server);

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
    submitted_second.open();

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

    server.only().assert_gated_on(&submitted_second);
    server.only().assert_script_completed();
}

#[test]
fn async_cancelled_queued_request_never_reaches_wire() {
    // A request cancelled while still queued must never be written. The
    // real-socket version inferred that from a 400ms read that timed out,
    // which cannot distinguish "never sent" from "not sent yet". The scripted
    // connection records every byte the client wrote, so the claim is checked
    // directly against the request path.
    //
    // The gate holds request 1 open until the cancel is in, so the queued
    // request is genuinely waiting behind a live stream when it is cancelled.
    let cancelled_queued = Gate::shut();
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .await_gate(&cancelled_queued)
            .send(b"0\r\n\r\n".to_vec()),
    );

    let client = TestClient::scripted_async(&server);
    let mut first = client
        .submit(Method::Get, b"/first".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(first.next_block(), Some(Chunk::Head { .. })));
    assert_eq!(first.next_block(), Some(Chunk::Body(b"first".to_vec())));

    let mut queued = client
        .submit(Method::Get, b"/must-not-send".to_vec(), None, None, vec![])
        .unwrap();
    queued.cancel().unwrap();
    cancelled_queued.open();

    assert_eq!(first.next_block(), Some(Chunk::Eof));
    assert_eq!(queued.next_block(), Some(Chunk::Aborted));
    assert!(!queued.has_started());
    drop(client);

    server.only().assert_gated_on(&cancelled_queued);
    assert!(
        !server.only().written().contains("/must-not-send"),
        "cancelled queued request reached the wire:\n{}",
        server.only().written()
    );
}
