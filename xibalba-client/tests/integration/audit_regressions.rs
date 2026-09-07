//! Regressions for defects the audit found: credential leaks across origins,
//! interim-response handling, and connections left dirty after a failure.

use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::park::StopSignal;
use crate::support::server::RequestReader;
use crate::support::server::TestServer;
use xibalba_client::PlainConnector;
use xibalba_client::async_client::AsyncClient;
use xibalba_client::async_client::Chunk;
use xibalba_client::client::Config;
use xibalba_client::proto::error::ConnectionError;
use xibalba_client::proto::error::Error;
use xibalba_client::proto::method::Method;

#[test]
fn head_request_gets_empty_body_without_waiting_for_one() {
    // The server answers a HEAD with Content-Length: 5 and NO body (per
    // RFC 9110 the content-length describes the GET body). The framing
    // must be None; before the fix the client waited for five body bytes
    // that never come and hit the silence budget.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let req = RequestReader::read_head(&mut stream);
        assert!(req.starts_with(b"HEAD "));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
            .unwrap();
        // Deliberately hold the connection open instead of closing: a
        // client that tries to read a body blocks until the silence
        // budget expires.
        thread::sleep(Duration::from_millis(300));
    });

    let mut client = TestClient::connect(port);
    let resp = client
        .request(Method::Head, b"/resource", None, None)
        .unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}

#[test]
fn body_read_failure_marks_connection_dirty_and_next_request_reconnects() {
    // A body error (BodyTooLarge) leaves unread bytes on the socket. The
    // connection must be marked dirty so the next request reconnects;
    // reusing it would parse the stale body as the next response head.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        // Connection 1: head says 100 bytes, client's limit is 50.
        let (mut first, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut first);
        first
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
            .unwrap();
        first.write_all(&[b'X'; 100]).unwrap();

        // Connection 2: the follow-up request must land HERE, not parse
        // leftover X bytes as a response head.
        let (mut second, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    let config = Config {
        max_response_body: 50,
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);
    assert_eq!(
        client.get(b"/big").unwrap_err(),
        Error::Connection(ConnectionError::BodyTooLarge)
    );
    assert_eq!(client.get(b"/retry").unwrap().text().unwrap(), "fresh");

    server.join().unwrap();
}

#[test]
fn cross_origin_absolute_redirect_strips_credentials() {
    // A 302 to another origin must not carry Authorization or Cookie:
    // the redirect target would otherwise harvest the caller's bearer
    // token. The new Host must name the redirect origin.
    let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
    let port_a = listener_a.local_addr().unwrap().port();
    let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
    let port_b = listener_b.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        let (mut a, _) = listener_a.accept().unwrap();
        RequestReader::read_head(&mut a);
        let redirect = format!(
            "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: http://127.0.0.1:{port_b}/final\r\n\r\n"
        );
        a.write_all(redirect.as_bytes()).unwrap();

        let (mut b, _) = listener_b.accept().unwrap();
        let req2 = RequestReader::read_head(&mut b);
        b.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
        req2
    });

    let mut client = TestClient::connect(port_a);
    let resp = client
        .send(
            client
                .build(Method::Get, b"/start")
                .header(b"Authorization", b"Bearer sekrit")
                .header(b"Cookie", b"session=abc"),
        )
        .unwrap();
    assert_eq!(resp.text().unwrap(), "ok");

    let req2 = server.join().unwrap();
    let req2_str = String::from_utf8_lossy(&req2);
    assert!(
        !req2_str.contains("Authorization"),
        "credentials leaked on cross-origin redirect:\n{req2_str}"
    );
    assert!(
        !req2_str.contains("session=abc"),
        "cookies leaked on cross-origin redirect:\n{req2_str}"
    );
    let expected_host = format!("Host: 127.0.0.1:{port_b}\r\n");
    assert!(
        req2_str.contains(&expected_host),
        "Host must name the redirect origin:\n{req2_str}"
    );
    assert_eq!(
        req2_str.matches("Host:").count(),
        1,
        "redirect request must contain exactly one Host header:\n{req2_str}"
    );
}

#[test]
fn same_origin_absolute_redirect_keeps_credentials() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // First request: absolute redirect to the SAME origin.
        RequestReader::read_head(&mut stream);
        let redirect = format!(
            "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: http://127.0.0.1:{port}/final\r\n\r\n"
        );
        stream.write_all(redirect.as_bytes()).unwrap();
        stream.flush().unwrap();
        // Second request: echo it back for inspection.
        let req2 = RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
        req2
    });

    let mut client = TestClient::connect(port);
    let resp = client
        .send(
            client
                .build(Method::Get, b"/start")
                .header(b"Authorization", b"Bearer kept"),
        )
        .unwrap();
    assert_eq!(resp.text().unwrap(), "ok");

    let req2 = server.join().unwrap();
    let req2_str = String::from_utf8_lossy(&req2);
    assert!(
        req2_str.contains("Authorization: Bearer kept"),
        "same-origin redirect must keep credentials:\n{req2_str}"
    );
    assert!(
        req2_str.contains(&format!("Host: 127.0.0.1:{port}")),
        "same-origin redirect must keep the Host header:\n{req2_str}"
    );
}

#[test]
fn interim_100_response_is_skipped() {
    // A 100 Continue interim head precedes the real response on the same
    // connection. Treating it as the final response desyncs every later
    // request; it must be skipped and the real head parsed.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nreal")
            .unwrap();
        stream.flush().unwrap();
    });

    let mut client = TestClient::connect(port);
    let resp = client.get(b"/expected-100").unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "real");
    server.join().unwrap();
}

#[test]
fn interim_and_final_response_in_one_read_are_both_processed() {
    let (port, server) = TestServer::one_shot(
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nreal",
    );
    let config = Config {
        read_timeout: Some(Duration::from_millis(100)),
        head_silence: Duration::from_millis(300),
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);

    let started = std::time::Instant::now();
    let resp = client.get(b"/coalesced-100").unwrap();
    assert!(started.elapsed() < Duration::from_millis(300));
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "real");
    server.join().unwrap();
}

#[test]
fn excessive_interim_responses_are_rejected() {
    let mut response = Vec::new();
    for _ in 0..9 {
        response.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
    }
    response.extend_from_slice(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    let response: &'static [u8] = Box::leak(response.into_boxed_slice());
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    assert_eq!(
        client.get(b"/too-many-interims").unwrap_err(),
        Error::Connection(ConnectionError::TooManyInterimResponses)
    );
    server.join().unwrap();
}

#[test]
fn switching_protocols_is_surfaced_as_final_response() {
    // 101 hands the connection to another protocol; it must be surfaced
    // (not skipped like other 1xx) so the caller can take over.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n")
            .unwrap();
    });

    let mut client = TestClient::connect(port);
    let resp = client.get(b"/upgrade").unwrap();
    assert_eq!(
        resp.status,
        xibalba_client::proto::status::StatusCode::SWITCHING_PROTOCOLS
    );
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}

#[test]
fn async_client_drop_interrupts_in_flight_stream_quickly() {
    // Dropping the AsyncClient while the reader is parked on a stalled
    // in-flight response must not wait out the full stream_silence
    // budget: the shutdown flag turns the next WouldBlock retry into an
    // unwind, so drop returns within a couple of read-timeout windows.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, park) = StopSignal::new();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Stall without closing; the reader parks on the socket. The stream is
        // intentionally never terminated, and the park holds it open for
        // exactly as long as the client-side assertions need.
        park.wait();
    });

    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(100)),
        stream_silence: Duration::from_mins(5),
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

    // Drop with a body stream stalled: the reader is inside the retry
    // loop. Without the shutdown flag, the join inside drop blocks for
    // stream_silence (300 s here).
    let started = std::time::Instant::now();
    drop(client);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "drop took {elapsed:?}; the shutdown flag did not interrupt the stalled read"
    );

    drop(stop);
    server.join().unwrap();
}
