//! Servers that behave badly: responses arriving a byte at a time, heads
//! split across reads, stale keep-alives, and excess bytes after a body.

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::gate::Gate;
use crate::support::registry::ScriptedServer;
use crate::support::script::Script;
use crate::support::server::RequestReader;
use crate::support::server::TestServer;
use xibalba_client::PlainConnector;
use xibalba_client::client::Client;
use xibalba_client::client::Config;
use xibalba_client::proto::error::ConnectionError;
use xibalba_client::proto::error::Error;
use xibalba_client::proto::method::Method;

#[test]
fn server_sends_response_byte_at_a_time() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
    let (port, server) = TestServer::drip(response);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "abc");
    server.join().unwrap();
}

#[test]
fn server_sends_headers_split_across_reads() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let (port, server) = TestServer::split(response, 20);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn server_sends_empty_chunked_body() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}

#[test]
fn streaming_chunked_delivers_incrementally() {
    // A streaming reader must surface the first chunk without waiting for the
    // terminator. The old shape sent a chunk, slept 400ms, then sent the rest,
    // and asserted the first chunk arrived inside 300ms of it — measuring the
    // machine as much as the client, and only ever showing the chunk arrived
    // *early*, not that it could arrive at all before the rest existed.
    //
    // The gate states the stronger claim: the remaining chunks do not exist
    // until the test has already received the first one. Reading "first"
    // cannot have depended on bytes that were never sent.
    let first_delivered = Gate::shut();
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n".to_vec(),
            )
            .await_gate(&first_delivered)
            .send(b"6\r\nsecond\r\n0\r\n\r\n".to_vec())
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_vec()),
    );

    let mut client = TestClient::scripted(&server);
    let mut resp = client
        .send_streaming(client.build(Method::Get, b"/"))
        .expect("streaming request failed");

    let mut buf = [0u8; 64];
    let n = resp.body.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n],
        b"first",
        "the first chunk must surface while the rest is still unsent"
    );
    first_delivered.open();

    let mut remainder = Vec::new();
    resp.body.read_to_end(&mut remainder).unwrap();
    assert_eq!(remainder, b"second");
    assert!(resp.body.is_done());
    drop(resp);

    // Connection must be reusable without reconnecting: one script serves
    // both requests, so a reconnect would find nothing to connect to.
    let resp2 = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp2.text().unwrap(), "ok");

    server.only().assert_gated_on(&first_delivered);
    server.only().assert_script_completed();
}

#[test]
fn streaming_excess_bytes_after_content_length_force_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut first);
        first
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokgarbage")
            .unwrap();
        first.flush().unwrap();

        let (mut second, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    let mut client = TestClient::connect(port);
    {
        let mut response = client
            .send_streaming(client.build(Method::Get, b"/first"))
            .unwrap();
        let mut body = Vec::new();
        response.body.read_to_end(&mut body).unwrap();
        assert_eq!(body, b"ok");
    }
    assert_eq!(client.get(b"/second").unwrap().text().unwrap(), "fresh");
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
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nstale\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Second connection: serve the follow-up request.
        let (mut stream2, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream2);
        stream2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
        drop(stream);
    });

    let mut client = TestClient::connect(port);
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
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

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
        RequestReader::read_head(&mut s1);
        s1.write_all(r1).unwrap();
        s1.flush().unwrap();
        // Close the first connection: the client's next request will
        // hit EOF on this socket.
        drop(s1);

        let (mut s2, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut s2);
        s2.write_all(r2).unwrap();
        s2.flush().unwrap();
    });

    let mut client = TestClient::connect(port);
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
        RequestReader::read_head(&mut s2);
        s2.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();
        s2.flush().unwrap();
    });

    let mut client = TestClient::connect(port);
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
    // The head must arrive alone and the whole body land in one read. On a
    // socket that was two writes with a pause between, which only tends to
    // produce that split; the barrier makes it the actual framing. The
    // trailing hang stands in for holding the connection open — a buggy
    // client blocks there, and the read timeout is what ends it.
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec())
            .send(b"5\r\nhello\r\n0\r\n\r\n".to_vec())
            .hang(),
    );

    let config = Config {
        read_timeout: Some(Duration::from_secs(2)),
        ..Config::default()
    };
    let mut client = TestClient::scripted_with_config(&server, config);
    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");

    // Data and terminator must have shared a read; if they arrived separately
    // the decoder never reaches the Done-while-reporting-Data case.
    server.only().assert_data_reads(2);
}

#[test]
fn multiple_requests_same_connection() {
    let r1: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst";
    let r2: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond";
    let (port, server) = TestServer::keepalive(r1, r2);
    let mut client = TestClient::connect(port);

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
    let (port, server) = TestServer::one_shot(response_bytes);
    let mut client = TestClient::connect(port);

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
fn partial_response_is_not_retried() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut accepted = 0;
        while std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    accepted += 1;
                    RequestReader::read_head(&mut stream);
                    if accepted == 1 {
                        stream.write_all(b"HTTP/1.1 200 OK\r\nX-Partial:").unwrap();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept failed: {error}"),
            }
        }
        accepted
    });

    let mut client = TestClient::connect(port);
    assert!(client.post(b"/non-idempotent", b"charge").is_err());
    assert_eq!(
        server.join().unwrap(),
        1,
        "partial response triggered a replay"
    );
}

#[test]
fn excess_bytes_after_content_length_force_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut first);
        first
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokgarbage")
            .unwrap();
        first.flush().unwrap();

        let (mut second, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    let mut client = TestClient::connect(port);
    assert_eq!(client.get(b"/first").unwrap().text().unwrap(), "ok");
    assert_eq!(client.get(b"/second").unwrap().text().unwrap(), "fresh");
    server.join().unwrap();
}

#[test]
fn infinite_read_timeout_is_rejected() {
    let config = Config {
        read_timeout: None,
        ..Config::default()
    };
    let error = Client::<PlainConnector>::connect(b"http://127.0.0.1:1/", (), config)
        .err()
        .expect("an infinite read timeout must be rejected before connect");
    assert_eq!(
        error,
        Error::Connection(ConnectionError::InfiniteReadTimeout)
    );
}

/// A zero duration is always a configuration mistake: a zero read timeout
/// makes every read tick instantly and a zero budget is spent before the
/// first read returns, so every request fails immediately. Rejecting it at
/// connect turns a puzzling runtime failure into a startup error.
#[test]
fn zero_durations_are_rejected() {
    let zero = Duration::ZERO;
    let cases = [
        Config {
            read_timeout: Some(zero),
            ..Config::default()
        },
        Config {
            write_timeout: Some(zero),
            ..Config::default()
        },
        Config {
            head_silence: zero,
            ..Config::default()
        },
        Config {
            stream_silence: zero,
            ..Config::default()
        },
    ];
    for config in cases {
        let error = Client::<PlainConnector>::connect(b"http://127.0.0.1:1/", (), config)
            .err()
            .expect("a zero duration must be rejected before connect");
        assert_eq!(error, Error::Connection(ConnectionError::ZeroDuration));
    }
}

/// The silence budgets are only consulted between reads, so a per-read
/// timeout longer than a budget lets one blocked read overshoot it.
#[test]
fn a_read_timeout_longer_than_a_silence_budget_is_rejected() {
    let cases = [
        Config {
            read_timeout: Some(Duration::from_secs(30)),
            head_silence: Duration::from_secs(5),
            ..Config::default()
        },
        Config {
            read_timeout: Some(Duration::from_secs(30)),
            stream_silence: Duration::from_secs(5),
            ..Config::default()
        },
    ];
    for config in cases {
        let error = Client::<PlainConnector>::connect(b"http://127.0.0.1:1/", (), config)
            .err()
            .expect("a read timeout exceeding a silence budget must be rejected");
        assert_eq!(
            error,
            Error::Connection(ConnectionError::TimeoutExceedsBudget)
        );
    }
}

/// A read timeout equal to the budget subdivides it exactly once, which is
/// the boundary the check must not reject.
#[test]
fn a_read_timeout_equal_to_the_budget_is_accepted() {
    let config = Config {
        read_timeout: Some(Duration::from_secs(5)),
        head_silence: Duration::from_secs(5),
        stream_silence: Duration::from_secs(5),
        ..Config::default()
    };
    let error = Client::<PlainConnector>::connect(b"http://127.0.0.1:1/", (), config)
        .err()
        .expect("nothing is listening on port 1");
    assert!(
        matches!(error, Error::Io(_)),
        "expected the connect to fail on the socket, not on validation: {error:?}"
    );
}

#[test]
fn server_closes_connection_before_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        drop(stream);
    });
    let mut client = TestClient::connect(port);
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

    let (port, server) = TestServer::one_shot(response_bytes);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    let header_count = resp.headers().count();
    assert!(header_count >= 30);
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}
