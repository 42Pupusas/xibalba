//! Whether a connection survives a response whose framing was ambiguous.
//!
//! RFC 9112 §6.3 lets a recipient decide framing when `Transfer-Encoding` and
//! `Content-Length` disagree — the transfer coding wins — but the connection
//! is a separate question. The sender's two framings mean two readings of
//! where this response ends, and an intermediary that took the other one has
//! left bytes on the wire that this client would read as the *next* response.
//! Deciding the body correctly is not enough; the connection has to go.

use crate::support::client::TestClient;
use crate::support::registry::ScriptedServer;
use crate::support::script::Script;
use xibalba_client::proto::method::Method;

/// A keep-alive exchange whose first response declares `headers`, followed by
/// a second response on whatever connection the client chooses to use.
///
/// The second response is scripted on *both* connections, so the test never
/// fails merely because the client reconnected: whichever it picks, an answer
/// is waiting. What differs is which connection recorded the second request,
/// and that is what the assertions read.
struct AmbiguousFraming;

impl AmbiguousFraming {
    /// The first hop's response head, with a body framed both ways.
    fn first(head: &str, body: &str) -> Vec<u8> {
        format!("{head}\r\n\r\n{body}").into_bytes()
    }

    fn second() -> Vec<u8> {
        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond".to_vec()
    }

    /// Both connections answer, so reuse and reconnect are equally supported
    /// and the test decides between them by evidence rather than by which one
    /// happens not to hang.
    fn server(head: &str, body: &str) -> ScriptedServer {
        ScriptedServer::serving(vec![
            Script::new()
                .expect_request()
                .send_then_await(Self::first(head, body))
                .expect_request()
                .send(Self::second())
                .close(),
            Script::new().expect_request().send(Self::second()).close(),
        ])
    }
}

/// The headline case: `Transfer-Encoding: chunked` alongside `Content-Length`.
/// The chunked framing is authoritative and the body decodes cleanly, so
/// nothing about the *response* forces a reconnect — which is exactly why the
/// connection is the thing worth asserting on.
#[test]
fn a_response_framed_both_ways_does_not_carry_the_next_request() {
    let server = AmbiguousFraming::server(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 3",
        "3\r\nabc\r\n0\r\n\r\n",
    );
    let mut client = TestClient::scripted(&server);

    let first = client.get(b"/one").unwrap();
    assert_eq!(
        first.text().unwrap(),
        "abc",
        "the transfer coding frames the body"
    );

    let second = client.get(b"/two").unwrap();
    assert_eq!(second.text().unwrap(), "second");

    assert!(
        server.connection(1).written().contains("GET /two"),
        "the second request reused a connection whose framing was ambiguous; \
         an intermediary reading the Content-Length instead left bytes on it"
    );
}

/// HTTP/1.0 plus a transfer coding: RFC 9112 §6.1 calls the framing faulty
/// outright and requires the connection to close, because the sender may have
/// retained buffered bytes that further use would misread.
#[test]
fn an_http10_response_with_a_transfer_coding_ends_the_connection() {
    let server = AmbiguousFraming::server(
        "HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive",
        "3\r\nabc\r\n0\r\n\r\n",
    );
    let mut client = TestClient::scripted(&server);

    let first = client.get(b"/one").unwrap();
    assert_eq!(first.text().unwrap(), "abc");

    let second = client.get(b"/two").unwrap();
    assert_eq!(second.text().unwrap(), "second");

    assert!(
        server.connection(1).written().contains("GET /two"),
        "an HTTP/1.0 response carrying Transfer-Encoding kept its connection, \
         which RFC 9112 6.1 forbids even when keep-alive is requested"
    );
}

/// The control. A plainly framed keep-alive response must still be reused, or
/// the tests above would pass under a client that simply never reuses
/// anything.
#[test]
fn an_unambiguously_framed_response_still_reuses_its_connection() {
    let server = AmbiguousFraming::server("HTTP/1.1 200 OK\r\nContent-Length: 3", "abc");
    let mut client = TestClient::scripted(&server);

    let first = client.get(b"/one").unwrap();
    assert_eq!(first.text().unwrap(), "abc");

    let second = client.get(b"/two").unwrap();
    assert_eq!(second.text().unwrap(), "second");

    assert!(
        server.connection(0).written().contains("GET /two"),
        "a cleanly framed response must keep its connection, or the reuse \
         assertions elsewhere prove nothing"
    );
}

/// A caller that sends `Connection: close` has announced it will not use the
/// socket again. The first response is unambiguously self-delimited and
/// nothing about it forces a close, so a client deriving reuse from the
/// response alone would happily write the second request to the same
/// connection — this is the request-side half R04 requires: the client's own
/// announcement must end the connection regardless of what the response says.
#[test]
fn a_request_side_connection_close_is_not_reused_even_for_a_self_delimited_response() {
    let server = ScriptedServer::serving(vec![
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi".to_vec())
            .close(),
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond".to_vec())
            .close(),
    ]);
    let mut client = TestClient::scripted(&server);

    let closing = client
        .build(Method::Get, b"/one")
        .header(b"Connection", b"close");
    let first = client.send(closing).expect("the first request succeeds");
    assert_eq!(first.text().unwrap(), "hi");

    let second = client
        .get(b"/two")
        .expect("the next request must open a new connection");
    assert_eq!(second.text().unwrap(), "second");

    assert!(
        server.connection(1).written().contains("GET /two"),
        "the second request must reach a fresh connection, not the one the \
         client itself announced it would close: connection 0 saw {:?}",
        server.connection(0).written()
    );
    assert!(
        !server.connection(0).written().contains("GET /two"),
        "the connection the client closed must never see a second request"
    );
}
