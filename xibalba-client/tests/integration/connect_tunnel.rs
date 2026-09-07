//! What this client does when asked to establish a tunnel.
//!
//! CONNECT is the one method whose successful response is not a message with
//! content but a change of what the connection *is*. RFC 9110 §9.3.6: "Any 2xx
//! (Successful) response indicates that the sender (and all inbound proxies)
//! will switch to tunnel mode immediately after the response header section;
//! data received after that header section is from the server identified by
//! the request target."
//!
//! This client speaks HTTP over a connection it owns; it has no API for handing
//! that connection back to the caller as a tunnel. So the honest answer to
//! CONNECT is to refuse it, and the dangerous answer is to treat the tunnel's
//! first bytes as a response body — which is what reading `Content-Length` on a
//! CONNECT 2xx amounts to, and why §9.3.6 tells a client to ignore that field.

use crate::support::client::TestClient;
use crate::support::registry::ScriptedServer;
use crate::support::script::Script;
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::method::Method;

/// A proxy that accepts the tunnel and then speaks the tunnelled protocol.
///
/// The bytes after the head are deliberately *not* HTTP: they are what the
/// destination server would send once the tunnel is up. A client that frames
/// them as a body will hand them to the caller as content, and one that frames
/// them by `Content-Length` will sit waiting for bytes that only the far end
/// can send.
struct Tunnel;

impl Tunnel {
    /// A 2xx with a `Content-Length` the client is required to ignore. The
    /// value is larger than the tunnel data that follows, so a client that
    /// honours it blocks rather than returning wrong bytes — the failure is
    /// loud either way.
    fn accepted_with_a_length() -> Vec<u8> {
        let mut wire =
            b"HTTP/1.1 200 Connection Established\r\nContent-Length: 4096\r\n\r\n".to_vec();
        wire.extend_from_slice(b"\x16\x03\x01\x00\x01tunnelled bytes, not a body");
        wire
    }

    /// The same acceptance without the misleading field, which is what a
    /// conforming proxy sends.
    fn accepted() -> Vec<u8> {
        let mut wire = b"HTTP/1.1 200 Connection Established\r\n\r\n".to_vec();
        wire.extend_from_slice(b"\x16\x03\x01\x00\x01tunnelled bytes, not a body");
        wire
    }

    fn serving(response: Vec<u8>) -> ScriptedServer {
        ScriptedServer::serving(vec![Script::new().expect_request().send(response).close()])
    }
}

/// The defect this test was written for: a CONNECT 2xx must never be read as a
/// message with content, whatever its headers claim.
///
/// The assertion names the refusal rather than accepting any error. An earlier
/// draft asserted only `is_err()` and passed against the unfixed client — which
/// had happily framed a 4096-byte body and then failed on EOF. That is the
/// defect reporting itself as a transport failure, not the fix working.
#[test]
fn a_connect_response_is_not_read_as_a_message_with_content() {
    let server = Tunnel::serving(Tunnel::accepted_with_a_length());
    let mut client = TestClient::scripted(&server);

    let error = client
        .request(Method::Connect, b"example.com:443", None, None)
        .expect_err("CONNECT returned a response; the tunnel's first bytes were framed as a body");

    assert!(
        matches!(
            error,
            Error::Connection(ConnectionError::TunnelingNotSupported)
        ),
        "CONNECT failed for the wrong reason ({error:?}); the client must refuse \
         the method rather than stumble over the tunnel it cannot read"
    );
}

/// Refusing before the write is what makes the refusal safe: a CONNECT that
/// reached the proxy would leave it in tunnel mode with no one to speak to.
#[test]
fn a_refused_connect_never_reaches_the_wire() {
    let server = Tunnel::serving(Tunnel::accepted());
    let mut client = TestClient::scripted(&server);

    let _ = client.request(Method::Connect, b"example.com:443", None, None);

    assert!(
        !server.connection(0).written().contains("CONNECT"),
        "the CONNECT was written to the proxy before being refused, leaving \
         the peer in tunnel mode with a client that cannot speak the tunnel"
    );
}

/// The connection is untouched by the refusal, so an ordinary request still
/// works on it. This is what distinguishes rejecting the *method* from
/// poisoning the connection.
#[test]
fn refusing_connect_leaves_the_connection_usable() {
    let server = ScriptedServer::serving(vec![
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_vec())
            .close(),
    ]);
    let mut client = TestClient::scripted(&server);

    assert!(matches!(
        client.request(Method::Connect, b"example.com:443", None, None),
        Err(Error::Connection(ConnectionError::TunnelingNotSupported))
    ));

    let after = client.get(b"/still-works").unwrap();
    assert_eq!(
        after.text().unwrap(),
        "ok",
        "a rejected CONNECT spent the connection instead of leaving it clean"
    );
}
