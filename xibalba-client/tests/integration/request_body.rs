//! Sending a request body, including the explicit zero length on a bodyless
//! POST.

use crate::support::client::TestClient;
use crate::support::server::TestServer;
use xibalba_client::proto::method::Method;

#[test]
fn post_with_body() {
    let (port, server) = TestServer::echo_body();
    let mut client = TestClient::connect(port);

    let resp = client.post(b"/submit", b"hello world").unwrap();
    assert_eq!(resp.text().unwrap(), "hello world");
    let echoed = server.join().unwrap();
    assert_eq!(echoed, b"hello world");
}

#[test]
fn bodyless_post_sends_content_length_zero() {
    let (port, server) = TestServer::echo_request();
    let mut client = TestClient::connect(port);

    let response = client
        .request(Method::Post, b"/submit", None, None)
        .unwrap();
    assert_eq!(response.text().unwrap(), "");
    let request = String::from_utf8_lossy(&server.join().unwrap()).into_owned();
    assert!(
        request.contains("Content-Length: 0\r\n"),
        "bodyless POST omitted its explicit zero length:\n{request}"
    );
}

#[test]
fn post_with_empty_body() {
    let (port, server) = TestServer::echo_body();
    let mut client = TestClient::connect(port);

    let resp = client
        .request(Method::Post, b"/submit", None, Some(b""))
        .unwrap();
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}
