//! Caller-supplied headers, and the managed ones the builder refuses.

use std::net::TcpListener;

use crate::support::client::TestClient;
use crate::support::server::TestServer;
use xibalba_client::PlainConnector;
use xibalba_client::async_client::AsyncClient;
use xibalba_client::client::Config;
use xibalba_client::proto::method::Method;

#[test]
fn builder_sends_custom_headers() {
    let (port, server) = TestServer::echo_request();
    let mut client = TestClient::connect(port);

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
    let (port, server) = TestServer::echo_body();
    let mut client = TestClient::connect(port);

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
fn builder_rejects_managed_header_as_an_error() {
    // Validation fires before any byte is written, so a bare listener
    // is enough for the client to connect to.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TestClient::connect(listener.local_addr().unwrap().port());

    let err = client
        .send(
            client
                .build(Method::Get, b"/")
                .header(b"Host", b"attacker.example"),
        )
        .unwrap_err();
    assert_eq!(
        err,
        xibalba_client::proto::error::Error::Serialize(
            xibalba_client::proto::error::SerializeError::DuplicateHeader
        )
    );
}

#[test]
fn builder_rejects_duplicate_header_name_as_an_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TestClient::connect(listener.local_addr().unwrap().port());

    let err = client
        .send(
            client
                .build(Method::Get, b"/")
                .header(b"X-Trace", b"a")
                .header(b"x-trace", b"b"),
        )
        .unwrap_err();
    assert_eq!(
        err,
        xibalba_client::proto::error::Error::Serialize(
            xibalba_client::proto::error::SerializeError::DuplicateHeader
        )
    );
}

#[test]
fn builder_multiple_cookie_headers_are_allowed() {
    let (port, server) = TestServer::echo_request();
    let mut client = TestClient::connect(port);

    client
        .send(
            client
                .build(Method::Get, b"/")
                .header(b"Cookie", b"a=1")
                .header(b"Cookie", b"b=2"),
        )
        .unwrap();

    let req_bytes = server.join().unwrap();
    let req = String::from_utf8_lossy(&req_bytes);
    assert!(req.contains("Cookie: a=1"), "missing first cookie:\n{req}");
    assert!(req.contains("Cookie: b=2"), "missing second cookie:\n{req}");
}

#[test]
fn async_submit_rejects_duplicate_header_name_as_an_error() {
    // TestClient::connect() must succeed, so something has to listen; validation
    // then rejects the headers before the reader thread is involved.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://127.0.0.1:{}/",
        listener.local_addr().unwrap().port()
    );
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), Config::default()).unwrap();

    let err = client
        .submit(
            Method::Get,
            b"/".to_vec(),
            None,
            None,
            vec![
                (b"X-Trace".to_vec(), b"a".to_vec()),
                (b"x-trace".to_vec(), b"b".to_vec()),
            ],
        )
        .err()
        .expect("submit must reject duplicate header names");
    assert_eq!(
        err,
        xibalba_client::proto::error::Error::Serialize(
            xibalba_client::proto::error::SerializeError::DuplicateHeader
        )
    );
}

#[test]
fn builder_no_hardcoded_user_agent() {
    let (port, server) = TestServer::echo_request();
    let mut client = TestClient::connect(port);

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
