//! Reading a response: each body framing, plus the transfer codings the
//! client refuses to decode.

use crate::support::client::TestClient;
use crate::support::server::TestServer;
use xibalba_client::PlainConnector;
use xibalba_client::client::Client;
use xibalba_client::proto::error::Error;
use xibalba_client::proto::method::Method;

#[test]
fn get_content_length_response() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn get_chunked_response() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nworld\r\n0\r\n\r\n";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "world");
    server.join().unwrap();
}

#[test]
fn get_no_body_204() {
    let response = b"HTTP/1.1 204 No Content\r\n\r\n";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

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
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "until close body");
    server.join().unwrap();
}

#[test]
fn gzip_transfer_coding_surfaces_as_an_error() {
    // Transfer-coded gzip is not something this client can decode. Returning
    // the bytes as the body hands the caller compressed data presented as the
    // decoded response, with nothing indicating it is still encoded.
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n\x1f\x8b\x08";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    let err = client
        .request(Method::Get, b"/", None, None)
        .expect_err("an undecodable transfer coding must surface");
    assert!(
        matches!(
            err,
            Error::Parse(xibalba_client::proto::error::ParseError::UnsupportedTransferCoding)
        ),
        "expected UnsupportedTransferCoding, got {err:?}"
    );
    server.join().unwrap();
}

#[test]
fn gzip_over_chunked_surfaces_as_an_error() {
    // The dangerous shape: framing is readable, so the body was dechunked and
    // returned while still gzip-encoded.
    let response =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

    let err = client
        .request(Method::Get, b"/", None, None)
        .expect_err("a chunked body still gzip-coded must surface");
    assert!(
        matches!(
            err,
            Error::Parse(xibalba_client::proto::error::ParseError::UnsupportedTransferCoding)
        ),
        "expected UnsupportedTransferCoding, got {err:?}"
    );
    server.join().unwrap();
}

#[test]
fn response_headers_accessible() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nhi";
    let (port, server) = TestServer::one_shot(response);
    let mut client = TestClient::connect(port);

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
