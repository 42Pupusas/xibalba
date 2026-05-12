use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use xibalba::client::Client;

/// Spawn a minimal HTTP/1.1 server on a random port that serves one response,
/// then closes. Returns the bound port and a join handle.
fn one_shot_server(response: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Drain the request, accumulating until we see the blank line
        let mut acc = Vec::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        loop {
            let n = stream.read(&mut tmp).unwrap();
            if n == 0 {
                break;
            }
            acc.extend_from_slice(&tmp[..n]);
            if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        stream.write_all(response).unwrap();
    });
    (port, handle)
}

#[test]
fn get_content_length_response() {
    let body = b"hello";
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
    let (port, server) = one_shot_server(response);

    let client = Client::new().unwrap();
    let url = format!("http://127.0.0.1:{port}/");
    let mut resp = client.get(url.as_bytes()).unwrap();

    assert_eq!(resp.status, xibalba::status::StatusCode::OK);
    let text = resp.text().unwrap();
    assert_eq!(text.as_bytes(), body);
    server.join().unwrap();
}

#[test]
fn get_chunked_response() {
    let response =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nworld\r\n0\r\n\r\n";
    let (port, server) = one_shot_server(response);

    let client = Client::new().unwrap();
    let url = format!("http://127.0.0.1:{port}/");
    let mut resp = client.get(url.as_bytes()).unwrap();

    assert_eq!(resp.status, xibalba::status::StatusCode::OK);
    let text = resp.text().unwrap();
    assert_eq!(text, "world");
    server.join().unwrap();
}

#[test]
fn get_no_body_204() {
    let response = b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
    let (port, server) = one_shot_server(response);

    let client = Client::new().unwrap();
    let url = format!("http://127.0.0.1:{port}/");
    let mut resp = client.get(url.as_bytes()).unwrap();

    assert_eq!(resp.status, xibalba::status::StatusCode::NO_CONTENT);
    let text = resp.text().unwrap();
    assert!(text.is_empty());
    server.join().unwrap();
}

#[test]
fn get_until_close_response() {
    let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nuntil close body";
    let (port, server) = one_shot_server(response);

    let client = Client::new().unwrap();
    let url = format!("http://127.0.0.1:{port}/");
    let mut resp = client.get(url.as_bytes()).unwrap();

    let text = resp.text().unwrap();
    assert_eq!(text, "until close body");
    server.join().unwrap();
}

#[test]
fn response_headers_accessible() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nhi";
    let (port, server) = one_shot_server(response);

    let client = Client::new().unwrap();
    let url = format!("http://127.0.0.1:{port}/");
    let resp = client.get(url.as_bytes()).unwrap();

    let ct = resp
        .headers
        .iter()
        .find(|(name, _)| name == b"Content-Type")
        .map(|(_, v)| v.as_slice());
    assert_eq!(ct, Some(b"text/plain" as &[u8]));
    server.join().unwrap();
}

#[test]
fn connection_refused_returns_error() {
    // Port 1 is reserved and will always refuse connections
    let client = Client::new().unwrap();
    let result = client.get(b"http://127.0.0.1:1/");
    assert!(result.is_err());
}
