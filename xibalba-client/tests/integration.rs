use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use xibalba_client::client::{Client, Config};
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::method::Method;
use xibalba_client::proto::url::Url;

// ── Plain TCP connector ───────────────────────────────────────────────────────

struct PlainConnector;
struct PlainStream(TcpStream);

impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> std::io::Result<()> {
        self.0.set_read_timeout(dur)
    }
}

impl Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl Connector for PlainConnector {
    type Stream = PlainStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, _tls_config: &()) -> Result<PlainStream, Error> {
        let host = std::str::from_utf8(url.host).map_err(|_| {
            Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into()))
        })?;
        let addr = format!("{}:{}", host, url.effective_port());
        TcpStream::connect(&addr)
            .map_err(Error::from)
            .map(PlainStream)
    }
}

// ── Test server helpers ───────────────────────────────────────────────────────

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 4096];
    let mut acc = Vec::new();
    loop {
        let n = stream.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&buf[..n]);
        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    acc
}

fn one_shot_server(response: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.write_all(response).unwrap();
    });
    (port, handle)
}

fn connect(port: u16) -> Client<PlainConnector> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector>::connect_default(url.as_bytes(), ()).unwrap()
}

fn connect_with_config(port: u16, config: Config) -> Client<PlainConnector> {
    let url = format!("http://127.0.0.1:{port}/");
    Client::<PlainConnector>::connect(url.as_bytes(), (), config).unwrap()
}

// ── Original tests (updated API) ─────────────────────────────────────────────

#[test]
fn get_content_length_response() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn get_chunked_response() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nworld\r\n0\r\n\r\n";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "world");
    server.join().unwrap();
}

#[test]
fn get_no_body_204() {
    let response = b"HTTP/1.1 204 No Content\r\n\r\n";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::NO_CONTENT);
    assert!(resp.text().unwrap().is_empty());
    server.join().unwrap();
}

#[test]
fn get_until_close_response() {
    let response = b"HTTP/1.1 200 OK\r\n\r\nuntil close body";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "until close body");
    server.join().unwrap();
}

#[test]
fn response_headers_accessible() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nhi";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

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

// ── Adversarial integration tests ────────────────────────────────────────────

fn drip_server(response: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        for &b in response {
            stream.write_all(&[b]).unwrap();
        }
    });
    (port, handle)
}

fn split_server(response: &'static [u8], split_at: usize) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let mid = split_at.min(response.len());
        stream.write_all(&response[..mid]).unwrap();
        stream.flush().unwrap();
        stream.write_all(&response[mid..]).unwrap();
    });
    (port, handle)
}

fn keepalive_server(r1: &'static [u8], r2: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        for response in [r1, r2] {
            let mut acc = Vec::new();
            loop {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    return;
                }
                acc.extend_from_slice(&buf[..n]);
                if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            stream.write_all(response).unwrap();
            stream.flush().unwrap();
        }
    });
    (port, handle)
}

#[test]
fn server_sends_response_byte_at_a_time() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
    let (port, server) = drip_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "abc");
    server.join().unwrap();
}

#[test]
fn server_sends_headers_split_across_reads() {
    let response: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let (port, server) = split_server(response, 20);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn server_sends_empty_chunked_body() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    let (port, server) = one_shot_server(response);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "");
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
    let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    let body = b"5\r\nhello\r\n0\r\n\r\n";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.set_nodelay(true).unwrap();
        // Two writes with a pause so the head arrives alone and the
        // entire chunked body (data + terminator) lands in one read.
        stream.write_all(head).unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(100));
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
        // Hold the connection open: a buggy client blocks here.
        thread::sleep(Duration::from_millis(500));
    });

    let config = Config {
        read_timeout: Some(Duration::from_secs(2)),
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn multiple_requests_same_connection() {
    let r1: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst";
    let r2: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond";
    let (port, server) = keepalive_server(r1, r2);
    let mut client = connect(port);

    let resp1 = client.request(Method::Get, b"/one", None, None).unwrap();
    assert_eq!(resp1.text().unwrap(), "first");

    let resp2 = client.request(Method::Get, b"/two", None, None).unwrap();
    assert_eq!(resp2.text().unwrap(), "second");

    server.join().unwrap();
}

#[test]
fn very_large_header_value() {
    let big_value = "X".repeat(4096);
    let response_str = format!(
        "HTTP/1.1 200 OK\r\nX-Big: {}\r\nContent-Length: 2\r\n\r\nok",
        big_value
    );
    let response_bytes: &'static [u8] = Box::leak(response_str.into_bytes().into_boxed_slice());
    let (port, server) = one_shot_server(response_bytes);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    let big_hdr = resp
        .headers()
        .find(|(name, _)| *name == b"X-Big")
        .map(|(_, v)| v);
    assert_eq!(big_hdr.map(|v| v.len()), Some(4096));
    assert_eq!(resp.text().unwrap(), "ok");
    server.join().unwrap();
}

#[test]
fn server_closes_connection_before_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        drop(stream);
    });
    let mut client = connect(port);
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

    let (port, server) = one_shot_server(response_bytes);
    let mut client = connect(port);

    let resp = client.request(Method::Get, b"/", None, None).unwrap();
    let header_count = resp.headers().count();
    assert!(header_count >= 30);
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}

// ── Host header tests ────────────────────────────────────────────────────────

fn echo_request_server() -> (u16, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let req = read_request(&mut stream);
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        stream.write_all(response).unwrap();
        req
    });
    (port, handle)
}

#[test]
fn host_header_sent() {
    let (port, server) = echo_request_server();
    let mut client = connect(port);
    client.get(b"/test").unwrap();
    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains(&format!("Host: 127.0.0.1:{port}")),
        "expected Host header with port, got:\n{req_str}"
    );
}

#[test]
fn host_header_with_default_port() {
    let (port, server) = echo_request_server();
    // Non-default port so Host should include it
    let mut client = connect(port);
    client.get(b"/").unwrap();
    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains("Host: 127.0.0.1:"),
        "Host header must include non-default port"
    );
}

// ── Timeout tests ────────────────────────────────────────────────────────────

#[test]
fn timeout_fires_on_stalled_server() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        // Never send a response — just sleep
        thread::sleep(Duration::from_secs(10));
        drop(stream);
    });

    let config = Config {
        read_timeout: Some(Duration::from_millis(100)),
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let start = std::time::Instant::now();
    let result = client.request(Method::Get, b"/", None, None);
    assert!(result.is_err(), "expected timeout error");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "should time out quickly"
    );
    drop(server);
}

// ── Request body tests ───────────────────────────────────────────────────────

fn echo_body_server() -> (u16, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let req_head = read_request(&mut stream);
        let req_str = String::from_utf8_lossy(&req_head);
        let content_length: usize = req_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);

        // Read request body
        let head_end = req_head.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already_read = req_head.len() - head_end;
        let mut body = req_head[head_end..].to_vec();
        if body.len() < content_length {
            let remaining = content_length - already_read;
            let mut rest = vec![0u8; remaining];
            stream.read_exact(&mut rest).unwrap();
            body.extend_from_slice(&rest);
        }
        body.truncate(content_length);

        // Echo the body back
        let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
        body
    });
    (port, handle)
}

#[test]
fn post_with_body() {
    let (port, server) = echo_body_server();
    let mut client = connect(port);

    let resp = client.post(b"/submit", b"hello world").unwrap();
    assert_eq!(resp.text().unwrap(), "hello world");
    let echoed = server.join().unwrap();
    assert_eq!(echoed, b"hello world");
}

#[test]
fn post_with_empty_body() {
    let (port, server) = echo_body_server();
    let mut client = connect(port);

    let resp = client
        .request(Method::Post, b"/submit", None, Some(b""))
        .unwrap();
    assert_eq!(resp.text().unwrap(), "");
    server.join().unwrap();
}

// ── Redirect tests ───────────────────────────────────────────────────────────

fn redirect_server(
    redirect_status: u16,
    location: &'static str,
    final_response: &'static [u8],
) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // First request: redirect
        read_request(&mut stream);
        let redirect = format!(
            "HTTP/1.1 {redirect_status} Redirect\r\nContent-Length: 0\r\nLocation: {location}\r\n\r\n"
        );
        stream.write_all(redirect.as_bytes()).unwrap();
        stream.flush().unwrap();

        // Second request: final response
        read_request(&mut stream);
        stream.write_all(final_response).unwrap();
    });
    (port, handle)
}

#[test]
fn redirect_301_followed() {
    let final_resp = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone";
    let (port, server) = redirect_server(301, "/final", final_resp);
    let mut client = connect(port);

    let resp = client.get(b"/start").unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}

#[test]
fn redirect_chain() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Hop 1: 301 → /hop2
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /hop2\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Hop 2: 302 → /final
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /final\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Final
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\narrived")
            .unwrap();
    });
    let mut client = connect(port);
    let resp = client.get(b"/start").unwrap();
    assert_eq!(resp.text().unwrap(), "arrived");
    server.join().unwrap();
}

#[test]
fn redirect_max_exceeded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Infinite redirect loop
        loop {
            let req = read_request(&mut stream);
            if req.is_empty() {
                break;
            }
            let resp = b"HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /loop\r\n\r\n";
            if stream.write_all(resp).is_err() {
                break;
            }
            stream.flush().ok();
        }
    });

    let config = Config {
        max_redirects: 3,
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let result = client.get(b"/start");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::TooManyRedirects)
    );
    drop(server);
}

#[test]
fn redirect_307_preserves_method_and_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // First request: 307 redirect
        let req1 = read_request(&mut stream);
        assert!(String::from_utf8_lossy(&req1).starts_with("POST "));
        stream
            .write_all(b"HTTP/1.1 307 Temporary\r\nContent-Length: 0\r\nLocation: /target\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Read the body from first request
        let req1_str = String::from_utf8_lossy(&req1);
        let cl: usize = req1_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let head_end = req1.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already = req1.len() - head_end;
        if already < cl {
            let mut rest = vec![0u8; cl - already];
            stream.read_exact(&mut rest).ok();
        }

        // Second request: should still be POST with body
        let req2 = read_request(&mut stream);
        let req2_str = String::from_utf8_lossy(&req2);
        assert!(
            req2_str.starts_with("POST "),
            "expected POST after 307, got: {req2_str}"
        );
        // Read the body
        let cl2: usize = req2_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let head_end2 = req2.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already2 = req2.len() - head_end2;
        let mut body2 = req2[head_end2..].to_vec();
        if already2 < cl2 {
            let mut rest = vec![0u8; cl2 - already2];
            stream.read_exact(&mut rest).ok();
            body2.extend_from_slice(&rest);
        }
        body2.truncate(cl2);

        let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body2.len());
        stream.write_all(resp.as_bytes()).unwrap();
        stream.write_all(&body2).unwrap();
    });

    let mut client = connect(port);
    let resp = client.post(b"/original", b"preserved").unwrap();
    assert_eq!(resp.text().unwrap(), "preserved");
    server.join().unwrap();
}

// ── Builder / custom header tests ────────────────────────────────────────────

#[test]
fn builder_sends_custom_headers() {
    let (port, server) = echo_request_server();
    let mut client = connect(port);

    client
        .build(Method::Get, b"/api")
        .header(b"Authorization", b"Bearer tok123")
        .header(b"Content-Type", b"application/json")
        .send()
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
    let (port, server) = echo_body_server();
    let mut client = connect(port);

    let resp = client
        .build(Method::Put, b"/upload")
        .header(b"Content-Type", b"text/plain")
        .body(b"file contents")
        .send()
        .unwrap();

    assert_eq!(resp.text().unwrap(), "file contents");
    server.join().unwrap();
}

#[test]
fn builder_no_hardcoded_user_agent() {
    let (port, server) = echo_request_server();
    let mut client = connect(port);

    client.build(Method::Get, b"/check").send().unwrap();

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

// ── Size limit tests ─────────────────────────────────────────────────────────

#[test]
fn body_too_large_rejected() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.write_all(response).unwrap();
        // Send 100 bytes of body
        stream.write_all(&[b'X'; 100]).unwrap();
    });

    let config = Config {
        max_response_body: 50,
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let result = client.get(b"/big");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::BodyTooLarge)
    );
    drop(server);
}

#[test]
fn head_too_large_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        // Send a response with a huge header that exceeds the limit
        stream.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        stream.write_all(&[b'A'; 2000]).unwrap();
        stream.write_all(b"\r\nContent-Length: 0\r\n\r\n").unwrap();
    });

    let config = Config {
        max_head_size: 256,
        ..Config::default()
    };
    let mut client = connect_with_config(port, config);
    let result = client.get(b"/huge-head");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::HeadTooLarge)
    );
    drop(server);
}
