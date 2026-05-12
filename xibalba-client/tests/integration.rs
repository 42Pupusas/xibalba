use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use xibalba_client::client::Client;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_proto::error::Error;
use xibalba_proto::url::Url;

// --- Minimal plain-TCP connector (no TLS, no vendor deps) ---

struct PlainConnector;

// Newtype to satisfy the orphan rule.
struct PlainStream(TcpStream);

impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
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
        let host = std::str::from_utf8(url.host)
            .map_err(|_| Error::Connection("invalid UTF-8 in host".into()))?;
        let addr = format!("{}:{}", host, url.effective_port());
        TcpStream::connect(&addr).map_err(Error::from).map(PlainStream)
    }
}

// --- One-shot test server ---

fn one_shot_server(response: &'static [u8]) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
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

fn client() -> Client<PlainConnector> {
    Client::<PlainConnector>::new(())
}

// --- Tests ---

#[test]
fn get_content_length_response() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
    let (port, server) = one_shot_server(response);

    let mut resp = client()
        .get(format!("http://127.0.0.1:{port}/").as_bytes())
        .unwrap();

    assert_eq!(resp.status, xibalba_proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "hello");
    server.join().unwrap();
}

#[test]
fn get_chunked_response() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nworld\r\n0\r\n\r\n";
    let (port, server) = one_shot_server(response);

    let mut resp = client()
        .get(format!("http://127.0.0.1:{port}/").as_bytes())
        .unwrap();

    assert_eq!(resp.text().unwrap(), "world");
    server.join().unwrap();
}

#[test]
fn get_no_body_204() {
    let response = b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
    let (port, server) = one_shot_server(response);

    let mut resp = client()
        .get(format!("http://127.0.0.1:{port}/").as_bytes())
        .unwrap();

    assert_eq!(
        resp.status,
        xibalba_proto::status::StatusCode::NO_CONTENT
    );
    assert!(resp.text().unwrap().is_empty());
    server.join().unwrap();
}

#[test]
fn get_until_close_response() {
    let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nuntil close body";
    let (port, server) = one_shot_server(response);

    let mut resp = client()
        .get(format!("http://127.0.0.1:{port}/").as_bytes())
        .unwrap();

    assert_eq!(resp.text().unwrap(), "until close body");
    server.join().unwrap();
}

#[test]
fn response_headers_accessible() {
    let response =
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nhi";
    let (port, server) = one_shot_server(response);

    let resp = client()
        .get(format!("http://127.0.0.1:{port}/").as_bytes())
        .unwrap();

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
    let result = client().get(b"http://127.0.0.1:1/");
    assert!(result.is_err());
}
