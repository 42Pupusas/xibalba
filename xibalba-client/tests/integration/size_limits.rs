//! Head and body size limits, and the reconnect an oversized head forces.

use std::io::Write;
use std::net::TcpListener;
use std::thread;

use crate::support::client::SMALL_HEAD_SIZE;
use crate::support::client::TestClient;
use crate::support::server::RequestReader;
use xibalba_client::PlainConnector;
use xibalba_client::async_client::AsyncClient;
use xibalba_client::async_client::Chunk;
use xibalba_client::client::Config;
use xibalba_client::proto::error::ConnectionError;
use xibalba_client::proto::error::Error;
use xibalba_client::proto::method::Method;

#[test]
fn body_too_large_rejected() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream.write_all(response).unwrap();
        // Send 100 bytes of body
        stream.write_all(&[b'X'; 100]).unwrap();
    });

    let config = Config {
        max_response_body: 50,
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);
    let result = client.get(b"/big");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::BodyTooLarge)
    );
    drop(server);
}

#[test]
fn default_limit_accepts_large_api_response_head() {
    // Gateways can add substantial tracing and rate-limit metadata. The
    // default must accept a normal 32 KiB response head, and the complete
    // header must remain available after parsing rather than being truncated
    // to the socket read buffer.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nX-Gateway-Metadata: ")
            .unwrap();
        stream.write_all(&vec![b'M'; 32 * 1024]).unwrap();
        stream
            .write_all(b"\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });

    let mut client = TestClient::connect(port);
    let response = client.get(b"/large-head").unwrap();
    let metadata = response
        .headers()
        .find(|(name, _)| *name == b"X-Gateway-Metadata")
        .map(|(_, value)| value)
        .expect("large gateway header must be preserved");
    assert_eq!(metadata.len(), 32 * 1024);
    assert!(metadata.iter().all(|&byte| byte == b'M'));
    assert_eq!(response.text().unwrap(), "ok");

    server.join().unwrap();
}

#[test]
fn head_at_limit_with_body_tail_is_accepted() {
    // The read that finds `\r\n\r\n` often includes body bytes too. Count
    // only the head toward max_head; rejecting the combined read would make a
    // correctly sized gateway response fail depending on packet boundaries.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        let prefix = b"HTTP/1.1 200 OK\r\nX-Fill: ";
        let suffix = b"\r\nContent-Length: 2\r\n\r\n";
        let fill_len = 256 - prefix.len() - suffix.len();
        stream.write_all(prefix).unwrap();
        stream.write_all(&vec![b'F'; fill_len]).unwrap();
        stream.write_all(suffix).unwrap();
        stream.write_all(b"ok").unwrap();
    });

    let mut client = TestClient::with_small_head_limit(port, Config::default());
    assert_eq!(client.get(b"/at-limit").unwrap().text().unwrap(), "ok");

    server.join().unwrap();
}

#[test]
fn oversized_head_forces_reconnect_before_next_request() {
    // Reading enough bytes to reject a head leaves an indeterminate suffix on
    // the socket. The following request must use a new connection, not parse
    // that suffix as a response head.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut first);
        first.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        first.write_all(&[b'A'; 2_000]).unwrap();
        first
            .write_all(b"\r\nContent-Length: 5\r\n\r\nstale")
            .unwrap();
        first.flush().unwrap();

        // The client deliberately abandons `first`; a clean retry arrives on
        // a separate socket and gets an unrelated valid answer.
        let (mut second, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    let mut client = TestClient::with_small_head_limit(port, Config::default());
    assert_eq!(
        client.get(b"/too-large").unwrap_err(),
        Error::Connection(ConnectionError::HeadTooLarge)
    );
    assert_eq!(client.get(b"/retry").unwrap().text().unwrap(), "fresh");

    server.join().unwrap();
}

#[test]
fn async_oversized_head_forces_reconnect_before_next_request() {
    // The agent uses AsyncClient. Its reader must carry the dirty marker from
    // a rejected head into the next queued request just like Client does.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut first);
        first.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        first.write_all(&vec![b'A'; 2_000]).unwrap();
        first
            .write_all(b"\r\nContent-Length: 5\r\n\r\nstale")
            .unwrap();
        first.flush().unwrap();

        let (mut second, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
            .unwrap();
    });

    // The async facade carries the same const generic into its reader-owned
    // Client, so deployments can pick a tighter bound without runtime state.
    let url = format!("http://127.0.0.1:{port}/");
    let client = AsyncClient::<SMALL_HEAD_SIZE>::connect::<PlainConnector>(
        url.as_bytes(),
        (),
        Config::default(),
    )
    .unwrap();
    let mut rejected = client
        .submit(Method::Get, b"/too-large".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        rejected.next_block(),
        Some(Chunk::Error(error))
            if *error == Error::Connection(ConnectionError::HeadTooLarge)
    ));
    drop(rejected);

    let mut retry = client
        .submit(Method::Get, b"/retry".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        retry.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(retry.next_block(), Some(Chunk::Body(b"fresh".to_vec())));
    assert_eq!(retry.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn head_too_large_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        // Send a response with a huge header that exceeds the limit
        stream.write_all(b"HTTP/1.1 200 OK\r\nX-Huge: ").unwrap();
        stream.write_all(&[b'A'; 2000]).unwrap();
        stream.write_all(b"\r\nContent-Length: 0\r\n\r\n").unwrap();
    });

    let mut client = TestClient::with_small_head_limit(port, Config::default());
    let result = client.get(b"/huge-head");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::HeadTooLarge)
    );
    drop(server);
}
