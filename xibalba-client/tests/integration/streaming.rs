//! Chunked streaming where the chunk boundaries fall awkwardly against the
//! reads that deliver them.

use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::registry::ScriptedServer;
use crate::support::script::Script;
use crate::support::server::RequestReader;
use xibalba_client::async_client::Chunk;
use xibalba_client::proto::method::Method;

#[test]
fn streaming_chunked_need_more_is_not_eof() {
    // Regression test: when the chunked decoder consumes a whole chunk but
    // the next chunk has not arrived yet, `StreamingBody::read` used to
    // return `Ok(0)`. The async reader (and any `Read` consumer) interprets
    // a zero-byte read as EOF, cutting the stream short.
    //
    // The gap between the two chunks is the whole test, and it used to be a
    // 250ms sleep chosen to exceed a read-timeout window. The barrier states
    // the same thing exactly: the second chunk is not sent until the client
    // has read the first, so the decoder is guaranteed to hit `NeedMore`
    // mid-body rather than probably hitting it.
    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send_then_await(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .send(b"6\r\nsecond\r\n0\r\n\r\n"),
    );

    let client = TestClient::scripted_async(&server);
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();

    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));

    let mut chunks = Vec::new();
    while let Some(chunk) = handle.next_block() {
        if matches!(chunk, Chunk::Eof) {
            break;
        }
        chunks.push(chunk);
    }

    assert_eq!(
        chunks,
        vec![
            Chunk::Body(b"first".to_vec()),
            Chunk::Body(b"second".to_vec()),
        ]
    );

    server.only().assert_script_completed();
    // Without this the barrier is unfalsifiable: delete it, the two chunks
    // arrive in one read, and every assertion above still passes while the
    // decoder never reaches the `NeedMore` this test exists for.
    server.only().assert_data_reads(2);
}

#[test]
fn streaming_chunked_many_short_chunks_with_gaps() {
    // Adversarial: many tiny chunks delivered with small gaps. Each gap is
    // an opportunity for the decoder to return `NeedMore` -> `Ok(0)` -> EOF.
    // The stream must survive all of them and the connection stays clean.
    let chunks: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        for data in &chunks {
            let hex = format!("{:x}\r\n", data.len());
            stream.write_all(hex.as_bytes()).unwrap();
            stream.write_all(data).unwrap();
            stream.write_all(b"\r\n").unwrap();
            stream.flush().unwrap();
            thread::sleep(Duration::from_millis(30));
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();

        // Prove the connection is still reusable.
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });

    let client = TestClient::connect_async(port);
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();

    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));

    let mut body = String::new();
    while let Some(chunk) = handle.next_block() {
        match chunk {
            Chunk::Body(b) => body.push_str(&String::from_utf8_lossy(&b)),
            Chunk::Eof => break,
            other => panic!("unexpected chunk: {other:?}"),
        }
    }
    assert_eq!(body, "abcde");

    let mut handle2 = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle2.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));
    assert_eq!(handle2.next_block(), Some(Chunk::Body(b"ok".to_vec())));
    assert_eq!(handle2.next_block(), Some(Chunk::Eof));

    server.join().unwrap();
}

#[test]
fn streaming_chunked_single_byte_chunks() {
    // Adversarial: one byte per chunk. The decoder crosses `ReadingDataCr`,
    // `ReadingDataLf`, and `ReadingSize` repeatedly; it must not confuse
    // the chunk boundary parsing with EOF.
    let body = b"hello";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        for &b in body {
            stream.write_all(b"1\r\n").unwrap();
            stream.write_all(&[b]).unwrap();
            stream.write_all(b"\r\n").unwrap();
            stream.flush().unwrap();
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();
    });

    let client = TestClient::connect_async(port);
    let mut handle = client
        .submit(Method::Get, b"/".to_vec(), None, None, vec![])
        .unwrap();
    assert!(matches!(
        handle.next_block(),
        Some(Chunk::Head { status: 200, .. })
    ));

    let mut got = Vec::new();
    while let Some(chunk) = handle.next_block() {
        match chunk {
            Chunk::Body(b) => got.extend_from_slice(&b),
            Chunk::Eof => break,
            other => panic!("unexpected chunk: {other:?}"),
        }
    }
    assert_eq!(got, body);

    server.join().unwrap();
}
