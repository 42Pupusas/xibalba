//! Chunked streaming where the chunk boundaries fall awkwardly against the
//! reads that deliver them.
//!
//! These run scripted: the subject is which bytes share a read, and a socket
//! decides that by kernel buffering and scheduling rather than by anything the
//! test states. Where a test depends on a particular split, it asserts the
//! read count, because otherwise a collapsed split still passes.

use crate::support::client::TestClient;
use crate::support::registry::ScriptedServer;
use crate::support::script::Script;
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
    // Adversarial: many tiny chunks, each separated from the next. Every gap
    // is an opportunity for the decoder to return `NeedMore` -> `Ok(0)` ->
    // EOF. The stream must survive all of them and the connection stays
    // clean.
    //
    // The gaps were 30ms sleeps, which only made separate reads likely; a
    // barrier after each chunk makes every gap real, and the read count below
    // holds the script to it.
    const CHUNKS: [&[u8]; 5] = [b"a", b"b", b"c", b"d", b"e"];
    let mut script = Script::new()
        .expect_request()
        .send_then_await(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec());
    for data in CHUNKS {
        let mut chunk = format!("{:x}\r\n", data.len()).into_bytes();
        chunk.extend_from_slice(data);
        chunk.extend_from_slice(b"\r\n");
        script = script.send_then_await(chunk);
    }
    let server = ScriptedServer::serving_one(
        script
            .send(b"0\r\n\r\n".to_vec())
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_vec()),
    );

    let client = TestClient::scripted_async(&server);
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

    // Head, one read per chunk, the terminator, and the second response. If
    // any gap collapsed, the decoder never faced the `NeedMore` at that
    // boundary and this test would be quietly weaker than it reads.
    server.only().assert_data_reads(CHUNKS.len() + 3);
    server.only().assert_script_completed();
}

#[test]
fn streaming_chunked_single_byte_chunks() {
    // Adversarial: one byte per chunk. The decoder crosses `ReadingDataCr`,
    // `ReadingDataLf`, and `ReadingSize` repeatedly; it must not confuse
    // the chunk boundary parsing with EOF.
    //
    // Unlike its neighbours this test does not care how the bytes are split —
    // only that the decoder handles minimal chunks — so the script sends them
    // without barriers and asserts no read count.
    const BODY: &[u8] = b"hello";
    let mut chunks = Vec::new();
    for &b in BODY {
        chunks.extend_from_slice(b"1\r\n");
        chunks.push(b);
        chunks.extend_from_slice(b"\r\n");
    }
    chunks.extend_from_slice(b"0\r\n\r\n");

    let server = ScriptedServer::serving_one(
        Script::new()
            .expect_request()
            .send(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec())
            .send(chunks),
    );

    let client = TestClient::scripted_async(&server);
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
    assert_eq!(got, BODY);

    server.only().assert_script_completed();
}
