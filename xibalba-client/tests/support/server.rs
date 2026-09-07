//! Loopback HTTP servers that answer a scripted response.
//!
//! Each constructor binds an ephemeral port, spawns a thread, and returns the
//! port with its join handle. The handle is the synchronisation point: joining
//! it proves the server finished, and for the echoing servers it carries back
//! what the client actually sent.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

/// Reads a request head from a stream, stopping at the blank line.
pub(crate) struct RequestReader;

impl RequestReader {
    /// Read until the end of the head, returning every byte consumed.
    ///
    /// The tail after `\r\n\r\n` is included when it arrived in the same
    /// packet, which the body-echoing servers rely on.
    pub(crate) fn read_head(stream: &mut TcpStream) -> Vec<u8> {
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
}

/// Scripted loopback servers.
pub(crate) struct TestServer;

impl TestServer {
    /// Bind an ephemeral loopback port, returning it with its listener.
    pub(crate) fn bind() -> (u16, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        (port, listener)
    }

    /// Answer one request with `response`, then close.
    pub(crate) fn one_shot(response: &'static [u8]) -> (u16, JoinHandle<()>) {
        let (port, listener) = Self::bind();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            RequestReader::read_head(&mut stream);
            stream.write_all(response).unwrap();
        });
        (port, handle)
    }

    /// Answer one request one byte at a time, so the client's parser sees the
    /// response arrive in the smallest possible pieces.
    pub(crate) fn drip(response: &'static [u8]) -> (u16, JoinHandle<()>) {
        let (port, listener) = Self::bind();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            RequestReader::read_head(&mut stream);
            for &b in response {
                stream.write_all(&[b]).unwrap();
            }
        });
        (port, handle)
    }

    /// Answer one request in two writes, flushing at `split_at` so the client
    /// observes a partial head.
    pub(crate) fn split(response: &'static [u8], split_at: usize) -> (u16, JoinHandle<()>) {
        let (port, listener) = Self::bind();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            RequestReader::read_head(&mut stream);
            let mid = split_at.min(response.len());
            stream.write_all(&response[..mid]).unwrap();
            stream.flush().unwrap();
            stream.write_all(&response[mid..]).unwrap();
        });
        (port, handle)
    }

    /// Answer two requests on one connection, proving the client reused it.
    pub(crate) fn keepalive(r1: &'static [u8], r2: &'static [u8]) -> (u16, JoinHandle<()>) {
        let (port, listener) = Self::bind();
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

    /// Answer with an empty 200 and hand the request head back through the
    /// join handle, for asserting on what the client sent.
    pub(crate) fn echo_request() -> (u16, JoinHandle<Vec<u8>>) {
        let (port, listener) = Self::bind();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let req = RequestReader::read_head(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            req
        });
        (port, handle)
    }

    /// Read a Content-Length delimited request body, echo it back, and return
    /// it through the join handle.
    pub(crate) fn echo_body() -> (u16, JoinHandle<Vec<u8>>) {
        let (port, listener) = Self::bind();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let req_head = RequestReader::read_head(&mut stream);
            let req_str = String::from_utf8_lossy(&req_head);
            let content_length: usize = req_str
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                .and_then(|l| l.split(':').nth(1))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);

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

            let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(&body).unwrap();
            body
        });
        (port, handle)
    }

    /// Redirect the first request to `location`, then serve `final_response`
    /// to the second on the same connection.
    pub(crate) fn redirect(
        redirect_status: u16,
        location: &'static str,
        final_response: &'static [u8],
    ) -> (u16, JoinHandle<()>) {
        let (port, listener) = Self::bind();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            RequestReader::read_head(&mut stream);
            let redirect = format!(
                "HTTP/1.1 {redirect_status} Redirect\r\nContent-Length: 0\r\nLocation: {location}\r\n\r\n"
            );
            stream.write_all(redirect.as_bytes()).unwrap();
            stream.flush().unwrap();

            RequestReader::read_head(&mut stream);
            stream.write_all(final_response).unwrap();
        });
        (port, handle)
    }
}
