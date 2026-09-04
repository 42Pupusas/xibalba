use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

/// A throwaway HTTP/1.1 echo server for benchmarks and examples.
///
/// Accepts keep-alive connections, drains each request (headers plus any
/// `Content-Length` body), and replies with a fixed response on every
/// request on the same connection.
pub struct EchoServer;

impl EchoServer {
    /// Spawn a server that replies with `response` to every request,
    /// sharing one buffer across connections.
    ///
    /// # Panics
    ///
    /// Panics if binding a local TCP listener fails.
    #[must_use]
    pub fn spawn(response: &'static [u8]) -> u16 {
        Self::spawn_owned(response.to_vec())
    }

    /// Spawn a server that replies with `response` to every request. Each
    /// accepted connection clones its own copy, so an owned (non-`'static`)
    /// buffer works too.
    ///
    /// # Panics
    ///
    /// Panics if binding a local TCP listener fails.
    #[must_use]
    pub fn spawn_owned(response: Vec<u8>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(conn) = stream else { continue };
                let resp = response.clone();
                std::thread::spawn(move || Self::serve_connection(conn, &resp));
            }
        });
        port
    }

    fn serve_connection(mut conn: TcpStream, response: &[u8]) {
        let mut hdr_buf = Vec::with_capacity(512);
        let mut raw = [0u8; 4096];
        loop {
            let Some(header_end) = Self::read_headers(&mut conn, &mut hdr_buf, &mut raw) else {
                return;
            };
            if !Self::drain_body(&mut conn, &hdr_buf, header_end) {
                return;
            }
            if conn.write_all(response).is_err() {
                return;
            }
            hdr_buf.clear();
        }
    }

    fn read_headers(
        conn: &mut TcpStream,
        hdr_buf: &mut Vec<u8>,
        raw: &mut [u8; 4096],
    ) -> Option<usize> {
        loop {
            let n = conn.read(raw).unwrap_or(0);
            if n == 0 {
                return None;
            }
            hdr_buf.extend_from_slice(&raw[..n]);
            if let Some(pos) = hdr_buf.windows(4).position(|w| w == b"\r\n\r\n") {
                return Some(pos + 4);
            }
        }
    }

    fn drain_body(conn: &mut TcpStream, hdr_buf: &[u8], header_end: usize) -> bool {
        let body_len: usize = std::str::from_utf8(&hdr_buf[..header_end])
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split_once(':'))
                    .and_then(|(_, v)| v.trim().parse().ok())
            })
            .unwrap_or(0);
        let already_read = hdr_buf.len() - header_end;
        let mut remaining = body_len.saturating_sub(already_read);
        let mut discard = [0u8; 4096];
        while remaining > 0 {
            let n = conn.read(&mut discard[..remaining.min(4096)]).unwrap_or(0);
            if n == 0 {
                return false;
            }
            remaining -= n;
        }
        true
    }
}
