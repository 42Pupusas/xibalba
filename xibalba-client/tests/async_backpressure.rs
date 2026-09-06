//! Shutdown must stay responsive while the response ring is full.
//!
//! The reader delivers body chunks into a 32-slot ring. A caller that holds a
//! handle without draining it fills that ring, and the reader then waits for
//! capacity. If that wait cannot observe shutdown, dropping the client blocks
//! forever inside `join()`.
//!
//! Every test here runs under [`Watchdog`], which fails the assertion from a
//! side thread if the operation under test does not return. A test that merely
//! deadlocks proves nothing and would hang the suite, so the bound is external
//! to the code being measured.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

use xibalba_client::async_client::{AsyncClient, Chunk};
use xibalba_client::client::Config;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::method::Method;
use xibalba_client::proto::url::Url;

/// Mirrors `async_client::CHUNK_RING_CAP`, which is private. If the reader's
/// ring grows, this must grow with it or these tests stop proving saturation.
const CHUNK_RING_CAP: usize = 32;

/// Runs a closure on a side thread and requires it to finish within a
/// deadline. The panic is raised on the test thread, so a wedged operation
/// reports as a failure instead of stalling the run.
struct Watchdog;

impl Watchdog {
    /// Run `op` under a time limit, returning its value.
    ///
    /// A timeout leaks the worker thread deliberately: it is parked in the
    /// wedged call under test and cannot be joined. The process exits when the
    /// suite finishes.
    fn run<T, F>(limit: Duration, what: &str, op: F) -> T
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let (tx, rx) = channel();
        thread::spawn(move || {
            let _ = tx.send(op());
        });
        match rx.recv_timeout(limit) {
            Ok(value) => value,
            Err(RecvTimeoutError::Timeout) => {
                panic!("{what} did not finish within {limit:?}")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("{what} panicked before producing a value")
            }
        }
    }
}

struct PlainConnector;
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

    fn connect(url: &Url<'_>, _tls: &()) -> Result<PlainStream, Error> {
        let host = std::str::from_utf8(url.host).map_err(|_| {
            Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into()))
        })?;
        let addr = format!("{}:{}", host, url.effective_port());
        TcpStream::connect(&addr)
            .map_err(Error::from)
            .map(PlainStream)
    }
}

/// A server that answers one request with an unterminated chunked body and
/// keeps writing until the client goes away.
struct FloodServer {
    port: u16,
    handle: thread::JoinHandle<usize>,
    written: Arc<AtomicUsize>,
}

impl FloodServer {
    /// `chunks` bounds how many body chunks are offered; the ring is 32
    /// slots, so a larger number guarantees the reader blocks for capacity.
    fn spawn(chunks: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        let written = Arc::new(AtomicUsize::new(0));
        let server_written = Arc::clone(&written);
        let handle = thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return 0;
            };
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            if stream
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .is_err()
            {
                return 0;
            }
            let mut sent = 0;
            for _ in 0..chunks {
                if stream.write_all(b"5\r\nflood\r\n").is_err() {
                    break;
                }
                sent += 1;
                server_written.store(sent, Ordering::Release);
            }
            let _ = stream.flush();
            // Hold the connection open; the test drives shutdown, not EOF.
            thread::sleep(Duration::from_secs(10));
            sent
        });
        Self {
            port,
            handle,
            written,
        }
    }

    /// Block until the server has pushed more chunks than the response ring
    /// can hold, so the reader is necessarily waiting for capacity rather
    /// than merely idle. Returns how many were sent.
    fn wait_until_ring_is_saturated(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let sent = self.written.load(Ordering::Acquire);
            if sent > CHUNK_RING_CAP * 2 {
                return sent;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("server never wrote enough chunks to saturate the response ring")
    }

    fn client(&self) -> AsyncClient {
        let url = format!("http://127.0.0.1:{}/", self.port);
        let config = Config {
            read_timeout: Some(Duration::from_millis(50)),
            stream_silence: Duration::from_mins(5),
            ..Config::default()
        };
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config)
            .expect("connect to the local flood server")
    }

    fn shutdown(self) {
        let _ = self.handle.join();
    }
}

#[test]
fn drop_returns_while_a_full_response_ring_is_undrained() {
    // The caller reads the head, then stops. The reader fills the 32-slot
    // ring and waits for room that never comes. Drop must still return.
    let server = FloodServer::spawn(512);
    let client = server.client();

    let mut handle = client
        .submit(Method::Get, b"/flood".to_vec(), None, None, vec![])
        .expect("submit succeeds");
    assert!(
        matches!(handle.next_block(), Some(Chunk::Head { status: 200, .. })),
        "the head must arrive before the ring fills"
    );

    server.wait_until_ring_is_saturated();

    let elapsed = Watchdog::run(
        Duration::from_secs(10),
        "drop with a full response ring",
        move || {
            let started = Instant::now();
            drop(client);
            started.elapsed()
        },
    );

    assert!(
        elapsed < Duration::from_secs(5),
        "drop took {elapsed:?}; shutdown was not observed while waiting for ring capacity"
    );
    drop(handle);
    server.shutdown();
}

#[test]
fn cancel_returns_while_a_full_response_ring_is_undrained() {
    // Same wedge, reached through cancel: the control message must be
    // accepted even though the reader is waiting on the response ring.
    let server = FloodServer::spawn(512);
    let client = server.client();

    let mut handle = client
        .submit(Method::Get, b"/flood".to_vec(), None, None, vec![])
        .expect("submit succeeds");
    assert!(
        matches!(handle.next_block(), Some(Chunk::Head { status: 200, .. })),
        "the head must arrive before the ring fills"
    );
    server.wait_until_ring_is_saturated();

    let client = Watchdog::run(
        Duration::from_secs(10),
        "cancel with a full response ring",
        move || {
            client.cancel().expect("cancel reaches the reader");
            client
        },
    );

    drop(handle);
    drop(client);
    server.shutdown();
}

#[test]
fn dropping_the_handle_lets_the_reader_finish() {
    // A dropped consumer closes the ring. The reader must notice and stop
    // rather than wait for capacity on a ring nobody will drain.
    let server = FloodServer::spawn(512);
    let client = server.client();

    let mut handle = client
        .submit(Method::Get, b"/flood".to_vec(), None, None, vec![])
        .expect("submit succeeds");
    assert!(
        matches!(handle.next_block(), Some(Chunk::Head { status: 200, .. })),
        "the head must arrive before the ring fills"
    );
    server.wait_until_ring_is_saturated();
    drop(handle);

    let elapsed = Watchdog::run(
        Duration::from_secs(10),
        "drop after the consumer went away",
        move || {
            let started = Instant::now();
            drop(client);
            started.elapsed()
        },
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "drop took {elapsed:?} after the handle was dropped"
    );
    server.shutdown();
}
