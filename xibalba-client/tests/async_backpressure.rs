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
//!
//! Servers hold their sockets open with [`StopSignal`] rather than a fixed
//! sleep. Emptying `StopPark::wait` fails exactly one test here,
//! `cancel_interrupts_a_silent_response_head` — the only one whose server
//! reaches the park while the client is still waiting on it. `FloodServer`
//! blocks writing into a socket the client has stopped draining and never
//! reaches the park at all, so what pins those tests is backpressure; their
//! former sleeps were holding nothing and cost the suite 20 seconds of pure
//! teardown.
//!
//! These tests keep a real socket while the rest of the suite moved to a
//! scripted connector, because kernel buffering is their subject rather than
//! their nuisance: an in-memory stream has no fixed-size buffer to fill, so
//! there is no backpressure to observe and no write that can genuinely block.
//! Scripting them would delete what they test.
//!
//! Waiting is still stated rather than guessed. [`Milestone`] carries the
//! other direction from [`StopSignal`] — the server reporting that a request
//! arrived, so a test proceeds on that fact instead of on a sleep long enough
//! to assume it. Where a loop polls, it polls a condition and fails on a
//! deadline, which is a bounded wait for an event and not a fixed delay.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

mod support;
use support::milestone::{Milestone, Reached};
use support::park::StopSignal;

use xibalba_client::DEFAULT_MAX_OUTSTANDING;
use xibalba_client::PlainConnector;
use xibalba_client::async_client::{AsyncClient, Chunk};
use xibalba_client::client::Config;
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::method::Method;

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

/// A server that answers one request with an unterminated chunked body and
/// keeps writing until the client goes away.
struct FloodServer {
    port: u16,
    handle: thread::JoinHandle<usize>,
    written: Arc<AtomicUsize>,
    stop: Option<StopSignal>,
}

impl FloodServer {
    /// `chunks` bounds how many body chunks are offered; the ring is 32
    /// slots, so a larger number guarantees the reader blocks for capacity.
    fn spawn(chunks: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        let written = Arc::new(AtomicUsize::new(0));
        let server_written = Arc::clone(&written);
        let (stop, park) = StopSignal::new();
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
            park.wait();
            sent
        });
        Self {
            port,
            handle,
            written,
            stop: Some(stop),
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

    fn shutdown(mut self) {
        drop(self.stop.take());
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
            let deadline = Instant::now() + Duration::from_secs(5);
            while client.outstanding() != 0 && Instant::now() < deadline {
                thread::yield_now();
            }
            assert_eq!(
                client.outstanding(),
                0,
                "cancellation must release admission"
            );
            client
        },
    );

    drop(handle);
    drop(client);
    server.shutdown();
}

#[test]
fn repeated_cancellation_does_not_saturate_the_control_ring() {
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

    let canceller = thread::spawn(move || {
        for _ in 0..256 {
            handle
                .cancel()
                .expect("repeated cancellation reaches the reader");
        }
    });

    Watchdog::run(
        Duration::from_secs(10),
        "repeated cancellation with a full response ring",
        move || {
            canceller
                .join()
                .expect("the cancellation producer must not remain blocked");
        },
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while client.outstanding() != 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(
        client.outstanding(),
        0,
        "repeated cancellation must release the request admission slot"
    );

    drop(client);
    server.shutdown();
}

#[test]
fn sustained_submission_against_a_slow_response_stays_bounded() {
    // The reader drains the control ring into its pending queue whenever it
    // polls for a cancel, so the eight-slot ring bounds nothing by itself.
    // A caller that keeps submitting while one response crawls must be told
    // to stop rather than growing the queue without limit.
    let server = FloodServer::spawn(512);
    let client = server.client();

    let mut first = client
        .submit(Method::Get, b"/slow".to_vec(), None, None, vec![])
        .expect("the first request is admitted");
    assert!(
        matches!(first.next_block(), Some(Chunk::Head { status: 200, .. })),
        "the first response must start before the flood of submissions"
    );

    let mut refusals = 0;
    let mut admitted = 0;
    let mut handles = Vec::new();
    for _ in 0..512 {
        match client.submit(Method::Get, b"/queued".to_vec(), None, None, vec![]) {
            Ok(handle) => {
                admitted += 1;
                handles.push(handle);
            }
            Err(Error::Connection(ConnectionError::TooManyRequests)) => refusals += 1,
            Err(other) => panic!("unexpected submit failure: {other}"),
        }
    }

    assert!(
        refusals > 0,
        "submitting 512 requests behind a stalled response must hit the bound"
    );
    assert!(
        admitted < 512,
        "admitted {admitted} of 512; the queue grew without limit"
    );
    assert!(
        client.outstanding() <= DEFAULT_MAX_OUTSTANDING,
        "outstanding {} exceeded the configured bound {DEFAULT_MAX_OUTSTANDING}",
        client.outstanding()
    );

    // Cancellation must keep working while at the admission ceiling: it
    // travels on the control ring, which admission does not gate.
    Watchdog::run(
        Duration::from_secs(10),
        "cancel while at the admission ceiling",
        move || {
            client.cancel().expect("cancel reaches the reader");
            drop(handles);
            drop(first);
            drop(client);
        },
    );
    server.shutdown();
}

#[test]
fn finished_requests_release_their_admission_slots() {
    // A bound that never releases is just a smaller leak.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    let server = thread::spawn(move || {
        for _ in 0..4 {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
            let _ = stream.flush();
        }
    });

    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        ..Config::default()
    };
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config).expect("connect");

    for _ in 0..4 {
        let mut handle = client
            .submit(Method::Get, b"/".to_vec(), None, None, vec![])
            .expect("each request is admitted after the previous one finishes");
        while let Some(chunk) = handle.next_block() {
            if matches!(chunk, Chunk::Eof | Chunk::Error(_) | Chunk::Aborted) {
                break;
            }
        }
        drop(handle);
    }

    // The reader releases a permit when it finishes with the request, which
    // races the assertion; poll briefly rather than sleeping a fixed span.
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.outstanding() > 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        client.outstanding(),
        0,
        "completed requests must release their slots"
    );

    drop(client);
    let _ = server.join();
}

/// Accepts a connection, reads the request, and then answers with nothing.
/// The reader is left parked on a head that never arrives.
struct SilentHeadServer {
    port: u16,
    handle: thread::JoinHandle<()>,
    stop: Option<StopSignal>,
    got_request: Reached,
}

impl SilentHeadServer {
    fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        let (stop, park) = StopSignal::new();
        let (milestone, got_request) = Milestone::new();
        let handle = thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            // The request is on the wire, so the reader has nothing left to do
            // but wait for a head. Saying so lets the test proceed on that
            // fact rather than on a sleep long enough to assume it.
            milestone.reached();
            // Never answer. Hold the socket open so the client waits on the
            // head rather than seeing EOF.
            park.wait();
        });
        Self {
            port,
            handle,
            stop: Some(stop),
            got_request,
        }
    }

    /// Block until the client's request has actually arrived.
    fn await_request(&self) {
        self.got_request.wait("the client's request");
    }

    /// `head_silence` is deliberately far longer than the test's patience:
    /// if cancellation does not reach the head read, the only thing that can
    /// end the wait is this budget, and the watchdog fires first.
    fn client(&self) -> AsyncClient {
        let url = format!("http://127.0.0.1:{}/", self.port);
        let config = Config {
            read_timeout: Some(Duration::from_millis(50)),
            head_silence: Duration::from_mins(5),
            stream_silence: Duration::from_mins(5),
            ..Config::default()
        };
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config)
            .expect("connect to the local silent server")
    }

    fn shutdown(mut self) {
        drop(self.stop.take());
        let _ = self.handle.join();
    }
}

#[test]
fn cancel_interrupts_a_silent_response_head() {
    // Cancellation used to wrap body reads only, so a head that never arrived
    // left the request pinned for the whole head-silence budget. The cancel
    // must be observed on the next read tick instead.
    let server = SilentHeadServer::spawn();
    let client = server.client();

    let mut handle = client
        .submit(Method::Get, b"/silent".to_vec(), None, None, vec![])
        .expect("submit succeeds");

    // Wait until the request is actually on the wire, so the cancel lands
    // during the head read rather than while it is still queued. The server
    // confirms receipt, which is the event itself rather than a delay chosen
    // to be longer than it.
    server.await_request();
    assert!(handle.has_started(), "the request never reached the wire");

    handle.cancel().expect("cancel reaches the reader");

    let chunk = Watchdog::run(
        Duration::from_secs(10),
        "cancel during a silent response head",
        move || {
            let chunk = handle.next_block();
            (chunk, handle)
        },
    );
    assert!(
        matches!(chunk.0, Some(Chunk::Aborted)),
        "expected Aborted, got {:?}",
        chunk.0
    );

    drop(client);
    server.shutdown();
}

#[test]
fn drop_interrupts_a_silent_response_head() {
    // The same gap reached through shutdown: dropping the client while the
    // reader waits on a head must not block for the silence budget.
    let server = SilentHeadServer::spawn();
    let client = server.client();

    let handle = client
        .submit(Method::Get, b"/silent".to_vec(), None, None, vec![])
        .expect("submit succeeds");
    server.await_request();
    assert!(handle.has_started(), "the request never reached the wire");

    let elapsed = Watchdog::run(
        Duration::from_secs(10),
        "drop during a silent response head",
        move || {
            let started = Instant::now();
            drop(client);
            started.elapsed()
        },
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "drop took {elapsed:?}; shutdown did not reach the head read"
    );

    drop(handle);
    server.shutdown();
}

/// The per-write ceiling for the blocked-upload test, and so the granularity
/// at which its drop can be observed.
const WRITE_TIMEOUT: Duration = Duration::from_millis(50);

#[test]
fn drop_interrupts_a_blocked_request_upload() {
    // A peer that accepts the connection and then stops reading fills the
    // socket buffer, blocking the request write. Cancellation covered reads
    // only, so the writing reader had no path back to the control channel.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    let (stop, park) = StopSignal::new();
    let (accepted, connection_made) = Milestone::new();
    let server = thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        accepted.reached();
        // Never read. Hold the connection so the client's write blocks once
        // the kernel buffers fill.
        park.wait();
        drop(stream);
    });

    let url = format!("http://127.0.0.1:{port}/");
    let config = Config {
        read_timeout: Some(Duration::from_millis(50)),
        // The write-side granularity at which shutdown becomes observable.
        // Without a bound here the write blocks inside one syscall and no
        // cancellation check is ever reached.
        write_timeout: Some(WRITE_TIMEOUT),
        head_silence: Duration::from_mins(5),
        stream_silence: Duration::from_mins(5),
        ..Config::default()
    };
    let client: AsyncClient =
        AsyncClient::connect::<PlainConnector>(url.as_bytes(), (), config).expect("connect");

    // Large enough that it cannot fit in the socket buffers, so the write
    // must block partway rather than completing into the kernel.
    let body = vec![b'x'; 8 * 1024 * 1024];
    let handle = client
        .submit(Method::Post, b"/upload".to_vec(), None, Some(body), vec![])
        .expect("submit succeeds");

    connection_made.wait("the client's connection");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !handle.has_started() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(handle.has_started(), "the upload never began");

    // The write blocking is what this test needs, and neither side can
    // observe it directly: the server never reads, so it sees nothing, and
    // the client is inside the blocked call. What makes the wait unnecessary
    // is that blocking is not a race — the body is far larger than any socket
    // buffer, so once the upload has started the write must block, and drop
    // has to cope whether it has happened yet or not.

    let elapsed = Watchdog::run(
        Duration::from_secs(15),
        "drop during a blocked request upload",
        move || {
            let started = Instant::now();
            drop(client);
            started.elapsed()
        },
    );
    // A few write ticks, not ten seconds. The write budget retries a timeout
    // tick instead of failing on it, so the loose bound this once carried
    // could not tell a prompt shutdown from one that waited out the whole
    // five-minute head-silence budget. The interrupt is consulted before
    // every write, and the retry is paced, so a drop costs about one tick.
    assert!(
        elapsed < WRITE_TIMEOUT * 8,
        "drop took {elapsed:?} while the request write was blocked; \
         a cancel must be seen within about one write tick ({WRITE_TIMEOUT:?})"
    );

    drop(handle);
    drop(stop);
    let _ = server.join();
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
