//! What the client hands to [`Connector::connect`], and what it does with a
//! connector that reports the deadline passed.
//!
//! The dialler's own bound is unit-tested in `dial.rs`. What cannot be checked
//! there is the wiring: that the deadline reaching a connector comes from
//! `Config::connect_timeout`, that a reconnect gets a whole one rather than
//! the remains of the first, and that an overrun is surfaced rather than
//! retried.

use std::io::{Read, Write};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use xibalba_client::client::{Client, Config};
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::url::Url;
use xibalba_client::{Deadline, TimeLeft};

/// What one `connect` call was told, and what it did about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observed {
    deadline: Deadline,
    time_left: TimeLeft,
}

/// The deadlines successive connects received.
///
/// `Connector::connect` is a static method with no receiver, so per-test state
/// has nowhere else to live than a process-wide slot. Tests in one binary run
/// in parallel and would then overwrite each other's observations, so a test
/// takes the log for its whole duration through [`DeadlineLog::claim`] rather
/// than clearing a shared one and hoping to be alone.
struct DeadlineLog;

/// Exclusive use of the log. Held for the length of a test.
struct ClaimedLog {
    _exclusive: MutexGuard<'static, ()>,
}

impl DeadlineLog {
    fn slot() -> &'static Mutex<Vec<Observed>> {
        static SLOT: OnceLock<Mutex<Vec<Observed>>> = OnceLock::new();
        SLOT.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn turnstile() -> &'static Mutex<()> {
        static TURNSTILE: OnceLock<Mutex<()>> = OnceLock::new();
        TURNSTILE.get_or_init(|| Mutex::new(()))
    }

    /// Take the log for this test, emptied. A failing test poisons the lock;
    /// recovering the guard keeps that one failure from cascading into every
    /// other test in the binary.
    fn claim() -> ClaimedLog {
        let exclusive = Self::turnstile()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Self::slot()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        ClaimedLog {
            _exclusive: exclusive,
        }
    }

    fn record(deadline: Deadline) {
        Self::slot()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Observed {
                deadline,
                time_left: deadline.time_left(),
            });
    }
}

impl ClaimedLog {
    /// Takes `&self` because holding a `ClaimedLog` is what makes the reading
    /// meaningful: without it another test could be writing to the same log.
    #[expect(
        clippy::unused_self,
        reason = "the receiver is the proof of exclusive access, not data"
    )]
    fn observed(&self) -> Vec<Observed> {
        DeadlineLog::slot()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn only(&self) -> Observed {
        let observed = self.observed();
        assert_eq!(observed.len(), 1, "expected exactly one connect");
        observed[0]
    }
}

/// Serves one canned response and records the deadline it was given.
struct RecordingConnector;

struct CannedStream {
    response: &'static [u8],
    read_pos: usize,
}

impl Read for CannedStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.response[self.read_pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.read_pos += n;
        Ok(n)
    }
}

impl Write for CannedStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SetReadTimeout for CannedStream {
    fn set_read_timeout(&self, _dur: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
}

impl Connector for RecordingConnector {
    type Stream = CannedStream;
    type TlsConfig = ();

    fn connect(_url: &Url<'_>, (): &(), deadline: Deadline) -> Result<Self::Stream, Error> {
        DeadlineLog::record(deadline);
        Ok(CannedStream {
            // `Connection: close` makes the client discard the connection, so
            // the next request must reconnect and log a second deadline.
            response: b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi",
            read_pos: 0,
        })
    }
}

impl RecordingConnector {
    fn client(connect_timeout: Option<Duration>) -> (ClaimedLog, Client<Self>) {
        Self::client_with(connect_timeout, None)
    }

    fn client_with(
        connect_timeout: Option<Duration>,
        request_deadline: Option<Duration>,
    ) -> (ClaimedLog, Client<Self>) {
        let log = DeadlineLog::claim();
        let config = Config {
            connect_timeout,
            request_deadline,
            ..Config::default()
        };
        let client =
            Client::<Self>::connect(b"http://recorded.test/", (), config).expect("connect");
        (log, client)
    }
}

/// A connector whose peer never completes the connection, and which honours
/// its deadline rather than waiting for the OS.
///
/// The wait is a `recv_timeout` on a channel nothing ever sends to, bounded by
/// the deadline. That is a connector implementing the contract — the same
/// shape as `TcpStream::connect_timeout` blocking in the kernel — not a test
/// standing in for one.
struct StallingConnector;

impl Connector for StallingConnector {
    type Stream = CannedStream;
    type TlsConfig = ();

    fn connect(_url: &Url<'_>, (): &(), deadline: Deadline) -> Result<Self::Stream, Error> {
        DeadlineLog::record(deadline);
        let (_never_sends, waiting) = std::sync::mpsc::channel::<()>();
        match deadline.time_left() {
            TimeLeft::Unbounded => panic!("this test must always pass a bounded deadline"),
            TimeLeft::Remaining(left) => {
                let _ = waiting.recv_timeout(left);
            }
            TimeLeft::Expired => {}
        }
        deadline.check()?;
        Err(Error::Connection(ConnectionError::ConnectDeadlineExceeded))
    }
}

#[test]
fn the_configured_connect_timeout_is_what_reaches_the_connector() {
    let (log, _client) = RecordingConnector::client(Some(Duration::from_secs(7)));

    match log.only().time_left {
        TimeLeft::Remaining(left) => assert!(
            left <= Duration::from_secs(7) && left > Duration::from_secs(6),
            "the connector should see roughly the configured budget, saw {left:?}"
        ),
        other => panic!("expected a bounded deadline, got {other:?}"),
    }
}

/// `None` is a deliberate choice to accept OS-level bounds, and must arrive as
/// such: a connector cannot distinguish "no bound wanted" from "a bound was
/// meant but got lost" unless the two are different values.
#[test]
fn no_configured_timeout_reaches_the_connector_as_unbounded() {
    let (log, _client) = RecordingConnector::client(None);

    assert_eq!(
        log.only().time_left,
        TimeLeft::Unbounded,
        "an unset connect timeout must arrive as an unbounded deadline"
    );
}

/// A deadline built once and stored would leave a reconnect with whatever the
/// first connect and every request since had not used — eventually nothing, so
/// a long-lived client could never reconnect at all. Each attempt gets its own.
#[test]
fn a_reconnect_gets_a_fresh_deadline_rather_than_the_remains_of_the_first() {
    let (log, mut client) = RecordingConnector::client(Some(Duration::from_secs(7)));

    client.get(b"/first").expect("first request");
    client.get(b"/second").expect("second request reconnects");

    let observed = log.observed();
    assert_eq!(observed.len(), 2, "the second request must reconnect");
    assert_ne!(
        observed[0].deadline, observed[1].deadline,
        "the reconnect reused the original deadline instead of starting a new one"
    );
    match observed[1].time_left {
        TimeLeft::Remaining(left) => assert!(
            left > Duration::from_secs(6),
            "the reconnect should get the whole budget, got {left:?}"
        ),
        other => panic!("expected a bounded deadline, got {other:?}"),
    }
}

/// A reconnect while a total request deadline is running must not get a
/// fresh `connect_timeout` regardless of how little of the total remains: a
/// caller who set a 5s total do not expect one hop's reconnect to spend a
/// full fresh `connect_timeout` on top of what the total has already burned.
#[test]
fn a_reconnect_is_bounded_by_the_remaining_total_when_it_is_the_shorter_bound() {
    let (log, mut client) = RecordingConnector::client_with(
        Some(Duration::from_secs(30)),
        Some(Duration::from_millis(200)),
    );

    client.get(b"/first").expect("first request");
    client.get(b"/second").expect("second request reconnects");

    let observed = log.observed();
    assert_eq!(observed.len(), 2, "the second request must reconnect");
    match observed[1].time_left {
        TimeLeft::Remaining(left) => assert!(
            left < Duration::from_secs(1),
            "the reconnect must be bounded by what the total has left, not \
             a fresh connect_timeout; saw {left:?}"
        ),
        other => panic!("expected a bounded deadline, got {other:?}"),
    }
}

/// The converse: a short `connect_timeout` must still cap a reconnect even
/// when the total request deadline has plenty left, or a caller relying on
/// `connect_timeout` to bound one connect attempt would find a reconnect
/// silently exempt from it.
#[test]
fn a_reconnect_is_bounded_by_connect_timeout_when_it_is_the_shorter_bound() {
    let (log, mut client) = RecordingConnector::client_with(
        Some(Duration::from_millis(200)),
        Some(Duration::from_secs(30)),
    );

    client.get(b"/first").expect("first request");
    client.get(b"/second").expect("second request reconnects");

    let observed = log.observed();
    assert_eq!(observed.len(), 2, "the second request must reconnect");
    match observed[1].time_left {
        TimeLeft::Remaining(left) => assert!(
            left < Duration::from_secs(1),
            "the reconnect must still respect connect_timeout even with a \
             generous total left; saw {left:?}"
        ),
        other => panic!("expected a bounded deadline, got {other:?}"),
    }
}

/// The point of the whole contract: a peer that never completes the handshake
/// gives the caller an error at the configured bound, instead of holding the
/// thread for the OS SYN timeout.
#[test]
fn a_connector_that_runs_out_of_time_reports_it_instead_of_waiting_on_the_os() {
    let _log = DeadlineLog::claim();
    let config = Config {
        connect_timeout: Some(Duration::from_millis(150)),
        ..Config::default()
    };

    let started = std::time::Instant::now();
    let outcome = Client::<StallingConnector>::connect(b"http://stalls.test/", (), config);
    let elapsed = started.elapsed();
    let Err(error) = outcome else {
        panic!("a peer that never answers must not connect");
    };

    assert_eq!(
        error,
        Error::Connection(ConnectionError::ConnectDeadlineExceeded)
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the connect must end at its deadline, not the OS timeout; took {elapsed:?}"
    );
}

/// A zero connect timeout would expire before the first address was tried, so
/// every request would fail without a syscall. Rejected with the other zero
/// durations rather than silently making the client useless.
#[test]
fn a_zero_connect_timeout_is_rejected_at_construction() {
    let config = Config {
        connect_timeout: Some(Duration::ZERO),
        ..Config::default()
    };
    let Err(error) = Client::<RecordingConnector>::connect(b"http://recorded.test/", (), config)
    else {
        panic!("a zero connect timeout can never connect");
    };
    assert_eq!(error, Error::Connection(ConnectionError::ZeroDuration));
}
