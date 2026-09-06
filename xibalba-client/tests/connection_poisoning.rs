//! Transport failures must never leave a connection eligible for reuse.
//!
//! A request that put bytes on the wire and then failed leaves the peer's
//! parser mid-request. Reusing that socket appends the next request to the
//! truncated one. These tests drive failures through a scripted connector so
//! the failure point is exact, and assert on how many connections were opened
//! rather than on the returned body alone.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::time::Duration;

use xibalba_client::client::Client;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::Error;
use xibalba_client::proto::url::Url;

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";

/// How one scripted connection behaves: what it serves, how many request
/// bytes it accepts, and how it fails afterwards.
#[derive(Clone)]
struct StreamPlan {
    response: Vec<u8>,
    write_budget: usize,
    write_error: Option<ErrorKind>,
    flush_error: Option<ErrorKind>,
}

impl StreamPlan {
    fn healthy() -> Self {
        Self {
            response: RESPONSE.to_vec(),
            write_budget: usize::MAX,
            write_error: None,
            flush_error: None,
        }
    }

    /// Accept `budget` request bytes, then fail. `TimedOut` is deliberate:
    /// it is outside the stale-keep-alive set, so no automatic retry runs
    /// and the test observes poisoning alone.
    fn fails_write_after(budget: usize) -> Self {
        Self {
            write_budget: budget,
            write_error: Some(ErrorKind::TimedOut),
            ..Self::healthy()
        }
    }

    fn fails_flush() -> Self {
        Self {
            flush_error: Some(ErrorKind::TimedOut),
            ..Self::healthy()
        }
    }
}

thread_local! {
    static PLANS: RefCell<VecDeque<StreamPlan>> = const { RefCell::new(VecDeque::new()) };
    static CONNECTS: Cell<usize> = const { Cell::new(0) };
}

/// Test-local control over the scripted connector.
struct Script;

impl Script {
    fn load(plans: &[StreamPlan]) {
        PLANS.with(|queued| {
            let mut queued = queued.borrow_mut();
            queued.clear();
            queued.extend(plans.iter().cloned());
        });
        CONNECTS.with(|count| count.set(0));
    }

    fn connects() -> usize {
        CONNECTS.with(Cell::get)
    }

    fn next_plan() -> StreamPlan {
        CONNECTS.with(|count| count.set(count.get() + 1));
        PLANS.with(|queued| {
            queued
                .borrow_mut()
                .pop_front()
                .expect("connector opened more connections than the script allows")
        })
    }
}

struct ScriptedStream {
    plan: StreamPlan,
    written: usize,
    read_pos: usize,
}

impl Read for ScriptedStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.plan.response[self.read_pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.read_pos += n;
        Ok(n)
    }
}

impl Write for ScriptedStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let room = self.plan.write_budget.saturating_sub(self.written);
        if room == 0
            && let Some(kind) = self.plan.write_error
        {
            return Err(std::io::Error::new(kind, "scripted write failure"));
        }
        let n = buf.len().min(room);
        self.written += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.plan.flush_error.map_or(Ok(()), |kind| {
            Err(std::io::Error::new(kind, "scripted flush failure"))
        })
    }
}

impl SetReadTimeout for ScriptedStream {
    fn set_read_timeout(&self, _dur: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
}

struct ScriptedConnector;

impl Connector for ScriptedConnector {
    type Stream = ScriptedStream;
    type TlsConfig = ();

    fn connect(_url: &Url<'_>, (): &()) -> Result<Self::Stream, Error> {
        Ok(ScriptedStream {
            plan: Script::next_plan(),
            written: 0,
            read_pos: 0,
        })
    }
}

impl ScriptedConnector {
    fn client() -> Client<Self> {
        Client::<Self>::connect_default(b"http://scripted.test/", ())
            .expect("scripted connect always succeeds")
    }
}

#[test]
fn partial_request_write_forces_reconnect_before_next_request() {
    Script::load(&[StreamPlan::fails_write_after(10), StreamPlan::healthy()]);
    let mut client = ScriptedConnector::client();

    client
        .get(b"/first")
        .expect_err("a failed request write must surface as an error");
    assert_eq!(
        Script::connects(),
        1,
        "no reconnect should have happened yet"
    );

    let response = client
        .get(b"/second")
        .expect("the next request must succeed on a fresh connection");

    assert_eq!(
        Script::connects(),
        2,
        "the partially-written connection must not be reused"
    );
    assert_eq!(response.text().unwrap(), "hi");
}

#[test]
fn failed_flush_forces_reconnect_before_next_request() {
    Script::load(&[StreamPlan::fails_flush(), StreamPlan::healthy()]);
    let mut client = ScriptedConnector::client();

    client
        .get(b"/first")
        .expect_err("a failed flush must surface as an error");

    client
        .get(b"/second")
        .expect("the next request must succeed on a fresh connection");
    assert_eq!(
        Script::connects(),
        2,
        "a connection whose flush failed must not be reused"
    );
}

#[test]
fn partial_body_write_forces_reconnect_before_next_request() {
    // A body past the inline threshold is written separately, so the failure
    // lands after a complete head has already reached the peer.
    let body = vec![b'x'; 128 * 1024];
    Script::load(&[StreamPlan::fails_write_after(200), StreamPlan::healthy()]);
    let mut client = ScriptedConnector::client();

    client
        .post(b"/upload", &body)
        .expect_err("a failed body write must surface as an error");

    client
        .get(b"/second")
        .expect("the next request must succeed on a fresh connection");
    assert_eq!(
        Script::connects(),
        2,
        "a connection with a half-written body must not be reused"
    );
}

#[test]
fn rejected_request_keeps_the_connection_usable() {
    // Duplicate-header rejection happens before any byte is written, so it
    // must not cost a reconnect: poisoning has to be precise, not blanket.
    Script::load(&[StreamPlan::healthy()]);
    let mut client = ScriptedConnector::client();

    let duplicate = client
        .build(xibalba_client::proto::method::Method::Get, b"/first")
        .header(b"X-Dup", b"a")
        .header(b"X-Dup", b"b");
    client
        .send(duplicate)
        .expect_err("duplicate headers must be rejected");

    client
        .get(b"/second")
        .expect("a request rejected before the wire must leave the connection usable");
    assert_eq!(
        Script::connects(),
        1,
        "a preflight rejection must not force a reconnect"
    );
}
