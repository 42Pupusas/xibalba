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

use xibalba_client::Deadline;
use xibalba_client::client::{Client, Config};
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::Error;
use xibalba_client::proto::url::Url;

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";

/// How one scripted connection behaves: what it serves, how many request
/// bytes it accepts, and how it fails afterwards.
#[derive(Clone)]
struct StreamPlan {
    /// One entry per request this connection will answer, in order. A real
    /// server writes a response only after reading the request, so serving
    /// them upfront would fake pipelining the client never performs.
    responses: VecDeque<Vec<u8>>,
    write_budget: usize,
    write_error: Option<ErrorKind>,
    flush_error: Option<ErrorKind>,
}

impl StreamPlan {
    fn healthy() -> Self {
        Self {
            responses: VecDeque::from(vec![RESPONSE.to_vec()]),
            write_budget: usize::MAX,
            write_error: None,
            flush_error: None,
        }
    }

    /// Answer each successive request on this connection with one response.
    fn serving_each(responses: &[&[u8]]) -> Self {
        Self {
            responses: responses.iter().map(|r| r.to_vec()).collect(),
            ..Self::healthy()
        }
    }

    /// The kind these plans fail with, chosen to sit outside two sets at
    /// once. It must not be in the stale-keep-alive set (`BrokenPipe`,
    /// `ConnectionReset`, `UnexpectedEof`, `ConnectionAborted`) or the client
    /// reconnects and retries, and the test would count that reconnect
    /// instead of the poisoning it means to observe. It must also not be a
    /// timeout tick (`WouldBlock`, `TimedOut`), which the write budget
    /// absorbs and retries until the budget expires — these plans fail every
    /// write, so a tick would make each test wait out the whole head-silence
    /// budget before failing.
    const FAILURE: ErrorKind = ErrorKind::NotConnected;

    /// Accept `budget` request bytes, then fail permanently.
    fn fails_write_after(budget: usize) -> Self {
        Self {
            write_budget: budget,
            write_error: Some(Self::FAILURE),
            ..Self::healthy()
        }
    }

    fn fails_flush() -> Self {
        Self {
            flush_error: Some(Self::FAILURE),
            ..Self::healthy()
        }
    }

    fn serving(response: &[u8]) -> Self {
        Self::serving_each(&[response])
    }

    /// Accept a whole request and answer with nothing. The client reads a
    /// clean EOF, the signature of a keep-alive connection the server had
    /// already closed.
    fn silently_closed() -> Self {
        Self {
            responses: VecDeque::new(),
            ..Self::healthy()
        }
    }
}

thread_local! {
    static PLANS: RefCell<VecDeque<StreamPlan>> = const { RefCell::new(VecDeque::new()) };
    static CONNECTS: Cell<usize> = const { Cell::new(0) };
    /// Bytes accepted by each connection, in the order the connections were
    /// opened. Asserting on this is what distinguishes a request that really
    /// reached the wire from one answered by leftover bytes.
    static SENT: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
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
        SENT.with(|sent| sent.borrow_mut().clear());
    }

    fn connects() -> usize {
        CONNECTS.with(Cell::get)
    }

    /// Everything the client wrote to the connection at `index`.
    fn sent_on(index: usize) -> Vec<u8> {
        SENT.with(|sent| sent.borrow().get(index).cloned().unwrap_or_default())
    }

    fn next_plan() -> (StreamPlan, usize) {
        let index = CONNECTS.with(|count| {
            let index = count.get();
            count.set(index + 1);
            index
        });
        SENT.with(|sent| sent.borrow_mut().push(Vec::new()));
        let plan = PLANS.with(|queued| {
            queued
                .borrow_mut()
                .pop_front()
                .expect("connector opened more connections than the script allows")
        });
        (plan, index)
    }
}

struct ScriptedStream {
    plan: StreamPlan,
    written: usize,
    read_pos: usize,
    index: usize,
    /// Response bytes released so far: one response per request seen.
    available: Vec<u8>,
    /// Request bytes not yet matched to a `\r\n\r\n` request terminator.
    request_acc: Vec<u8>,
}

impl ScriptedStream {
    /// Release the next scripted response once a full request head has been
    /// written, mirroring a server that reads before it answers.
    fn note_request_bytes(&mut self, bytes: &[u8]) {
        self.request_acc.extend_from_slice(bytes);
        while let Some(end) = self.request_acc.windows(4).position(|w| w == b"\r\n\r\n") {
            self.request_acc.drain(..end + 4);
            if let Some(response) = self.plan.responses.pop_front() {
                self.available.extend_from_slice(&response);
            }
        }
    }
}

impl Read for ScriptedStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.available[self.read_pos..];
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
        SENT.with(|sent| sent.borrow_mut()[self.index].extend_from_slice(&buf[..n]));
        self.note_request_bytes(&buf[..n]);
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

    fn connect(_url: &Url<'_>, (): &(), _deadline: Deadline) -> Result<Self::Stream, Error> {
        let (plan, index) = Script::next_plan();
        Ok(ScriptedStream {
            plan,
            written: 0,
            read_pos: 0,
            index,
            available: Vec::new(),
            request_acc: Vec::new(),
        })
    }
}

impl ScriptedConnector {
    fn client() -> Client<Self> {
        Self::client_with(Config::default())
    }

    fn client_with(config: Config) -> Client<Self> {
        Client::<Self>::connect(b"http://scripted.test/", (), config)
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
fn same_origin_redirect_does_not_reuse_a_close_delimited_connection() {
    // The redirect body is close-delimited, so the connection cannot carry
    // the next hop: the peer signalled the end of the response by ending the
    // stream. `ensure_clean` runs once before the redirect loop, so without a
    // per-hop check the second hop is written to this dead connection.
    const REDIRECT: &[u8] = b"HTTP/1.1 302 Found\r\nLocation: /target\r\n\r\nignored";

    Script::load(&[StreamPlan::serving(REDIRECT), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    let response = client
        .get(b"/start")
        .expect("the redirect must be followed on a fresh connection");

    assert_eq!(
        Script::connects(),
        2,
        "the redirected hop must open a new connection"
    );
    let second = Script::sent_on(1);
    assert!(
        second.starts_with(b"GET /target "),
        "the second hop must be written to the new connection, got: {}",
        String::from_utf8_lossy(&second)
    );
    assert_eq!(response.text().unwrap(), "hi");

    // The hop must not have been written to the spent connection first. If it
    // was, the request only survived because the retry path resent it, which
    // means the peer may have received it twice.
    let first = Script::sent_on(0);
    assert!(
        !first.windows(12).any(|w| w == b"GET /target "),
        "the redirected hop was written to the spent connection: {}",
        String::from_utf8_lossy(&first)
    );
}

#[test]
fn redirect_hop_after_excess_body_bytes_uses_a_fresh_connection() {
    // The peer sends more body than Content-Length declares. Those extra
    // bytes stay on the socket, so the next hop must not read them as its
    // own response head.
    const REDIRECT: &[u8] =
        b"HTTP/1.1 302 Found\r\nLocation: /target\r\nContent-Length: 2\r\n\r\nokEXTRA";

    Script::load(&[StreamPlan::serving(REDIRECT), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    let response = client
        .get(b"/start")
        .expect("the redirect must be followed on a fresh connection");

    assert_eq!(
        Script::connects(),
        2,
        "leftover body bytes must force a reconnect before the next hop"
    );
    assert_eq!(response.text().unwrap(), "hi");
}

#[test]
fn redirect_hop_never_answers_from_a_leftover_response() {
    // The peer pipelines a second, complete response behind the redirect.
    // Those leftover bytes parse as a perfectly valid head, so reusing the
    // connection returns the wrong response body to the redirected hop
    // instead of failing — the silent desync a stale-connection retry
    // cannot catch, because nothing looks broken.
    const REDIRECT_THEN_LEFTOVER: &[u8] = b"HTTP/1.1 302 Found\r\nLocation: /target\r\nContent-Length: 0\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nLEFTOVR";

    Script::load(&[
        StreamPlan::serving(REDIRECT_THEN_LEFTOVER),
        StreamPlan::serving(RESPONSE),
    ]);
    let mut client = ScriptedConnector::client();

    let response = client
        .get(b"/start")
        .expect("the redirect must be followed on a fresh connection");

    assert_eq!(
        Script::connects(),
        2,
        "the hop must not be served from bytes left over by the previous hop"
    );
    let second = Script::sent_on(1);
    assert!(
        second.starts_with(b"GET /target "),
        "the second hop must reach the wire, got: {}",
        String::from_utf8_lossy(&second)
    );
    assert_eq!(
        response.text().unwrap(),
        "hi",
        "the response must come from the redirected request, not the leftover bytes"
    );
}

#[test]
fn connection_close_response_is_not_reused() {
    const CLOSING: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi";

    Script::load(&[StreamPlan::serving(CLOSING), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    client.get(b"/first").expect("the first request succeeds");
    client
        .get(b"/second")
        .expect("the next request must open a new connection");

    assert_eq!(
        Script::connects(),
        2,
        "a peer that announced Connection: close must not be reused"
    );
    let first = Script::sent_on(0);
    assert!(
        !first.windows(12).any(|w| w == b"GET /second "),
        "the second request must not be written to the closing connection"
    );
}

#[test]
fn http10_response_without_keep_alive_is_not_reused() {
    const HTTP10: &[u8] = b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nhi";

    Script::load(&[StreamPlan::serving(HTTP10), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    client.get(b"/first").expect("the first request succeeds");
    client
        .get(b"/second")
        .expect("the next request must open a new connection");

    assert_eq!(
        Script::connects(),
        2,
        "HTTP/1.0 defaults to closing unless keep-alive was announced"
    );
}

#[test]
fn http10_response_with_keep_alive_is_reused() {
    const HTTP10_KEEP: &[u8] =
        b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nhi";

    Script::load(&[StreamPlan::serving_each(&[HTTP10_KEEP, HTTP10_KEEP])]);
    let mut client = ScriptedConnector::client();

    client.get(b"/first").expect("the first request succeeds");
    client
        .get(b"/second")
        .expect("an announced keep-alive connection must be reused");

    assert_eq!(
        Script::connects(),
        1,
        "HTTP/1.0 with keep-alive must not force a reconnect"
    );
}

#[test]
fn switching_protocols_connection_is_never_reused() {
    const UPGRADE: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n";

    Script::load(&[StreamPlan::serving(UPGRADE), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    let response = client
        .get(b"/upgrade")
        .expect("101 is surfaced to the caller");
    assert_eq!(response.status.as_u16(), 101);

    client
        .get(b"/second")
        .expect("the next request must open a new connection");
    assert_eq!(
        Script::connects(),
        2,
        "an upgraded connection is no longer carrying HTTP and must not be reused"
    );
}

#[test]
fn post_is_not_replayed_after_an_ambiguous_failure() {
    // The server may have applied the POST and lost the response on the way
    // back. Resending would apply it twice, so the error must surface.
    Script::load(&[StreamPlan::silently_closed(), StreamPlan::healthy()]);
    let mut client = ScriptedConnector::client();

    client
        .post(b"/charge", b"amount=100")
        .expect_err("an ambiguous POST failure must surface, not be retried");

    assert_eq!(
        Script::connects(),
        1,
        "a non-idempotent request must not be resent on a fresh connection"
    );
    assert!(
        !Script::sent_on(0).is_empty(),
        "the POST is expected to have reached the wire once"
    );
}

#[test]
fn get_is_still_replayed_after_a_stale_keepalive() {
    // The counterpart: replay-eligible methods must keep their retry, or the
    // fix for POST would be a regression for every idle keep-alive.
    Script::load(&[StreamPlan::silently_closed(), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    let response = client
        .get(b"/read")
        .expect("a stale keep-alive must be retried transparently for GET");

    assert_eq!(response.text().unwrap(), "hi");
    assert_eq!(
        Script::connects(),
        2,
        "the retry must run on a fresh connection"
    );
}

#[test]
fn post_is_replayed_when_the_caller_opts_in() {
    // An endpoint keyed by an idempotency token is safe to resend, but only
    // the caller can know that.
    Script::load(&[StreamPlan::silently_closed(), StreamPlan::serving(RESPONSE)]);
    let mut client = ScriptedConnector::client();

    let request = client
        .build(xibalba_client::proto::method::Method::Post, b"/charge")
        .header(b"Idempotency-Key", b"abc123")
        .body(b"amount=100")
        .allow_replay(true);
    let response = client
        .send(request)
        .expect("an opted-in POST must be retried");

    assert_eq!(response.text().unwrap(), "hi");
    assert_eq!(
        Script::connects(),
        2,
        "the opted-in retry must run on a fresh connection"
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
