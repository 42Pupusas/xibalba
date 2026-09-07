//! Redirect decisions must be made before a connection is opened.
//!
//! The hop budget, the target's scheme, and its origin all determine whether
//! the client should contact the next host at all. Deciding after connecting
//! reaches a host the caller never agreed to reach, which no assertion on the
//! returned response would notice — so these tests record every host the
//! connector was asked for and assert on that list.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::time::Duration;

use xibalba_client::Deadline;
use xibalba_client::client::{Client, Config};
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_client::proto::error::{ConnectionError, Error};
use xibalba_client::proto::url::Url;

thread_local! {
    /// Responses handed out in order. A connection takes the next one each
    /// time it finishes reading a request, so a reused keep-alive connection
    /// serves several — as a real server would.
    static PLANS: RefCell<VecDeque<Vec<u8>>> = const { RefCell::new(VecDeque::new()) };
    /// Every authority the connector was asked to reach, in order. This is
    /// the evidence: a host appearing here was contacted.
    static DIALED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// Request lines written on each connection, so a hop's resolved target
    /// can be checked and not just its host.
    static REQUESTED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// Full request heads, in order, for assertions about which headers a
    /// rewritten request still carries.
    static HEADS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

struct Script;

impl Script {
    fn load(responses: &[&[u8]]) {
        PLANS.with(|p| {
            *p.borrow_mut() = responses.iter().map(|r| r.to_vec()).collect();
        });
        DIALED.with(|d| d.borrow_mut().clear());
        REQUESTED.with(|r| r.borrow_mut().clear());
        HEADS.with(|h| h.borrow_mut().clear());
    }

    fn record_head(head: String) {
        HEADS.with(|h| h.borrow_mut().push(head));
    }

    /// The full head of the `index`-th request written, in order.
    fn head_of_request(index: usize) -> String {
        HEADS.with(|h| h.borrow().get(index).cloned().unwrap_or_default())
    }

    fn record_request(line: String) {
        REQUESTED.with(|r| r.borrow_mut().push(line));
    }

    fn requested() -> Vec<String> {
        REQUESTED.with(|r| r.borrow().clone())
    }

    fn next_response() -> Vec<u8> {
        PLANS.with(|p| p.borrow_mut().pop_front().unwrap_or_default())
    }

    fn record(authority: String) {
        DIALED.with(|d| d.borrow_mut().push(authority));
    }

    fn dialed() -> Vec<String> {
        DIALED.with(|d| d.borrow().clone())
    }
}

struct ScriptedStream {
    response: Vec<u8>,
    read_pos: usize,
    request_acc: Vec<u8>,
}

impl Read for ScriptedStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.response[self.read_pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.read_pos += n;
        Ok(n)
    }
}

impl ScriptedStream {
    /// Release the next scripted response once a full request head has been
    /// written, mirroring a server that reads before it answers.
    ///
    /// A recorded head starts at its request line: any request body still in
    /// the buffer belongs to the previous request and would otherwise be
    /// prepended to the next one.
    fn note_request_bytes(&mut self) {
        while let Some(end) = self.request_acc.windows(4).position(|w| w == b"\r\n\r\n") {
            let raw = String::from_utf8_lossy(&self.request_acc[..end]).into_owned();
            let head = raw
                .split_inclusive('\n')
                .skip_while(|line| !line.contains(" HTTP/1."))
                .collect::<String>();
            Script::record_head(head);
            self.request_acc.drain(..end + 4);
            self.response.extend_from_slice(&Script::next_response());
        }
    }
}

impl Write for ScriptedStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(end) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = String::from_utf8_lossy(&buf[..end]).into_owned();
            if line.contains(" HTTP/1.") {
                Script::record_request(line);
            }
        }
        self.request_acc.extend_from_slice(buf);
        self.note_request_bytes();
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SetReadTimeout for ScriptedStream {
    fn set_read_timeout(&self, _dur: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
}

struct RecordingConnector;

impl Connector for RecordingConnector {
    type Stream = ScriptedStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, (): &(), _deadline: Deadline) -> Result<Self::Stream, Error> {
        let host = std::str::from_utf8(url.host)
            .map_err(|_| Error::Connection(ConnectionError::Other("invalid host".into())))?;
        Script::record(format!("{host}:{}", url.effective_port()));
        Ok(ScriptedStream {
            response: Vec::new(),
            read_pos: 0,
            request_acc: Vec::new(),
        })
    }
}

impl RecordingConnector {
    fn client(max_redirects: u8) -> Client<Self> {
        Self::client_for(b"http://origin.test/", max_redirects)
    }

    fn client_for(url: &[u8], max_redirects: u8) -> Client<Self> {
        let config = Config {
            max_redirects,
            ..Config::default()
        };
        Client::<Self>::connect(url, (), config).expect("the scripted connect always succeeds")
    }
}

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";

fn redirect_to(target: &str) -> Vec<u8> {
    format!("HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\n\r\n").into_bytes()
}

#[test]
fn zero_budget_does_not_contact_the_redirect_target() {
    // With no hops allowed the client must refuse before connecting. Applying
    // the Location first opens a connection to a host the caller never agreed
    // to reach, then discards it — the response is an error either way, so
    // only the dialled list distinguishes the two.
    Script::load(&[&redirect_to("http://elsewhere.test/target"), OK]);
    let mut client = RecordingConnector::client(0);

    let err = client
        .get(b"/start")
        .expect_err("no redirect hops are permitted");
    assert!(
        matches!(err, Error::Connection(ConnectionError::TooManyRedirects)),
        "expected TooManyRedirects, got {err:?}"
    );
    assert_eq!(
        Script::dialed(),
        vec!["origin.test:80".to_owned()],
        "the redirect target must never have been contacted"
    );
}

#[test]
fn an_exhausted_budget_does_not_contact_the_final_target() {
    // Same rule one hop in: the last redirect is refused before its target is
    // reached, not after.
    Script::load(&[
        &redirect_to("http://second.test/a"),
        &redirect_to("http://third.test/b"),
        OK,
    ]);
    let mut client = RecordingConnector::client(1);

    client
        .get(b"/start")
        .expect_err("the second redirect exceeds the budget");
    assert_eq!(
        Script::dialed(),
        vec!["origin.test:80".to_owned(), "second.test:80".to_owned()],
        "third.test is past the budget and must not be contacted"
    );
}

#[test]
fn a_budget_within_range_still_follows() {
    // The guard must not refuse hops the caller allowed.
    Script::load(&[&redirect_to("http://second.test/a"), OK]);
    let mut client = RecordingConnector::client(1);

    let response = client.get(b"/start").expect("one hop is within budget");
    assert_eq!(response.text().unwrap(), "hi");
    assert_eq!(
        Script::dialed(),
        vec!["origin.test:80".to_owned(), "second.test:80".to_owned()]
    );
}

#[test]
fn an_uppercase_scheme_is_treated_as_absolute() {
    // Schemes are case-insensitive. Matching only lowercase treats
    // "HTTP://other.test/x" as a relative path and grafts the whole URL onto
    // the current one, so the client requests a nonsense path from the
    // original host instead of following the redirect.
    Script::load(&[&redirect_to("HTTP://other.test/target"), OK]);
    let mut client = RecordingConnector::client(5);

    let response = client
        .get(b"/start")
        .expect("an uppercase scheme is still absolute");
    assert_eq!(response.text().unwrap(), "hi");
    assert_eq!(
        Script::dialed(),
        vec!["origin.test:80".to_owned(), "other.test:80".to_owned()],
        "the uppercase-scheme target must be contacted as a new origin"
    );
}

#[test]
fn an_https_to_http_redirect_is_refused_before_connecting() {
    // Following a downgrade moves the request, and any credentials it carries,
    // onto an unprotected connection. Detecting it after reconnecting would
    // mean the plaintext connection had already been opened.
    Script::load(&[&redirect_to("http://plain.test/target"), OK]);
    let mut client = RecordingConnector::client_for(b"https://secure.test/", 5);

    let err = client
        .get(b"/start")
        .expect_err("an https-to-http redirect must be refused");
    assert!(
        matches!(err, Error::Connection(ConnectionError::InsecureRedirect)),
        "expected InsecureRedirect, got {err:?}"
    );
    assert_eq!(
        Script::dialed(),
        vec!["secure.test:443".to_owned()],
        "the plaintext target must never have been contacted"
    );
}

#[test]
fn an_http_to_https_redirect_is_allowed() {
    // Upgrades are not downgrades; the guard must not block them.
    Script::load(&[&redirect_to("https://secure.test/target"), OK]);
    let mut client = RecordingConnector::client(5);

    let response = client.get(b"/start").expect("an upgrade is permitted");
    assert_eq!(response.text().unwrap(), "hi");
    assert_eq!(
        Script::dialed(),
        vec!["origin.test:80".to_owned(), "secure.test:443".to_owned()]
    );
}

#[test]
fn a_post_rewritten_to_get_drops_representation_headers() {
    // A 303 turns POST into a bodyless GET. Keeping Content-Type describes a
    // body that is no longer being sent.
    const SEE_OTHER: &[u8] =
        b"HTTP/1.1 303 See Other\r\nLocation: /done\r\nContent-Length: 0\r\n\r\n";
    Script::load(&[SEE_OTHER, OK]);
    let mut client = RecordingConnector::client(5);

    let request = client
        .build(xibalba_client::proto::method::Method::Post, b"/submit")
        .header(b"Content-Type", b"application/json")
        .header(b"X-Keep", b"kept")
        .body(b"{}");
    let response = client.send(request).expect("the redirect is followed");
    assert_eq!(response.text().unwrap(), "hi");

    let second = Script::head_of_request(1);
    assert!(
        !second.to_ascii_lowercase().contains("content-type"),
        "the rewritten GET must not describe a body it no longer sends: {second}"
    );
    assert!(
        second.contains("X-Keep: kept"),
        "unrelated headers must survive the rewrite: {second}"
    );
}

#[test]
fn dot_segments_in_a_relative_redirect_are_resolved() {
    // "/a/b" + "../c" must become "/c", not "/a/../c": the server should
    // never see a target the client could resolve itself.
    Script::load(&[&redirect_to("../c"), OK]);
    let mut client = RecordingConnector::client(5);

    let response = client.get(b"/a/b").expect("the relative hop resolves");
    assert_eq!(response.text().unwrap(), "hi");
    assert_eq!(
        Script::requested(),
        vec!["GET /a/b HTTP/1.1".to_owned(), "GET /c HTTP/1.1".to_owned()],
        "the hop must be resolved to /c rather than sent as /a/../c"
    );
}
