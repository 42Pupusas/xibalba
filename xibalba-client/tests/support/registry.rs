//! A [`Connector`] that answers from a script instead of a socket.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Mutex, OnceLock};

use xibalba_client::Deadline;
use xibalba_client::connector::Connector;
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::url::Url;

use super::gate::Gate;
use super::script::{Script, Step};
use super::scripted::{Progress, ScriptedStream, Written};

/// The scripts waiting to be served, keyed by port.
///
/// [`Connector::connect`] is a static method: it receives a URL and a TLS
/// config, and nothing else. There is no receiver to hang per-test state on,
/// and reconnects happen on the client's reader thread, so a thread-local
/// would not be visible either. A process-wide registry keyed by a synthetic
/// port is what the trait leaves available.
///
/// The port is a lookup key here, never a socket. Keys come from a counter
/// above the ephemeral range so a scripted test cannot collide with a real
/// listener in a suite that runs both.
struct Registry {
    scripts: Mutex<HashMap<u16, Vec<Endpoint>>>,
    next_port: AtomicU16,
}

impl Registry {
    /// The one registry, shared by every scripted connection in the binary.
    fn get() -> &'static Self {
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        REGISTRY.get_or_init(|| Self {
            scripts: Mutex::new(HashMap::new()),
            next_port: AtomicU16::new(Self::FIRST_PORT),
        })
    }

    /// Well clear of the ephemeral range, so a scripted key is never mistaken
    /// for a port a real test server could have been given.
    const FIRST_PORT: u16 = 9000;

    fn claim_port(&self) -> u16 {
        self.next_port.fetch_add(1, Ordering::Relaxed)
    }

    fn register(&self, port: u16, endpoints: Vec<Endpoint>) {
        self.scripts
            .lock()
            .expect("script registry poisoned")
            .insert(port, endpoints);
    }

    /// Take the next unserved endpoint for `port`.
    fn take(&self, port: u16) -> Option<Endpoint> {
        let mut scripts = self.scripts.lock().expect("script registry poisoned");
        let queued = scripts.get_mut(&port)?;
        let endpoint = if queued.is_empty() {
            None
        } else {
            Some(queued.remove(0))
        };
        drop(scripts);
        endpoint
    }
}

/// One connection's script, with the handles the test observes it through.
struct Endpoint {
    script: Script,
    written: Written,
    progress: Progress,
}

/// A connection a scripted server will accept, and what the client did with it.
///
/// Held by the test for the whole exchange: [`Self::written`] is what the
/// client sent, [`Self::completed_steps`] how far the script ran.
pub(crate) struct ScriptedConnection {
    written: Written,
    progress: Progress,
    expected_steps: usize,
    script: Script,
}

impl ScriptedConnection {
    /// The bytes the client wrote to this connection.
    pub(crate) fn written(&self) -> String {
        self.written.text()
    }

    /// The raw bytes the client wrote to this connection.
    pub(crate) fn written_bytes(&self) -> Vec<u8> {
        self.written.bytes()
    }

    /// How many script steps ran.
    pub(crate) fn completed_steps(&self) -> usize {
        self.progress.completed()
    }

    /// How many reads delivered data to the client.
    pub(crate) fn data_reads(&self) -> usize {
        self.progress.data_reads()
    }

    /// Assert the response reached the client in `expected` separate reads.
    ///
    /// Use where the *splitting* is the subject. Without this a barrier is
    /// unfalsifiable: remove it and the bytes coalesce into one read, the
    /// response is unchanged, and the test keeps passing while no longer
    /// testing the fragmentation it was written for.
    pub(crate) fn assert_data_reads(&self, expected: usize) {
        assert_eq!(
            self.progress.data_reads(),
            expected,
            "response arrived in {} reads, not {expected}: the script's \
             fragmentation is not reaching the client",
            self.progress.data_reads()
        );
    }

    /// Assert the script really waits on `gate`.
    ///
    /// A gate is unfalsifiable from the outside: delete the step that waits on
    /// it and the bytes are identical, every assertion still holds, and the
    /// ordering the test was written to force silently reverts to whatever the
    /// scheduler happens to do.
    ///
    /// This checks the script rather than watching the reader thread. Whether
    /// the reader was *parked* at the gate when the test opened it depends on
    /// thread timing and is not the guarantee being relied on; that the script
    /// cannot proceed without the test is.
    pub(crate) fn assert_gated_on(&self, gate: &Gate) {
        assert!(
            self.script
                .steps()
                .iter()
                .any(|step| matches!(step, Step::AwaitGate(g) if g == gate)),
            "no step waits on this gate: the ordering it exists to force is \
             left to timing"
        );
    }

    /// Assert the client drove the whole script.
    ///
    /// A script that stops early means the client read fewer times than the
    /// test assumed, which otherwise passes silently: the assertions on the
    /// response still hold, they were simply satisfied by less of the
    /// exchange than intended.
    pub(crate) fn assert_script_completed(&self) {
        assert_eq!(
            self.progress.completed(),
            self.expected_steps,
            "client stopped after {} of {} script steps",
            self.progress.completed(),
            self.expected_steps
        );
    }
}

/// A scripted server: a port key, and the scripts it answers with.
///
/// Answers each connection with the next script in turn, so a test covering a
/// reconnect describes both connections up front.
pub(crate) struct ScriptedServer {
    port: u16,
    connections: Vec<ScriptedConnection>,
}

impl ScriptedServer {
    /// Register `scripts`, one per expected connection, in order.
    pub(crate) fn serving(scripts: Vec<Script>) -> Self {
        let registry = Registry::get();
        let port = registry.claim_port();

        let mut endpoints = Vec::with_capacity(scripts.len());
        let mut connections = Vec::with_capacity(scripts.len());
        for script in scripts {
            let expected_steps = script.len();
            let written = Written::default();
            let progress = Progress::default();
            connections.push(ScriptedConnection {
                written: written.clone(),
                progress: progress.clone(),
                expected_steps,
                script: script.clone(),
            });
            endpoints.push(Endpoint {
                script,
                written,
                progress,
            });
        }

        registry.register(port, endpoints);
        Self { port, connections }
    }

    /// Register a single script.
    pub(crate) fn serving_one(script: Script) -> Self {
        Self::serving(vec![script])
    }

    /// The key to address this server by, in place of a real port.
    pub(crate) const fn port(&self) -> u16 {
        self.port
    }

    /// The nth connection the client made, in the order the scripts were given.
    pub(crate) fn connection(&self, index: usize) -> &ScriptedConnection {
        &self.connections[index]
    }

    /// The only connection, for a server serving one script.
    pub(crate) fn only(&self) -> &ScriptedConnection {
        assert_eq!(
            self.connections.len(),
            1,
            "only() needs exactly one scripted connection"
        );
        &self.connections[0]
    }
}

/// A [`Connector`] backed by [`Script`]s rather than sockets.
///
/// Deterministic where a socket is not: no kernel buffering, no partial
/// writes, no scheduler. Use it for parsing, framing, and state-machine
/// behaviour, where those are noise. Do not use it for backpressure or
/// blocked-write behaviour, where those *are* the subject — those tests need
/// a real socket and keep one.
pub(crate) struct ScriptedConnector;

impl Connector for ScriptedConnector {
    type Stream = ScriptedStream;
    type TlsConfig = ();

    fn connect(
        url: &Url<'_>,
        _tls_config: &(),
        _deadline: Deadline,
    ) -> Result<ScriptedStream, Error> {
        let port = url.effective_port();
        let endpoint = Registry::get().take(port).ok_or_else(|| {
            Error::Connection(ConnectionError::Other(format!(
                "no script registered for port {port}: the client connected \
                 more times than the test scripted"
            )))
        })?;

        let mut stream = ScriptedStream::new(endpoint.script);
        stream.adopt(endpoint.written, endpoint.progress);
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn url_for(port: u16) -> String {
        format!("http://127.0.0.1:{port}/")
    }

    #[test]
    fn a_connection_plays_the_script_registered_for_its_port() {
        let server = ScriptedServer::serving_one(Script::new().send(b"hello".to_vec()));
        let url = url_for(server.port());
        let mut stream = ScriptedConnector::connect(
            &Url::parse(url.as_bytes()).unwrap(),
            &(),
            Deadline::never(),
        )
        .expect("registered port must connect");

        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");
    }

    #[test]
    fn each_connection_takes_the_next_script_in_order() {
        let server = ScriptedServer::serving(vec![
            Script::new().send(b"first".to_vec()),
            Script::new().send(b"second".to_vec()),
        ]);
        let url = url_for(server.port());
        let parsed = Url::parse(url.as_bytes()).unwrap();

        let mut buf = [0u8; 16];
        let mut one = ScriptedConnector::connect(&parsed, &(), Deadline::never()).unwrap();
        let n = one.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"first");

        let mut two = ScriptedConnector::connect(&parsed, &(), Deadline::never()).unwrap();
        let n = two.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"second");
    }

    #[test]
    fn connecting_more_times_than_scripted_fails_rather_than_hanging() {
        let server = ScriptedServer::serving_one(Script::new().send(b"only".to_vec()));
        let url = url_for(server.port());
        let parsed = Url::parse(url.as_bytes()).unwrap();

        assert!(ScriptedConnector::connect(&parsed, &(), Deadline::never()).is_ok());
        let Err(err) = ScriptedConnector::connect(&parsed, &(), Deadline::never()) else {
            panic!("a second connection has no script and must be refused");
        };
        assert!(
            matches!(err, Error::Connection(ConnectionError::Other(ref m)) if m.contains("no script registered")),
            "unhelpful error for an unscripted connection: {err}"
        );
    }

    #[test]
    fn an_unregistered_port_refuses_the_connection() {
        let url = url_for(1);
        let Err(err) = ScriptedConnector::connect(
            &Url::parse(url.as_bytes()).unwrap(),
            &(),
            Deadline::never(),
        ) else {
            panic!("an unregistered port must refuse");
        };
        assert!(matches!(err, Error::Connection(ConnectionError::Other(_))));
    }

    #[test]
    fn servers_never_share_a_port_key() {
        let a = ScriptedServer::serving_one(Script::new());
        let b = ScriptedServer::serving_one(Script::new());
        assert_ne!(a.port(), b.port());
        assert!(a.port() >= Registry::FIRST_PORT);
    }

    #[test]
    fn a_gated_script_is_recognised_as_gated_on_that_gate() {
        let gate = Gate::shut();
        let server = ScriptedServer::serving_one(Script::new().await_gate(&gate).send(b"x"));
        server.only().assert_gated_on(&gate);
    }

    #[test]
    #[should_panic(expected = "no step waits on this gate")]
    fn a_script_missing_its_gate_is_reported() {
        let gate = Gate::shut();
        let server = ScriptedServer::serving_one(Script::new().send(b"x"));
        server.only().assert_gated_on(&gate);
    }

    /// Waiting on *a* gate is not the same as waiting on *this* gate.
    #[test]
    #[should_panic(expected = "no step waits on this gate")]
    fn a_script_gated_on_a_different_gate_does_not_count() {
        let checked = Gate::shut();
        let server = ScriptedServer::serving_one(Script::new().await_gate(&Gate::shut()));
        server.only().assert_gated_on(&checked);
    }

    #[test]
    fn the_test_observes_what_the_client_wrote() {
        let server = ScriptedServer::serving_one(Script::new().expect_request());
        let url = url_for(server.port());
        let mut stream = ScriptedConnector::connect(
            &Url::parse(url.as_bytes()).unwrap(),
            &(),
            Deadline::never(),
        )
        .unwrap();

        stream.write_all(b"GET /x HTTP/1.1\r\n\r\n").unwrap();

        assert_eq!(server.only().written(), "GET /x HTTP/1.1\r\n\r\n");
    }
}
