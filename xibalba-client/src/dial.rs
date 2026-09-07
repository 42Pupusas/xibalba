use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::scheme::Scheme;
use xibalba_proto::url::Url;

use crate::deadline::Deadline;

/// Opens the TCP half of a connection the way a production connector should.
///
/// Every [`Connector`](crate::connector::Connector) backed by TCP repeats the
/// same four steps, and the obvious spelling of each is wrong:
///
/// - resolution must use [`Url::connection_host`], not `Url::host`, or every
///   IPv6 URL fails on the brackets;
/// - a host resolving to several addresses must be tried in turn, or a
///   dual-stack name whose first address is unreachable never connects even
///   though a working address was returned;
/// - `TcpStream::connect` blocks with no bound of its own, so a silent peer
///   holds the caller for the OS SYN timeout, past any client-level budget;
/// - `TCP_NODELAY` must be set, as [`Connector::connect`] asks, or a
///   head/body write pair meets delayed ACK.
///
/// The per-address timeout is not by itself a bound on `dial`: a host
/// resolving to eight addresses, all blackholed, takes eight times as long as
/// any one attempt. The [`Deadline`] passed to [`Connector::connect`] is the
/// bound, and it is shared across resolution and every attempt.
///
/// TLS is layered on top by the caller: this type resolves and connects,
/// nothing more, and the client crate keeps no TLS dependency.
///
/// [`Connector::connect`]: crate::connector::Connector::connect
pub struct TcpDialer {
    connect_timeout: Duration,
}

impl TcpDialer {
    /// Long enough for a distant peer over a slow path, short enough that a
    /// blackholed address does not hold the caller for the OS SYN timeout,
    /// which is typically ~130s on Linux.
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

    #[must_use]
    pub const fn new(connect_timeout: Duration) -> Self {
        Self { connect_timeout }
    }

    /// Resolve `url` and connect to the first address that accepts, returning
    /// by `deadline`.
    ///
    /// # Errors
    /// Returns `Error::Connection` if the host is not UTF-8 or resolves to no
    /// address, `ConnectionError::ConnectDeadlineExceeded` if `deadline`
    /// passes, and `Error::Io` carrying the last address's failure if every
    /// resolved address refused.
    pub fn dial(&self, url: &Url<'_>, deadline: Deadline) -> Result<TcpStream, Error> {
        let host = std::str::from_utf8(url.connection_host()).map_err(|_| {
            Error::Connection(ConnectionError::Other("invalid UTF-8 in host".into()))
        })?;
        deadline.check()?;
        // `getaddrinfo` takes no timeout, so this call cannot be cut short.
        // Checking after it bounds when a resolved address is *used*, which is
        // as far as a blocking resolver allows; a caller needing the lookup
        // itself bounded must resolve on its own thread and hand the addresses
        // to `dial_addresses`.
        let addresses = (host, url.effective_port())
            .to_socket_addrs()
            .map_err(|e| Error::Connection(ConnectionError::Other(format!("DNS failed: {e}"))))?;
        deadline.check()?;
        self.dial_addresses(addresses, deadline)
    }

    /// Reject HTTPS before connecting, then [`Self::dial`].
    ///
    /// A plaintext connector handed an HTTPS URL would otherwise send the
    /// request, credentials included, in the clear to a port expecting TLS.
    /// Failing closed makes the misconfiguration an error rather than a
    /// silent downgrade.
    ///
    /// # Errors
    /// Returns `ConnectionError::PlaintextConnectorForHttps` for an HTTPS
    /// URL, otherwise as [`Self::dial`].
    pub fn dial_plaintext(&self, url: &Url<'_>, deadline: Deadline) -> Result<TcpStream, Error> {
        if url.scheme == Scheme::Https {
            return Err(Error::Connection(
                ConnectionError::PlaintextConnectorForHttps,
            ));
        }
        self.dial(url, deadline)
    }

    /// Try each address in turn until one accepts, within `deadline`.
    ///
    /// Each attempt gets the shorter of `connect_timeout` and the time the
    /// deadline leaves, so the total is the deadline rather than a multiple of
    /// the per-address timeout. Running out mid-list reports the deadline, not
    /// the last address's refusal: no address was proven unreachable, the
    /// client simply stopped asking.
    ///
    /// # Errors
    /// Returns `Error::Connection` if `addresses` is empty,
    /// `ConnectionError::ConnectDeadlineExceeded` if the deadline passes with
    /// attempts outstanding, otherwise the last connect failure.
    pub fn dial_addresses(
        &self,
        addresses: impl IntoIterator<Item = SocketAddr>,
        deadline: Deadline,
    ) -> Result<TcpStream, Error> {
        let mut last: Option<std::io::Error> = None;
        for address in addresses {
            let allowed = deadline.clamp(self.connect_timeout)?;
            match TcpStream::connect_timeout(&address, allowed) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    return Ok(stream);
                }
                Err(e) => last = Some(e),
            }
        }
        last.map_or_else(
            || {
                Err(Error::Connection(ConnectionError::Other(
                    "host resolved to no addresses".into(),
                )))
            },
            |e| Err(e.into()),
        )
    }
}

impl Default for TcpDialer {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CONNECT_TIMEOUT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr, TcpListener};

    struct Loopback;

    impl Loopback {
        fn listener(address: SocketAddr) -> Option<TcpListener> {
            TcpListener::bind(address).ok()
        }

        fn closed_port() -> SocketAddr {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let address = listener.local_addr().unwrap();
            drop(listener);
            address
        }
    }

    #[test]
    fn an_ipv4_loopback_url_connects() {
        let listener = Loopback::listener((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let port = listener.local_addr().unwrap().port();
        let raw = format!("http://127.0.0.1:{port}/");
        let url = Url::parse(raw.as_bytes()).unwrap();
        TcpDialer::default()
            .dial(&url, Deadline::never())
            .expect("dial the listener");
    }

    /// The bracketed literal is exactly what a naive connector passes to
    /// `ToSocketAddrs`, where it fails; the dialer must unbracket it.
    #[test]
    fn an_ipv6_loopback_url_connects_despite_its_brackets() {
        let Some(listener) = Loopback::listener((Ipv6Addr::LOCALHOST, 0).into()) else {
            return;
        };
        let port = listener.local_addr().unwrap().port();
        let raw = format!("http://[::1]:{port}/");
        let url = Url::parse(raw.as_bytes()).unwrap();
        TcpDialer::default()
            .dial(&url, Deadline::never())
            .expect("dial the IPv6 listener");
    }

    /// A dual-stack name commonly resolves to an address that refuses and one
    /// that accepts; stopping at the first is the bug this guards.
    #[test]
    fn a_failing_first_address_falls_through_to_a_working_one() {
        let listener = Loopback::listener((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let working = listener.local_addr().unwrap();
        let addresses = [Loopback::closed_port(), working];
        TcpDialer::default()
            .dial_addresses(addresses, Deadline::never())
            .expect("the second address accepts");
    }

    #[test]
    fn every_address_failing_reports_the_last_error() {
        let error = TcpDialer::default()
            .dial_addresses([Loopback::closed_port()], Deadline::never())
            .expect_err("nothing is listening");
        assert!(
            matches!(error, Error::Io(_)),
            "expected the transport error, got {error:?}"
        );
    }

    #[test]
    fn no_addresses_is_reported_as_a_connection_error() {
        let error = TcpDialer::default()
            .dial_addresses([], Deadline::never())
            .expect_err("no address can be dialled");
        assert!(matches!(
            error,
            Error::Connection(ConnectionError::Other(_))
        ));
    }

    #[test]
    fn a_dialled_stream_has_nodelay_enabled() {
        let listener = Loopback::listener((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let stream = TcpDialer::default()
            .dial_addresses([listener.local_addr().unwrap()], Deadline::never())
            .unwrap();
        assert!(stream.nodelay().unwrap(), "TCP_NODELAY must be set");
    }

    #[test]
    fn a_plaintext_dial_refuses_an_https_url() {
        let url = Url::parse(b"https://127.0.0.1:1/").unwrap();
        let error = TcpDialer::default()
            .dial_plaintext(&url, Deadline::never())
            .expect_err("a plaintext connector must not serve https");
        assert_eq!(
            error,
            Error::Connection(ConnectionError::PlaintextConnectorForHttps)
        );
    }

    #[test]
    fn a_plaintext_dial_still_serves_http() {
        let listener = Loopback::listener((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let port = listener.local_addr().unwrap().port();
        let raw = format!("http://127.0.0.1:{port}/");
        let url = Url::parse(raw.as_bytes()).unwrap();
        TcpDialer::default()
            .dial_plaintext(&url, Deadline::never())
            .expect("http ok");
    }

    /// Counts how many addresses the dialler actually pulled.
    ///
    /// Wall-clock cannot show this on loopback: a closed port refuses
    /// instantly, so timing a list of them passes whether or not the deadline
    /// is consulted. Reaching a blackholed address that really stalls means
    /// depending on the host's routing. Counting attempts is the same property
    /// with neither problem.
    struct CountingAddresses {
        remaining: usize,
        pulled: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl Iterator for CountingAddresses {
        type Item = SocketAddr;

        fn next(&mut self) -> Option<SocketAddr> {
            self.remaining = self.remaining.checked_sub(1)?;
            self.pulled.set(self.pulled.get() + 1);
            Some(Loopback::closed_port())
        }
    }

    /// The bug the deadline exists to fix: every address used to get the full
    /// `connect_timeout`, so a name resolving to N unreachable addresses took
    /// N times the configured bound, with nothing at the client level able to
    /// interrupt it.
    #[test]
    fn an_expired_deadline_stops_the_list_instead_of_trying_every_address() {
        let pulled = std::rc::Rc::new(std::cell::Cell::new(0));
        let addresses = CountingAddresses {
            remaining: 64,
            pulled: std::rc::Rc::clone(&pulled),
        };

        let error = TcpDialer::new(Duration::from_secs(10))
            .dial_addresses(addresses, Deadline::after(Duration::from_micros(1)))
            .expect_err("the deadline passes before the list ends");

        assert!(
            pulled.get() < 64,
            "the deadline must cut the list short; all {} addresses were tried",
            pulled.get()
        );
        assert_eq!(
            error,
            Error::Connection(ConnectionError::ConnectDeadlineExceeded)
        );
    }

    /// An already-expired deadline must stop before the first syscall, or the
    /// bound is one connect attempt looser than it claims.
    #[test]
    fn an_expired_deadline_attempts_no_address_at_all() {
        let listener = Loopback::listener((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let error = TcpDialer::default()
            .dial_addresses(
                [listener.local_addr().unwrap()],
                Deadline::after(Duration::ZERO),
            )
            .expect_err("an expired deadline connects to nothing, even a live listener");
        assert_eq!(
            error,
            Error::Connection(ConnectionError::ConnectDeadlineExceeded)
        );
    }

    /// Running out of time is not the same as being refused: no address was
    /// proven unreachable, so reporting the last refusal would misattribute a
    /// client-side give-up to the peer.
    #[test]
    fn exhausting_the_deadline_is_reported_as_the_deadline_not_the_last_refusal() {
        let refusing: Vec<SocketAddr> = (0..64).map(|_| Loopback::closed_port()).collect();
        let error = TcpDialer::new(Duration::from_secs(10))
            .dial_addresses(refusing, Deadline::after(Duration::from_micros(1)))
            .expect_err("the deadline passes long before the list ends");
        assert_eq!(
            error,
            Error::Connection(ConnectionError::ConnectDeadlineExceeded),
            "a client-side give-up must not be reported as the peer refusing"
        );
    }
}
