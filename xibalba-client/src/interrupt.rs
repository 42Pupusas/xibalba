use std::fmt;
use std::io::{Read, Write};

/// Marks an [`std::io::Error`] as *this client cancelled the request*, as
/// opposed to the peer or the OS failing it.
///
/// The kind is deliberately not `Interrupted`. `write_all` and `read_exact`
/// retry `Interrupted` internally — that kind means "a signal arrived, try
/// again", which is precisely the opposite of what a cancel wants — so a
/// cancellation reported that way is swallowed and the operation reissued
/// against a still-cancelled interrupt, forever. The kind is `Other` and the
/// signal is the payload, which no std retry loop inspects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("request cancelled")
    }
}

impl std::error::Error for Cancelled {}

impl Cancelled {
    /// The error a cancelled operation returns.
    #[must_use]
    pub fn error() -> std::io::Error {
        std::io::Error::other(Self)
    }

    /// Whether `error` is a cancellation rather than a transport failure.
    /// A genuine `Interrupted` from the OS is *not* one of these.
    pub fn marks(error: &std::io::Error) -> bool {
        error
            .get_ref()
            .is_some_and(<dyn std::error::Error + Send + Sync + 'static>::is::<Self>)
    }
}

/// Remembers whether the interrupt it wraps ever fired.
///
/// A cancellation crosses several layers before it is classified, and the
/// `std::io::Error` payload is lost at the `xibalba_proto::error::Error`
/// boundary, which keeps only a kind and a message. Asking the interrupt is
/// authoritative where inspecting the resulting error is guesswork.
pub struct Latch<I: Interrupt> {
    inner: I,
    fired: bool,
}

impl<I: Interrupt> Latch<I> {
    pub const fn new(inner: I) -> Self {
        Self {
            inner,
            fired: false,
        }
    }

    /// Whether the wrapped interrupt has reported cancellation at any point.
    pub const fn fired(&self) -> bool {
        self.fired
    }
}

impl<I: Interrupt> Interrupt for Latch<I> {
    fn is_cancelled(&mut self) -> bool {
        self.fired |= self.inner.is_cancelled();
        self.fired
    }
}

/// Asked before each blocking step of a request whether to give up.
///
/// [`Client`](crate::client::Client) has no notion of cancellation: it runs on
/// the calling thread, which can simply stop calling it. The async reader does
/// — it must observe a cancel or a shutdown flag while parked mid-request —
/// but the control channel it polls belongs to `async_client`. This trait is
/// the seam: the client asks, the caller answers.
pub trait Interrupt {
    /// Whether the operation in progress should abort now.
    fn is_cancelled(&mut self) -> bool;
}

/// Lets an interrupt be lent to a nested operation rather than moved, so one
/// source can cover a whole request's write, head read, and retry.
impl<I: Interrupt + ?Sized> Interrupt for &mut I {
    fn is_cancelled(&mut self) -> bool {
        (**self).is_cancelled()
    }
}

/// The interrupt for callers that never cancel, such as the synchronous
/// [`Client`](crate::client::Client) used directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeverCancelled;

impl Interrupt for NeverCancelled {
    fn is_cancelled(&mut self) -> bool {
        false
    }
}

/// Wraps a stream so every read *and* write consults an [`Interrupt`] first.
///
/// Cancellation previously covered body reads alone. A response head that
/// never arrives, or a request body write to a peer that stopped reading,
/// left the reader blocked with no path back to the control channel: the
/// cancel was only observed once the head-silence budget expired.
pub struct InterruptibleStream<'a, S, I: Interrupt> {
    inner: &'a mut S,
    interrupt: I,
}

impl<'a, S, I: Interrupt> InterruptibleStream<'a, S, I> {
    pub const fn new(inner: &'a mut S, interrupt: I) -> Self {
        Self { inner, interrupt }
    }

    fn interrupted() -> std::io::Error {
        Cancelled::error()
    }
}

impl<S: Read, I: Interrupt> Read for InterruptibleStream<'_, S, I> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.interrupt.is_cancelled() {
            return Err(Self::interrupted());
        }
        self.inner.read(buf)
    }
}

impl<S: Write, I: Interrupt> Write for InterruptibleStream<'_, S, I> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.interrupt.is_cancelled() {
            return Err(Self::interrupted());
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.interrupt.is_cancelled() {
            return Err(Self::interrupted());
        }
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Always;
    impl Interrupt for Always {
        fn is_cancelled(&mut self) -> bool {
            true
        }
    }

    /// `write_all` retries `ErrorKind::Interrupted` internally, so a
    /// cancellation reported through that kind is swallowed and the write is
    /// reissued against a still-cancelled interrupt, forever.
    #[test]
    fn a_cancelled_write_all_reports_instead_of_retrying_forever() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut sink: Vec<u8> = Vec::new();
            let mut stream = InterruptibleStream::new(&mut sink, Always);
            let _ = done_tx.send(stream.write_all(b"payload").is_err());
        });
        match done_rx.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(errored) => assert!(errored, "a cancelled write_all must report it"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("write_all never returned: the cancel was retried forever")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("the writing thread died")
            }
        }
    }

    /// Cancelled only once a given number of checks have passed, so a test can
    /// place the cancel at a chosen point in a multi-step operation.
    struct After(usize);
    impl Interrupt for After {
        fn is_cancelled(&mut self) -> bool {
            if self.0 == 0 {
                return true;
            }
            self.0 -= 1;
            false
        }
    }

    /// A signal-interrupted read is the OS asking for a retry, not a cancel.
    #[test]
    fn an_os_interrupted_error_is_not_mistaken_for_cancellation() {
        let os = std::io::Error::new(std::io::ErrorKind::Interrupted, "signal");
        assert!(!Cancelled::marks(&os));
        assert!(Cancelled::marks(&Cancelled::error()));
    }

    #[test]
    fn a_latch_remembers_a_cancel_after_the_source_stops_reporting() {
        struct Once(bool);
        impl Interrupt for Once {
            fn is_cancelled(&mut self) -> bool {
                let first = self.0;
                self.0 = false;
                first
            }
        }
        let mut latch = Latch::new(Once(true));
        assert!(latch.is_cancelled());
        assert!(
            latch.is_cancelled(),
            "the latch holds after the source clears"
        );
        assert!(latch.fired());
    }

    #[test]
    fn a_cancelled_read_does_not_touch_the_stream() {
        let mut source: &[u8] = b"payload";
        let mut stream = InterruptibleStream::new(&mut source, Always);
        let mut buf = [0u8; 8];
        let err = stream.read(&mut buf).expect_err("cancelled read fails");
        assert!(Cancelled::marks(&err));
        assert_eq!(source.len(), 7, "no bytes should have been consumed");
    }

    #[test]
    fn a_cancelled_write_does_not_reach_the_stream() {
        let mut sink: Vec<u8> = Vec::new();
        let mut stream = InterruptibleStream::new(&mut sink, Always);
        let err = stream.write(b"request").expect_err("cancelled write fails");
        assert!(Cancelled::marks(&err));
        assert!(sink.is_empty(), "no bytes should have reached the peer");
    }

    #[test]
    fn a_cancelled_flush_is_reported() {
        let mut sink: Vec<u8> = Vec::new();
        let mut stream = InterruptibleStream::new(&mut sink, Always);
        let err = stream.flush().expect_err("cancelled flush fails");
        assert!(Cancelled::marks(&err));
    }

    #[test]
    fn an_uncancelled_stream_passes_bytes_through() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut stream = InterruptibleStream::new(&mut sink, NeverCancelled);
            stream.write_all(b"request").expect("write succeeds");
            stream.flush().expect("flush succeeds");
        }
        assert_eq!(sink, b"request");
    }

    #[test]
    fn cancellation_takes_effect_partway_through() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut stream = InterruptibleStream::new(&mut sink, After(1));
            stream
                .write_all(b"first")
                .expect("the first write is allowed");
            let err = stream
                .write(b"second")
                .expect_err("the second is cancelled");
            assert!(Cancelled::marks(&err));
        }
        assert_eq!(sink, b"first", "only the permitted write reached the peer");
    }
}
