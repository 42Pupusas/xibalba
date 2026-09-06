use std::io::{Read, Write};

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
        std::io::Error::new(std::io::ErrorKind::Interrupted, "request cancelled")
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

    #[test]
    fn a_cancelled_read_does_not_touch_the_stream() {
        let mut source: &[u8] = b"payload";
        let mut stream = InterruptibleStream::new(&mut source, Always);
        let mut buf = [0u8; 8];
        let err = stream.read(&mut buf).expect_err("cancelled read fails");
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
        assert_eq!(source.len(), 7, "no bytes should have been consumed");
    }

    #[test]
    fn a_cancelled_write_does_not_reach_the_stream() {
        let mut sink: Vec<u8> = Vec::new();
        let mut stream = InterruptibleStream::new(&mut sink, Always);
        let err = stream.write(b"request").expect_err("cancelled write fails");
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
        assert!(sink.is_empty(), "no bytes should have reached the peer");
    }

    #[test]
    fn a_cancelled_flush_is_reported() {
        let mut sink: Vec<u8> = Vec::new();
        let mut stream = InterruptibleStream::new(&mut sink, Always);
        let err = stream.flush().expect_err("cancelled flush fails");
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
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
            assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
        }
        assert_eq!(sink, b"first", "only the permitted write reached the peer");
    }
}
