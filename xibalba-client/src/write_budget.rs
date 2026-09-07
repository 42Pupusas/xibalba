use std::io::Write;
use std::time::{Duration, Instant};

use crate::tick::Tick;

/// Wall-clock tolerance for a write making no progress, the write-side twin of
/// [`SilenceBudget`](crate::silence::SilenceBudget).
///
/// `write_timeout` is a per-call ceiling that exists for cancel latency, not a
/// failure threshold, exactly as `read_timeout` is on the read side. Without a
/// budget above it, the first `SO_SNDTIMEO` expiry ends the request and the raw
/// `WouldBlock` reaches the caller.
///
/// A write is not only a write. A lazily negotiated TLS session drives its
/// handshake inside the first `write`, so that call blocks waiting for the
/// *peer's* records and expires on the socket's receive timeout. A server that
/// is merely slow to begin its handshake then fails a healthy request after one
/// `read_timeout`, with the silence budget configured for the exchange never
/// consulted.
///
/// The budget resets on every byte accepted: it bounds a *stall*, not the total
/// time a body takes to upload.
pub(crate) struct WriteBudget {
    limit: Duration,
    last_progress: Instant,
}

impl WriteBudget {
    pub(crate) fn new(limit: Duration) -> Self {
        Self {
            limit,
            last_progress: Instant::now(),
        }
    }

    /// The error surfaced when the budget is exhausted: a descriptive
    /// `TimedOut`, never the raw `WouldBlock`/`EAGAIN` the socket produced.
    fn expired(&self) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "peer accepted no data for {:.0?} (write budget exhausted)",
                self.limit
            ),
        )
    }

    /// Write all of `buf`, absorbing timeout ticks until the peer accepts the
    /// bytes or the budget is exhausted.
    ///
    /// This is `write_all` with the retry the budget needs, rather than a call
    /// to it: `write_all` treats a tick as fatal, and a partial write followed
    /// by a tick must resume at the offset already accepted, not restart.
    pub(crate) fn write_all(&mut self, stream: &mut impl Write, buf: &[u8]) -> std::io::Result<()> {
        let mut off = 0;
        while off < buf.len() {
            let tick_start = Instant::now();
            match stream.write(&buf[off..]) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "peer accepted no bytes",
                    ));
                }
                Ok(n) => {
                    off += n;
                    self.last_progress = Instant::now();
                }
                Err(e) if Tick::marks(&e) => {
                    if self.last_progress.elapsed() >= self.limit {
                        return Err(self.expired());
                    }
                    Tick::pace(tick_start);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Flush, absorbing ticks on the same budget. A TLS session flushes
    /// handshake records here as well as in `write`.
    pub(crate) fn flush(&self, stream: &mut impl Write) -> std::io::Result<()> {
        loop {
            let tick_start = Instant::now();
            match stream.flush() {
                Ok(()) => return Ok(()),
                Err(e) if Tick::marks(&e) => {
                    if self.last_progress.elapsed() >= self.limit {
                        return Err(self.expired());
                    }
                    Tick::pace(tick_start);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails every write with a fixed kind, counting the attempts.
    struct AlwaysTicks {
        kind: std::io::ErrorKind,
        attempts: usize,
    }

    impl AlwaysTicks {
        const fn new(kind: std::io::ErrorKind) -> Self {
            Self { kind, attempts: 0 }
        }
    }

    impl Write for AlwaysTicks {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.attempts += 1;
            Err(std::io::Error::new(self.kind, "tick"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(self.kind, "tick"))
        }
    }

    /// Ticks `ticks` times, then accepts everything.
    struct TicksThenAccepts {
        ticks: usize,
        kind: std::io::ErrorKind,
        accepted: Vec<u8>,
    }

    impl Write for TicksThenAccepts {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.ticks > 0 {
                self.ticks -= 1;
                return Err(std::io::Error::new(self.kind, "tick"));
            }
            self.accepted.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// This is the shape of a lazily negotiated TLS session against a server
    /// slow to start its handshake: the socket's receive timeout expires
    /// inside `write`, several times, before any byte is accepted.
    #[test]
    fn a_write_that_ticks_before_it_is_accepted_still_completes() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            let mut stream = TicksThenAccepts {
                ticks: 3,
                kind,
                accepted: Vec::new(),
            };
            let mut budget = WriteBudget::new(Duration::from_secs(5));
            budget
                .write_all(&mut stream, b"GET / HTTP/1.1\r\n\r\n")
                .unwrap_or_else(|e| panic!("{kind:?} must be absorbed as a tick, got: {e}"));
            assert_eq!(stream.accepted, b"GET / HTTP/1.1\r\n\r\n");
        }
    }

    #[test]
    fn an_exhausted_budget_reports_timed_out_not_the_raw_tick() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            let mut stream = AlwaysTicks::new(kind);
            let mut budget = WriteBudget::new(Duration::from_millis(20));
            let err = budget
                .write_all(&mut stream, b"payload")
                .expect_err("the budget must expire");
            assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
            assert!(err.to_string().contains("write budget"));
        }
    }

    #[test]
    fn a_genuine_error_is_not_absorbed() {
        let mut stream = AlwaysTicks::new(std::io::ErrorKind::BrokenPipe);
        let mut budget = WriteBudget::new(Duration::from_secs(5));
        let err = budget
            .write_all(&mut stream, b"payload")
            .expect_err("a broken pipe must surface");
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(stream.attempts, 1, "a real error must not be retried");
    }

    /// A cancel is an `Other` carrying a payload, not a tick, so it must end
    /// the write instead of being retried against a still-cancelled source.
    #[test]
    fn a_cancellation_ends_the_write_rather_than_being_retried() {
        struct AlwaysCancelled;
        impl Write for AlwaysCancelled {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(crate::interrupt::Cancelled::error())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut budget = WriteBudget::new(Duration::from_mins(1));
        let err = budget
            .write_all(&mut AlwaysCancelled, b"payload")
            .expect_err("a cancel must surface");
        assert!(crate::interrupt::Cancelled::marks(&err));
    }

    #[test]
    fn a_non_blocking_stream_does_not_busy_loop() {
        let mut stream = AlwaysTicks::new(std::io::ErrorKind::WouldBlock);
        let mut budget = WriteBudget::new(Duration::from_millis(50));
        let _ = budget.write_all(&mut stream, b"payload");
        assert!(
            stream.attempts < 200,
            "budget spun {} times in 50ms; the retry loop is not paced",
            stream.attempts
        );
    }

    /// A partial write followed by a tick must resume where it stopped.
    /// Restarting would resend the accepted prefix, which the peer reads as
    /// part of the same request.
    #[test]
    fn a_partial_write_resumes_instead_of_resending_its_prefix() {
        struct PartialThenTick {
            accepted: Vec<u8>,
            ticked: bool,
        }
        impl Write for PartialThenTick {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                if self.accepted.is_empty() {
                    self.accepted.extend_from_slice(&buf[..3]);
                    return Ok(3);
                }
                if !self.ticked {
                    self.ticked = true;
                    return Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "tick"));
                }
                self.accepted.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut stream = PartialThenTick {
            accepted: Vec::new(),
            ticked: false,
        };
        let mut budget = WriteBudget::new(Duration::from_secs(5));
        budget.write_all(&mut stream, b"abcdefgh").unwrap();
        assert_eq!(stream.accepted, b"abcdefgh");
    }

    /// Progress resets the budget, so a slow but steadily accepted upload is
    /// never cut off for taking longer than the limit in total.
    #[test]
    fn steady_progress_never_exhausts_the_budget() {
        struct AcceptsOneByte;
        impl Write for AcceptsOneByte {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                std::thread::sleep(Duration::from_millis(15));
                Ok(buf.len().min(1))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut budget = WriteBudget::new(Duration::from_millis(40));
        budget
            .write_all(&mut AcceptsOneByte, b"abcdefgh")
            .expect("steady progress must never exhaust a write budget");
    }

    #[test]
    fn a_flush_that_ticks_completes_and_an_endless_one_expires() {
        let mut stream = TicksThenAccepts {
            ticks: 0,
            kind: std::io::ErrorKind::WouldBlock,
            accepted: Vec::new(),
        };
        WriteBudget::new(Duration::from_secs(5))
            .flush(&mut stream)
            .expect("a flush that succeeds is not delayed");

        let mut stalled = AlwaysTicks::new(std::io::ErrorKind::WouldBlock);
        let err = WriteBudget::new(Duration::from_millis(20))
            .flush(&mut stalled)
            .expect_err("an endlessly ticking flush must expire");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(err.to_string().contains("write budget"));
    }
}
