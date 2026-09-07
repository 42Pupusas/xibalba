use std::fmt;
use std::io::Read;
use std::time::{Duration, Instant};

use xibalba_proto::error::Error;

use crate::tick::Tick;

/// Marks an [`std::io::Error`] as *the request deadline passed*, as opposed
/// to the peer going silent (a silence budget) or the caller cancelling.
///
/// The kind is `TimedOut` because that is what a deadline is; the payload
/// keeps it distinguishable from a socket timeout, which the budget absorbs
/// as a tick. Like [`Cancelled`](crate::interrupt::Cancelled), it exists
/// because the payload is dropped at the `proto::Error` boundary and the
/// kind alone cannot answer "which limit fired".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestDeadlineExceeded;

impl fmt::Display for RequestDeadlineExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("request deadline exceeded")
    }
}

impl std::error::Error for RequestDeadlineExceeded {}

/// The total wall-clock bound for one operation, as opposed to the
/// silence budgets, which bound a *gap* between bytes.
///
/// Built once at the operation's entry and passed down through every read
/// it covers, so redirect hops and the buffered body share one budget and
/// a deadline spent on the head shortens what the body may take.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestDeadline(Option<Instant>);

impl RequestDeadline {
    /// No total bound: silence budgets alone govern the operation.
    pub(crate) const NONE: Self = Self(None);

    /// A deadline `budget` from now, or [`Self::NONE`] when no budget was
    /// configured.
    pub(crate) fn after(budget: Option<Duration>) -> Self {
        Self(budget.and_then(|b| Instant::now().checked_add(b)))
    }

    fn expired(&self) -> bool {
        self.0.is_some_and(|at| at <= Instant::now())
    }

    /// The error a passing total produces, in the `io::Error` shape the
    /// read loop works in.
    fn error() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::TimedOut, RequestDeadlineExceeded)
    }

    /// Refuse work once the total has passed, before anything further is
    /// started.
    ///
    /// # Errors
    /// Returns [`ConnectionError::RequestDeadlineExceeded`] once the total
    /// for the operation has passed.
    pub(crate) fn check(&self) -> Result<(), Error> {
        if self.expired() {
            return Err(xibalba_proto::error::ConnectionError::RequestDeadlineExceeded.into());
        }
        Ok(())
    }

    /// Whether `error` is a total-deadline expiry rather than a silence
    /// gap or a transport failure.
    pub(crate) fn marks(error: &std::io::Error) -> bool {
        error.get_ref().is_some_and(
            <dyn std::error::Error + Send + Sync + 'static>::is::<RequestDeadlineExceeded>,
        )
    }

    /// Re-type a deadline expiry at the `io::Error` boundary, where its
    /// payload would otherwise be dropped in favour of a kind and a message.
    /// Any other error passes through unchanged.
    pub(crate) fn classify(error: std::io::Error) -> Error {
        if Self::marks(&error) {
            return xibalba_proto::error::ConnectionError::RequestDeadlineExceeded.into();
        }
        error.into()
    }
}

/// Wall-clock tolerance for peer silence across read-timeout ticks.
///
/// A blocking socket with `SO_RCVTIMEO` returns `EAGAIN`/`WouldBlock` when
/// no data arrives within `read_timeout`. That per-read ceiling exists for
/// cancel latency, not as a failure threshold — so silence tolerance must
/// be measured in wall-clock time, independent of how short the per-read
/// timeout is. The previous design capped retry *counts*, which silently
/// changed meaning with the configured `read_timeout` (a 5s timeout gave
/// only ~20s of head tolerance) and leaked the raw `EAGAIN` ("os error 11")
/// to callers when it tripped. The budget resets on every successful read:
/// it bounds *silence*, not total transfer time.
#[derive(Debug)]
pub(crate) struct SilenceBudget {
    limit: Duration,
    last_progress: Instant,
    deadline: RequestDeadline,
}

impl SilenceBudget {
    pub(crate) fn new(limit: Duration) -> Self {
        Self::with_deadline(limit, RequestDeadline::NONE)
    }

    /// A gap budget that also answers to a total deadline for the whole
    /// operation. Either limit ending the read is final; the deadline
    /// catches transfers that stay *under* the silence limit forever by
    /// trickling, which a gap budget cannot see.
    pub(crate) fn with_deadline(limit: Duration, deadline: RequestDeadline) -> Self {
        Self {
            limit,
            last_progress: Instant::now(),
            deadline,
        }
    }

    /// The error surfaced when the budget is exhausted: a descriptive
    /// `TimedOut`, never the raw `WouldBlock`/`EAGAIN` the socket produced.
    fn expired(&self) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "peer sent no data for {:.0?} (silence budget exhausted)",
                self.limit
            ),
        )
    }

    /// Read from `stream`, absorbing timeout ticks until data arrives or the
    /// silence budget is exhausted. Progress resets the budget.
    pub(crate) fn read(
        &mut self,
        stream: &mut impl Read,
        buf: &mut [u8],
    ) -> std::io::Result<usize> {
        loop {
            // Checked before touching the stream, not only after a tick: a
            // peer that delivers a byte every few seconds produces no ticks
            // at all, and ticks are all the gap budget below ever sees. The
            // deadline is the only bound that fires in that case.
            if self.deadline.expired() {
                return Err(RequestDeadline::error());
            }
            let tick_start = Instant::now();
            match stream.read(buf) {
                Ok(n) => {
                    self.last_progress = Instant::now();
                    return Ok(n);
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
    }

    /// Like [`std::io::Read::read_exact`] but through the silence budget.
    pub(crate) fn read_exact(
        &mut self,
        stream: &mut impl Read,
        buf: &mut [u8],
    ) -> std::io::Result<()> {
        let mut off = 0;
        while off < buf.len() {
            let n = self.read(stream, &mut buf[off..])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed mid-body",
                ));
            }
            off += n;
        }
        Ok(())
    }

    /// [`Self::read`] with a deadline expiry re-typed as the protocol
    /// error, for call sites whose `?` would otherwise flatten it to a
    /// kind and a message at the `io::Error` boundary.
    pub(crate) fn read_proto(
        &mut self,
        stream: &mut impl Read,
        buf: &mut [u8],
    ) -> Result<usize, Error> {
        self.read(stream, buf).map_err(RequestDeadline::classify)
    }

    /// [`Self::read_exact`], typed as [`Self::read_proto`] is.
    pub(crate) fn read_exact_proto(
        &mut self,
        stream: &mut impl Read,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        self.read_exact(stream, buf)
            .map_err(RequestDeadline::classify)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails every read with a fixed kind, counting the attempts.
    struct AlwaysTicks {
        kind: std::io::ErrorKind,
        attempts: usize,
    }

    impl AlwaysTicks {
        const fn new(kind: std::io::ErrorKind) -> Self {
            Self { kind, attempts: 0 }
        }
    }

    impl Read for AlwaysTicks {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            self.attempts += 1;
            Err(std::io::Error::new(self.kind, "tick"))
        }
    }

    /// Ticks `ticks` times, then delivers `payload`.
    struct TicksThenData {
        ticks: usize,
        kind: std::io::ErrorKind,
        payload: &'static [u8],
    }

    impl Read for TicksThenData {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.ticks > 0 {
                self.ticks -= 1;
                return Err(std::io::Error::new(self.kind, "tick"));
            }
            let n = self.payload.len().min(buf.len());
            buf[..n].copy_from_slice(&self.payload[..n]);
            Ok(n)
        }
    }

    /// Linux reports a `SO_RCVTIMEO` expiry as `WouldBlock`, but Windows and
    /// several TLS wrappers report `TimedOut`. Both are ticks, not failures.
    #[test]
    fn a_timed_out_tick_is_absorbed_like_would_block() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            let mut stream = TicksThenData {
                ticks: 3,
                kind,
                payload: b"body",
            };
            let mut budget = SilenceBudget::new(Duration::from_secs(5));
            let mut buf = [0u8; 8];
            let n = budget
                .read(&mut stream, &mut buf)
                .unwrap_or_else(|e| panic!("{kind:?} must be absorbed as a tick, got: {e}"));
            assert_eq!(&buf[..n], b"body");
        }
    }

    #[test]
    fn an_exhausted_budget_reports_timed_out_not_the_raw_tick() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            let mut stream = AlwaysTicks::new(kind);
            let mut budget = SilenceBudget::new(Duration::from_millis(20));
            let err = budget
                .read(&mut stream, &mut [0u8; 8])
                .expect_err("the budget must expire");
            assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
            assert!(err.to_string().contains("silence budget"));
        }
    }

    /// A non-blocking stream ticks instantly, so the retry loop must pace
    /// itself or spin at full speed for the whole budget.
    #[test]
    fn an_immediately_ticking_stream_does_not_busy_loop() {
        let mut stream = AlwaysTicks::new(std::io::ErrorKind::WouldBlock);
        let mut budget = SilenceBudget::new(Duration::from_millis(50));
        let _ = budget.read(&mut stream, &mut [0u8; 8]);
        assert!(
            stream.attempts < 200,
            "budget spun {} times in 50ms; the retry loop is not paced",
            stream.attempts
        );
    }

    #[test]
    fn a_genuine_error_is_not_absorbed() {
        let mut stream = AlwaysTicks::new(std::io::ErrorKind::ConnectionReset);
        let mut budget = SilenceBudget::new(Duration::from_secs(5));
        let err = budget
            .read(&mut stream, &mut [0u8; 8])
            .expect_err("a reset must surface");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
        assert_eq!(stream.attempts, 1, "a real error must not be retried");
    }

    #[test]
    fn progress_resets_the_budget() {
        let mut stream = TicksThenData {
            ticks: 0,
            kind: std::io::ErrorKind::WouldBlock,
            payload: b"x",
        };
        let mut budget = SilenceBudget::new(Duration::from_millis(40));
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(15));
            budget
                .read(&mut stream, &mut [0u8; 8])
                .expect("steady progress must never exhaust a silence budget");
        }
    }

    /// Delivering one byte per read forever, with never a tick between: the
    /// gap budget resets each time and would run for the age of the peer.
    /// The total deadline is the only thing that can stop this transfer.
    struct TrickleForever;

    impl Read for TrickleForever {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            buf[0] = b'.';
            Ok(1)
        }
    }

    #[test]
    fn a_trickling_stream_hits_the_total_deadline_the_gap_budget_cannot_see() {
        let mut budget = SilenceBudget::with_deadline(
            Duration::from_secs(500),
            RequestDeadline::after(Some(Duration::from_millis(30))),
        );
        let mut whole: Vec<u8> = Vec::new();
        let mut buf = [0u8; 64];
        let err = loop {
            match budget.read(&mut TrickleForever, &mut buf) {
                Ok(n) => whole.extend_from_slice(&buf[..n]),
                Err(e) => break e,
            }
        };
        assert!(
            !whole.is_empty(),
            "the trickle did deliver bytes before the total"
        );

        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(
            RequestDeadline::marks(&err),
            "the payload must mark the deadline, not a silence gap"
        );
    }

    /// An already-passed deadline stops the read before the stream is
    /// touched at all, so a request whose hops spent the total never
    /// writes its next bytes.
    #[test]
    fn an_expired_deadline_ends_the_read_without_touching_the_stream() {
        struct Untouched;
        impl Read for Untouched {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                panic!("the deadline had passed; nothing may be read")
            }
        }
        let mut budget = SilenceBudget::with_deadline(
            Duration::from_secs(500),
            RequestDeadline::after(Some(Duration::ZERO)),
        );
        let err = budget
            .read(&mut Untouched, &mut [0u8; 8])
            .expect_err("the deadline passed before the read started");
        assert!(RequestDeadline::marks(&err));
    }

    /// Without a total, a well-behaved stream is unaffected: the deadline
    /// must not leak into requests that did not configure one.
    #[test]
    fn no_deadline_leaves_the_gap_budget_alone() {
        let mut budget = SilenceBudget::new(Duration::from_secs(5));
        let n = budget
            .read(&mut TrickleForever, &mut [0u8; 16])
            .expect("an unbounded request reads happily");
        assert_eq!(n, 1, "one read returns what the stream gave, no more");
    }
}
