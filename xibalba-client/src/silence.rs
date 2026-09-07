use std::io::Read;
use std::time::{Duration, Instant};

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
}

impl SilenceBudget {
    /// Whether `error` is a per-read timeout tick rather than a failure.
    ///
    /// Linux returns `EAGAIN`/`WouldBlock` from a `SO_RCVTIMEO` socket, but
    /// that is a platform detail, not a guarantee: Windows sockets and several
    /// TLS wrappers report the same condition as `TimedOut`. Absorbing only
    /// `WouldBlock` ended the request on the first tick everywhere else,
    /// collapsing the whole silence budget to a single `read_timeout`.
    fn is_tick(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    }

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
            let tick_start = Instant::now();
            match stream.read(buf) {
                Ok(n) => {
                    self.last_progress = Instant::now();
                    return Ok(n);
                }
                Err(e) if Self::is_tick(&e) => {
                    if self.last_progress.elapsed() >= self.limit {
                        return Err(self.expired());
                    }
                    if let Some(pause) = Self::MIN_TICK.checked_sub(tick_start.elapsed()) {
                        std::thread::sleep(pause);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Shortest gap between two consecutive timeout ticks before the budget
    /// assumes it is spinning rather than waiting.
    ///
    /// A blocking socket with `SO_RCVTIMEO` parks for the whole read timeout
    /// before ticking, so this never fires for one. A connector left in
    /// non-blocking mode returns `WouldBlock` immediately and the retry loop
    /// becomes a busy loop that burns a core for the entire silence budget;
    /// the pause keeps it a wait. It bounds cancellation latency too, so it
    /// stays far below any useful read timeout.
    const MIN_TICK: Duration = Duration::from_millis(1);

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
}
