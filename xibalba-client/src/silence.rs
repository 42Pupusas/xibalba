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
pub struct SilenceBudget {
    limit: Duration,
    last_progress: Instant,
}

impl SilenceBudget {
    pub fn new(limit: Duration) -> Self {
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

    /// Read from `stream`, absorbing `WouldBlock` ticks until data arrives
    /// or the silence budget is exhausted. Progress resets the budget.
    pub fn read(&mut self, stream: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match stream.read(buf) {
                Ok(n) => {
                    self.last_progress = Instant::now();
                    return Ok(n);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if self.last_progress.elapsed() >= self.limit {
                        return Err(self.expired());
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Like [`std::io::Read::read_exact`] but through the silence budget.
    pub fn read_exact(&mut self, stream: &mut impl Read, buf: &mut [u8]) -> std::io::Result<()> {
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
