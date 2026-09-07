use std::time::{Duration, Instant};

/// A blocking socket reporting "nothing happened within the timeout", and the
/// pacing that keeps retrying it a wait rather than a spin.
///
/// Both silence budgets classify their I/O errors the same way and pace their
/// retries the same way. The rule lives here so the read side and the write
/// side cannot drift apart on what counts as a failure.
pub(crate) struct Tick;

impl Tick {
    /// Shortest gap between two consecutive ticks before the caller assumes it
    /// is spinning rather than waiting.
    ///
    /// A blocking socket with `SO_RCVTIMEO`/`SO_SNDTIMEO` parks for the whole
    /// timeout before ticking, so this never fires for one. A connector left
    /// in non-blocking mode ticks immediately and turns the retry loop into a
    /// busy loop that burns a core for the entire budget; the pause keeps it a
    /// wait. It bounds cancellation latency too, so it stays far below any
    /// useful timeout.
    const MIN: Duration = Duration::from_millis(1);

    /// Whether `error` is a timeout tick rather than a failure.
    ///
    /// Linux reports an expiry as `WouldBlock`, but that is a platform detail,
    /// not a guarantee: Windows sockets and several TLS wrappers report the
    /// same condition as `TimedOut`. Absorbing only `WouldBlock` ends the
    /// operation on the first tick everywhere else, collapsing a whole budget
    /// to a single socket timeout.
    pub(crate) fn marks(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    }

    /// Sleep out the remainder of [`Self::MIN`] since `started`, so a stream
    /// that ticks instantly does not spin.
    pub(crate) fn pace(started: Instant) {
        if let Some(pause) = Self::MIN.checked_sub(started.elapsed()) {
            std::thread::sleep(pause);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_platform_spellings_of_a_timeout_are_ticks() {
        for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
            assert!(Tick::marks(&std::io::Error::new(kind, "tick")));
        }
    }

    #[test]
    fn a_transport_failure_is_not_a_tick() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            assert!(!Tick::marks(&std::io::Error::new(kind, "real")));
        }
    }

    /// The client marks its own cancellation with an `Other` payload, which
    /// must never be absorbed as a tick or a cancel would be retried away.
    #[test]
    fn a_cancellation_is_not_a_tick() {
        assert!(!Tick::marks(&crate::interrupt::Cancelled::error()));
    }

    #[test]
    fn pacing_an_instant_tick_sleeps_and_a_slow_one_does_not() {
        let started = Instant::now();
        Tick::pace(started);
        assert!(started.elapsed() >= Tick::MIN);

        let long_ago = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .unwrap();
        let before = Instant::now();
        Tick::pace(long_ago);
        assert!(before.elapsed() < Tick::MIN);
    }
}
