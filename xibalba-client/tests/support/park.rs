//! Holding a test server open for exactly as long as the test needs.

use std::sync::mpsc::{Receiver, Sender, channel};

/// Keeps a server thread parked until the test releases it.
///
/// A test server must usually hold its socket open past the operation under
/// test: closing early lets the client finish for the wrong reason, so the
/// assertion would pass without proving anything. Sleeping for a fixed span
/// achieves that, but it costs the suite that span on every join and is only
/// ever a guess at how long the test needs. Blocking on a channel ends the
/// instant the test drops its [`StopSignal`], so the hold is exact and
/// teardown is immediate.
///
/// Dropping the signal is the release; nothing is ever sent. That way a test
/// that panics still frees its server, because the unwind drops the signal.
pub(crate) struct StopSignal(
    #[allow(dead_code, reason = "held for its drop, not read")] Sender<()>,
);

impl StopSignal {
    /// Returns the signal for the test to hold and the park for its server.
    #[must_use]
    pub(crate) fn new() -> (Self, StopPark) {
        let (tx, rx) = channel();
        (Self(tx), StopPark(rx))
    }
}

/// The server-thread half of a [`StopSignal`].
pub(crate) struct StopPark(Receiver<()>);

impl StopPark {
    /// Block until the paired [`StopSignal`] is dropped, or `limit` passes.
    /// `true` means released; `false` means the span elapsed and the server
    /// may do one more unit of work.
    pub(crate) fn wait_for(&self, limit: std::time::Duration) -> bool {
        match self.0.recv_timeout(limit) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => true,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => false,
        }
    }

    /// Block until the paired [`StopSignal`] is dropped. The disconnect is the
    /// message, so this returns on a deliberate release and on a panicking
    /// test alike.
    pub(crate) fn wait(&self) {
        let _ = self.0.recv();
    }
}
