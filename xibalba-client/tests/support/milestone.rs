//! A point a test server reports reaching, for the test to wait on.

use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::Duration;

/// The server-thread half: reports that a point in the exchange was reached.
///
/// [`StopSignal`] holds a server open until the test releases it. This is the
/// other direction \u2014 the server telling the test it has got somewhere, so the
/// test can stop guessing how long that took.
///
/// [`StopSignal`]: super::park::StopSignal
pub(crate) struct Milestone(Sender<()>);

impl Milestone {
    /// Returns the reporter for the server and the waiter for the test.
    #[must_use]
    pub(crate) fn new() -> (Self, Reached) {
        let (tx, rx) = channel();
        (Self(tx), Reached(rx))
    }

    /// Report that the point has been reached.
    ///
    /// Ignores a disconnected receiver: a test that has already moved on, or
    /// panicked, must not take the server thread down with it.
    pub(crate) fn reached(&self) {
        let _ = self.0.send(());
    }
}

/// The test-thread half of a [`Milestone`].
pub(crate) struct Reached(Receiver<()>);

impl Reached {
    /// Block until the server reports the point, or fail.
    ///
    /// A deadline rather than an unbounded wait, so a server that never gets
    /// there fails the test with `what` instead of hanging the suite.
    pub(crate) fn wait(&self, what: &str) {
        match self.0.recv_timeout(Duration::from_secs(10)) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) => panic!("the server never reached: {what}"),
            Err(RecvTimeoutError::Disconnected) => {
                panic!("the server thread ended before reaching: {what}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn a_reported_milestone_releases_the_waiter() {
        let (milestone, reached) = Milestone::new();
        thread::spawn(move || milestone.reached());
        reached.wait("a milestone that is reported");
    }

    #[test]
    #[should_panic(expected = "the server thread ended before reaching")]
    fn a_dropped_reporter_fails_rather_than_hanging() {
        let (milestone, reached) = Milestone::new();
        drop(milestone);
        reached.wait("a milestone nobody reports");
    }

    /// Reporting into a dropped waiter must not panic the server thread.
    #[test]
    fn reporting_after_the_test_moved_on_is_harmless() {
        let (milestone, reached) = Milestone::new();
        drop(reached);
        milestone.reached();
    }
}
