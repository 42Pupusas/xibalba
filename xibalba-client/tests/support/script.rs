//! What a scripted server says, and when it says it.
//!
//! A test server that must deliver bytes *after* the client has consumed
//! earlier ones has to wait for that consumption somehow. Sleeping is the
//! usual answer, and it is wrong in both directions: too short and the test
//! fails on a loaded machine, too long and every run pays for the margin.
//! Neither duration is checked against what actually happened.
//!
//! A [`Script`] states the ordering instead of approximating it with time.
//! [`Step::AwaitRead`] blocks until the client has read everything queued
//! before it, so "send the head, let the client parse it, then send the body"
//! becomes an assertion about the client rather than a bet on the scheduler.

use std::time::Duration;

/// One action in a scripted exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    /// Deliver these bytes to the client.
    Send(Vec<u8>),
    /// Block until the client has read every byte sent so far.
    ///
    /// This is the replacement for a sleep. The client reaching this point is
    /// the event being waited on, so the wait is exactly as long as the client
    /// takes and no longer.
    AwaitRead,
    /// Expect a request from the client, and discard its head.
    ///
    /// Reads until the CRLFCRLF that terminates a request head, so a script
    /// can describe a keep-alive connection as request/response pairs.
    ExpectRequest,
    /// Report no data for `.0`, as a silent peer does.
    ///
    /// The client's read timeout must fire during this, so it drives the
    /// silence-budget paths that a merely slow server would not reach.
    Stall(Duration),
    /// Close the connection.
    Close,
}

impl Step {
    /// A [`Step::Send`] from anything byte-shaped.
    pub(crate) fn send(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Send(bytes.into())
    }
}

/// An ordered exchange for a scripted connection.
///
/// Built as a sequence of steps rather than a closure so the exchange can be
/// inspected and asserted on after the fact: [`ScriptedStream`] records how far
/// it advanced, and a test can require that the whole script was consumed.
///
/// [`ScriptedStream`]: super::scripted::ScriptedStream
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Script {
    steps: Vec<Step>,
}

impl Script {
    /// An empty script, to be filled by the builder methods.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Queue bytes for delivery.
    #[must_use]
    pub(crate) fn send(mut self, bytes: impl Into<Vec<u8>>) -> Self {
        self.steps.push(Step::send(bytes));
        self
    }

    /// Wait for the client to consume everything queued so far.
    #[must_use]
    pub(crate) fn await_read(mut self) -> Self {
        self.steps.push(Step::AwaitRead);
        self
    }

    /// Consume a request head from the client.
    #[must_use]
    pub(crate) fn expect_request(mut self) -> Self {
        self.steps.push(Step::ExpectRequest);
        self
    }

    /// Go silent for `dur`, so the client's read timeout ticks.
    #[must_use]
    pub(crate) fn stall(mut self, dur: Duration) -> Self {
        self.steps.push(Step::Stall(dur));
        self
    }

    /// Close the connection.
    #[must_use]
    pub(crate) fn close(mut self) -> Self {
        self.steps.push(Step::Close);
        self
    }

    /// Send bytes, then wait for the client to read them.
    ///
    /// The pairing behind almost every removed sleep: deliver a piece of a
    /// response and do not proceed until it has landed.
    #[must_use]
    pub(crate) fn send_then_await(self, bytes: impl Into<Vec<u8>>) -> Self {
        self.send(bytes).await_read()
    }

    /// The steps, in order.
    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// How many steps this script contains.
    pub(crate) const fn len(&self) -> usize {
        self.steps.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_then_await_expands_to_a_send_and_a_wait() {
        let script = Script::new().send_then_await(b"abc".to_vec());
        assert_eq!(
            script.steps(),
            &[Step::Send(b"abc".to_vec()), Step::AwaitRead]
        );
    }

    #[test]
    fn steps_keep_the_order_they_were_added() {
        let script = Script::new()
            .expect_request()
            .send(b"head".to_vec())
            .await_read()
            .stall(Duration::from_millis(5))
            .send(b"body".to_vec())
            .close();

        assert_eq!(
            script.steps(),
            &[
                Step::ExpectRequest,
                Step::Send(b"head".to_vec()),
                Step::AwaitRead,
                Step::Stall(Duration::from_millis(5)),
                Step::Send(b"body".to_vec()),
                Step::Close,
            ]
        );
    }
}
