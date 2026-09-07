//! A hold in a script that the test itself releases.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A point a script waits at until the test opens it.
///
/// [`Step::AwaitRead`] covers waits whose condition the stream can see for
/// itself. Some waits depend on something only the test knows: that a second
/// request has been submitted, or a cancel pushed onto the control ring. Those
/// were sleeps long enough for the test to have got there, which is a guess at
/// the test's own speed.
///
/// A gate names the condition instead. The script holds, the test does the
/// thing, the test opens the gate. The wait is over when the event has
/// happened rather than when a duration chosen in advance runs out.
///
/// What a gate guarantees is that the script *cannot* deliver the bytes after
/// it until the test opens it. That holds whether or not the reader thread
/// happened to arrive at the gate first, so "did the script actually stop
/// here?" is the wrong question to check: the answer depends on thread
/// timing, and a test asserting it would fail on a fast reader for no reason.
///
/// The falsifiable claim is structural instead — that a step waits on this
/// gate at all. Delete the step and the ordering silently reverts to whatever
/// the scheduler does; [`ScriptedConnection::assert_gated_on`] catches that,
/// deterministically and without observing any thread.
///
/// [`Step::AwaitRead`]: super::script::Step::AwaitRead
/// [`ScriptedConnection::assert_gated_on`]:
///     super::registry::ScriptedConnection::assert_gated_on
#[derive(Debug, Clone, Default)]
pub(crate) struct Gate(Arc<AtomicBool>);

impl Gate {
    /// A gate that holds until [`Self::open`] is called.
    pub(crate) fn shut() -> Self {
        Self::default()
    }

    /// Let the script past this point.
    pub(crate) fn open(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether the script may proceed.
    pub(crate) fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Two handles to the same gate are the same gate; two separate gates differ
/// even while both are shut, because a script identifies a gate by which one
/// it is, not by whether it is currently open.
impl PartialEq for Gate {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Gate {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gate_starts_shut_and_opens_once() {
        let gate = Gate::shut();
        assert!(!gate.is_open());
        gate.open();
        assert!(gate.is_open());
    }

    /// Identity survives opening: a script must still recognise its own gate
    /// after the test has released it.
    #[test]
    fn an_opened_gate_is_still_the_same_gate() {
        let gate = Gate::shut();
        let script_side = gate.clone();
        gate.open();
        assert_eq!(script_side, gate);
    }

    #[test]
    fn a_clone_shares_the_original_state() {
        let gate = Gate::shut();
        let held_by_script = gate.clone();
        gate.open();
        assert!(held_by_script.is_open());
    }

    #[test]
    fn separate_gates_are_distinct_even_while_both_are_shut() {
        assert_ne!(Gate::shut(), Gate::shut());
        let gate = Gate::shut();
        assert_eq!(gate.clone(), gate);
    }
}
