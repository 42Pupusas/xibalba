use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// How many requests may be outstanding at once by default: submitted but not
/// yet finished, wherever they currently sit.
pub const DEFAULT_MAX_OUTSTANDING: usize = 64;

/// Bounds how many requests can be in flight across the whole async client.
///
/// The control ring holds only eight slots, but the reader drains it into an
/// unbounded pending queue whenever it polls for a cancel, so ring capacity
/// bounds nothing on its own. Each queued request owns its path, headers,
/// body, and a response ring, so an unbounded queue is unbounded memory.
///
/// Admission is counted here instead: a request occupies a permit from the
/// moment it is submitted until the reader finishes with it, whether it is
/// waiting in the ring, parked in the pending queue, or on the wire.
#[derive(Debug)]
pub(crate) struct Admission {
    outstanding: AtomicUsize,
    max: usize,
}

impl Admission {
    pub(crate) const fn new(max: usize) -> Self {
        Self {
            outstanding: AtomicUsize::new(0),
            max,
        }
    }

    /// Take a permit, or return `None` when the client is already at its
    /// limit. The permit releases itself when dropped.
    pub(crate) fn try_admit(self: &Arc<Self>) -> Option<Permit> {
        let mut current = self.outstanding.load(Ordering::Acquire);
        loop {
            if current >= self.max {
                return None;
            }
            match self.outstanding.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Permit(Arc::clone(self))),
                Err(actual) => current = actual,
            }
        }
    }

    /// How many requests are currently admitted.
    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Acquire)
    }
}

/// One admitted request's slot, released on drop.
///
/// Held by the request itself, so every path that finishes, cancels, aborts,
/// or discards a request releases the slot without a matching manual call.
///
/// This and [`Admission`] refer to each other, which a structural graph
/// reports as a back-edge. It is the shape of an RAII guard rather than a
/// layering mistake: a guard that releases itself must reach what it borrowed
/// from. Breaking it would mean releasing the slot by hand at every exit, and
/// the paths that would have to remember include the ones that are easy to
/// forget — a cancel, a dropped consumer, a request discarded while queued.
#[derive(Debug)]
pub(crate) struct Permit(Arc<Admission>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_are_refused_at_the_limit() {
        let admission = Arc::new(Admission::new(2));
        let first = admission.try_admit().expect("first fits");
        let second = admission.try_admit().expect("second fits");
        assert!(admission.try_admit().is_none(), "third exceeds the limit");
        assert_eq!(admission.outstanding(), 2);
        drop(first);
        drop(second);
    }

    #[test]
    fn dropping_a_permit_frees_a_slot() {
        let admission = Arc::new(Admission::new(1));
        let permit = admission.try_admit().expect("first fits");
        assert!(admission.try_admit().is_none());
        drop(permit);
        assert_eq!(admission.outstanding(), 0);
        assert!(
            admission.try_admit().is_some(),
            "the freed slot must be reusable"
        );
    }

    #[test]
    fn permits_are_counted_across_threads() {
        let admission = Arc::new(Admission::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let admission = Arc::clone(&admission);
            handles.push(std::thread::spawn(move || {
                admission.try_admit().map(|permit| {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    drop(permit);
                })
            }));
        }
        let admitted = handles
            .into_iter()
            .filter_map(|h| h.join().expect("worker thread completes"))
            .count();
        assert_eq!(admitted, 8, "every slot should have been claimable");
        assert_eq!(
            admission.outstanding(),
            0,
            "all permits must have been released"
        );
    }
}
