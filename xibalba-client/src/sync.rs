//! The concurrency primitives the client's shared state is built from.
//!
//! Every atomic and thread handle in this crate comes from here rather than
//! from `std` directly, because [`loom`](https://docs.rs/loom) can only check
//! code that uses *its* primitives: it replaces them with instrumented
//! versions and then runs the test body once per thread interleaving. A single
//! re-export keeps that swap to one place instead of a `cfg` on every `use`.
//!
//! Under `--cfg loom` this is loom's model; otherwise it is plain `std` and
//! compiles to exactly what it replaced.
//!
//! Only the *shared state* is swapped, not everything with a `sync` flavour.
//! `Chunk::Error` keeps `std::sync::Arc` because it is a refcount for handing
//! one error to the caller and guards nothing, and the reader keeps
//! `std::thread` because it owns a socket no model checker can re-run. Routing
//! those through here would not check more, it would only fail to compile.
//!
//! # What this can and cannot check
//!
//! Loom explores the interleavings of the atomics it can see. It cannot see
//! inside `quetzalcoatl`'s rings, which use `std` atomics of their own, so a
//! model-checked result here is a statement about *this crate's* shared state
//! — admission counting and the delivery flag protocol — and not about the
//! ring buffers underneath. Claiming otherwise would be the more dangerous
//! error, since a green loom run reads like a proof of the whole system.

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

#[cfg(test)]
mod tests {
    /// The shim must not drift into being a partial `std` re-export that some
    /// modules use and others bypass. Every atomic in the crate's shared state
    /// comes from here; this pins that the names exist under both cfgs, so a
    /// non-loom build fails at compile time rather than at campaign time.
    #[test]
    fn the_shim_exports_the_primitives_the_shared_state_uses() {
        use super::{Arc, AtomicBool, AtomicUsize, Ordering};

        let flag = Arc::new(AtomicBool::new(false));
        flag.store(true, Ordering::Release);
        assert!(flag.load(Ordering::Acquire));

        let count = AtomicUsize::new(0);
        count.fetch_add(1, Ordering::AcqRel);
        assert_eq!(count.load(Ordering::Acquire), 1);
    }
}
