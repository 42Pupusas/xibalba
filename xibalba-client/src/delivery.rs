use std::time::Duration;

use crate::sync::{Arc, AtomicBool, Ordering};

use quetzalcoatl::spsc::{Consumer, Producer};

use crate::async_client::Chunk;

/// Yields before falling back to sleeping while the ring stays full.
const SPIN_LIMIT: u32 = 128;

/// How often a blocked send re-checks shutdown and consumer liveness once
/// spinning has not freed a slot. Also the upper bound on how long shutdown
/// can go unnoticed by a reader waiting for capacity.
const FULL_RING_POLL: Duration = Duration::from_millis(1);

/// Why a chunk could not be handed to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Undelivered {
    /// The caller dropped its handle; nobody will read the rest.
    ConsumerGone,
    /// The client is shutting down and the ring never freed a slot.
    ShuttingDown,
}

/// Tracks whether the caller still holds the receiving end of a chunk ring.
///
/// `quetzalcoatl`'s producer reports a dropped consumer only through
/// `push_block`, which parks until a slot frees. The reader cannot afford to
/// park, so liveness is published here instead: the guard travels with the
/// consumer and clears the flag when that consumer is dropped.
#[derive(Debug)]
pub(crate) struct ConsumerGuard(Arc<AtomicBool>);

impl ConsumerGuard {
    pub(crate) fn new() -> (Self, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(true));
        (Self(Arc::clone(&flag)), flag)
    }
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// The receiving end of one request's chunk ring.
///
/// Returned by [`StreamHandle::into_stream`](crate::StreamHandle::into_stream)
/// so a caller can park on the ring directly. It owns an internal
/// `ConsumerGuard`, so dropping it tells the reader to stop producing rather
/// than leaving the reader waiting for capacity on a ring nobody will drain.
pub struct ChunkStream {
    rx: Consumer<Chunk>,
    _guard: ConsumerGuard,
}

impl std::fmt::Debug for ChunkStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStream").finish_non_exhaustive()
    }
}

impl ChunkStream {
    pub(crate) const fn new(rx: Consumer<Chunk>, guard: ConsumerGuard) -> Self {
        Self { rx, _guard: guard }
    }

    /// The next chunk, or `None` when none is buffered.
    pub fn try_next(&mut self) -> Option<Chunk> {
        self.rx.pop()
    }

    /// Park until a chunk arrives, or return `None` once the reader closes
    /// the ring.
    pub fn next_block(&mut self) -> Option<Chunk> {
        self.rx.pop_block()
    }
}

/// Hands response chunks to the caller without ever parking indefinitely.
///
/// `Producer::push_block` parks until the consumer frees a slot. A caller that
/// holds a handle and stops reading therefore wedges the reader thread: it is
/// no longer polling the shutdown flag, so `AsyncClient::drop` waits on a
/// `join` that cannot finish. This retries a non-blocking push and gives up as
/// soon as either shutdown is signalled or the consumer goes away.
pub(crate) struct ChunkSink<'a> {
    tx: &'a Producer<Chunk>,
    consumer_alive: &'a AtomicBool,
    cancelled: &'a AtomicBool,
    cancel_any: &'a AtomicBool,
    shutting_down: &'a AtomicBool,
}

impl<'a> ChunkSink<'a> {
    pub(crate) const fn new(
        tx: &'a Producer<Chunk>,
        consumer_alive: &'a AtomicBool,
        cancelled: &'a AtomicBool,
        cancel_any: &'a AtomicBool,
        shutting_down: &'a AtomicBool,
    ) -> Self {
        Self {
            tx,
            consumer_alive,
            cancelled,
            cancel_any,
            shutting_down,
        }
    }

    /// Why this sink must stop waiting for ring capacity, or `None` while it
    /// may keep trying.
    ///
    /// This is the whole exit condition of [`send`](Self::send)'s wait loop,
    /// named so it can be checked on its own. Both loads are `Acquire` and
    /// pair with the `Release` in [`ConsumerGuard::drop`] and in
    /// `AsyncClient::begin_shutdown`: without that pairing a reader could
    /// keep waiting on a ring whose consumer is provably gone.
    ///
    /// Shutdown is checked first. A client shutting down is stopping every
    /// request, so reporting a dropped consumer instead would be true but
    /// less useful, and the two race by nature — `Drop` sets the flag and
    /// drops handles.
    pub(crate) fn blocked_reason(&self, observe_cancel: bool) -> Option<Undelivered> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Some(Undelivered::ShuttingDown);
        }
        if observe_cancel
            && (self.cancelled.load(Ordering::Acquire) || self.cancel_any.load(Ordering::Acquire))
        {
            return Some(Undelivered::ConsumerGone);
        }
        if !self.consumer_alive.load(Ordering::Acquire) {
            return Some(Undelivered::ConsumerGone);
        }
        None
    }

    /// Deliver `chunk`, waiting for ring capacity while the client is live.
    ///
    /// # Errors
    ///
    /// Returns [`Undelivered::ConsumerGone`] when the handle was dropped and
    /// [`Undelivered::ShuttingDown`] when shutdown began before a slot freed.
    pub(crate) fn send(&self, chunk: Chunk) -> Result<(), Undelivered> {
        self.send_inner(chunk, true)
    }

    fn send_inner(&self, chunk: Chunk, observe_cancel: bool) -> Result<(), Undelivered> {
        let mut pending = chunk;
        let mut spins = 0u32;
        loop {
            if let Some(reason) = self.blocked_reason(observe_cancel) {
                return Err(reason);
            }
            match self.tx.push(pending) {
                Ok(()) => return Ok(()),
                Err(returned) => pending = returned,
            }
            // A consumer that reads slowly can keep the ring full for a long
            // time. Spinning throughout would burn a core, and parking would
            // reintroduce the unbounded wait this type exists to avoid, so
            // back off to a short sleep and keep re-checking both flags.
            if spins < SPIN_LIMIT {
                spins += 1;
                std::thread::yield_now();
            } else {
                std::thread::sleep(FULL_RING_POLL);
            }
        }
    }

    /// Deliver a terminal chunk, where failure is not actionable: the caller
    /// is either gone or shutting down.
    pub(crate) fn send_terminal(&self, chunk: Chunk) {
        let _ = self.send_inner(chunk, false);
    }
}
