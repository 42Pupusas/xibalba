//! What travels on the reader's control channel, and the queue that owns it.
//!
//! The channel multiplexes new requests and cancels, so reading it is not a
//! plain receive: hunting for a cancel means popping requests that must not
//! be lost. That pairing — a consumer plus the queue of requests displaced
//! from it — is the invariant this module owns.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use quetzalcoatl::mpsc::Consumer as MpscConsumer;
use quetzalcoatl::spsc;
use xibalba_proto::method::Method;

use crate::admission::Permit;
use crate::async_client::Chunk;
use crate::delivery::ChunkSink;
use crate::interrupt::Interrupt;

/// Ticket value meaning "cancel whatever request is in flight", used by
/// [`AsyncClient::cancel`](crate::AsyncClient::cancel) where the caller has
/// no specific handle.
///
/// Every other cancel names the exact request it belongs to. That
/// distinction is load-bearing: the control ring is shared by every
/// request, so an unscoped cancel that arrives *after* its intended
/// request already finished would otherwise be applied to whichever
/// request happens to be streaming next, aborting a perfectly healthy
/// response. Tickets make a late cancel a no-op instead.
pub(crate) const CANCEL_ANY: u64 = u64::MAX;

/// One streaming request handed to the reader thread. The reader
/// takes ownership of the request buffers and the chunk-ring
/// producer.
pub(crate) struct AsyncRequest {
    pub(crate) method: Method,
    pub(crate) path: Vec<u8>,
    pub(crate) query: Option<Vec<u8>>,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub(crate) chunk_tx: spsc::Producer<Chunk>,
    /// Cleared when the caller drops its side of the chunk ring, so the
    /// reader can stop producing instead of waiting for capacity that
    /// nobody will free.
    pub(crate) consumer_alive: Arc<AtomicBool>,
    /// Identifies this request on the shared control ring so a cancel
    /// can name it precisely.
    pub(crate) ticket: u64,
    /// Flipped by the reader when it takes this request off the queue.
    /// Shared with the caller's [`StreamHandle`](crate::StreamHandle) so a
    /// response-head deadline can measure time on the wire rather than time
    /// spent queued behind an earlier request.
    pub(crate) started: Arc<AtomicBool>,
    /// Frees this request's admission slot when the request is dropped,
    /// whether it completed, was cancelled, or was discarded in the queue.
    pub(crate) _permit: Permit,
}

impl std::fmt::Debug for AsyncRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("body_len", &self.body.as_ref().map(Vec::len))
            .field("headers_len", &self.headers.len())
            .field("ticket", &self.ticket)
            .finish_non_exhaustive()
    }
}

/// Control message sent from the caller to the reader thread.
/// All coordination goes through this single MPSC ring: new
/// requests and cancel signals share the same channel.
#[derive(Debug)]
pub(crate) enum Control {
    /// Start a new streaming request.
    Request(AsyncRequest),
    /// Cancel the request with this ticket, or any in-flight request
    /// when the ticket is [`CANCEL_ANY`]. A cancel naming a request that
    /// has already finished is discarded rather than applied to its
    /// successor.
    Cancel(u64),
}

/// The reader's end of the control ring, plus the requests displaced from it.
///
/// The consumer has no non-destructive peek, so checking for a cancel must
/// `pop`, and a `Request` popped while hunting for a cancel would be lost if
/// it were merely dropped. Holding both halves together is what makes that
/// safe: displaced requests land in `pending` and are served before the ring.
pub(crate) struct ControlQueue {
    rx: MpscConsumer<Control>,
    pending: VecDeque<AsyncRequest>,
    shutting_down: Arc<AtomicBool>,
}

impl ControlQueue {
    pub(crate) const fn new(rx: MpscConsumer<Control>, shutting_down: Arc<AtomicBool>) -> Self {
        Self {
            rx,
            pending: VecDeque::new(),
            shutting_down,
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// A handle on the shutdown flag, for building a [`ChunkSink`] that
    /// outlives a borrow of this queue.
    pub(crate) fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutting_down)
    }

    /// The next request to serve, or `None` once the reader should stop.
    ///
    /// Requests displaced into `pending` come first, so submission order
    /// survives the destructive cancel polling. A cancel arriving with
    /// nothing in flight has no target and is discarded.
    pub(crate) fn next_request(&mut self) -> Option<AsyncRequest> {
        loop {
            if self.is_shutting_down() {
                return None;
            }
            if let Some(request) = self.pending.pop_front() {
                return Some(request);
            }
            match self.rx.pop_block()? {
                Control::Request(request) => return Some(request),
                Control::Cancel(_) => {}
            }
        }
    }

    /// Drain the ring without blocking, reporting whether a cancel addressed
    /// to `current` was seen.
    ///
    /// Cancels are matched against the in-flight request's ticket. A cancel
    /// naming some *other* request is dropped: its target already finished,
    /// and applying it to the current request would abort a healthy response
    /// because an unrelated one timed out. [`CANCEL_ANY`] always matches, and
    /// `current == None` means nothing is in flight to cancel.
    ///
    /// A cancel naming a request still waiting in `pending` retires that
    /// request here, so it never reaches the wire.
    pub(crate) fn poll_cancel(&mut self, current: Option<u64>) -> bool {
        let mut cancelled = false;
        loop {
            if self.is_shutting_down() {
                return true;
            }
            match self.rx.pop() {
                Some(Control::Cancel(ticket)) => {
                    if current.is_some_and(|c| ticket == CANCEL_ANY || ticket == c) {
                        cancelled = true;
                    } else if ticket != CANCEL_ANY {
                        self.abort_pending(ticket);
                    }
                }
                Some(Control::Request(request)) => self.pending.push_back(request),
                None => return cancelled,
            }
        }
    }

    fn abort_pending(&mut self, ticket: u64) {
        let Some(index) = self
            .pending
            .iter()
            .position(|request| request.ticket == ticket)
        else {
            return;
        };
        let request = self
            .pending
            .remove(index)
            .expect("pending index came from the same queue");
        ChunkSink::new(
            &request.chunk_tx,
            &request.consumer_alive,
            &self.shutting_down,
        )
        .send_terminal(Chunk::Aborted);
    }
}

/// Answers "should this request stop?" from the control channel, so the
/// client can consult it during writes and head reads without knowing the
/// channel exists.
///
/// It borrows the control queue, which is disjoint from the `Client` that
/// owns the stream — that is what lets a single request's write, head read,
/// and stale-connection retry all be covered.
pub(crate) struct ControlInterrupt<'a> {
    queue: &'a mut ControlQueue,
    ticket: u64,
}

impl<'a> ControlInterrupt<'a> {
    /// Scope `queue` to one request.
    ///
    /// Built here rather than handed out by [`ControlQueue`] so the
    /// dependency runs one way: the interrupt knows the queue, the queue
    /// knows nothing of the interrupt.
    pub(crate) const fn new(queue: &'a mut ControlQueue, ticket: u64) -> Self {
        Self { queue, ticket }
    }
}

impl Interrupt for ControlInterrupt<'_> {
    fn is_cancelled(&mut self) -> bool {
        self.queue.poll_cancel(Some(self.ticket))
    }
}
