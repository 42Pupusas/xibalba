//! Background-reader HTTP/1.1 client.
//!
//! Wraps a synchronous [`Client`] and runs it on a dedicated reader
//! thread. The public side is an [`AsyncClient`] (a request
//! producer + the reader-thread join handle) and, per request, a
//! [`StreamHandle`] that yields the response as a sequence of
//! [`Chunk`]s.
//!
//! # Why a reader thread?
//!
//! The synchronous `Client` borrows its stream for the lifetime of a
//! streaming response, so the caller can neither observe a cancel
//! signal mid-read nor share the stream with another thread. A
//! dedicated reader fixes both: it owns the stream, and all
//! communication with the caller goes through quetzalcoatl channels.
//!
//! # Topology
//!
//! - `mpsc::RingBuffer<Control>`: caller → reader. Carries new requests
//!   and cancel signals; owned on the reader side by the internal
//!   `ControlQueue`.
//! - `spsc::RingBuffer<Chunk>`: per-request, reader → caller. The
//!   reader pushes a single [`Chunk::Head`] first, then zero or more
//!   [`Chunk::Body`] chunks, then exactly one terminator:
//!   [`Chunk::Eof`] (clean end), [`Chunk::Error`] (read failure), or
//!   [`Chunk::Aborted`] (caller cancelled).
//!
//! No `Mutex`: request and response data travel only on those rings.
//! Shared flags are `Arc<AtomicBool>` — shutdown, consumer liveness, and
//! whether a request has left the queue — because each is a one-way latch
//! that must be readable while the reader is parked in a socket read, which
//! is exactly when it cannot be servicing a ring.
//!
//! # Read timeout
//!
//! The reader uses the [`Client`]'s configured `read_timeout` for
//! every socket read. To bound the worst-case cancel latency, set a
//! short read timeout (e.g. 1s) when constructing the client; the
//! cancel message is then observed on the next read attempt, at most
//! one `read_timeout` later.
//!
//! # One thread per client
//!
//! `AsyncClient` is `!Clone` — exactly one reader thread per
//! `AsyncClient`. The API client crates (deepseek, kimi, …) own
//! their `AsyncClient` directly, and the worker thread drives it
//! via `&mut`. If two callers ever need to share, the user can
//! wrap the `AsyncClient` in `Arc<Mutex<_>>` at a higher layer —
//! but the in-tree design keeps it single-owner.

// The reader runs on a real OS thread. Loom models only the flags and
// counters it shares with the caller, never `AsyncClient` itself, which owns a
// socket the model checker cannot re-run.
use std::thread::{self, JoinHandle};

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::{self, Producer as MpscProducer};
use quetzalcoatl::spsc::{self, Consumer as SpscConsumer};
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::method::Method;

use crate::admission::{Admission, DEFAULT_MAX_OUTSTANDING};
use crate::client::{Client, Config, DEFAULT_MAX_HEAD_SIZE};
use crate::control::{AsyncRequest, CANCEL_ANY, Control, ControlQueue};
use crate::delivery::{ChunkStream, ConsumerGuard};
use crate::params::RequestParams;
use crate::reader::ReaderWorker;
use crate::sync::{Arc, AtomicBool, AtomicU64, Ordering};

/// Default capacity for the per-request chunk ring: a few SSE
/// events worth of buffering. The ring parks the caller on full
/// and the reader on empty, so the size only affects memory and
/// wakeup latency, not correctness.
const CHUNK_RING_CAP: usize = 32;

/// Capacity of the control ring feeding the reader thread.
///
/// Sized to hold every admissible request plus room for cancels, so
/// [`Admission`] is the only thing that refuses a submission. A ring smaller
/// than the admission bound would make `submit` report backpressure while the
/// reader had merely not drained yet, and blocking instead would stall the
/// caller for as long as the reader stayed busy.
const CONTROL_RING_CAP: usize = DEFAULT_MAX_OUTSTANDING * 2;

/// One response chunk delivered from the reader thread to the
/// caller.
///
/// The ring is per-request: the reader pushes a single `Head`,
/// then `Body` chunks, then exactly one terminator (`Eof`,
/// `Error`, or `Aborted`).
#[derive(Debug, PartialEq, Eq)]
pub enum Chunk {
    /// The response head was read; the status and headers are
    /// ready. Always the first chunk for a successful head read.
    Head {
        /// HTTP status code, e.g. `200`.
        status: u16,
        /// Header name/value pairs in the order they appeared.
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// A piece of response body. Multiple `Body` chunks arrive in
    /// order; each is a `Vec<u8>` of bytes the reader pulled from
    /// the socket this round.
    Body(Vec<u8>),
    /// The response ended cleanly. After `Eof` the handle yields
    /// no more chunks.
    Eof,
    /// A read or write failed. The error matches the shape
    /// `Client` would have returned to a synchronous caller.
    Error(std::sync::Arc<Error>),
    /// The caller cancelled the request; the reader stopped
    /// reading and dropped the in-flight response. Distinct from
    /// `Error` so the caller can branch user-cancel vs. genuine
    /// failure.
    Aborted,
}

/// The caller-facing side of one in-flight request: just the chunk
/// consumer.
///
/// Dropping the handle does *not* cancel the request; use
/// [`AsyncClient::cancel`] or the handle's [`cancel`](Self::cancel)
/// method.
pub struct StreamHandle {
    chunk_rx: SpscConsumer<Chunk>,
    /// Clears the request's `consumer_alive` flag when this handle is
    /// dropped. Held for its `Drop`, never read directly.
    guard: ConsumerGuard,
    control_tx: MpscProducer<Control>,
    /// This request's ticket, so [`cancel`](Self::cancel) targets only
    /// this request and never a successor that reused the reader.
    ticket: u64,
    /// Set by the reader once this request leaves the queue and reaches
    /// the socket. See [`has_started`](Self::has_started).
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl StreamHandle {
    /// Signal the reader to abandon this request. The next request write,
    /// response-head read, or body read observes the cancel, pushes
    /// [`Chunk::Aborted`], and the handle yields no more chunks.
    ///
    /// How soon that happens depends on where the request is:
    ///
    /// | Stage | Observed within |
    /// |-------|-----------------|
    /// | Queued, not yet written | immediately, before reaching the wire |
    /// | Request write | one `write_timeout` tick |
    /// | Response head | one `read_timeout` tick |
    /// | Response body | one `read_timeout` tick |
    /// | Connect / TLS handshake | the remaining `connect_timeout` |
    ///
    /// The write bound holds only for connectors implementing
    /// [`set_write_timeout`](crate::connector::SetReadTimeout::set_write_timeout).
    ///
    /// Connecting is the one stage a cancel does not shorten. It runs inside
    /// `Connector::connect`, where there is no stream yet and so no point
    /// between I/O calls at which the reader could notice anything; the cancel
    /// is seen when the connect returns. What bounds it is
    /// [`Config::connect_timeout`](crate::config::Config::connect_timeout),
    /// and only for a connector that honours the deadline it is given — see
    /// [`Connector::connect`](crate::connector::Connector::connect) for that
    /// contract. A reconnect between requests is the usual way to be here.
    ///
    /// Idempotent: sending a second cancel is a no-op.
    ///
    /// # Errors
    ///
    /// Returns `Error::Connection` if the reader thread has already
    /// exited.
    pub fn cancel(&self) -> Result<(), Error> {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.control_tx.push(Control::Cancel(self.ticket));
        Ok(())
    }

    /// Whether the reader has begun processing this request.
    ///
    /// Requests serialize through a single reader thread, so a submitted
    /// request may sit in the control ring for as long as its
    /// predecessor takes. A caller enforcing its own response-head
    /// deadline must not start that clock at submit time — the request
    /// has not been written yet, and timing out here reports a transport
    /// failure for bytes that were never sent. Gate the deadline on this
    /// flag and it measures the peer's silence instead of the queue's.
    #[must_use]
    pub fn has_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    /// The next chunk, or `None` if the ring is empty. The caller
    /// drives the iteration; if it needs to park until a chunk
    /// arrives, use [`next_block`](Self::next_block).
    pub fn try_next(&mut self) -> Option<Chunk> {
        self.chunk_rx.pop()
    }

    /// Park until a chunk is available (or the reader closes the
    /// ring, in which case returns `None`).
    pub fn next_block(&mut self) -> Option<Chunk> {
        self.chunk_rx.pop_block()
    }

    /// Take the chunk stream out of the handle. The caller can then park on
    /// it directly — useful when integrating with an existing event loop.
    ///
    /// The returned stream carries the same liveness guard as the handle, so
    /// dropping it still releases the reader.
    #[must_use]
    pub fn into_stream(self) -> ChunkStream {
        ChunkStream::new(self.chunk_rx, self.guard)
    }
}

/// The background-reader client. Owns the reader thread directly;
/// `Drop` closes the control ring and joins the thread.
pub struct AsyncClient<const MAX_HEAD_SIZE: usize = DEFAULT_MAX_HEAD_SIZE> {
    /// Wrapped in `Option` so `Drop` can take it out and close the
    /// control ring *before* joining the reader thread. The
    /// reader parks in `pop_block` on the consumer; with the
    /// producer still alive, that pop would block forever, and so
    /// would `join`. (Rust's `Drop` runs user code first, then
    /// drops fields in declaration order — leaving the field
    /// alive across `join.join()` would deadlock the drop itself.)
    control_tx: Option<MpscProducer<Control>>,
    join: Option<JoinHandle<()>>,
    /// Source of per-request tickets. Monotonic; the only reserved value
    /// is [`CANCEL_ANY`], which the counter cannot reach in practice.
    next_ticket: AtomicU64,
    /// Set by `Drop` before the control ring closes. Closing the ring
    /// alone cannot interrupt a read already parked on the socket — the
    /// ring-closed signal only reaches the reader between reads. The
    /// flag is what turns the next `WouldBlock` retry into an immediate
    /// shutdown, so `drop` cannot block for `stream_silence` behind a
    /// stalled in-flight response.
    shutting_down: Arc<AtomicBool>,
    cancel_any: Arc<AtomicBool>,
    /// Bounds requests submitted but not yet finished. The reader drains the
    /// control ring into an unbounded pending queue, so ring capacity alone
    /// does not limit how many requests (and their buffers) can pile up.
    admission: Arc<Admission>,
}

impl<const MAX_HEAD_SIZE: usize> AsyncClient<MAX_HEAD_SIZE> {
    fn control(&self) -> Result<&MpscProducer<Control>, Error> {
        self.control_tx
            .as_ref()
            .ok_or(Error::Connection(ConnectionError::ReaderGone))
    }

    /// Open a connection and spawn the reader thread. The reader
    /// owns the [`Client`] and its stream for the thread's
    /// lifetime.
    ///
    /// # Errors
    /// Returns `Error` if the connection cannot be established or
    /// the thread cannot be spawned.
    pub fn connect<C>(url: &[u8], tls_config: C::TlsConfig, config: Config) -> Result<Self, Error>
    where
        C: crate::connector::Connector + Send + 'static,
        C::Stream: Send + 'static,
        C::TlsConfig: Send + 'static,
    {
        let client = Client::<C, MAX_HEAD_SIZE>::connect(url, tls_config, config)?;

        let (control_tx, control_rx) =
            mpsc::RingBuffer::<Control>::new(Capacity::at_least(CONTROL_RING_CAP)).split();

        let shutting_down = Arc::new(AtomicBool::new(false));
        let cancel_any = Arc::new(AtomicBool::new(false));
        let queue = ControlQueue::new(
            control_rx,
            Arc::clone(&shutting_down),
            Arc::clone(&cancel_any),
        );
        let join = thread::Builder::new()
            .name("xibalba-reader".to_owned())
            .spawn(move || {
                ReaderWorker::new(client, queue).run();
            })
            .map_err(|e| {
                Error::Connection(ConnectionError::Other(format!(
                    "failed to spawn reader thread: {e}"
                )))
            })?;

        Ok(Self {
            control_tx: Some(control_tx),
            join: Some(join),
            next_ticket: AtomicU64::new(0),
            shutting_down,
            cancel_any,
            admission: Arc::new(Admission::new(DEFAULT_MAX_OUTSTANDING)),
        })
    }

    /// How many submitted requests have not finished yet.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.admission.outstanding()
    }

    /// Submit a streaming request and get back a handle. The
    /// reader processes requests in submission order, so two
    /// outstanding requests from the same client serialize.
    ///
    /// The handle's chunk ring is created here (one per call); the
    /// producer half travels with the request to the reader.
    ///
    /// # Errors
    /// Returns `Error::Serialize(DuplicateHeader)` when `headers`
    /// contains a header the client serializes itself (`Host`,
    /// `Content-Length`, `Transfer-Encoding`) or the same name twice;
    /// `Error::Connection` if the reader thread has already exited;
    /// [`ConnectionError::TooManyRequests`] when
    /// [`DEFAULT_MAX_OUTSTANDING`] requests are already in flight. That
    /// last case is backpressure, not a transport failure: retry once an
    /// earlier response completes.
    pub fn submit(
        &self,
        method: Method,
        path: Vec<u8>,
        query: Option<Vec<u8>>,
        body: Option<Vec<u8>>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<StreamHandle, Error> {
        RequestParams::validate_extra_headers(&headers)?;
        let permit = Admission::try_admit(&self.admission)
            .ok_or(Error::Connection(ConnectionError::TooManyRequests))?;
        let (chunk_tx, chunk_rx) =
            spsc::RingBuffer::<Chunk>::new(Capacity::at_least(CHUNK_RING_CAP)).split();
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let started = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let (guard, consumer_alive) = ConsumerGuard::new();
        let request = AsyncRequest {
            method,
            path,
            query,
            body,
            headers,
            chunk_tx,
            consumer_alive,
            cancelled: Arc::clone(&cancelled),
            ticket,
            started: Arc::clone(&started),
            _permit: permit,
        };

        let control_tx = self.control()?;
        // Non-blocking: `push_block` parks until the reader drains a slot,
        // which turns a busy reader into an unbounded stall inside submit and
        // makes the admission bound unreachable (the ring is far smaller).
        // A full ring is backpressure and is reported as such.
        control_tx
            .push(Control::Request(request))
            .map_err(|_| Error::Connection(ConnectionError::TooManyRequests))?;
        Ok(StreamHandle {
            chunk_rx,
            guard,
            control_tx: control_tx.clone(),
            ticket,
            started,
            cancelled,
        })
    }

    /// Cancel the currently in-flight request, if any. Has no effect
    /// if no request is in flight or if the current request has
    /// already finished.
    ///
    /// # Errors
    /// Returns `Error::Connection` if the reader thread has already
    /// exited.
    pub fn cancel(&self) -> Result<(), Error> {
        self.cancel_any.store(true, Ordering::Release);
        let _ = self.control()?.push(Control::Cancel(CANCEL_ANY));
        Ok(())
    }
}

impl<const MAX_HEAD_SIZE: usize> Drop for AsyncClient<MAX_HEAD_SIZE> {
    fn drop(&mut self) {
        // Flag shutdown before closing the ring: a reader parked in a
        // socket read never sees the ring close, but its next WouldBlock
        // retry sees the flag and unwinds immediately. Without this the
        // join below would wait out the full `stream_silence` budget
        // behind a stalled in-flight response.
        self.shutting_down.store(true, Ordering::Release);

        // Handles clone the producer, so dropping this client's producer
        // does not necessarily close the ring. Push a wakeup after setting
        // the flag: an idle reader leaves pop_block, and an active reader's
        // next control poll interrupts its socket read.
        if let Some(control_tx) = &self.control_tx {
            let _ = control_tx.push_block(Control::Cancel(CANCEL_ANY));
        }
        drop(self.control_tx.take());

        // The flag above bounds this join to one read-timeout tick while the
        // reader waits on a head or body, and one write-timeout tick while it
        // writes a request. It is *not* bounded when the reader is inside
        // `Connector::connect`: a stuck DNS lookup, TCP connect, or TLS
        // handshake is beyond this flag's reach and drop waits for it.
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl<const MAX_HEAD_SIZE: usize> std::fmt::Debug for AsyncClient<MAX_HEAD_SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncClient").finish_non_exhaustive()
    }
}
