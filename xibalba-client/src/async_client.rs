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
//! - `mpsc::RingBuffer<Control>`: caller → reader. The only shared
//!   state; carries new requests and cancel signals.
//! - `spsc::RingBuffer<Chunk>`: per-request, reader → caller. The
//!   reader pushes a single [`Chunk::Head`] first, then zero or more
//!   [`Chunk::Body`] chunks, then exactly one terminator:
//!   [`Chunk::Eof`] (clean end), [`Chunk::Error`] (read failure), or
//!   [`Chunk::Aborted`] (caller cancelled).
//!
//! No `Mutex`, no `Arc<AtomicBool>` — only quetzalcoatl ring buffers.
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

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::{self, Consumer as MpscConsumer, Producer as MpscProducer};
use quetzalcoatl::spsc::{self, Consumer as SpscConsumer};
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::method::Method;

use crate::admission::{Admission, DEFAULT_MAX_OUTSTANDING, Permit};
use crate::body::{BodyCollector, StreamingBody};
use crate::client::{Client, Config, DEFAULT_MAX_HEAD_SIZE};
use crate::config::HEAD_BUF_SIZE;
use crate::delivery::{ChunkSink, ChunkStream, ConsumerGuard};
use crate::params::RequestParams;

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

/// Ticket value meaning "cancel whatever request is in flight", used by
/// [`AsyncClient::cancel`] where the caller has no specific handle.
///
/// Every other cancel names the exact request it belongs to. That
/// distinction is load-bearing: the control ring is shared by every
/// request, so an unscoped cancel that arrives *after* its intended
/// request already finished would otherwise be applied to whichever
/// request happens to be streaming next, aborting a perfectly healthy
/// response. Tickets make a late cancel a no-op instead.
const CANCEL_ANY: u64 = u64::MAX;

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

/// Control message sent from the caller to the reader thread.
/// All coordination goes through this single MPSC ring: new
/// requests and cancel signals share the same channel.
#[derive(Debug)]
enum Control {
    /// Start a new streaming request.
    Request(AsyncRequest),
    /// Cancel the request with this ticket, or any in-flight request
    /// when the ticket is [`CANCEL_ANY`]. A cancel naming a request that
    /// has already finished is discarded rather than applied to its
    /// successor.
    Cancel(u64),
}

/// One streaming request handed to the reader thread. The reader
/// takes ownership of the request buffers and the chunk-ring
/// producer.
pub struct AsyncRequest {
    method: Method,
    path: Vec<u8>,
    query: Option<Vec<u8>>,
    body: Option<Vec<u8>>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    chunk_tx: spsc::Producer<Chunk>,
    /// Cleared when the caller drops its side of the chunk ring, so the
    /// reader can stop producing instead of waiting for capacity that
    /// nobody will free.
    consumer_alive: Arc<AtomicBool>,
    /// Identifies this request on the shared control ring so a cancel
    /// can name it precisely.
    ticket: u64,
    /// Flipped by the reader when it takes this request off the queue.
    /// Shared with the caller's [`StreamHandle`] so a response-head
    /// deadline can measure time on the wire rather than time spent
    /// queued behind an earlier request.
    started: Arc<AtomicBool>,
    /// Frees this request's admission slot when the request is dropped,
    /// whether it completed, was cancelled, or was discarded in the queue.
    _permit: Permit,
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
}

impl StreamHandle {
    /// Signal the reader to abandon this response. The next body read
    /// observes the cancel message, pushes [`Chunk::Aborted`], and the
    /// handle yields no more chunks.
    ///
    /// Idempotent: sending a second cancel is a no-op.
    ///
    /// # Errors
    ///
    /// Returns `Error::Connection` if the reader thread has already
    /// exited.
    pub fn cancel(&self) -> Result<(), Error> {
        self.control_tx
            .push_block(Control::Cancel(self.ticket))
            .map_err(|_| Error::Connection(ConnectionError::Other("reader is gone".into())))
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
        let reader_shutting_down = Arc::clone(&shutting_down);
        let join = thread::Builder::new()
            .name("xibalba-reader".to_owned())
            .spawn(move || {
                run_reader(client, control_rx, &reader_shutting_down);
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
        let permit = self
            .admission
            .try_admit()
            .ok_or(Error::Connection(ConnectionError::TooManyRequests))?;
        let (chunk_tx, chunk_rx) =
            spsc::RingBuffer::<Chunk>::new(Capacity::at_least(CHUNK_RING_CAP)).split();
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let started = Arc::new(AtomicBool::new(false));
        let (guard, consumer_alive) = ConsumerGuard::new();
        let request = AsyncRequest {
            method,
            path,
            query,
            body,
            headers,
            chunk_tx,
            consumer_alive,
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
        self.control()?
            .push_block(Control::Cancel(CANCEL_ANY))
            .map_err(|_| Error::Connection(ConnectionError::ReaderGone))
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

        // Best-effort join: the flag above bounds the wait to at most one
        // read-timeout window; if the reader is wedged below the kernel's
        // cancel reach (e.g. a stuck TLS handshake), we don't hang the
        // drop indefinitely.
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

/// The reader thread's main loop: pop a control message, process
/// requests, honor cancels. Errors and aborts are delivered as
/// terminator chunks.
fn run_reader<C, const MAX_HEAD_SIZE: usize>(
    mut client: Client<C, MAX_HEAD_SIZE>,
    mut control_rx: MpscConsumer<Control>,
    shutting_down: &AtomicBool,
) where
    C: crate::connector::Connector,
{
    // Requests popped off the control ring while polling for a cancel
    // (during the start-of-request drain or mid-stream) are stashed here
    // rather than dropped, then processed in submission order.
    let mut pending: VecDeque<AsyncRequest> = VecDeque::new();
    loop {
        if shutting_down.load(Ordering::Acquire) {
            break;
        }
        if let Some(request) = pending.pop_front() {
            process_request(
                &mut client,
                request,
                &mut control_rx,
                &mut pending,
                shutting_down,
            );
            continue;
        }
        match control_rx.pop_block() {
            Some(Control::Request(request)) => {
                process_request(
                    &mut client,
                    request,
                    &mut control_rx,
                    &mut pending,
                    shutting_down,
                );
            }
            Some(Control::Cancel(_)) => {
                // No request in flight; cancel is a no-op.
            }
            None => break,
        }
    }
}

/// Poll the control channel without blocking, returning `true` if a
/// `Cancel` addressed to `current` was observed.
///
/// The control ring multiplexes cancels and new requests, and the
/// consumer has no non-destructive peek — checking for a cancel must
/// `pop`. Any `Request` popped while hunting for a cancel is moved into
/// `pending` (processed later in order) instead of being discarded; that
/// is what stops a request submitted mid-stream from vanishing.
///
/// Cancels are matched against the in-flight request's ticket. A cancel
/// naming some *other* request is dropped: its target already finished,
/// and applying it to the current request would abort a healthy response
/// because an unrelated one timed out. `CANCEL_ANY` (from
/// [`AsyncClient::cancel`], which names no request) always matches, and
/// `current == None` means nothing is in flight to cancel.
fn poll_control(
    control_rx: &mut MpscConsumer<Control>,
    pending: &mut VecDeque<AsyncRequest>,
    current: Option<u64>,
    shutting_down: &AtomicBool,
) -> bool {
    let mut cancelled = false;
    loop {
        if shutting_down.load(Ordering::Acquire) {
            return true;
        }
        match control_rx.pop() {
            Some(Control::Cancel(ticket)) => {
                if current.is_some_and(|c| ticket == CANCEL_ANY || ticket == c) {
                    cancelled = true;
                } else if ticket != CANCEL_ANY
                    && let Some(index) = pending.iter().position(|request| request.ticket == ticket)
                {
                    let request = pending
                        .remove(index)
                        .expect("pending index came from the same queue");
                    ChunkSink::new(&request.chunk_tx, &request.consumer_alive, shutting_down)
                        .send_terminal(Chunk::Aborted);
                }
            }
            Some(Control::Request(request)) => pending.push_back(request),
            None => return cancelled,
        }
    }
}

// One linear pass over a request's lifecycle (clean → send → head → body
// → terminator); splitting it would scatter the dirty-flag invariants that
// the whole desync class of bugs hinges on.
#[allow(clippy::too_many_lines)]
fn process_request<C, const MAX_HEAD_SIZE: usize>(
    client: &mut Client<C, MAX_HEAD_SIZE>,
    request: AsyncRequest,
    control_rx: &mut MpscConsumer<Control>,
    pending: &mut VecDeque<AsyncRequest>,
    shutting_down: &AtomicBool,
) where
    C: crate::connector::Connector,
{
    // A cancel can queue directly behind this request while it waits for
    // an earlier response. Match it to this request before reconnecting or
    // publishing started: a cancelled queued request must never reach the
    // wire.
    if poll_control(control_rx, pending, Some(request.ticket), shutting_down) {
        ChunkSink::new(&request.chunk_tx, &request.consumer_alive, shutting_down)
            .send_terminal(Chunk::Aborted);
        return;
    }

    // If a previous streaming response was abandoned mid-body,
    // reconnect before we send the next request. `send_head` does
    // not call `ensure_clean()` itself, and the previous response's
    // leftover bytes would otherwise be interpreted as this response's
    // head, corrupting the stream.
    //
    // A failed reconnect is a HARD error for this request. Swallowing
    // it (as an earlier version did with `let _ =`) proceeds on the
    // still-dirty socket: the stale response bytes get parsed as this
    // request's head, and from then on every request receives the
    // previous request's response — a permanent, silent desync that
    // survives until the process restarts. `dirty` stays set, so the
    // next request simply retries the reconnect.
    if client.dirty
        && let Err(e) = client.ensure_clean()
    {
        ChunkSink::new(&request.chunk_tx, &request.consumer_alive, shutting_down)
            .send_terminal(Chunk::Error(std::sync::Arc::new(e)));
        return;
    }

    let AsyncRequest {
        method,
        path,
        query,
        body,
        headers,
        chunk_tx,
        consumer_alive,
        ticket,
        started,
        // Held until this function returns: the slot must stay claimed for
        // as long as the request is on the wire, not just while queued.
        _permit,
    } = request;
    let sink = ChunkSink::new(&chunk_tx, &consumer_alive, shutting_down);

    // Publish "this request has left the queue" before the first byte is
    // written. A caller's response-head deadline keys off this, so it
    // times the peer rather than the time spent queued behind a slow
    // predecessor.
    started.store(true, Ordering::Release);

    // Send the head. `send_head` handles the stale-keep-alive
    // reconnect internally (the read failure was before any
    // response byte reached the caller, so retry is safe).
    let request_params = RequestParams {
        method,
        path: &path,
        query: query.as_deref(),
        body: body.as_deref(),
        extra_headers: headers,
        allow_replay: method.is_replay_eligible(),
    };
    let send_result = client.send_head(&request_params);

    let (head_data, framing, tail_offset) = match send_result {
        Ok(parts) => parts,
        Err(e) => {
            sink.send_terminal(Chunk::Error(std::sync::Arc::new(e)));
            return;
        }
    };

    let status = head_data.status.as_u16();
    let reuse = head_data.connection_reuse();
    let header_vec: Vec<(Vec<u8>, Vec<u8>)> = head_data
        .headers()
        .map(|(n, v)| (n.to_vec(), v.to_vec()))
        .collect();

    // From this point on the response body sits unread on the socket.
    // Mark the connection dirty *before* anything can bail out early
    // (caller dropped the handle, body-read failure, …); each success
    // path below clears it once the body really is consumed. Leaving
    // this flag unset on any early return is a session-corruption bug:
    // the next request would reuse the socket and parse this response's
    // leftover body bytes as its own head, silently returning response
    // N's data to request N+1.
    client.dirty = true;

    if sink
        .send(Chunk::Head {
            status,
            headers: header_vec,
        })
        .is_err()
    {
        // Caller dropped the handle; the unread body stays on the
        // socket. `dirty` is already set, so the next request
        // reconnects instead of desyncing.
        return;
    }

    // Non-2xx: drain the body as a single chunk for the caller to inspect.
    // It still runs through CancellableStream: an error response can stall
    // exactly like a success response, and client drop/cancel must interrupt
    // both paths.
    if !(200..300).contains(&status) {
        let tail = client.head_buf[tail_offset..].to_vec();
        let mut cancellable = CancellableStream::new(
            &mut client.stream,
            control_rx,
            pending,
            ticket,
            shutting_down,
        );
        let mut collector = BodyCollector::new(
            client.config.max_response_body,
            client.config.stream_silence,
        );
        match collector.read(&mut cancellable, &framing, &tail) {
            Ok(bytes) => {
                client.dirty = !collector.is_reusable() || !reuse.is_keep();
                if !bytes.is_empty() && sink.send(Chunk::Body(bytes)).is_err() {
                    return;
                }
            }
            Err(Error::Io(error)) if error.kind == std::io::ErrorKind::Interrupted => {
                sink.send_terminal(Chunk::Aborted);
                return;
            }
            Err(error) => {
                sink.send_terminal(Chunk::Error(std::sync::Arc::new(error)));
                return;
            }
        }
        sink.send_terminal(Chunk::Eof);
        return;
    }

    // 2xx: streaming body. The cancel message is observed by the
    // `CancellableStream` wrapper around the live connection; it
    // checks the control channel at the start of every read, so the
    // cancel is observed on the next read attempt (bounded by the
    // client's `read_timeout`).
    //
    // `dirty` is already set above; `StreamingBody` clears it when the
    // body is read to completion, so cancelling/abandoning the stream
    // leaves the connection in a state the next request reconnects from.
    let tail = client.head_buf[tail_offset..].to_vec();
    let silence = client.config.stream_silence;
    let mut cancellable = CancellableStream::new(
        &mut client.stream,
        control_rx,
        pending,
        ticket,
        shutting_down,
    );
    let mut body = StreamingBody::new(
        &mut cancellable,
        &mut client.dirty,
        &framing,
        tail,
        silence,
        reuse.is_keep(),
    );
    let mut buf = vec![0u8; HEAD_BUF_SIZE];
    loop {
        match body.read(&mut buf) {
            Ok(0) => {
                sink.send_terminal(Chunk::Eof);
                return;
            }
            Ok(n) => {
                if sink.send(Chunk::Body(buf[..n].to_vec())).is_err() {
                    return;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                sink.send_terminal(Chunk::Aborted);
                return;
            }
            Err(e) => {
                sink.send_terminal(Chunk::Error(std::sync::Arc::new(Error::from(e))));
                return;
            }
        }
    }
}

/// A read wrapper that returns `Interrupted` when a cancel message
/// is waiting on the control channel.
///
/// The reader constructs one around the Client's stream before each
/// read; the wrapper checks the channel at the start of every read,
/// so the cancel is observed on the next read attempt (bounded by the
/// client's `read_timeout`).
pub struct CancellableStream<'a, S: Read> {
    inner: &'a mut S,
    control_rx: &'a mut MpscConsumer<Control>,
    pending: &'a mut VecDeque<AsyncRequest>,
    /// The in-flight request's ticket; only a cancel naming it (or
    /// `CANCEL_ANY`) interrupts this stream.
    ticket: u64,
    /// Set when the owning [`AsyncClient`] is dropping; the next read
    /// unwinds instead of waiting out the silence budget.
    shutting_down: &'a AtomicBool,
}

impl<'a, S: Read> CancellableStream<'a, S> {
    const fn new(
        inner: &'a mut S,
        control_rx: &'a mut MpscConsumer<Control>,
        pending: &'a mut VecDeque<AsyncRequest>,
        ticket: u64,
        shutting_down: &'a AtomicBool,
    ) -> Self {
        Self {
            inner,
            control_rx,
            pending,
            ticket,
            shutting_down,
        }
    }
}

impl<S: Read> Read for CancellableStream<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if poll_control(
            self.control_rx,
            self.pending,
            Some(self.ticket),
            self.shutting_down,
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "request cancelled",
            ));
        }
        self.inner.read(buf)
    }
}
