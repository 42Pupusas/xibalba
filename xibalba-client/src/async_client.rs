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
use std::thread::{self, JoinHandle};

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::{self, Consumer as MpscConsumer, Producer as MpscProducer};
use quetzalcoatl::spsc::{self, Consumer as SpscConsumer};
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;

use crate::body::{HEAD_BUF_SIZE, StreamingBody};
use crate::client::{Client, Config, DEFAULT_MAX_HEAD_SIZE};

/// Default capacity for the per-request chunk ring: a few SSE
/// events worth of buffering. The ring parks the caller on full
/// and the reader on empty, so the size only affects memory and
/// wakeup latency, not correctness.
const CHUNK_RING_CAP: usize = 32;

/// Default capacity for the control ring feeding the reader
/// thread. One slot per in-flight request; the worker only ever
/// has one model round in flight, so a handful of slots is plenty.
const CONTROL_RING_CAP: usize = 8;

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
    /// Cancel the currently in-flight request, if any.
    Cancel,
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
}

impl std::fmt::Debug for AsyncRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("body_len", &self.body.as_ref().map(Vec::len))
            .field("headers_len", &self.headers.len())
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
    control_tx: MpscProducer<Control>,
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
            .push_block(Control::Cancel)
            .map_err(|_| Error::Connection(ConnectionError::Other("reader is gone".into())))
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

    /// Take the chunk consumer out of the handle. The caller can then
    /// `pop_block` directly on the consumer — useful when integrating
    /// with an existing event loop that already parks on its own
    /// consumer.
    #[must_use]
    pub fn into_consumer(self) -> SpscConsumer<Chunk> {
        self.chunk_rx
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

        let join = thread::Builder::new()
            .name("xibalba-reader".to_owned())
            .spawn(move || {
                run_reader(client, control_rx);
            })
            .map_err(|e| {
                Error::Connection(ConnectionError::Other(format!(
                    "failed to spawn reader thread: {e}"
                )))
            })?;

        Ok(Self {
            control_tx: Some(control_tx),
            join: Some(join),
        })
    }

    /// Submit a streaming request and get back a handle. The
    /// reader processes requests in submission order, so two
    /// outstanding requests from the same client serialize.
    ///
    /// The handle's chunk ring is created here (one per call); the
    /// producer half travels with the request to the reader.
    ///
    /// # Errors
    /// Returns `Error::Connection` if the reader thread has
    /// already exited.
    pub fn submit(
        &self,
        method: Method,
        path: Vec<u8>,
        query: Option<Vec<u8>>,
        body: Option<Vec<u8>>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<StreamHandle, Error> {
        let (chunk_tx, chunk_rx) =
            spsc::RingBuffer::<Chunk>::new(Capacity::at_least(CHUNK_RING_CAP)).split();
        let request = AsyncRequest {
            method,
            path,
            query,
            body,
            headers,
            chunk_tx,
        };
        let control_tx = self.control()?;
        control_tx
            .push_block(Control::Request(request))
            .map_err(|_| Error::Connection(ConnectionError::ReaderGone))?;
        Ok(StreamHandle {
            chunk_rx,
            control_tx: control_tx.clone(),
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
            .push_block(Control::Cancel)
            .map_err(|_| Error::Connection(ConnectionError::ReaderGone))
    }
}

impl<const MAX_HEAD_SIZE: usize> Drop for AsyncClient<MAX_HEAD_SIZE> {
    fn drop(&mut self) {
        // Close the control ring first. The reader's `pop_block`
        // observes the closed producer and returns `None`,
        // unwinding to the outer loop and exiting the thread.
        // Without this, the reader would block forever in
        // `pop_block` and `join.join()` below would deadlock.
        drop(self.control_tx.take());

        // Best-effort join: if the reader is stuck on a request
        // that never finishes (e.g. a mid-flight read with a cancel
        // message not yet observed), we don't block the drop forever.
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
) where
    C: crate::connector::Connector,
{
    // Requests popped off the control ring while polling for a cancel
    // (during the start-of-request drain or mid-stream) are stashed here
    // rather than dropped, then processed in submission order.
    let mut pending: VecDeque<AsyncRequest> = VecDeque::new();
    loop {
        if let Some(request) = pending.pop_front() {
            process_request(&mut client, request, &mut control_rx, &mut pending);
            continue;
        }
        match control_rx.pop_block() {
            Some(Control::Request(request)) => {
                process_request(&mut client, request, &mut control_rx, &mut pending);
            }
            Some(Control::Cancel) => {
                // No request in flight; cancel is a no-op.
            }
            None => break,
        }
    }
}

/// Poll the control channel without blocking, returning `true` if a
/// `Cancel` was observed.
///
/// The control ring multiplexes cancels and new requests, and the
/// consumer has no non-destructive peek — checking for a cancel must
/// `pop`. Any `Request` popped while hunting for a cancel is moved into
/// `pending` (processed later in order) instead of being discarded; that
/// is what stops a request submitted mid-stream from vanishing.
fn poll_control(
    control_rx: &mut MpscConsumer<Control>,
    pending: &mut VecDeque<AsyncRequest>,
) -> bool {
    loop {
        match control_rx.pop() {
            Some(Control::Cancel) => return true,
            Some(Control::Request(request)) => pending.push_back(request),
            None => return false,
        }
    }
}

fn process_request<C, const MAX_HEAD_SIZE: usize>(
    client: &mut Client<C, MAX_HEAD_SIZE>,
    request: AsyncRequest,
    control_rx: &mut MpscConsumer<Control>,
    pending: &mut VecDeque<AsyncRequest>,
) where
    C: crate::connector::Connector,
{
    // If a previous streaming response was abandoned mid-body,
    // reconnect before we send the next request. `send_head` does
    // not call `ensure_clean()` itself, and the previous response's
    // leftover bytes would otherwise be interpreted as this response's
    // head, corrupting the stream.
    if client.dirty {
        let _ = client.ensure_clean();
    }

    // Drain any stale cancel messages that may have accumulated while
    // no request was in flight, stashing any queued requests so they
    // are not dropped.
    while poll_control(control_rx, pending) {}

    let AsyncRequest {
        method,
        path,
        query,
        body,
        headers,
        chunk_tx,
    } = request;

    // Translate the request's owned header buffers into the
    // xibalba `Header<'_>` shape the Client expects (borrowing
    // from the local `headers` Vec).
    let header_refs: Vec<Header<'_>> = headers
        .iter()
        .map(|(n, v)| Header {
            name: HeaderName::from_bytes(n),
            value: v,
        })
        .collect();

    // Send the head. `send_head` handles the stale-keep-alive
    // reconnect internally (the read failure was before any
    // response byte reached the caller, so retry is safe).
    let request_params = crate::client::RequestParams {
        method,
        path: &path,
        query: query.as_deref(),
        body: body.as_deref(),
        extra_headers: header_refs,
    };
    let send_result = client.send_head(&request_params);

    let (head_data, framing, tail_offset) = match send_result {
        Ok(parts) => parts,
        Err(e) => {
            let _ = chunk_tx.push_block(Chunk::Error(std::sync::Arc::new(e)));
            return;
        }
    };

    let status = head_data.status.as_u16();
    let header_vec: Vec<(Vec<u8>, Vec<u8>)> = head_data
        .headers()
        .map(|(n, v)| (n.to_vec(), v.to_vec()))
        .collect();

    if chunk_tx
        .push_block(Chunk::Head {
            status,
            headers: header_vec,
        })
        .is_err()
    {
        // Caller dropped the handle; nothing to do.
        return;
    }

    // Non-2xx: drain the body as a single chunk for the caller
    // to inspect (e.g. error message from the API).
    if !(200..300).contains(&status) {
        let body_result = crate::body::read_body(
            &mut client.stream,
            &framing,
            &client.head_buf[tail_offset..],
            client.config.max_response_body,
        );
        match body_result {
            Ok(bytes) => {
                if !bytes.is_empty() {
                    let _ = chunk_tx.push_block(Chunk::Body(bytes));
                }
            }
            Err(e) => {
                let _ = chunk_tx.push_block(Chunk::Error(std::sync::Arc::new(e)));
                return;
            }
        }
        let _ = chunk_tx.push_block(Chunk::Eof);
        return;
    }

    // 2xx: streaming body. The cancel message is observed by the
    // `CancellableStream` wrapper around the live connection; it
    // checks the control channel at the start of every read, so the
    // cancel is observed on the next read attempt (bounded by the
    // client's `read_timeout`).
    //
    // Mark the *client's* dirty flag so that cancelling/abandoning
    // the stream leaves the connection in a state the next request
    // will reconnect from.
    let tail = client.head_buf[tail_offset..].to_vec();
    client.dirty = true;
    let mut cancellable = CancellableStream::new(&mut client.stream, control_rx, pending);
    let mut body = StreamingBody::new(&mut cancellable, &mut client.dirty, &framing, tail);
    let mut buf = vec![0u8; HEAD_BUF_SIZE];
    loop {
        match body.read(&mut buf) {
            Ok(0) => {
                let _ = chunk_tx.push_block(Chunk::Eof);
                return;
            }
            Ok(n) => {
                if chunk_tx.push_block(Chunk::Body(buf[..n].to_vec())).is_err() {
                    return;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                let _ = chunk_tx.push_block(Chunk::Aborted);
                return;
            }
            Err(e) => {
                let _ = chunk_tx.push_block(Chunk::Error(std::sync::Arc::new(Error::from(e))));
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
}

impl<'a, S: Read> CancellableStream<'a, S> {
    const fn new(
        inner: &'a mut S,
        control_rx: &'a mut MpscConsumer<Control>,
        pending: &'a mut VecDeque<AsyncRequest>,
    ) -> Self {
        Self {
            inner,
            control_rx,
            pending,
        }
    }
}

impl<S: Read> Read for CancellableStream<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if poll_control(self.control_rx, self.pending) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "request cancelled",
                ));
            }
            match self.inner.read(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Read timeout expired — loop to re-check cancel message.
                }
                Err(e) => return Err(e),
            }
        }
    }
}
