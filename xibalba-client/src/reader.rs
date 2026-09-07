//! The reader thread: owns the connection and serves requests from the
//! control queue, delivering each response as a sequence of chunks.

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use xibalba_proto::error::Error;
use xibalba_proto::response::BodyFraming;

use crate::body::{BodyCollector, StreamingBody};
use crate::client::Client;
use crate::config::HEAD_BUF_SIZE;
use crate::connector::Connector;
use crate::control::{AsyncRequest, ControlInterrupt, ControlQueue};
use crate::delivery::ChunkSink;
use crate::interrupt::{Cancelled, InterruptibleStream, Latch};
use crate::params::RequestParams;
use crate::silence::RequestDeadline;
use crate::{Chunk, response::HeadData, reuse::ConnectionReuse};

/// Owns the connection for the reader thread's lifetime and serves one
/// request at a time from the control queue.
pub(crate) struct ReaderWorker<C: Connector, const MAX_HEAD_SIZE: usize> {
    client: Client<C, MAX_HEAD_SIZE>,
    queue: ControlQueue,
}

impl<C: Connector, const MAX_HEAD_SIZE: usize> ReaderWorker<C, MAX_HEAD_SIZE> {
    pub(crate) const fn new(client: Client<C, MAX_HEAD_SIZE>, queue: ControlQueue) -> Self {
        Self { client, queue }
    }

    /// Serve requests until the control ring closes or shutdown is signalled.
    ///
    /// Each request is owned for exactly one iteration, which is what frees
    /// its admission slot: the permit it carries must outlive the response,
    /// and drops with the request once the response is finished.
    pub(crate) fn run(&mut self) {
        while let Some(request) = self.queue.next_request() {
            self.serve(&request);
        }
    }

    fn serve(&mut self, request: &AsyncRequest) {
        let shutdown = self.queue.shutdown_flag();
        // One total per request, covering dispatch, head, and a drained error
        // body. A success body is not bounded by it: its consumer paces the
        // transfer, so only the silence budget applies.
        let deadline = RequestDeadline::after(self.client.config.request_deadline);
        if let Some(head) = self.dispatch(request, deadline, &shutdown) {
            self.stream_body(request, head, deadline, &shutdown);
        }
    }

    /// Get the request onto the wire and its head back, or deliver the
    /// terminator explaining why that did not happen.
    fn dispatch(
        &mut self,
        request: &AsyncRequest,
        deadline: RequestDeadline,
        shutdown: &AtomicBool,
    ) -> Option<ResponseStart> {
        let sink = ChunkSink::new(&request.chunk_tx, &request.consumer_alive, shutdown);

        // A cancel can queue directly behind this request while it waits for
        // an earlier response. Match it to this request before reconnecting or
        // publishing started: a cancelled queued request must never reach the
        // wire.
        if self.queue.poll_cancel(Some(request.ticket)) {
            sink.send_terminal(Chunk::Aborted);
            return None;
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
        if self.client.dirty
            && let Err(e) = self.client.ensure_clean()
        {
            sink.send_terminal(Chunk::Error(Arc::new(e)));
            return None;
        }

        // Publish "this request has left the queue" before the first byte is
        // written. A caller's response-head deadline keys off this, so it
        // times the peer rather than the time spent queued behind a slow
        // predecessor.
        request.started.store(true, Ordering::Release);

        let params = RequestParams {
            method: request.method,
            path: &request.path,
            query: request.query.as_deref(),
            body: request.body.as_deref(),
            extra_headers: request.headers.clone(),
            allow_replay: request.method.is_replay_eligible(),
        };

        // The latch, not the returned error, is what classifies a cancel: the
        // `std::io::Error` payload marking one is dropped at the `proto::Error`
        // boundary, which keeps only a kind and a message. Asking the interrupt
        // whether it fired is authoritative.
        let mut interrupt = Latch::new(ControlInterrupt::new(&mut self.queue, request.ticket));
        let sent = self
            .client
            .send_head_interruptible(&params, deadline, &mut interrupt);
        let fired = interrupt.fired();

        match sent {
            Ok((head_data, framing, tail_offset)) => Some(ResponseStart {
                head_data,
                framing,
                tail_offset,
            }),
            // Report a cancel during the request write or head read as Aborted,
            // matching the body path, so the caller can tell its own
            // cancellation from a transport failure.
            Err(_) if fired => {
                sink.send_terminal(Chunk::Aborted);
                None
            }
            Err(e) => {
                sink.send_terminal(Chunk::Error(Arc::new(e)));
                None
            }
        }
    }

    fn stream_body(
        &mut self,
        request: &AsyncRequest,
        start: ResponseStart,
        deadline: RequestDeadline,
        shutdown: &AtomicBool,
    ) {
        let sink = ChunkSink::new(&request.chunk_tx, &request.consumer_alive, shutdown);
        let ResponseStart {
            head_data,
            framing,
            tail_offset,
        } = start;
        let status = head_data.status().as_u16();
        let reuse = head_data.connection_reuse();
        let headers: Vec<(Vec<u8>, Vec<u8>)> = head_data
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
        self.client.dirty = true;

        if sink.send(Chunk::Head { status, headers }).is_err() {
            return;
        }

        // Split the borrow: the interrupt holds the queue for as long as the
        // body is being read, and the body needs the client. They are
        // disjoint fields, which the compiler only sees once they are named
        // separately.
        let Self { client, queue } = self;
        let mut interrupt = Latch::new(ControlInterrupt::new(queue, request.ticket));
        let tail = client.head_buf[tail_offset..].to_vec();

        if (200..300).contains(&status) {
            Self::stream_success(client, &sink, &mut interrupt, &framing, tail, reuse);
        } else {
            Self::drain_error_body(
                client,
                &sink,
                &mut interrupt,
                &framing,
                &tail,
                deadline,
                reuse,
            );
        }
    }

    /// Drain a non-2xx body as a single chunk for the caller to inspect.
    ///
    /// It still runs through the interrupt: an error body can stall
    /// exactly like a success response, and client drop/cancel must
    /// interrupt both. The request's total deadline bounds it — the body
    /// belongs to this request's budget, unlike a success stream whose
    /// consumer paces the transfer.
    fn drain_error_body(
        client: &mut Client<C, MAX_HEAD_SIZE>,
        sink: &ChunkSink<'_>,
        interrupt: &mut Latch<ControlInterrupt<'_>>,
        framing: &BodyFraming,
        tail: &[u8],
        deadline: RequestDeadline,
        reuse: ConnectionReuse,
    ) {
        let mut collector = BodyCollector::with_deadline(
            client.config.max_response_body,
            client.config.stream_silence,
            deadline,
        );
        let collected = {
            let mut cancellable = InterruptibleStream::new(&mut client.stream, &mut *interrupt);
            collector.read(&mut cancellable, framing, tail)
        };
        match collected {
            Ok(bytes) => {
                client.dirty = !collector.is_reusable() || !reuse.is_keep();
                if !bytes.is_empty() && sink.send(Chunk::Body(bytes)).is_err() {
                    return;
                }
                sink.send_terminal(Chunk::Eof);
            }
            Err(_) if interrupt.fired() => sink.send_terminal(Chunk::Aborted),
            Err(error) => sink.send_terminal(Chunk::Error(Arc::new(error))),
        }
    }

    /// Stream a 2xx body chunk by chunk. The same interrupt covers it,
    /// checked at the start of every read, so a cancel is observed within one
    /// read timeout.
    ///
    /// `dirty` is already set by the caller; [`StreamingBody`] clears it when
    /// the body is read to completion, so cancelling or abandoning the stream
    /// leaves the connection in a state the next request reconnects from.
    fn stream_success(
        client: &mut Client<C, MAX_HEAD_SIZE>,
        sink: &ChunkSink<'_>,
        interrupt: &mut Latch<ControlInterrupt<'_>>,
        framing: &BodyFraming,
        tail: Vec<u8>,
        reuse: ConnectionReuse,
    ) {
        let silence = client.config.stream_silence;
        let mut cancellable = InterruptibleStream::new(&mut client.stream, interrupt);
        let mut body = StreamingBody::new(
            &mut cancellable,
            &mut client.dirty,
            framing,
            tail,
            silence,
            reuse.is_keep(),
        );
        let mut buf = vec![0u8; HEAD_BUF_SIZE];
        loop {
            match body.read(&mut buf) {
                Ok(0) => return sink.send_terminal(Chunk::Eof),
                Ok(n) => {
                    if sink.send(Chunk::Body(buf[..n].to_vec())).is_err() {
                        return;
                    }
                }
                Err(e) if Cancelled::marks(&e) => return sink.send_terminal(Chunk::Aborted),
                Err(e) => return sink.send_terminal(Chunk::Error(Arc::new(Error::from(e)))),
            }
        }
    }
}

/// The parts of a response that are known once its head has been read.
struct ResponseStart {
    head_data: HeadData,
    framing: BodyFraming,
    tail_offset: usize,
}
