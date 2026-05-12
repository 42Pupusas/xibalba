//! io_uring-backed HTTP/1.1 connection pool.
//!
//! # Architecture
//!
//! ```text
//! caller (owns Submitter) ──SQ──► kernel ──CQ──► complete thread ──response_tx──► caller
//!                                                       │
//!                                                       └── rearm_rx (rare) ──► caller
//! ```
//!
//! The send SQE carries the `request_id` in its `user_data`.  Its CQE arrives in
//! the complete thread and is used to enqueue the id before the matching recv
//! CQEs arrive — no separate channel needed.
//!
//! The caller thread pushes SQEs directly — no submit thread, no message-passing
//! for the hot path.  The complete thread blocks on `wait(1)`, drains CQEs, and
//! pushes finished responses back to the caller.
//!
//! Multishot recv stays armed across keep-alive requests.  If the kernel cancels
//! a multishot mid-response (rare), the complete thread notifies the caller via
//! `rearm_rx` so it can re-submit a recv SQE.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::net::ToSocketAddrs;
use std::thread::JoinHandle;

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::{self, Producer as MpscProducer, RingBuffer as MpscRingBuffer};
use quetzalcoatl::spmc::{Consumer as SpmcConsumer, Producer as SpmcProducer, RingBuffer as SpmcRingBuffer};
use ququmatz::types::{MsgFlags, SockAddrIn, TimeoutFlags, Timespec};
use ququmatz::{Completer, ProvidedBufferRing, RawFd, Sqe, Submitter};

use xibalba_proto::error::Error;
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::response::{
    BodyFraming, ChunkedDecoder, DecodeResult, determine_body_framing, parse_response_head,
};
use xibalba_proto::status::StatusCode;
use xibalba_proto::url::Url;
use xibalba_proto::version::Version;

// ── size constants ────────────────────────────────────────────────────────────

const MAX_REQ_SIZE: usize = 8192;
const MAX_HEADERS: usize = 64;
const PBUF_BUF_SIZE: u32 = 8192;
const PBUF_COUNT: u32 = 64;
const PBUF_BGID: u16 = 0;
const BLOCK_SIZE: usize = 8192;
const RING_ENTRIES: u32 = 256;
const SHUTDOWN_UD: u64 = u64::MAX;
// Second-highest value — cannot collide with recv (bit63=0), send (bit63=1 and not MAX), or SHUTDOWN.
const TIMEOUT_UD: u64 = u64::MAX - 1;

// ── user_data helpers ─────────────────────────────────────────────────────────

// Upper 32 bits: fd, lower 32 bits: conn_id.
#[allow(clippy::cast_possible_truncation, clippy::cast_lossless)]
const fn ud_recv(fd: usize, conn_id: u32) -> u64 {
    (fd as u64) << 32 | (conn_id as u64 & 0xffff_ffff)
}

const fn ud_fd(ud: u64) -> usize { (ud >> 32) as usize }
const fn ud_conn_id(ud: u64) -> u32 { (ud & 0xffff_ffff) as u32 }

// ── public types ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnHandle {
    conn_id: u32,
    fd: usize,
}

pub type RequestId = u64;

pub struct HeadData {
    pub head_buf: Vec<u8>,
    pub ranges: [(u16, u16, u16, u16); MAX_HEADERS],
    pub header_count: usize,
}

impl HeadData {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let buf = &self.head_buf;
        self.ranges[..self.header_count].iter().map(move |&(ns, nl, vs, vl)| (
            &buf[ns as usize..ns as usize + nl as usize],
            &buf[vs as usize..vs as usize + vl as usize],
        ))
    }
}

pub struct Response {
    pub request_id: RequestId,
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: Vec<u8>,
}

impl Response {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }
}

/// Result delivered to the caller for each request.
pub enum ConnResult {
    Response(Box<Response>),
    /// The connection was closed or an I/O error occurred.
    Error {
        request_id: RequestId,
        /// The connection on which the error occurred.  The caller should
        /// treat this handle as permanently dead and call `disconnect`.
        conn: ConnHandle,
        errno: i32,
    },
    /// A `recv_timeout` deadline elapsed before any response arrived.
    Timeout,
}

// ── send SQE user_data encoding ───────────────────────────────────────────────
//
// Recv SQEs:  bit63=0, bits62-32=fd, bits31-0=conn_id
// Send SQEs:  bit63=1, bits62-32=conn_id, bits31-0=request_seq (per-conn u32)
// SHUTDOWN:   all bits set
//
// The complete thread sees the send CQE first (TCP send happens before the
// response arrives), extracts conn_id and request_seq, and enqueues the pair
// into the connection's pending queue.  The recv CQEs then find the id waiting.

const SEND_UD_FLAG: u64 = 1 << 63;

const fn ud_send(conn_id: u32, seq: u32) -> u64 {
    #[allow(clippy::cast_lossless)]
    { SEND_UD_FLAG | (conn_id as u64) << 32 | seq as u64 }
}
const fn is_send_cqe(ud: u64) -> bool { ud & SEND_UD_FLAG != 0 && ud != SHUTDOWN_UD }
const fn ud_send_conn_id(ud: u64) -> u32 { ((ud >> 32) & 0x7fff_ffff) as u32 }
const fn ud_send_seq(ud: u64) -> u32 { (ud & 0xffff_ffff) as u32 }

// ── inter-thread messages ─────────────────────────────────────────────────────

/// Complete thread → caller: multishot recv was cancelled by the kernel;
/// please re-arm.
struct RearmMsg {
    conn_id: u32,
    fd: usize,
}

// ── per-connection state (complete thread) ────────────────────────────────────

struct Slot {
    head_accum: Vec<u8>,
    partial: Option<PartialResponse>,
}

impl Slot {
    fn reset(&mut self) {
        self.head_accum.clear();
        self.partial = None;
    }
}

struct ConnData {
    /// Queue of `request_ids` in submission order.  The front entry is always the
    /// one currently being received.
    pending_ids: VecDeque<RequestId>,
    slot: Slot,
}

impl ConnData {
    fn current_request_id(&self) -> RequestId {
        self.pending_ids.front().copied().unwrap_or(0)
    }

    fn advance(&mut self) {
        self.pending_ids.pop_front();
        self.slot.reset();
    }
}

struct PartialResponse {
    version: Version,
    status: StatusCode,
    head: HeadData,
    body_buf: Vec<u8>,
    body_done: bool,
    framing: InternalFraming,
}

enum InternalFraming {
    ContentLength { remaining: u64 },
    Chunked { decoder: ChunkedDecoder },
    UntilClose,
    Done,
}

// ── per-connection state (caller thread) ──────────────────────────────────────

struct CallerConn {
    fd: usize,
    recv_armed: bool,
    errored: bool,
    send_buf: [u8; MAX_REQ_SIZE],
}

// ── pool ──────────────────────────────────────────────────────────────────────

pub struct Pool {
    sub: Submitter,
    conns: HashMap<u32, CallerConn>,
    free_ids: BTreeSet<u32>,
    next_conn_id: u32,
    response_rx: SpmcConsumer<ConnResult>,
    rearm_rx: mpsc::Consumer<RearmMsg>,
    /// Rearms that could not be pushed because the SQ was full; retried at
    /// the start of every `request()` call.
    pending_rearms: VecDeque<RearmMsg>,
    next_request_id: u32,
    complete_thread: Option<JoinHandle<()>>,
}

impl Drop for Pool {
    fn drop(&mut self) {
        let _ = self.sub.push_nop(SHUTDOWN_UD);
        let _ = self.sub.submit();
        if let Some(h) = self.complete_thread.take() { let _ = h.join(); }
    }
}

impl Pool {
    /// # Errors
    /// Returns an error if the `io_uring` instance or provided-buffer ring cannot be set up.
    pub fn new() -> Result<Self, Error> {
        let mut ring = ququmatz::IoUring::builder(RING_ENTRIES)
            .build()
            .map_err(|e| Error::Connection(format!("io_uring setup: {e}")))?;

        let pbuf = ring
            .register_provided_buffers(PBUF_BGID, PBUF_COUNT, PBUF_BUF_SIZE)
            .map_err(|e| Error::Connection(format!("register provided buffers: {e}")))?;

        let (submitter, completer) = ring.split();

        let (response_tx, response_rx) = SpmcRingBuffer::<ConnResult>::new(Capacity::exact(128)).split();
        let (rearm_tx, rearm_rx) = MpscRingBuffer::<RearmMsg>::new(Capacity::exact(32)).split();

        let complete_thread = std::thread::spawn(move || {
            complete_loop(completer, pbuf, &response_tx, &rearm_tx);
        });

        Ok(Self {
            sub: submitter,
            conns: HashMap::new(),
            free_ids: BTreeSet::new(),
            next_conn_id: 0,
            response_rx,
            rearm_rx,
            pending_rearms: VecDeque::new(),
            next_request_id: 1,
            complete_thread: Some(complete_thread),
        })
    }

    /// # Errors
    /// Returns an error if the URL is invalid, DNS resolution fails, or TCP connect fails.
    pub fn connect(&mut self, url: &[u8]) -> Result<ConnHandle, Error> {
        let url = Url::parse(url)?;
        let host_str = std::str::from_utf8(url.host)
            .map_err(|_| Error::Connection("invalid UTF-8 in host".into()))?;
        let addr = resolve(host_str, url.effective_port())?;
        let fd = blocking_connect(&addr)
            .map_err(|()| Error::Connection("TCP connect failed".into()))?;

        let conn_id = self.alloc_conn_id();
        self.conns.insert(conn_id, CallerConn {
            fd,
            recv_armed: false,
            errored: false,
            send_buf: [0u8; MAX_REQ_SIZE],
        });

        Ok(ConnHandle { conn_id, fd })
    }

    pub fn disconnect(&mut self, conn: ConnHandle) {
        if let Some(c) = self.conns.remove(&conn.conn_id) {
            libc_close(c.fd);
            self.free_ids.insert(conn.conn_id);
        }
    }

    /// # Errors
    /// Returns an error if the connection is unknown, in an error state, the request is too
    /// large, or the `io_uring` submission queue is full.
    pub fn request(
        &mut self,
        conn: ConnHandle,
        method: Method,
        path: &[u8],
        query: Option<&[u8]>,
    ) -> Result<RequestId, Error> {
        self.drain_rearms();

        let c = self.conns.get_mut(&conn.conn_id)
            .ok_or_else(|| Error::Connection("unknown conn_id".into()))?;
        if c.errored {
            return Err(Error::Connection("connection is in an error state; call disconnect".into()));
        }

        let headers = [
            Header { name: HeaderName::Host, value: b"placeholder" },
            Header { name: HeaderName::Connection, value: b"keep-alive" },
            Header { name: HeaderName::UserAgent, value: b"xibalba/0.1" },
        ];
        let req = Request { method, path, query, version: Version::Http11, headers: &headers };
        let len = req.serialize_to_buf(&mut c.send_buf)?;

        let seq = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);

        let raw_fd = RawFd::from_raw(c.fd);
        let send = Sqe::send(raw_fd, &c.send_buf[..len], MsgFlags::default())
            .user_data(ud_send(conn.conn_id, seq));
        self.sub.push(send)
            .map_err(|e| Error::Connection(format!("SQ full (send): {e}")))?;

        if !c.recv_armed {
            let recv = Sqe::recv_multishot(raw_fd, MsgFlags::default())
                .buffer_select(PBUF_BGID)
                .user_data(ud_recv(c.fd, conn.conn_id));
            self.sub.push(recv)
                .map_err(|e| Error::Connection(format!("SQ full (recv): {e}")))?;
            c.recv_armed = true;
        }

        self.sub.submit()
            .map_err(|e| Error::Connection(format!("submit: {e}")))?;

        Ok(RequestId::from(seq))
    }

    /// # Errors
    /// Returns an error if the connection is unknown, in an error state, or the submission queue is full.
    pub fn get(&mut self, conn: ConnHandle, path: &[u8]) -> Result<RequestId, Error> {
        self.request(conn, Method::Get, path, None)
    }

    pub fn poll(&mut self) -> Option<ConnResult> {
        let r = self.response_rx.pop()?;
        self.mark_errored_if_needed(&r);
        Some(r)
    }

    /// # Panics
    /// Panics if the complete thread has exited.
    pub fn recv(&mut self) -> ConnResult {
        let r = self.response_rx.pop_block().expect("complete thread exited");
        self.mark_errored_if_needed(&r);
        r
    }

    /// Returns `None` if no response arrives within `timeout`.
    ///
    /// A `Sqe::timeout` is submitted into the same ring so the complete thread
    /// wakes up and pushes `ConnResult::Timeout` through the existing response
    /// channel — no extra thread or channel needed.  If a real response arrives
    /// first, the pending timeout SQE is cancelled with `timeout_remove`.
    ///
    /// # Panics
    /// Panics if the complete thread has exited.
    pub fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<ConnResult> {
        let ts = Timespec::from_millis(
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        );
        let tsqe = Sqe::timeout(&ts, 0, TimeoutFlags::default()).user_data(TIMEOUT_UD);
        // If the SQ is full we can't submit the timeout — fall back to a
        // non-blocking poll so we don't block forever.
        if self.sub.push(tsqe).is_err() {
            return self.poll();
        }
        let _ = self.sub.submit();

        let r = self.response_rx.pop_block().expect("complete thread exited");
        match r {
            ConnResult::Timeout => None,
            other => {
                self.mark_errored_if_needed(&other);
                // Cancel the pending timeout so it doesn't fire later.
                let cancel = Sqe::timeout_remove(TIMEOUT_UD);
                let _ = self.sub.push(cancel);
                let _ = self.sub.submit();
                Some(other)
            }
        }
    }

    pub fn recv_n(&mut self, mut n: usize, mut f: impl FnMut(ConnResult)) {
        while n > 0 {
            let r = self.recv();
            f(r);
            n -= 1;
        }
    }

    fn mark_errored_if_needed(&mut self, result: &ConnResult) {
        if let ConnResult::Error { conn, .. } = result
            && let Some(c) = self.conns.get_mut(&conn.conn_id) {
                c.errored = true;
            }
    }

    pub fn subscribe(&self) -> SpmcConsumer<ConnResult> {
        self.response_rx.clone()
    }

    fn alloc_conn_id(&mut self) -> u32 {
        if let Some(&id) = self.free_ids.iter().next() {
            self.free_ids.remove(&id);
            id
        } else {
            let id = self.next_conn_id;
            self.next_conn_id = self.next_conn_id.checked_add(1)
                .expect("conn_id exhausted (>4B connections allocated without recycling)");
            id
        }
    }

    fn drain_rearms(&mut self) {
        // Collect new rearm requests from the complete thread.
        while let Some(msg) = self.rearm_rx.pop() {
            self.pending_rearms.push_back(msg);
        }
        // Flush pending rearms into the SQ.  Stop as soon as the SQ is full;
        // the remainder stays in pending_rearms and is retried next call.
        while let Some(msg) = self.pending_rearms.front() {
            let raw_fd = RawFd::from_raw(msg.fd);
            let recv = Sqe::recv_multishot(raw_fd, MsgFlags::default())
                .buffer_select(PBUF_BGID)
                .user_data(ud_recv(msg.fd, msg.conn_id));
            if self.sub.push(recv).is_err() {
                break; // SQ full — leave msg at the front, retry next time
            }
            let msg = self.pending_rearms.pop_front().unwrap();
            if let Some(c) = self.conns.get_mut(&msg.conn_id) {
                c.recv_armed = true;
            }
        }
        if !self.pending_rearms.is_empty() {
            let _ = self.sub.submit();
        }
    }
}

// ── complete thread ────────────────────────────────────────────────────────────

fn complete_loop(
    mut cmp: Completer,
    mut pbuf: ProvidedBufferRing,
    response_tx: &SpmcProducer<ConnResult>,
    rearm_tx: &MpscProducer<RearmMsg>,
) {
    let mut conns: HashMap<u32, ConnData> = HashMap::new();

    loop {
        let _ = cmp.wait(1);

        while let Some(cqe) = cmp.complete() {
            if cqe.user_data == SHUTDOWN_UD {
                return;
            }

            if cqe.user_data == TIMEOUT_UD {
                // Timeout fired before a response arrived; wake the caller.
                let _ = response_tx.push_block(ConnResult::Timeout);
                continue;
            }

            // Send CQE: carries the request_id; enqueue it for the connection.
            if is_send_cqe(cqe.user_data) {
                if cqe.result >= 0 {
                    let conn_id = ud_send_conn_id(cqe.user_data);
                    let seq = ud_send_seq(cqe.user_data);
                    let conn = conns.entry(conn_id).or_insert_with(|| ConnData {
                        pending_ids: VecDeque::new(),
                        slot: Slot { head_accum: Vec::new(), partial: None },
                    });
                    conn.pending_ids.push_back(RequestId::from(seq));
                }
                // Send errors are ignored — the recv path will surface them.
                continue;
            }

            let conn_id = ud_conn_id(cqe.user_data);
            let fd = ud_fd(cqe.user_data);

            // Negative result → I/O error; zero with no buffer → EOF (UntilClose).
            if cqe.result < 0 {
                let conn = conns.entry(conn_id).or_insert_with(|| ConnData {
                    pending_ids: VecDeque::new(),
                    slot: Slot { head_accum: Vec::new(), partial: None },
                });
                let request_id = conn.current_request_id();
                let _ = response_tx.push_block(ConnResult::Error {
                    request_id,
                    conn: ConnHandle { conn_id, fd },
                    errno: -cqe.result,
                });
                conn.slot.reset();
                conn.pending_ids.pop_front();
                continue;
            }

            #[allow(clippy::cast_sign_loss)]
            let n = cqe.result as usize;

            // result == 0 with no buffer_id means EOF on the socket.
            if n == 0 && cqe.buffer_id().is_none() {
                let conn = conns.entry(conn_id).or_insert_with(|| ConnData {
                    pending_ids: VecDeque::new(),
                    slot: Slot { head_accum: Vec::new(), partial: None },
                });
                finish_until_close(ConnHandle { conn_id, fd }, conn, response_tx);
                continue;
            }

            let (bid, data) = match cqe.buffer_id() {
                #[allow(clippy::cast_possible_truncation)]
                Some(bid) => match pbuf.buffer_pinned(bid, n as u32) {
                    Some(slice) => (bid, slice),
                    None => continue,
                },
                None => continue,
            };

            let conn = conns.entry(conn_id).or_insert_with(|| ConnData {
                pending_ids: VecDeque::new(),
                slot: Slot { head_accum: Vec::new(), partial: None },
            });

            let more = cqe.flags.contains(ququmatz::types::CqeFlags::MORE);
            let request_id = conn.current_request_id();

            if let Some(resp) = process_recv_data(&mut conn.slot, data, request_id) {
                conn.advance();
                pbuf.recycle_and_commit(bid);
                let _ = response_tx.push_block(ConnResult::Response(Box::new(resp)));
            } else if !more {
                pbuf.recycle_and_commit(bid);
                let _ = rearm_tx.push_block(RearmMsg { conn_id, fd });
            } else {
                pbuf.recycle_and_commit(bid);
            }
        }
    }
}

/// Called when the socket reaches EOF.  If a `UntilClose` response is in
/// progress, deliver it; otherwise emit an error.
fn finish_until_close(conn_handle: ConnHandle, conn: &mut ConnData, response_tx: &SpmcProducer<ConnResult>) {
    let request_id = conn.current_request_id();
    if let Some(ref mut partial) = conn.slot.partial
        && matches!(partial.framing, InternalFraming::UntilClose)
    {
        partial.body_done = true;
        let p = conn.slot.partial.take().unwrap();
        let _ = response_tx.push_block(ConnResult::Response(Box::new(Response {
            request_id,
            version: p.version,
            status: p.status,
            head: p.head,
            body: p.body_buf,
        })));
        conn.advance();
        return;
    }
    // EOF without a completed response is an error.
    let _ = response_tx.push_block(ConnResult::Error { request_id, conn: conn_handle, errno: 0 });
    conn.slot.reset();
    conn.pending_ids.pop_front();
}

// ── CQE data processing ───────────────────────────────────────────────────────

fn process_recv_data(slot: &mut Slot, data: &[u8], request_id: RequestId) -> Option<Response> {
    if let Some(ref mut partial) = slot.partial {
        if !data.is_empty() {
            pump_partial(partial, data);
        }
        if partial.body_done {
            let p = slot.partial.take().unwrap();
            return Some(Response {
                request_id,
                version: p.version,
                status: p.status,
                head: p.head,
                body: p.body_buf,
            });
        }
        return None;
    }

    slot.head_accum.extend_from_slice(data);
    parse_head_and_maybe_finish(slot, request_id)
}

fn parse_head_and_maybe_finish(slot: &mut Slot, request_id: RequestId) -> Option<Response> {
    let head_end = slot.head_accum.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head_bytes_len = head_end + 4;

    let mut hdr_buf = [const { xibalba_proto::header::Header::empty() }; MAX_HEADERS];
    let (head, consumed) =
        parse_response_head(&slot.head_accum[..head_bytes_len], &mut hdr_buf).ok()?;

    let framing = determine_body_framing(
        head.status, false, &hdr_buf[..head.header_count], head.header_count,
    );
    let ranges =
        build_ranges(&hdr_buf[..head.header_count], &slot.head_accum[..head_bytes_len]).ok()?;
    let (version, status, header_count) = (head.version, head.status, head.header_count);
    let head_buf = slot.head_accum[..head_bytes_len].to_vec();

    let head_data = HeadData { head_buf, ranges, header_count };

    let (internal_framing, body_done) = match framing {
        BodyFraming::None => (InternalFraming::Done, true),
        BodyFraming::ContentLength(n) => (InternalFraming::ContentLength { remaining: n }, n == 0),
        BodyFraming::Chunked => (InternalFraming::Chunked { decoder: ChunkedDecoder::new() }, false),
        BodyFraming::UntilClose => (InternalFraming::UntilClose, false),
    };

    let body_capacity = match framing {
        #[allow(clippy::cast_possible_truncation)] // capacity hint; truncation on 32-bit is harmless
        BodyFraming::ContentLength(n) => n as usize,
        _ => 0,
    };
    let mut partial = PartialResponse {
        version, status, head: head_data,
        body_buf: Vec::with_capacity(body_capacity), body_done, framing: internal_framing,
    };

    if consumed < slot.head_accum.len() && !body_done {
        pump_partial(&mut partial, &slot.head_accum[consumed..]);
    }

    if partial.body_done {
        Some(Response {
            request_id,
            version: partial.version,
            status: partial.status,
            head: partial.head,
            body: partial.body_buf,
        })
    } else {
        slot.partial = Some(partial);
        None
    }
}

fn pump_partial(resp: &mut PartialResponse, data: &[u8]) {
    let mut out = [0u8; BLOCK_SIZE];
    match &mut resp.framing {
        InternalFraming::Done => { resp.body_done = true; }
        InternalFraming::ContentLength { remaining } => {
            #[allow(clippy::cast_possible_truncation)]
            let to_take = data.len().min(*remaining as usize);
            resp.body_buf.extend_from_slice(&data[..to_take]);
            *remaining -= to_take as u64;
            if *remaining == 0 { resp.body_done = true; }
        }
        InternalFraming::Chunked { decoder } => {
            let mut pos = 0;
            while pos < data.len() && !decoder.is_done() {
                let (result, consumed) = decoder.decode(&data[pos..], &mut out);
                pos += consumed;
                match result {
                    DecodeResult::Data(n) => resp.body_buf.extend_from_slice(&out[..n]),
                    DecodeResult::Done | DecodeResult::Error(_) => { resp.body_done = true; break; }
                    DecodeResult::NeedMore => break,
                }
            }
        }
        // UntilClose body is accumulated but only marked done on EOF (handled
        // in finish_until_close, not here).
        InternalFraming::UntilClose => { resp.body_buf.extend_from_slice(data); }
    }
}

// ── blocking connect ──────────────────────────────────────────────────────────

fn blocking_connect(addr: &SockAddrIn) -> Result<usize, ()> {
    use std::os::unix::io::IntoRawFd;
    let stream = std::net::TcpStream::connect(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::from(u32::from_be(addr.sin_addr)),
        u16::from_be(addr.sin_port),
    ))
    .map_err(|_| ())?;
    stream.set_nodelay(true).ok();
    #[allow(clippy::cast_sign_loss)]
    Ok(stream.into_raw_fd() as usize)
}

// ── free helpers ──────────────────────────────────────────────────────────────

fn resolve(host: &str, port: u16) -> Result<SockAddrIn, Error> {
    let addr = (host, port)
        .to_socket_addrs()
        .map_err(|e| Error::Connection(format!("DNS resolution failed: {e}")))?
        .find_map(|a| if let std::net::SocketAddr::V4(v4) = a { Some(v4) } else { None })
        .ok_or_else(|| Error::Connection("no IPv4 address found".into()))?;
    Ok(SockAddrIn {
        sin_family: 2,
        sin_port: addr.port().to_be(),
        sin_addr: u32::from(*addr.ip()).to_be(),
        sin_zero: [0u8; 8],
    })
}

fn build_ranges(
    headers: &[xibalba_proto::header::Header<'_>],
    src: &[u8],
) -> Result<[(u16, u16, u16, u16); MAX_HEADERS], Error> {
    let src_base = src.as_ptr() as usize;
    let src_end = src_base + src.len();
    let mut ranges = [(0u16, 0u16, 0u16, 0u16); MAX_HEADERS];
    for (i, h) in headers.iter().enumerate() {
        let name = h.name.as_bytes();
        let value = h.value;
        let vs_off = value.as_ptr() as usize - src_base;
        let name_ptr = name.as_ptr() as usize;
        let ns_off = if name_ptr >= src_base && name_ptr < src_end {
            name_ptr - src_base
        } else {
            src.windows(name.len())
                .position(|w| w == name)
                .ok_or_else(|| Error::Connection("header name not in head buffer".into()))?
        };
        ranges[i] = (
            u16::try_from(ns_off).map_err(|_| Error::Connection("ns_off overflows u16".into()))?,
            u16::try_from(name.len()).map_err(|_| Error::Connection("name len overflows u16".into()))?,
            u16::try_from(vs_off).map_err(|_| Error::Connection("vs_off overflows u16".into()))?,
            u16::try_from(value.len()).map_err(|_| Error::Connection("val len overflows u16".into()))?,
        );
    }
    Ok(ranges)
}

// ── libc shim ─────────────────────────────────────────────────────────────────

fn libc_close(fd: usize) {
    use std::os::unix::io::{FromRawFd, OwnedFd};
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    // SAFETY: we have exclusive ownership of this fd at this point.
    drop(unsafe { OwnedFd::from_raw_fd(fd as i32) });
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn keep_alive_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                let resp = resp;
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = s.read(&mut buf).unwrap_or(0);
                        if n == 0 { break; }
                        if buf[..n].windows(4).any(|w| w == b"\r\n\r\n")
                            && s.write_all(resp).is_err() { break; }
                    }
                });
            }
        });
        port
    }

    /// Server that closes the connection immediately after accepting, before
    /// sending any bytes.
    fn drop_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                drop(stream); // close immediately
            }
        });
        port
    }

    /// Server that accepts but never writes anything.
    fn silent_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            #[allow(clippy::collection_is_never_read)] // intentional: keep sockets alive
            let mut conns: Vec<_> = Vec::new();
            for stream in listener.incoming() {
                conns.push(stream);
            }
        });
        port
    }

    #[test]
    fn test_happy_path() {
        let port = keep_alive_server();
        let mut pool = Pool::new().unwrap();
        let conn = pool.connect(format!("http://127.0.0.1:{port}").as_bytes()).unwrap();
        for _ in 0..3 {
            pool.get(conn, b"/").unwrap();
            match pool.recv() {
                ConnResult::Response(r) => assert_eq!(&r.body, b"ok"),
                other => panic!("unexpected: {}", match other {
                    ConnResult::Error { errno, .. } => format!("error errno={errno}"),
                    ConnResult::Timeout => "timeout".into(),
                    ConnResult::Response(_) => unreachable!(),
                }),
            }
        }
    }

    #[test]
    fn test_server_closes_connection() {
        let port = drop_server();
        let mut pool = Pool::new().unwrap();
        let conn = pool.connect(format!("http://127.0.0.1:{port}").as_bytes()).unwrap();
        pool.get(conn, b"/").unwrap();
        match pool.recv() {
            ConnResult::Error { conn: err_conn, .. } => {
                // The errored handle should match the one we used.
                assert_eq!(err_conn.conn_id, conn.conn_id);
                // Further requests on the dead connection must fail immediately.
                let err = pool.get(conn, b"/").unwrap_err();
                assert!(err.to_string().contains("error state"));
            }
            other => panic!("expected error, got: {}", match other {
                ConnResult::Response(_) => "response",
                ConnResult::Timeout => "timeout",
                ConnResult::Error { .. } => unreachable!(),
            }),
        }
    }

    #[test]
    fn test_request_on_errored_conn_rejected() {
        let port = drop_server();
        let mut pool = Pool::new().unwrap();
        let conn = pool.connect(format!("http://127.0.0.1:{port}").as_bytes()).unwrap();
        pool.get(conn, b"/").unwrap();
        // Drain the error to mark the connection as errored.
        let _ = pool.recv();
        // Now any subsequent request must return Err immediately.
        let result = pool.get(conn, b"/");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("error state"));
    }

    #[test]
    fn test_recv_timeout_fires() {
        let port = silent_server();
        let mut pool = Pool::new().unwrap();
        let conn = pool.connect(format!("http://127.0.0.1:{port}").as_bytes()).unwrap();
        pool.get(conn, b"/").unwrap();
        let result = pool.recv_timeout(std::time::Duration::from_millis(200));
        assert!(result.is_none(), "expected timeout, got a result");
    }

    #[test]
    fn test_recv_timeout_succeeds_when_server_responds() {
        let port = keep_alive_server();
        let mut pool = Pool::new().unwrap();
        let conn = pool.connect(format!("http://127.0.0.1:{port}").as_bytes()).unwrap();
        pool.get(conn, b"/").unwrap();
        let result = pool.recv_timeout(std::time::Duration::from_secs(5));
        match result {
            Some(ConnResult::Response(r)) => assert_eq!(&r.body, b"ok"),
            Some(ConnResult::Error { errno, .. }) => panic!("error errno={errno}"),
            Some(ConnResult::Timeout) => panic!("unexpected timeout variant"),
            None => panic!("timed out unexpectedly"),
        }
    }
}
