use std::io::Read;

use quetzalcoatl::spsc::{Consumer, Producer};

use xibalba_proto::error::Error;
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

// ── Size constants ────────────────────────────────────────────────────────────

pub const BLOCK_SIZE: usize = 8192;
pub const MAX_REQ_SIZE: usize = 8192;
pub const HEAD_BUF_SIZE: usize = 8192;
pub const MAX_HEADERS: usize = 64;
// Kernel read buffer — 8× BLOCK_SIZE so one syscall can fill multiple ring
// slots without re-entering the kernel. Lives on the io thread's stack.
const RAW_BUF_SIZE: usize = BLOCK_SIZE * 8;

// ── Wire messages ─────────────────────────────────────────────────────────────

/// Inline fixed-size request slot — written via `reserve_block`, zero copy.
pub struct IoRequest {
    pub buf: [u8; MAX_REQ_SIZE],
    pub len: usize,
}

/// `(name_start, name_len, val_start, val_len)` — byte offsets into `HeadData::head_buf`.
pub type HeaderRange = (u16, u16, u16, u16);

pub struct HeadData {
    pub version: Version,
    pub status: StatusCode,
    /// Raw bytes of the response head (status line + headers + \r\n\r\n).
    pub head_buf: [u8; HEAD_BUF_SIZE],
    pub head_len: usize,
    pub ranges: [HeaderRange; MAX_HEADERS],
    pub header_count: usize,
}

impl HeadData {
    /// Iterator over `(name, value)` byte slices borrowed from `head_buf`.
    /// Zero allocations.
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let buf = &self.head_buf[..self.head_len];
        self.ranges[..self.header_count].iter().map(move |&(ns, nl, vs, vl)| {
            (
                &buf[ns as usize..ns as usize + nl as usize],
                &buf[vs as usize..vs as usize + vl as usize],
            )
        })
    }
}

#[allow(clippy::large_enum_variant)] // inline by design — boxing defeats the zero-alloc goal
pub enum IoResponse {
    Head(HeadData),
    BodyChunk([u8; BLOCK_SIZE], usize),
    BodyDone,
    Error(Error),
}

// ── BodyReader ────────────────────────────────────────────────────────────────

pub struct BodyReader {
    rx: Consumer<IoResponse>,
    leftover: Option<([u8; BLOCK_SIZE], usize, usize)>, // (data, len, pos)
    done: bool,
}

impl BodyReader {
    pub(crate) const fn new(rx: Consumer<IoResponse>) -> Self {
        Self { rx, leftover: None, done: false }
    }

    pub(crate) fn into_consumer(self) -> Consumer<IoResponse> {
        self.rx
    }
}

fn append_chunk(
    msg: IoResponse,
    buf: &mut Vec<u8>,
    done: &mut bool,
    error: &mut Option<std::io::Error>,
) {
    match msg {
        IoResponse::BodyChunk(data, len) => buf.extend_from_slice(&data[..len]),
        IoResponse::BodyDone => *done = true,
        IoResponse::Head(_) => *error = Some(std::io::Error::other("unexpected Head during body read")),
        IoResponse::Error(e) => *error = Some(std::io::Error::other(e.to_string())),
    }
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.done || buf.is_empty() {
            return Ok(0);
        }

        if let Some((data, len, pos)) = &mut self.leftover {
            let available = &data[*pos..*len];
            let n = available.len().min(buf.len());
            buf[..n].copy_from_slice(&available[..n]);
            *pos += n;
            if *pos >= *len {
                self.leftover = None;
            }
            return Ok(n);
        }

        match self.rx.pop_block() {
            Some(IoResponse::BodyChunk(data, len)) => {
                let n = len.min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                if n < len {
                    self.leftover = Some((data, len, n));
                }
                Ok(n)
            }
            Some(IoResponse::BodyDone) | None => {
                self.done = true;
                Ok(0)
            }
            Some(IoResponse::Head(_)) => {
                Err(std::io::Error::other("unexpected Head during body read"))
            }
            Some(IoResponse::Error(e)) => Err(std::io::Error::other(e.to_string())),
        }
    }

    /// Drains the entire body into `buf`.
    ///
    /// Uses `pop_block` to wait for the next item, then `drain` to
    /// batch-consume everything already queued in the same burst —
    /// amortising the head.store to one per burst rather than one per chunk.
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        if self.done {
            return Ok(0);
        }

        // Flush any leftover bytes from a prior partial read first.
        if let Some((data, len, pos)) = self.leftover.take() {
            buf.extend_from_slice(&data[pos..len]);
        }

        let start_len = buf.len();
        let mut error: Option<std::io::Error> = None;
        let mut done = false;

        loop {
            // Block until at least one item is available.
            let Some(first) = self.rx.pop_block() else { break };
            append_chunk(first, buf, &mut done, &mut error);
            if done || error.is_some() {
                break;
            }
            // Batch-consume everything already queued — single head.store.
            self.rx.drain(|msg| append_chunk(msg, buf, &mut done, &mut error));
            if done || error.is_some() {
                break;
            }
        }

        self.done = true;
        if let Some(e) = error {
            return Err(e);
        }
        Ok(buf.len() - start_len)
    }
}

// ── io thread state machine ───────────────────────────────────────────────────

#[allow(clippy::large_enum_variant)]
enum IoState {
    Idle,
    ReadingHead,
    ReadingBody { framing: BodyFraming },
}

#[allow(clippy::large_enum_variant)]
enum BodyFraming {
    ContentLength { remaining: u64 },
    Chunked { decoder: xibalba_proto::response::ChunkedDecoder, leftover: [u8; RAW_BUF_SIZE], leftover_len: usize },
    UntilClose,
}

#[allow(clippy::too_many_lines, clippy::cognitive_complexity, clippy::large_stack_frames, clippy::large_stack_arrays)]
pub(crate) fn io_thread<S: Read + std::io::Write>(
    mut stream: S,
    rx_req: &mut Consumer<IoRequest>,
    tx_resp: &Producer<IoResponse>,
) {
    use xibalba_proto::header::Header;
    use xibalba_proto::response::{
        BodyFraming as ProtoFraming, ChunkedDecoder, DecodeResult, determine_body_framing,
        parse_response_head,
    };

    let mut state = IoState::Idle;
    // Persistent head read buffer — cleared per request, never reallocated.
    let mut head_buf: Vec<u8> = Vec::with_capacity(HEAD_BUF_SIZE);
    let mut raw_buf = [0u8; RAW_BUF_SIZE];

    'io: loop {
        match state {
            IoState::Idle => {
                let Some(req) = rx_req.pop_ref_block() else { break };
                if stream.write_all(&req.buf[..req.len]).is_err() || stream.flush().is_err() {
                    let _ = tx_resp.push_block(IoResponse::Error(
                        Error::Connection("write failed".into()),
                    ));
                    break;
                }
                drop(req);
                head_buf.clear();
                state = IoState::ReadingHead;
            }

            IoState::ReadingHead => {
                match stream.read(&mut raw_buf) {
                    Ok(0) => {
                        let _ = tx_resp.push_block(IoResponse::Error(Error::Connection(
                            "connection closed before headers complete".into(),
                        )));
                        break;
                    }
                    Ok(n) => head_buf.extend_from_slice(&raw_buf[..n]),
                    Err(e) => {
                        let _ = tx_resp.push_block(IoResponse::Error(e.into()));
                        break;
                    }
                }

                let Some(head_end) = find_header_end(&head_buf) else {
                    continue;
                };

                let head_bytes_len = head_end + 4;
                let mut hdr_buf = [const { Header::empty() }; MAX_HEADERS];
                let (head, consumed) =
                    match parse_response_head(&head_buf[..head_bytes_len], &mut hdr_buf) {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = tx_resp.push_block(IoResponse::Error(e));
                            break;
                        }
                    };

                let copy_len = head_bytes_len.min(HEAD_BUF_SIZE);
                let ranges = match build_ranges(&hdr_buf[..head.header_count], &head_buf[..copy_len]) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx_resp.push_block(IoResponse::Error(e));
                        break 'io;
                    }
                };
                let mut head_data = HeadData {
                    version: head.version,
                    status: head.status,
                    head_buf: [0u8; HEAD_BUF_SIZE],
                    head_len: head_bytes_len,
                    ranges,
                    header_count: head.header_count,
                };
                head_data.head_buf[..copy_len].copy_from_slice(&head_buf[..copy_len]);

                let framing = determine_body_framing(
                    head.status,
                    false,
                    &hdr_buf[..head.header_count],
                    head.header_count,
                );

                if tx_resp.push_block(IoResponse::Head(head_data)).is_err() {
                    break;
                }

                // Carry leftover bytes (past the head) into the body framing inline —
                // no Vec allocation.
                let tail = &head_buf[consumed..];
                let mut lo_buf = [0u8; RAW_BUF_SIZE];
                let lo_len = tail.len().min(RAW_BUF_SIZE);
                lo_buf[..lo_len].copy_from_slice(&tail[..lo_len]);

                state = IoState::ReadingBody {
                    framing: match framing {
                        ProtoFraming::None => {
                            let _ = tx_resp.push_block(IoResponse::BodyDone);
                            state = IoState::Idle;
                            continue;
                        }
                        ProtoFraming::ContentLength(n) => {
                            let remaining = n.saturating_sub(lo_len as u64);
                            if lo_len > 0 && push_chunk(tx_resp, &lo_buf[..lo_len]).is_err() {
                                break;
                            }
                            if remaining == 0 {
                                let _ = tx_resp.push_block(IoResponse::BodyDone);
                                state = IoState::Idle;
                                continue;
                            }
                            BodyFraming::ContentLength { remaining }
                        }
                        ProtoFraming::Chunked => BodyFraming::Chunked {
                            decoder: ChunkedDecoder::new(),
                            leftover: lo_buf,
                            leftover_len: lo_len,
                        },
                        ProtoFraming::UntilClose => {
                            if lo_len > 0 && push_chunk(tx_resp, &lo_buf[..lo_len]).is_err() {
                                break;
                            }
                            BodyFraming::UntilClose
                        }
                    },
                };
            }

            IoState::ReadingBody { ref mut framing } => {
                let done = match framing {
                    BodyFraming::ContentLength { remaining, .. } => {
                        match stream.read(&mut raw_buf) {
                            Ok(0) => {
                                let _ = tx_resp.push_block(IoResponse::Error(Error::Connection(
                                    "connection closed mid-body".into(),
                                )));
                                break;
                            }
                            Ok(n) => {
                                let to_send =
                                    usize::try_from((n as u64).min(*remaining)).unwrap_or(usize::MAX);
                                if push_chunk(tx_resp, &raw_buf[..to_send]).is_err() {
                                    break;
                                }
                                *remaining -= to_send as u64;
                                if *remaining == 0 {
                                    let _ = tx_resp.push_block(IoResponse::BodyDone);
                                    true
                                } else {
                                    false
                                }
                            }
                            Err(e) => {
                                let _ = tx_resp.push_block(IoResponse::Error(e.into()));
                                break;
                            }
                        }
                    }

                    BodyFraming::Chunked { decoder, leftover, leftover_len } => {
                        if decoder.is_done() {
                            let _ = tx_resp.push_block(IoResponse::BodyDone);
                            true
                        } else if *leftover_len > 0 {
                            let mut out = [0u8; BLOCK_SIZE];
                            let (result, consumed) = decoder.decode(&leftover[..*leftover_len], &mut out);
                            // Drain consumed bytes by shifting remaining bytes to the front.
                            *leftover_len -= consumed;
                            leftover.copy_within(consumed..consumed + *leftover_len, 0);
                            match result {
                                DecodeResult::Data(n) => {
                                    if push_chunk(tx_resp, &out[..n]).is_err() {
                                        break;
                                    }
                                    false
                                }
                                DecodeResult::Done => {
                                    let _ = tx_resp.push_block(IoResponse::BodyDone);
                                    true
                                }
                                DecodeResult::NeedMore => false,
                                DecodeResult::Error(e) => {
                                    let _ = tx_resp.push_block(IoResponse::Error(Error::Parse(e)));
                                    break;
                                }
                            }
                        } else {
                            match stream.read(&mut raw_buf) {
                                Ok(0) => {
                                    let _ = tx_resp.push_block(IoResponse::Error(Error::Connection(
                                        "connection closed mid-chunk".into(),
                                    )));
                                    break;
                                }
                                Ok(n) => {
                                    let mut out = [0u8; BLOCK_SIZE];
                                    let (result, consumed) =
                                        decoder.decode(&raw_buf[..n], &mut out);
                                    if consumed < n {
                                        let tail = n - consumed;
                                        leftover[*leftover_len..*leftover_len + tail]
                                            .copy_from_slice(&raw_buf[consumed..n]);
                                        *leftover_len += tail;
                                    }
                                    match result {
                                        DecodeResult::Data(n) => {
                                            if push_chunk(tx_resp, &out[..n]).is_err() {
                                                break;
                                            }
                                            false
                                        }
                                        DecodeResult::Done => {
                                            let _ = tx_resp.push_block(IoResponse::BodyDone);
                                            true
                                        }
                                        DecodeResult::NeedMore => false,
                                        DecodeResult::Error(e) => {
                                            let _ = tx_resp
                                                .push_block(IoResponse::Error(Error::Parse(e)));
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = tx_resp.push_block(IoResponse::Error(e.into()));
                                    break;
                                }
                            }
                        }
                    }

                    BodyFraming::UntilClose => match stream.read(&mut raw_buf) {
                        Ok(0) => {
                            let _ = tx_resp.push_block(IoResponse::BodyDone);
                            true
                        }
                        Ok(n) => {
                            if push_chunk(tx_resp, &raw_buf[..n]).is_err() {
                                break;
                            }
                            false
                        }
                        Err(e) => {
                            let _ = tx_resp.push_block(IoResponse::Error(e.into()));
                            break;
                        }
                    },
                };
                if done {
                    state = IoState::Idle;
                }
            }
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn push_chunk(tx: &Producer<IoResponse>, data: &[u8]) -> Result<(), ()> {
    for slice in data.chunks(BLOCK_SIZE) {
        let mut chunk = [0u8; BLOCK_SIZE];
        chunk[..slice.len()].copy_from_slice(slice);
        tx.push_block(IoResponse::BodyChunk(chunk, slice.len())).map_err(|_| ())?;
    }
    Ok(())
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Compute `HeaderRange` offsets for each parsed header against `src`.
fn build_ranges(
    headers: &[xibalba_proto::header::Header<'_>],
    src: &[u8],
) -> Result<[HeaderRange; MAX_HEADERS], Error> {
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
            find_subsequence(src, name)
                .ok_or_else(|| Error::Connection("header name not found in head buffer".into()))?
        };

        ranges[i] = (
            u16::try_from(ns_off)
                .map_err(|_| Error::Connection("header name offset overflows u16".into()))?,
            u16::try_from(name.len())
                .map_err(|_| Error::Connection("header name length overflows u16".into()))?,
            u16::try_from(vs_off)
                .map_err(|_| Error::Connection("header value offset overflows u16".into()))?,
            u16::try_from(value.len())
                .map_err(|_| Error::Connection("header value length overflows u16".into()))?,
        );
    }

    Ok(ranges)
}
