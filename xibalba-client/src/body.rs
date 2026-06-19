use std::io::Read;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::response::HeaderRange;
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

pub const HEAD_BUF_SIZE: usize = 8192;
pub use xibalba_proto::response::MAX_HEADERS;

#[derive(Debug)]
pub struct HeadData {
    pub version: Version,
    pub status: StatusCode,
    pub head_buf: [u8; HEAD_BUF_SIZE],
    pub head_len: usize,
    pub ranges: [HeaderRange; MAX_HEADERS],
    pub header_count: usize,
}

impl HeadData {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let buf = &self.head_buf[..self.head_len];
        self.ranges[..self.header_count]
            .iter()
            .filter_map(move |r| {
                let ns = r.name_start as usize;
                let vs = r.value_start as usize;
                Some((
                    buf.get(ns..ns + r.name_len as usize)?,
                    buf.get(vs..vs + r.value_len as usize)?,
                ))
            })
    }
}

/// In-memory body reader with a specialised `read_to_end` that does a single
/// `extend_from_slice` instead of reading through the `Read` trait.
#[derive(Debug)]
pub struct BodyReader {
    data: Vec<u8>,
    pos: usize,
}

impl BodyReader {
    pub(crate) const fn new(data: Vec<u8>) -> Self {
        Self { data, pos: 0 }
    }
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.data[self.pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        Ok(n)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let remaining = &self.data[self.pos..];
        let n = remaining.len();
        buf.extend_from_slice(remaining);
        self.pos = self.data.len();
        Ok(n)
    }
}

/// Incremental body reader over a live connection stream.
///
/// Unlike [`BodyReader`], which is handed a fully-buffered body, this
/// decodes framing (chunked / content-length / until-close) on the fly
/// as the caller reads — required for server-sent events, where the
/// response only ends after the server is done generating.
///
/// Holds `&mut` borrows of the client's stream and its dirty flag for
/// the duration of the response. The flag is cleared when the body is
/// read to completion; if the reader is dropped early, the flag stays
/// set and the client reconnects before its next request instead of
/// reading stale bytes.
#[derive(Debug)]
pub struct StreamingBody<'a, S: Read> {
    stream: &'a mut S,
    dirty: &'a mut bool,
    state: StreamState,
    /// Raw bytes already pulled off the socket but not yet decoded:
    /// first the head-read overshoot, then refills from `stream`.
    raw: Vec<u8>,
    raw_pos: usize,
}

#[derive(Debug)]
enum StreamState {
    Length {
        remaining: u64,
    },
    Chunked {
        decoder: xibalba_proto::response::ChunkedDecoder,
    },
    UntilClose,
    Done,
}

impl<'a, S: Read> StreamingBody<'a, S> {
    pub(crate) const fn new(
        stream: &'a mut S,
        dirty: &'a mut bool,
        framing: &xibalba_proto::response::BodyFraming,
        tail: Vec<u8>,
    ) -> Self {
        use xibalba_proto::response::{BodyFraming, ChunkedDecoder};
        let state = match *framing {
            BodyFraming::ContentLength(0) | BodyFraming::None => StreamState::Done,
            BodyFraming::ContentLength(len) => StreamState::Length { remaining: len },
            BodyFraming::Chunked => StreamState::Chunked {
                decoder: ChunkedDecoder::new(),
            },
            BodyFraming::UntilClose => StreamState::UntilClose,
        };
        let mut body = Self {
            stream,
            dirty,
            state,
            raw: tail,
            raw_pos: 0,
        };
        if matches!(body.state, StreamState::Done) {
            body.finish();
        }
        body
    }

    /// Whether the body has been fully consumed.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        matches!(self.state, StreamState::Done)
    }

    const fn finish(&mut self) {
        self.state = StreamState::Done;
        // An until-close body ends with a dead connection; everything
        // else leaves it positioned at the next response.
        *self.dirty = false;
    }

    /// Bytes available without touching the socket; refills from the
    /// socket when empty. `Ok(&[])` means clean EOF from the peer.
    fn input(&mut self) -> std::io::Result<&[u8]> {
        if self.raw_pos >= self.raw.len() {
            self.raw.resize(HEAD_BUF_SIZE, 0);
            let n = read_retry(&mut self.stream, &mut self.raw)?;
            self.raw.truncate(n);
            self.raw_pos = 0;
        }
        Ok(&self.raw[self.raw_pos..])
    }
}

impl<S: Read> Read for StreamingBody<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        match self.state {
            StreamState::Done => Ok(0),
            StreamState::Length { remaining } => self.read_content_length(buf, remaining),
            StreamState::UntilClose => self.read_until_close(buf),
            StreamState::Chunked { .. } => match self.read_chunked(buf)? {
                ChunkedRead::Progress(n) => Ok(n),
                ChunkedRead::NeedMore => Ok(0),
            },
        }
    }
}

enum ChunkedRead {
    Progress(usize),
    NeedMore,
}

impl<S: Read> StreamingBody<'_, S> {
    fn read_content_length(&mut self, buf: &mut [u8], remaining: u64) -> std::io::Result<usize> {
        let input = self.input()?;
        if input.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-body",
            ));
        }
        let n = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(input.len())
            .min(buf.len());
        buf[..n].copy_from_slice(&input[..n]);
        self.raw_pos += n;
        let remaining = remaining - u64::try_from(n).unwrap_or(u64::MAX);
        if remaining == 0 {
            self.finish();
        } else {
            self.state = StreamState::Length { remaining };
        }
        Ok(n)
    }

    fn read_until_close(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let input = self.input()?;
        if input.is_empty() {
            self.state = StreamState::Done;
            // Connection is dead; leave the dirty flag set
            // so the client reconnects.
            return Ok(0);
        }
        let n = input.len().min(buf.len());
        buf[..n].copy_from_slice(&input[..n]);
        self.raw_pos += n;
        Ok(n)
    }

    fn read_chunked(&mut self, buf: &mut [u8]) -> std::io::Result<ChunkedRead> {
        use xibalba_proto::response::DecodeResult;

        if self.raw_pos >= self.raw.len() {
            self.raw.resize(HEAD_BUF_SIZE, 0);
            let n = read_retry(&mut self.stream, &mut self.raw)?;
            self.raw.truncate(n);
            self.raw_pos = 0;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed mid-chunked-body",
                ));
            }
        }

        let StreamState::Chunked { decoder } = &mut self.state else {
            unreachable!("read_chunked called outside Chunked state");
        };

        let (result, consumed) = decoder.decode(&self.raw[self.raw_pos..], buf);
        self.raw_pos += consumed;
        match result {
            DecodeResult::Data(n) => {
                if decoder.is_done() {
                    self.finish();
                }
                Ok(ChunkedRead::Progress(n))
            }
            DecodeResult::Done => {
                self.finish();
                Ok(ChunkedRead::Progress(0))
            }
            DecodeResult::NeedMore => Ok(ChunkedRead::NeedMore),
            DecodeResult::Error(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                Error::Parse(e).to_string(),
            )),
        }
    }
}

fn read_body_chunked<S: Read>(
    stream: &mut S,
    tail: &[u8],
    max_body: usize,
) -> Result<Vec<u8>, Error> {
    use xibalba_proto::response::{ChunkedDecoder, DecodeResult};

    let mut decoder = ChunkedDecoder::new();
    let mut body = Vec::new();
    let mut raw = [0u8; HEAD_BUF_SIZE];

    let mut input = tail;
    loop {
        let mut decode_buf = [0u8; HEAD_BUF_SIZE];
        let (result, consumed) = decoder.decode(input, &mut decode_buf);
        input = &input[consumed..];
        match result {
            DecodeResult::Data(n) => {
                body.extend_from_slice(&decode_buf[..n]);
                if body.len() > max_body {
                    return Err(ConnectionError::BodyTooLarge.into());
                }
                if decoder.is_done() {
                    return Ok(body);
                }
            }
            DecodeResult::Done => return Ok(body),
            DecodeResult::NeedMore => break,
            DecodeResult::Error(e) => return Err(Error::Parse(e)),
        }
    }

    loop {
        let n = read_retry(stream, &mut raw)?;
        if n == 0 {
            return Err(ConnectionError::ConnectionClosed.into());
        }
        let mut pos = 0;
        while pos < n {
            let mut decode_buf = [0u8; HEAD_BUF_SIZE];
            let (result, consumed) = decoder.decode(&raw[pos..n], &mut decode_buf);
            pos += consumed;
            match result {
                DecodeResult::Data(dn) => {
                    body.extend_from_slice(&decode_buf[..dn]);
                    if body.len() > max_body {
                        return Err(ConnectionError::BodyTooLarge.into());
                    }
                    if decoder.is_done() {
                        return Ok(body);
                    }
                }
                DecodeResult::Done => return Ok(body),
                DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => return Err(Error::Parse(e)),
            }
        }
    }
}

// ── Inline read helpers ──────────────────────────────────────────────────────

/// How many consecutive [`std::io::ErrorKind::WouldBlock`] timeouts to
/// tolerate before giving up. A slow API that takes 8s to send headers
/// with a 5s per-read ceiling needs one retry; a genuinely stalled
/// server exhausts these quickly (3 × timeout).
const MAX_WOULDBLOCK_RETRIES: u32 = 3;

/// Read from `stream`, retrying on [`std::io::ErrorKind::WouldBlock`]
/// up to [`MAX_WOULDBLOCK_RETRIES`] times.
///
/// A blocking socket with `SO_RCVTIMEO` returns `EAGAIN`/`WouldBlock` when
/// no data arrives within the timeout window. The timeout is a ceiling, not
/// a failure — retrying resets the timer and waits for the next window.
/// This is what [`super::CancellableStream`] already does for the streaming
/// body path; this helper extends the same treatment to the head-read and
/// synchronous body-read paths, with a cap so a dead server doesn't hang
/// the caller forever.
fn read_retry(stream: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut attempts = 0u32;
    loop {
        match stream.read(buf) {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                attempts += 1;
                if attempts > MAX_WOULDBLOCK_RETRIES {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Like [`std::io::Read::read_exact`] but retries on `WouldBlock`.
fn read_exact_retry(stream: &mut impl Read, buf: &mut [u8]) -> std::io::Result<()> {
    let mut off = 0;
    while off < buf.len() {
        let n = read_retry(stream, &mut buf[off..])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-body",
            ));
        }
        off += n;
    }
    Ok(())
}

/// Returns `(HeadData, framing, tail_offset)`. The tail bytes (body prefix
/// that arrived with the head read) live in `head_acc[tail_offset..]`.
pub(crate) fn read_response_head<S: Read>(
    stream: &mut S,
    head_acc: &mut Vec<u8>,
    max_head: usize,
) -> Result<(HeadData, xibalba_proto::response::BodyFraming, usize), Error> {
    use xibalba_proto::header::Header;
    use xibalba_proto::response::{build_ranges, determine_body_framing, parse_response_head};

    head_acc.clear();
    let mut raw = [0u8; HEAD_BUF_SIZE];

    let head_end = loop {
        let n = read_retry(stream, &mut raw)?;
        if n == 0 {
            return Err(ConnectionError::ConnectionClosed.into());
        }
        head_acc.extend_from_slice(&raw[..n]);
        if head_acc.len() > max_head {
            return Err(ConnectionError::HeadTooLarge.into());
        }
        if let Some(pos) = head_acc.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };

    let mut hdr_buf = [const { Header::empty() }; MAX_HEADERS];
    let (head, consumed) = parse_response_head(&head_acc[..head_end], &mut hdr_buf)?;

    let copy_len = head_end.min(HEAD_BUF_SIZE);
    let ranges = build_ranges(&hdr_buf[..head.header_count], &head_acc[..copy_len])?;
    let mut head_data = HeadData {
        version: head.version,
        status: head.status,
        head_buf: [0u8; HEAD_BUF_SIZE],
        head_len: head_end,
        ranges,
        header_count: head.header_count,
    };
    head_data.head_buf[..copy_len].copy_from_slice(&head_acc[..copy_len]);

    let framing = determine_body_framing(
        head.status,
        false,
        &hdr_buf[..head.header_count],
        head.header_count,
    );

    Ok((head_data, framing, consumed))
}

pub(crate) fn read_body<S: Read>(
    stream: &mut S,
    framing: &xibalba_proto::response::BodyFraming,
    tail: &[u8],
    max_body: usize,
) -> Result<Vec<u8>, Error> {
    use xibalba_proto::response::BodyFraming as ProtoFraming;

    match *framing {
        ProtoFraming::None => Ok(Vec::new()),

        ProtoFraming::ContentLength(len) => {
            let len = usize::try_from(len)
                .map_err(|_| Error::from(ConnectionError::ContentLengthOverflow))?;
            if len > max_body {
                return Err(ConnectionError::BodyTooLarge.into());
            }
            let mut body = Vec::with_capacity(len);
            let from_tail = tail.len().min(len);
            body.extend_from_slice(&tail[..from_tail]);
            if body.len() < len {
                body.resize(len, 0);
                read_exact_retry(stream, &mut body[from_tail..])?;
            }
            Ok(body)
        }

        ProtoFraming::Chunked => read_body_chunked(stream, tail, max_body),

        ProtoFraming::UntilClose => {
            let mut body = tail.to_vec();
            let mut raw = [0u8; HEAD_BUF_SIZE];
            loop {
                let n = read_retry(stream, &mut raw)?;
                if n == 0 {
                    return Ok(body);
                }
                body.extend_from_slice(&raw[..n]);
                if body.len() > max_body {
                    return Err(ConnectionError::BodyTooLarge.into());
                }
            }
        }
    }
}
