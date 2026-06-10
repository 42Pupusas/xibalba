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
    Length { remaining: u64 },
    Chunked { decoder: xibalba_proto::response::ChunkedDecoder },
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
            let n = self.stream.read(&mut self.raw)?;
            self.raw.truncate(n);
            self.raw_pos = 0;
        }
        Ok(&self.raw[self.raw_pos..])
    }
}

impl<S: Read> Read for StreamingBody<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use xibalba_proto::response::DecodeResult;

        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            match self.state {
                StreamState::Done => return Ok(0),

                StreamState::Length { remaining } => {
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
                    let remaining = remaining - n as u64;
                    if remaining == 0 {
                        self.finish();
                    } else {
                        self.state = StreamState::Length { remaining };
                    }
                    return Ok(n);
                }

                StreamState::UntilClose => {
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
                    return Ok(n);
                }

                StreamState::Chunked { ref mut decoder } => {
                    if self.raw_pos >= self.raw.len() {
                        self.raw.resize(HEAD_BUF_SIZE, 0);
                        let n = self.stream.read(&mut self.raw)?;
                        self.raw.truncate(n);
                        self.raw_pos = 0;
                        if n == 0 {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "connection closed mid-chunked-body",
                            ));
                        }
                    }
                    let (result, consumed) = decoder.decode(&self.raw[self.raw_pos..], buf);
                    self.raw_pos += consumed;
                    match result {
                        DecodeResult::Data(n) => {
                            if decoder.is_done() {
                                self.finish();
                            }
                            return Ok(n);
                        }
                        DecodeResult::Done => {
                            self.finish();
                            return Ok(0);
                        }
                        DecodeResult::NeedMore => {}
                        DecodeResult::Error(e) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                Error::Parse(e).to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }
}

// ── Inline read helpers ──────────────────────────────────────────────────────

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
        let n = stream.read(&mut raw)?;
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
    use xibalba_proto::response::{BodyFraming as ProtoFraming, ChunkedDecoder, DecodeResult};

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
                stream.read_exact(&mut body[from_tail..])?;
            }
            Ok(body)
        }

        ProtoFraming::Chunked => {
            let mut decoder = ChunkedDecoder::new();
            let mut body = Vec::new();
            let mut raw = [0u8; HEAD_BUF_SIZE];
            let mut decode_buf = [0u8; HEAD_BUF_SIZE];

            let mut input = tail;
            loop {
                let (result, consumed) = decoder.decode(input, &mut decode_buf);
                input = &input[consumed..];
                match result {
                    DecodeResult::Data(n) => {
                        body.extend_from_slice(&decode_buf[..n]);
                        if body.len() > max_body {
                            return Err(ConnectionError::BodyTooLarge.into());
                        }
                        // `Data` takes priority over `Done` in the decoder's
                        // return value; without this check a read that ends
                        // exactly at the terminal chunk would block forever
                        // waiting for input that never comes.
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
                let n = stream.read(&mut raw)?;
                if n == 0 {
                    return Err(ConnectionError::ConnectionClosed.into());
                }
                let mut pos = 0;
                while pos < n {
                    let (result, consumed) = decoder.decode(&raw[pos..n], &mut decode_buf);
                    pos += consumed;
                    match result {
                        DecodeResult::Data(dn) => {
                            body.extend_from_slice(&decode_buf[..dn]);
                            if body.len() > max_body {
                                return Err(ConnectionError::BodyTooLarge.into());
                            }
                            // See the tail loop above: `Data` masks `Done`,
                            // and this loop only re-enters the decoder while
                            // unconsumed bytes remain in `raw`.
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

        ProtoFraming::UntilClose => {
            let mut body = tail.to_vec();
            let mut raw = [0u8; HEAD_BUF_SIZE];
            loop {
                let n = stream.read(&mut raw)?;
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
