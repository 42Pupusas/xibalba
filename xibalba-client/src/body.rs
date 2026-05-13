use std::io::Read;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::response::HeaderRange;
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

pub const HEAD_BUF_SIZE: usize = 8192;
pub use xibalba_proto::response::MAX_HEADERS;

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
        self.ranges[..self.header_count].iter().filter_map(move |r| {
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

// ── Inline read helpers ──────────────────────────────────────────────────────

/// Returns `(HeadData, framing, tail_offset)`. The tail bytes (body prefix
/// that arrived with the head read) live in `head_acc[tail_offset..]`.
pub(crate) fn read_response_head<S: Read>(
    stream: &mut S,
    head_acc: &mut Vec<u8>,
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
) -> Result<Vec<u8>, Error> {
    use xibalba_proto::response::{BodyFraming as ProtoFraming, ChunkedDecoder, DecodeResult};

    match *framing {
        ProtoFraming::None => Ok(Vec::new()),

        ProtoFraming::ContentLength(len) => {
            let len = usize::try_from(len)
                .map_err(|_| Error::from(ConnectionError::ContentLengthOverflow))?;
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
                    DecodeResult::Data(n) => body.extend_from_slice(&decode_buf[..n]),
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
                        DecodeResult::Data(dn) => body.extend_from_slice(&decode_buf[..dn]),
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
            }
        }
    }
}
