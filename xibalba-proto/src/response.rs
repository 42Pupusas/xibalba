use crate::bytes::ByteSliceExt;
use crate::coding::{TransferCoding, TransferCodings};
use crate::error::{ConnectionError, Error, ParseError};
use crate::header::{Header, HeaderName, Tchar};
use crate::method::Method;
use crate::status::StatusCode;
use crate::version::Version;

/// Result of parsing the response head (status line + headers).
#[derive(Debug)]
pub struct ResponseHead<'a> {
    pub version: Version,
    pub status: StatusCode,
    pub reason: &'a [u8],
    /// Number of headers parsed into the caller's buffer.
    pub header_count: usize,
}

impl<'a> ResponseHead<'a> {
    /// The headers this head parsed, taken from the buffer that was passed
    /// to [`Self::parse`].
    ///
    /// Pairing the count with its own buffer is what makes an inconsistent
    /// pair possible; this narrows the buffer once, at the point where the
    /// count is known to describe it.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::TooManyHeaders`] if `headers` is shorter than
    /// the count, which means it is not the buffer that was parsed into.
    pub fn headers<'h>(&self, headers: &'h [Header<'a>]) -> Result<&'h [Header<'a>], ParseError> {
        headers
            .get(..self.header_count)
            .ok_or(ParseError::TooManyHeaders)
    }

    /// Parse the response head from `buf`.
    ///
    /// `headers` is a caller-provided buffer that will be filled with parsed headers.
    ///
    /// On success, returns `(ResponseHead, bytes_consumed)`.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::Incomplete` if the buffer doesn't contain a complete
    /// response head. Returns other `ParseError` variants for malformed input.
    pub fn parse(buf: &'a [u8], headers: &mut [Header<'a>]) -> Result<(Self, usize), Error> {
        let status_line_end = buf.find_crlf().ok_or(ParseError::Incomplete)?;
        let status_line = &buf[..status_line_end];

        if status_line.len() < 12 {
            return Err(ParseError::InvalidVersion.into());
        }

        let version = Version::try_from(&status_line[..8])?;

        if status_line[8] != b' ' {
            return Err(ParseError::InvalidVersion.into());
        }

        let status = StatusCode::try_from(&status_line[9..12])?;

        let reason = match status_line.get(12) {
            None => b"".as_slice(),
            Some(b' ') => &status_line[13..],
            Some(_) => return Err(ParseError::InvalidStatusCode.into()),
        };
        // reason-phrase = *( HTAB / SP / VCHAR / obs-text ). CR and LF cannot
        // appear (the line ended at CRLF), but NUL and the other C0 controls
        // otherwise reach the caller inside a field the grammar forbids them
        // from occupying.
        if reason
            .iter()
            .any(|&b| b != b'\t' && (b < 0x20 || b == 0x7f))
        {
            return Err(ParseError::InvalidReasonPhrase.into());
        }

        let mut pos = status_line_end + 2;
        let mut count = 0;

        loop {
            if pos >= buf.len() {
                return Err(ParseError::Incomplete.into());
            }

            if buf[pos] == b'\r' {
                // A lone trailing CR is the head terminator with its LF still
                // in flight, not a header line beginning with a control byte.
                if pos + 1 >= buf.len() {
                    return Err(ParseError::Incomplete.into());
                }
                if buf[pos + 1] == b'\n' {
                    pos += 2;
                    break;
                }
            }

            // Single left-to-right scan: validate name tchar-by-tchar, find ':', then
            // scan value bytes until \r\n — all in one pass with no restarts.
            let (name_bytes, value, line_end) = Self::parse_header_line(&buf[pos..])?;

            if count >= headers.len() {
                return Err(ParseError::TooManyHeaders.into());
            }
            headers[count] = Header {
                name: HeaderName::raw(name_bytes),
                value,
            };
            count += 1;

            pos += line_end;
        }

        Ok((
            Self {
                version,
                status,
                reason,
                header_count: count,
            },
            pos,
        ))
    }

    /// Single-pass header line parser.
    ///
    /// Scans `line` left-to-right once: validates name tchars, finds ':', then
    /// finds the terminating CRLF, trimming OWS from the value along the way.
    /// Returns `(name_bytes, value, bytes_consumed_including_crlf)`.
    fn parse_header_line(line: &[u8]) -> Result<(&[u8], &[u8], usize), Error> {
        if line.is_empty() {
            return Err(ParseError::InvalidHeaderName.into());
        }

        // Phase 1: scan name, validating tchars and stopping at ':'. Iterating
        // rather than indexing keeps one bounds check per byte instead of two.
        //
        // Validation must run in the same loop as the ':' search, not after it.
        // Searching for ':' alone would run past this line's CRLF and into
        // later headers when a name is malformed; stopping at the first
        // non-tchar keeps a bad line from consuming the rest of the buffer.
        let mut name_len = None;
        for (idx, &b) in line.iter().enumerate() {
            if b == b':' {
                name_len = Some(idx);
                break;
            }
            if !Tchar::is_valid(b) {
                return Err(ParseError::InvalidHeaderName.into());
            }
        }
        // Every byte so far was a valid tchar and the input ran out, so the
        // colon may still be on its way. Reporting a malformed line here
        // would make an incremental caller reject a response that is merely
        // still in flight; a terminated line without a colon fails above on
        // CR, which is not a tchar.
        let mut i = name_len.ok_or(ParseError::Incomplete)?;
        if i == 0 {
            return Err(ParseError::InvalidHeaderName.into());
        }
        let name_bytes = &line[..i];
        i += 1; // skip ':'

        // Phase 2: skip leading OWS
        while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        let value_start = i;

        // Phase 3: one vectorizable pass locates the CR *and* validates the
        // value. CR is itself a control byte, so the first byte matching the
        // control-character predicate is either the terminating CR or an
        // illegal byte — a separate validation pass would re-read the same
        // bytes to learn what this one already knows. Tab is legal inside a
        // value, and obs-text (>= 0x80) is accepted per RFC 9110.
        let ctl_pos = line[i..]
            .iter()
            .position(|&b| (b < 0x20 || b == 0x7f) && b != b'\t')
            .ok_or(ParseError::Incomplete)?;
        let crlf = i + ctl_pos;
        if line[crlf] != b'\r' {
            return Err(ParseError::InvalidHeaderValue.into());
        }
        if crlf + 1 >= line.len() || line[crlf + 1] != b'\n' {
            return Err(ParseError::Incomplete.into());
        }

        // Trailing OWS is rare, so test the last byte before walking backwards;
        // the scan is skipped entirely for the overwhelmingly common value.
        let raw_value = &line[value_start..crlf];
        let trimmed = match raw_value.last() {
            Some(&b' ' | &b'\t') => {
                let value_end = raw_value
                    .iter()
                    .rposition(|&b| b != b' ' && b != b'\t')
                    .map_or(0, |p| p + 1);
                &raw_value[..value_end]
            }
            #[allow(clippy::match_same_arms)]
            None | Some(_) => raw_value,
        };

        Ok((name_bytes, trimmed, crlf + 2))
    }
}

/// How the response body is framed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyFraming {
    /// Body length is given by `Content-Length`.
    ContentLength(u64),
    /// Body uses chunked `Transfer-Encoding`.
    Chunked,
    /// Body ends when the connection closes.
    UntilClose,
    /// No body (1xx, 204, 304, or HEAD response).
    None,
}

impl BodyFraming {
    /// Determine body framing from the request method, response status, and
    /// headers.
    ///
    /// The method is part of framing, not merely context: RFC 9110 §6.4.2
    /// makes a HEAD response bodiless whatever its headers say, and §9.3.6
    /// does the same for a 2xx answer to CONNECT, where the bytes after the
    /// head belong to the tunnel rather than to HTTP. Both cases carry
    /// `Content-Length` or `Transfer-Encoding` values that describe a message
    /// that was never sent, and §9.3.6 requires a client to ignore them.
    ///
    /// Any `Transfer-Encoding` overrides `Content-Length` (RFC 9112
    /// §6.3): when `chunked` is the final coding the body is chunked;
    /// when it is absent or not final the body runs until close, since
    /// a `Content-Length` next to a transfer coding is a smuggling
    /// vector. Multiple `Content-Length` headers are accepted only when
    /// every value parses to the same number.
    ///
    /// `headers` must be exactly the parsed headers. The count is the
    /// slice's own length rather than a separate argument: a redundant count
    /// can disagree with the slice, and this function panicked on an
    /// oversized one despite returning `Result`. Callers holding a larger
    /// buffer pass `&buf[..head.header_count]`.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::InvalidContentLength`] when any
    /// `Content-Length` value is not valid digits or two of them
    /// disagree.
    pub fn from_response(
        status: StatusCode,
        request_method: Method,
        headers: &[Header<'_>],
    ) -> Result<Self, ParseError> {
        if status.is_informational()
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED
            || !request_method.response_can_have_content(status)
        {
            return Ok(Self::None);
        }

        match TransferCodings::parse(headers)? {
            TransferCoding::Chunked => return Ok(Self::Chunked),
            // A header applying no encoding still takes precedence over
            // Content-Length, and leaves the body delimited by the close.
            TransferCoding::Identity => return Ok(Self::UntilClose),
            TransferCoding::Absent => {}
        }

        let mut content_length: Option<u64> = None;
        for h in headers {
            if h.name == HeaderName::ContentLength {
                let len = h
                    .value
                    .parse_u64()
                    .ok_or(ParseError::InvalidContentLength)?;
                if content_length.is_some_and(|prev| prev != len) {
                    return Err(ParseError::InvalidContentLength);
                }
                content_length = Some(len);
            }
        }

        if let Some(len) = content_length {
            return Ok(Self::ContentLength(len));
        }

        Ok(Self::UntilClose)
    }
}

// --- Chunked transfer decoder state machine ---

/// Largest chunk extension, in bytes, accepted on a single chunk.
pub const MAX_CHUNK_EXTENSION: usize = 4096;

/// Largest trailer section, in bytes, accepted after the final chunk.
pub const MAX_TRAILER_SECTION: usize = 8192;

/// Bounds the metadata a peer may send between body bytes.
///
/// Extensions and trailers carry no payload, so a peer that streams them
/// indefinitely keeps a request occupied without ever making progress. The
/// budget is charged per byte and refuses the stream once exhausted.
#[derive(Debug, Clone)]
struct MetadataBudget {
    extension: usize,
    trailer: usize,
}

impl MetadataBudget {
    const fn new() -> Self {
        Self {
            extension: 0,
            trailer: 0,
        }
    }

    /// Charge one extension byte. Reset per chunk, since each chunk is
    /// allowed its own extension.
    const fn charge_extension(&mut self) -> bool {
        self.extension += 1;
        self.extension <= MAX_CHUNK_EXTENSION
    }

    /// Charge one trailer byte. Not reset: the whole trailer section shares
    /// one budget, so many small trailer lines cannot evade it.
    const fn charge_trailer(&mut self) -> bool {
        self.trailer += 1;
        self.trailer <= MAX_TRAILER_SECTION
    }

    const fn reset_extension(&mut self) {
        self.extension = 0;
    }
}

/// State machine for decoding chunked `Transfer-Encoding`.
#[derive(Debug, Clone)]
pub struct ChunkedDecoder {
    state: ChunkedState,
    chunk_size: u64,
    saw_size_digit: bool,
    remaining: u64,
    trailer_line_empty: bool,
    metadata: MetadataBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkedState {
    ReadingSize,
    ReadingExtension,
    ReadingSizeLf,
    ReadingData,
    ReadingDataCr,
    ReadingDataLf,
    ReadingTrailer,
    ReadingTrailerLf,
    Done,
    /// A verdict already reached, kept so it can be reported after the data
    /// that preceded it in the same call.
    Failed(ParseError),
}

/// What the chunked decoder wants the caller to do next.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeResult {
    /// `n` bytes of decoded body data are in the output buffer.
    Data(usize),
    /// Need more input data.
    NeedMore,
    /// The chunked stream is complete.
    Done,
    /// A parse error occurred.
    Error(ParseError),
}

impl Default for ChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedDecoder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: ChunkedState::ReadingSize,
            chunk_size: 0,
            saw_size_digit: false,
            remaining: 0,
            trailer_line_empty: true,
            metadata: MetadataBudget::new(),
        }
    }

    #[must_use]
    pub const fn is_done(&self) -> bool {
        matches!(self.state, ChunkedState::Done)
    }

    /// Decode chunked data from `input` into `output`.
    ///
    /// Returns `(DecodeResult, bytes_consumed_from_input)`.
    pub fn decode(&mut self, input: &[u8], output: &mut [u8]) -> (DecodeResult, usize) {
        let mut in_pos = 0;
        let mut out_pos = 0;

        if let ChunkedState::Failed(e) = self.state {
            return (DecodeResult::Error(e), 0);
        }

        while in_pos < input.len() {
            match self.state {
                ChunkedState::ReadingSize
                | ChunkedState::ReadingExtension
                | ChunkedState::ReadingSizeLf => match self.read_size_line(&input[in_pos..]) {
                    Step::Advance(n) => in_pos += n,
                    Step::Yield((result, consumed)) => {
                        return self.deliver(result, in_pos + consumed, out_pos);
                    }
                    Step::EmitAndContinue(n) => {
                        in_pos += n;
                        if out_pos > 0 {
                            return (DecodeResult::Data(out_pos), in_pos);
                        }
                    }
                },
                ChunkedState::ReadingData => {
                    match self.read_data(&input[in_pos..], &mut output[out_pos..]) {
                        DataStep::Copied(n) => {
                            in_pos += n;
                            out_pos += n;
                        }
                        DataStep::BufferFull => {
                            return (DecodeResult::Data(out_pos), in_pos);
                        }
                    }
                }
                ChunkedState::ReadingDataCr | ChunkedState::ReadingDataLf => {
                    match self.read_data_terminator(&input[in_pos..]) {
                        Step::Advance(n) => in_pos += n,
                        Step::Yield((result, consumed)) => {
                            return self.deliver(result, in_pos + consumed, out_pos);
                        }
                        Step::EmitAndContinue(_) => unreachable!(),
                    }
                }
                ChunkedState::ReadingTrailer | ChunkedState::ReadingTrailerLf => {
                    match self.read_trailer(&input[in_pos..]) {
                        Step::Advance(n) => in_pos += n,
                        Step::Yield((result, consumed)) => {
                            return self.deliver(result, in_pos + consumed, out_pos);
                        }
                        Step::EmitAndContinue(_) => unreachable!(),
                    }
                }
                ChunkedState::Failed(e) => return (DecodeResult::Error(e), in_pos),
                ChunkedState::Done => break,
            }
        }

        if out_pos > 0 {
            (DecodeResult::Data(out_pos), in_pos)
        } else if self.is_done() {
            (DecodeResult::Done, in_pos)
        } else {
            (DecodeResult::NeedMore, in_pos)
        }
    }

    /// Deliver body bytes decoded before a verdict was reached, holding the
    /// verdict for the next call.
    ///
    /// Where a socket read happens to break is not something the peer chose,
    /// so it must not change what the caller receives. Returning an error
    /// while `output` holds decoded bytes discards them, which means a body
    /// split one way delivers data a body split another way loses. The same
    /// rule already governs `Done`, which `read_trailer` defers for exactly
    /// this reason.
    const fn deliver(
        &mut self,
        result: DecodeResult,
        consumed: usize,
        out_pos: usize,
    ) -> (DecodeResult, usize) {
        match result {
            DecodeResult::Error(e) if out_pos > 0 => {
                self.state = ChunkedState::Failed(e);
                (DecodeResult::Data(out_pos), consumed)
            }
            other => (other, consumed),
        }
    }

    fn read_size_line(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingSize => {
                    if let Some(digit) = HexDigit::decode(b) {
                        self.saw_size_digit = true;
                        self.chunk_size = match self
                            .chunk_size
                            .checked_mul(16)
                            .and_then(|v| v.checked_add(u64::from(digit)))
                        {
                            Some(v) => v,
                            None => {
                                return Step::Yield((
                                    DecodeResult::Error(ParseError::InvalidChunkSize),
                                    i,
                                ));
                            }
                        };
                    } else if (b == b'\r' || b == b';') && self.saw_size_digit {
                        if b == b'\r' {
                            self.remaining = self.chunk_size;
                            self.state = ChunkedState::ReadingSizeLf;
                        } else {
                            self.state = ChunkedState::ReadingExtension;
                        }
                    } else {
                        return Step::Yield((DecodeResult::Error(ParseError::InvalidChunkSize), i));
                    }
                }
                ChunkedState::ReadingExtension => {
                    if b == b'\r' {
                        self.remaining = self.chunk_size;
                        self.state = ChunkedState::ReadingSizeLf;
                    } else if !Self::is_metadata_byte(b) || !self.metadata.charge_extension() {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkMetadata),
                            i,
                        ));
                    }
                }
                ChunkedState::ReadingSizeLf => {
                    if b != b'\n' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    if self.chunk_size == 0 {
                        self.state = ChunkedState::ReadingTrailer;
                        self.trailer_line_empty = true;
                        self.metadata.reset_extension();
                        // Hand control back to `decode`, which routes the
                        // remaining bytes to `read_trailer`. Falling through
                        // would re-enter this loop in `ReadingTrailer`, a
                        // state this function does not handle.
                        return Step::Advance(i + 1);
                    }
                    self.state = ChunkedState::ReadingData;
                    return Step::EmitAndContinue(i + 1);
                }
                _ => unreachable!(),
            }
        }
        Step::Advance(input.len())
    }

    fn read_data(&mut self, input: &[u8], output: &mut [u8]) -> DataStep {
        if output.is_empty() {
            return DataStep::BufferFull;
        }
        let to_copy = input
            .len()
            .min(output.len())
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        if to_copy == 0 {
            // Remaining is zero; next state should have been ReadingDataCr.
            self.state = ChunkedState::ReadingDataCr;
            return DataStep::Copied(0);
        }
        output[..to_copy].copy_from_slice(&input[..to_copy]);
        self.remaining -= u64::try_from(to_copy).unwrap_or(u64::MAX);
        if self.remaining == 0 {
            self.state = ChunkedState::ReadingDataCr;
        }
        DataStep::Copied(to_copy)
    }

    fn read_data_terminator(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingDataCr => {
                    if b != b'\r' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    self.state = ChunkedState::ReadingDataLf;
                }
                ChunkedState::ReadingDataLf => {
                    if b != b'\n' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    self.chunk_size = 0;
                    self.saw_size_digit = false;
                    self.metadata.reset_extension();
                    self.state = ChunkedState::ReadingSize;
                    return Step::Advance(i + 1);
                }
                _ => unreachable!(),
            }
        }
        Step::Advance(input.len())
    }

    fn read_trailer(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingTrailer => {
                    if b == b'\r' {
                        self.state = ChunkedState::ReadingTrailerLf;
                    } else if Self::is_metadata_byte(b) && self.metadata.charge_trailer() {
                        self.trailer_line_empty = false;
                    } else {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkMetadata),
                            i,
                        ));
                    }
                }
                ChunkedState::ReadingTrailerLf => {
                    if b != b'\n' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    if self.trailer_line_empty {
                        self.state = ChunkedState::Done;
                        // Return control to `decode` instead of yielding
                        // `Done` directly: data decoded earlier in the same
                        // call must take precedence (`Data` before `Done`),
                        // which `decode`'s final `out_pos > 0` check handles.
                        // Yielding here would discard that pending data.
                        return Step::Advance(i + 1);
                    }
                    self.state = ChunkedState::ReadingTrailer;
                    self.trailer_line_empty = true;
                }
                _ => unreachable!(),
            }
        }
        Step::Advance(input.len())
    }

    /// Whether `b` may appear inside a chunk extension or trailer line.
    ///
    /// Bare LF, NUL, and the other C0 controls are excluded: accepting them
    /// lets a peer smuggle line structure past a downstream parser that
    /// treats LF alone as a line ending. HTAB is allowed, as in field values.
    const fn is_metadata_byte(b: u8) -> bool {
        b == b'\t' || (b >= 0x20 && b != 0x7f)
    }
}

enum Step {
    Advance(usize),
    Yield((DecodeResult, usize)),
    EmitAndContinue(usize),
}

enum DataStep {
    Copied(usize),
    BufferFull,
}

/// Single hex-digit decoder used by the chunked decoder.
struct HexDigit;

impl HexDigit {
    const fn decode(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
}

pub const MAX_HEADERS: usize = 64;

/// Largest response head, in bytes, whose header positions can be
/// represented. Offsets are `u32`, so this is the ceiling a
/// `MAX_HEAD_SIZE` const generic can usefully take.
pub const MAX_ADDRESSABLE_HEAD: usize = u32::MAX as usize;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeaderRange {
    pub name_start: u32,
    pub name_len: u32,
    pub value_start: u32,
    pub value_len: u32,
}

impl HeaderRange {
    /// Compute byte-offset ranges for each parsed header against the raw `src` buffer.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::TooManyHeaders`] if `headers` exceeds
    /// [`MAX_HEADERS`], [`ConnectionError::HeaderNotInBuffer`] if a header name
    /// cannot be located in `src`, or [`ConnectionError::HeaderRangeOverflow`]
    /// if any offset or length exceeds [`MAX_ADDRESSABLE_HEAD`].
    pub fn build_ranges(headers: &[Header<'_>], src: &[u8]) -> Result<[Self; MAX_HEADERS], Error> {
        if headers.len() > MAX_HEADERS {
            return Err(ParseError::TooManyHeaders.into());
        }
        let src_base = src.as_ptr().addr();
        let src_end = src_base + src.len();
        let mut ranges = [Self::default(); MAX_HEADERS];
        for (i, h) in headers.iter().enumerate() {
            let name = h.name.as_bytes();
            let value = h.value;
            let ns_off = Self::offset_in_or_find(src, src_base, src_end, name)
                .ok_or(Error::Connection(ConnectionError::HeaderNotInBuffer))?;
            let vs_off = Self::offset_in_or_find(src, src_base, src_end, value)
                .ok_or(Error::Connection(ConnectionError::HeaderNotInBuffer))?;
            ranges[i] = Self::from_parts(ns_off, name.len(), vs_off, value.len())?;
        }
        Ok(ranges)
    }

    #[must_use]
    fn offset_in_or_find(
        src: &[u8],
        src_base: usize,
        src_end: usize,
        part: &[u8],
    ) -> Option<usize> {
        let ptr = part.as_ptr().addr();
        if (src_base..src_end).contains(&ptr) {
            Some(ptr - src_base)
        } else {
            src.find_subsequence(part)
        }
    }

    fn from_parts(
        name_start: usize,
        name_len: usize,
        value_start: usize,
        value_len: usize,
    ) -> Result<Self, Error> {
        let overflow = |_| Error::from(ConnectionError::HeaderRangeOverflow);
        Ok(Self {
            name_start: u32::try_from(name_start).map_err(overflow)?,
            name_len: u32::try_from(name_len).map_err(overflow)?,
            value_start: u32::try_from(value_start).map_err(overflow)?,
            value_len: u32::try_from(value_len).map_err(overflow)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_response(text: &[u8]) -> (ResponseHead<'_>, usize, Vec<Header<'_>>) {
        let mut headers = vec![Header::empty(); 32];
        let (head, consumed) = ResponseHead::parse(text, &mut headers).unwrap();
        headers.truncate(head.header_count);
        (head, consumed, headers)
    }

    #[test]
    fn parse_simple_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
        let (head, consumed, headers) = make_response(raw);
        assert_eq!(head.version, Version::Http11);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"OK");
        assert_eq!(head.header_count, 1);
        assert_eq!(consumed, raw.len());
        assert_eq!(headers[0].name, HeaderName::ContentLength);
        assert_eq!(headers[0].value, b"5");
    }

    #[test]
    fn parse_no_reason_phrase() {
        let raw = b"HTTP/1.1 200\r\n\r\n";
        let (head, _, _) = make_response(raw);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"");
    }

    #[test]
    fn parse_multiple_headers() {
        let raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 42\r\nServer: test\r\n\r\n";
        let (head, _, headers) = make_response(raw);
        assert_eq!(head.header_count, 3);
        assert_eq!(headers[0].name, HeaderName::ContentType);
        assert_eq!(headers[1].name, HeaderName::ContentLength);
        assert_eq!(headers[2].name, HeaderName::Server);
    }

    #[test]
    fn incomplete_returns_error() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
        let mut headers = [const { Header::empty() }; 32];
        let result = ResponseHead::parse(raw, &mut headers);
        assert_eq!(result.unwrap_err(), Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn every_prefix_of_a_valid_head_is_incomplete() {
        // A caller feeding bytes as they arrive uses Incomplete as the only
        // instruction to read more. Any other error on a truncated but
        // well-formed head makes it reject a response that is merely still
        // in flight.
        let raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 42\r\nServer: test\r\n\r\n";
        for end in 1..raw.len() {
            let mut headers = [const { Header::empty() }; 32];
            let err = ResponseHead::parse(&raw[..end], &mut headers)
                .expect_err("a truncated head cannot parse");
            assert_eq!(
                err,
                Error::Parse(ParseError::Incomplete),
                "prefix of length {end} reported {err:?}: {:?}",
                std::str::from_utf8(&raw[..end])
            );
        }
    }

    #[test]
    fn partial_header_name_is_incomplete_not_missing_colon() {
        // The specific shape behind the class above: every byte so far is a
        // valid token character, so the colon may still be coming.
        let mut headers = [const { Header::empty() }; 32];
        let err = ResponseHead::parse(b"HTTP/1.1 200 OK\r\nCont", &mut headers)
            .expect_err("a truncated header name cannot parse");
        assert_eq!(err, Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn complete_line_without_a_colon_is_still_rejected() {
        // The fix must not swallow genuinely malformed lines. CR is not a
        // tchar, so a terminated line with no colon fails on the name scan
        // before the end of input is ever reached.
        let mut headers = [const { Header::empty() }; 32];
        let err = ResponseHead::parse(b"HTTP/1.1 200 OK\r\nNoColonHere\r\n\r\n", &mut headers)
            .expect_err("a complete line without a colon is malformed");
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn too_many_headers_error() {
        let raw = b"HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\n\r\n";
        let mut headers = [Header::empty(); 1];
        let result = ResponseHead::parse(raw, &mut headers);
        assert_eq!(
            result.unwrap_err(),
            Error::Parse(ParseError::TooManyHeaders)
        );
    }

    #[test]
    fn body_framing_content_length() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"42",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(42)
        );
    }

    #[test]
    fn body_framing_chunked() {
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::Chunked
        );
    }

    #[test]
    fn body_framing_none_for_204() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::NO_CONTENT, Method::Get, &headers).unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_none_for_head() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"42",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Head, &headers).unwrap(),
            BodyFraming::None
        );
    }

    /// RFC 9110 §9.3.6: a client "MUST ignore any Content-Length or
    /// Transfer-Encoding header fields received in a successful response to
    /// CONNECT". Framing them as a body would hand the caller the tunnel's
    /// first bytes as content.
    #[test]
    fn body_framing_none_for_a_successful_connect() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"4096",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Connect, &headers).unwrap(),
            BodyFraming::None
        );
    }

    /// The transfer coding is ignored on a successful CONNECT for the same
    /// reason as the length, and is worth pinning separately: chunked framing
    /// takes precedence everywhere else in this function.
    #[test]
    fn a_transfer_coding_does_not_frame_a_successful_connect() {
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Connect, &headers).unwrap(),
            BodyFraming::None
        );
    }

    /// The boundary of the rule. §9.3.6: "Any response other than a successful
    /// response indicates that the tunnel has not yet been formed", so a
    /// refusal is an ordinary response and its content is framed normally —
    /// otherwise a proxy's error page would be unreadable.
    #[test]
    fn a_refused_connect_is_framed_like_any_other_response() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"6",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::FORBIDDEN, Method::Connect, &headers).unwrap(),
            BodyFraming::ContentLength(6)
        );
    }

    #[test]
    fn body_framing_until_close() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn body_framing_chunked_takes_precedence() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::Chunked
        );
    }

    #[test]
    fn chunked_simple() {
        let input = b"5\r\nhello\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, consumed) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(&output[..5], b"hello");

        let (result, _) = decoder.decode(&input[consumed..], &mut output);
        assert_eq!(result, DecodeResult::Done);
        assert!(decoder.is_done());
    }

    #[test]
    fn chunked_multiple_chunks() {
        let input = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done | DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello world");
        assert!(decoder.is_done());
    }

    #[test]
    fn data_before_an_error_is_delivered_not_discarded() {
        // Found by fuzzing: fed whole, this lost the "a" that a byte-at-a-time
        // feed of the same bytes delivered. Where a socket read happens to
        // break is not something the peer chose, so it must not change what
        // the caller receives.
        let input = b"1\r\na\r\nz\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];

        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(1));
        assert_eq!(&output[..1], b"a");

        let (result, consumed) = decoder.decode(b"anything", &mut output);
        assert_eq!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize),
            "the held verdict must arrive on the next call"
        );
        assert_eq!(consumed, 0, "a failed decoder consumes no further input");
    }

    #[test]
    fn a_failed_decoder_reports_the_same_error_forever() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        decoder.decode(b"1\r\na\r\nz\r\n", &mut output);

        for _ in 0..3 {
            let (result, _) = decoder.decode(b"0\r\n\r\n", &mut output);
            assert_eq!(result, DecodeResult::Error(ParseError::InvalidChunkSize));
        }
        assert!(
            !decoder.is_done(),
            "a stream that failed never completed"
        );
    }

    #[test]
    fn an_error_with_no_pending_data_is_reported_immediately() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(b"z\r\n", &mut output);
        assert_eq!(result, DecodeResult::Error(ParseError::InvalidChunkSize));
    }

    #[test]
    fn chunked_byte_at_a_time() {
        let input = b"3\r\nabc\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();

        for &byte in input {
            let one = [byte];
            let (result, _) = decoder.decode(&one, &mut output);
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"abc");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunked_with_extension() {
        let input = b"5;ext=val\r\nhello\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, consumed) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(&output[..5], b"hello");

        let (result, _) = decoder.decode(&input[consumed..], &mut output);
        assert_eq!(result, DecodeResult::Done);
    }

    #[test]
    fn chunked_with_trailers() {
        let input = b"5\r\nhello\r\n0\r\nTrailer: value\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done | DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
        assert!(decoder.is_done());
    }

    /// Feed `input` until the decoder yields a terminal result.
    fn drive(decoder: &mut ChunkedDecoder, input: &[u8]) -> DecodeResult {
        let mut output = [0u8; 256];
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(_) if pos < input.len() => {}
                other => return other,
            }
        }
    }

    #[test]
    fn oversized_chunk_extension_is_rejected() {
        // Extensions carry no payload, so an unbounded one lets a peer occupy
        // the connection forever without delivering a single body byte.
        let mut input = b"5;".to_vec();
        input.extend(std::iter::repeat_n(b'x', MAX_CHUNK_EXTENSION + 1));
        input.extend_from_slice(b"\r\nhello\r\n0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, &input),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn extension_within_the_budget_is_accepted() {
        // The bound must not reject ordinary extensions.
        let mut input = b"5;".to_vec();
        input.extend(std::iter::repeat_n(b'x', MAX_CHUNK_EXTENSION - 1));
        input.extend_from_slice(b"\r\nhello\r\n0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        assert_eq!(drive(&mut decoder, &input), DecodeResult::Data(5));
    }

    #[test]
    fn extension_budget_resets_between_chunks() {
        // Each chunk gets its own extension allowance; a long stream of
        // normally-extended chunks must not accumulate into a refusal.
        let mut input = Vec::new();
        for _ in 0..8 {
            input.extend_from_slice(b"1;");
            input.extend(std::iter::repeat_n(b'x', MAX_CHUNK_EXTENSION - 1));
            input.extend_from_slice(b"\r\na\r\n");
        }
        input.extend_from_slice(b"0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 256];
        let mut total = Vec::new();
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert_eq!(total, b"aaaaaaaa");
    }

    #[test]
    fn oversized_trailer_section_is_rejected() {
        // Many small trailer lines share one budget, so the section cannot be
        // extended indefinitely by splitting it up. CRLF is not charged, so
        // the loop counts the field bytes the budget actually sees.
        const LINE: &[u8] = b"X-Pad: value\r\n";
        let charged_per_line = LINE.len() - 2;
        let lines = MAX_TRAILER_SECTION / charged_per_line + 2;

        let mut input = b"0\r\n".to_vec();
        for _ in 0..lines {
            input.extend_from_slice(LINE);
        }
        input.extend_from_slice(b"\r\n");

        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, &input),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn bare_lf_in_a_chunk_extension_is_rejected() {
        // A bare LF inside metadata lets a peer smuggle line structure past a
        // downstream parser that treats LF alone as a line ending.
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext\nvalue\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn nul_in_a_chunk_extension_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext\0bad\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn bare_lf_in_a_trailer_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\nTrailer: a\nb\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn chunked_hex_cases() {
        let input = b"A\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(10));
        assert_eq!(&output[..10], b"0123456789");

        let input = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(10));
    }

    #[test]
    fn chunked_small_output_buffer() {
        let input = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 4];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done | DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"0123456789");
        assert!(decoder.is_done());
    }

    // ── Adversarial response head parsing ────────────────────────────────────

    #[test]
    fn garbage_status_line() {
        let raw = b"\x00\xff\xfe garbage\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert!(ResponseHead::parse(raw, &mut headers).is_err());
    }

    #[test]
    fn truncated_version() {
        let raw = b"HTTP/1\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidVersion));
    }

    #[test]
    fn status_code_followed_by_non_space_rejected() {
        let raw = b"HTTP/1.1 200X\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidStatusCode));
    }

    #[test]
    fn status_line_trailing_space_empty_reason() {
        let raw = b"HTTP/1.1 200 \r\n\r\n";
        let (head, _, _) = make_response(raw);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"");
    }

    #[test]
    fn status_line_no_space_after_version() {
        let raw = b"HTTP/1.1200 OK\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert!(ResponseHead::parse(raw, &mut headers).is_err());
    }

    #[test]
    fn header_with_nul_byte_in_name() {
        let raw = b"HTTP/1.1 200 OK\r\nBad\x00Name: val\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_missing_colon() {
        // Parser scans tchar-by-tchar; \r is not a tchar, so InvalidHeaderName
        // fires before MissingColon can be reached.
        let raw = b"HTTP/1.1 200 OK\r\nHeaderNoColon\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_empty_name() {
        let raw = b"HTTP/1.1 200 OK\r\n: value\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_name_with_space() {
        let raw = b"HTTP/1.1 200 OK\r\nBad Name: val\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_value_ows_trimmed() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Test:   spaced   \r\n\r\n";
        let (head, _, headers) = make_response(raw);
        assert_eq!(head.header_count, 1);
        assert_eq!(headers[0].value, b"spaced");
    }

    #[test]
    fn only_crlf_no_status_line() {
        let raw = b"\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert!(ResponseHead::parse(raw, &mut headers).is_err());
    }

    #[test]
    fn status_line_exactly_min_length() {
        // "HTTP/1.1 200" is exactly 12 bytes — the minimum.
        let raw = b"HTTP/1.1 200\r\n\r\n";
        let (head, _, _) = make_response(raw);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"");
    }

    // ── Adversarial body framing ─────────────────────────────────────────────

    #[test]
    fn body_framing_304_no_body() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"1000",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::NOT_MODIFIED, Method::Get, &headers).unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_1xx_no_body() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::CONTINUE, Method::Get, &headers).unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn content_length_zero() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"0",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(0)
        );
    }

    #[test]
    fn content_length_with_ows() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b" 42 ",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(42)
        );
    }

    #[test]
    fn content_length_non_numeric_rejected() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"abc",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidContentLength
        );
    }

    #[test]
    fn conflicting_content_length_rejected() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"6",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidContentLength
        );
    }

    #[test]
    fn duplicate_identical_content_length_accepted() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(5)
        );
    }

    #[test]
    fn header_value_with_ctl_byte_rejected() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Bad: a\x00b\r\nContent-Length: 0\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderValue));
    }

    #[test]
    fn malformed_name_does_not_scan_past_its_own_line() {
        // The colon here belongs to a *later* header. A name scan that looked
        // for ':' without stopping at the first non-tchar would swallow the
        // CRLF and treat "bad\r\nX-Next" as one name.
        let raw = b"HTTP/1.1 200 OK\r\nbad\r\nX-Next: v\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_value_with_bare_lf_rejected() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Bad: a\nb\r\nContent-Length: 0\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderValue));
    }

    #[test]
    fn header_value_with_del_byte_rejected() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Bad: a\x7fb\r\nContent-Length: 0\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderValue));
    }

    #[test]
    fn header_value_unterminated_is_incomplete_not_invalid() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: value";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn header_value_cr_without_lf_is_incomplete() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: value\r";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn header_value_with_obs_text_accepted() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: caf\xc3\xa9\r\nContent-Length: 0\r\n\r\n";
        let (_, _, headers) = make_response(raw);
        assert_eq!(headers[0].value, b"caf\xc3\xa9");
    }

    #[test]
    fn header_value_with_tab_accepted() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: a\tb\r\nContent-Length: 0\r\n\r\n";
        let (_, _, headers) = make_response(raw);
        assert_eq!(headers[0].value, b"a\tb");
    }

    #[test]
    fn transfer_encoding_not_chunked() {
        // gzip is a coding this client cannot undo. Framing the body as
        // UntilClose would present compressed bytes as the response body.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn transfer_encoding_overrides_content_length() {
        // Transfer-Encoding still takes precedence over Content-Length: the
        // unsupported coding decides the outcome, not the length header.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"gzip",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );

        // identity applies no encoding, so the precedence is still visible
        // without an unsupported coding masking it.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"identity",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn chunked_before_another_coding_is_rejected() {
        // chunked delimits the body, so a coding applied after it would have
        // to be decoded before the framing could be read.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked, gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn chunked_final_across_multiple_transfer_encoding_headers() {
        // The codings of every header form one list in order, so a trailing
        // chunked frames the body even when split across headers.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"identity",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"Chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::Chunked
        );

        // The same list with an undecodable coding underneath is refused.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"gzip",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"Chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn empty_transfer_encoding_is_until_close() {
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn reason_phrase_with_nul_is_rejected() {
        // The status line ends at CRLF, so a reason phrase cannot contain
        // CR or LF -- but NUL and the other C0 controls reach the caller as
        // part of a field the RFC restricts to HTAB / SP / VCHAR / obs-text.
        let raw = b"HTTP/1.1 200 O\0K\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert_eq!(
            ResponseHead::parse(raw, &mut headers).unwrap_err(),
            Error::Parse(ParseError::InvalidReasonPhrase)
        );
    }

    #[test]
    fn reason_phrase_allows_tab_and_obs_text() {
        // The permitted set is wider than ASCII graphic characters; the
        // check must not reject phrases servers legitimately send.
        let raw = b"HTTP/1.1 200 OK\tdone\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let (head, _) = ResponseHead::parse(raw, &mut headers).expect("HTAB is permitted");
        assert_eq!(head.reason, b"OK\tdone");

        let raw = b"HTTP/1.1 200 caf\xc3\xa9\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let (head, _) = ResponseHead::parse(raw, &mut headers).expect("obs-text is permitted");
        assert_eq!(head.reason, b"caf\xc3\xa9");
    }

    #[test]
    fn unsupported_transfer_coding_is_rejected() {
        // gzip is a transfer coding this client cannot decode. Framing the
        // body as UntilClose hands the caller compressed bytes as though they
        // were the response body.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn chunked_over_an_unsupported_coding_is_rejected() {
        // "gzip, chunked" was dechunked and returned as the body, still gzip
        // transfer-coded, with no indication anything was left encoded.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip, chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn repeated_chunked_is_rejected() {
        // RFC 9112 6.1: chunked must not be applied more than once. Accepting
        // it is a framing difference an intermediary may not share.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked, chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn repeated_chunked_across_headers_is_rejected() {
        // The codings of every Transfer-Encoding header form one list, so
        // splitting the repeat across two headers must not evade the check.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn identity_coding_is_accepted_as_unframed() {
        // "identity" applies no encoding, so it neither frames the body nor
        // leaves it encoded.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"identity",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn duplicate_transfer_encoding_both_chunked() {
        // Previously accepted as Chunked. RFC 9112 6.1 forbids applying
        // chunked more than once, and accepting it is a framing difference an
        // intermediary may not share.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    // ── Adversarial chunked decoder ──────────────────────────────────────────

    #[test]
    fn chunk_size_overflow() {
        let input = b"FFFFFFFFFFFFFFFF0\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_size_many_leading_zeros_decodes_normally() {
        for zeros in [255usize, 256, 257, 512] {
            let mut input = vec![b'0'; zeros];
            input.extend_from_slice(b"7\r\nabcdefg\r\n0\r\n\r\n");

            let mut decoder = ChunkedDecoder::new();
            let mut output = [0u8; 64];
            let mut total = Vec::new();
            let mut pos = 0;

            loop {
                let (result, consumed) = decoder.decode(&input[pos..], &mut output);
                pos += consumed;
                match result {
                    DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                    DecodeResult::Done => break,
                    DecodeResult::NeedMore => {}
                    DecodeResult::Error(e) => panic!("unexpected error at {zeros} zeros: {e}"),
                }
            }

            assert_eq!(total, b"abcdefg", "{zeros} leading zeros");
            assert!(decoder.is_done(), "{zeros} leading zeros");
        }
    }

    #[test]
    fn chunk_size_leading_zeros_split_across_feeds() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();

        for _ in 0..300 {
            let (result, _) = decoder.decode(b"0", &mut output);
            assert_eq!(result, DecodeResult::NeedMore);
        }

        let rest = b"7\r\nabcdefg\r\n0\r\n\r\n";
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&rest[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"abcdefg");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_size_leading_zeros() {
        let input = b"007\r\nabcdefg\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"abcdefg");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_missing_crlf_after_data() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];

        // Feed size + data in one call so decoder yields the body data.
        let (result, _) = decoder.decode(b"5\r\nhello", &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(&output[..5], b"hello");

        // Now feed 'X' where \r\n was expected.
        let (result, _) = decoder.decode(b"X", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkTerminator)
        ));
    }

    #[test]
    fn chunk_decode_empty_input() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, consumed) = decoder.decode(b"", &mut output);
        assert_eq!(result, DecodeResult::NeedMore);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn chunk_multiple_trailers() {
        let input = b"5\r\nhello\r\n0\r\nTrailer-A: 1\r\nTrailer-B: 2\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_extension_with_quoted_value() {
        let input = b"5;ext=\"val\"\r\nhello\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
    }

    #[test]
    fn chunk_interleaved_partial_feeds() {
        // Feed the chunk size partially, then the rest
        let part1 = b"1";
        let part2 = b"0\r\n0123456789abcdef\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();

        let (result, _) = decoder.decode(part1, &mut output);
        assert_eq!(result, DecodeResult::NeedMore);

        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&part2[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"0123456789abcdef");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_empty_size_line_rejected() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(b"\r\n", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_extension_without_size_rejected() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(b";ext\r\n", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_empty_size_line_after_data_rejected() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let wire = b"5\r\nhello\r\n";
        let (result, consumed) = decoder.decode(wire, &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(consumed, wire.len());
        let (result, _) = decoder.decode(b"\r\n\r\n", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    /// `from_response` returns `Result`, so a caller has every reason to
    /// expect it not to panic. It sliced by a caller-supplied count and did.
    #[test]
    fn an_oversized_header_count_is_an_error_not_a_panic() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"5",
        }];
        let head = ResponseHead {
            version: Version::Http11,
            status: StatusCode::OK,
            reason: b"OK",
            header_count: headers.len() + 8,
        };
        assert_eq!(head.headers(&headers), Err(ParseError::TooManyHeaders));
    }

    #[test]
    fn a_consistent_count_yields_exactly_the_parsed_headers() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
            Header::empty(),
        ];
        let head = ResponseHead {
            version: Version::Http11,
            status: StatusCode::OK,
            reason: b"OK",
            header_count: 1,
        };
        let parsed = head.headers(&headers).expect("the count fits");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value, b"5");
    }

    #[test]
    fn build_ranges_too_many_headers_is_an_error() {
        let src = b"A: 1\r\n";
        let headers = vec![
            Header {
                name: HeaderName::from_bytes(b"A"),
                value: &src[3..4],
            };
            MAX_HEADERS + 1
        ];
        assert_eq!(
            HeaderRange::build_ranges(&headers, src).unwrap_err(),
            Error::Parse(ParseError::TooManyHeaders)
        );
    }

    #[test]
    fn chunk_invalid_hex_in_size() {
        let input = b"ZZ\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_empty_body_immediate_terminator() {
        let input = b"0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Done);
        assert!(decoder.is_done());
    }

    #[test]
    fn build_ranges_matches_header_slices() {
        let src = b"Content-Length: 42\r\nServer: test\r\n\r\n";
        let headers = [
            Header {
                name: HeaderName::from_bytes(b"Content-Length"),
                value: &src[16..18],
            },
            Header {
                name: HeaderName::from_bytes(b"Server"),
                value: &src[28..32],
            },
        ];
        let ranges = HeaderRange::build_ranges(&headers, src).unwrap();
        let slice = |range: &HeaderRange, name: bool| {
            let (start, len) = if name {
                (range.name_start as usize, range.name_len as usize)
            } else {
                (range.value_start as usize, range.value_len as usize)
            };
            &src[start..start + len]
        };
        assert_eq!(slice(&ranges[0], true), b"Content-Length");
        assert_eq!(slice(&ranges[0], false), b"42");
        assert_eq!(slice(&ranges[1], true), b"Server");
        assert_eq!(slice(&ranges[1], false), b"test");
    }

    #[test]
    fn build_ranges_overflow_fails() {
        let oversized = MAX_ADDRESSABLE_HEAD + 1;
        assert_eq!(
            HeaderRange::from_parts(oversized, 1, 0, 0).unwrap_err(),
            Error::Connection(ConnectionError::HeaderRangeOverflow)
        );
        assert_eq!(
            HeaderRange::from_parts(0, oversized, 0, 0).unwrap_err(),
            Error::Connection(ConnectionError::HeaderRangeOverflow)
        );
    }

    #[test]
    fn offsets_past_64_kib_are_representable() {
        // The README suggests a 128 KiB head limit. With u16 offsets a header
        // positioned past 64 KiB failed with HeaderRangeOverflow, so that
        // limit could not be used as documented.
        let past_64k = usize::from(u16::MAX) + 1;
        let range = HeaderRange::from_parts(past_64k, 4, past_64k + 6, 2)
            .expect("a header beyond 64 KiB must be addressable");
        assert_eq!(range.name_start as usize, past_64k);
        assert_eq!(range.value_start as usize, past_64k + 6);
    }

    #[test]
    fn a_header_positioned_past_64_kib_builds_its_range() {
        // The same limit reached through the public entry point, with the
        // header genuinely sitting beyond the old ceiling.
        let mut src = b"HTTP/1.1 200 OK\r\n".to_vec();
        src.extend_from_slice(b"X-Pad: ");
        src.extend(std::iter::repeat_n(b'p', 70_000));
        src.extend_from_slice(b"\r\nServer: late\r\n\r\n");

        let value_start = src
            .windows(4)
            .position(|w| w == b"late")
            .expect("the late header is present");
        assert!(value_start > usize::from(u16::MAX));

        let headers = [Header {
            name: HeaderName::Server,
            value: &src[value_start..value_start + 4],
        }];
        let ranges = HeaderRange::build_ranges(&headers, &src)
            .expect("a late header must not overflow its range");
        assert_eq!(ranges[0].value_start as usize, value_start);
    }
}
