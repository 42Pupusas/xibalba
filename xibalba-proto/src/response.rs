use crate::bytes::ByteSliceExt;
use crate::error::{ConnectionError, Error, ParseError};
use crate::header::{Header, HeaderName, Tchar};
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

        let reason = if status_line.len() > 13 && status_line[12] == b' ' {
            &status_line[13..]
        } else {
            b""
        };

        let mut pos = status_line_end + 2;
        let mut count = 0;

        loop {
            if pos >= buf.len() {
                return Err(ParseError::Incomplete.into());
            }

            if pos + 1 < buf.len() && buf[pos] == b'\r' && buf[pos + 1] == b'\n' {
                pos += 2;
                break;
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

        // Phase 1: scan name, validating tchars and stopping at ':'
        let mut i = 0;
        loop {
            if i >= line.len() {
                return Err(ParseError::MissingColon.into());
            }
            let b = line[i];
            if b == b':' {
                break;
            }
            if !Tchar::is_valid(b) {
                return Err(ParseError::InvalidHeaderName.into());
            }
            i += 1;
        }
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

        // Phase 3: use iterator position so LLVM can auto-vectorize the \r scan
        let cr_pos = line[i..]
            .iter()
            .position(|&b| b == b'\r')
            .ok_or(ParseError::Incomplete)?;
        let crlf = i + cr_pos;
        if crlf + 1 >= line.len() || line[crlf + 1] != b'\n' {
            return Err(ParseError::Incomplete.into());
        }

        // Trim trailing OWS in one backward pass — only paid when OWS is present
        let raw_value = &line[value_start..crlf];
        if raw_value
            .iter()
            .any(|&b| b != b'\t' && (b < 0x20 || b == 0x7f))
        {
            return Err(ParseError::InvalidHeaderValue.into());
        }
        let value_end = raw_value
            .iter()
            .rposition(|&b| b != b' ' && b != b'\t')
            .map_or(0, |p| p + 1);

        Ok((name_bytes, &raw_value[..value_end], crlf + 2))
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
    /// Determine body framing from the response status and headers.
    ///
    /// `Transfer-Encoding: chunked` wins over `Content-Length` (RFC 9112
    /// §6.1). Multiple `Content-Length` headers are accepted only when
    /// every value parses to the same number — a conflicting or
    /// unparseable value is a smuggling vector and is rejected.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::InvalidContentLength`] when any
    /// `Content-Length` value is not valid digits or two of them
    /// disagree.
    pub fn from_response(
        status: StatusCode,
        request_method_is_head: bool,
        headers: &[Header<'_>],
        header_count: usize,
    ) -> Result<Self, ParseError> {
        if status.is_informational()
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED
            || request_method_is_head
        {
            return Ok(Self::None);
        }

        let hdrs = &headers[..header_count];

        for h in hdrs {
            if h.name == HeaderName::TransferEncoding
                && h.value.contains_token_ignore_case(b"chunked")
            {
                return Ok(Self::Chunked);
            }
        }

        let mut content_length: Option<u64> = None;
        for h in hdrs {
            if h.name == HeaderName::ContentLength {
                let len = h.value.parse_u64().ok_or(ParseError::InvalidContentLength)?;
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

/// State machine for decoding chunked `Transfer-Encoding`.
#[derive(Debug, Clone)]
pub struct ChunkedDecoder {
    state: ChunkedState,
    chunk_size: u64,
    remaining: u64,
    trailer_line_empty: bool,
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
            remaining: 0,
            trailer_line_empty: true,
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

        while in_pos < input.len() {
            match self.state {
                ChunkedState::ReadingSize
                | ChunkedState::ReadingExtension
                | ChunkedState::ReadingSizeLf => match self.read_size_line(&input[in_pos..]) {
                    Step::Advance(n) => in_pos += n,
                    Step::Yield((result, consumed)) => return (result, in_pos + consumed),
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
                        Step::Yield((result, consumed)) => return (result, in_pos + consumed),
                        Step::EmitAndContinue(_) => unreachable!(),
                    }
                }
                ChunkedState::ReadingTrailer | ChunkedState::ReadingTrailerLf => {
                    match self.read_trailer(&input[in_pos..]) {
                        Step::Advance(n) => in_pos += n,
                        Step::Yield((result, consumed)) => return (result, in_pos + consumed),
                        Step::EmitAndContinue(_) => unreachable!(),
                    }
                }
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

    fn read_size_line(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingSize => {
                    if let Some(digit) = HexDigit::decode(b) {
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
                    } else if b == b'\r' {
                        self.remaining = self.chunk_size;
                        self.state = ChunkedState::ReadingSizeLf;
                    } else if b == b';' {
                        self.state = ChunkedState::ReadingExtension;
                    } else {
                        return Step::Yield((DecodeResult::Error(ParseError::InvalidChunkSize), i));
                    }
                }
                ChunkedState::ReadingExtension => {
                    if b == b'\r' {
                        self.remaining = self.chunk_size;
                        self.state = ChunkedState::ReadingSizeLf;
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
                    } else {
                        self.trailer_line_empty = false;
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeaderRange {
    pub name_start: u16,
    pub name_len: u16,
    pub value_start: u16,
    pub value_len: u16,
}

impl HeaderRange {
    /// Compute byte-offset ranges for each parsed header against the raw `src` buffer.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionError::HeaderNotInBuffer`] if a header name cannot be
    /// located in `src`, or [`ConnectionError::HeaderRangeOverflow`] if any offset
    /// or length overflows `u16`.
    pub fn build_ranges(headers: &[Header<'_>], src: &[u8]) -> Result<[Self; MAX_HEADERS], Error> {
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
            name_start: u16::try_from(name_start).map_err(overflow)?,
            name_len: u16::try_from(name_len).map_err(overflow)?,
            value_start: u16::try_from(value_start).map_err(overflow)?,
            value_len: u16::try_from(value_len).map_err(overflow)?,
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 1)
                .unwrap(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 1)
                .unwrap(),
            BodyFraming::Chunked
        );
    }

    #[test]
    fn body_framing_none_for_204() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::NO_CONTENT, false, &headers, 0)
                .unwrap(),
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
            BodyFraming::from_response(StatusCode::OK, true, &headers, 1)
                .unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_until_close() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, false, &headers, 0)
                .unwrap(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 2)
                .unwrap(),
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
            BodyFraming::from_response(StatusCode::NOT_MODIFIED, false, &headers, 1)
                .unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_1xx_no_body() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::CONTINUE, false, &headers, 0)
                .unwrap(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 1).unwrap(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 1)
                .unwrap(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 1).unwrap_err(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 2).unwrap_err(),
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 2).unwrap(),
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
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, false, &headers, 1).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn duplicate_transfer_encoding_both_chunked() {
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
            BodyFraming::from_response(StatusCode::OK, false, &headers, 2)
                .unwrap(),
            BodyFraming::Chunked
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
        assert_eq!(
            &src[usize::from(ranges[0].name_start)
                ..usize::from(ranges[0].name_start) + usize::from(ranges[0].name_len)],
            b"Content-Length"
        );
        assert_eq!(
            &src[usize::from(ranges[0].value_start)
                ..usize::from(ranges[0].value_start) + usize::from(ranges[0].value_len)],
            b"42"
        );
        assert_eq!(
            &src[usize::from(ranges[1].name_start)
                ..usize::from(ranges[1].name_start) + usize::from(ranges[1].name_len)],
            b"Server"
        );
        assert_eq!(
            &src[usize::from(ranges[1].value_start)
                ..usize::from(ranges[1].value_start) + usize::from(ranges[1].value_len)],
            b"test"
        );
    }

    #[test]
    fn build_ranges_overflow_fails() {
        let oversized = usize::from(u16::MAX) + 1;
        assert_eq!(
            HeaderRange::from_parts(oversized, 1, 0, 0).unwrap_err(),
            Error::Connection(ConnectionError::HeaderRangeOverflow)
        );
        assert_eq!(
            HeaderRange::from_parts(0, oversized, 0, 0).unwrap_err(),
            Error::Connection(ConnectionError::HeaderRangeOverflow)
        );
    }
}
