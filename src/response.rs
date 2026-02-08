use crate::error::{Error, ParseError};
use crate::header::{
    Header, HeaderName, contains_token_ignore_case, is_tchar, parse_u64_from_bytes, trim_ows,
};
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
pub fn parse_response_head<'a>(
    buf: &'a [u8],
    headers: &mut [Header<'a>],
) -> Result<(ResponseHead<'a>, usize), Error> {
    let status_line_end = find_crlf(buf).ok_or(ParseError::Incomplete)?;
    let status_line = &buf[..status_line_end];

    // Minimum: "HTTP/1.1 200" (12 bytes)
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
        if pos + 1 < buf.len() && buf[pos] == b'\r' && buf[pos + 1] == b'\n' {
            pos += 2;
            break;
        }

        let line_end = find_crlf_from(buf, pos).ok_or(ParseError::Incomplete)?;
        let line = &buf[pos..line_end];

        let colon_pos = line
            .iter()
            .position(|&b| b == b':')
            .ok_or(ParseError::MissingColon)?;

        let name_bytes = &line[..colon_pos];
        validate_token(name_bytes)?;

        let name = HeaderName::from_bytes(name_bytes);
        let value = trim_ows(&line[colon_pos + 1..]);

        if count >= headers.len() {
            return Err(ParseError::TooManyHeaders.into());
        }
        headers[count] = Header { name, value };
        count += 1;

        pos = line_end + 2;
    }

    Ok((
        ResponseHead {
            version,
            status,
            reason,
            header_count: count,
        },
        pos,
    ))
}

/// Determine body framing from the response status and headers.
#[must_use]
pub fn determine_body_framing(
    status: StatusCode,
    request_method_is_head: bool,
    headers: &[Header<'_>],
    header_count: usize,
) -> BodyFraming {
    if status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED
        || request_method_is_head
    {
        return BodyFraming::None;
    }

    let hdrs = &headers[..header_count];

    for h in hdrs {
        if h.name == HeaderName::TransferEncoding && contains_token_ignore_case(h.value, b"chunked")
        {
            return BodyFraming::Chunked;
        }
    }

    for h in hdrs {
        if h.name == HeaderName::ContentLength
            && let Some(len) = parse_u64_from_bytes(h.value)
        {
            return BodyFraming::ContentLength(len);
        }
    }

    BodyFraming::UntilClose
}

// --- Chunked transfer decoder state machine ---

/// State machine for decoding chunked `Transfer-Encoding`.
///
/// Processes bytes incrementally — feed it slices as they arrive.
/// No allocations.
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
    #[allow(clippy::too_many_lines)]
    pub fn decode(&mut self, input: &[u8], output: &mut [u8]) -> (DecodeResult, usize) {
        let mut in_pos = 0;
        let mut out_pos = 0;

        while in_pos < input.len() {
            let b = input[in_pos];

            match self.state {
                ChunkedState::ReadingSize => {
                    if let Some(digit) = hex_digit(b) {
                        self.chunk_size = match self
                            .chunk_size
                            .checked_mul(16)
                            .and_then(|v| v.checked_add(u64::from(digit)))
                        {
                            Some(v) => v,
                            None => {
                                return (DecodeResult::Error(ParseError::InvalidChunkSize), in_pos);
                            }
                        };
                        in_pos += 1;
                    } else if b == b'\r' {
                        self.remaining = self.chunk_size;
                        self.state = ChunkedState::ReadingSizeLf;
                        in_pos += 1;
                    } else if b == b';' {
                        self.state = ChunkedState::ReadingExtension;
                        in_pos += 1;
                    } else {
                        return (DecodeResult::Error(ParseError::InvalidChunkSize), in_pos);
                    }
                }

                ChunkedState::ReadingExtension => {
                    if b == b'\r' {
                        self.remaining = self.chunk_size;
                        self.state = ChunkedState::ReadingSizeLf;
                    }
                    in_pos += 1;
                }

                ChunkedState::ReadingSizeLf => {
                    if b != b'\n' {
                        return (
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            in_pos,
                        );
                    }
                    in_pos += 1;
                    if self.chunk_size == 0 {
                        self.state = ChunkedState::ReadingTrailer;
                        self.trailer_line_empty = true;
                    } else {
                        self.state = ChunkedState::ReadingData;
                        if out_pos > 0 {
                            return (DecodeResult::Data(out_pos), in_pos);
                        }
                    }
                }

                ChunkedState::ReadingData => {
                    let available_in = input.len() - in_pos;
                    let available_out = output.len() - out_pos;
                    let remaining_usize = usize::try_from(self.remaining).unwrap_or(usize::MAX);
                    let to_copy = available_in.min(available_out).min(remaining_usize);

                    if to_copy == 0 && available_out == 0 {
                        return (DecodeResult::Data(out_pos), in_pos);
                    }

                    output[out_pos..out_pos + to_copy]
                        .copy_from_slice(&input[in_pos..in_pos + to_copy]);
                    in_pos += to_copy;
                    out_pos += to_copy;
                    self.remaining -= to_copy as u64;

                    if self.remaining == 0 {
                        self.state = ChunkedState::ReadingDataCr;
                    }

                    if out_pos == output.len() {
                        return (DecodeResult::Data(out_pos), in_pos);
                    }
                }

                ChunkedState::ReadingDataCr => {
                    if b != b'\r' {
                        return (
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            in_pos,
                        );
                    }
                    self.state = ChunkedState::ReadingDataLf;
                    in_pos += 1;
                }

                ChunkedState::ReadingDataLf => {
                    if b != b'\n' {
                        return (
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            in_pos,
                        );
                    }
                    self.chunk_size = 0;
                    self.state = ChunkedState::ReadingSize;
                    in_pos += 1;
                }

                ChunkedState::ReadingTrailer => {
                    if b == b'\r' {
                        self.state = ChunkedState::ReadingTrailerLf;
                    } else {
                        self.trailer_line_empty = false;
                    }
                    in_pos += 1;
                }

                ChunkedState::ReadingTrailerLf => {
                    if b != b'\n' {
                        return (
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            in_pos,
                        );
                    }
                    in_pos += 1;
                    if self.trailer_line_empty {
                        self.state = ChunkedState::Done;
                        break;
                    }
                    self.state = ChunkedState::ReadingTrailer;
                    self.trailer_line_empty = true;
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
}

const fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    find_crlf_from(buf, 0)
}

fn find_crlf_from(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| p + start)
}

fn validate_token(bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() {
        return Err(ParseError::InvalidHeaderName.into());
    }
    for &b in bytes {
        if !is_tchar(b) {
            return Err(ParseError::InvalidHeaderName.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_response(text: &[u8]) -> (ResponseHead<'_>, usize, Vec<Header<'_>>) {
        let mut headers = vec![Header::empty(); 32];
        let (head, consumed) = parse_response_head(text, &mut headers).unwrap();
        headers.truncate(head.header_count);
        (head, consumed, headers)
    }

    // --- Response head parsing ---

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
        let result = parse_response_head(raw, &mut headers);
        assert_eq!(result.unwrap_err(), Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn too_many_headers_error() {
        let raw = b"HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\n\r\n";
        let mut headers = [Header::empty(); 1];
        let result = parse_response_head(raw, &mut headers);
        assert_eq!(
            result.unwrap_err(),
            Error::Parse(ParseError::TooManyHeaders)
        );
    }

    // --- Body framing ---

    #[test]
    fn body_framing_content_length() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"42",
        }];
        assert_eq!(
            determine_body_framing(StatusCode::OK, false, &headers, 1),
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
            determine_body_framing(StatusCode::OK, false, &headers, 1),
            BodyFraming::Chunked
        );
    }

    #[test]
    fn body_framing_none_for_204() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            determine_body_framing(StatusCode::NO_CONTENT, false, &headers, 0),
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
            determine_body_framing(StatusCode::OK, true, &headers, 1),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_until_close() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            determine_body_framing(StatusCode::OK, false, &headers, 0),
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
            determine_body_framing(StatusCode::OK, false, &headers, 2),
            BodyFraming::Chunked
        );
    }

    // --- Chunked decoder ---

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
                DecodeResult::Done => break,
                DecodeResult::NeedMore => break,
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

        for &byte in input.iter() {
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
                DecodeResult::Done => break,
                DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunked_hex_cases() {
        // Uppercase hex
        let input = b"A\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(10));
        assert_eq!(&output[..10], b"0123456789");

        // Lowercase hex
        let input = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(10));
    }

    #[test]
    fn chunked_small_output_buffer() {
        let input = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 4]; // smaller than chunk
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"0123456789");
        assert!(decoder.is_done());
    }
}
