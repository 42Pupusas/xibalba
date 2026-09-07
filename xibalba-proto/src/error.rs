use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoError {
    pub kind: std::io::ErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsError {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    UrlParse(UrlError),
    Parse(ParseError),
    Serialize(SerializeError),
    Io(IoError),
    Tls(TlsError),
    Connection(ConnectionError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionError {
    ConnectionClosed,
    ContentLengthOverflow,
    InvalidUtf8Body,
    HeaderRangeOverflow,
    HeaderNotInBuffer,
    BodyTooLarge,
    HeadTooLarge,
    TooManyRedirects,
    /// More 1xx interim heads arrived than the client will skip before a
    /// final response.
    TooManyInterimResponses,
    /// A blocking socket timeout is required for silence budgets and async
    /// cancellation to regain control from a read.
    InfiniteReadTimeout,
    /// A timeout or budget was configured as zero. A zero read timeout makes
    /// every read tick instantly and a zero budget expires before the first
    /// read completes, so both turn every request into an immediate failure.
    ZeroDuration,
    /// A per-read timeout is longer than a silence budget it subdivides, so
    /// the first blocked read overshoots the budget it is meant to enforce.
    TimeoutExceedsBudget,
    /// The operation ran past the total deadline configured for it. Unlike
    /// the silence budgets, which reset on every byte received and so permit
    /// an indefinitely slow but unbroken transfer, this bounds the exchange
    /// as a whole; no further work was started once it passed.
    RequestDeadlineExceeded,
    /// A plaintext connector was given an HTTPS URL. Connecting anyway would
    /// send the request, and any credentials it carries, in the clear.
    PlaintextConnectorForHttps,
    /// Establishing the connection ran past the deadline the client gave it.
    /// Resolution, TCP connect, and any TLS handshake share one deadline, so
    /// this covers all three; the connection was not established.
    ConnectDeadlineExceeded,
    /// The async client's background reader thread has exited.
    ReaderGone,
    /// A redirect from an HTTPS origin to a plaintext HTTP target. Following
    /// it would move the request, and any credentials it carries, onto an
    /// unprotected connection.
    InsecureRedirect,
    /// Too many requests are already in flight. Backpressure rather than a
    /// transport failure: the request was not sent, and submitting again
    /// once an earlier response finishes will succeed.
    TooManyRequests,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UrlError {
    /// Input is empty.
    Empty,
    /// Scheme portion is missing or unrecognized.
    InvalidScheme,
    /// The authority section (host) is missing.
    MissingHost,
    /// Port is present but not a valid `u16`.
    InvalidPort,
    /// A byte that is not allowed in the given URL component was found.
    InvalidByte(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseError {
    /// Response is incomplete; need more data.
    Incomplete,
    /// Version string is not `HTTP/1.0` or `HTTP/1.1`.
    InvalidVersion,
    /// Status code is not three ASCII digits or is out of range.
    InvalidStatusCode,
    /// Method is not a recognized HTTP method token.
    InvalidMethod,
    /// A header line lacks the `:` separator.
    MissingColon,
    /// A header name contains an invalid byte (per RFC 7230 token rule).
    InvalidHeaderName,
    /// A header value contains a byte outside the allowed range.
    InvalidHeaderValue,
    /// The caller-provided header buffer is too small.
    TooManyHeaders,
    /// `Content-Length` value is not valid ASCII digits.
    InvalidContentLength,
    /// The reason phrase contains a byte outside HTAB / SP / VCHAR /
    /// obs-text.
    InvalidReasonPhrase,
    /// `Transfer-Encoding` violates the grammar: `chunked` appears more than
    /// once, or a coding is empty.
    InvalidTransferEncoding,
    /// `Transfer-Encoding` names a coding this client cannot decode, such as
    /// `gzip`. Framing the body anyway would present still-encoded bytes as
    /// the decoded response.
    UnsupportedTransferCoding,
    /// Chunked encoding: chunk size line is malformed.
    InvalidChunkSize,
    /// Chunked encoding: expected CRLF but got something else.
    InvalidChunkTerminator,
    /// Chunked encoding: a chunk extension or trailer section exceeded the
    /// decoder's metadata budget, or contained a byte the grammar forbids.
    /// A peer can otherwise send metadata forever without ever delivering
    /// body data or ending the stream.
    InvalidChunkMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SerializeError {
    /// The output buffer is too small to hold the serialized request.
    BufferTooSmall,
    /// The request target is empty or contains bytes that cannot appear
    /// in a request line (CTLs, space, DEL) — including CRLF injection.
    InvalidPath,
    /// A header name is not a valid token, or a value contains bytes
    /// outside the field-value set (CTLs other than HTAB) — including
    /// CRLF injection.
    InvalidHeader,
    /// The request declares a header the client serializes itself
    /// (`Host`, `Content-Length`, `Transfer-Encoding`) or the same
    /// header name twice.
    DuplicateHeader,
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.message, self.kind)
    }
}

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionClosed => f.write_str("connection closed unexpectedly"),
            Self::ContentLengthOverflow => f.write_str("content-length exceeds usize"),
            Self::InvalidUtf8Body => f.write_str("response body is not valid UTF-8"),
            Self::HeaderRangeOverflow => f.write_str("header offset/length overflows u16"),
            Self::HeaderNotInBuffer => f.write_str("header name not in head buffer"),
            Self::BodyTooLarge => f.write_str("response body exceeds size limit"),
            Self::HeadTooLarge => f.write_str("response head exceeds size limit"),
            Self::TooManyRedirects => f.write_str("too many redirects"),
            Self::TooManyInterimResponses => f.write_str("too many 1xx interim responses"),
            Self::InfiniteReadTimeout => f.write_str("read timeout must be finite"),
            Self::ZeroDuration => f.write_str("timeouts and silence budgets must be non-zero"),
            Self::TimeoutExceedsBudget => {
                f.write_str("per-read timeout exceeds the silence budget it subdivides")
            }
            Self::RequestDeadlineExceeded => {
                f.write_str("request deadline exceeded before the operation completed")
            }
            Self::PlaintextConnectorForHttps => {
                f.write_str("plaintext connector refused an https url")
            }
            Self::ConnectDeadlineExceeded => {
                f.write_str("connect deadline exceeded before the connection was established")
            }
            Self::ReaderGone => f.write_str("background reader thread has exited"),
            Self::TooManyRequests => f.write_str("too many requests are already in flight"),
            Self::InsecureRedirect => f.write_str("redirect downgrades https to http"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UrlParse(e) => write!(f, "url error: {e}"),
            Self::Parse(e) => write!(f, "parse error: {e}"),
            Self::Serialize(e) => write!(f, "serialize error: {e}"),
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Tls(e) => write!(f, "tls error: {e}"),
            Self::Connection(e) => write!(f, "connection error: {e}"),
        }
    }
}

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("input is empty"),
            Self::InvalidScheme => f.write_str("invalid or missing scheme"),
            Self::MissingHost => f.write_str("missing host"),
            Self::InvalidPort => f.write_str("invalid port number"),
            Self::InvalidByte(pos) => write!(f, "invalid byte at offset {pos}"),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::Incomplete => "incomplete input",
            Self::InvalidVersion => "invalid HTTP version",
            Self::InvalidStatusCode => "invalid status code",
            Self::InvalidMethod => "invalid HTTP method",
            Self::MissingColon => "missing colon in header line",
            Self::InvalidHeaderName => "invalid header name byte",
            Self::InvalidHeaderValue => "invalid header value byte",
            Self::TooManyHeaders => "caller header buffer is too small",
            Self::InvalidContentLength => "invalid Content-Length value",
            Self::InvalidReasonPhrase => "invalid reason phrase",
            Self::InvalidTransferEncoding => "invalid transfer-encoding",
            Self::UnsupportedTransferCoding => "unsupported transfer coding",
            Self::InvalidChunkSize => "invalid chunk size",
            Self::InvalidChunkTerminator => "invalid chunk terminator",
            Self::InvalidChunkMetadata => "chunk extension or trailer is invalid or too large",
        };
        f.write_str(msg)
    }
}

impl fmt::Display for SerializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall => f.write_str("output buffer too small"),
            Self::InvalidPath => f.write_str("invalid request target"),
            Self::InvalidHeader => f.write_str("invalid request header"),
            Self::DuplicateHeader => f.write_str("duplicate request header"),
        }
    }
}

impl From<UrlError> for Error {
    fn from(e: UrlError) -> Self {
        Self::UrlParse(e)
    }
}

impl From<ParseError> for Error {
    fn from(e: ParseError) -> Self {
        Self::Parse(e)
    }
}

impl From<SerializeError> for Error {
    fn from(e: SerializeError) -> Self {
        Self::Serialize(e)
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Self::Io(e)
    }
}

impl From<TlsError> for Error {
    fn from(e: TlsError) -> Self {
        Self::Tls(e)
    }
}

impl From<ConnectionError> for Error {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(IoError {
            kind: e.kind(),
            message: e.to_string(),
        })
    }
}

impl std::error::Error for IoError {}
impl std::error::Error for TlsError {}
impl std::error::Error for UrlError {}
impl std::error::Error for ParseError {}
impl std::error::Error for SerializeError {}
impl std::error::Error for ConnectionError {}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UrlParse(e) => Some(e),
            Self::Parse(e) => Some(e),
            Self::Serialize(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Tls(e) => Some(e),
            Self::Connection(e) => Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    fn display<E: fmt::Display>(e: E) -> String {
        e.to_string()
    }

    #[test]
    fn io_error_display() {
        let e = IoError {
            kind: std::io::ErrorKind::NotFound,
            message: "oops".into(),
        };
        assert_eq!(display(e), "oops: entity not found");
    }

    #[test]
    fn tls_error_display() {
        let e = TlsError {
            message: "handshake failed".into(),
        };
        assert_eq!(display(e), "handshake failed");
    }

    #[test]
    fn connection_error_display() {
        let cases = [
            (
                ConnectionError::ConnectionClosed,
                "connection closed unexpectedly",
            ),
            (
                ConnectionError::ContentLengthOverflow,
                "content-length exceeds usize",
            ),
            (
                ConnectionError::InvalidUtf8Body,
                "response body is not valid UTF-8",
            ),
            (
                ConnectionError::HeaderRangeOverflow,
                "header offset/length overflows u16",
            ),
            (
                ConnectionError::HeaderNotInBuffer,
                "header name not in head buffer",
            ),
            (
                ConnectionError::BodyTooLarge,
                "response body exceeds size limit",
            ),
            (
                ConnectionError::HeadTooLarge,
                "response head exceeds size limit",
            ),
            (ConnectionError::TooManyRedirects, "too many redirects"),
            (
                ConnectionError::TooManyInterimResponses,
                "too many 1xx interim responses",
            ),
            (
                ConnectionError::InfiniteReadTimeout,
                "read timeout must be finite",
            ),
            (
                ConnectionError::RequestDeadlineExceeded,
                "request deadline exceeded before the operation completed",
            ),
            (
                ConnectionError::ConnectDeadlineExceeded,
                "connect deadline exceeded before the connection was established",
            ),
            (
                ConnectionError::ReaderGone,
                "background reader thread has exited",
            ),
            (ConnectionError::Other("custom".into()), "custom"),
        ];
        for (err, expected) in cases {
            assert_eq!(display(err), expected);
        }
    }

    #[test]
    fn url_error_display() {
        let cases = [
            (UrlError::Empty, "input is empty"),
            (UrlError::InvalidScheme, "invalid or missing scheme"),
            (UrlError::MissingHost, "missing host"),
            (UrlError::InvalidPort, "invalid port number"),
        ];
        for (err, expected) in cases {
            assert_eq!(display(err.clone()), expected);
        }
        assert_eq!(
            display(UrlError::InvalidByte(7)),
            "invalid byte at offset 7"
        );
    }

    #[test]
    fn parse_error_display() {
        let cases = [
            (ParseError::Incomplete, "incomplete input"),
            (ParseError::InvalidVersion, "invalid HTTP version"),
            (ParseError::InvalidStatusCode, "invalid status code"),
            (ParseError::InvalidMethod, "invalid HTTP method"),
            (ParseError::MissingColon, "missing colon in header line"),
            (ParseError::InvalidHeaderName, "invalid header name byte"),
            (ParseError::InvalidHeaderValue, "invalid header value byte"),
            (
                ParseError::TooManyHeaders,
                "caller header buffer is too small",
            ),
            (
                ParseError::InvalidContentLength,
                "invalid Content-Length value",
            ),
            (ParseError::InvalidChunkSize, "invalid chunk size"),
            (
                ParseError::InvalidChunkTerminator,
                "invalid chunk terminator",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(display(err), expected);
        }
    }

    #[test]
    fn serialize_error_display() {
        assert_eq!(
            display(SerializeError::BufferTooSmall),
            "output buffer too small"
        );
        assert_eq!(
            display(SerializeError::InvalidPath),
            "invalid request target"
        );
        assert_eq!(
            display(SerializeError::InvalidHeader),
            "invalid request header"
        );
        assert_eq!(
            display(SerializeError::DuplicateHeader),
            "duplicate request header"
        );
    }

    #[test]
    fn error_display_wraps_variants() {
        assert!(
            display(Error::UrlParse(UrlError::Empty)).contains("url error"),
            "{}",
            display(Error::UrlParse(UrlError::Empty))
        );
        assert!(
            display(Error::Parse(ParseError::Incomplete)).contains("parse error"),
            "{}",
            display(Error::Parse(ParseError::Incomplete))
        );
        assert!(
            display(Error::Serialize(SerializeError::BufferTooSmall)).contains("serialize error"),
            "{}",
            display(Error::Serialize(SerializeError::BufferTooSmall))
        );
        assert!(
            display(Error::Io(IoError {
                kind: std::io::ErrorKind::Other,
                message: "x".into(),
            }))
            .contains("io error"),
            "{}",
            display(Error::Io(IoError {
                kind: std::io::ErrorKind::Other,
                message: "x".into(),
            }))
        );
        assert!(
            display(Error::Tls(TlsError {
                message: "x".into(),
            }))
            .contains("tls error"),
            "{}",
            display(Error::Tls(TlsError {
                message: "x".into(),
            }))
        );
        assert!(
            display(Error::Connection(ConnectionError::ReaderGone)).contains("connection error"),
            "{}",
            display(Error::Connection(ConnectionError::ReaderGone))
        );
    }

    #[test]
    fn error_source_delegates_to_inner() {
        let err = Error::UrlParse(UrlError::Empty);
        assert!(StdError::source(&err).is_some());

        let err = Error::Parse(ParseError::Incomplete);
        assert!(StdError::source(&err).is_some());

        let err = Error::Serialize(SerializeError::BufferTooSmall);
        assert!(StdError::source(&err).is_some());

        let err = Error::Io(IoError {
            kind: std::io::ErrorKind::Other,
            message: "x".into(),
        });
        assert!(StdError::source(&err).is_some());

        let err = Error::Tls(TlsError {
            message: "x".into(),
        });
        assert!(StdError::source(&err).is_some());

        let err = Error::Connection(ConnectionError::ReaderGone);
        assert!(StdError::source(&err).is_some());
    }
}
