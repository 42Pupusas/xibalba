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
pub enum Error {
    Url(UrlError),
    Parse(ParseError),
    Serialize(SerializeError),
    Io(IoError),
    Tls(TlsError),
    Connection(ConnectionError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionError {
    ConnectionClosed,
    ContentLengthOverflow,
    InvalidUtf8Body,
    HeaderRangeOverflow,
    HeaderNotInBuffer,
    BodyTooLarge,
    HeadTooLarge,
    TooManyRedirects,
    /// The async client's background reader thread has exited.
    ReaderGone,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Chunked encoding: chunk size line is malformed.
    InvalidChunkSize,
    /// Chunked encoding: expected CRLF but got something else.
    InvalidChunkTerminator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerializeError {
    /// The output buffer is too small to hold the serialized request.
    BufferTooSmall,
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
            Self::ReaderGone => f.write_str("background reader thread has exited"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(e) => write!(f, "url error: {e}"),
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
            Self::InvalidChunkSize => "invalid chunk size",
            Self::InvalidChunkTerminator => "invalid chunk terminator",
        };
        f.write_str(msg)
    }
}

impl fmt::Display for SerializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall => f.write_str("output buffer too small"),
        }
    }
}

impl From<UrlError> for Error {
    fn from(e: UrlError) -> Self {
        Self::Url(e)
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
            Self::Url(e) => Some(e),
            Self::Parse(e) => Some(e),
            Self::Serialize(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Tls(e) => Some(e),
            Self::Connection(e) => Some(e),
        }
    }
}
