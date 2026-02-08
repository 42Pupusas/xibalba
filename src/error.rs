use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Url(UrlError),
    Parse(ParseError),
    Serialize(SerializeError),
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(e) => write!(f, "url error: {e}"),
            Self::Parse(e) => write!(f, "parse error: {e}"),
            Self::Serialize(e) => write!(f, "serialize error: {e}"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_formats() {
        let e = Error::Url(UrlError::Empty);
        assert_eq!(e.to_string(), "url error: input is empty");

        let e = Error::Parse(ParseError::Incomplete);
        assert_eq!(e.to_string(), "parse error: incomplete input");

        let e = Error::Serialize(SerializeError::BufferTooSmall);
        assert_eq!(e.to_string(), "serialize error: output buffer too small");
    }

    #[test]
    fn url_error_invalid_byte_shows_offset() {
        let e = UrlError::InvalidByte(42);
        assert_eq!(e.to_string(), "invalid byte at offset 42");
    }

    #[test]
    fn from_conversions() {
        let e: Error = UrlError::MissingHost.into();
        assert_eq!(e, Error::Url(UrlError::MissingHost));

        let e: Error = ParseError::InvalidMethod.into();
        assert_eq!(e, Error::Parse(ParseError::InvalidMethod));

        let e: Error = SerializeError::BufferTooSmall.into();
        assert_eq!(e, Error::Serialize(SerializeError::BufferTooSmall));
    }
}
