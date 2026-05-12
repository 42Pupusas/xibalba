use core::fmt;

use crate::error::ParseError;

/// Compare two byte slices for ASCII-case-insensitive equality.
#[inline]
pub(crate) fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(&x, &y)| {
        if x == y {
            return true;
        }
        let xl = x | 0x20;
        let yl = y | 0x20;
        xl == yl && xl.is_ascii_lowercase()
    })
}

/// Trim optional whitespace (SP, HTAB) from both ends of a byte slice.
#[must_use]
pub fn trim_ows(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|&b| b != b' ' && b != b'\t')
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|&b| b != b' ' && b != b'\t')
        .map_or(start, |p| p + 1);
    &bytes[start..end]
}

/// Parse a `u64` from ASCII digit bytes without going through str.
/// Skips leading/trailing OWS.
#[must_use]
pub fn parse_u64_from_bytes(bytes: &[u8]) -> Option<u64> {
    let bytes = trim_ows(bytes);
    if bytes.is_empty() {
        return None;
    }
    let mut result: u64 = 0;
    for &b in bytes {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        result = result.checked_mul(10)?.checked_add(u64::from(digit))?;
    }
    Some(result)
}

/// Check if a comma-separated header value contains a token (case-insensitive).
#[must_use]
pub fn contains_token_ignore_case(value: &[u8], token: &[u8]) -> bool {
    value.split(|&b| b == b',').any(|part| {
        let trimmed = trim_ows(part);
        ascii_eq_ignore_case(trimmed, token)
    })
}

/// RFC 7230 token character validation.
///
/// 256-byte lookup table: one byte per possible input value, non-zero = valid
/// tchar. Compiles to a single indexed load + test — no shifts, no branches.
const TCHAR_TABLE: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut b: u8 = 0;
    loop {
        if matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
                | b'0'..=b'9'
                | b'A'..=b'Z'
                | b'a'..=b'z'
        ) {
            table[b as usize] = 1;
        }
        if b == 255 {
            break;
        }
        b += 1;
    }
    table
};

pub(crate) const fn is_tchar(b: u8) -> bool {
    TCHAR_TABLE[b as usize] != 0
}

#[derive(Debug, Clone)]
pub enum HeaderName<'a> {
    Host,
    ContentLength,
    ContentType,
    TransferEncoding,
    Connection,
    Accept,
    UserAgent,
    AcceptEncoding,
    Location,
    CacheControl,
    Date,
    Server,
    ContentEncoding,
    SetCookie,
    Cookie,
    Authorization,
    /// A header name not in the well-known set.
    Unknown(&'a [u8]),
    /// Raw unparsed bytes from the wire — compared case-insensitively.
    /// Used by the parser to defer classification until the name is actually needed.
    Raw(&'a [u8]),
}

impl PartialEq for HeaderName<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Raw participates in case-insensitive comparison against canonical bytes.
            (Self::Raw(a), Self::Raw(b)) => ascii_eq_ignore_case(a, b),
            (Self::Raw(raw), known) | (known, Self::Raw(raw)) => {
                ascii_eq_ignore_case(raw, known.as_bytes())
            }
            // Unknown is exact-bytes equality (caller controls casing).
            (Self::Unknown(a), Self::Unknown(b)) => a == b,
            // Two known variants are equal iff they are the same variant.
            _ => self.as_bytes() == other.as_bytes(),
        }
    }
}

impl Eq for HeaderName<'_> {}

impl core::hash::Hash for HeaderName<'_> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        // Hash the lowercased canonical bytes so Raw and known variants hash the same.
        match self {
            Self::Raw(b) | Self::Unknown(b) => {
                for byte in *b {
                    state.write_u8(byte.to_ascii_lowercase());
                }
            }
            known => {
                for byte in known.as_bytes() {
                    state.write_u8(byte.to_ascii_lowercase());
                }
            }
        }
    }
}

impl<'a> HeaderName<'a> {
    /// Canonical wire-format name as bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Host => b"Host",
            Self::ContentLength => b"Content-Length",
            Self::ContentType => b"Content-Type",
            Self::TransferEncoding => b"Transfer-Encoding",
            Self::Connection => b"Connection",
            Self::Accept => b"Accept",
            Self::UserAgent => b"User-Agent",
            Self::AcceptEncoding => b"Accept-Encoding",
            Self::Location => b"Location",
            Self::CacheControl => b"Cache-Control",
            Self::Date => b"Date",
            Self::Server => b"Server",
            Self::ContentEncoding => b"Content-Encoding",
            Self::SetCookie => b"Set-Cookie",
            Self::Cookie => b"Cookie",
            Self::Authorization => b"Authorization",
            Self::Unknown(raw) | Self::Raw(raw) => raw,
        }
    }

    /// Canonical wire-format name as str.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Host => "Host",
            Self::ContentLength => "Content-Length",
            Self::ContentType => "Content-Type",
            Self::TransferEncoding => "Transfer-Encoding",
            Self::Connection => "Connection",
            Self::Accept => "Accept",
            Self::UserAgent => "User-Agent",
            Self::AcceptEncoding => "Accept-Encoding",
            Self::Location => "Location",
            Self::CacheControl => "Cache-Control",
            Self::Date => "Date",
            Self::Server => "Server",
            Self::ContentEncoding => "Content-Encoding",
            Self::SetCookie => "Set-Cookie",
            Self::Cookie => "Cookie",
            Self::Authorization => "Authorization",
            Self::Unknown(raw) | Self::Raw(raw) => {
                core::str::from_utf8(raw).unwrap_or("<invalid>")
            }
        }
    }

    /// Parse a header name from raw bytes, case-insensitively.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        match bytes.len() {
            4 => {
                if ascii_eq_ignore_case(bytes, b"Host") {
                    return Self::Host;
                }
                if ascii_eq_ignore_case(bytes, b"Date") {
                    return Self::Date;
                }
            }
            6 => {
                if ascii_eq_ignore_case(bytes, b"Accept") {
                    return Self::Accept;
                }
                if ascii_eq_ignore_case(bytes, b"Cookie") {
                    return Self::Cookie;
                }
                if ascii_eq_ignore_case(bytes, b"Server") {
                    return Self::Server;
                }
            }
            8 if ascii_eq_ignore_case(bytes, b"Location") => {
                return Self::Location;
            }
            10 => {
                if ascii_eq_ignore_case(bytes, b"User-Agent") {
                    return Self::UserAgent;
                }
                if ascii_eq_ignore_case(bytes, b"Connection") {
                    return Self::Connection;
                }
                if ascii_eq_ignore_case(bytes, b"Set-Cookie") {
                    return Self::SetCookie;
                }
            }
            12 if ascii_eq_ignore_case(bytes, b"Content-Type") => {
                return Self::ContentType;
            }
            13 => {
                if ascii_eq_ignore_case(bytes, b"Authorization") {
                    return Self::Authorization;
                }
                if ascii_eq_ignore_case(bytes, b"Cache-Control") {
                    return Self::CacheControl;
                }
            }
            14 if ascii_eq_ignore_case(bytes, b"Content-Length") => {
                return Self::ContentLength;
            }
            15 if ascii_eq_ignore_case(bytes, b"Accept-Encoding") => {
                return Self::AcceptEncoding;
            }
            16 if ascii_eq_ignore_case(bytes, b"Content-Encoding") => {
                return Self::ContentEncoding;
            }
            17 if ascii_eq_ignore_case(bytes, b"Transfer-Encoding") => {
                return Self::TransferEncoding;
            }
            _ => {}
        }
        Self::Unknown(bytes)
    }
}

impl AsRef<str> for HeaderName<'_> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for HeaderName<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for HeaderName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header<'a> {
    pub name: HeaderName<'a>,
    pub value: &'a [u8],
}

impl Header<'_> {
    /// Create an empty placeholder header for initializing buffers.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            name: HeaderName::Host,
            value: b"",
        }
    }
}

/// A view over a caller-provided buffer of parsed headers.
#[derive(Debug)]
pub struct Headers<'buf, 'data> {
    headers: &'buf [Header<'data>],
    len: usize,
}

impl<'buf, 'data> Headers<'buf, 'data> {
    #[must_use]
    pub const fn new(headers: &'buf [Header<'data>], len: usize) -> Self {
        Self { headers, len }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &Header<'data>> {
        self.headers[..self.len].iter()
    }

    /// Find the first header with the given name.
    #[must_use]
    pub fn get(&self, name: &HeaderName<'_>) -> Option<&'data [u8]> {
        self.headers[..self.len]
            .iter()
            .find(|h| &h.name == name)
            .map(|h| h.value)
    }

    /// Parse the `Content-Length` value as `u64`.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::InvalidContentLength` if the value is present
    /// but not valid ASCII digits.
    #[must_use]
    pub fn content_length(&self) -> Option<Result<u64, ParseError>> {
        self.get(&HeaderName::ContentLength)
            .map(|val| parse_u64_from_bytes(val).ok_or(ParseError::InvalidContentLength))
    }

    /// Check if `Transfer-Encoding` includes "chunked" (case-insensitive).
    #[must_use]
    pub fn is_chunked(&self) -> bool {
        self.get(&HeaderName::TransferEncoding)
            .is_some_and(|val| contains_token_ignore_case(val, b"chunked"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_eq_exact_match() {
        assert!(ascii_eq_ignore_case(b"hello", b"hello"));
    }

    #[test]
    fn ascii_eq_case_insensitive() {
        assert!(ascii_eq_ignore_case(b"Host", b"host"));
        assert!(ascii_eq_ignore_case(b"HOST", b"host"));
        assert!(ascii_eq_ignore_case(b"Content-Length", b"content-length"));
    }

    #[test]
    fn ascii_eq_different_lengths() {
        assert!(!ascii_eq_ignore_case(b"Host", b"Hosts"));
    }

    #[test]
    fn ascii_eq_non_alpha_no_alias() {
        assert!(!ascii_eq_ignore_case(b"@", b"`"));
    }

    #[test]
    fn trim_ows_both_ends() {
        assert_eq!(trim_ows(b"  hello  "), b"hello");
        assert_eq!(trim_ows(b"\thello\t"), b"hello");
        assert_eq!(trim_ows(b"hello"), b"hello");
        assert_eq!(trim_ows(b""), b"");
        assert_eq!(trim_ows(b"   "), b"");
    }

    #[test]
    fn parse_u64_valid() {
        assert_eq!(parse_u64_from_bytes(b"0"), Some(0));
        assert_eq!(parse_u64_from_bytes(b"12345"), Some(12345));
        assert_eq!(parse_u64_from_bytes(b" 42 "), Some(42));
    }

    #[test]
    fn parse_u64_invalid() {
        assert_eq!(parse_u64_from_bytes(b""), None);
        assert_eq!(parse_u64_from_bytes(b"abc"), None);
        assert_eq!(parse_u64_from_bytes(b"12x"), None);
    }

    #[test]
    fn contains_token_single() {
        assert!(contains_token_ignore_case(b"chunked", b"chunked"));
        assert!(contains_token_ignore_case(b"Chunked", b"chunked"));
    }

    #[test]
    fn contains_token_in_list() {
        assert!(contains_token_ignore_case(b"gzip, chunked", b"chunked"));
        assert!(contains_token_ignore_case(b"gzip , Chunked ", b"chunked"));
    }

    #[test]
    fn contains_token_missing() {
        assert!(!contains_token_ignore_case(b"gzip, deflate", b"chunked"));
    }

    #[test]
    fn tchar_validation() {
        assert!(is_tchar(b'a'));
        assert!(is_tchar(b'Z'));
        assert!(is_tchar(b'0'));
        assert!(is_tchar(b'-'));
        assert!(is_tchar(b'!'));
        assert!(!is_tchar(b' '));
        assert!(!is_tchar(b':'));
        assert!(!is_tchar(b'\0'));
    }

    #[test]
    fn header_name_known_case_insensitive() {
        assert_eq!(
            HeaderName::from_bytes(b"content-length"),
            HeaderName::ContentLength
        );
        assert_eq!(
            HeaderName::from_bytes(b"CONTENT-LENGTH"),
            HeaderName::ContentLength
        );
        assert_eq!(
            HeaderName::from_bytes(b"Transfer-Encoding"),
            HeaderName::TransferEncoding
        );
        assert_eq!(
            HeaderName::from_bytes(b"transfer-encoding"),
            HeaderName::TransferEncoding
        );
        assert_eq!(HeaderName::from_bytes(b"host"), HeaderName::Host);
        assert_eq!(HeaderName::from_bytes(b"HOST"), HeaderName::Host);
    }

    #[test]
    fn header_name_unknown_preserved() {
        let name = HeaderName::from_bytes(b"X-Custom");
        assert_eq!(name, HeaderName::Unknown(b"X-Custom"));
        assert_eq!(name.as_bytes(), b"X-Custom");
    }

    #[test]
    fn header_name_display() {
        assert_eq!(HeaderName::ContentLength.to_string(), "Content-Length");
        assert_eq!(HeaderName::Host.to_string(), "Host");
    }

    #[test]
    fn headers_collection() {
        let hdrs = [
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];

        let headers = Headers::new(&hdrs, 2);
        assert_eq!(headers.len(), 2);
        assert!(!headers.is_empty());
        assert_eq!(
            headers.get(&HeaderName::ContentLength),
            Some(b"42" as &[u8])
        );
        assert_eq!(headers.content_length(), Some(Ok(42)));
        assert!(headers.is_chunked());
    }

    #[test]
    fn headers_empty() {
        let hdrs: [Header<'_>; 0] = [];
        let headers = Headers::new(&hdrs, 0);
        assert!(headers.is_empty());
        assert_eq!(headers.get(&HeaderName::Host), None);
        assert_eq!(headers.content_length(), None);
        assert!(!headers.is_chunked());
    }
}
