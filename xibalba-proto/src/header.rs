use core::fmt;

use crate::error::ParseError;

/// Compare two byte slices for ASCII-case-insensitive equality.
#[inline]
#[must_use]
pub fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
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

/// A parsed or constructed HTTP header name.
///
/// Internally either a well-known header (looked up case-insensitively) or
/// an arbitrary byte slice. The public API preserves the original enum-like
/// usage through associated constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderName<'a> {
    inner: HeaderNameInner<'a>,
}

#[derive(Debug, Clone, Copy)]
enum HeaderNameInner<'a> {
    Known(KnownHeader),
    Unknown(&'a [u8]),
    Raw(&'a [u8]),
}

impl HeaderName<'_> {
    #[allow(non_upper_case_globals)]
    pub const Host: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Host),
    };
    #[allow(non_upper_case_globals)]
    pub const ContentLength: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::ContentLength),
    };
    #[allow(non_upper_case_globals)]
    pub const ContentType: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::ContentType),
    };
    #[allow(non_upper_case_globals)]
    pub const TransferEncoding: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::TransferEncoding),
    };
    #[allow(non_upper_case_globals)]
    pub const Connection: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Connection),
    };
    #[allow(non_upper_case_globals)]
    pub const Accept: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Accept),
    };
    #[allow(non_upper_case_globals)]
    pub const UserAgent: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::UserAgent),
    };
    #[allow(non_upper_case_globals)]
    pub const AcceptEncoding: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::AcceptEncoding),
    };
    #[allow(non_upper_case_globals)]
    pub const Location: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Location),
    };
    #[allow(non_upper_case_globals)]
    pub const CacheControl: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::CacheControl),
    };
    #[allow(non_upper_case_globals)]
    pub const Date: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Date),
    };
    #[allow(non_upper_case_globals)]
    pub const Server: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Server),
    };
    #[allow(non_upper_case_globals)]
    pub const ContentEncoding: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::ContentEncoding),
    };
    #[allow(non_upper_case_globals)]
    pub const SetCookie: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::SetCookie),
    };
    #[allow(non_upper_case_globals)]
    pub const Cookie: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Cookie),
    };
    #[allow(non_upper_case_globals)]
    pub const Authorization: HeaderName<'static> = HeaderName {
        inner: HeaderNameInner::Known(KnownHeader::Authorization),
    };
}

/// Internal lookup table for well-known header names.
///
/// Each entry carries its own canonical byte representation, so we can parse
/// case-insensitively and render back to the canonical form without a giant
/// literal match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
enum KnownHeader {
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
}

const KNOWN_CANONICAL: [&[u8]; 16] = [
    b"Host",
    b"Content-Length",
    b"Content-Type",
    b"Transfer-Encoding",
    b"Connection",
    b"Accept",
    b"User-Agent",
    b"Accept-Encoding",
    b"Location",
    b"Cache-Control",
    b"Date",
    b"Server",
    b"Content-Encoding",
    b"Set-Cookie",
    b"Cookie",
    b"Authorization",
];

impl KnownHeader {
    const fn canonical(self) -> &'static [u8] {
        KNOWN_CANONICAL[self as usize]
    }
}

const KNOWN_HEADERS: &[(KnownHeader, &[u8])] = &[
    (KnownHeader::Host, b"host"),
    (KnownHeader::ContentLength, b"content-length"),
    (KnownHeader::ContentType, b"content-type"),
    (KnownHeader::TransferEncoding, b"transfer-encoding"),
    (KnownHeader::Connection, b"connection"),
    (KnownHeader::Accept, b"accept"),
    (KnownHeader::UserAgent, b"user-agent"),
    (KnownHeader::AcceptEncoding, b"accept-encoding"),
    (KnownHeader::Location, b"location"),
    (KnownHeader::CacheControl, b"cache-control"),
    (KnownHeader::Date, b"date"),
    (KnownHeader::Server, b"server"),
    (KnownHeader::ContentEncoding, b"content-encoding"),
    (KnownHeader::SetCookie, b"set-cookie"),
    (KnownHeader::Cookie, b"cookie"),
    (KnownHeader::Authorization, b"authorization"),
];

impl<'a> HeaderName<'a> {
    /// Canonical wire-format name as bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        match self.inner {
            HeaderNameInner::Known(k) => k.canonical(),
            HeaderNameInner::Unknown(raw) | HeaderNameInner::Raw(raw) => raw,
        }
    }

    /// Canonical wire-format name as str.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self.inner {
            HeaderNameInner::Known(k) => core::str::from_utf8(k.canonical()).unwrap_or("<invalid>"),
            HeaderNameInner::Unknown(raw) | HeaderNameInner::Raw(raw) => {
                core::str::from_utf8(raw).unwrap_or("<invalid>")
            }
        }
    }

    /// Parse a header name from raw bytes, case-insensitively.
    #[must_use]
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        KNOWN_HEADERS
            .iter()
            .find(|(_, lower)| ascii_eq_ignore_case(bytes, lower))
            .map_or_else(
                || Self {
                    inner: HeaderNameInner::Unknown(bytes),
                },
                |(known, _)| Self {
                    inner: HeaderNameInner::Known(*known),
                },
            )
    }

    /// Wrap raw bytes from the wire; comparison is case-insensitive.
    #[must_use]
    pub const fn raw(bytes: &'a [u8]) -> Self {
        Self {
            inner: HeaderNameInner::Raw(bytes),
        }
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

impl PartialEq for HeaderNameInner<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Known variants are equal iff they are the same variant.
            (Self::Known(a), Self::Known(b)) => a == b,
            // Raw participates in case-insensitive comparison against canonical bytes.
            (Self::Raw(a), Self::Raw(b)) => ascii_eq_ignore_case(a, b),
            (Self::Raw(raw), Self::Known(k)) | (Self::Known(k), Self::Raw(raw)) => {
                ascii_eq_ignore_case(raw, k.canonical())
            }
            // Unknown is exact-bytes equality (caller controls casing).
            (Self::Unknown(a), Self::Unknown(b)) => a == b,
            (Self::Unknown(_), Self::Known(_) | Self::Raw(_))
            | (Self::Known(_) | Self::Raw(_), Self::Unknown(_)) => false,
        }
    }
}

impl Eq for HeaderNameInner<'_> {}

impl core::hash::Hash for HeaderNameInner<'_> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        // Hash the lowercased canonical bytes so Raw and known variants hash the same.
        let bytes = match self {
            Self::Known(k) => k.canonical(),
            Self::Unknown(b) | Self::Raw(b) => b,
        };
        for byte in bytes {
            state.write_u8(byte.to_ascii_lowercase());
        }
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
        assert_eq!(name.as_bytes(), b"X-Custom");
        assert_eq!(name, HeaderName::from_bytes(b"X-Custom"));
    }

    // ── Adversarial tests ────────────────────────────────────────────────────

    #[test]
    fn ascii_eq_digits_not_case_folded() {
        assert!(ascii_eq_ignore_case(b"Content-1", b"content-1"));
        assert!(!ascii_eq_ignore_case(b"Content-1", b"content-2"));
    }

    #[test]
    fn ascii_eq_hyphens_not_case_folded() {
        assert!(ascii_eq_ignore_case(b"X-My-Header", b"x-my-header"));
    }

    #[test]
    fn parse_u64_overflow() {
        assert_eq!(parse_u64_from_bytes(b"99999999999999999999"), None);
    }

    #[test]
    fn parse_u64_leading_zeros() {
        assert_eq!(parse_u64_from_bytes(b"007"), Some(7));
    }

    #[test]
    fn parse_u64_max() {
        assert_eq!(
            parse_u64_from_bytes(b"18446744073709551615"),
            Some(u64::MAX)
        );
    }

    #[test]
    fn parse_u64_just_past_max() {
        assert_eq!(parse_u64_from_bytes(b"18446744073709551616"), None);
    }

    #[test]
    fn parse_u64_only_whitespace() {
        assert_eq!(parse_u64_from_bytes(b"   "), None);
    }

    #[test]
    fn contains_token_empty_value() {
        assert!(!contains_token_ignore_case(b"", b"chunked"));
    }

    #[test]
    fn contains_token_whitespace_only_commas() {
        assert!(!contains_token_ignore_case(b"  ,  ,  ", b"chunked"));
    }

    #[test]
    fn header_name_length_collision() {
        assert_eq!(
            HeaderName::from_bytes(b"Vary"),
            HeaderName::from_bytes(b"Vary")
        );
        assert_eq!(HeaderName::from_bytes(b"Host"), HeaderName::Host);
        assert_eq!(HeaderName::from_bytes(b"Date"), HeaderName::Date);
    }

    #[test]
    fn raw_vs_known_equality() {
        assert_eq!(
            HeaderName::raw(b"content-length"),
            HeaderName::ContentLength
        );
        assert_eq!(
            HeaderName::raw(b"CONTENT-LENGTH"),
            HeaderName::ContentLength
        );
        assert_eq!(HeaderName::raw(b"host"), HeaderName::Host);
    }

    #[test]
    fn raw_vs_raw_case_insensitive() {
        assert_eq!(HeaderName::raw(b"FOO"), HeaderName::raw(b"foo"));
        assert_ne!(HeaderName::raw(b"FOO"), HeaderName::raw(b"BAR"));
    }

    #[test]
    fn unknown_vs_unknown_exact_match() {
        assert_eq!(
            HeaderName::from_bytes(b"X-Custom"),
            HeaderName::from_bytes(b"X-Custom")
        );
        assert_ne!(
            HeaderName::from_bytes(b"X-Custom"),
            HeaderName::from_bytes(b"x-custom")
        );
    }

    #[test]
    fn tchar_boundary_exhaustive() {
        for &b in b"!#$%&'*+-.^_`|~" {
            assert!(is_tchar(b), "expected tchar: {}", b as char);
        }
        for &b in b" \t\r\n\"(),/:;<=>?@[\\]{}\x7f" {
            assert!(!is_tchar(b), "unexpected tchar: {b:02x}");
        }
    }

    #[test]
    fn contains_token_partial_match_not_accepted() {
        assert!(!contains_token_ignore_case(b"chunked", b"chunk"));
        assert!(!contains_token_ignore_case(b"chunk", b"chunked"));
    }

    #[test]
    fn known_header_name_as_str() {
        assert_eq!(HeaderName::Host.as_str(), "Host");
        assert_eq!(HeaderName::ContentLength.as_str(), "Content-Length");
        assert_eq!(HeaderName::TransferEncoding.as_str(), "Transfer-Encoding");
        assert_eq!(HeaderName::Authorization.as_str(), "Authorization");
    }

    #[test]
    fn unknown_header_name_as_str_preserved() {
        assert_eq!(HeaderName::from_bytes(b"X-Custom").as_str(), "X-Custom");
    }

    #[test]
    fn raw_header_name_as_bytes_case_preserved() {
        assert_eq!(HeaderName::raw(b"x-raw").as_bytes(), b"x-raw");
    }

    #[test]
    fn header_name_hash_known_equals_raw() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash(name: &HeaderName<'_>) -> u64 {
            let mut hasher = DefaultHasher::new();
            name.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(hash(&HeaderName::Host), hash(&HeaderName::raw(b"host")));
        assert_eq!(
            hash(&HeaderName::ContentLength),
            hash(&HeaderName::raw(b"content-length"))
        );
        assert_eq!(
            hash(&HeaderName::from_bytes(b"x-custom")),
            hash(&HeaderName::raw(b"x-custom"))
        );
    }

    #[test]
    fn display_uses_canonical_name() {
        assert_eq!(HeaderName::UserAgent.to_string(), "User-Agent");
        assert_eq!(HeaderName::from_bytes(b"X-Thing").to_string(), "X-Thing");
    }
}
