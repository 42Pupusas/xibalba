use crate::bytes::ByteSliceExt;
use crate::error::{ConnectionError, Error, ParseError};
use crate::header::Header;

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
    use crate::header::HeaderName;
    use crate::response::head::ResponseHead;
    use crate::status::StatusCode;
    use crate::version::Version;

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
