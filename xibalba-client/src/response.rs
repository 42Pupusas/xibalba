use std::io::Read;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::header::Header;
use xibalba_proto::method::Method;
use xibalba_proto::response::{BodyFraming, HeaderRange, MAX_HEADERS, ResponseHead};
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

use crate::config::HEAD_BUF_SIZE;
use crate::silence::{RequestDeadline, SilenceBudget};

/// The parsed response head: status line plus headers, kept verbatim so
/// header ranges stay valid.
///
/// The buffer, the ranges into it, and the count of live ranges are one
/// invariant, so they are private and set together by [`Self::new`]. They
/// were public and mutable, which let a caller raise the count past the
/// ranges it describes, or swap the buffer the ranges point into.
#[derive(Debug)]
pub struct HeadData {
    version: Version,
    status: StatusCode,
    /// Exactly the status line and headers, excluding any body bytes read in
    /// the same socket operation. A `Vec` keeps header ranges valid for every
    /// configured head size instead of silently truncating them at one read.
    head_buf: Vec<u8>,
    ranges: [HeaderRange; MAX_HEADERS],
    header_count: usize,
}

impl HeadData {
    /// Build a head from a buffer and the ranges describing it.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionError::HeaderRangeOverflow`] if `header_count`
    /// exceeds [`MAX_HEADERS`], and [`ConnectionError::HeaderNotInBuffer`] if
    /// any live range falls outside `head_buf`. Both mean the parts do not
    /// describe one another, which used to surface as a header silently
    /// missing from iteration.
    pub fn new(
        version: Version,
        status: StatusCode,
        head_buf: Vec<u8>,
        ranges: [HeaderRange; MAX_HEADERS],
        header_count: usize,
    ) -> Result<Self, Error> {
        if header_count > MAX_HEADERS {
            return Err(ConnectionError::HeaderRangeOverflow.into());
        }
        for range in &ranges[..header_count] {
            if !Self::range_within(range, head_buf.len()) {
                return Err(ConnectionError::HeaderNotInBuffer.into());
            }
        }
        Ok(Self {
            version,
            status,
            head_buf,
            ranges,
            header_count,
        })
    }

    /// A live range must land inside the buffer and name a non-empty header.
    ///
    /// The emptiness check is what catches a count raised past the ranges
    /// that describe it: the unused tail is all-zero, and a zero range is
    /// trivially in bounds. `field-name = 1*tchar` (RFC 9110 §5.1), so an
    /// empty name cannot come from a parsed header.
    fn range_within(range: &HeaderRange, len: usize) -> bool {
        let ends_within = |start: u32, span: u32| {
            (start as usize)
                .checked_add(span as usize)
                .is_some_and(|end| end <= len)
        };
        range.name_len > 0
            && ends_within(range.name_start, range.name_len)
            && ends_within(range.value_start, range.value_len)
    }

    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }

    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// The status line and headers as received.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.head_buf
    }

    #[must_use]
    pub const fn header_count(&self) -> usize {
        self.header_count
    }

    /// Whether the connection that carried this response may serve another
    /// request once the body has been consumed.
    pub(crate) fn connection_reuse(&self) -> crate::reuse::ConnectionReuse {
        crate::reuse::ConnectionReuse::evaluate(self.version, self.status, self.headers())
    }

    /// Every header, in the order received.
    ///
    /// [`Self::new`] rejects ranges that fall outside the buffer, so this
    /// yields exactly `header_count` headers rather than quietly skipping the
    /// ones that do not resolve.
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let buf = &self.head_buf;
        self.ranges[..self.header_count].iter().map(move |r| {
            let ns = r.name_start as usize;
            let vs = r.value_start as usize;
            (
                &buf[ns..ns + r.name_len as usize],
                &buf[vs..vs + r.value_len as usize],
            )
        })
    }

    /// Read one response head (skipping 1xx interim responses) and derive
    /// the body framing from it.
    ///
    /// Existed as the free function `read_response_head`; reading a head
    /// produces a `HeadData`, so the loop belongs to the type. The
    /// scratch buffer is borrowed by the framing derivation and handed
    /// back with the framing, leaving the body tail in `head_acc`.
    pub(crate) fn read_response<S: Read>(
        stream: &mut S,
        head_acc: &mut Vec<u8>,
        max_head: usize,
        silence: std::time::Duration,
        deadline: RequestDeadline,
        request_method: Method,
    ) -> Result<(Self, BodyFraming, usize), Error> {
        head_acc.clear();
        let mut raw = [0u8; HEAD_BUF_SIZE];
        let mut budget = SilenceBudget::with_deadline(silence, deadline);
        let mut interim_seen = 0usize;

        let (head, ranges, head_end, framing) = loop {
            let head_end =
                Self::read_until_head_end(stream, head_acc, &mut raw, &mut budget, max_head)?;

            let mut hdr_buf = [const { Header::empty() }; MAX_HEADERS];
            let (head, consumed) = ResponseHead::parse(&head_acc[..head_end], &mut hdr_buf)?;

            // 1xx interim responses precede the real response on the same
            // connection; drop the interim head and keep reading. 101 is the
            // exception — it hands the connection over to another protocol, so
            // it is surfaced as a final response.
            if head.status.is_informational() && head.status != StatusCode::SWITCHING_PROTOCOLS {
                interim_seen += 1;
                if interim_seen > MAX_INTERIM_RESPONSES {
                    return Err(ConnectionError::TooManyInterimResponses.into());
                }
                head_acc.drain(..head_end);
                continue;
            }

            // Derive ranges while headers still borrow `head_acc`, then preserve the
            // complete head for the response. This keeps duplicate header names or
            // values positional rather than re-searching their byte patterns in a
            // copy.
            let ranges =
                HeaderRange::build_ranges(&hdr_buf[..head.header_count], &head_acc[..head_end])?;
            let framing =
                BodyFraming::from_response(head.status, request_method, head.headers(&hdr_buf)?)?;
            debug_assert_eq!(consumed, head_end);
            break (head, ranges, head_end, framing);
        };

        let head_data = Self::new(
            head.version,
            head.status,
            head_acc[..head_end].to_vec(),
            ranges,
            head.header_count,
        )?;

        Ok((head_data, framing, head_end))
    }

    /// Grow `head_acc` from `stream` until it holds a complete head, and
    /// return the offset just past the terminating CRLFCRLF. Bytes already
    /// in `head_acc` (a second head that arrived in the same read as a
    /// skipped 1xx) are scanned before the socket is touched, so a head
    /// that is already buffered never waits on another read.
    ///
    /// A socket read can contain both the final header bytes and the body
    /// prefix. The delimiter is found before applying the limit so a valid
    /// `max_head`-sized head is not rejected merely because its first body
    /// bytes arrived in the same read.
    fn read_until_head_end<S: Read>(
        stream: &mut S,
        head_acc: &mut Vec<u8>,
        raw: &mut [u8; HEAD_BUF_SIZE],
        budget: &mut SilenceBudget,
        max_head: usize,
    ) -> Result<usize, Error> {
        let mut scanned = 0usize;
        loop {
            let scan_from = scanned.saturating_sub(3);
            if let Some(pos) = head_acc[scan_from..]
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
            {
                let head_end = scan_from + pos + 4;
                if head_end > max_head {
                    return Err(ConnectionError::HeadTooLarge.into());
                }
                return Ok(head_end);
            }
            if head_acc.len() > max_head {
                return Err(ConnectionError::HeadTooLarge.into());
            }
            scanned = head_acc.len();

            let n = budget.read_proto(stream, raw)?;
            if n == 0 {
                return Err(ConnectionError::ConnectionClosed.into());
            }
            head_acc.extend_from_slice(&raw[..n]);
        }
    }
}

/// Upper bound on 1xx heads skipped before one final response. Servers
/// send at most a handful (100, 102, 103); an unbounded run is a peer
/// keeping the client reading forever without ever answering.
const MAX_INTERIM_RESPONSES: usize = 8;

/// A fully-buffered response: body already read off the wire.
#[derive(Debug)]
pub struct Response {
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: crate::body::BodyReader,
}

impl Response {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }

    /// # Errors
    ///
    /// Returns `Error::Io` on read failure, or `Error::Connection` if the
    /// body is not valid UTF-8.
    pub fn text(mut self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.body.read_to_end(&mut buf).map_err(Error::from)?;
        String::from_utf8(buf).map_err(|_| Error::from(ConnectionError::InvalidUtf8Body))
    }
}

/// A response whose body is decoded incrementally from the live
/// connection. Produced by [`crate::client::Client::send_streaming`].
///
/// Borrows the client mutably until dropped. Reading the body to
/// completion leaves the connection reusable; dropping early marks it
/// dirty so the next request reconnects.
#[derive(Debug)]
pub struct StreamingResponse<'a, S: Read> {
    pub version: Version,
    pub status: StatusCode,
    pub head: HeadData,
    pub body: crate::body::StreamingBody<'a, S>,
}

impl<S: Read> StreamingResponse<'_, S> {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.head.headers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Head;

    impl Head {
        const BUF: &'static [u8] = b"HTTP/1.1 200 OK\r\nA: b\r\n\r\n";

        fn ranges(count: usize) -> [HeaderRange; MAX_HEADERS] {
            let mut ranges = [HeaderRange::default(); MAX_HEADERS];
            for range in ranges.iter_mut().take(count) {
                *range = HeaderRange {
                    name_start: 17,
                    name_len: 1,
                    value_start: 20,
                    value_len: 1,
                };
            }
            ranges
        }

        fn build(ranges: &[HeaderRange; MAX_HEADERS], count: usize) -> Result<HeadData, Error> {
            HeadData::new(
                Version::Http11,
                StatusCode::OK,
                Self::BUF.to_vec(),
                *ranges,
                count,
            )
        }
    }

    #[test]
    fn a_consistent_head_yields_its_headers() {
        let head = Head::build(&Head::ranges(1), 1).expect("the range is inside the buffer");
        let headers: Vec<_> = head.headers().collect();
        assert_eq!(headers, vec![(b"A".as_slice(), b"b".as_slice())]);
        assert_eq!(head.header_count(), 1);
        assert_eq!(head.as_bytes(), Head::BUF);
    }

    /// A count past the ranges that describe it used to be constructible,
    /// and iteration then read default (zero) ranges as real headers.
    #[test]
    fn a_count_beyond_the_live_ranges_is_rejected() {
        let error = Head::build(&Head::ranges(1), 2).expect_err("range 1 is a zero default");
        assert_eq!(error, Error::Connection(ConnectionError::HeaderNotInBuffer));
    }

    #[test]
    fn a_count_past_the_range_array_is_rejected() {
        let error = Head::build(&Head::ranges(1), MAX_HEADERS + 1)
            .expect_err("the count cannot exceed the array");
        assert_eq!(
            error,
            Error::Connection(ConnectionError::HeaderRangeOverflow)
        );
    }

    /// Previously a range pointing outside the buffer was skipped by
    /// `filter_map`, so a corrupt head silently lost a header instead of
    /// reporting that its parts disagreed.
    #[test]
    fn a_range_reaching_past_the_buffer_is_rejected_not_skipped() {
        let mut ranges = Head::ranges(1);
        ranges[0].value_len = 999;
        let error = Head::build(&ranges, 1).expect_err("the value runs past the buffer");
        assert_eq!(error, Error::Connection(ConnectionError::HeaderNotInBuffer));
    }

    #[test]
    fn an_offset_that_would_overflow_on_addition_is_rejected() {
        let mut ranges = Head::ranges(1);
        ranges[0].name_start = u32::MAX;
        ranges[0].name_len = u32::MAX;
        let error = Head::build(&ranges, 1).expect_err("start + len must not wrap");
        assert_eq!(error, Error::Connection(ConnectionError::HeaderNotInBuffer));
    }

    #[test]
    fn a_zero_header_head_is_valid() {
        let head = Head::build(&Head::ranges(0), 0).expect("no headers is consistent");
        assert_eq!(head.headers().count(), 0);
    }
}
