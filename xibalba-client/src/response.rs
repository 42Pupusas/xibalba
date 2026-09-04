use std::io::Read;

use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::header::Header;
use xibalba_proto::response::{BodyFraming, HeaderRange, MAX_HEADERS, ResponseHead};
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

use crate::config::HEAD_BUF_SIZE;
use crate::silence::SilenceBudget;

/// The parsed response head: status line plus headers, kept verbatim so
/// header ranges stay valid.
#[derive(Debug)]
pub struct HeadData {
    pub version: Version,
    pub status: StatusCode,
    /// Exactly the status line and headers, excluding any body bytes read in
    /// the same socket operation. A `Vec` keeps header ranges valid for every
    /// configured head size instead of silently truncating them at one read.
    pub head_buf: Vec<u8>,
    pub ranges: [HeaderRange; MAX_HEADERS],
    pub header_count: usize,
}

impl HeadData {
    pub fn headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let buf = &self.head_buf;
        self.ranges[..self.header_count]
            .iter()
            .filter_map(move |r| {
                let ns = r.name_start as usize;
                let vs = r.value_start as usize;
                Some((
                    buf.get(ns..ns + r.name_len as usize)?,
                    buf.get(vs..vs + r.value_len as usize)?,
                ))
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
        request_method_is_head: bool,
    ) -> Result<(Self, BodyFraming, usize), Error> {
        head_acc.clear();
        let mut raw = [0u8; HEAD_BUF_SIZE];
        let mut budget = SilenceBudget::new(silence);
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
            // copy. `HeaderRange` offsets are `u16`, so the 64 KiB default is the
            // largest useful standard limit; a custom larger head fails safely if an
            // offset cannot be represented.
            let ranges =
                HeaderRange::build_ranges(&hdr_buf[..head.header_count], &head_acc[..head_end])?;
            let framing = BodyFraming::from_response(
                head.status,
                request_method_is_head,
                &hdr_buf[..head.header_count],
                head.header_count,
            )?;
            debug_assert_eq!(consumed, head_end);
            break (head, ranges, head_end, framing);
        };

        let head_buf = head_acc[..head_end].to_vec();
        let head_data = Self {
            version: head.version,
            status: head.status,
            head_buf,
            ranges,
            header_count: head.header_count,
        };

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

            let n = budget.read(stream, raw)?;
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
