//! Whether a connection survives the response it just carried.

use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

/// Which headers in the response head declared where the body ends.
///
/// Kept apart from the connection tokens because the two answer different
/// questions: these decide whether the *message* boundary is agreed, the
/// tokens whether the peer intends to stay.
#[derive(Debug, Default, Clone, Copy)]
struct FramingHeaders {
    transfer_encoding: bool,
    content_length: bool,
}

impl FramingHeaders {
    /// Whether the sender described where this message ends in two ways that
    /// a recipient could read differently.
    ///
    /// Both cases come from RFC 9112. Transfer-Encoding beside Content-Length
    /// (§6.3) is resolved in favour of the coding for *this* client, but an
    /// intermediary that chose the length has left the remainder of the body
    /// on the wire, where the next response would be read from. An HTTP/1.0
    /// message carrying any transfer coding (§6.1) is faulty framing outright,
    /// Content-Length or not, since the sender may hold buffered bytes that
    /// further use of the connection would misread.
    const fn is_ambiguous(self, version: Version) -> bool {
        let both_framings = self.transfer_encoding && self.content_length;
        let coding_on_http10 = self.transfer_encoding && matches!(version, Version::Http10);
        both_framings || coding_on_http10
    }
}

/// What the response head said about the connection and its framing.
///
/// One pass over the headers, because the iterator yields borrowed slices and
/// walking it twice would mean either collecting or re-parsing.
#[derive(Debug, Default, Clone, Copy)]
struct HeadSurvey {
    announced_close: bool,
    announced_keep_alive: bool,
    framing: FramingHeaders,
}

impl HeadSurvey {
    fn of<'a>(headers: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Self {
        let mut survey = Self::default();
        for (name, value) in headers {
            if name.ascii_eq_ignore_case(b"Connection") {
                survey.announced_close |= value.contains_token_ignore_case(b"close");
                survey.announced_keep_alive |= value.contains_token_ignore_case(b"keep-alive");
            } else if name.ascii_eq_ignore_case(b"Transfer-Encoding") {
                survey.framing.transfer_encoding = true;
            } else if name.ascii_eq_ignore_case(b"Content-Length") {
                survey.framing.content_length = true;
            }
        }
        survey
    }
}

/// Whether the connection may carry another request once the current
/// response has been fully consumed.
///
/// Draining a body is necessary for reuse but not sufficient: the peer can
/// announce a close, HTTP/1.0 defaults to closing, a protocol switch takes the
/// socket out of HTTP entirely, and framing the message two ways leaves no
/// agreed byte for the next one to start at. Every client path derives this
/// from the response head so the rules live in one place instead of being
/// re-decided per call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionReuse {
    /// Reusable once the body has been fully read.
    Keep,
    /// Must not carry another request.
    Close,
}

impl ConnectionReuse {
    pub(crate) fn evaluate<'a>(
        version: Version,
        status: StatusCode,
        headers: impl Iterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Self {
        // 101 hands the socket to another protocol; whatever follows is not
        // an HTTP response, so the connection can never be reused here.
        if status == StatusCode::SWITCHING_PROTOCOLS {
            return Self::Close;
        }

        let survey = HeadSurvey::of(headers);

        if survey.announced_close || survey.framing.is_ambiguous(version) {
            return Self::Close;
        }

        match version {
            Version::Http11 => Self::Keep,
            Version::Http10 => {
                if survey.announced_keep_alive {
                    Self::Keep
                } else {
                    Self::Close
                }
            }
        }
    }

    pub(crate) const fn is_keep(self) -> bool {
        matches!(self, Self::Keep)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evaluate(
        version: Version,
        status: StatusCode,
        headers: &[(&[u8], &[u8])],
    ) -> ConnectionReuse {
        ConnectionReuse::evaluate(version, status, headers.iter().copied())
    }

    #[test]
    fn http11_without_connection_header_is_reusable() {
        assert_eq!(
            evaluate(Version::Http11, StatusCode::OK, &[]),
            ConnectionReuse::Keep
        );
    }

    #[test]
    fn connection_close_ends_the_connection() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[(b"Connection", b"close")]
            ),
            ConnectionReuse::Close
        );
    }

    #[test]
    fn connection_close_is_recognised_in_a_token_list() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[(b"connection", b"keep-alive, Close")]
            ),
            ConnectionReuse::Close
        );
    }

    #[test]
    fn close_is_not_matched_as_a_substring() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[(b"Connection", b"close-enough")]
            ),
            ConnectionReuse::Keep
        );
    }

    #[test]
    fn http10_closes_unless_keep_alive_is_requested() {
        assert_eq!(
            evaluate(Version::Http10, StatusCode::OK, &[]),
            ConnectionReuse::Close
        );
        assert_eq!(
            evaluate(
                Version::Http10,
                StatusCode::OK,
                &[(b"Connection", b"Keep-Alive")]
            ),
            ConnectionReuse::Keep
        );
    }

    /// RFC 9112 §6.3: the coding wins the framing, but the disagreement is
    /// what ends the connection — an intermediary that read the length instead
    /// left the rest of the body where the next response would begin.
    #[test]
    fn framing_a_response_two_ways_ends_the_connection() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[
                    (b"Transfer-Encoding", b"chunked"),
                    (b"Content-Length", b"3")
                ]
            ),
            ConnectionReuse::Close
        );
    }

    /// Order is not part of the rule, and neither is casing.
    #[test]
    fn the_two_framings_are_recognised_in_either_order() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[
                    (b"content-length", b"3"),
                    (b"transfer-encoding", b"chunked")
                ]
            ),
            ConnectionReuse::Close
        );
    }

    /// RFC 9112 §6.1: an HTTP/1.0 message carrying a transfer coding is faulty
    /// framing on its own, with no Content-Length needed to make it ambiguous,
    /// and keep-alive does not redeem it.
    #[test]
    fn a_transfer_coding_on_http10_ends_the_connection_despite_keep_alive() {
        assert_eq!(
            evaluate(
                Version::Http10,
                StatusCode::OK,
                &[
                    (b"Transfer-Encoding", b"chunked"),
                    (b"Connection", b"keep-alive")
                ]
            ),
            ConnectionReuse::Close
        );
    }

    /// The boundary the rule turns on: each framing alone is unambiguous, so
    /// neither may cost the connection on its own.
    #[test]
    fn either_framing_alone_keeps_the_connection() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[(b"Transfer-Encoding", b"chunked")]
            ),
            ConnectionReuse::Keep
        );
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::OK,
                &[(b"Content-Length", b"3")]
            ),
            ConnectionReuse::Keep
        );
    }

    #[test]
    fn switching_protocols_ends_the_connection() {
        assert_eq!(
            evaluate(
                Version::Http11,
                StatusCode::SWITCHING_PROTOCOLS,
                &[(b"Upgrade", b"websocket")]
            ),
            ConnectionReuse::Close
        );
    }
}
