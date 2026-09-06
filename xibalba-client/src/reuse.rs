//! Whether a connection survives the response it just carried.

use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::status::StatusCode;
use xibalba_proto::version::Version;

/// Whether the connection may carry another request once the current
/// response has been fully consumed.
///
/// Draining a body is necessary for reuse but not sufficient: the peer can
/// announce a close, HTTP/1.0 defaults to closing, and a protocol switch
/// takes the socket out of HTTP entirely. Every client path derives this
/// from the response head so the three rules live in one place instead of
/// being re-decided per call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionReuse {
    /// Reusable once the body has been fully read.
    Keep,
    /// Must not carry another request.
    Close,
}

impl ConnectionReuse {
    pub fn evaluate<'a>(
        version: Version,
        status: StatusCode,
        headers: impl Iterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Self {
        // 101 hands the socket to another protocol; whatever follows is not
        // an HTTP response, so the connection can never be reused here.
        if status == StatusCode::SWITCHING_PROTOCOLS {
            return Self::Close;
        }

        let mut announced_close = false;
        let mut announced_keep_alive = false;
        for (name, value) in headers {
            if name.ascii_eq_ignore_case(b"Connection") {
                announced_close |= value.contains_token_ignore_case(b"close");
                announced_keep_alive |= value.contains_token_ignore_case(b"keep-alive");
            }
        }

        if announced_close {
            return Self::Close;
        }

        match version {
            Version::Http11 => Self::Keep,
            Version::Http10 => {
                if announced_keep_alive {
                    Self::Keep
                } else {
                    Self::Close
                }
            }
        }
    }

    pub const fn is_keep(self) -> bool {
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
