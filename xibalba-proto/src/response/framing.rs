use crate::bytes::ByteSliceExt;
use crate::coding::{TransferCoding, TransferCodings};
use crate::error::ParseError;
use crate::header::{Header, HeaderName};
use crate::method::Method;
use crate::status::StatusCode;

/// How the response body is framed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyFraming {
    /// Body length is given by `Content-Length`.
    ContentLength(u64),
    /// Body uses chunked `Transfer-Encoding`.
    Chunked,
    /// Body ends when the connection closes.
    UntilClose,
    /// No body (1xx, 204, 304, or HEAD response).
    None,
}

impl BodyFraming {
    /// Determine body framing from the request method, response status, and
    /// headers.
    ///
    /// The method is part of framing, not merely context: RFC 9110 §6.4.2
    /// makes a HEAD response bodiless whatever its headers say, and §9.3.6
    /// does the same for a 2xx answer to CONNECT, where the bytes after the
    /// head belong to the tunnel rather than to HTTP. Both cases carry
    /// `Content-Length` or `Transfer-Encoding` values that describe a message
    /// that was never sent, and §9.3.6 requires a client to ignore them.
    ///
    /// Any `Transfer-Encoding` overrides `Content-Length` (RFC 9112
    /// §6.3): when `chunked` is the final coding the body is chunked;
    /// when it is absent or not final the body runs until close, since
    /// a `Content-Length` next to a transfer coding is a smuggling
    /// vector. Multiple `Content-Length` headers are accepted only when
    /// every value parses to the same number.
    ///
    /// `headers` must be exactly the parsed headers. The count is the
    /// slice's own length rather than a separate argument: a redundant count
    /// can disagree with the slice, and this function panicked on an
    /// oversized one despite returning `Result`. Callers holding a larger
    /// buffer pass `&buf[..head.header_count]`.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::InvalidContentLength`] when any
    /// `Content-Length` value is not valid digits or two of them
    /// disagree.
    pub fn from_response(
        status: StatusCode,
        request_method: Method,
        headers: &[Header<'_>],
    ) -> Result<Self, ParseError> {
        if status.is_informational()
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED
            || !request_method.response_can_have_content(status)
        {
            return Ok(Self::None);
        }

        match TransferCodings::parse(headers)? {
            TransferCoding::Chunked => return Ok(Self::Chunked),
            // A header applying no encoding still takes precedence over
            // Content-Length, and leaves the body delimited by the close.
            TransferCoding::Identity => return Ok(Self::UntilClose),
            TransferCoding::Absent => {}
        }

        let mut content_length: Option<u64> = None;
        for h in headers {
            if h.name == HeaderName::ContentLength {
                let len = h
                    .value
                    .parse_u64()
                    .ok_or(ParseError::InvalidContentLength)?;
                if content_length.is_some_and(|prev| prev != len) {
                    return Err(ParseError::InvalidContentLength);
                }
                content_length = Some(len);
            }
        }

        if let Some(len) = content_length {
            return Ok(Self::ContentLength(len));
        }

        Ok(Self::UntilClose)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ParseError;

    #[test]
    fn body_framing_content_length() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"42",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(42)
        );
    }

    #[test]
    fn body_framing_chunked() {
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::Chunked
        );
    }

    #[test]
    fn body_framing_none_for_204() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::NO_CONTENT, Method::Get, &headers).unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_none_for_head() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"42",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Head, &headers).unwrap(),
            BodyFraming::None
        );
    }

    /// RFC 9110 §9.3.6: a client "MUST ignore any Content-Length or
    /// Transfer-Encoding header fields received in a successful response to
    /// CONNECT". Framing them as a body would hand the caller the tunnel's
    /// first bytes as content.
    #[test]
    fn body_framing_none_for_a_successful_connect() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"4096",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Connect, &headers).unwrap(),
            BodyFraming::None
        );
    }

    /// The transfer coding is ignored on a successful CONNECT for the same
    /// reason as the length, and is worth pinning separately: chunked framing
    /// takes precedence everywhere else in this function.
    #[test]
    fn a_transfer_coding_does_not_frame_a_successful_connect() {
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Connect, &headers).unwrap(),
            BodyFraming::None
        );
    }

    /// The boundary of the rule. §9.3.6: "Any response other than a successful
    /// response indicates that the tunnel has not yet been formed", so a
    /// refusal is an ordinary response and its content is framed normally —
    /// otherwise a proxy's error page would be unreadable.
    #[test]
    fn a_refused_connect_is_framed_like_any_other_response() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"6",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::FORBIDDEN, Method::Connect, &headers).unwrap(),
            BodyFraming::ContentLength(6)
        );
    }

    #[test]
    fn body_framing_until_close() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn body_framing_chunked_takes_precedence() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::Chunked
        );
    }

    // ── Adversarial body framing ──────────────────────────────────────

    #[test]
    fn body_framing_304_no_body() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"1000",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::NOT_MODIFIED, Method::Get, &headers).unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn body_framing_1xx_no_body() {
        let headers: [Header<'_>; 0] = [];
        assert_eq!(
            BodyFraming::from_response(StatusCode::CONTINUE, Method::Get, &headers).unwrap(),
            BodyFraming::None
        );
    }

    #[test]
    fn content_length_zero() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"0",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(0)
        );
    }

    #[test]
    fn content_length_with_ows() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b" 42 ",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(42)
        );
    }

    #[test]
    fn content_length_non_numeric_rejected() {
        let headers = [Header {
            name: HeaderName::ContentLength,
            value: b"abc",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidContentLength
        );
    }

    #[test]
    fn conflicting_content_length_rejected() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"6",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidContentLength
        );
    }

    #[test]
    fn duplicate_identical_content_length_accepted() {
        let headers = [
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::ContentLength(5)
        );
    }

    #[test]
    fn transfer_encoding_not_chunked() {
        // gzip is a coding this client cannot undo. Framing the body as
        // UntilClose would present compressed bytes as the response body.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn transfer_encoding_overrides_content_length() {
        // Transfer-Encoding still takes precedence over Content-Length: the
        // unsupported coding decides the outcome, not the length header.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"gzip",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );

        // identity applies no encoding, so the precedence is still visible
        // without an unsupported coding masking it.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"identity",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn chunked_before_another_coding_is_rejected() {
        // chunked delimits the body, so a coding applied after it would have
        // to be decoded before the framing could be read.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked, gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn chunked_final_across_multiple_transfer_encoding_headers() {
        // The codings of every header form one list in order, so a trailing
        // chunked frames the body even when split across headers.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"identity",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"Chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::Chunked
        );

        // The same list with an undecodable coding underneath is refused.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"gzip",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"Chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn empty_transfer_encoding_is_until_close() {
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"5",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn unsupported_transfer_coding_is_rejected() {
        // gzip is a transfer coding this client cannot decode. Framing the
        // body as UntilClose hands the caller compressed bytes as though they
        // were the response body.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn chunked_over_an_unsupported_coding_is_rejected() {
        // "gzip, chunked" was dechunked and returned as the body, still gzip
        // transfer-coded, with no indication anything was left encoded.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"gzip, chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn repeated_chunked_is_rejected() {
        // RFC 9112 6.1: chunked must not be applied more than once. Accepting
        // it is a framing difference an intermediary may not share.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"chunked, chunked",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn repeated_chunked_across_headers_is_rejected() {
        // The codings of every Transfer-Encoding header form one list, so
        // splitting the repeat across two headers must not evade the check.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn identity_coding_is_accepted_as_unframed() {
        // "identity" applies no encoding, so it neither frames the body nor
        // leaves it encoded.
        let headers = [Header {
            name: HeaderName::TransferEncoding,
            value: b"identity",
        }];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap(),
            BodyFraming::UntilClose
        );
    }

    #[test]
    fn duplicate_transfer_encoding_both_chunked() {
        // Previously accepted as Chunked. RFC 9112 6.1 forbids applying
        // chunked more than once, and accepting it is a framing difference an
        // intermediary may not share.
        let headers = [
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
            Header {
                name: HeaderName::TransferEncoding,
                value: b"chunked",
            },
        ];
        assert_eq!(
            BodyFraming::from_response(StatusCode::OK, Method::Get, &headers).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }
}
