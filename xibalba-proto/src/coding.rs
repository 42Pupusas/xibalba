use crate::bytes::ByteSliceExt;
use crate::error::ParseError;
use crate::header::{Header, HeaderName};

/// What a `Transfer-Encoding` list asks the recipient to do with the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferCoding {
    /// No `Transfer-Encoding` header was present.
    Absent,
    /// The body is chunked, and nothing remains encoded once dechunked.
    Chunked,
    /// A header was present but applies no encoding (`identity`, or an empty
    /// list). The body is not self-delimiting.
    Identity,
}

/// Parses the `Transfer-Encoding` codings of a response as one ordered list.
///
/// RFC 9112 §6.1 makes this a sequence, not a set: `chunked` frames the body
/// only as the final coding, may not be applied twice, and any other coding
/// leaves bytes encoded that this client cannot decode. Reading only the last
/// coding — as the previous implementation did — accepts `gzip, chunked` and
/// hands the caller dechunked but still-compressed bytes as the body.
pub struct TransferCodings;

impl TransferCodings {
    /// Interpret every `Transfer-Encoding` header in order.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::InvalidTransferEncoding`] when `chunked` repeats
    /// or a coding is empty, and [`ParseError::UnsupportedTransferCoding`]
    /// when a coding other than `chunked` or `identity` is present.
    pub fn parse(headers: &[Header<'_>]) -> Result<TransferCoding, ParseError> {
        let mut present = false;
        let mut codings: Vec<&[u8]> = Vec::new();

        for header in headers {
            if header.name != HeaderName::TransferEncoding {
                continue;
            }
            present = true;
            for coding in header.value.split(|&b| b == b',') {
                let coding = coding.trim_ows();
                if !coding.is_empty() {
                    codings.push(coding);
                }
            }
        }

        if !present {
            return Ok(TransferCoding::Absent);
        }
        if codings.is_empty() {
            return Ok(TransferCoding::Identity);
        }

        let chunked_count = codings
            .iter()
            .filter(|c| c.ascii_eq_ignore_case(b"chunked"))
            .count();
        if chunked_count > 1 {
            return Err(ParseError::InvalidTransferEncoding);
        }

        // `chunked` delimits the body, so it must be the coding applied last;
        // anything after it would have to be decoded before the framing could
        // be read, which is impossible.
        let chunked_is_final = codings
            .last()
            .is_some_and(|c| c.ascii_eq_ignore_case(b"chunked"));
        if chunked_count == 1 && !chunked_is_final {
            return Err(ParseError::InvalidTransferEncoding);
        }

        // Every remaining coding must be one this client can undo. `identity`
        // applies nothing; anything else would leave the body encoded.
        let leading = if chunked_is_final {
            &codings[..codings.len() - 1]
        } else {
            &codings[..]
        };
        if leading.iter().any(|c| !c.ascii_eq_ignore_case(b"identity")) {
            return Err(ParseError::UnsupportedTransferCoding);
        }

        if chunked_is_final {
            Ok(TransferCoding::Chunked)
        } else {
            Ok(TransferCoding::Identity)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codings(values: &[&[u8]]) -> Result<TransferCoding, ParseError> {
        let headers: Vec<Header<'_>> = values
            .iter()
            .map(|v| Header {
                name: HeaderName::TransferEncoding,
                value: v,
            })
            .collect();
        TransferCodings::parse(&headers)
    }

    #[test]
    fn absent_header_is_absent() {
        assert_eq!(TransferCodings::parse(&[]).unwrap(), TransferCoding::Absent);
    }

    #[test]
    fn chunked_alone_frames_the_body() {
        assert_eq!(codings(&[b"chunked"]).unwrap(), TransferCoding::Chunked);
        assert_eq!(codings(&[b"Chunked"]).unwrap(), TransferCoding::Chunked);
    }

    #[test]
    fn identity_applies_no_encoding() {
        assert_eq!(codings(&[b"identity"]).unwrap(), TransferCoding::Identity);
        assert_eq!(
            codings(&[b"identity, chunked"]).unwrap(),
            TransferCoding::Chunked
        );
    }

    #[test]
    fn an_empty_list_is_identity() {
        assert_eq!(codings(&[b""]).unwrap(), TransferCoding::Identity);
    }

    #[test]
    fn unsupported_codings_are_rejected() {
        assert_eq!(
            codings(&[b"gzip"]).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
        assert_eq!(
            codings(&[b"gzip, chunked"]).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
        assert_eq!(
            codings(&[b"gzip", b"chunked"]).unwrap_err(),
            ParseError::UnsupportedTransferCoding
        );
    }

    #[test]
    fn chunked_must_be_the_final_coding() {
        assert_eq!(
            codings(&[b"chunked, gzip"]).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn chunked_may_not_repeat() {
        assert_eq!(
            codings(&[b"chunked, chunked"]).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
        assert_eq!(
            codings(&[b"chunked", b"chunked"]).unwrap_err(),
            ParseError::InvalidTransferEncoding
        );
    }

    #[test]
    fn codings_split_across_headers_form_one_list() {
        assert_eq!(
            codings(&[b"identity", b"chunked"]).unwrap(),
            TransferCoding::Chunked
        );
    }
}
