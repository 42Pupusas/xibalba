use crate::bytes::ByteSliceExt;
use crate::error::{Error, ParseError};
use crate::header::{Header, HeaderName, Tchar};
use crate::status::StatusCode;
use crate::version::Version;

/// Result of parsing the response head (status line + headers).
#[derive(Debug)]
pub struct ResponseHead<'a> {
    pub version: Version,
    pub status: StatusCode,
    pub reason: &'a [u8],
    /// Number of headers parsed into the caller's buffer.
    pub header_count: usize,
}

impl<'a> ResponseHead<'a> {
    /// The headers this head parsed, taken from the buffer that was passed
    /// to [`Self::parse`].
    ///
    /// Pairing the count with its own buffer is what makes an inconsistent
    /// pair possible; this narrows the buffer once, at the point where the
    /// count is known to describe it.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::TooManyHeaders`] if `headers` is shorter than
    /// the count, which means it is not the buffer that was parsed into.
    pub fn headers<'h>(&self, headers: &'h [Header<'a>]) -> Result<&'h [Header<'a>], ParseError> {
        headers
            .get(..self.header_count)
            .ok_or(ParseError::TooManyHeaders)
    }

    /// Parse the response head from `buf`.
    ///
    /// `headers` is a caller-provided buffer that will be filled with parsed headers.
    ///
    /// On success, returns `(ResponseHead, bytes_consumed)`.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::Incomplete` if the buffer doesn't contain a complete
    /// response head. Returns other `ParseError` variants for malformed input.
    pub fn parse(buf: &'a [u8], headers: &mut [Header<'a>]) -> Result<(Self, usize), Error> {
        let status_line_end = buf.find_crlf().ok_or(ParseError::Incomplete)?;
        let status_line = &buf[..status_line_end];

        if status_line.len() < 12 {
            return Err(ParseError::InvalidVersion.into());
        }

        let version = Version::try_from(&status_line[..8])?;

        if status_line[8] != b' ' {
            return Err(ParseError::InvalidVersion.into());
        }

        let status = StatusCode::try_from(&status_line[9..12])?;

        let reason = match status_line.get(12) {
            None => b"".as_slice(),
            Some(b' ') => &status_line[13..],
            Some(_) => return Err(ParseError::InvalidStatusCode.into()),
        };
        // reason-phrase = *( HTAB / SP / VCHAR / obs-text ). CR and LF cannot
        // appear (the line ended at CRLF), but NUL and the other C0 controls
        // otherwise reach the caller inside a field the grammar forbids them
        // from occupying.
        if reason
            .iter()
            .any(|&b| b != b'\t' && (b < 0x20 || b == 0x7f))
        {
            return Err(ParseError::InvalidReasonPhrase.into());
        }

        let mut pos = status_line_end + 2;
        let mut count = 0;

        loop {
            if pos >= buf.len() {
                return Err(ParseError::Incomplete.into());
            }

            if buf[pos] == b'\r' {
                // A lone trailing CR is the head terminator with its LF still
                // in flight, not a header line beginning with a control byte.
                if pos + 1 >= buf.len() {
                    return Err(ParseError::Incomplete.into());
                }
                if buf[pos + 1] == b'\n' {
                    pos += 2;
                    break;
                }
            }

            // Single left-to-right scan: validate name tchar-by-tchar, find ':', then
            // scan value bytes until \r\n — all in one pass with no restarts.
            let (name_bytes, value, line_end) = Self::parse_header_line(&buf[pos..])?;

            if count >= headers.len() {
                return Err(ParseError::TooManyHeaders.into());
            }
            headers[count] = Header {
                name: HeaderName::raw(name_bytes),
                value,
            };
            count += 1;

            pos += line_end;
        }

        Ok((
            Self {
                version,
                status,
                reason,
                header_count: count,
            },
            pos,
        ))
    }

    /// Single-pass header line parser.
    ///
    /// Scans `line` left-to-right once: validates name tchars, finds ':', then
    /// finds the terminating CRLF, trimming OWS from the value along the way.
    /// Returns `(name_bytes, value, bytes_consumed_including_crlf)`.
    fn parse_header_line(line: &[u8]) -> Result<(&[u8], &[u8], usize), Error> {
        if line.is_empty() {
            return Err(ParseError::InvalidHeaderName.into());
        }

        // Phase 1: scan name, validating tchars and stopping at ':'. Iterating
        // rather than indexing keeps one bounds check per byte instead of two.
        //
        // Validation must run in the same loop as the ':' search, not after it.
        // Searching for ':' alone would run past this line's CRLF and into
        // later headers when a name is malformed; stopping at the first
        // non-tchar keeps a bad line from consuming the rest of the buffer.
        let mut name_len = None;
        for (idx, &b) in line.iter().enumerate() {
            if b == b':' {
                name_len = Some(idx);
                break;
            }
            if !Tchar::is_valid(b) {
                return Err(ParseError::InvalidHeaderName.into());
            }
        }
        // Every byte so far was a valid tchar and the input ran out, so the
        // colon may still be on its way. Reporting a malformed line here
        // would make an incremental caller reject a response that is merely
        // still in flight; a terminated line without a colon fails above on
        // CR, which is not a tchar.
        let mut i = name_len.ok_or(ParseError::Incomplete)?;
        if i == 0 {
            return Err(ParseError::InvalidHeaderName.into());
        }
        let name_bytes = &line[..i];
        i += 1; // skip ':'

        // Phase 2: skip leading OWS
        while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        let value_start = i;

        // Phase 3: one vectorizable pass locates the CR *and* validates the
        // value. CR is itself a control byte, so the first byte matching the
        // control-character predicate is either the terminating CR or an
        // illegal byte — a separate validation pass would re-read the same
        // bytes to learn what this one already knows. Tab is legal inside a
        // value, and obs-text (>= 0x80) is accepted per RFC 9110.
        let ctl_pos = line[i..]
            .iter()
            .position(|&b| (b < 0x20 || b == 0x7f) && b != b'\t')
            .ok_or(ParseError::Incomplete)?;
        let crlf = i + ctl_pos;
        if line[crlf] != b'\r' {
            return Err(ParseError::InvalidHeaderValue.into());
        }
        if crlf + 1 >= line.len() || line[crlf + 1] != b'\n' {
            return Err(ParseError::Incomplete.into());
        }

        // Trailing OWS is rare, so test the last byte before walking backwards;
        // the scan is skipped entirely for the overwhelmingly common value.
        let raw_value = &line[value_start..crlf];
        let trimmed = match raw_value.last() {
            Some(&b' ' | &b'\t') => {
                let value_end = raw_value
                    .iter()
                    .rposition(|&b| b != b' ' && b != b'\t')
                    .map_or(0, |p| p + 1);
                &raw_value[..value_end]
            }
            #[allow(clippy::match_same_arms)]
            None | Some(_) => raw_value,
        };

        Ok((name_bytes, trimmed, crlf + 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn make_response(text: &[u8]) -> (ResponseHead<'_>, usize, Vec<Header<'_>>) {
        let mut headers = vec![Header::empty(); 32];
        let (head, consumed) = ResponseHead::parse(text, &mut headers).unwrap();
        headers.truncate(head.header_count);
        (head, consumed, headers)
    }

    #[test]
    fn parse_simple_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
        let (head, consumed, headers) = make_response(raw);
        assert_eq!(head.version, Version::Http11);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"OK");
        assert_eq!(head.header_count, 1);
        assert_eq!(consumed, raw.len());
        assert_eq!(headers[0].name, HeaderName::ContentLength);
        assert_eq!(headers[0].value, b"5");
    }

    #[test]
    fn parse_no_reason_phrase() {
        let raw = b"HTTP/1.1 200\r\n\r\n";
        let (head, _, _) = make_response(raw);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"");
    }

    #[test]
    fn parse_multiple_headers() {
        let raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 42\r\nServer: test\r\n\r\n";
        let (head, _, headers) = make_response(raw);
        assert_eq!(head.header_count, 3);
        assert_eq!(headers[0].name, HeaderName::ContentType);
        assert_eq!(headers[1].name, HeaderName::ContentLength);
        assert_eq!(headers[2].name, HeaderName::Server);
    }

    #[test]
    fn incomplete_returns_error() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
        let mut headers = [const { Header::empty() }; 32];
        let result = ResponseHead::parse(raw, &mut headers);
        assert_eq!(result.unwrap_err(), Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn every_prefix_of_a_valid_head_is_incomplete() {
        // A caller feeding bytes as they arrive uses Incomplete as the only
        // instruction to read more. Any other error on a truncated but
        // well-formed head makes it reject a response that is merely still
        // in flight.
        let raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 42\r\nServer: test\r\n\r\n";
        for end in 1..raw.len() {
            let mut headers = [const { Header::empty() }; 32];
            let err = ResponseHead::parse(&raw[..end], &mut headers)
                .expect_err("a truncated head cannot parse");
            assert_eq!(
                err,
                Error::Parse(ParseError::Incomplete),
                "prefix of length {end} reported {err:?}: {:?}",
                std::str::from_utf8(&raw[..end])
            );
        }
    }

    #[test]
    fn partial_header_name_is_incomplete_not_missing_colon() {
        // The specific shape behind the class above: every byte so far is a
        // valid token character, so the colon may still be coming.
        let mut headers = [const { Header::empty() }; 32];
        let err = ResponseHead::parse(b"HTTP/1.1 200 OK\r\nCont", &mut headers)
            .expect_err("a truncated header name cannot parse");
        assert_eq!(err, Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn complete_line_without_a_colon_is_still_rejected() {
        // The fix must not swallow genuinely malformed lines. CR is not a
        // tchar, so a terminated line with no colon fails on the name scan
        // before the end of input is ever reached.
        let mut headers = [const { Header::empty() }; 32];
        let err = ResponseHead::parse(b"HTTP/1.1 200 OK\r\nNoColonHere\r\n\r\n", &mut headers)
            .expect_err("a complete line without a colon is malformed");
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn too_many_headers_error() {
        let raw = b"HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\n\r\n";
        let mut headers = [Header::empty(); 1];
        let result = ResponseHead::parse(raw, &mut headers);
        assert_eq!(
            result.unwrap_err(),
            Error::Parse(ParseError::TooManyHeaders)
        );
    }

    // ── Adversarial response head parsing ───────────────────────────────────────

    #[test]
    fn garbage_status_line() {
        let raw = b"\x00\xff\xfe garbage\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert!(ResponseHead::parse(raw, &mut headers).is_err());
    }

    #[test]
    fn truncated_version() {
        let raw = b"HTTP/1\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidVersion));
    }

    #[test]
    fn status_code_followed_by_non_space_rejected() {
        let raw = b"HTTP/1.1 200X\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidStatusCode));
    }

    #[test]
    fn status_line_trailing_space_empty_reason() {
        let raw = b"HTTP/1.1 200 \r\n\r\n";
        let (head, _, _) = make_response(raw);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"");
    }

    #[test]
    fn status_line_no_space_after_version() {
        let raw = b"HTTP/1.1200 OK\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert!(ResponseHead::parse(raw, &mut headers).is_err());
    }

    #[test]
    fn header_with_nul_byte_in_name() {
        let raw = b"HTTP/1.1 200 OK\r\nBad\x00Name: val\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_missing_colon() {
        // Parser scans tchar-by-tchar; \r is not a tchar, so InvalidHeaderName
        // fires before MissingColon can be reached.
        let raw = b"HTTP/1.1 200 OK\r\nHeaderNoColon\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_empty_name() {
        let raw = b"HTTP/1.1 200 OK\r\n: value\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_name_with_space() {
        let raw = b"HTTP/1.1 200 OK\r\nBad Name: val\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_value_ows_trimmed() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Test:   spaced   \r\n\r\n";
        let (head, _, headers) = make_response(raw);
        assert_eq!(head.header_count, 1);
        assert_eq!(headers[0].value, b"spaced");
    }

    #[test]
    fn only_crlf_no_status_line() {
        let raw = b"\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert!(ResponseHead::parse(raw, &mut headers).is_err());
    }

    #[test]
    fn status_line_exactly_min_length() {
        // "HTTP/1.1 200" is exactly 12 bytes — the minimum.
        let raw = b"HTTP/1.1 200\r\n\r\n";
        let (head, _, _) = make_response(raw);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.reason, b"");
    }

    #[test]
    fn header_value_with_ctl_byte_rejected() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Bad: a\x00b\r\nContent-Length: 0\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderValue));
    }

    #[test]
    fn malformed_name_does_not_scan_past_its_own_line() {
        // The colon here belongs to a *later* header. A name scan that looked
        // for ':' without stopping at the first non-tchar would swallow the
        // CRLF and treat "bad\r\nX-Next" as one name.
        let raw = b"HTTP/1.1 200 OK\r\nbad\r\nX-Next: v\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderName));
    }

    #[test]
    fn header_value_with_bare_lf_rejected() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Bad: a\nb\r\nContent-Length: 0\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderValue));
    }

    #[test]
    fn header_value_with_del_byte_rejected() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Bad: a\x7fb\r\nContent-Length: 0\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::InvalidHeaderValue));
    }

    #[test]
    fn header_value_unterminated_is_incomplete_not_invalid() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: value";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn header_value_cr_without_lf_is_incomplete() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: value\r";
        let mut headers = [const { Header::empty() }; 4];
        let err = ResponseHead::parse(raw, &mut headers).unwrap_err();
        assert_eq!(err, Error::Parse(ParseError::Incomplete));
    }

    #[test]
    fn header_value_with_obs_text_accepted() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: caf\xc3\xa9\r\nContent-Length: 0\r\n\r\n";
        let (_, _, headers) = make_response(raw);
        assert_eq!(headers[0].value, b"caf\xc3\xa9");
    }

    #[test]
    fn header_value_with_tab_accepted() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Name: a\tb\r\nContent-Length: 0\r\n\r\n";
        let (_, _, headers) = make_response(raw);
        assert_eq!(headers[0].value, b"a\tb");
    }

    #[test]
    fn reason_phrase_with_nul_is_rejected() {
        // The status line ends at CRLF, so a reason phrase cannot contain
        // CR or LF -- but NUL and the other C0 controls reach the caller as
        // part of a field the RFC restricts to HTAB / SP / VCHAR / obs-text.
        let raw = b"HTTP/1.1 200 O\0K\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        assert_eq!(
            ResponseHead::parse(raw, &mut headers).unwrap_err(),
            Error::Parse(ParseError::InvalidReasonPhrase)
        );
    }

    #[test]
    fn reason_phrase_allows_tab_and_obs_text() {
        // The permitted set is wider than ASCII graphic characters; the
        // check must not reject phrases servers legitimately send.
        let raw = b"HTTP/1.1 200 OK\tdone\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let (head, _) = ResponseHead::parse(raw, &mut headers).expect("HTAB is permitted");
        assert_eq!(head.reason, b"OK\tdone");

        let raw = b"HTTP/1.1 200 caf\xc3\xa9\r\n\r\n";
        let mut headers = [const { Header::empty() }; 4];
        let (head, _) = ResponseHead::parse(raw, &mut headers).expect("obs-text is permitted");
        assert_eq!(head.reason, b"caf\xc3\xa9");
    }

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
}
