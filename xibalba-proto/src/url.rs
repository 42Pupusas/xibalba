use crate::error::{Error, UrlError};
use crate::scheme::Scheme;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Url<'a> {
    pub scheme: Scheme,
    pub host: &'a [u8],
    pub port: Option<u16>,
    pub path: &'a [u8],
    pub query: Option<&'a [u8]>,
    pub fragment: Option<&'a [u8]>,
}

impl<'a> Url<'a> {
    /// Parse a URL from bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Url` variants for malformed input.
    pub fn parse(input: &'a [u8]) -> Result<Self, Error> {
        if input.is_empty() {
            return Err(UrlError::Empty.into());
        }

        let sep_pos = find_subsequence(input, b"://").ok_or(UrlError::InvalidScheme)?;
        let scheme = Scheme::try_from(&input[..sep_pos])?;
        let rest = &input[sep_pos + 3..];

        let authority_end = rest
            .iter()
            .position(|&b| b == b'/' || b == b'?' || b == b'#')
            .unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        let after_authority = &rest[authority_end..];

        let (host, port) = parse_authority(authority)?;
        if host.is_empty() {
            return Err(UrlError::MissingHost.into());
        }

        let (path, query, fragment) = parse_path_query_fragment(after_authority);

        Ok(Self {
            scheme,
            host,
            port,
            path,
            query,
            fragment,
        })
    }

    /// The effective port (explicit or scheme default).
    #[must_use]
    pub fn effective_port(&self) -> u16 {
        self.port.unwrap_or_else(|| self.scheme.default_port())
    }

    /// The path to use in the request line. Returns `/` if path is empty.
    #[must_use]
    pub const fn request_path(&self) -> &[u8] {
        if self.path.is_empty() {
            b"/"
        } else {
            self.path
        }
    }

    /// Iterate over query parameters as `(key, Option<value>)` byte slices.
    #[must_use]
    pub fn query_params(&self) -> QueryParams<'a> {
        QueryParams {
            remaining: self.query.unwrap_or(b""),
            done: self.query.is_none(),
        }
    }
}

/// Zero-copy iterator over query string key-value pairs.
pub struct QueryParams<'a> {
    remaining: &'a [u8],
    done: bool,
}

impl<'a> Iterator for QueryParams<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let (pair, rest) = if let Some(pos) = self.remaining.iter().position(|&b| b == b'&') {
            (&self.remaining[..pos], &self.remaining[pos + 1..])
        } else {
            self.done = true;
            (self.remaining, &[] as &[u8])
        };
        self.remaining = rest;

        if pair.is_empty() && self.done {
            return None;
        }

        Some(
            pair.iter()
                .position(|&b| b == b'=')
                .map_or((pair, None), |pos| (&pair[..pos], Some(&pair[pos + 1..]))),
        )
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_authority(authority: &[u8]) -> Result<(&[u8], Option<u16>), Error> {
    if authority.is_empty() {
        return Ok((b"", None));
    }

    if authority[0] == b'[' {
        let bracket_end = authority
            .iter()
            .position(|&b| b == b']')
            .ok_or(UrlError::InvalidByte(0))?;
        let host = &authority[1..bracket_end];
        let after_bracket = &authority[bracket_end + 1..];
        if after_bracket.is_empty() {
            Ok((host, None))
        } else if after_bracket[0] == b':' {
            let port = parse_port(&after_bracket[1..])?;
            Ok((host, Some(port)))
        } else {
            Err(UrlError::InvalidByte(bracket_end + 1).into())
        }
    } else {
        match authority.iter().rposition(|&b| b == b':') {
            Some(colon_pos) => {
                let host = &authority[..colon_pos];
                let port = parse_port(&authority[colon_pos + 1..])?;
                Ok((host, Some(port)))
            }
            None => Ok((authority, None)),
        }
    }
}

fn parse_port(bytes: &[u8]) -> Result<u16, Error> {
    if bytes.is_empty() {
        return Err(UrlError::InvalidPort.into());
    }
    let mut result: u32 = 0;
    for &b in bytes {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return Err(UrlError::InvalidPort.into());
        }
        result = result * 10 + u32::from(digit);
        if result > u32::from(u16::MAX) {
            return Err(UrlError::InvalidPort.into());
        }
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(result as u16)
}

fn parse_path_query_fragment(input: &[u8]) -> (&[u8], Option<&[u8]>, Option<&[u8]>) {
    if input.is_empty() {
        return (b"", None, None);
    }

    let (before_fragment, fragment) = input
        .iter()
        .position(|&b| b == b'#')
        .map_or((input, None), |pos| {
            (&input[..pos], Some(&input[pos + 1..]))
        });

    let (path, query) = before_fragment
        .iter()
        .position(|&b| b == b'?')
        .map_or((before_fragment, None), |pos| {
            (&before_fragment[..pos], Some(&before_fragment[pos + 1..]))
        });

    (path, query, fragment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_url() {
        let url = Url::parse(b"http://example.com/path?q=1#frag").unwrap();
        assert_eq!(url.scheme, Scheme::Http);
        assert_eq!(url.host, b"example.com");
        assert_eq!(url.port, None);
        assert_eq!(url.path, b"/path");
        assert_eq!(url.query, Some(b"q=1" as &[u8]));
        assert_eq!(url.fragment, Some(b"frag" as &[u8]));
        assert_eq!(url.effective_port(), 80);
    }

    #[test]
    fn parse_https_with_port() {
        let url = Url::parse(b"https://example.com:8443/api").unwrap();
        assert_eq!(url.scheme, Scheme::Https);
        assert_eq!(url.host, b"example.com");
        assert_eq!(url.port, Some(8443));
        assert_eq!(url.path, b"/api");
        assert_eq!(url.query, None);
        assert_eq!(url.fragment, None);
        assert_eq!(url.effective_port(), 8443);
    }

    #[test]
    fn parse_no_path() {
        let url = Url::parse(b"http://example.com").unwrap();
        assert_eq!(url.host, b"example.com");
        assert_eq!(url.path, b"");
        assert_eq!(url.request_path(), b"/");
    }

    #[test]
    fn parse_root_path() {
        let url = Url::parse(b"http://example.com/").unwrap();
        assert_eq!(url.path, b"/");
        assert_eq!(url.request_path(), b"/");
    }

    #[test]
    fn parse_query_no_path() {
        let url = Url::parse(b"http://example.com?q=1").unwrap();
        assert_eq!(url.path, b"");
        assert_eq!(url.query, Some(b"q=1" as &[u8]));
    }

    #[test]
    fn parse_fragment_only() {
        let url = Url::parse(b"http://example.com#frag").unwrap();
        assert_eq!(url.path, b"");
        assert_eq!(url.query, None);
        assert_eq!(url.fragment, Some(b"frag" as &[u8]));
    }

    #[test]
    fn parse_ipv6() {
        let url = Url::parse(b"http://[::1]:8080/test").unwrap();
        assert_eq!(url.host, b"::1");
        assert_eq!(url.port, Some(8080));
        assert_eq!(url.path, b"/test");
    }

    #[test]
    fn parse_ipv6_no_port() {
        let url = Url::parse(b"http://[::1]/test").unwrap();
        assert_eq!(url.host, b"::1");
        assert_eq!(url.port, None);
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(Url::parse(b"").unwrap_err(), Error::Url(UrlError::Empty));
    }

    #[test]
    fn rejects_no_scheme() {
        assert!(Url::parse(b"example.com/path").is_err());
    }

    #[test]
    fn rejects_missing_host() {
        assert_eq!(
            Url::parse(b"http:///path").unwrap_err(),
            Error::Url(UrlError::MissingHost)
        );
    }

    #[test]
    fn rejects_invalid_port() {
        assert!(Url::parse(b"http://example.com:99999").is_err());
        assert!(Url::parse(b"http://example.com:abc").is_err());
    }

    #[test]
    fn default_ports() {
        let http = Url::parse(b"http://x.com").unwrap();
        assert_eq!(http.effective_port(), 80);

        let https = Url::parse(b"https://x.com").unwrap();
        assert_eq!(https.effective_port(), 443);
    }

    #[test]
    fn case_insensitive_scheme() {
        let url = Url::parse(b"HTTP://example.com").unwrap();
        assert_eq!(url.scheme, Scheme::Http);
    }

    #[test]
    fn percent_encoded_preserved() {
        let url = Url::parse(b"http://example.com/path%20here?q=a%20b").unwrap();
        assert_eq!(url.path, b"/path%20here");
        assert_eq!(url.query, Some(b"q=a%20b" as &[u8]));
    }

    #[test]
    fn query_params_multiple() {
        let url = Url::parse(b"http://x.com/p?a=1&b=2&c=3").unwrap();
        let params: Vec<_> = url.query_params().collect();
        assert_eq!(params.len(), 3);
        assert_eq!(params[0], (b"a" as &[u8], Some(b"1" as &[u8])));
        assert_eq!(params[1], (b"b" as &[u8], Some(b"2" as &[u8])));
        assert_eq!(params[2], (b"c" as &[u8], Some(b"3" as &[u8])));
    }

    #[test]
    fn query_params_no_value() {
        let url = Url::parse(b"http://x.com/?flag&key=val").unwrap();
        let params: Vec<_> = url.query_params().collect();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], (b"flag" as &[u8], None));
        assert_eq!(params[1], (b"key" as &[u8], Some(b"val" as &[u8])));
    }

    #[test]
    fn query_params_empty_value() {
        let url = Url::parse(b"http://x.com/?key=").unwrap();
        let params: Vec<_> = url.query_params().collect();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], (b"key" as &[u8], Some(b"" as &[u8])));
    }

    #[test]
    fn query_params_none_when_no_query() {
        let url = Url::parse(b"http://x.com/path").unwrap();
        assert!(url.query_params().next().is_none());
    }

    #[test]
    fn query_params_single() {
        let url = Url::parse(b"http://x.com/?only=one").unwrap();
        let params: Vec<_> = url.query_params().collect();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], (b"only" as &[u8], Some(b"one" as &[u8])));
    }

    // ── Adversarial URL tests ────────────────────────────────────────────────

    #[test]
    fn empty_port() {
        let err = Url::parse(b"http://host:/path").unwrap_err();
        assert_eq!(err, Error::Url(UrlError::InvalidPort));
    }

    #[test]
    fn port_zero() {
        let url = Url::parse(b"http://host:0/path").unwrap();
        assert_eq!(url.port, Some(0));
    }

    #[test]
    fn port_65535() {
        let url = Url::parse(b"http://host:65535/path").unwrap();
        assert_eq!(url.port, Some(65535));
    }

    #[test]
    fn port_65536() {
        assert!(Url::parse(b"http://host:65536/path").is_err());
    }

    #[test]
    fn scheme_only_no_host() {
        let err = Url::parse(b"http://").unwrap_err();
        assert_eq!(err, Error::Url(UrlError::MissingHost));
    }

    #[test]
    fn double_slash_in_path() {
        let url = Url::parse(b"http://host//path").unwrap();
        assert_eq!(url.path, b"//path");
    }

    #[test]
    fn query_with_consecutive_ampersands() {
        let url = Url::parse(b"http://x.com/?a=1&&b=2").unwrap();
        let params: Vec<_> = url.query_params().collect();
        assert_eq!(params.len(), 3);
        assert_eq!(params[0], (b"a" as &[u8], Some(b"1" as &[u8])));
        assert_eq!(params[1], (b"" as &[u8], None));
        assert_eq!(params[2], (b"b" as &[u8], Some(b"2" as &[u8])));
    }

    #[test]
    fn fragment_with_query_chars() {
        let url = Url::parse(b"http://x.com/#?foo=bar").unwrap();
        assert_eq!(url.query, None);
        assert_eq!(url.fragment, Some(b"?foo=bar" as &[u8]));
    }

    #[test]
    fn very_long_host() {
        let mut input = b"http://".to_vec();
        input.extend_from_slice(&[b'a'; 256]);
        input.extend_from_slice(b"/path");
        let url = Url::parse(&input).unwrap();
        assert_eq!(url.host.len(), 256);
    }

    #[test]
    fn ipv6_missing_bracket() {
        assert!(Url::parse(b"http://[::1/path").is_err());
    }

    #[test]
    fn ipv6_garbage_after_bracket() {
        assert!(Url::parse(b"http://[::1]garbage/path").is_err());
    }

    #[test]
    fn path_with_special_chars() {
        let url = Url::parse(b"http://host/a/b/../c").unwrap();
        // No normalization — raw path preserved
        assert_eq!(url.path, b"/a/b/../c");
    }

    #[test]
    fn query_with_equals_in_value() {
        let url = Url::parse(b"http://x.com/?key=a=b=c").unwrap();
        let params: Vec<_> = url.query_params().collect();
        assert_eq!(params[0], (b"key" as &[u8], Some(b"a=b=c" as &[u8])));
    }

    #[test]
    fn scheme_with_trailing_colon_no_slashes() {
        assert!(Url::parse(b"http:host/path").is_err());
    }
}
