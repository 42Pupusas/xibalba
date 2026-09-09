use crate::bytes::ByteSliceExt;
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
    /// Returns `Error::UrlParse` variants for malformed input.
    pub fn parse(input: &'a [u8]) -> Result<Self, Error> {
        if input.is_empty() {
            return Err(UrlError::Empty.into());
        }

        let sep_pos = input
            .find_subsequence(b"://")
            .ok_or(UrlError::InvalidScheme)?;
        let scheme = Scheme::try_from(&input[..sep_pos])?;
        let rest = &input[sep_pos + 3..];

        let authority_end = rest
            .iter()
            .position(|&b| b == b'/' || b == b'?' || b == b'#')
            .unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        let after_authority = &rest[authority_end..];

        let (host, port) = Self::parse_authority(authority)?;
        if host.is_empty() {
            return Err(UrlError::MissingHost.into());
        }
        if let Some(bad) = host.iter().position(|&b| !Self::is_host_byte(b)) {
            return Err(UrlError::InvalidByte(bad).into());
        }

        let (path, query, fragment) = Self::parse_path_query_fragment(after_authority);

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

    /// The host to connect to, with the brackets of an IPv6 literal removed.
    ///
    /// [`Self::host`] keeps the authority as written, because that is what
    /// belongs in the `Host` header (RFC 9110 §7.2 uses the authority form,
    /// brackets and all). Name resolution and TLS server names take the
    /// address itself: `ToSocketAddrs`, `IpAddr::from_str`, and rustls'
    /// `ServerName` all reject `[::1]` and accept `::1`. A connector passing
    /// [`Self::host`] to any of them fails on every IPv6 URL.
    ///
    /// A zone ID stays attached: it is part of the scoped address and the
    /// resolver is the layer that decides what to do with it.
    #[must_use]
    pub fn connection_host(&self) -> &'a [u8] {
        match (self.host.first(), self.host.last()) {
            (Some(b'['), Some(b']')) => &self.host[1..self.host.len() - 1],
            _ => self.host,
        }
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

    fn parse_authority(authority: &[u8]) -> Result<(&[u8], Option<u16>), Error> {
        if authority.is_empty() {
            return Ok((b"", None));
        }

        if let Some(at) = authority.iter().position(|&b| b == b'@') {
            return Err(UrlError::InvalidByte(at).into());
        }

        if authority[0] == b'[' {
            let bracket_end = authority
                .iter()
                .position(|&b| b == b']')
                .ok_or(UrlError::InvalidByte(0))?;
            let host = &authority[..=bracket_end];
            Self::validate_ip_literal(&authority[1..bracket_end])?;
            let after_bracket = &authority[bracket_end + 1..];
            if after_bracket.is_empty() {
                Ok((host, None))
            } else if after_bracket[0] == b':' {
                let port = Self::parse_port(&after_bracket[1..])?;
                Ok((host, Some(port)))
            } else {
                Err(UrlError::InvalidByte(bracket_end + 1).into())
            }
        } else if let Some(colon_pos) = authority.iter().rposition(|&b| b == b':') {
            let host = &authority[..colon_pos];
            Self::validate_reg_name(host)?;
            let port = Self::parse_port(&authority[colon_pos + 1..])?;
            Ok((host, Some(port)))
        } else {
            Self::validate_reg_name(authority)?;
            Ok((authority, None))
        }
    }

    /// Check the bytes between brackets look like an `IP-literal`
    /// (RFC 3986 §3.2.2): an IPv6 address, optionally with a zone ID, or an
    /// `IPvFuture` form. A reg-name in brackets is malformed rather than a
    /// hostname, and empty brackets name no host at all.
    fn validate_ip_literal(inner: &[u8]) -> Result<(), Error> {
        if inner.is_empty() {
            return Err(UrlError::InvalidByte(1).into());
        }
        if inner[0] == b'v' || inner[0] == b'V' {
            return Self::validate_ipvfuture(inner);
        }
        // A zone ID is separated by a percent-encoded '%' ("%25", RFC 6874);
        // only the address part is subject to IPv6address's own grammar.
        let (address, zone) = inner
            .windows(3)
            .position(|w| w == b"%25")
            .map_or((inner, None), |pos| {
                (&inner[..pos], Some(&inner[pos + 3..]))
            });
        Self::validate_ipv6_address(address)?;
        if let Some(zone) = zone {
            Self::validate_zone_id(zone)?;
        }
        Ok(())
    }

    /// `IPv6address` (RFC 3986 §3.2.2), the full production rather than a
    /// character-class approximation: a fixed count of 16-bit `h16` groups
    /// (or fewer with one `::` standing in for the run of zero groups it
    /// elides), the last two of which may instead be an embedded
    /// `IPv4address` (`ls32`). A character-class check alone accepts
    /// `[:]` and `[1:2:3]`, neither of which names an address; counting
    /// groups and requiring exactly one `::` when the count is short is
    /// what a real parser of the production does.
    fn validate_ipv6_address(address: &[u8]) -> Result<(), Error> {
        let err = || Error::from(UrlError::InvalidByte(1));
        if address.is_empty() {
            return Err(err());
        }

        let double_colon = address.windows(2).position(|w| w == b"::");
        // At most one "::" may appear; a second occurrence is invalid, which
        // `windows(2)` finding a *further* one after the first would show as
        // three colons in a row or two separate "::" runs. Reject either by
        // checking there is no second match past the first.
        if let Some(first) = double_colon
            && address[first + 2..].windows(2).any(|w| w == b"::")
        {
            return Err(err());
        }

        let (left, right, elided) = double_colon.map_or_else(
            || (address, &b""[..], false),
            |pos| (&address[..pos], &address[pos + 2..], true),
        );

        let left_groups = Self::split_h16_groups(left)?;
        let (right_groups, right_has_embedded_v4) = Self::split_h16_groups_allowing_v4(right)?;

        let right_weight = right_groups.len() + usize::from(right_has_embedded_v4);
        let total = left_groups.len() + right_weight;

        if elided {
            // "::" must stand for at least one elided group, or the address
            // could have been written without it.
            if total >= 8 {
                return Err(err());
            }
        } else if total != 8 {
            return Err(err());
        }
        Ok(())
    }

    /// Split `part` on `:` into `h16` groups (1-4 hex digits each),
    /// rejecting anything that is not a plain hex group — used for the side
    /// of a `::` that cannot embed an `IPv4address`.
    fn split_h16_groups(part: &[u8]) -> Result<Vec<&[u8]>, Error> {
        Self::h16_groups(part, false).map(|(groups, _)| groups)
    }

    /// As [`Self::split_h16_groups`], but the final group may instead be a
    /// dotted-decimal `IPv4address` (`ls32`), as `2001:db8::a.b.c.d`
    /// permits. Returns whether that embedded form was used, since it
    /// counts as two `h16` groups' worth of address space.
    fn split_h16_groups_allowing_v4(part: &[u8]) -> Result<(Vec<&[u8]>, bool), Error> {
        Self::h16_groups(part, true)
    }

    fn h16_groups(part: &[u8], allow_trailing_v4: bool) -> Result<(Vec<&[u8]>, bool), Error> {
        let err = || Error::from(UrlError::InvalidByte(1));
        if part.is_empty() {
            return Ok((Vec::new(), false));
        }
        let raw_groups: Vec<&[u8]> = part.split(|&b| b == b':').collect();
        if raw_groups.iter().any(|g| g.is_empty()) {
            // A leading/trailing/doubled ':' outside the one "::" already
            // consumed is not a valid group boundary.
            return Err(err());
        }
        let last = raw_groups.last().copied().unwrap_or(b"");
        if allow_trailing_v4 && Self::is_ipv4_address(last) {
            return Ok((raw_groups[..raw_groups.len() - 1].to_vec(), true));
        }
        for group in &raw_groups {
            if group.is_empty() || group.len() > 4 || !group.iter().all(u8::is_ascii_hexdigit) {
                return Err(err());
            }
        }
        Ok((raw_groups, false))
    }

    /// `IPv4address = dec-octet "." dec-octet "." dec-octet "." dec-octet`,
    /// each octet `0`-`255` with no extraneous leading zero.
    fn is_ipv4_address(bytes: &[u8]) -> bool {
        let octets: Vec<&[u8]> = bytes.split(|&b| b == b'.').collect();
        octets.len() == 4
            && octets.iter().all(|octet| {
                !octet.is_empty()
                    && octet.len() <= 3
                    && octet.iter().all(u8::is_ascii_digit)
                    && (octet.len() == 1 || octet[0] != b'0')
                    && octet
                        .iter()
                        .fold(0u32, |acc, &b| acc * 10 + u32::from(b - b'0'))
                        <= 255
            })
    }

    /// `ZoneID = 1*( unreserved / pct-encoded )` (RFC 6874). Interfaces
    /// names are typically plain `unreserved` bytes (`eth0`, `en0`); percent
    /// escapes are permitted but, like a reg-name's, must be well-formed.
    fn validate_zone_id(zone: &[u8]) -> Result<(), Error> {
        if zone.is_empty() {
            return Err(UrlError::InvalidByte(1).into());
        }
        Self::validate_pct_encoded_unreserved(zone, Self::is_zone_id_byte)
    }

    const fn is_zone_id_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
    }

    /// `IPvFuture = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )`
    fn validate_ipvfuture(inner: &[u8]) -> Result<(), Error> {
        let dot = inner
            .iter()
            .position(|&b| b == b'.')
            .ok_or(UrlError::InvalidByte(1))?;
        let version = &inner[1..dot];
        let rest = &inner[dot + 1..];
        if version.is_empty() || rest.is_empty() || !version.iter().all(u8::is_ascii_hexdigit) {
            return Err(UrlError::InvalidByte(1).into());
        }
        let is_future_byte = |b: u8| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'-' | b'.'
                        | b'_'
                        | b'~'
                        | b'!'
                        | b'$'
                        | b'&'
                        | b'\''
                        | b'('
                        | b')'
                        | b'*'
                        | b'+'
                        | b','
                        | b';'
                        | b'='
                        | b':'
                )
        };
        if !rest.iter().all(|&b| is_future_byte(b)) {
            return Err(UrlError::InvalidByte(1).into());
        }
        Ok(())
    }

    /// Validate a byte sequence built from `unreserved` bytes (accepted by
    /// `plain`) and well-formed `pct-encoded` (`%` `HEXDIG` `HEXDIG`)
    /// triples, per RFC 3986 §2.1. Shared by reg-name and zone-ID
    /// validation, which differ only in which unescaped bytes they accept.
    fn validate_pct_encoded_unreserved(
        bytes: &[u8],
        plain: impl Fn(u8) -> bool,
    ) -> Result<(), Error> {
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'%' {
                let hi = bytes.get(i + 1).copied();
                let lo = bytes.get(i + 2).copied();
                if !hi.is_some_and(|h| h.is_ascii_hexdigit())
                    || !lo.is_some_and(|l| l.is_ascii_hexdigit())
                {
                    return Err(UrlError::InvalidByte(i).into());
                }
                i += 3;
            } else if plain(b) {
                i += 1;
            } else {
                return Err(UrlError::InvalidByte(i).into());
            }
        }
        Ok(())
    }

    /// Check an unbracketed host is a `reg-name = *( unreserved /
    /// pct-encoded / sub-delims )` (RFC 3986 §3.2.2): well-formed percent
    /// escapes, and no bytes reserved for an `IP-literal` or the port
    /// delimiter (the port has already been split off, so a colon here can
    /// only be a second, malformed one).
    ///
    /// `is_host_byte` at the parse entry point already excludes CTLs, space,
    /// and the path/query/fragment delimiters; this narrows further to what
    /// `reg-name` itself allows, catching a `%` not followed by two hex
    /// digits (`http://bad%zz/`) that a character-class check alone would
    /// accept as three ordinary bytes.
    fn validate_reg_name(host: &[u8]) -> Result<(), Error> {
        Self::validate_pct_encoded_unreserved(host, Self::is_reg_name_byte)
    }

    const fn is_reg_name_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
            )
    }

    /// Bytes allowed in a host: `reg-name` / `IPv4address` / bracketed
    /// `IP-literal` (RFC 3986 §3.2.2). Excludes space, CTLs, DEL and the
    /// authority/path delimiters that would already have been split off.
    const fn is_host_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'%'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b'['
                    | b']'
                    | b':'
            )
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
        #[expect(
            clippy::cast_possible_truncation,
            reason = "result is checked > u16::MAX just above; value is guaranteed in range"
        )]
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
        assert_eq!(url.host, b"[::1]");
        assert_eq!(url.port, Some(8080));
        assert_eq!(url.path, b"/test");
    }

    #[test]
    fn parse_ipv6_no_port() {
        let url = Url::parse(b"http://[::1]/test").unwrap();
        assert_eq!(url.host, b"[::1]");
        assert_eq!(url.port, None);
    }

    /// The `Host` header carries the authority as written, but resolvers and
    /// TLS server names take the bare address. Keeping only one of the two
    /// forms breaks whichever consumer needs the other.
    #[test]
    fn connection_host_strips_ipv6_brackets_but_host_keeps_them() {
        let url = Url::parse(b"http://[::1]:8080/test").unwrap();
        assert_eq!(url.host, b"[::1]");
        assert_eq!(url.connection_host(), b"::1");
    }

    #[test]
    fn connection_host_leaves_a_reg_name_and_ipv4_untouched() {
        for input in [
            b"http://example.com/".as_slice(),
            b"http://127.0.0.1:8080/".as_slice(),
        ] {
            let url = Url::parse(input).unwrap();
            assert_eq!(url.connection_host(), url.host);
        }
    }

    /// A zone ID scopes the address and belongs to the resolver, so it must
    /// survive unbracketing rather than being silently dropped.
    #[test]
    fn connection_host_keeps_a_zone_id() {
        let url = Url::parse(b"http://[fe80::1%25eth0]:80/").unwrap();
        assert_eq!(url.connection_host(), b"fe80::1%25eth0");
    }

    #[test]
    fn userinfo_rejected() {
        assert!(matches!(
            Url::parse(b"http://user:pass@example.com/path").unwrap_err(),
            Error::UrlParse(UrlError::InvalidByte(_))
        ));
        assert!(Url::parse(b"http://a@b/").is_err());
        assert!(Url::parse(b"http://user@example.com").is_err());
    }

    #[test]
    fn at_sign_in_path_is_not_userinfo() {
        let url = Url::parse(b"http://example.com/u@ser").unwrap();
        assert_eq!(url.host, b"example.com");
        assert_eq!(url.path, b"/u@ser");
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(
            Url::parse(b"").unwrap_err(),
            Error::UrlParse(UrlError::Empty)
        );
    }

    #[test]
    fn rejects_no_scheme() {
        assert!(Url::parse(b"example.com/path").is_err());
    }

    #[test]
    fn rejects_missing_host() {
        assert_eq!(
            Url::parse(b"http:///path").unwrap_err(),
            Error::UrlParse(UrlError::MissingHost)
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
        assert_eq!(err, Error::UrlParse(UrlError::InvalidPort));
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
        assert_eq!(err, Error::UrlParse(UrlError::MissingHost));
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
    fn empty_bracket_literal_rejected() {
        // "[]" contains no address at all. Accepting it hands a connector an
        // authority it cannot resolve.
        assert!(Url::parse(b"http://[]/").is_err());
    }

    #[test]
    fn bracket_literal_must_look_like_an_ip() {
        // A bracketed literal is an IP-literal by definition; a reg-name in
        // brackets is malformed, not a hostname.
        assert!(Url::parse(b"http://[not-an-ip]/").is_err());
        assert!(Url::parse(b"http://[example.com]/").is_err());
    }

    #[test]
    fn unbracketed_host_rejects_extra_colons() {
        // "a:b:80" parses as host "a:b" with port 80, so a colon inside an
        // unbracketed reg-name silently becomes part of the host.
        assert!(Url::parse(b"http://a:b:80/").is_err());
    }

    #[test]
    fn unbracketed_host_rejects_brackets() {
        // Brackets delimit an IP-literal; inside a reg-name they are junk.
        assert!(Url::parse(b"http://ex[ample.com/").is_err());
        assert!(Url::parse(b"http://example]com/").is_err());
    }

    #[test]
    fn valid_ipv6_literals_are_still_accepted() {
        // The stricter check must not reject real IP literals.
        assert_eq!(Url::parse(b"http://[::1]/").unwrap().host, b"[::1]");
        assert_eq!(
            Url::parse(b"http://[2001:db8::1]:8080/").unwrap().host,
            b"[2001:db8::1]"
        );
        assert_eq!(
            Url::parse(b"http://[fe80::1%25eth0]/").unwrap().host,
            b"[fe80::1%25eth0]"
        );
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
    fn host_with_space_rejected() {
        assert!(matches!(
            Url::parse(b"http://exa mple.com/").unwrap_err(),
            Error::UrlParse(UrlError::InvalidByte(3))
        ));
    }

    #[test]
    fn host_with_ctl_rejected() {
        assert!(Url::parse(b"http://example.com\r\nX: y/").is_err());
        assert!(Url::parse(b"http://exam\x00ple.com/").is_err());
    }

    #[test]
    fn host_with_non_ascii_rejected() {
        assert!(Url::parse("http://bücher.example/".as_bytes()).is_err());
    }

    #[test]
    fn scheme_with_trailing_colon_no_slashes() {
        assert!(Url::parse(b"http:host/path").is_err());
    }

    // ── R07: real IPv6/reg-name/IPvFuture grammar, not a character class ────

    /// The exact case REAUDIT.md calls out: `[:]` passes a colon-and-hex
    /// character-class check but names no address at all.
    #[test]
    fn ipv6_bare_colon_is_rejected() {
        assert!(Url::parse(b"http://[:]/").is_err());
    }

    /// The other exact case: `[1:2:3]` is three groups of a syntax that
    /// needs eight (or a `::` eliding some of them), so a character-class
    /// check that only inspects the bytes present accepts it wrongly.
    #[test]
    fn ipv6_too_few_groups_without_elision_is_rejected() {
        assert!(Url::parse(b"http://[1:2:3]/").is_err());
    }

    #[test]
    fn ipv6_full_eight_groups_is_accepted() {
        assert!(Url::parse(b"http://[2001:db8:0:0:0:0:0:1]/").is_ok());
    }

    #[test]
    fn ipv6_elided_zero_run_is_accepted() {
        for addr in ["::1", "::", "fe80::1", "2001:db8::1", "1::2:3:4:5:6:7"] {
            let url = format!("http://[{addr}]/");
            assert!(
                Url::parse(url.as_bytes()).is_ok(),
                "{addr} should be a valid elided IPv6 address"
            );
        }
    }

    /// `::` stands for *at least* one elided group; using it when all eight
    /// groups are already spelled out is not a shorter way to write the
    /// same address, it changes what the elision would mean.
    #[test]
    fn ipv6_double_colon_with_all_eight_groups_already_present_is_rejected() {
        assert!(Url::parse(b"http://[1:2:3:4:5:6:7::8]/").is_err());
    }

    #[test]
    fn ipv6_two_double_colons_is_rejected() {
        assert!(Url::parse(b"http://[1::2::3]/").is_err());
    }

    #[test]
    fn ipv6_group_with_too_many_hex_digits_is_rejected() {
        assert!(Url::parse(b"http://[fffff::1]/").is_err());
    }

    #[test]
    fn ipv6_group_with_non_hex_byte_is_rejected() {
        assert!(Url::parse(b"http://[fg::1]/").is_err());
    }

    #[test]
    fn ipv6_embedded_ipv4_tail_is_accepted() {
        assert!(Url::parse(b"http://[::ffff:192.168.1.1]/").is_ok());
        assert!(Url::parse(b"http://[2001:db8::1:192.168.1.1]/").is_ok());
    }

    #[test]
    fn ipv6_embedded_ipv4_with_an_invalid_octet_is_rejected() {
        assert!(Url::parse(b"http://[::ffff:192.168.1.999]/").is_err());
        assert!(Url::parse(b"http://[::ffff:192.168.1]/").is_err());
    }

    #[test]
    fn ipv6_valid_zone_id_is_accepted() {
        assert!(Url::parse(b"http://[fe80::1%25eth0]/").is_ok());
        assert!(Url::parse(b"http://[fe80::1%25en0]/").is_ok());
    }

    #[test]
    fn ipv6_empty_zone_id_is_rejected() {
        assert!(Url::parse(b"http://[fe80::1%25]/").is_err());
    }

    /// The exact case REAUDIT.md calls out: an invalid percent escape in an
    /// unbracketed host must be caught, not accepted as three literal bytes.
    #[test]
    fn reg_name_with_an_invalid_percent_escape_is_rejected() {
        assert!(Url::parse(b"http://bad%zz/").is_err());
    }

    #[test]
    fn reg_name_with_a_truncated_percent_escape_is_rejected() {
        assert!(Url::parse(b"http://bad%2/").is_err());
        assert!(Url::parse(b"http://bad%/").is_err());
    }

    #[test]
    fn reg_name_with_a_well_formed_percent_escape_is_accepted() {
        assert!(Url::parse(b"http://ex%41mple.com/").is_ok());
    }

    #[test]
    fn ipvfuture_with_valid_suffix_grammar_is_accepted() {
        assert!(Url::parse(b"http://[v1.fe80::1]/").is_ok());
        assert!(Url::parse(b"http://[vA.abc123:xyz]/").is_ok());
    }

    /// The suffix grammar excludes bytes such as `@`, `/`, and `"`, which
    /// are neither `unreserved`, `sub-delims`, nor `:`.
    #[test]
    fn ipvfuture_with_an_invalid_suffix_byte_is_rejected() {
        assert!(Url::parse(b"http://[v1.ab@cd]/").is_err());
        assert!(Url::parse(b"http://[v1.a\"b]/").is_err());
    }

    #[test]
    fn ipvfuture_with_non_hex_version_is_rejected() {
        assert!(Url::parse(b"http://[vZ.abc]/").is_err());
    }
}
