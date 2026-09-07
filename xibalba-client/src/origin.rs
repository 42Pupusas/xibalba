//! The scheme/host/port triple a connection is bound to.

use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::scheme::Scheme;
use xibalba_proto::url::Url;

/// What a connection is connected *to*.
///
/// Kept together because every question worth asking spans all three: two
/// URLs share an origin only if scheme, host and port all agree, and the
/// `Host` header omits the port only when it is that scheme's default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    scheme: Scheme,
    host: Vec<u8>,
    port: u16,
}

impl Origin {
    #[must_use]
    pub fn from_url(url: &Url<'_>) -> Self {
        Self {
            scheme: url.scheme,
            host: url.host.to_vec(),
            port: url.effective_port(),
        }
    }

    #[must_use]
    pub const fn scheme(&self) -> Scheme {
        self.scheme
    }

    #[must_use]
    pub const fn scheme_bytes(&self) -> &'static [u8] {
        self.scheme.as_bytes()
    }

    /// Whether `url` points at this origin.
    #[must_use]
    pub fn covers(&self, url: &Url<'_>) -> bool {
        url.host.ascii_eq_ignore_case(&self.host)
            && url.effective_port() == self.port
            && url.scheme == self.scheme
    }

    /// Whether reaching `target` from here would downgrade HTTPS to HTTP.
    #[must_use]
    pub fn downgrades_to(&self, target: Scheme) -> bool {
        self.scheme == Scheme::Https && target == Scheme::Http
    }

    /// The `Host` header value: the port is elided when it is the scheme's
    /// default, as RFC 9110 §7.2 expects.
    #[must_use]
    pub fn host_header_value(&self) -> Vec<u8> {
        if self.port == self.scheme.default_port() {
            return self.host.clone();
        }
        let mut value = self.host.clone();
        value.push(b':');
        value.extend_from_slice(self.port.to_string().as_bytes());
        value
    }

    /// This origin as a URL for its root, so a reconnection to the same host
    /// does not have to re-parse one.
    #[must_use]
    pub fn root_url(&self) -> Url<'_> {
        Url {
            scheme: self.scheme,
            host: &self.host,
            port: Some(self.port),
            path: b"/",
            query: None,
            fragment: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(text: &[u8]) -> Url<'_> {
        Url::parse(text).expect("test urls parse")
    }

    #[test]
    fn a_default_port_is_omitted_from_the_host_header() {
        let origin = Origin::from_url(&url(b"http://example.com/x"));
        assert_eq!(origin.host_header_value(), b"example.com");
    }

    #[test]
    fn a_non_default_port_is_included_in_the_host_header() {
        let origin = Origin::from_url(&url(b"http://example.com:8080/x"));
        assert_eq!(origin.host_header_value(), b"example.com:8080");
    }

    #[test]
    fn https_on_443_omits_the_port() {
        let origin = Origin::from_url(&url(b"https://example.com/x"));
        assert_eq!(origin.host_header_value(), b"example.com");
    }

    #[test]
    fn an_explicit_default_port_still_counts_as_the_same_origin() {
        let origin = Origin::from_url(&url(b"http://example.com/x"));
        assert!(origin.covers(&url(b"http://example.com:80/other")));
    }

    #[test]
    fn a_host_differing_only_in_case_is_the_same_origin() {
        let origin = Origin::from_url(&url(b"http://Example.COM/x"));
        assert!(origin.covers(&url(b"http://example.com/x")));
    }

    #[test]
    fn a_different_scheme_port_or_host_is_a_different_origin() {
        let origin = Origin::from_url(&url(b"https://example.com/x"));
        assert!(!origin.covers(&url(b"http://example.com/x")));
        assert!(!origin.covers(&url(b"https://example.com:8443/x")));
        assert!(!origin.covers(&url(b"https://other.com/x")));
    }

    #[test]
    fn only_https_to_http_is_a_downgrade() {
        let secure = Origin::from_url(&url(b"https://example.com/"));
        let plain = Origin::from_url(&url(b"http://example.com/"));
        assert!(secure.downgrades_to(Scheme::Http));
        assert!(!secure.downgrades_to(Scheme::Https));
        assert!(!plain.downgrades_to(Scheme::Http));
        assert!(!plain.downgrades_to(Scheme::Https));
    }

    #[test]
    fn a_root_url_round_trips_the_origin() {
        let origin = Origin::from_url(&url(b"https://example.com:8443/deep/path?q=1"));
        let root = origin.root_url();
        assert_eq!(Origin::from_url(&root), origin);
        assert_eq!(root.path, b"/");
        assert_eq!(root.query, None);
    }
}
