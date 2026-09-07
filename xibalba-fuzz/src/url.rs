use xibalba_proto::error::Error;
use xibalba_proto::url::Url;

/// Properties of [`Url::parse`] that must hold for arbitrary bytes.
pub struct UrlInvariants<'a> {
    data: &'a [u8],
}

impl<'a> UrlInvariants<'a> {
    #[must_use]
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn check_all(&self) {
        match Url::parse(self.data) {
            Ok(url) => {
                self.every_part_is_borrowed(&url);
                self.a_host_is_never_empty(&url);
                self.connection_host_stays_within_the_host(&url);
                Self::a_request_path_is_absolute(&url);
                Self::no_part_smuggles_a_line_ending(&url);
                Self::query_params_terminate(&url);
            }
            Err(e) => Self::a_failure_is_a_classification_not_a_crash(&e),
        }

        self.parsing_twice_agrees();
    }

    /// A `Url` borrows the bytes it was parsed from. A part pointing elsewhere
    /// would mean the parser invented authority data — the exact thing a
    /// connector then dials.
    fn every_part_is_borrowed(&self, url: &Url<'_>) {
        for (name, part) in [
            ("host", url.host),
            ("path", url.path),
            ("query", url.query.unwrap_or(b"")),
            ("fragment", url.fragment.unwrap_or(b"")),
        ] {
            assert!(
                self.is_borrowed_from_input(part),
                "the parsed {name} does not point into the input buffer"
            );
        }
    }

    /// Everything downstream — the `Host` header, the DNS lookup, the TLS
    /// server name — assumes a host exists. An empty one is rejected at parse
    /// time so no later layer has to handle it.
    fn a_host_is_never_empty(&self, url: &Url<'_>) {
        assert!(
            !url.host.is_empty(),
            "parsed a URL with an empty host from {:?}",
            self.as_text()
        );
    }

    /// `connection_host` strips IPv6 brackets for the resolver. It must strip
    /// only that: a host that grows or drifts outside the parsed one would be
    /// a different destination than the URL named.
    fn connection_host_stays_within_the_host(&self, url: &Url<'_>) {
        let connection = url.connection_host();
        assert!(
            connection.len() <= url.host.len(),
            "connection_host is longer than the host it came from"
        );
        assert!(
            self.is_borrowed_from_input(connection),
            "connection_host does not point into the input buffer"
        );
        assert!(
            !connection.starts_with(b"[") && !connection.ends_with(b"]"),
            "connection_host kept the brackets a resolver rejects: {:?}",
            core::str::from_utf8(connection)
        );
    }

    /// The request line needs an absolute path; an empty one becomes `/`.
    /// A relative or empty path here would produce a malformed request.
    fn a_request_path_is_absolute(url: &Url<'_>) {
        let path = url.request_path();
        assert!(
            path.starts_with(b"/"),
            "request_path returned {:?}, which is not absolute",
            core::str::from_utf8(path)
        );
    }

    /// The host reaches the `Host` header and the TLS server name, and it is
    /// validated at parse time — a CR or LF here would split the request.
    ///
    /// Path and query are deliberately *not* checked: `Url::parse` preserves
    /// them as written, and it is `Request`'s serializer that rejects bytes
    /// no target may contain. Asserting it here would demand a guarantee this
    /// layer never made and does not need to make.
    fn no_part_smuggles_a_line_ending(url: &Url<'_>) {
        assert!(
            !url.host.iter().any(|&b| b == b'\r' || b == b'\n'),
            "the parsed host contains CR or LF, which splits the request line"
        );
    }

    /// The iterator is driven by a caller walking a query string it did not
    /// write. It must end.
    fn query_params_terminate(url: &Url<'_>) {
        let budget = url.query.unwrap_or(b"").len() + 1;
        let seen = url.query_params().take(budget + 1).count();
        assert!(
            seen <= budget,
            "query_params yielded more items than the query has bytes"
        );
    }

    fn a_failure_is_a_classification_not_a_crash(error: &Error) {
        assert!(
            matches!(error, Error::UrlParse(_) | Error::Parse(_)),
            "parsing a URL produced {error:?}, which is not a URL verdict"
        );
    }

    fn parsing_twice_agrees(&self) {
        assert_eq!(
            Url::parse(self.data),
            Url::parse(self.data),
            "the same bytes parsed to two different URLs"
        );
    }

    fn is_borrowed_from_input(&self, slice: &[u8]) -> bool {
        if slice.is_empty() {
            return true;
        }
        let outer = self.data.as_ptr_range();
        let inner = slice.as_ptr_range();
        inner.start >= outer.start && inner.end <= outer.end
    }

    fn as_text(&self) -> &str {
        core::str::from_utf8(self.data).unwrap_or("<non-utf8>")
    }
}
