use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::method::Method;
use xibalba_proto::status::StatusCode;
use xibalba_proto::url::Url;

use crate::origin::Origin;
use crate::params::RequestParams;
use crate::reference::UriReference;
use crate::response::Response;

/// What following one `Location` requires of the connection.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Hop {
    /// Same origin: reuse the connection as it stands.
    SameOrigin,
    /// A different origin, which the caller must connect to before
    /// dispatching. Credentials have already been stripped from the request.
    Reconnect(Vec<u8>),
}

/// Per-request state carried across redirect hops.
#[derive(Debug)]
pub(crate) struct RedirectState {
    method: Method,
    path: Vec<u8>,
    query: Option<Vec<u8>>,
    body: Option<Vec<u8>>,
    extra_headers: Vec<(Vec<u8>, Vec<u8>)>,
    allow_replay: bool,
}

impl RedirectState {
    pub(crate) fn new(params: &RequestParams<'_>) -> Self {
        Self {
            method: params.method,
            path: params.path.to_vec(),
            query: params.query.map(<[u8]>::to_vec),
            body: params.body.map(<[u8]>::to_vec),
            extra_headers: params.extra_headers.clone(),
            allow_replay: params.allow_replay,
        }
    }

    fn to_params(&self) -> RequestParams<'_> {
        RequestParams {
            method: self.method,
            path: &self.path,
            query: self.query.as_deref(),
            body: self.body.as_deref(),
            extra_headers: self.extra_headers.clone(),
            allow_replay: self.allow_replay,
        }
    }

    /// Drop credentials-bearing headers when a redirect leaves the origin.
    /// The client writes `Host` from its current connection state on every
    /// request; adding it here would serialize two Host fields.
    fn strip_cross_origin_headers(&mut self) {
        self.extra_headers.retain(|(name, _)| {
            !(name.ascii_eq_ignore_case(b"Authorization")
                || name.ascii_eq_ignore_case(b"Proxy-Authorization")
                || name.ascii_eq_ignore_case(b"Cookie")
                || name.ascii_eq_ignore_case(b"Cookie2"))
        });
    }

    /// The parameters for the next dispatch.
    pub(crate) fn params(&self) -> RequestParams<'_> {
        self.to_params()
    }

    /// The `Location` of a response worth following, if there is one.
    ///
    /// `None` means this response is the answer: either its status does not
    /// redirect, or it redirects without saying where.
    pub(crate) fn location_to_follow(response: &Response) -> Option<Vec<u8>> {
        if !Self::is_followed_status(response.status) {
            return None;
        }
        response
            .headers()
            .find(|(name, _)| name.ascii_eq_ignore_case(b"Location"))
            .map(|(_, value)| value.to_vec())
    }

    /// Apply one `Location` to this request, reporting what the connection
    /// must do to serve the result.
    ///
    /// Nothing here touches a connection: a cross-origin hop is *reported*,
    /// so the decision to dial is the caller's and is made after this
    /// returns. That is what lets a refused downgrade be refused before
    /// anything is dialled.
    pub(crate) fn advance(
        &mut self,
        from: &Origin,
        status: StatusCode,
        location: &[u8],
    ) -> Result<Hop, Error> {
        self.apply_method_redirect(status);
        self.apply_location(from, location)
    }

    const fn is_followed_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::MOVED_PERMANENTLY
                | StatusCode::FOUND
                | StatusCode::SEE_OTHER
                | StatusCode::TEMPORARY_REDIRECT
                | StatusCode::PERMANENT_REDIRECT
        )
    }

    fn apply_method_redirect(&mut self, status: StatusCode) {
        let becomes_get = match status {
            StatusCode::SEE_OTHER => self.method != Method::Head,
            StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND => self.method == Method::Post,
            _ => false,
        };
        if becomes_get {
            self.method = Method::Get;
            self.body = None;
            self.strip_representation_headers();
        }
    }

    /// Drop headers that describe a body the rewritten request no longer
    /// carries. Leaving `Content-Type` on a bodyless GET misdescribes it, and
    /// `Content-Length`/`Transfer-Encoding` are serialized by the client
    /// itself.
    fn strip_representation_headers(&mut self) {
        self.extra_headers.retain(|(name, _)| {
            !(name.ascii_eq_ignore_case(b"Content-Type")
                || name.ascii_eq_ignore_case(b"Content-Encoding")
                || name.ascii_eq_ignore_case(b"Content-Language")
                || name.ascii_eq_ignore_case(b"Content-Location"))
        });
    }

    fn apply_location(&mut self, from: &Origin, location: &[u8]) -> Result<Hop, Error> {
        let without_fragment = location
            .iter()
            .position(|&b| b == b'#')
            .map_or(location, |pos| &location[..pos]);

        if Self::has_absolute_scheme(without_fragment) {
            return self.apply_absolute_location(from, without_fragment);
        }
        if without_fragment.starts_with(b"//") {
            let mut absolute = from.scheme_bytes().to_vec();
            absolute.push(b':');
            absolute.extend_from_slice(without_fragment);
            return self.apply_absolute_location(from, &absolute);
        }

        let (path_part, query) = without_fragment.iter().position(|&b| b == b'?').map_or(
            (without_fragment, None),
            |pos| {
                (
                    &without_fragment[..pos],
                    Some(without_fragment[pos + 1..].to_vec()),
                )
            },
        );

        if path_part.is_empty() {
            if query.is_some() {
                self.query = query;
            }
            return Ok(Hop::SameOrigin);
        }

        self.path = if path_part.starts_with(b"/") {
            UriReference::remove_dot_segments(path_part)
        } else {
            self.resolve_relative_path(path_part)
        };
        self.query = query;
        Ok(Hop::SameOrigin)
    }

    fn apply_absolute_location(&mut self, from: &Origin, location: &[u8]) -> Result<Hop, Error> {
        let url = Url::parse(location)?;
        // Refuse before reporting a hop: a downgrade must not be detected by
        // observing that we already opened a plaintext connection.
        if from.downgrades_to(url.scheme) {
            return Err(ConnectionError::InsecureRedirect.into());
        }
        let hop = if from.covers(&url) {
            Hop::SameOrigin
        } else {
            self.strip_cross_origin_headers();
            Hop::Reconnect(location.to_vec())
        };
        self.path = if url.path.is_empty() {
            b"/".to_vec()
        } else {
            UriReference::remove_dot_segments(url.path)
        };
        self.query = url.query.map(<[u8]>::to_vec);
        Ok(hop)
    }

    /// Whether `location` begins with an absolute HTTP(S) scheme.
    ///
    /// Schemes are case-insensitive, and `Url::parse` accepts mixed case, so
    /// matching only lowercase here would treat `HTTPS://host/` as a relative
    /// path and graft the whole URL onto the current one.
    fn has_absolute_scheme(location: &[u8]) -> bool {
        location
            .iter()
            .position(|&b| b == b':')
            .is_some_and(|colon| {
                let scheme = &location[..colon];
                (scheme.ascii_eq_ignore_case(b"http") || scheme.ascii_eq_ignore_case(b"https"))
                    && location[colon..].starts_with(b"://")
            })
    }

    fn resolve_relative_path(&self, relative: &[u8]) -> Vec<u8> {
        UriReference::resolve(&self.path, relative)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params_with(headers: &[(&str, &str)]) -> RequestParams<'static> {
        RequestParams {
            method: Method::Get,
            path: b"/",
            query: None,
            body: None,
            extra_headers: headers
                .iter()
                .map(|(n, v)| (n.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .collect(),
            allow_replay: true,
        }
    }

    fn header<'a>(params: &'a RequestParams<'_>, name: &str) -> Option<&'a [u8]> {
        params
            .extra_headers
            .iter()
            .find(|(n, _)| n.ascii_eq_ignore_case(name.as_bytes()))
            .map(|(_, v)| v.as_slice())
    }

    #[test]
    fn cross_origin_redirect_strips_credentials() {
        let mut state = RedirectState::new(&params_with(&[
            ("Authorization", "Bearer s3cret"),
            ("Proxy-Authorization", "Basic dXNlcjpwYXNz"),
            ("Cookie", "session=abc"),
            ("X-Custom", "kept"),
        ]));
        state.strip_cross_origin_headers();
        let p = state.to_params();
        assert_eq!(header(&p, "Authorization"), None);
        assert_eq!(header(&p, "Proxy-Authorization"), None);
        assert_eq!(header(&p, "Cookie"), None);
        assert_eq!(header(&p, "Cookie2"), None);
        assert_eq!(header(&p, "X-Custom"), Some(&b"kept"[..]));
        assert_eq!(header(&p, "Host"), None);
    }

    #[test]
    fn same_origin_redirect_keeps_credentials() {
        let state = RedirectState::new(&params_with(&[
            ("Authorization", "Bearer s3cret"),
            ("Cookie", "session=abc"),
        ]));
        let p = state.to_params();
        assert_eq!(header(&p, "Authorization"), Some(&b"Bearer s3cret"[..]));
        assert_eq!(header(&p, "Cookie"), Some(&b"session=abc"[..]));
    }

    fn origin(text: &[u8]) -> Origin {
        Origin::from_url(&Url::parse(text).expect("test urls parse"))
    }

    /// The point of returning a decision: a refused downgrade is refused
    /// while deciding, so nothing has been dialled by the time it is known.
    #[test]
    fn a_downgrade_is_refused_rather_than_reported_as_a_hop() {
        let mut state = RedirectState::new(&params_with(&[]));
        let error = state
            .advance(
                &origin(b"https://secure.example/"),
                StatusCode::FOUND,
                b"http://secure.example/",
            )
            .expect_err("https must not silently become http");
        assert_eq!(error, Error::Connection(ConnectionError::InsecureRedirect));
    }

    #[test]
    fn an_upgrade_to_https_is_a_reconnect() {
        let mut state = RedirectState::new(&params_with(&[]));
        let hop = state
            .advance(
                &origin(b"http://plain.example/"),
                StatusCode::FOUND,
                b"https://plain.example/",
            )
            .expect("an upgrade is allowed");
        assert_eq!(hop, Hop::Reconnect(b"https://plain.example/".to_vec()));
    }

    #[test]
    fn an_absolute_location_on_the_same_origin_reuses_the_connection() {
        let mut state = RedirectState::new(&params_with(&[("Authorization", "Bearer s3cret")]));
        let hop = state
            .advance(
                &origin(b"https://api.example/"),
                StatusCode::FOUND,
                b"https://api.example/v2/thing",
            )
            .expect("same origin");
        assert_eq!(hop, Hop::SameOrigin);
        assert_eq!(state.path, b"/v2/thing");
        assert_eq!(
            header(&state.to_params(), "Authorization"),
            Some(&b"Bearer s3cret"[..])
        );
    }

    #[test]
    fn a_cross_origin_hop_strips_credentials_before_it_is_reported() {
        let mut state = RedirectState::new(&params_with(&[
            ("Authorization", "Bearer s3cret"),
            ("X-Custom", "kept"),
        ]));
        let hop = state
            .advance(
                &origin(b"https://api.example/"),
                StatusCode::FOUND,
                b"https://evil.example/steal",
            )
            .expect("cross origin is allowed, just not with credentials");
        assert_eq!(hop, Hop::Reconnect(b"https://evil.example/steal".to_vec()));
        let params = state.to_params();
        assert_eq!(header(&params, "Authorization"), None);
        assert_eq!(header(&params, "X-Custom"), Some(&b"kept"[..]));
    }

    #[test]
    fn a_relative_location_never_needs_a_reconnect() {
        let mut state = RedirectState::new(&params_with(&[]));
        let hop = state
            .advance(
                &origin(b"https://api.example/"),
                StatusCode::FOUND,
                b"/elsewhere?q=1",
            )
            .expect("relative");
        assert_eq!(hop, Hop::SameOrigin);
        assert_eq!(state.path, b"/elsewhere");
        assert_eq!(state.query.as_deref(), Some(&b"q=1"[..]));
    }

    /// A protocol-relative location inherits the current scheme, so from
    /// HTTPS it must not resolve to a plaintext hop.
    #[test]
    fn a_protocol_relative_location_inherits_the_current_scheme() {
        let mut state = RedirectState::new(&params_with(&[]));
        let hop = state
            .advance(
                &origin(b"https://api.example/"),
                StatusCode::FOUND,
                b"//other.example/path",
            )
            .expect("protocol-relative");
        assert_eq!(hop, Hop::Reconnect(b"https://other.example/path".to_vec()));
    }

    #[test]
    fn a_non_redirect_status_has_no_location_to_follow() {
        assert!(
            !RedirectState::is_followed_status(StatusCode::OK),
            "200 is the answer, not a hop"
        );
        assert!(RedirectState::is_followed_status(StatusCode::FOUND));
    }

    #[test]
    fn method_and_body_drop_on_301() {
        let mut state = RedirectState::new(&RequestParams {
            method: Method::Post,
            path: b"/submit",
            query: Some(b"a=1"),
            body: Some(b"payload"),
            extra_headers: Vec::new(),
            allow_replay: false,
        });
        state.apply_method_redirect(StatusCode::MOVED_PERMANENTLY);
        let p = state.to_params();
        assert_eq!(p.method, Method::Get);
        assert_eq!(p.body, None);
        assert_eq!(p.path, b"/submit");
    }
}
