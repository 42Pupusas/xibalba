use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::method::Method;
use xibalba_proto::status::StatusCode;
use xibalba_proto::url::Url;

use crate::client::Client;
use crate::connector::Connector;
use crate::params::RequestParams;
use crate::reference::UriReference;
use crate::response::Response;
use xibalba_proto::scheme::Scheme;

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

    /// Execute a fully-buffered request, following redirects up to the
    /// configured `max_redirects` value.
    pub(crate) fn follow<C: Connector, const MAX_HEAD_SIZE: usize>(
        client: &mut Client<C, MAX_HEAD_SIZE>,
        params: &RequestParams<'_>,
    ) -> Result<Response, Error> {
        let mut current = Self::new(params);
        let mut hops_left = client.config.max_redirects;

        loop {
            let resp = client.send_one(&current.to_params())?;

            if !Self::is_followed_status(resp.status) {
                return Ok(resp);
            }

            let location = resp
                .headers()
                .find(|(name, _)| name.ascii_eq_ignore_case(b"Location"))
                .map(|(_, v)| v);

            let location = match location {
                Some(loc) => loc.to_vec(),
                None => return Ok(resp),
            };

            // Check the budget before applying the Location. Applying it first
            // opens a connection to the next hop -- possibly cross-origin --
            // only to discard it, which with max_redirects = 0 contacts a host
            // the caller never agreed to reach.
            if hops_left == 0 {
                return Err(ConnectionError::TooManyRedirects.into());
            }
            hops_left -= 1;

            current.apply_method_redirect(resp.status);
            current.apply_location(client, &location)?;
        }
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

    fn apply_location<C: Connector, const MAX_HEAD_SIZE: usize>(
        &mut self,
        client: &mut Client<C, MAX_HEAD_SIZE>,
        location: &[u8],
    ) -> Result<(), Error> {
        let without_fragment = location
            .iter()
            .position(|&b| b == b'#')
            .map_or(location, |pos| &location[..pos]);

        if Self::has_absolute_scheme(without_fragment) {
            return self.apply_absolute_location(client, without_fragment);
        }
        if without_fragment.starts_with(b"//") {
            let mut absolute = client.scheme_bytes().to_vec();
            absolute.push(b':');
            absolute.extend_from_slice(without_fragment);
            return self.apply_absolute_location(client, &absolute);
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
            return Ok(());
        }

        self.path = if path_part.starts_with(b"/") {
            UriReference::remove_dot_segments(path_part)
        } else {
            self.resolve_relative_path(path_part)
        };
        self.query = query;
        Ok(())
    }

    fn apply_absolute_location<C: Connector, const MAX_HEAD_SIZE: usize>(
        &mut self,
        client: &mut Client<C, MAX_HEAD_SIZE>,
        location: &[u8],
    ) -> Result<(), Error> {
        let url = Url::parse(location)?;
        // Refuse before reconnecting: a downgrade must not be detected by
        // observing that we already opened a plaintext connection.
        if client.scheme() == Scheme::Https && url.scheme == Scheme::Http {
            return Err(ConnectionError::InsecureRedirect.into());
        }
        if !client.is_same_origin(&url) {
            client.reconnect(&url)?;
            self.strip_cross_origin_headers();
        }
        self.path = if url.path.is_empty() {
            b"/".to_vec()
        } else {
            UriReference::remove_dot_segments(url.path)
        };
        self.query = url.query.map(<[u8]>::to_vec);
        Ok(())
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
