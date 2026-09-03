use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::error::{ConnectionError, Error};
use xibalba_proto::method::Method;
use xibalba_proto::status::StatusCode;
use xibalba_proto::url::Url;

use crate::client::Client;
use crate::connector::Connector;
use crate::params::RequestParams;
use crate::response::Response;

/// Per-request state carried across redirect hops.
#[derive(Debug)]
pub(crate) struct RedirectState {
    method: Method,
    path: Vec<u8>,
    query: Option<Vec<u8>>,
    body: Option<Vec<u8>>,
    extra_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

impl RedirectState {
    pub(crate) fn new(params: &RequestParams<'_>) -> Self {
        Self {
            method: params.method,
            path: params.path.to_vec(),
            query: params.query.map(<[u8]>::to_vec),
            body: params.body.map(<[u8]>::to_vec),
            extra_headers: params.extra_headers.clone(),
        }
    }

    fn to_params(&self) -> RequestParams<'_> {
        RequestParams {
            method: self.method,
            path: &self.path,
            query: self.query.as_deref(),
            body: self.body.as_deref(),
            extra_headers: self.extra_headers.clone(),
        }
    }

    /// Drop credentials-bearing headers when a redirect leaves the origin.
    /// `Authorization` and the proxy variant are stripped outright; `Cookie`
    /// is replaced with a `Host`-scoped placeholder so the target origin
    /// never receives another origin's cookies.
    fn retarget_headers(&mut self, new_host: Option<&[u8]>) {
        let Some(new_host) = new_host else {
            return;
        };
        let new_host = new_host.to_vec();
        self.extra_headers.retain(|(name, _)| {
            !(name.ascii_eq_ignore_case(b"Authorization")
                || name.ascii_eq_ignore_case(b"Proxy-Authorization")
                || name.ascii_eq_ignore_case(b"Cookie")
                || name.ascii_eq_ignore_case(b"Cookie2"))
        });
        self.extra_headers.push((b"Host".to_vec(), new_host));
    }

    /// Execute a fully-buffered request, following redirects up to the
    /// configured `max_redirects` value.
    pub(crate) fn follow<C: Connector, const MAX_HEAD_SIZE: usize>(
        client: &mut Client<C, MAX_HEAD_SIZE>,
        params: &RequestParams<'_>,
    ) -> Result<Response, Error> {
        let mut current = Self::new(params);

        for _ in 0..=client.config.max_redirects {
            let resp = client.send_one(&current.to_params())?;

            if !resp.status.is_redirect() {
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

            if !Self::is_method_preserving(resp.status) {
                current.method = Method::Get;
                current.body = None;
            }

            current.apply_location(client, &location)?;
        }

        Err(ConnectionError::TooManyRedirects.into())
    }

    const fn is_method_preserving(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
        )
    }

    fn apply_location<C: Connector, const MAX_HEAD_SIZE: usize>(
        &mut self,
        client: &mut Client<C, MAX_HEAD_SIZE>,
        location: &[u8],
    ) -> Result<(), Error> {
        if location.starts_with(b"http://") || location.starts_with(b"https://") {
            let url = Url::parse(location)?;
            if client.is_same_origin(&url) {
                self.retarget_headers(None);
            } else {
                client.reconnect(&url)?;
                self.retarget_headers(Some(url.host));
            }
            self.path = normalize_path(url.path);
            self.query = url.query.map(<[u8]>::to_vec);
        } else {
            let (path_part, query_part) = location
                .iter()
                .position(|&b| b == b'?')
                .map_or((location, None), |pos| {
                    (&location[..pos], Some(location[pos + 1..].to_vec()))
                });
            self.path = normalize_path(path_part);
            self.query = query_part;
        }
        Ok(())
    }
}

fn normalize_path(path: &[u8]) -> Vec<u8> {
    if path.is_empty() {
        b"/".to_vec()
    } else {
        path.to_vec()
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
        state.retarget_headers(Some(b"other.example"));
        let p = state.to_params();
        assert_eq!(header(&p, "Authorization"), None);
        assert_eq!(header(&p, "Proxy-Authorization"), None);
        assert_eq!(header(&p, "Cookie"), None);
        assert_eq!(header(&p, "Cookie2"), None);
        assert_eq!(header(&p, "X-Custom"), Some(&b"kept"[..]));
        assert_eq!(header(&p, "Host"), Some(&b"other.example"[..]));
    }

    #[test]
    fn same_origin_redirect_keeps_credentials() {
        let mut state = RedirectState::new(&params_with(&[
            ("Authorization", "Bearer s3cret"),
            ("Cookie", "session=abc"),
        ]));
        state.retarget_headers(None);
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
        });
        assert!(RedirectState::is_method_preserving(
            StatusCode::TEMPORARY_REDIRECT
        ));
        assert!(RedirectState::is_method_preserving(
            StatusCode::PERMANENT_REDIRECT
        ));
        assert!(!RedirectState::is_method_preserving(
            StatusCode::MOVED_PERMANENTLY
        ));
        assert!(!RedirectState::is_method_preserving(StatusCode::FOUND));

        if !RedirectState::is_method_preserving(StatusCode::MOVED_PERMANENTLY) {
            state.method = Method::Get;
            state.body = None;
        }
        let p = state.to_params();
        assert_eq!(p.method, Method::Get);
        assert_eq!(p.body, None);
        assert_eq!(p.path, b"/submit");
    }
}
