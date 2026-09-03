use xibalba_proto::error::Error;
use xibalba_proto::method::Method;

use crate::client::Client;
use crate::connector::Connector;
use crate::response::Response;

/// Wire-level request data: what `Client` serializes for one dispatch.
#[derive(Debug)]
pub(crate) struct RequestParams<'a> {
    pub(crate) method: Method,
    pub(crate) path: &'a [u8],
    pub(crate) query: Option<&'a [u8]>,
    pub(crate) body: Option<&'a [u8]>,
    /// Owned header bytes: redirects splice in a new `Host` borrowed from
    /// the `Location` header, so extra headers cannot share one lifetime
    /// with the original request data.
    pub(crate) extra_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

impl RequestParams<'_> {
    /// Header bytes the client itself writes; duplicates would produce a
    /// malformed or misleading request. `Cookie` and `Cookie2` are exempt:
    /// multiple cookie headers are legal and common.
    pub(crate) fn is_client_managed(name: &[u8]) -> bool {
        use xibalba_proto::bytes::ByteSliceExt;
        name.ascii_eq_ignore_case(b"Host")
            || name.ascii_eq_ignore_case(b"Content-Length")
            || name.ascii_eq_ignore_case(b"Transfer-Encoding")
    }
}

/// Collects request parameters for deferred execution.
///
/// Created by [`Client::build`]; consumed by [`Client::send`] or
/// [`Client::send_streaming`].  The builder borrows only the request
/// data (path, headers, body), never the client.
#[derive(Debug)]
pub struct RequestBuilder<'a> {
    method: Method,
    path: &'a [u8],
    query: Option<&'a [u8]>,
    body: Option<&'a [u8]>,
    extra_headers: Vec<(&'a [u8], &'a [u8])>,
}

impl<'a> RequestBuilder<'a> {
    pub(crate) const fn new(method: Method, path: &'a [u8]) -> Self {
        Self {
            method,
            path,
            query: None,
            body: None,
            extra_headers: Vec::new(),
        }
    }

    /// Attach a header to the request.
    ///
    /// # Panics
    ///
    /// Panics if `name` duplicates a header the client writes itself
    /// (`Host`, `Content-Length`, `Transfer-Encoding`) or duplicates an
    /// earlier call. Multiple `Cookie` headers are allowed.
    #[must_use]
    #[track_caller]
    pub fn header(mut self, name: &'a [u8], value: &'a [u8]) -> Self {
        assert!(
            !RequestParams::is_client_managed(name),
            "duplicate managed header: {}",
            String::from_utf8_lossy(name),
        );
        assert!(
            !self
                .extra_headers
                .iter()
                .any(|(existing, _)| names_match(existing, name)),
            "duplicate header: {}",
            String::from_utf8_lossy(name),
        );
        self.extra_headers.push((name, value));
        self
    }

    #[must_use]
    pub const fn body(mut self, data: &'a [u8]) -> Self {
        self.body = Some(data);
        self
    }

    #[must_use]
    pub const fn query(mut self, q: &'a [u8]) -> Self {
        self.query = Some(q);
        self
    }

    /// Execute this request on `client` and return the full response.
    ///
    /// # Errors
    ///
    /// Returns `Error` on serialization or connection failure.
    pub fn send<C: Connector, const MAX_HEAD_SIZE: usize>(
        self,
        client: &mut Client<C, MAX_HEAD_SIZE>,
    ) -> Result<Response, Error> {
        client.send(self)
    }

    /// Materialize the collected parameters into [`RequestParams`].
    pub(crate) fn into_params(self) -> RequestParams<'a> {
        RequestParams {
            method: self.method,
            path: self.path,
            query: self.query,
            body: self.body,
            extra_headers: self
                .extra_headers
                .iter()
                .map(|(n, v)| (n.to_vec(), v.to_vec()))
                .collect(),
        }
    }
}

/// Case-insensitive header-name comparison on raw bytes.
fn names_match(a: &[u8], b: &[u8]) -> bool {
    use xibalba_proto::bytes::ByteSliceExt;
    a.ascii_eq_ignore_case(b)
}
