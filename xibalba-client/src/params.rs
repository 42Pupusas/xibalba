use xibalba_proto::error::{Error, SerializeError};
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
    /// Whether this request may be resent after an ambiguous transport
    /// failure. The client resends automatically only when the method is
    /// replay-eligible and this is set; see [`Method::is_replay_eligible`].
    pub(crate) allow_replay: bool,
}

impl RequestParams<'_> {
    /// Header bytes the client serializes itself; duplicates would produce a
    /// malformed or misleading request. `Cookie` and `Cookie2` are exempt:
    /// multiple cookie headers are legal and common.
    pub(crate) fn is_managed(name: &[u8]) -> bool {
        use xibalba_proto::bytes::ByteSliceExt;
        name.ascii_eq_ignore_case(b"Host")
            || name.ascii_eq_ignore_case(b"Content-Length")
            || name.ascii_eq_ignore_case(b"Transfer-Encoding")
    }

    /// Headers that may legitimately appear several times in one
    /// request; duplicates of any other name are a caller bug.
    fn is_repeatable(name: &[u8]) -> bool {
        use xibalba_proto::bytes::ByteSliceExt;
        name.ascii_eq_ignore_case(b"Cookie") || name.ascii_eq_ignore_case(b"Cookie2")
    }

    /// Reject headers the client serializes itself and duplicate names
    /// before any byte is written. [`RequestBuilder::into_params`] and
    /// [`AsyncClient::submit`](crate::async_client::AsyncClient::submit)
    /// both funnel request headers through this check: a builder cannot
    /// carry the error through its infallible chain, so it surfaces at
    /// the fallible boundary instead.
    pub(crate) fn validate_extra_headers(
        extra_headers: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), SerializeError> {
        for (name, _) in extra_headers {
            if Self::is_managed(name) {
                return Err(SerializeError::DuplicateHeader);
            }
            if extra_headers
                .iter()
                .filter(|(other, _)| Self::names_match(other, name))
                .count()
                > 1
                && !Self::is_repeatable(name)
            {
                return Err(SerializeError::DuplicateHeader);
            }
        }
        Ok(())
    }

    fn names_match(a: &[u8], b: &[u8]) -> bool {
        use xibalba_proto::bytes::ByteSliceExt;
        a.ascii_eq_ignore_case(b)
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
    allow_replay: bool,
}

impl<'a> RequestBuilder<'a> {
    pub(crate) const fn new(method: Method, path: &'a [u8]) -> Self {
        Self {
            method,
            path,
            query: None,
            body: None,
            extra_headers: Vec::new(),
            allow_replay: method.is_replay_eligible(),
        }
    }

    /// Allow this request to be resent after an ambiguous transport
    /// failure even when its method is not replay-eligible.
    ///
    /// A resend can repeat a side effect the server already applied but
    /// whose response was lost. Set this only when the endpoint is
    /// idempotent by construction, such as one keyed by an
    /// caller-supplied idempotency token.
    #[must_use]
    pub const fn allow_replay(mut self, allow: bool) -> Self {
        self.allow_replay = allow;
        self
    }

    /// Attach a header to the request. Multiple `Cookie` headers are
    /// allowed.
    ///
    /// The builder cannot fail per call — a rejected header is reported
    /// when the builder is sent, as [`Error::Serialize`] with
    /// [`SerializeError::DuplicateHeader`] (headers the client serializes
    /// itself: `Host`, `Content-Length`, `Transfer-Encoding`, or a
    /// duplicate name).
    #[must_use]
    pub fn header(mut self, name: &'a [u8], value: &'a [u8]) -> Self {
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
    /// Returns `Error` on serialization, duplicate-header, or connection
    /// failure.
    pub fn send<C: Connector, const MAX_HEAD_SIZE: usize>(
        self,
        client: &mut Client<C, MAX_HEAD_SIZE>,
    ) -> Result<Response, Error> {
        client.send(self)
    }

    /// Materialize the collected parameters into [`RequestParams`].
    ///
    /// # Errors
    ///
    /// Returns [`SerializeError::DuplicateHeader`] when the builder
    /// carries a header the client serializes itself (`Host`,
    /// `Content-Length`, `Transfer-Encoding`) or the same header name
    /// twice.
    pub(crate) fn into_params(self) -> Result<RequestParams<'a>, Error> {
        let params = RequestParams {
            method: self.method,
            path: self.path,
            query: self.query,
            body: self.body,
            extra_headers: self
                .extra_headers
                .iter()
                .map(|(n, v)| (n.to_vec(), v.to_vec()))
                .collect(),
            allow_replay: self.allow_replay,
        };
        RequestParams::validate_extra_headers(&params.extra_headers)?;
        Ok(params)
    }
}
