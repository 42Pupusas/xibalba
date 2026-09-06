# Changelog

Notable changes per release. Versions follow [semver](https://semver.org);
pre-1.0, a minor bump may break API.

## [Unreleased]

`xibalba-proto` 0.3.0 · `xibalba-client` 0.4.0

The first release after a security and correctness audit of the protocol
layer. It closes request-smuggling and header-injection gaps, makes several
hangs surface as errors, and speeds up header parsing. Both crates break API.

`xibalba-iouring` is not published; it is a workspace-internal experiment.

### Breaking: behavior that changes silently

These compile without error and return different results. Check them first.

- **Redirects that downgrade https to http are refused.**
  A `Location` of `http://...` from an https origin now fails with
  `ConnectionError::InsecureRedirect` instead of being followed onto a
  plaintext connection. The target is no longer contacted at all.
- **Non-idempotent requests are no longer retried automatically.**
  A POST or PATCH whose response is lost to an ambiguous transport failure
  surfaces the error rather than being resent, since the server may already
  have applied it. Opt back in per request with
  `RequestBuilder::allow_replay(true)` where the endpoint is idempotent by
  construction. GET and the other replay-eligible methods are unchanged.
- **The redirect hop budget is checked before the next hop is contacted.**
  With `max_redirects = 0` the client previously opened a connection to the
  redirect target and then discarded it; it now returns `TooManyRedirects`
  without connecting.
- **Relative redirects resolve dot segments.** `/a/b` + `../c` now requests
  `/c`, previously `/a/../c`.
- **A `Location` with an uppercase scheme is treated as absolute.**
  `HTTPS://host/x` was previously handled as a relative path.
- **Rewriting POST to GET drops representation headers.** `Content-Type`,
  `Content-Encoding`, `Content-Language` and `Content-Location` no longer
  survive onto the bodyless GET.
- **Truncated response heads report `ParseError::Incomplete`.** A partial
  header name previously returned `MissingColon`, and a head split inside its
  final CRLF returned `InvalidHeaderName`.
- **Chunk extensions and trailers are bounded and validated.** Metadata past
  `MAX_CHUNK_EXTENSION`/`MAX_TRAILER_SECTION`, or containing C0 controls or
  DEL, now fails with `ParseError::InvalidChunkMetadata`.

- **`HeaderName` comparison is now case-insensitive for unrecognized names.**
  `HeaderName::from_bytes(b"X-Custom") == HeaderName::from_bytes(b"x-custom")`
  was `false` and is now `true`. HTTP field names are case-insensitive, so the
  old exact-byte comparison for unknown names was wrong and made lookups miss
  headers depending on how a server cased them. The `Unknown`/`Raw` internal
  split that caused it is gone. Original bytes are still preserved for
  rendering; only comparison and hashing changed.
- **`Url::host` keeps the brackets on IPv6 literals.**
  `Url::parse(b"http://[::1]/")` now yields host `[::1]`, previously `::1`.
  The bracketed form is what belongs in a `Host` header. Code passing
  `url.host` straight to a resolver must strip the brackets itself.

### Breaking: API

- `ConnectionError`, `SerializeError`, `ParseError`, `UrlError` and `Error` are
  now `#[non_exhaustive]`, so matches on them need a wildcard arm. This is a
  one-time break: later variants become non-breaking additions. `IoError` and
  `TlsError` stay exhaustive — `Connector` implementors construct them.
- New variants: `ConnectionError::{TooManyInterimResponses, InfiniteReadTimeout}`,
  `SerializeError::{InvalidPath, InvalidHeader, DuplicateHeader}`.
- `xibalba_proto::header::Headers` removed. It was an unused view type over a
  header buffer; parsed headers are reached through `ResponseHead` and
  `HeadData::headers`.
- `xibalba-client` items moved as the client and body god-files were split.
  Re-exported at the crate root, so `use xibalba_client::X` keeps working:
  - `body::HeadData` → `response::HeadData`
  - `body::HEAD_BUF_SIZE` → `config::HEAD_BUF_SIZE`
  - `client::{Config, DEFAULT_MAX_HEAD_SIZE}` → `config::`
  - `client::RequestBuilder` → `params::`
  - `client::{Response, StreamingResponse}` → `response::`
- `xibalba_client::body::MAX_HEADERS` is no longer re-exported; use
  `xibalba_proto::response::MAX_HEADERS`.
- `HeaderRange` offsets and lengths widened from `u16` to `u32`, with the new
  `MAX_ADDRESSABLE_HEAD` naming the ceiling. A `MAX_HEAD_SIZE` above 64 KiB
  previously failed with `HeaderRangeOverflow` whenever a header sat past that
  point, so the documented 128 KiB example could not be used as written. Code
  reading these fields needs `as usize` rather than `usize::from`.
- `StreamHandle::into_consumer` is now `into_stream`, returning a
  `ChunkStream`. The bare ring consumer bypassed the liveness guard that lets
  the reader stop producing when a caller drops its handle.
- New variants: `ConnectionError::{TooManyRequests, InsecureRedirect}`,
  `ParseError::InvalidChunkMetadata`.
- `AsyncClient::submit` returns `ConnectionError::TooManyRequests` once
  `DEFAULT_MAX_OUTSTANDING` requests are in flight, and no longer blocks when
  the control ring is full. This is backpressure, not a transport failure.

### Security

- Reject CRLF injection in request targets and header values, and reject
  duplicate or client-managed headers (`Host`, `Content-Length`,
  `Transfer-Encoding`), closing request-smuggling vectors.
- Reject userinfo (`user:pass@host`) in URLs, which could redirect a request
  to an unintended host.
- Validate host bytes, rejecting spaces, CTLs, NUL and non-ASCII.
- Strip credentials on cross-origin redirects; keep them same-origin.
- Reject bare LF inside a header value instead of accepting it as a line
  terminator — another smuggling difference between intermediaries.
- Refuse to reuse a connection after a partial or oversized response head,
  which could otherwise desync a keep-alive session.

### Fixed

- Malformed responses surface as errors instead of hanging `recv` (io_uring).
- Pipelined bytes are preserved when a body completes in a later feed.
- Interim 1xx responses are consumed from the buffer without another read, and
  an unbounded run of them is now an error.
- Duplicate request headers report an error rather than panicking.
- Error-body drains and queued async requests are cancellable; a queued
  request can no longer reach the wire after cancellation.
- Redirect targets and methods follow RFC 9110.
- An infinite read timeout is rejected: silence budgets and async cancellation
  both need a finite one to regain control.

### Performance

- Header-value parsing no longer walks each value three times. Locating the
  terminating CR and rejecting control bytes are one pass (CR is itself a
  control byte), and the trailing-whitespace trim is skipped unless the last
  byte is whitespace. Typical 6-header heads 183.7 → 124.9 ns (1.47x), heavy
  22-header heads 588.4 → 376.7 ns (1.56x).

### Documentation

- The TLS-agnostic contract is documented on `Connector` and enforced by tests:
  the client links no TLS or crypto crate and exposes no feature selecting one.
  Note that rustls resolves a process-wide default provider in
  `ClientConfig::builder()`, which this crate cannot prevent.
- `BENCHMARKS.md` records measured comparisons against `ureq`, `httparse`,
  `http` and `url`, including the cases xibalba loses and the caveats that make
  some comparisons not like-for-like.
