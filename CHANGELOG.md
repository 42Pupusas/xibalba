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
- **`ChunkedDecoder` delivers decoded body bytes before reporting an error.**
  When a malformed chunk followed valid data inside one `decode` call, the
  error was returned and the already-decoded bytes in the output buffer were
  dropped; feeding the same stream in smaller slices delivered them. The
  decoder now returns the data, then reports the error on the next call and
  on every call after it. Callers that treat `Data` as "body so far" and stop
  at `Error` are unaffected; a caller that assumed `Error` meant no output was
  written will now see bytes. Found by fuzzing.
- **Chunk extensions and trailers are bounded and validated.** Metadata past
  `MAX_CHUNK_EXTENSION`/`MAX_TRAILER_SECTION`, or containing C0 controls or
  DEL, now fails with `ParseError::InvalidChunkMetadata`.
- **Undecodable transfer codings are refused instead of framed.**
  `Transfer-Encoding: gzip` and `gzip, chunked` previously produced a body —
  the latter dechunked but still compressed, with nothing marking it as
  encoded. Both now fail with `ParseError::UnsupportedTransferCoding`.
  `chunked` must be the final coding and may not repeat, so `chunked, gzip`
  and a doubled `chunked` fail with `ParseError::InvalidTransferEncoding`.
  `identity` is accepted and applies no encoding.
- **The reason phrase is validated.** A phrase containing NUL or another C0
  control now fails with `ParseError::InvalidReasonPhrase`; HTAB and obs-text
  remain accepted.
- **`Config::validate` rejects zero durations and oversized read timeouts.**
  A zero read timeout makes every read tick instantly and a zero budget is
  spent before the first read returns; a read timeout longer than a silence
  budget lets one blocked read overshoot the budget it subdivides. Both now
  fail at `connect` with `ConnectionError::ZeroDuration` or
  `TimeoutExceedsBudget` instead of failing every request at runtime.
- **`TimedOut` is accepted as a read-timeout tick alongside `WouldBlock`.**
  Only `WouldBlock` was absorbed, which is a Linux detail; a connector or
  platform reporting `TimedOut` ended the request on the first tick, cutting
  the silence budget to a single `read_timeout`.
- **A cancel during a request write or response head now aborts it.**
  Cancellation covered body reads only, so a head that never arrived pinned
  the request for the whole `head_silence` budget and `cancel`/`drop` waited
  it out. The cancel is reported as `Chunk::Aborted`, matching the body path.

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
  `url.host` straight to a resolver must use `Url::connection_host` instead,
  which returns the address without brackets.

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
  - `client::{Response, StreamingResponse}` → `response::`
- `xibalba_client::body::MAX_HEADERS` is no longer re-exported; use
  `xibalba_proto::response::MAX_HEADERS`.
- **Modules carrying no usable public API are now private:** `control`,
  `redirect`, `params`, and `admission`. `RequestBuilder` (from `params`) and
  `DEFAULT_MAX_OUTSTANDING` (from `admission`) are unaffected — both are
  reached through the crate root, which is how the documentation always
  spelled them. `control` and `redirect` exported no public item at all.
- `AsyncRequest`, `Admission` and `Permit` are no longer public. All three are
  internal to `AsyncClient`: `AsyncRequest` had no public constructor and no
  public accessor, and the admission bound is fixed at
  `DEFAULT_MAX_OUTSTANDING`, so no caller could construct or configure one.
  `AsyncClient::outstanding()` remains the way to read the current count.
- `HeaderRange` offsets and lengths widened from `u16` to `u32`, with the new
  `MAX_ADDRESSABLE_HEAD` naming the ceiling. A `MAX_HEAD_SIZE` above 64 KiB
  previously failed with `HeaderRangeOverflow` whenever a header sat past that
  point, so the documented 128 KiB example could not be used as written. Code
  reading these fields needs `as usize` rather than `usize::from`.
- `StreamHandle::into_consumer` is now `into_stream`, returning a
  `ChunkStream`. The bare ring consumer bypassed the liveness guard that lets
  the reader stop producing when a caller drops its handle.
- New variants: `ConnectionError::{TooManyRequests, InsecureRedirect}`,
  `ParseError::{InvalidChunkMetadata, InvalidReasonPhrase,
  InvalidTransferEncoding, UnsupportedTransferCoding}`.
- New `xibalba_proto::coding` module with `TransferCodings` and
  `TransferCoding`, which own `Transfer-Encoding` list parsing.
- `SetReadTimeout` gains `set_write_timeout`, defaulted to a no-op so existing
  connectors keep compiling. A peer that stops reading blocks `write_all`
  inside one syscall, where no cancellation check is reached; a connector that
  leaves the default in place is declaring its writes cannot block
  indefinitely. `Config::write_timeout` sets the value (30 s by default).
- New `xibalba_client::interrupt` module with `Interrupt`,
  `InterruptibleStream`, `NeverCancelled`, `Cancelled`, and `Latch`, the seam
  through which a caller that can cancel tells the client to stop between I/O
  calls. Cancellation is marked by the `Cancelled` payload rather than
  `ErrorKind::Interrupted`, which `write_all` and `read_exact` retry
  internally — a cancelled `write_all` previously never returned.
- `SetReadTimeout` documents the blocking and error contract an implementor
  must satisfy: blocking mode, a tick reported as `WouldBlock` or `TimedOut`,
  and `Interrupted` reserved for genuine signals.
- New `xibalba_client::dial` module with `TcpDialer`, the TCP half of a
  production connector: it resolves via `Url::connection_host`, tries every
  resolved address rather than the first, bounds the connect with a timeout
  (`TcpStream::connect` otherwise blocks for the OS SYN timeout, past any
  client-level budget), and sets `TCP_NODELAY`. `dial_plaintext` additionally
  refuses HTTPS instead of sending the request in the clear. It is std-only,
  so the crate stays TLS-agnostic.
- New `xibalba_proto::url::Url::connection_host`, the host with IPv6 brackets
  removed. `Url::host` keeps them for the `Host` header, but `ToSocketAddrs`
  and rustls' `ServerName` both reject `[::1]`, so a connector using
  `Url::host` fails on every IPv6 URL.
- New variant `ConnectionError::PlaintextConnectorForHttps`.
- New `PlainConnector` and `PlainStream`: a cleartext-only `Connector` for
  `http://` URLs, refusing `https://` rather than connecting in the clear.
  Four private copies of it existed across the tests, benches and examples;
  this is the one implementation. HTTPS still requires a caller-supplied
  connector, and the crate still links no TLS stack.
- **`RequestBuilder::send(&mut client)` is removed.** It only called
  `client.send(builder)`, which remains and is the one way to dispatch a
  built request. The convenience was what made the builder depend on the
  client that constructs it.
- New `Origin` type (scheme + host + port) with `covers`, `downgrades_to`,
  `host_header_value` and `root_url`. `Client` holds one instead of three
  separate fields.
- **`BodyFraming::from_response` no longer takes a `header_count`.** It sliced
  `headers[..header_count]` and panicked on an oversized count despite
  returning `Result`. The slice carries its own length; callers holding a
  larger buffer pass `&buf[..head.header_count]` or the new
  `ResponseHead::headers`, which narrows a buffer by the count it parsed and
  returns `TooManyHeaders` when the two disagree.
- **`HeadData`'s fields are private, reached through accessors.** `version`,
  `status`, `header_count` and `as_bytes` replace direct field access, and
  `HeadData::new` returns `Result`. The buffer, the ranges into it, and the
  live-range count are one invariant; public mutability let them disagree.
  `headers()` no longer silently skips a range that falls outside the buffer,
  which turned a corrupt head into a quietly missing header — `new` rejects it
  instead.

### Removed

- `xibalba_client::async_client::CancellableStream`. It wrapped reads alone and
  duplicated the control-channel poll now performed by the request's single
  latched interrupt, which also covers writes and the response head.
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
