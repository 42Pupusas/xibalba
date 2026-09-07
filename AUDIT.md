# Core and standard TCP client audit

## Scope and evidence

Audited baseline: `18191e44cb479c8b0e91e8ac1a5054e2115f2b4d`.

Primary scope: every production module in `xibalba-proto` and `xibalba-client`, including the background-reader client because it shares the standard synchronous transport. Reviewed the client integration tests, protocol tests, manifests, README, the TCP/rustls connector example, and benchmark plaintext connector. The experimental `xibalba-iouring` implementation is excluded. TLS provider internals, dependency source audits, live external endpoints, fuzzing, and performance remeasurement are not covered.

This is a source audit plus execution of existing checks, not a claim of exhaustive security verification. Findings below are derived from the named implementation paths; new adversarial regression tests have **not** been implemented or run. Test cases in the action plan are acceptance criteria, not passing reproducers. No production code was changed.

There is no packaged default std TCP connector: `xibalba-client` is connector-generic. The standard TCP path consists of `Client`/`AsyncClient` over application-owned `Read + Write + SetReadTimeout` streams; the in-tree TCP example is therefore part of this review.

### Executed validation

Commands used the absolute workspace manifest path because the environment doctor ran its build probe from `/home/cuarentaydos`, not this repository.

| Check | Result |
|---|---|
| Core/client default debug tests | 269 passed: 185 protocol, 8 client unit, 73 integration, 3 TLS-boundary tests |
| Core/client release tests with `--all-features` | Same 269 passed |
| Core/client `clippy --all-targets --all-features -- -D warnings` | Passed |
| Core/client `fmt -- --check` | Passed |
| Workspace `build --exclude xibalba-iouring --all-targets` | Passed |
| Workspace `test --exclude xibalba-iouring` | Passed; 287 tests including 18 TLS example tests |
| Workspace `clippy --exclude xibalba-iouring --all-targets --all-features -- -D warnings` | Passed |
| `cargo graph --report xibalba-proto` | DAG; 45 edges, 13 skip edges |
| `cargo graph --report xibalba-client` | NOT a DAG according to report; 34 edges, 14 skip edges, 2 dropped back-edges, reported 0 SCC cycles |

Neither primary crate declares Cargo features; there are no independent primary-crate feature modes to test today. Workspace exclusion only removes io_uring as a selected package: benchmark dependencies still pull it into compilation (visible in workspace Clippy output). No io_uring tests or implementation audit were intentionally selected. Use direct `-p xibalba-proto -p xibalba-client` commands for strict dependency isolation from the benchmark package.

MSRV 1.91 is declared and toolchain 1.95 is pinned, but an explicit MSRV run was not performed. The doctor reported system Cargo 1.97.1; do not infer MSRV compliance from these results. Dependency advisories were not checked.

## Priorities

- **P1:** correctness, availability, or security-sensitive behavior to resolve before the next release.
- **P2:** interoperability, bounded-resource guarantees, and public-contract defects.
- **P3:** architecture, test organization, and documentation cleanup.

## Findings and required actions

### A01 — P1: chunk-size digit counter overflows on peer input

**Evidence:** `xibalba-proto/src/response.rs`, `ChunkedDecoder::size_digits` and `read_size_line`.

`size_digits` is a `u8` incremented for every hexadecimal digit. Checked arithmetic on `chunk_size` does not protect that counter: leading zeros do not overflow the size. A size line containing 256 zero digits panics with overflow checks enabled. Without checks, the counter wraps to zero; a following CR/semicolon is incorrectly rejected, and behavior depends on the number of digits modulo 256. The panic can terminate an async reader thread or propagate to a synchronous caller.

**Action:** replace the counter with a boolean recording whether any digit was seen, or a checked bounded counter as part of an explicit size-line limit. Keep checked numeric accumulation. Add 255/256/257/512-leading-zero tests in debug and release, split across all relevant input boundaries. No panic on arbitrary bytes.

### A02 — P1: automatic retry replays non-idempotent requests

**Evidence:** `xibalba-client/src/client.rs`, `send_head`, `is_stale_connection`.

Every method is retried once after selected transport errors if `head_buf` is empty, including POST and PATCH. Absence of a response does not establish that the server did not commit the request. A server can process a payment or append operation and disconnect before returning the response, leading to duplicate side effects. The internal documentation acknowledges at-least-once behavior, but callers have no retry policy control.

**Action:** default automatic replay to eligible idempotent requests; require explicit per-request opt-in for replaying non-idempotent operations. Document indeterminate outcomes on public send APIs. Test a server that records a POST, closes before responding, and counts reconnect/replay attempts. Separately retain stale idle-GET recovery tests.

### A03 — P1: dirty connection protection is bypassed between redirect hops

**Evidence:** `client.rs`, `execute`, `send_one`, `read_full_body`; `redirect.rs`, `RedirectState::follow`.

`ensure_clean()` runs once before the redirect loop. `read_full_body()` can successfully return a body while marking the connection dirty because it is close-delimited or has excess buffered bytes. The next same-origin redirect hop calls `send_one()` without ensuring a clean connection. The safety invariant enforced between top-level requests is therefore absent inside one redirected request. Depending on timing, this sends on a closed connection or risks treating remaining old response bytes as the next head.

**Action:** enforce clean-connection acquisition at the lowest common dispatch boundary, before every hop. Make reconnect state transitions explicit and reset dirty state only after successful replacement/configuration. Test same-origin redirects with close-delimited bodies and extra bytes both buffered and delayed on the old socket.

### A04 — P1: request write/flush failures can leave a partially written connection reusable

**Evidence:** `client.rs`, `send_head_once`.

`write_all` and `flush` use early `?` returns before the response-read error path marks the connection dirty. A write that sends a prefix and then returns an error not handled by the stale retry branch, such as a write timeout, leaves a previously clean client clean. A subsequent request can be appended to the incomplete request on that socket. A failed second retry has the same underlying issue.

**Action:** distinguish preflight serialization errors from transport errors. Mark the connection unusable before the first transport write and clear it only after the entire exchange satisfies reuse rules. Test partial head write, partial body write, flush failure, and retry failure with scripted connectors; the following request must use a fresh connection.

### A05 — P1: async backpressure can indefinitely block cancellation and Drop

**Evidence:** `async_client.rs`, `process_request` body/terminator `chunk_tx.push_block`, `StreamHandle::cancel`, `AsyncClient::cancel`, `Drop`.

The reader blocks when the 32-slot response ring fills. While blocked there, it cannot poll control messages or the shutdown flag. Retaining a response handle without draining it and dropping the client can block forever in `join()`. A full control ring can also block cancellation or Drop in `push_block`, before joining. The comment describing a bounded/best-effort join is not implemented: `join()` has no timeout.

**Action:** use cancellable/close-aware response delivery that services shutdown while waiting for capacity; make shutdown signaling independent of a potentially full request queue. Preserve bounded memory and define terminal-event behavior on cancellation with a full output queue. Add watchdog-bounded tests for a held but undrained handle, full output and control rings, dropped consumers, and shutdown at each delivery stage. Do not test hangs without an external test-process timeout.

### A06 — P1: async cancellation does not cover response heads, writes, or reconnects

**Evidence:** `async_client.rs`, `process_request` calls to `ensure_clean` and `send_head`; `CancellableStream` is only installed for bodies. `client.rs`, `apply_timeouts`; `connector.rs`, `Connector::connect`.

Cancellation/shutdown checks wrap body reads, but response-head reads use the raw stream. A head stall can delay cancellation/Drop for the full head silence budget rather than one read-timeout tick. Request writes, DNS, connects, and lazy TLS handshakes also lack a client-level cancellation/deadline mechanism. A blocked write can wait indefinitely. The default read tick itself is 30 seconds, not a short cancellation interval.

**Action:** define separate queue, connect/handshake, write, head, body-idle, and optional total deadlines; route cancellation through head reads as well as body reads. Add connector capabilities or a documented contract for bounded connect/write operations. Correct public cancellation claims until all stages are bounded. Test cancel during a silent head, blocked request upload, reconnect, and handshake, not only body reads.

### A07 — P1: connection persistence and protocol transitions are not consistently handled

**Evidence:** `response.rs`, `HeadData::read_response`; `client.rs`, `send_one`/`execute_streaming`; `body/streaming.rs`, `StreamingBody::new`/`finish`; `async_client.rs`, non-2xx collector path; proto `BodyFraming::from_response`.

No reuse decision examines `Connection: close` or HTTP/1.0 persistence defaults. Buffered `send_one()` explicitly discards 101 connections, but streaming and async paths can mark a 101 with no buffered tail clean and issue a later HTTP request on an upgraded connection. `Method::Connect` is exposed, yet framing knows only whether the method is HEAD; successful CONNECT is interpreted as an ordinary response body, potentially reading or waiting on tunnel data.

**Action:** derive a reusable/close/upgraded/tunnel disposition together with framing, shared by all client paths. Honor connection tokens and HTTP version. Reject unsupported upgrades/tunneling explicitly or provide a transport takeover API; never return those connections to HTTP reuse. Test buffered, streaming, and async 101 with no coalesced protocol bytes; CONNECT 2xx; HTTP/1.0 with/without keep-alive; request/response `Connection: close`.

### A08 — P2: close-delimited buffered body limit can be bypassed by head overshoot

**Evidence:** `body/collect.rs`, `BodyCollector::read_until_close`.

The collector starts with `tail.to_vec()` without checking its size. If the next read returns EOF, it returns that body even when the tail already exceeds `max_body`. The limit is checked only after a nonempty subsequent read. This is reachable when a head and small close-delimited body arrive together and the configured maximum is smaller than the overshoot.

**Action:** check the initial tail before copying/returning it, and check prospective growth before appending in all collection paths. Test `max_body = 0`, tail exactly at the limit, tail above the limit followed immediately by EOF, and split equivalents. Consider fallible allocation for untrusted configured lengths.

### A09 — P2: chunk metadata has neither size limits nor sufficient validation

**Evidence:** proto `response.rs`, `ChunkedDecoder::read_size_line`/`read_trailer`; client `SilenceBudget`.

Extensions accept arbitrary bytes until CR, including bare LF and control characters. Trailer lines accept arbitrary non-CR bytes without header grammar validation. There is no extension/trailer byte or count limit. Body limits count decoded data only, while silence resets on every read. A peer can keep a buffered request occupied indefinitely by continuously sending extension/trailer bytes without delivering body data or finishing. Memory remains bounded in the decoder; the exposure is bandwidth, CPU, and client occupancy.

**Action:** add bounded chunk metadata accounting and incremental extension/trailer validation, with an explicit policy for ignored trailer fields. Include optional overall deadlines for callers that need finite request duration. Test oversized metadata, bare LF/NUL, malformed trailer names, CR split boundaries, and continuous metadata progress with no decoded payload.

### A10 — P2: incremental response-head API misclassifies partial header names

**Evidence:** proto `response.rs`, `ResponseHead::parse_header_line`.

The public parser promises `Incomplete` for incomplete input, but a valid partial header name such as `HTTP/1.1 200 OK\r\nCont` returns `MissingColon`. A caller using `Incomplete` as the only instruction to fetch more bytes rejects valid fragmented responses. The in-tree client masks this by waiting for CRLFCRLF before invoking the parser.

**Action:** return `Incomplete` when an otherwise-valid name reaches input end; reject a missing colon only once invalid syntax or a complete malformed line establishes the error. Audit the CR-followed-by-non-LF case, currently also reported as incomplete. Add a prefix test for every byte boundary of valid response heads, plus malformed complete lines.

### A11 — P2: response syntax/framing validation is incomplete

**Evidence:** proto `response.rs`, `ResponseHead::parse` reason slice and `BodyFraming::from_transfer_encoding`.

The reason phrase is not validated for prohibited control characters. Transfer coding handling chooses the last nonempty comma component, accepts repeated `chunked` (an existing test explicitly expects acceptance), and does not distinguish unsupported transfer decoding from body framing. A `gzip, chunked` response is dechunked but remains gzip transfer-coded, without an explicit unsupported-coding error. These are interoperability and parser-differential risks; this audit does not demonstrate a cross-proxy exploit.

**Action:** validate reason-phrase bytes, parse transfer-coding grammar and ordering, reject repeated chunked, and define unsupported-coding behavior. Prefer rejecting unsupported transfer decoding over silently presenting it as a fully decoded body. Keep TE precedence over Content-Length and conflicting Content-Length rejection. Explicitly document/review TE+CL connection reuse policy. Add a malformed/unsupported coding matrix and differential tests against a mature parser, interpreting differences against RFC 9110/9112 rather than assuming the other parser is authoritative.

### A12 — P2: advertised larger head limits exceed header range representation

**Evidence:** proto `response.rs`, `HeaderRange` has `u16` offsets/lengths; client `response.rs`, range construction; README 128 KiB example.

A const head limit above 64 KiB does not reliably permit such heads: offsets or individual values exceeding `u16` fail with `HeaderRangeOverflow`. Failure is safe, but contradicts the useful behavior suggested by the README's 128 KiB example. The fixed 64-header limit is also independent of byte capacity.

**Action:** choose a consistent public contract: wider checked offsets, or reject unsupported head limits and remove misleading examples. Document byte and count limits separately. Test large values, late-positioned headers, empty values, and 64/65 headers at configured limits.

### A13 — P2: redirect resolution and policy are incomplete

**Evidence:** `redirect.rs`, `apply_location`, `resolve_relative_path`, `apply_method_redirect`, `follow`.

- Relative resolution concatenates paths without RFC 3986 dot-segment removal (`/a/b` + `../c` stays `/a/../c`).
- Absolute HTTP scheme detection is case-sensitive although `Url::parse` accepts mixed case. Unsupported schemes such as `ftp:` are treated as relative paths instead of being rejected.
- After exhausting the hop budget, the loop still applies Location and can open a cross-origin connection before returning `TooManyRedirects`; this happens even with `max_redirects = 0`.
- HTTPS-to-HTTP redirects are allowed without explicit downgrade policy. Standard credential headers are stripped across origins, but a 307/308 still forwards the body, and application secret headers have no sensitivity designation.
- Rewriting POST to GET drops the body but retains body-specific headers such as Content-Type/Content-Encoding.

**Action:** introduce a URI-reference resolver independent of `Client`, and a redirect policy that checks hop budget, scheme, downgrade, and origin permission **before** connecting. Define sensitive headers and replay/body behavior; strip obsolete representation headers on method rewrite. Keep cross-origin credential stripping tests. Add RFC 3986 relative-reference cases, mixed-case schemes, unsupported schemes, zero-budget/no-connect assertions, downgrade rejection, and method/header matrices. Applications fetching untrusted URLs still need destination/SSRF policy; a general HTTP client is not an SSRF boundary by itself.

### A14 — P2: timeout abstraction assumes Linux-specific error behavior

**Evidence:** `silence.rs`, `SilenceBudget::read`; `config.rs`, `Config::validate`.

Only `WouldBlock` is treated as a timeout tick. A connector/platform returning `TimedOut` ends the request after a single tick instead of honoring the silence budget. `Interrupted` is used as an internal cancellation signal, so ordinary interrupt retry semantics and cancellation are conflated. Config validation rejects `None` but not zero durations; it also permits read ticks longer than the requested silence budget, so the first blocked read can overshoot that budget. A nonblocking connector returning immediate WouldBlock causes a busy loop.

**Action:** specify connector blocking/error semantics, normalize supported timeout ticks, separate explicit cancellation from ordinary interrupted I/O, validate zero/inconsistent durations, and bound each operation by the remaining deadline where supported. Test scripted WouldBlock, TimedOut, Interrupted, immediate nonblocking failure, and budget/tick boundary combinations. Keep a Linux TCP integration test in addition to deterministic clock/stream tests.

### A15 — P2: async pending requests bypass the bounded control-ring capacity

**Evidence:** `async_client.rs`, `poll_control`, `pending: VecDeque<AsyncRequest>`.

Polling for cancellation drains new requests into an unbounded pending queue. Each request owns payload buffers and a response ring. The nominal eight-slot control ring does not bound outstanding requests or memory while an earlier stream is active. Continuous submitters can also keep the drain loop occupied.

**Action:** introduce an admission bound covering active + ring + pending requests and ideally queued bytes; expose try-submit/backpressure semantics. Keep cancellation/shutdown independent of admission capacity. Limit work per control poll. Stress-test a slow response with sustained submission and assert fixed queue/memory bounds and prompt cancellation.

### A16 — P2: URL acceptance is looser than its host grammar claims

**Evidence:** proto `url.rs`, `parse_authority`, `is_host_byte`.

The host-byte whitelist allows colon and brackets anywhere. Inputs such as `http://a:b:80/`, `http://[]/`, and `http://[not-an-ip]/` pass authority parsing, as do malformed percent escapes. Path/query bytes are not validated by URL parsing, although request serialization subsequently rejects spaces/control bytes. Thus successful URL parsing is not a general validation guarantee and malformed authorities can reach connectors.

**Action:** validate bracketed IP literals and unbracketed reg-name/port forms separately, define percent-escape handling and supported IPvFuture/zone-ID behavior, and document whether path/query parsing is structural or validating. Preserve bracketed authority bytes for Host while exposing an unbracketed connection host. Add malformed literal/colon/escape tests and connector-boundary tests.

### A17 — P2: in-tree TCP connector is not a robust production template

**Evidence:** `examples/tcp-rustls/src/main.rs`, `TcpConnector::connect`; `xibalba-benches/src/plain_connector.rs`.

The rustls example resolves only the first address, does not enable TCP_NODELAY despite the connector trait's guidance, uses blocking connect without a timeout, and passes bracketed IPv6 URL hosts to tuple DNS resolution and `ServerName`. Brackets are authority syntax, not part of an IPv6 address for these APIs. TLS negotiation is lazy, so connect success does not mean a completed verified handshake. Root certificates and provider selection are explicit and appropriately application-owned.

The benchmark plaintext connector ignores `url.scheme`, so accidentally supplying an HTTPS URL sends plaintext; its current benchmark role limits exposure, but it should fail closed on unsupported schemes. It also omits NODELAY.

**Action:** make plaintext connectors reject HTTPS; normalize the connection host separately from Host serialization; try appropriate resolved addresses; set NODELAY; bound DNS/connect/write/handshake operations and document when handshake completion occurs. Add IPv4/IPv6 loopback, first-address-fails, HTTPS-rejected-by-plaintext, and stalled-handshake tests. Keep TLS/provider dependencies out of the client crate.

### A18 — P2: public raw header-count APIs can panic on inconsistent input

**Evidence:** proto `response.rs`, `BodyFraming::from_response` slices `headers[..header_count]`; client `response.rs`, `HeadData` exposes mutable public buffer/ranges/count and `headers()` slices by count.

The library's own parser supplies consistent counts, so this is not a demonstrated peer-triggered panic in `Client`. External safe-code callers can supply an oversized count and panic despite the framing API returning Result. Public mutable `HeadData` permits the same inconsistency; invalid individual ranges are silently omitted, obscuring corruption.

**Action:** accept the exact header slice rather than a redundant count, or check it. Encapsulate HeadData invariants behind constructors/read-only accessors and decide whether invalid ranges should be impossible or explicit errors. Test malformed public inputs without panic. Treat visibility changes as an API compatibility decision.

### A19 — P3: module ownership and graph structure need incremental cleanup

**Evidence:** `async_client.rs`, proto `response.rs`, client `response.rs`, `params.rs`, `redirect.rs`, graph reports above.

The async module combines public handles/events, queue scheduling, cancellation, thread ownership, and request execution, with free `run_reader`, `poll_control`, and `process_request` functions. Proto response combines head parsing, framing, chunk decoding, and header ranges. Client response combines network head acquisition and public response data. RequestBuilder and RedirectState depend back on Client, matching the graph report's dropped back-edges. Numerous historical/procedural comments explain former designs rather than current invariants; async module docs even say there is no Arc<AtomicBool> while the implementation uses it.

**Action:** after behavioral fixes, extract one cohesive owner/module at a time: `ChunkedDecoder`, `HeaderRange`, `ResponseHeadReader`, `ConnectionState`, `RedirectResolver`/policy, `ReaderWorker`, `ControlQueue`, and response delivery. Make redirect resolution return a decision rather than reconnecting a Client. Evaluate the convenience `RequestBuilder::send` back-edge explicitly rather than hiding it with lint suppression. Keep reusable data types below orchestration. Avoid free functions for new business logic; keep ownership methods in their own responsibility-focused files. Remove obsolete narrative comments, preserving only genuinely necessary invariant explanations.

Do not force every graph skip edge away: many protocol edges target shared error/header foundations. Record and justify these, but remove orchestration back-edges and verify graph reports after each extraction. The tool reports zero SCC cycles while dropping back-edges; report that faithfully rather than declaring the graph clean.

### A20 — P3: tests and documentation give incomplete assurance

**Evidence:** root `tests/integration.rs`; README; `xibalba-client/tests/integration.rs`; public module visibility.

The root is a virtual workspace, so root integration tests are not discovered. That file still imports the old `xibalba` API. README claims `response.body()` and `response.headers` iteration even though the API exposes `body` as a field and `headers()` as a method; the normal GET response is buffered, not a live streaming response. Zero documentation tests ran for both primary crates. The 2,629-line client integration file concentrates many responsibilities and relies on timing-based TCP tests. Internal implementation types/modules such as AsyncRequest, CancellableStream, and empty-public redirect/params surfaces deserve visibility review.

**Action:** reconcile or remove obsolete root tests after comparing their cases with the active suite; split integration fixtures and behavior suites into module files. Add compiling documentation examples for buffered vs streaming usage. Add deterministic scripted-connector tests, parser prefix/property/fuzz coverage, bounded concurrency tests, explicit MSRV checks, and dependency-advisory checks. Review the public API before further releases. Do not mistake passing Clippy for protocol/security coverage.

## Action plan and execution order

Work on one behavior or extraction at a time; do not combine the following phases into one refactor.

### Phase 1 — Prevent panics, replay, and connection corruption — **complete**

1. ~~**A01:** repair chunk digit accounting; add debug/release regression tests.~~ Done (`dee3cd2`). The counter only ever answered "was there a digit?", so it is now a `bool` and cannot overflow.
2. ~~**A04:** make all transport-write failures poison the connection; test scripted partial failures.~~ Done (`dee3cd2`). Poisoning happens before the first byte is written and clears only after the head is read.
3. ~~**A03:** enforce clean dispatch on every redirect hop.~~ Done (`5bae5c6`). The earlier green run was the stale-retry path masking the defect while silently writing the hop to a dead connection twice; `sent_on` assertions now pin the hop to the fresh connection.
4. ~~**A07:** centralize persistence/upgrade/tunnel disposition for buffered, streaming, and async paths.~~ Done (`a63417f`), via `ConnectionReuse` in `reuse.rs`. CONNECT tunnelling remains open and is tracked in A07's finding above.
5. ~~**A02:** introduce explicit replay policy with safe non-idempotent defaults.~~ Done (`c7bb141`). `Method::is_replay_eligible` gates the retry; `RequestBuilder::allow_replay` is the opt-in.

Acceptance met: the scripted connector records per-connection bytes, and every failure/reuse test asserts on connection count and on which connection carried the request. 291 tests pass in debug and release; Clippy is clean under `-D warnings`.

Carried into later phases: CONNECT tunnel handling (A07), and the async paths still retry via `send_head` without the cancellation coverage A06 describes.

### Phase 2 — Make cancellation and resource bounds real — **complete (A05, A06, A08, A09, A14, A15)**

6. ~~**A05:** cancellation-aware delivery and nonblocking shutdown signaling; verify backpressure shutdown under a process watchdog.~~ Done (`e001879`). `ChunkSink` replaces every `push_block` on the delivery path. `quetzalcoatl` 0.14 exposes no producer-side "consumer gone" check outside the blocking push, so `ConsumerGuard` publishes handle liveness on our side; `into_consumer` became `into_stream` so the guard cannot be bypassed.
7. **A06 and A14 done** (`6be0fed`, `9525e63`, this commit). The `Interrupt` trait routes cancellation through request writes and response-head reads as well as bodies, and the stale-connection retry re-checks it. Both new head tests time out against the parent commit. Blocked writes needed a second mechanism: `write_all` blocks inside one syscall where no check is reached, so `SetReadTimeout::set_write_timeout` (no-op default) and `Config::write_timeout` bound it, and the TCP connectors forward it. `StreamHandle::cancel` now documents observation latency per stage. **Not done:** DNS, TCP connect, and TLS handshake are inside `Connector::connect` before the client holds a stream, so they remain unbounded and `Drop` waits for them; that needs a connect-deadline contract. **A14:** cancellation used `ErrorKind::Interrupted`, the kind meaning "a signal arrived, retry", which `write_all` and `read_exact` retry internally — a cancelled `write_all` never returned, a bug A06 introduced and its upload test masked because `write_timeout` produced a different error first. Cancellation is now a `Cancelled` payload no std retry loop inspects, and `Latch` classifies it because the payload is dropped at the `proto::Error` boundary. `SilenceBudget` absorbs `TimedOut` as well as `WouldBlock` (the latter is a Linux detail; elsewhere the budget collapsed to one tick) and paces its retry loop — an immediately-ticking stream spun 433,663 times in 50 ms before this. `Config::validate` rejects zero durations and read timeouts exceeding a budget they subdivide. `SetReadTimeout` documents the blocking/error contract. `CancellableStream` is deleted as redundant. **Not done:** per-operation deadlines are still per-read budgets rather than a total remaining deadline.
8. ~~**A15:** bound total outstanding request count/bytes, not only ring slots.~~ Done (`6fe52ee`). `Admission` counts requests from submit to completion, released by `Permit`'s `Drop`. Two further stalls surfaced and were fixed: `submit` blocked on a full control ring, and a ring smaller than the admission bound reported false backpressure.
9. ~~**A08:** fix initial-tail body-limit enforcement.~~ Done (`1a22819`).
10. ~~**A09:** bound and validate chunk metadata.~~ Done (`38ae9cc`). `MetadataBudget` bounds extensions per chunk and the trailer section as a whole; C0 controls and DEL are rejected as they arrive.

Acceptance status: queue growth is bounded and asserted; every adversarial test runs under an external `Watchdog` that fails rather than hanging. Cancel/Drop latency is now bounded for body delivery, head reads, and request writes — one read or write tick each — but **not** for connect and TLS handshake, which A06 leaves open. Body-idle versus total-duration limits are still undifferentiated, so intentional SSE streams remain supported by the idle budget alone.

### Phase 3 — Protocol and URL interoperability — **A10, A11, A12, A13, A16, A17 done; A18 open**

11. ~~**A10/A11:** incremental-head contract, status reason validation, and transfer-coding rules.~~ Done (`19602da`, `b385d28`). Truncated heads report `Incomplete` (a partial header name previously gave `MissingColon`, a head split inside its final CRLF gave `InvalidHeaderName`). `TransferCodings` parses the codings as one ordered list: `chunked` only as the final coding and never repeated, anything but `identity` refused as undecodable. Probing the pre-fix parser confirmed `gzip, chunked` returned `status=200` with body `abc`, dechunked but still compressed. Reason phrases are checked against HTAB / SP / VCHAR / obs-text. **Not done:** TE+CL connection-reuse policy is unchanged, and no differential testing against a mature parser was run.
12. **A12 done** (`c311fed`); **A18 open.** `HeaderRange` offsets widened to `u32` so the documented 128 KiB head limit works as written, and the README states the byte and header-count limits separately. The range/count invariants are still public fields rather than an encapsulated type.
13. ~~**A13:** reference resolution and pre-connect redirect policy.~~ Done (`a71b1c5`, `a2d3e39`). Hop budget, scheme case, dot-segment resolution, https-to-http downgrade, and representation headers on a method rewrite. 307/308 still forward the body across origins, and applications cannot designate their own headers as sensitive.
14. **A16 and A17 done** (`c0225dd`, this commit). Bracketed IP-literals and unbracketed reg-names are validated separately, so malformed authorities no longer reach a connector. `TcpDialer` now owns the TCP half every connector was reimplementing: it resolves through the new `Url::connection_host` (A16 made `Url::host` keep its brackets, which `ToSocketAddrs` and rustls' `ServerName` both reject — a test confirms the naive spelling fails with "Name or service not known" on every IPv6 URL), tries every resolved address rather than the first, bounds the connect, and sets NODELAY. `dial_plaintext` fails closed on HTTPS. The rustls example and both bench/test connectors were rewired onto it, and the example documents that its handshake completes lazily inside the first read or write. **Not done:** no stalled-handshake test, since the lazy handshake is bounded by the client's existing read timeout rather than by connector code.

Acceptance status: RFC 3986 §5.4 cases run table-driven against the resolver, and every byte-prefix of a valid response head is asserted to parse as `Incomplete`. Redirect tests assert on the hosts actually dialled, since a wrongly-contacted host is invisible in the returned response. IPv4 and IPv6 loopback dials are covered by `dial.rs`, which also pins first-address-fails fall-through, NODELAY, and HTTPS-rejected-by-plaintext.

### Phase 4 — Structure and maintenance

15. **A19:** extract one module owner at a time, retaining regression behavior and checking graph changes after each step.
16. **A20:** compile README examples, reconcile orphan tests, modularize test fixtures, review public visibility, add CI/MSRV/advisory/fuzz checks.

Acceptance: no unexplained graph back-edges, no new free business functions, no newly scattered feature cfg branches, and no misleading documentation examples. Common-foundation skip edges may remain with an explicit rationale.

## Verification required for each fix/extraction

From the repository root:

```sh
cargo build -p xibalba-proto -p xibalba-client --all-targets
cargo test -p xibalba-proto -p xibalba-client
cargo test -p xibalba-proto -p xibalba-client --release --all-features
cargo clippy -p xibalba-proto -p xibalba-client --all-targets --all-features -- -D warnings
cargo fmt -p xibalba-proto -p xibalba-client -- --check
cargo graph --report xibalba-proto
cargo graph --report xibalba-client
```

For workspace compatibility, also run build/test/Clippy with `--workspace --exclude xibalba-iouring`, acknowledging the benchmark transitive dependency caveat above. Do not run benchmark workloads as ordinary correctness tests. If independent features are introduced, explicitly build, fully test, and lint each meaningful combination, including no-default-features; gate fields and provide real/no-op wrapper twins instead of scattering cfg through business logic.

Before release: run on the declared MSRV and pinned toolchain; execute dependency advisory checks with a current database; run a bounded fuzz campaign for URL/head/chunk parsing; exercise cancellation under a deterministic or model-checked concurrency harness where practical. Record actual results rather than marking planned checks complete.

## Positive observations to preserve

- Request serialization validates paths and header bytes before writing; managed Host/Content-Length/Transfer-Encoding headers cannot be supplied through the builder.
- Content-Length parsing uses checked arithmetic and rejects conflicting duplicate lengths.
- Response head bytes and interim response counts are bounded in the client.
- Existing dirty-state handling already covers many dropped-stream, body-error, oversized-head, and buffered-tail scenarios, with useful regression coverage.
- Standard credentials are stripped on cross-origin redirects.
- TLS configuration and crypto provider selection remain application-owned, with explicit tests protecting the dependency boundary.
- Reviewed production modules contain no unsafe blocks; this is not a claim about dependencies.
