# Core and standard TCP client audit

## Scope and evidence

Audited baseline: `18191e44cb479c8b0e91e8ac1a5054e2115f2b4d`.

Primary scope: every production module in `xibalba-proto` and `xibalba-client`, including the background-reader client because it shares the standard synchronous transport. Reviewed the client integration tests, protocol tests, manifests, README, the TCP/rustls connector example, and benchmark plaintext connector. The experimental `xibalba-iouring` implementation is excluded. TLS provider internals, dependency source audits, live external endpoints, fuzzing, and performance remeasurement are not covered. (Dependency advisories and fuzzing were out of scope for the *review* and were added by the action plan; see A20.)

This is a source audit plus execution of existing checks, not a claim of exhaustive security verification. Findings below are derived from the named implementation paths.

**The findings and the action plan are written in two different tenses, and it matters which you are reading.** Each `A01`–`A20` section states the defect *as found at the audited baseline* and is left unedited, so the original evidence survives; the Phase 1–4 entries below record what was actually done, with commit hashes. Every finding is now closed — do not read a finding's present tense as current behaviour.

There is no packaged default std TCP connector: `xibalba-client` is connector-generic. The standard TCP path consists of `Client`/`AsyncClient` over application-owned `Read + Write + SetReadTimeout` streams; the in-tree TCP example is therefore part of this review.

### Executed validation

These are the **baseline** results, at `18191e44`, kept as the starting point they were. The gate today runs 234 protocol + 99 client unit + 123 integration + 44 io_uring + 18 TLS + 6 fuzz-harness + 5 differential + 3 MSRV tests and 5 doctests, green in debug and release; CI in `.github/workflows/ci.yml` is what enforces it.

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

*Resolved* in `a63417f` and `197ef33` — see Phase 2, item 4.

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

*Resolved* in `c0225dd` and `cb15dba` — see Phase 3, item 14.

### A18 — P2: public raw header-count APIs can panic on inconsistent input

**Evidence:** proto `response.rs`, `BodyFraming::from_response` slices `headers[..header_count]`; client `response.rs`, `HeadData` exposes mutable public buffer/ranges/count and `headers()` slices by count.

The library's own parser supplies consistent counts, so this is not a demonstrated peer-triggered panic in `Client`. External safe-code callers can supply an oversized count and panic despite the framing API returning Result. Public mutable `HeadData` permits the same inconsistency; invalid individual ranges are silently omitted, obscuring corruption.

**Action:** accept the exact header slice rather than a redundant count, or check it. Encapsulate HeadData invariants behind constructors/read-only accessors and decide whether invalid ranges should be impossible or explicit errors. Test malformed public inputs without panic. Treat visibility changes as an API compatibility decision.

**Note:** `xibalba-iouring::driver::HeadData` still exposes the same public buffer/ranges/count triple. It is unpublished and outside the audited scope, so it was left alone; if it is ever published it needs the same treatment.

### A19 — P3: module ownership and graph structure need incremental cleanup — **done**

**Evidence:** `async_client.rs`, proto `response.rs`, client `response.rs`, `params.rs`, `redirect.rs`, graph reports above.

The async module combines public handles/events, queue scheduling, cancellation, thread ownership, and request execution, with free `run_reader`, `poll_control`, and `process_request` functions. Proto response combines head parsing, framing, chunk decoding, and header ranges. Client response combines network head acquisition and public response data. RequestBuilder and RedirectState depend back on Client, matching the graph report's dropped back-edges. Numerous historical/procedural comments explain former designs rather than current invariants; async module docs even say there is no Arc<AtomicBool> while the implementation uses it.

**Action:** after behavioral fixes, extract one cohesive owner/module at a time: `ChunkedDecoder`, `HeaderRange`, `ResponseHeadReader`, `ConnectionState`, `RedirectResolver`/policy, `ReaderWorker`, `ControlQueue`, and response delivery. Make redirect resolution return a decision rather than reconnecting a Client. Evaluate the convenience `RequestBuilder::send` back-edge explicitly rather than hiding it with lint suppression. Keep reusable data types below orchestration. Avoid free functions for new business logic; keep ownership methods in their own responsibility-focused files. Remove obsolete narrative comments, preserving only genuinely necessary invariant explanations.

Do not force every graph skip edge away: many protocol edges target shared error/header foundations. Record and justify these, but remove orchestration back-edges and verify graph reports after each extraction. The tool reports zero SCC cycles while dropping back-edges; report that faithfully rather than declaring the graph clean.

### A20 — P3: tests and documentation give incomplete assurance — **done**

**Evidence:** root `tests/integration.rs`; README; `xibalba-client/tests/integration.rs`; public module visibility.

The root is a virtual workspace, so root integration tests are not discovered. That file still imports the old `xibalba` API. README claims `response.body()` and `response.headers` iteration even though the API exposes `body` as a field and `headers()` as a method; the normal GET response is buffered, not a live streaming response. Zero documentation tests ran for both primary crates. The 2,629-line client integration file concentrates many responsibilities and relies on timing-based TCP tests. Internal implementation types/modules such as AsyncRequest, CancellableStream, and empty-public redirect/params surfaces deserve visibility review.

**Action:** reconcile or remove obsolete root tests after comparing their cases with the active suite; split integration fixtures and behavior suites into module files. Add compiling documentation examples for buffered vs streaming usage. Add deterministic scripted-connector tests, parser prefix/property/fuzz coverage, bounded concurrency tests, explicit MSRV checks, and dependency-advisory checks. Review the public API before further releases. Do not mistake passing Clippy for protocol/security coverage.

## Action plan and execution order

Work on one behavior or extraction at a time; do not combine the following phases into one refactor.

### Phase 1 — Prevent panics, replay, and connection corruption — **complete**

1. ~~**A01:** repair chunk digit accounting; add debug/release regression tests.~~ Done (`dee3cd2`). The counter only ever answered "was there a digit?", so it is now a `bool` and cannot overflow.
2. ~~**A04:** make all transport-write failures poison the connection; test scripted partial failures.~~ Done (`dee3cd2`). Poisoning happens before the first byte is written and clears only after the head is read.
3. ~~**A03:** enforce clean dispatch on every redirect hop.~~ Done (`5bae5c6`). The earlier green run was the stale-retry path masking the defect while silently writing the hop to a dead connection twice; `sent_on` assertions now pin the hop to the fresh connection.
4. ~~**A07:** centralize persistence/upgrade/tunnel disposition for buffered, streaming, and async paths.~~ Done (`a63417f`, `197ef33`), via `ConnectionReuse` in `reuse.rs` and, for tunnels, `Method::response_can_have_content`.

    The CONNECT residue is now closed. Framing took `request_method_is_head: bool` — a parameter that can only answer one question — so CONNECT had nowhere to be asked about, and a `CONNECT` 200 carrying `Content-Length: 4096` was framed as a 4096-byte body: the tunnel's first bytes (a TLS ClientHello, in the test) were collected and returned to the caller as content. RFC 9110 §9.3.6 requires the opposite — any 2xx switches to tunnel mode immediately after the header section, and a client must *ignore* `Content-Length` and `Transfer-Encoding` there. The signature now takes the `Method`, and `Method::response_can_have_content` owns the rule for both special cases (HEAD, and 2xx CONNECT). The boundary is tested rather than assumed: a refused CONNECT is an ordinary response — "any response other than a successful response indicates that the tunnel has not yet been formed" — so it keeps its content and framing.

    Correct framing is necessary but not sufficient, since this client has no API for surrendering the socket to a caller. CONNECT is therefore also refused *before the request is written*, as `ConnectionError::TunnelingNotSupported`. Refusing after the write would leave a proxy in tunnel mode facing a client that only speaks HTTP; refusing before it leaves the connection untouched, which a test pins with an ordinary GET afterwards. `xibalba-iouring` never carried the method to its response either (it passed `false` unconditionally, so HEAD was already mishandled there); it now passes `Method::Get`, the same behaviour spelled honestly, with the gap recorded at the call site.
5. ~~**A02:** introduce explicit replay policy with safe non-idempotent defaults.~~ Done (`c7bb141`). `Method::is_replay_eligible` gates the retry; `RequestBuilder::allow_replay` is the opt-in.

Acceptance met: the scripted connector records per-connection bytes, and every failure/reuse test asserts on connection count and on which connection carried the request. 291 tests pass in debug and release; Clippy is clean under `-D warnings`.

Carried into later phases: the async paths still retry via `send_head` without the cancellation coverage A06 describes. (CONNECT tunnel handling, previously carried here, closed in `197ef33`.)

### Phase 2 — Make cancellation and resource bounds real — **complete (A05, A06, A08, A09, A14, A15)**

6. ~~**A05:** cancellation-aware delivery and nonblocking shutdown signaling; verify backpressure shutdown under a process watchdog.~~ Done (`e001879`). `ChunkSink` replaces every `push_block` on the delivery path. `quetzalcoatl` 0.14 exposes no producer-side "consumer gone" check outside the blocking push, so `ConsumerGuard` publishes handle liveness on our side; `into_consumer` became `into_stream` so the guard cannot be bypassed.
7. **A06 and A14 done** (`6be0fed`, `9525e63`, this commit). The `Interrupt` trait routes cancellation through request writes and response-head reads as well as bodies, and the stale-connection retry re-checks it. Both new head tests time out against the parent commit. Blocked writes needed a second mechanism: `write_all` blocks inside one syscall where no check is reached, so `SetReadTimeout::set_write_timeout` (no-op default) and `Config::write_timeout` bound it, and the TCP connectors forward it. `StreamHandle::cancel` now documents observation latency per stage. **Was not done, now closed in `03e6a62`:** DNS, TCP connect, and TLS handshake were inside `Connector::connect` before the client held a stream, so they were unbounded and `Drop` waited for them; the connect-deadline contract that fixes it is written up under "A06 residue" below. **A14:** cancellation used `ErrorKind::Interrupted`, the kind meaning "a signal arrived, retry", which `write_all` and `read_exact` retry internally — a cancelled `write_all` never returned, a bug A06 introduced and its upload test masked because `write_timeout` produced a different error first. Cancellation is now a `Cancelled` payload no std retry loop inspects, and `Latch` classifies it because the payload is dropped at the `proto::Error` boundary. `SilenceBudget` absorbs `TimedOut` as well as `WouldBlock` (the latter is a Linux detail; elsewhere the budget collapsed to one tick) and paces its retry loop — an immediately-ticking stream spun 433,663 times in 50 ms before this. `Config::validate` rejects zero durations and read timeouts exceeding a budget they subdivide. `SetReadTimeout` documents the blocking/error contract. `CancellableStream` is deleted as redundant. **Was not done, now closed in `cfe6b7e`:** deadlines were per-read gap budgets with no total. `Config::request_deadline` is the optional total across dispatch, head, every redirect hop and a buffered body; a trickling peer produces no ticks at all, so the gap budget never fires and the total is the only bound that does. A streaming success body stays deliberately unbounded — its consumer paces it, and SSE is the case that must survive.
8. ~~**A15:** bound total outstanding request count/bytes, not only ring slots.~~ Done (`6fe52ee`). `Admission` counts requests from submit to completion, released by `Permit`'s `Drop`. Two further stalls surfaced and were fixed: `submit` blocked on a full control ring, and a ring smaller than the admission bound reported false backpressure.
9. ~~**A08:** fix initial-tail body-limit enforcement.~~ Done (`1a22819`).
10. ~~**A09:** bound and validate chunk metadata.~~ Done (`38ae9cc`). `MetadataBudget` bounds extensions per chunk and the trailer section as a whole; C0 controls and DEL are rejected as they arrive.

Acceptance status: queue growth is bounded and asserted; every adversarial test runs under an external `Watchdog` that fails rather than hanging. Cancel/Drop latency is now bounded for body delivery, head reads, and request writes — one read or write tick each, since the interrupt is consulted before every I/O call and the write budget's retry loop is paced to keep that granularity — and, since `03e6a62`, for connect and TLS handshake too, by deadline rather than by interrupt (connect holds no stream to check between calls). Body-idle and total-duration limits are now distinct: `cfe6b7e` added the total, and SSE stays supported because a streaming body is bounded by the idle budget alone.

### Phase 3 — Protocol and URL interoperability — **complete (A10, A11, A12, A13, A16, A17, A18)**

11. ~~**A10/A11:** incremental-head contract, status reason validation, and transfer-coding rules.~~ Done (`19602da`, `b385d28`, `eec444e`, `5888455`). Truncated heads report `Incomplete` (a partial header name previously gave `MissingColon`, a head split inside its final CRLF gave `InvalidHeaderName`). `TransferCodings` parses the codings as one ordered list: `chunked` only as the final coding and never repeated, anything but `identity` refused as undecodable. Probing the pre-fix parser confirmed `gzip, chunked` returned `status=200` with body `abc`, dechunked but still compressed. Reason phrases are checked against HTAB / SP / VCHAR / obs-text.

    The two remaining items are now closed. **TE+CL reuse policy** (`eec444e`): framing decided which bytes were the body but not whether the connection survived, so a response carrying both `Transfer-Encoding` and `Content-Length` was dechunked correctly and then *reused*, and an HTTP/1.0 response carrying a transfer coding was reused whenever it asked for keep-alive. RFC 9112 §6.3 resolves the framing in favour of the coding — which the parser already did — but an intermediary that read the `Content-Length` instead forwarded a different number of body bytes, leaving the remainder where the next response would be read from; §6.1 is stronger for HTTP/1.0, where a transfer coding is faulty framing outright. `ConnectionReuse` now takes the framing headers alongside version and status, with `FramingHeaders` owning the ambiguity question. Both defects were confirmed failing first; three integration tests script *both* a reuse and a reconnect so neither outcome can pass by hanging, and an unambiguous control must still reuse.

    **Differential testing** (`5888455`): the parser now runs beside `httparse` on one corpus of response heads and chunked bodies. `httparse` is treated as a reference, not an authority — it is a lenient syntax parser that decides no framing and accepts constructs RFC 9112 tells a recipient to reject — so each case records the expected *relationship* (both accept and agree on status/headers, both reject, or this parser is deliberately stricter), and an asymmetry without a written rationale fails the suite. Error kinds are not compared. Chunked cases compare at the chunk-size boundary, the field an attacker manipulates. One prediction was wrong and the suite caught it: obs-fold was recorded as an asymmetry, but `httparse` rejects it by default too (opt-in via `ParserConfig`), so the case became mutual rejection. `httparse` is a dev-dependency and a manifest test keeps it one — `xibalba-proto` still ships with zero runtime dependencies.
12. **A12 and A18 done** (`c311fed`, this commit). `HeaderRange` offsets widened to `u32` so the documented 128 KiB head limit works as written, and the README states the byte and header-count limits separately. `BodyFraming::from_response` no longer takes a redundant `header_count` — it sliced by it and panicked on an oversized one despite returning `Result`, which a scratch test reproduced as "range end index 9 out of range for slice of length 1"; `ResponseHead::headers` now narrows a buffer by the count that parsed it. `HeadData`'s buffer, ranges and count are private behind a fallible `new` and read-only accessors, so the three cannot disagree, and `headers()` yields every header rather than `filter_map`-skipping ranges that fall outside the buffer. Writing the constructor's bounds check revealed it was insufficient on its own: an unused range is all-zero and trivially in bounds, so validity also requires a non-empty name (`field-name = 1*tchar`).
13. ~~**A13:** reference resolution and pre-connect redirect policy.~~ Done (`a71b1c5`, `a2d3e39`). Hop budget, scheme case, dot-segment resolution, https-to-http downgrade, and representation headers on a method rewrite. 307/308 still forward the body across origins, and applications cannot designate their own headers as sensitive.
14. **A16 and A17 done** (`c0225dd`, this commit). Bracketed IP-literals and unbracketed reg-names are validated separately, so malformed authorities no longer reach a connector. `TcpDialer` now owns the TCP half every connector was reimplementing: it resolves through the new `Url::connection_host` (A16 made `Url::host` keep its brackets, which `ToSocketAddrs` and rustls' `ServerName` both reject — a test confirms the naive spelling fails with "Name or service not known" on every IPv6 URL), tries every resolved address rather than the first, bounds the connect, and sets NODELAY. `dial_plaintext` fails closed on HTTPS. The rustls example and both bench/test connectors were rewired onto it, and the example documents that its handshake completes lazily inside the first read or write.

    The stalled-handshake test is now done too (`cb15dba`), and writing it disproved the reason it had been deferred. "Bounded by the client's existing read timeout" was a claim made in three places — the connector trait, the config, and this document — and verified in none, partly because the connector lived in a `[[bin]]` that nothing could import. It was false. Reads go through `SilenceBudget`, which absorbs a socket timeout as a tick and retries it against a wall-clock budget; writes went straight to `write_all`, where the first tick is fatal. rustls drives its handshake inside the first *write*, blocking on the peer's records, so that write expires on the socket's **receive** timeout: a server that took three read timeouts to begin a perfectly good handshake failed after one, leaking a raw `WouldBlock` ("os error 11") to the caller while the configured head-silence budget went unconsulted. That is precisely the leak `SilenceBudget`'s own documentation says it exists to prevent, on the side that never got one.

    `WriteBudget` is the write-side twin, with `Tick` owning the classification both budgets share so they cannot drift on what counts as a failure. It is `write_all` with a retry rather than a call to it: a partial write followed by a tick must resume at the offset the peer accepted, since restarting would resend the prefix and the peer would read it as part of the same request. The example became a library so a test could reach its connector — the other half of why this went unverified. Two tests now pin both directions: a slow-but-healthy handshake is waited out, and a peer that never speaks TLS ends at the budget with an error that names it.

    One regression this surfaced in the existing suite is worth recording. `connection_poisoning`'s fixtures failed writes with `TimedOut`, chosen as a kind outside the stale-keepalive retry set; it is now also a tick, so three of those tests retried until the default two-minute budget expired — 120s where they had been instant. **The suite still passed.** A test that takes two minutes to assert the same thing is a regression that reports itself as success, and only the wall-clock reading caught it.

Acceptance status: RFC 3986 §5.4 cases run table-driven against the resolver, and every byte-prefix of a valid response head is asserted to parse as `Incomplete`. Corpus discrimination was checked by deliberately miscategorising a differential case and confirming the harness fails rather than passing quietly. Redirect tests assert on the hosts actually dialled, since a wrongly-contacted host is invisible in the returned response. IPv4 and IPv6 loopback dials are covered by `dial.rs`, which also pins first-address-fails fall-through, NODELAY, and HTTPS-rejected-by-plaintext.

### Phase 4 — Structure and maintenance — **complete (A19, A20)**

15. ~~**A19:** extract one module owner at a time, retaining regression behavior and checking graph changes after each step.~~ Done (`0a52933`, `476a728`, this commit), in three steps, each verified against the full suite and the graph before the next.

    `ControlQueue` and `ReaderWorker` took the async module's channel protocol and request lifecycle out of `async_client.rs` (783 → 370 lines), replacing the free `run_reader`/`poll_control`/`process_request` and the four parameters they threaded between them. `Hop` and `Origin` turned redirect resolution into a decision the client acts on, rather than a loop that reconnects the client from underneath it — which also made the downgrade refusal and cross-origin credential stripping unit-testable without a socket. `RequestBuilder::send` was deleted rather than suppressed: it duplicated `Client::send`, had no in-tree caller, and was the sole cause of its back-edge.

    **Back-edges 3 → 1**, and the survivor is judged rather than hidden: `Admission → Permit` is the shape of an RAII guard, since a guard that releases itself must reach what it borrowed from. Breaking it would mean releasing slots by hand on every exit path, including the ones easiest to forget (cancel, dropped consumer, request discarded while queued). It is documented as such on `Permit`.

    Skip edges stay high (135) and are *not* treated as a defect: nearly all target `Error`, `Header`, `HeaderName` and `StatusCode` — shared protocol foundations that everything legitimately depends on, exactly the case the finding says to record rather than force away. The tool still reports 0 SCC cycles while dropping the one back-edge; that is a dropped edge, not a clean graph.
16. ~~**A20:** compile README examples, reconcile orphan tests, modularize test fixtures, review public visibility, add CI/MSRV/advisory/fuzz checks.~~ Done (`052c71c`, `66834e8`, `6c736b6`, `a1f5dcf`, this commit).

    **Documentation is now executed** (`052c71c`). The root `tests/integration.rs` was deleted rather than ported: the root is a virtual workspace so it was never discovered, and it imported the pre-split `xibalba` crate, so it could not have compiled if it had been. All six of its cases were confirmed present in `xibalba-client/tests/integration.rs` first. The README had the API backwards in *both* directions — `headers` shown as a field when it is a method, `body()` as a method when it is a field — and both crates ran zero doctests, which is why nothing caught it. There are now five runnable ones against a loopback server, and `xibalba-proto` has crate documentation where it previously had none. `PlainConnector`/`PlainStream` became public: four copy-pasted duplicates existed because every test needs a cleartext connector and the crate shipped none. `cargo graph` caught the fourth, reporting both types at two levels at once.

    **Visibility reviewed** (this commit). `rust::unreachable_pub` is now enabled workspace-wide, which is the mechanical check; it immediately found 17 pointless `pub`s in private modules and, once `admission` was made private, that `Admission::max()` was entirely dead. Clippy's nursery `redundant_pub_crate` wants the exact opposite and is switched off with a note, since `unreachable_pub` is the one that catches accidental crate-root exports. `control`, `redirect`, `params` and `admission` are now private modules; `AsyncRequest`, `Admission` and `Permit` are no longer public. None had a usable public API: `AsyncRequest` has no public constructor or accessor, and the admission bound is fixed at `DEFAULT_MAX_OUTSTANDING`, so no caller could build or configure one. `RequestBuilder` and `DEFAULT_MAX_OUTSTANDING` keep working through the crate root, which is how the docs always spelled them. `Interrupt`, `InterruptibleStream`, `UriReference` and `Origin` stay public and were judged rather than swept: the first two are the extension seam a caller implements, `UriReference` is a pure RFC 3986 resolver, and `Origin` was deliberately announced in the changelog.

    **Fixed-span server holds replaced** (this commit). `async_backpressure` took 20.0 s of a 22 s suite, and none of it was work: the servers slept 10–20 s to hold their sockets open, and `shutdown()` joined those threads. A `StopSignal`/`StopPark` pair in `tests/support/` replaces the sleep with a channel the server blocks on, released by dropping the signal — including on a panicking test, since the unwind drops it. That file now runs in **0.21 s**, unchanged in what it asserts.

    Emptying `StopPark::wait` fails exactly one test, `cancel_interrupts_a_silent_response_head`, so the hold is only load-bearing there; `FloodServer` blocks writing into a socket the client has stopped draining and never reaches the park at all. Recording that rather than claiming the mutation caught everything: those tests are pinned by backpressure and their sleeps were pure teardown cost. Separately, `timeout_fires_on_stalled_server` ended in `drop(server)` rather than a join, leaking a 10 s thread past the test; it now joins.

    **Integration file modularized** (this commit). `tests/integration.rs` became `tests/integration/` with eleven modules, split along the `// ── ──` banners the file already carried — the seams were documented, just not enforced. Twelve helpers moved to `tests/support/{server,client}.rs` as methods on `TestServer`, `RequestReader` and `TestClient` rather than the free functions they were. Failures now name their area (`redirects::redirect_chain`).

    The move was performed by a throwaway Rust tool, not by hand, and checked by comparing the `#[test]` inventory of the new modules against the *committed* original read through `git show`: 78 before, 78 after, no names lost or gained. Worth noting that the mechanical rewrite was not correct first time — substituting `connect(` corrupted test names containing `reconnect(`, which the compiler caught. A hand-edit of 2,700 lines would have had the same class of error with no such check.

    **Scripted connector built** (this commit). `tests/support/{script,scripted,registry}.rs` add a `Connector` backed by an in-memory `Script` rather than a socket. `Step::AwaitRead` is a barrier: the next byte is not delivered until the client has read everything queued before it. That states the ordering a sleep was approximating, and takes as long as the client takes instead of a fixed guess.

    Scope was decided before building, not after: the scripted connector serves the parsing, framing and redirect tests, where a real socket contributes only nondeterminism. The backpressure and blocked-upload tests keep real TCP, because kernel buffering and partial writes are their actual subject — converting those would delete the coverage rather than stabilise it.

    One finding worth recording. The first converted test (`streaming_chunked_need_more_is_not_eof`) still passed with its barrier deleted: the response bytes were identical either way, so every assertion held while the decoder no longer hit the `NeedMore` the test exists for. A barrier is invisible to assertions about the result. `Progress::data_reads` now counts delivered reads and `assert_data_reads` makes the fragmentation itself checkable; with it, deleting the barrier fails the test with a message naming the cause. Tests whose subject is *how* bytes were split need that guard, or they quietly stop testing it.

    **All sequencing sleeps removed** (second commit). Every remaining `thread::sleep` used to order a server against a client is gone; what is left in the suite is bounded polling — loops whose condition is the event, with a deadline that fails the test rather than a duration that defines it.

    Three fixtures cover the cases a barrier could not. `Step::AwaitGate` holds a script until the test opens it, for waits whose condition only the test can see (a second request submitted, a cancel pushed). `Step::Hang` goes silent for good, so a client-side silence budget is what ends the wait — a `Close` would end it via EOF, which is the wrong reason and passes even when the framing is wrong. `Step::StallReads(n)` states a number of retries where the subject is a retry count rather than a duration. `Milestone` is the reverse of `StopSignal`: the server reporting it has reached a point, used by the real-socket tests that stay on TCP.

    A second unfalsifiability finding, in the same shape as the first. Gates are invisible to assertions about the result, so deleting one leaves a passing test. The first guard written for this was wrong: it recorded whether the reader was *parked* at the gate when the test opened it, which is a thread-timing coincidence, and it failed all five gated tests on a fast reader. The guarantee actually relied on is that the script *cannot* proceed without the test — a property of the script, not of any thread. `assert_gated_on` checks that instead, deterministically. Verified by deleting a gate: it fails, naming the cause.

    `async_backpressure.rs` keeps real TCP by design and now runs in 0.06s rather than ~1.5s.

    **MSRV stated once and checked** (`a1f5dcf`). The floor was declared in two manifests and verified by nothing, so it was measured rather than trusted — `cargo +1.xx check --ignore-rust-version`, since cargo otherwise refuses on the declared metadata before compiling anything. `xibalba-client` genuinely needs **1.91** (`Duration::from_mins`, stabilised there). `xibalba-proto` compiles on **1.88**; its own floor is let-chains in `request.rs`, and its declared 1.91 was inherited folklore. Three crates declared no MSRV at all.

    One workspace-wide floor at 1.91 is kept, which makes proto's 1.88 a deliberate policy choice rather than an accident — recorded here because lowering it later is compatible while raising it is a break. `msrv.rs` reads the manifests **as text**, because a parsed value reports the resolved number for both an inherited and a literal key and would miss exactly the drift it exists to catch; both assertions were mutation-verified. A trap worth naming: `rust-toolchain.toml` pins 1.95, which silently wins in CI, so the MSRV job overrides it explicitly or it rebuilds the gate's compiler and proves nothing.

    **`cargo-deny` found five real advisories on its first run** (`a1f5dcf`) — not a clean bill. `RUSTSEC-2023-0071` (rsa, Marvin timing attack, no fix exists), three `rustls-webpki` 0.102.8 issues including `RUSTSEC-2026-0104`, a panic reachable *before* signature verification, and unmaintained `paste`. All five arrive through one path: `rustls-rustcrypto` 0.0.2-alpha ← `examples/tls-providers`. `cargo update` cannot fix it, since rustcrypto pins `^0.102` and the patched webpki is 0.103.

    The deciding fact is what the published crates reach: `cargo tree -p xibalba-proto` is empty and `xibalba-client` has two dependencies, so **none of it is reachable from anything we ship**. The bench is therefore excluded from the workspace rather than the advisories being ignored — the audit now covers what we publish. Excluding it from the *audit* is the point; excluding it from the *build* is not, so it keeps its own lockfile and CI job and its 18 adversarial TLS tests still run. The licence allow-list is the tree's own licences read from `cargo metadata`; cargo-deny warns on an allowance nothing matches, so it cannot quietly widen.

    **Fuzzing, and the defect it found** (`d0460b2`). The invariants live in `xibalba-fuzz`, driven by both a fixed-corpus test in the ordinary gate and the libFuzzer targets under `fuzz/`; two drifting sets would mean the cheap gate said nothing about the expensive campaign. The properties worth having are the ones a hand-written case cannot express: every prefix of a parsable head must report `Incomplete` (an incremental caller reads on), parsed fields must point into the caller's buffer — the zero-copy claim, checked by pointer range — and splitting a chunked stream anywhere must decode identically.

    That last one failed immediately. `ChunkedDecoder::decode` returned an error while discarding body bytes it had already written to the caller's output buffer, so `1\r\na\r\nz\r\n` fed whole lost the `a` that the same bytes fed one at a time delivered. Where a socket read happens to break is not something the peer chose. The fix is the rule the code already applied three lines away — `read_trailer` defers `Done` so earlier data takes precedence — now applied to errors, which are held in the decoder state and reported on the next call and every call after it. Neither client call site was exploitable, since both discard the body on error; a decoder that answers differently depending on packet boundaries is still wrong.

    Verified by campaign, not by assertion. A first pass from an empty corpus (2.3M / 2.1M / 4.8M executions) was followed by a seeded 30-minute run per surface across 8 workers: **25M executions on `chunked_body`, 8.6M on `response_head`, 200M on `url`, no crashes and no artifacts written.** The seeds are the ones the in-gate test replays, written from `xibalba-fuzz` by `seed_corpus` so the two cannot drift; an unseeded fuzzer spends its early minutes rediscovering the letters `HTTP`. A test holds the surfaces, the fuzz targets and the CI matrix in step, since a renamed surface would otherwise stop being explored in silence.

    **CI** (`a1f5dcf`, `d0460b2`). The hand-run gate — fmt, clippy under `-D warnings`, tests in debug *and* release — plus the three checks a human cannot perform by rebuilding: that the declared floor compiles, that no dependency carries a known advisory (on a schedule, since advisories appear against unchanged code), and that the parsers survive inputs nobody wrote. Two self-inflicted errors are worth recording: the fuzz job named three targets that did not exist for one commit, and `cargo-deny` caught the new crate declared as a wildcard dependency. Both were caught by running the tools rather than by reading the files.

### A06 residue — the connect deadline (done)

`Connector::connect` now takes a `Deadline` and the trait requires an implementor to return by it. This closes the last row of `StreamHandle::cancel`'s latency table, which read "not bounded".

The gap was not an oversight in the client but a missing parameter: the trait passed a URL and a TLS config, so an implementor had nothing to bound itself *against*. No amount of client-side care could fix that from outside. Connecting is also the one stage cancellation cannot reach — every other blocking step runs on a stream the client owns and is checked between I/O calls, whereas `connect` holds no stream and offers no such point. The deadline is therefore the only bound that can exist here, which is why it belongs in the signature rather than in a connector's own configuration.

`Deadline` is an instant, not a duration, because connecting is several operations in sequence: resolution, then one attempt per resolved address, then possibly a handshake. A duration handed to each multiplies the bound by the number of steps, which is exactly the bug `TcpDialer` had — a 10s `connect_timeout` against a name resolving to eight blackholed addresses was an 80s stall. `TimeLeft` keeps `Unbounded`, `Remaining` and `Expired` apart, and `Remaining` is never zero: `SO_RCVTIMEO` and `TcpStream::connect_timeout` both read a zero duration as *no timeout*, so a naive "remaining time" of zero would produce an unbounded wait at the very moment the deadline was supposed to stop one.

Resolution is bounded honestly rather than hopefully. `getaddrinfo` takes no timeout and cannot be cut short, so the dialler checks the deadline either side of it; that bounds when a resolved address is *used*, and the docs say so instead of implying the lookup itself is interruptible.

**A vacuous test caught by mutation.** The first version of the multiple-address test asserted wall-clock: eight refusing addresses must finish in under two seconds. It passed against the *unfixed* code, because a closed loopback port refuses instantly — the timing never had a chance to bite. Reaching a genuinely blackholed address means depending on the host's routing. The replacement counts how many addresses the dialler pulls from the iterator, which is the same property with neither problem, and it does fail when the clamp is removed. Deleting the deadline check now fails five tests across `dial.rs` and `connect_deadline.rs`, each naming its own cause.

`Config::connect_timeout` defaults to 30s and is validated with the other durations; zero is rejected, since a zero connect deadline expires before the first address is tried. The deadline is built per attempt rather than stored, so a reconnect gets a whole budget instead of the remains of the original connect's — stored, it would shrink to nothing over a long-lived client's life and eventually make reconnection impossible. That is its own test, and it fails if the deadline is hoisted into a field.

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

Most of this is now enforced rather than remembered. `.github/workflows/ci.yml` runs the gate in debug and release, builds on the declared MSRV with the toolchain pin explicitly overridden, runs `cargo deny check all` on a weekly schedule as well as per push, and fuzzes each parser surface briefly; `cargo test` replays a fixed fuzz corpus through the same invariants. `deny.toml` and the excluded `examples/tls-providers` are the audit boundary, and `xibalba-proto/tests/msrv.rs` fails when a manifest drifts from the workspace floor or from what CI pins.

Cancellation is now model-checked rather than left to the scheduler. `xibalba-client/tests/loom_shared_state.rs` runs four `loom` tests that enumerate the interleavings of the admission counter and the producer/consumer handoff: that admission never exceeds its limit under a race, that a release racing a claim leaves the count exact, that a dropped consumer is always observed by a waiting producer, and that a concurrent shutdown and drop always unblock the producer. `src/sync.rs` is the one shim re-exporting `Arc` and the atomics from either `loom` or `std`, so no `cfg` is scattered through the logic; loom is a `[target.'cfg(loom)'.dependencies]` entry, not a dev-dependency, because the lib cannot see dev-deps.

Two limits are worth stating plainly. The suite models the shared-state protocol only — the chunk rings and the reader thread are outside it — and it reimplements that protocol rather than linking it, so a change to `try_admit` or `blocked_reason` must be mirrored into the test by hand; the file header says so. The tests were checked by mutation, not just by passing: breaking the CAS into a load-then-store fails both admission tests, and deleting the consumer-alive check fails the dropped-consumer test. That second mutation initially left `a_concurrent_shutdown_and_drop_always_unblocks_the_producer` green, because it depended only on the shutdown flag; a third thread was added so it depends on both, which grew the interleaving space from 0.02s to 1.26s of exploration and caught the mutation.

The flag is the whole test: `cargo test --test loom_shared_state` without `RUSTFLAGS="--cfg loom"` compiles the file to nothing and reports a cheerful `0 passed`. CI runs it under the flag in its own `loom` job, along with Clippy in the same configuration, and a test in the file asserts that job still exists.

Still manual before a release: a long fuzz campaign from a seeded corpus — `cargo run -p xibalba-fuzz --bin seed_corpus` then `cargo +nightly fuzz run <target> --target x86_64-unknown-linux-gnu -- -max_total_time=1800 -jobs=8`, since CI's 60s per target proves the target builds and catches an obvious regression but is not a campaign. Record actual results rather than marking planned checks complete.

## Release gate for 0.4.0 / 0.5.0

`xibalba-proto` 0.4.0 and `xibalba-client` 0.5.0 cover the audit work after the 0.3.0/0.4.0 tags. Both bumps are minor because both crates broke API again — `BodyFraming::from_response` lost its `header_count`, `HeaderRange` widened to `u32`, `ResponseHead::headers` arrived, and several `ConnectionError` variants were added.

Run before tagging, in this order, and record what came back:

- `cargo test --workspace` and `--workspace --release`. Several bounds here are asserted against wall-clock time, so optimisation changes what a test observes; debug alone is not the gate.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and again under `RUSTFLAGS="--cfg loom"` for `xibalba-client`. Clippy only sees the loom test file in the second configuration, and it demands `Admission::new` be `const` in the first.
- `cargo deny check all`.
- `cargo package -p xibalba-proto`. This is not covered by any other check and has broken twice: a `publish = false` dev-dependency carrying a version sends cargo looking for it on crates.io. `xibalba-proto` therefore depends on `xibalba-fuzz` by bare path, and `deny.toml` sets `allow-wildcard-paths` so the two rules do not contradict each other.
- The seeded fuzz campaign above.

`cargo package -p xibalba-client` cannot pass until proto is published, because verification builds the client against the registry's copy of proto rather than the workspace one. **Publish proto first, then the client.** A failure naming missing proto APIs is that ordering, not a defect.

The 2026-09 campaign ran 4 workers × 600s per target from the seeded corpus: `response_head` 14.3M executions, `chunked_body` 15.5M, `url` 212M. No crashes, no new coverage after the seeds (`cov: 195`, `169`, `263` held flat), and `fuzz/artifacts/` stayed empty.

## Positive observations to preserve

- Request serialization validates paths and header bytes before writing; managed Host/Content-Length/Transfer-Encoding headers cannot be supplied through the builder.
- Content-Length parsing uses checked arithmetic and rejects conflicting duplicate lengths.
- Response head bytes and interim response counts are bounded in the client.
- Existing dirty-state handling already covers many dropped-stream, body-error, oversized-head, and buffered-tail scenarios, with useful regression coverage.
- Standard credentials are stripped on cross-origin redirects.
- TLS configuration and crypto provider selection remain application-owned, with explicit tests protecting the dependency boundary.
- Reviewed production modules contain no unsafe blocks; this is not a claim about dependencies.
