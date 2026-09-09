# Pre-release re-audit

Baseline: `9fb1feb17103fb31bdecb1b6cfbb7ee679155559` (`xibalba-proto` 0.4.0, `xibalba-client` 0.5.0).

## Recommendation

**R07 repaired; publication remains held pending the release gate.** The final pre-publish check of `f80fa3d` found three IPv6 validation defects using a temporary Rust differential probe against `std::net::Ipv6Addr`. They are repaired below with permanent regression tests. The updated commit still needs the remaining release checks and publication ref updates.

The original recommendation, kept for the record: *Hold the release. The existing gate passes, but source inspection identifies unresolved cancellation, deadline, and connection-state defects. The statement in `AUDIT.md` that every finding is closed is not sufficient for release approval.*

This is a targeted re-audit of the repaired protocol/client paths, not a completed exhaustive review of every production module. Findings below are source-derived; new executable reproductions were not added in this pass. The listed regression tests are required work, not tests claimed to have run. No production code was changed.

## Findings

### R01 — P1: full response queues still prevent cancellation from taking effect

Evidence: `xibalba-client/src/delivery.rs`, `ChunkSink::send`/`blocked_reason`; `async_client.rs`, both `cancel` methods; `control.rs`, `ControlQueue::poll_cancel`; `reader.rs`, `stream_success`.

Response delivery checks only shutdown and consumer liveness. It never polls cancellation. A retained, undrained stream can leave the reader waiting in `ChunkSink::send` indefinitely after a cancel is queued. The request keeps its admission permit and later requests cannot run. Both public cancel methods use `push_block`, so repeated cancels can fill the control ring and block the caller as well. Cancel is not operationally idempotent: each call occupies another slot.

The existing `cancel_returns_while_a_full_response_ring_is_undrained` test checks acceptance of one cancel, not completion of cancellation. Its server-side count of small chunks also does not prove response-ring saturation: decoded chunks can be coalesced into fewer delivery events.

Required: per-request cancellation observable independently of control-queue capacity; cancellation-aware delivery and an explicit terminal-event policy when output is full. Add watchdog-bounded tests that prove the reader has reached a full ring, cancel without draining, verify admission is released and the next request progresses, and repeat cancellation beyond control-ring capacity. Preserve shutdown and stale-ticket behavior. Reopens A05 in part.

### R02 — P1: deadline expiry after a head leaves an unread response reusable

Evidence: `client.rs`, `send_head_once` and `send_one`; `response.rs`, `HeadData::read_response`; `silence.rs`, `SilenceBudget::read`.

`send_head_once` clears the dirty flag as soon as the head has been read. `send_one` then calls `deadline.check()?` before collecting the body. If the deadline passes during the final head read or before that check, it returns without marking the connection dirty. The body is still unread. The next request passes `ensure_clean`, writes to the same connection, and can parse the old body's bytes as its response.

This does not require a connector to violate its contract: the configured socket read tick can exceed the remaining total deadline, and `SilenceBudget::read` checks the deadline before the read, not after a successful read.

Required: retain the dirty state until the whole response is safely consumed; body collection should positively establish reusability rather than relying on head acquisition clearing it. Add a scripted final head read crossing the deadline with a delayed body containing response-like bytes; assert that the next request uses a different connection. Cover no-body and close-disposition variants too.

### R03 — P1: total request deadlines do not bound uploads or reconnects

Evidence: `client.rs`, `send_head_once`, `send_head_interruptible`, `send_one`, `execute_streaming`, `execute`; `write_budget.rs`; `config.rs`, `request_deadline`.

The deadline is checked before serialization/write, but `WriteBudget` receives only `head_silence`. Upload writes and flush retries never consult the total. An upload making slow progress can substantially exceed the total deadline; a stalled write waits for the silence budget instead. A later response-head check cannot undo bytes already transmitted after expiry.

Reconnects get a fresh `connect_timeout`, not the minimum of that and the remaining request deadline. Same-origin cleanup and redirect reconnects may start even after the total has expired. Streaming creates its deadline only after `ensure_clean`, excluding that reconnect entirely. Read operations can also overshoot the total by a configured socket tick.

Required: propagate one operation deadline through upload, flush, retries, and reconnects; check before initiating a connect, and cap connector deadlines by remaining request time. Define and test the permitted per-I/O overshoot. Include progressing uploads, flush ticks, stale retries, same-origin cleanup, cross-origin redirects, and the streaming head path. Reopens A06/A09 total-bound assurance in part.

### R04 — P2: request-side Connection: close is ignored for reuse

Evidence: `params.rs`, extra-header policy; `client.rs`, response-only reuse decisions; `reuse.rs`, `ConnectionReuse::evaluate`; `reader.rs`, `stream_body`.

Callers may send `Connection: close`, but reuse is derived exclusively from response headers/version/status. A self-delimited HTTP/1.1 response without a matching close header leaves the connection reusable. The client can send another request despite having announced that it will not. Depending on peer timing, this causes avoidable failures, especially for non-replayed methods.

Required: include request-side close disposition in the common reuse decision for buffered, streaming, and background-reader paths. Test a server that leaves the socket open after a self-delimited response and assert no second request reaches it. A07's request-side close case remains unresolved.

### R05 — P2: redirect policy still treats unsupported schemes as relative paths and forwards application secrets

Evidence: `redirect.rs`, `has_absolute_scheme`, `apply_location`, `strip_cross_origin_headers`, `apply_absolute_location`.

Only HTTP(S) followed by `://` is recognized as absolute. `Location: ftp://other/file` and `Location: mailto:user@example.com` are instead resolved as same-origin paths. This is not RFC 3986 URI-reference handling.

Cross-origin redirects remove four standard credential headers, but still forward arbitrary secret headers and preserve a POST body on 307/308. There is no per-request origin approval or sensitive-header designation. `max_redirects = 0` is the available way to disable automatic following; it does not provide selective approval. This is a policy/API gap, not a claim that this client is an SSRF boundary.

Required: recognize URI schemes before deciding whether a reference is relative; reject unsupported schemes before any request. Provide an explicit cross-origin policy and sensitive-header/body-forwarding contract, or document and accept these limitations instead of closing the entire A13 finding. Add unsupported-scheme, custom credential, and cross-origin 307/308 tests.

### R06 — P2: chunk metadata validation and size bounds remain incomplete

Evidence: `xibalba-proto/src/response.rs`, `ChunkedDecoder::read_size_line`, `read_trailer`, `is_metadata_byte`, `MetadataBudget`.

Hexadecimal size digits are not charged to the metadata budget. Arbitrarily many leading zeroes therefore consume input without producing data or reaching a bound. With no total deadline (the default), continuously supplied size digits keep a buffered request occupied indefinitely; streaming has no total body bound by design.

Extension and trailer validation rejects control bytes, but does not validate their grammar. Examples accepted by these branches include `1;=\r\na\r\n0\r\n\r\n` and `0\r\nnot-a-header\r\n\r\n`. Trailers need field-name/colon validation, not just a printable-byte test. The bounds added since A09 are useful but do not close its full action.

Required: a bound on the complete size line, including digits; incremental extension grammar and trailer field grammar validation. Test all split boundaries, long zero prefixes, malformed extension names/values, missing trailer colons, and valid quoted values. No cross-proxy exploit is asserted here.

### R07 — P2: URL authority validation is still only a character approximation

Evidence: `xibalba-proto/src/url.rs`, `validate_ip_literal`, `validate_ipvfuture`, `validate_reg_name`, `is_host_byte`.

IPv6 validation checks only for a colon and hexadecimal/colon/dot bytes. Invalid literals such as `http://[:]/` and `http://[1:2:3]/` pass that test. Reg-name percent escapes are not validated (`http://bad%zz/`). Zone suffixes are not structurally validated, and IPvFuture checks version and nonempty suffix without checking its specific suffix grammar.

These authorities can reach application connectors despite successful URL parsing. The standard resolver may reject them later; this finding does not claim a working destination bypass. A16 is only partially fixed.

Required: actual IPv6 syntax validation, strict percent-escape handling, and a defined IPvFuture/zone support policy. Test malformed accepted cases at both parser and connector boundaries.

## Release hygiene and structural follow-up

- `cargo doc` generated **7 warnings**, including links to the removed `RequestBuilder::send`, unresolved `Client` links in `params.rs`, and public links to private implementation types. Clippy does not cover this. Add a rustdoc warnings-as-errors gate.
- `cargo graph --report xibalba-client`: **80 edges, 35 skip edges; NOT a DAG; 0 reported SCC cycles, 1 dropped back-edge (`Admission → Permit`)**. Assess the admission/permit ownership relationship explicitly; do not report this as a clean DAG.
- Protocol graph: **53 edges, 25 skip edges, DAG**. Shared error/header dependencies account for many skips; counts alone are not defects.
- Real module extraction remains incomplete: protocol `response.rs` is 2,164 lines and still owns head parsing, framing, chunk decoding, metadata accounting, ranges, and tests. Client `response.rs` still combines head I/O with public response data. Address incrementally after correctness repairs; do not batch these refactors with the above fixes.
- The current audit summary's test counts do not match this execution. For example, io_uring ran 16 tests, not the summary's 44. Recompute counts from discovered targets; distinguish test-fixture unit tests from behavior tests.

## Executed checks

All commands used `/home/cuarentaydos/Code/tooling/xibalba/Cargo.toml` explicitly. The environment doctor's build probe ran from `/home/cuarentaydos` and could not find a manifest; that was not a repository build failure.

| Check | Result |
|---|---|
| `cargo test --workspace --all-features` | Passed |
| `cargo test --workspace --all-features --release --quiet` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo build --workspace --all-targets --all-features` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo deny check all` | Passed advisories, bans, licenses, sources; duplicate-version warnings for syn and windows-sys |
| `cargo doc -p xibalba-client -p xibalba-proto --no-deps` | Completed with 7 warnings |
| Graph reports for both published crates | Results above |

Neither published crate declares Cargo features. These tests exercised the ordinary build, not `--cfg loom`: the loom integration target ran **zero tests**, and that is not model-checking evidence. MSRV execution, a fresh fuzz campaign, loom execution, excluded TLS-provider checks, package/publish verification, external endpoint tests, dependency source review, and an exhaustive io_uring review were not performed in this pass. Existing CI definitions and historical successes are not substitutes for those fresh checks.

## Repair order

1. R02: connection poisoning on post-head deadline expiry.
2. R01: cancellation independent of output/control queue capacity.
3. R03: propagate the request deadline through writes and reconnects.
4. R04: request-side persistence disposition.
5. R05–R07: redirect and parser contracts.
6. Rustdoc, graph/module cleanup, then fresh complete release gates and packaging/MSRV/loom/fuzz verification.

## Resolution

The earlier repair pass reported all seven findings repaired. The final review below found R07 incomplete; the subsequent repair now closes F01–F03.

| Finding | Resolution |
|---|---|
| R01 | Delivery polls per-request cancellation; cancels are idempotent rather than consuming a control-ring slot. Covered by `repeated_cancellation_does_not_saturate_the_control_ring`, `cancel_returns_while_a_full_response_ring_is_undrained`, `finished_requests_release_their_admission_slots`. |
| R02 | The connection stays dirty until a response is consumed in full, so a deadline crossing between head and body cannot leave it reusable. Covered by `a_deadline_crossed_between_the_head_and_its_body_poisons_the_connection` and the `framing_reuse` suite. |
| R03 | One deadline propagates through upload, flush, retry, and reconnect, with reconnects capped by the lesser of the connect timeout and the remaining total. Covered by `tests/connect_deadline.rs` and `tests/integration/request_deadline.rs`. |
| R04 | Request-side close disposition feeds the shared reuse decision on all three paths. Covered by `a_request_side_connection_close_is_not_reused_even_for_a_self_delimited_response`. |
| R05 | Unsupported schemes are refused before connecting; `Config::allow_cross_origin_redirects` is the explicit opt-out. Covered by `an_unsupported_scheme_is_refused_rather_than_treated_as_relative` and the `redirects` cross-origin tests. |
| R06 | Size-line digits are charged to the metadata budget, and extension and trailer grammars are validated. Covered by the `response::chunked` grammar and budget tests. |
| R07 | Real `IPv6address`, `reg-name` percent-escape, `ZoneID`, and `IPvFuture` grammar. Covered by the `url::tests` R07 block. |

Accepted rather than fixed, with rationale:

- **`Admission → Permit` remains a dropped back-edge.** It is RAII ownership: the permit releases the admission slot on every exit path, which is what makes cancellation and drop release capacity. Breaking the edge would mean releasing by hand at each exit. Recorded here so the graph report is not read as a clean DAG by omission.
- **Cross-origin policy is a boolean, not per-origin approval.** `allow_cross_origin_redirects` refuses the hop; it does not offer per-origin allow-listing or sensitive-header designation. R05 permitted documenting this limitation instead of building the larger API, and that is the choice taken.

### Release-gate execution

The checks this pass listed as not performed have now been run.

| Check | Result |
|---|---|
| `cargo test --workspace` (debug and release) | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo doc` (default and `--all-features`) | Passed, zero warnings; the 7 above are fixed |
| `cargo deny check all` | Passed; the same syn/windows-sys duplicate warnings, which are not gating |
| MSRV 1.91.1 build and full test suite | Passed |
| loom, under `--cfg loom` | 5 tests, all passing — the zero-test run above was the missing flag |
| Fuzz, host GNU target, 60s per target | `response_head` 763,558 execs; `chunked_body` 970,162; `url` 4,417,208. No crashes, `fuzz/artifacts/` empty |
| `cargo package -p xibalba-proto` | Packaged and verified standalone |
| `cargo package --list -p xibalba-client` | Correct file manifest; full packaging needs proto published first, as documented in `AUDIT.md` |

The module extractions are done: protocol `response.rs` is now `head`, `framing`, `chunked`, and `ranges` behind an unchanged `xibalba_proto::response::*`; client `response.rs` is now `head` and `public`. The protocol graph is a DAG at 57 edges / 25 skip edges.

The fuzz campaign is shorter than the 4×600s one `AUDIT.md` records. It is a regression check, not a replacement for that campaign.

## Final pre-publish review

Target: `f80fa3deb79f85adfbaaa29417436948e9de9d91`. Publication is held pending repair and renewed release verification.

### F01 — P2: embedded IPv4 bypasses preceding h16 validation

In `xibalba-proto/src/url.rs`, `h16_groups` returns early when the final component is IPv4, before validating the preceding components. `http://[::gggg:192.168.1.1]/` and `http://[::fffff:192.168.1.1]/` are accepted despite invalid hexadecimal groups. No destination bypass exploit was demonstrated.

### F02 — P2: embedded IPv4 counts as one group rather than two

`h16_groups` removes the IPv4 component from the returned vector. `validate_ipv6_address` then adds only one to its length, although IPv4 occupies two h16 groups. `http://[1:2:3:4:5:6::192.168.1.1]/` is accepted even though its eight explicit groups leave no room for the required nonempty `::` compression.

### F03 — P2: valid uncompressed embedded IPv4 is rejected

Without `::`, `validate_ipv6_address` sends the complete address through the h16-only left-side parser. `http://[1:2:3:4:5:6:192.168.1.1]/` is valid but rejected.

All four examples were executed in a temporary extension of `ipv6_embedded_ipv4_tail_is_accepted`, comparing `Url::parse` acceptance with `std::net::Ipv6Addr`. The test failed with all four mismatches. The temporary probe was removed after execution; production code was unchanged at that point.

### Repair and verification

`h16_groups` now validates every preceding h16 group before accepting an IPv4 tail, counts the tail as two h16 groups, and permits the valid uncompressed form by allowing the tail on the non-elided side. Permanent tests cover valid compressed and uncompressed tails, malformed preceding groups, and the group-count boundary. The URL test suite passes 67 tests; the workspace all-feature suite passes 273 protocol tests and all other targets; workspace Clippy with warnings denied and `cargo fmt --all -- --check` pass.

The rustfmt CI failure had a separate cause: `rust-toolchain.toml` pinned Rust 1.95 without requesting the `rustfmt` component. It now requests both `rustfmt` and `clippy`, matching the CI commands.

Checks before the repair:

- `cargo test --workspace --all-features --quiet`: passed, but existing coverage missed these defects.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed with the temporary probe present.
- `origin/mera` pointed at `f80fa3d`; both remote release tags still peeled to `9fb1feb`. No refs were changed by that review.

The repaired tree now has fresh focused and workspace debug verification. Release-profile tests, MSRV, loom, fuzz, packaging, dependency checks, and rustdoc remain to be rerun before publication. Earlier results must not be represented as fresh results from the repaired commit.

Each fix needs its own regression reproduction and verification before the next structural change. Keep `AUDIT.md` as historical evidence, but revise its blanket closure claim or link it to this re-audit before release.
