# Benchmarks

Measured with [divan](https://github.com/nvzqz/divan) on an idle machine,
release profile. Network benchmarks talk to a local `EchoServer` over real TCP
sockets on loopback, so they measure syscall and parsing cost without WAN
latency or bandwidth limits.

Reproduce:

```sh
# Protocol-level comparisons against other crates
cargo run -p xibalba-benches --example compare_response_parse    --release -- --bench
cargo run -p xibalba-benches --example compare_url_parse         --release -- --bench
cargo run -p xibalba-benches --example compare_request_serialize --release -- --bench

# End-to-end, real sockets (gated so a plain `cargo bench` stays offline)
BENCH_NETWORK=1 cargo run -p xibalba-benches --example compare_network_ureq --release -- --bench
BENCH_NETWORK=1 cargo bench -p xibalba-benches --bench network
```

All figures below are medians.

## End-to-end vs `ureq` 3.4

Both clients reuse one keep-alive connection and read the body to completion.

| Scenario | xibalba | ureq | Ratio |
|---|---|---|---|
| small (13 B response) | 16.74 µs | 23.45 µs | 1.40x |
| large_resp (64 KiB) | 29.92 µs | 44.97 µs | 1.50x |
| large_req (512 B query) | 17.79 µs | 25.96 µs | 1.46x |
| stress (1000 sequential) | 16.07 ms | 23.42 ms | 1.46x |

Roughly 1.4–1.5x across the board, holding at both small and large body sizes.

## URL parsing vs the `url` crate

| Fixture | xibalba | `url` | Ratio |
|---|---|---|---|
| simple (`http://example.com/`) | 21.97 ns | 165 ns | 7.5x |
| full (userinfo, port, query, fragment) | 47.8 ns | 410.7 ns | 8.6x |

The gap is large because the two crates solve different problems. `Url::parse`
borrows from the input and validates HTTP URL structure; the `url` crate
implements WHATWG URL, which allocates and performs IDNA and percent-encoding
normalization. Prefer the `url` crate when that behaviour is needed.

## Request serialization vs the `http` crate

| Fixture | xibalba | `http` | Ratio |
|---|---|---|---|
| small | 34.83 ns | 232 ns | 6.7x |
| large | 262.7 ns | 1.011 µs | 3.8x |

Also not a like-for-like comparison: xibalba writes request bytes into a caller
buffer, while `http` builds a typed `Request` with a header map. The comparison
measures "get bytes on the wire", not equivalent data structures.

## Response head parsing vs `httparse`

This is the one place xibalba loses, and the crossover depends on header count.

| Fixture | xibalba | `httparse` | Ratio |
|---|---|---|---|
| minimal (no headers) | 22.67 ns | 34.17 ns | **1.51x faster** |
| typical (6 headers) | 183.7 ns | 115.5 ns | 0.63x — 1.59x slower |
| heavy (22 headers) | 588.4 ns | 320.7 ns | 0.55x — 1.83x slower |

xibalba is faster on the status line and slower per header. `httparse` uses
runtime-dispatched SSE4.2/AVX2 to scan header bytes in 16- and 32-byte strides;
`ResponseHead::parse` is scalar and validates each byte against the RFC 9110
`tchar` and field-value rules in one pass. Cost per header is therefore roughly
constant for xibalba and sublinear for `httparse`, so the two cross over at a
handful of headers and the gap widens from there.

This has not been optimized yet, and header-heavy parsing is the clearest
remaining target: the value scan is the hot loop and is vectorizable without
weakening validation.

Note also that end-to-end xibalba still wins by ~1.4x on responses carrying
three headers, because syscall and body-handling costs dominate head parsing at
realistic header counts.

## Blocking client vs the io_uring pool

| Scenario | blocking | io_uring | Ratio |
|---|---|---|---|
| small | 15.75 µs | 23.06 µs | 0.68x |
| large_resp | 30.03 µs | 38.31 µs | 0.78x |
| large_req | 14.9 µs | 17.97 µs | 0.83x |
| stress (1000 sequential) | 15.89 ms | 17.46 ms | 0.91x |

The experimental `xibalba-iouring` pool is **slower than the blocking client in
every scenario measured**, which is the expected result rather than a defect:
each benchmark keeps one request in flight and then waits for its response, so
every request pays `io_uring_enter` submission and completion overhead while
never batching. io_uring pays off when many operations are submitted per
syscall, and nothing here does that.

The pool should be treated as experimental and unproven for sequential
workloads. A benchmark that submits deep request batches per connection would
be needed to show whether it wins where it is supposed to.

## A gap worth naming

The `concurrent` scenario is currently **not a valid comparison** and no ratio
should be read from it. The three implementations do different things:

- `ureq` — 4 OS threads, one agent each (7.23 ms)
- `io_uring` — 4 connections on a single thread (8.43 ms)
- blocking client — no entry at all

Comparing a 4-thread result against a single-threaded one measures thread count,
not client efficiency. Making this meaningful requires a threaded blocking-client
arm and an io_uring arm that submits a real batch per connection.
