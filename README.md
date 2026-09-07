# xibalba

A minimal HTTP/1.1 client for Rust with a streaming body reader and zero-copy parsing.

## Features

- HTTP, and HTTPS through a TLS stack you supply — the library depends on none
- Chunked and `Content-Length` transfer encoding
- Streaming body reader — no full response buffering
- Zero-copy URL and header parsing (borrows input)
- No `unsafe` code

## Usage

```rust
use xibalba_client::Client;
// Supply the connector for the transport/TLS stack your application uses.
use my_connector::MyTlsConnector;

fn main() -> Result<(), xibalba_client::proto::error::Error> {
    let mut client = Client::<MyTlsConnector>::connect_default(
        b"https://example.com/", MyTlsConnector::tls_config()?
    )?;
    let response = client.get(b"/")?;

    println!("Status: {}", response.status);
    println!("Body: {}", response.text()?);
    Ok(())
}
```

For plain HTTP, `PlainConnector` ships with the crate and needs no TLS config:

```rust
use xibalba_client::{Client, PlainConnector};

let mut client = Client::<PlainConnector>::connect_default(b"http://127.0.0.1:8080/", ())?;
```

It refuses `https://` URLs rather than sending the request in the clear.

### Accessing headers

`headers()` is a method returning an iterator of `(&[u8], &[u8])`. Header
names are compared case-insensitively, as HTTP requires:

```rust
for (name, value) in response.headers() {
    println!("{}: {}", String::from_utf8_lossy(name), String::from_utf8_lossy(value));
}

let content_type = response
    .headers()
    .find(|(name, _)| name.eq_ignore_ascii_case(b"content-type"))
    .map(|(_, value)| value);
```

Read the headers before calling `text()`, which consumes the response.

### Streaming the body

`response.body` is a field implementing `std::io::Read`. A `get` returns as
soon as the head is parsed, so reading it incrementally never buffers the
whole body:

```rust
use std::io::Read;

let mut buf = [0u8; 8192];
loop {
    let n = response.body.read(&mut buf)?;
    if n == 0 { break; }
    // process &buf[..n]
}
```

Both forms are compiled and run as documentation tests on
[`xibalba_client`](xibalba-client/src/lib.rs); see the crate docs for the
complete runnable versions.

## Benchmarks

Against `ureq` over real loopback sockets, reusing one keep-alive connection,
xibalba is about 1.4-1.5x faster on small responses, 64 KiB responses, and
sequential throughput. URL parsing and request serialization are several times
faster than the `url` and `http` crates, though those crates do strictly more
work. Response-head parsing is faster than `httparse` with no headers and
**1.2-1.3x slower** once six or more headers are present, since `httparse` is
SIMD-accelerated and `ResponseHead::parse` is scalar.

The experimental io_uring pool is currently slower than the blocking client on
every sequential scenario measured.

See [BENCHMARKS.md](BENCHMARKS.md) for the numbers, the caveats, and how to
reproduce them.

## Transport and TLS

`xibalba-client` is transport-agnostic, and deliberately so: its only
dependencies are `xibalba-proto` and `quetzalcoatl`. It links no TLS
implementation, names no crypto crate in its types, and installs no default
provider. HTTPS reaches it entirely through the `Connector` trait, whose
associated `TlsConfig` type is chosen by the implementor.

That means the choice of TLS library, certificate roots, crypto provider,
protocol versions, and proxy behavior belongs to the application, and no
consumer pays for a stack it does not use.

The [`tcp-rustls` example](examples/tcp-rustls) shows a complete connector. It
takes a `rustls::crypto::CryptoProvider` as an argument rather than reaching for
a default, so swapping `ring` for `aws-lc-rs` or a custom provider is a
one-line change at the call site:

```rust
let tls_config = RustlsConfig::with_provider(rustls::crypto::ring::default_provider())?;
```

Note that rustls itself has a process-wide default provider, reachable via
`ClientConfig::builder()` and enabled by rustls' own default features. The
example opts out on both counts — `default-features = false` in its manifest,
and `builder_with_provider` at the call site — so the provider in use is always
the one passed in.

## Response-head limits

Two separate limits apply to a response head, and they are not derived from
each other:

| Limit | Value | Set by |
|-------|-------|--------|
| Head size in bytes | 64 KiB by default, up to 4 GiB | `MAX_HEAD_SIZE` const generic |
| Header count | 64, fixed | `xibalba_proto::response::MAX_HEADERS` |

A head within the byte limit is still rejected with `TooManyHeaders` if it
carries more than 64 header fields.

API gateways often add tracing and rate-limit headers, so the byte limit is
intentionally larger than the read buffer. Set an integration-specific
compile-time bound with the const generic when needed:

```rust
use xibalba_client::client::Client;

// Reject response heads larger than 128 KiB for this client type.
type ApiClient<C> = Client<C, { 128 * 1024 }>;
```

## Architecture

| Module | Responsibility |
|--------|---------------|
| `xibalba-proto` | Zero-copy URL/head parsing, request serialization, and body framing |
| `xibalba-client::client` | Synchronous connection reuse, redirects, and streaming bodies |
| `xibalba-client::async_client` | Background-reader API for cancellable streaming responses |
| `xibalba-client::connector` | Application-owned TCP/TLS connector boundary |
| `xibalba-iouring` | Experimental io_uring-backed HTTP/1.1 connection pool |

The synchronous client reads directly from the caller-supplied connector. The
async client owns the same synchronous client on one reader thread and delivers
response head/body chunks over `quetzalcoatl` rings. A streaming response that
is cancelled or fails while reading is deliberately discarded: the next request
reconnects rather than parsing unread bytes from the failed response as a new
one.
