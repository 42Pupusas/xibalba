# xibalba

A minimal HTTP/1.1 client for Rust with a streaming body reader and zero-copy parsing.

## Features

- HTTP and HTTPS (TLS 1.2+) via [rustls](https://github.com/rustls/rustls)
- Chunked and `Content-Length` transfer encoding
- Streaming body reader — no full response buffering
- Zero-copy URL and header parsing (borrows input)
- No `unsafe` code

## Usage

```rust
use xibalba::client::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new()?;
    let mut response = client.get(b"https://example.com/")?;

    println!("Status: {}", response.status);
    println!("Body: {}", response.text()?);
    Ok(())
}
```

### Accessing headers

```rust
for (name, value) in &response.headers {
    println!("{}: {}", String::from_utf8_lossy(name), String::from_utf8_lossy(value));
}
```

### Streaming the body

`response.body()` implements `std::io::Read`:

```rust
use std::io::Read;

let mut buf = [0u8; 8192];
let mut body = response.body();
loop {
    let n = body.read(&mut buf)?;
    if n == 0 { break; }
    // process &buf[..n]
}
```

## TLS crypto backend

By default xibalba uses [ring](https://github.com/briansmith/ring). To use
[aws-lc-rs](https://github.com/aws/aws-lc-rs) instead, disable default features
and enable the `aws-lc-rs` feature:

```toml
[dependencies]
xibalba = { version = "0.1", default-features = false, features = ["aws-lc-rs"] }
```

## Architecture

| Module | Responsibility |
|--------|---------------|
| `client` | `Client` and `Response` — top-level API |
| `connection` | TCP + TLS connection setup |
| `stream` | `Read`/`Write`/`SetReadTimeout` abstraction over plain/TLS streams |
| `body` | Streaming `BodyReader` backed by a lock-free ring buffer |
| `response` | Response head parser, body framing detection, chunked decoder state machine |
| `request` | Request serialiser |
| `url` | Zero-copy URL parser |
| `header` | Header name enum, parsing utilities |
| `error` | Typed error hierarchy |

The reader runs on a background thread and pushes raw bytes into an SPSC ring
buffer ([quetzalcoatl](../quetzalcoatl)). The main thread parses the response
head from that buffer, then hands the consumer side to `BodyReader` for
incremental body reads.
