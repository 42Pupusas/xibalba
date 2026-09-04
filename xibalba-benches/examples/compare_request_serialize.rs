//! `Request::serialize_to_buf` vs the `http` crate's request builder.
//!
//! Lives under `examples/` (not `benches/`) so the `http` crate — a
//! competing implementation, not something xibalba needs to build or
//! ship — only enters the dependency graph when this comparison is run,
//! not for a normal build of the library or its own benches.
//!
//! Run: `cargo run -p xibalba-benches --example compare_request_serialize --release -- --bench`

use divan::black_box;
use xibalba_benches as fx;
use xibalba_proto::header::{Header, HeaderName};
use xibalba_proto::method::Method;
use xibalba_proto::request::Request;
use xibalba_proto::version::Version;

fn main() {
    divan::main();
}

#[divan::bench]
fn serialize_small_xibalba(bencher: divan::Bencher) {
    let headers = [
        Header {
            name: HeaderName::Host,
            value: b"example.com",
        },
        Header {
            name: HeaderName::UserAgent,
            value: b"xibalba/0.1",
        },
    ];
    let req = Request {
        method: Method::Get,
        path: fx::REQ_PATH_SHORT,
        query: None,
        version: Version::Http11,
        headers: &headers,
    };
    bencher.bench_local(|| {
        let mut buf = [0u8; 256];
        req.serialize_to_buf(black_box(&mut buf)).unwrap();
    });
}

#[divan::bench]
fn serialize_small_http_crate() {
    black_box(
        http::Request::builder()
            .method("GET")
            .uri("/")
            .header("Host", "example.com")
            .header("User-Agent", "xibalba/0.1")
            .body(())
            .unwrap(),
    );
}

#[divan::bench]
fn serialize_large_xibalba(bencher: divan::Bencher) {
    let headers = [
        Header {
            name: HeaderName::Host,
            value: b"api.example.com",
        },
        Header {
            name: HeaderName::UserAgent,
            value: b"xibalba/0.1",
        },
        Header {
            name: HeaderName::Accept,
            value: b"application/json",
        },
        Header {
            name: HeaderName::AcceptEncoding,
            value: b"gzip, deflate",
        },
        Header {
            name: HeaderName::ContentType,
            value: b"application/json",
        },
        Header {
            name: HeaderName::ContentLength,
            value: b"42",
        },
        Header {
            name: HeaderName::Authorization,
            value: b"Bearer token123",
        },
        Header {
            name: HeaderName::CacheControl,
            value: b"no-cache",
        },
        Header {
            name: HeaderName::Connection,
            value: b"close",
        },
        Header {
            name: HeaderName::raw(b"X-Request-Id"),
            value: b"550e8400-e29b-41d4",
        },
    ];
    let req = Request {
        method: Method::Post,
        path: fx::REQ_PATH_LONG,
        query: Some(fx::REQ_QUERY_LONG),
        version: Version::Http11,
        headers: &headers,
    };
    bencher.bench_local(|| {
        let mut buf = [0u8; 1024];
        req.serialize_to_buf(black_box(&mut buf)).unwrap();
    });
}

#[divan::bench]
fn serialize_large_http_crate() {
    black_box(
        http::Request::builder()
            .method("POST")
            .uri("/api/v2/organizations/acme-corp/projects/main/environments/production/deployments?filter=active&sort=created_at")
            .header("Host", "api.example.com")
            .header("User-Agent", "xibalba/0.1")
            .header("Accept", "application/json")
            .header("Accept-Encoding", "gzip, deflate")
            .header("Content-Type", "application/json")
            .header("Content-Length", "42")
            .header("Authorization", "Bearer token123")
            .header("Cache-Control", "no-cache")
            .header("Connection", "close")
            .header("X-Request-Id", "550e8400-e29b-41d4")
            .body(())
            .unwrap(),
    );
}
