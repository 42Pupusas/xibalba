//! `ResponseHead::parse` vs `httparse`.
//!
//! Lives under `examples/` (not `benches/`) so `httparse` — a competing
//! implementation, not something xibalba needs to build or ship — only
//! enters the dependency graph when this comparison is run, not for a
//! normal build of the library or its own benches.
//!
//! Run: `cargo run -p xibalba-benches --example compare_response_parse --release -- --bench`

use divan::black_box;
use xibalba_benches as fx;
use xibalba_proto::header::Header;
use xibalba_proto::response::ResponseHead;

fn main() {
    divan::main();
}

#[divan::bench]
fn parse_minimal_xibalba() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_MINIMAL), &mut hdrs).unwrap();
}

#[divan::bench]
fn parse_minimal_httparse() {
    let mut hdrs = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut hdrs);
    resp.parse(black_box(fx::RESP_MINIMAL)).unwrap();
}

#[divan::bench]
fn parse_typical_xibalba() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_TYPICAL), &mut hdrs).unwrap();
}

#[divan::bench]
fn parse_typical_httparse() {
    let mut hdrs = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut hdrs);
    resp.parse(black_box(fx::RESP_TYPICAL)).unwrap();
}

#[divan::bench]
fn parse_heavy_xibalba() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_HEAVY), &mut hdrs).unwrap();
}

#[divan::bench]
fn parse_heavy_httparse() {
    let mut hdrs = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut hdrs);
    resp.parse(black_box(fx::RESP_HEAVY)).unwrap();
}
