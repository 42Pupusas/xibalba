//! `Url::parse` vs the `url` crate.
//!
//! Lives under `examples/` (not `benches/`) so the `url` crate — a
//! competing implementation, not something xibalba needs to build or
//! ship — only enters the dependency graph when this comparison is run,
//! not for a normal build of the library or its own benches.
//!
//! Run: `cargo run -p xibalba-benches --example compare_url_parse --release -- --bench`

use divan::black_box;
use xibalba_benches as fx;
use xibalba_proto::url::Url;

fn main() {
    divan::main();
}

#[divan::bench]
fn url_simple_xibalba() {
    Url::parse(black_box(fx::URL_SIMPLE)).unwrap();
}

#[divan::bench]
fn url_simple_url_crate() {
    let s = std::str::from_utf8(fx::URL_SIMPLE).unwrap();
    url::Url::parse(black_box(s)).unwrap();
}

#[divan::bench]
fn url_full_xibalba() {
    Url::parse(black_box(fx::URL_FULL)).unwrap();
}

#[divan::bench]
fn url_full_url_crate() {
    let s = std::str::from_utf8(fx::URL_FULL).unwrap();
    url::Url::parse(black_box(s)).unwrap();
}
