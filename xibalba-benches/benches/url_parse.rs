//! `Url::parse` and `query_params` benches for xibalba's own parser.
//!
//! A comparison against the `url` crate lives under `examples/` — see
//! `examples/compare_url_parse.rs` — so a competing crate never shows up
//! in this package's normal dependency graph.

use divan::black_box;
use xibalba_benches as fx;
use xibalba_proto::url::Url;

fn main() {
    divan::main();
}

// ── Url::parse internals ───────────────────────────────────────────────────

#[divan::bench]
fn url_parse_simple() {
    Url::parse(black_box(fx::URL_SIMPLE)).unwrap();
}

#[divan::bench]
fn url_parse_full() {
    Url::parse(black_box(fx::URL_FULL)).unwrap();
}

#[divan::bench]
fn url_parse_ipv6() {
    Url::parse(black_box(fx::URL_IPV6)).unwrap();
}

#[divan::bench]
fn url_parse_many_params() {
    Url::parse(black_box(fx::URL_MANY_PARAMS)).unwrap();
}

// ── query_params iterator ──────────────────────────────────────────────────

#[divan::bench]
fn query_params_one(bencher: divan::Bencher) {
    let url = Url::parse(fx::URL_ONE_PARAM).unwrap();
    bencher.bench(|| {
        black_box(url.query_params().count());
    });
}

#[divan::bench]
fn query_params_ten(bencher: divan::Bencher) {
    let url = Url::parse(fx::URL_TEN_PARAMS).unwrap();
    bencher.bench(|| {
        black_box(url.query_params().count());
    });
}

#[divan::bench]
fn query_params_fifty(bencher: divan::Bencher) {
    let url = Url::parse(fx::URL_MANY_PARAMS).unwrap();
    bencher.bench(|| {
        black_box(url.query_params().count());
    });
}
