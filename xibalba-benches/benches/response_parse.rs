//! `ResponseHead::parse` and `ChunkedDecoder` benches for xibalba's own
//! parser.
//!
//! A comparison against `httparse` lives under `examples/` — see
//! `examples/compare_response_parse.rs` — so a competing crate never shows
//! up in this package's normal dependency graph.

use divan::black_box;
use xibalba_benches as fx;
use xibalba_proto::header::Header;
use xibalba_proto::response::{ChunkedDecoder, DecodeResult, ResponseHead};

fn main() {
    divan::main();
}

// ── ResponseHead::parse internals ──────────────────────────────────────────

#[divan::bench]
fn parse_minimal() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_MINIMAL), &mut hdrs).unwrap();
}

#[divan::bench]
fn parse_typical_6_known() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_TYPICAL), &mut hdrs).unwrap();
}

#[divan::bench]
fn parse_heavy_22_mixed() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_HEAVY), &mut hdrs).unwrap();
}

#[divan::bench]
fn parse_heavy_20_unknown() {
    let mut hdrs = [const { Header::empty() }; 32];
    ResponseHead::parse(black_box(fx::RESP_ALL_UNKNOWN), &mut hdrs).unwrap();
}

// ── ChunkedDecoder internals ───────────────────────────────────────────────

#[divan::bench]
fn chunked_single_chunk() {
    let mut out = [0u8; 256];
    let mut dec = ChunkedDecoder::new();
    dec.decode(black_box(fx::CHUNKED_SINGLE), &mut out);
}

#[divan::bench]
fn chunked_multi_chunk_4() {
    let mut out = [0u8; 256];
    let mut dec = ChunkedDecoder::new();
    let mut pos = 0;
    loop {
        let (r, n) = dec.decode(black_box(&fx::CHUNKED_MULTI[pos..]), &mut out);
        pos += n;
        if matches!(r, DecodeResult::Done) || n == 0 {
            break;
        }
    }
}

#[divan::bench]
fn chunked_large_64kib(bencher: divan::Bencher) {
    let data = fx::chunked_large();
    bencher.bench_local(|| {
        let mut dec = ChunkedDecoder::new();
        let mut out = vec![0u8; 65600];
        dec.decode(black_box(&data), &mut out);
    });
}

#[divan::bench]
fn chunked_small_out_buf_256b(bencher: divan::Bencher) {
    let data = fx::chunked_large();
    bencher.bench_local(|| {
        let mut dec = ChunkedDecoder::new();
        let mut out = [0u8; 256];
        let mut pos = 0;
        loop {
            let (r, n) = dec.decode(black_box(&data[pos..]), &mut out);
            pos += n;
            if matches!(r, DecodeResult::Done) {
                break;
            }
            if n == 0 && pos >= data.len() {
                break;
            }
        }
    });
}

#[divan::bench]
fn chunked_byte_at_a_time() {
    let mut out = [0u8; 64];
    let mut dec = ChunkedDecoder::new();
    for &byte in fx::CHUNKED_SINGLE {
        let one = [byte];
        dec.decode(black_box(&one), &mut out);
        if dec.is_done() {
            break;
        }
    }
}
