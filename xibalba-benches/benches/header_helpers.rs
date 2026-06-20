use divan::black_box;
use xibalba_benches as fx;
use xibalba_proto::bytes::ByteSliceExt;
use xibalba_proto::header::HeaderName;

fn main() {
    divan::main();
}

// ── HeaderName::from_bytes ─────────────────────────────────────────────────

#[divan::bench]
fn header_name_host() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_HOST)));
}

#[divan::bench]
fn header_name_content_length() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_CONTENT_LENGTH)));
}

#[divan::bench]
fn header_name_content_type() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_CONTENT_TYPE)));
}

#[divan::bench]
fn header_name_transfer_encoding() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_TRANSFER_ENCODING)));
}

#[divan::bench]
fn header_name_accept_encoding() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_ACCEPT_ENCODING)));
}

#[divan::bench]
fn header_name_cache_control() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_CACHE_CONTROL)));
}

#[divan::bench]
fn header_name_unknown() {
    black_box(HeaderName::from_bytes(black_box(fx::HDR_UNKNOWN)));
}

// ── trim_ows ───────────────────────────────────────────────────────────────

#[divan::bench]
fn trim_ows_none() {
    black_box(black_box(fx::OWS_NONE).trim_ows());
}

#[divan::bench]
fn trim_ows_both_ends() {
    black_box(black_box(fx::OWS_BOTH).trim_ows());
}

#[divan::bench]
fn trim_ows_tabs() {
    black_box(black_box(fx::OWS_TAB).trim_ows());
}

// ── parse_u64 ──────────────────────────────────────────────────────────────

#[divan::bench]
fn parse_u64_short() {
    black_box(black_box(fx::U64_SHORT).parse_u64());
}

#[divan::bench]
fn parse_u64_long() {
    black_box(black_box(fx::U64_LONG).parse_u64());
}

#[divan::bench]
fn parse_u64_with_ows() {
    black_box(black_box(fx::U64_WITH_OWS).parse_u64());
}

// ── contains_token_ignore_case ─────────────────────────────────────────────

#[divan::bench]
fn contains_token_present_short() {
    black_box(black_box(fx::TOKEN_LIST_SHORT).contains_token_ignore_case(b"chunked"));
}

#[divan::bench]
fn contains_token_present_long() {
    black_box(black_box(fx::TOKEN_LIST_LONG).contains_token_ignore_case(b"chunked"));
}

#[divan::bench]
fn contains_token_absent() {
    black_box(black_box(fx::TOKEN_ABSENT).contains_token_ignore_case(b"chunked"));
}
