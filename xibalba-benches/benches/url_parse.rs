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

// ── Comparisons: Url::parse vs url crate ──────────────────────────────────

mod compare {
    use divan::black_box;
    use xibalba_benches as fx;
    use xibalba_proto::url::Url;

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
}
