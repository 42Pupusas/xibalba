#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::hint::black_box;
use xibalba_benches as fx;
use xibalba_proto::header::Header;
use xibalba_proto::response::ResponseHead;

const ITERATIONS: usize = 2_000_000;

fn run_minimal() {
    let mut hdrs = [const { Header::empty() }; 32];
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    for _ in 0..ITERATIONS {
        ResponseHead::parse(black_box(fx::RESP_MINIMAL), &mut hdrs).unwrap();
    }
}

fn run_typical() {
    let mut hdrs = [const { Header::empty() }; 32];
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    for _ in 0..ITERATIONS {
        ResponseHead::parse(black_box(fx::RESP_TYPICAL), &mut hdrs).unwrap();
    }
}

fn run_heavy() {
    let mut hdrs = [const { Header::empty() }; 32];
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    for _ in 0..ITERATIONS {
        ResponseHead::parse(black_box(fx::RESP_HEAVY), &mut hdrs).unwrap();
    }
}

fn main() {
    let scenario = std::env::args().nth(1).unwrap_or_else(|| "typical".into());
    match scenario.as_str() {
        "minimal" => run_minimal(),
        "typical" => run_typical(),
        "heavy" => run_heavy(),
        other => {
            eprintln!("unknown scenario: {other}. use: minimal | typical | heavy");
            std::process::exit(1);
        }
    }
}
