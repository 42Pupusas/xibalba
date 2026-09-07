#![no_main]

use libfuzzer_sys::fuzz_target;
use xibalba_fuzz::Surface;

fuzz_target!(|data: &[u8]| {
    Surface::Url.check(data);
});
