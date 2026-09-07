use crate::Surface;
use std::io;
use std::path::Path;

/// The starting inputs for each surface.
///
/// These live beside the invariants for the same reason the invariants live
/// together: the in-gate harness replays them on every `cargo test`, and a
/// libFuzzer campaign starts from them. A fuzzer given no seeds spends its
/// first minutes rediscovering that a response head begins with `HTTP/1.1`,
/// and coverage-guided search is only cheap once it is past that.
///
/// Each set covers the shapes where the parsers make a decision: valid,
/// truncated, ambiguous, and outright malformed.
pub struct Seeds;

impl Seeds {
    #[must_use]
    pub const fn for_surface(surface: Surface) -> &'static [&'static [u8]] {
        match surface {
            Surface::ResponseHead => &[
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
                b"HTTP/1.0 404 Not Found\r\n\r\n",
                b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 301 Moved\r\nLocation: http://a.example/b\r\n\r\n",
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
                b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
                b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n\r\n",
                b"HTTP/1.1 200 \r\n\r\n",
                b"HTTP/1.1 999 \xff\xfe\r\nX: y\r\n\r\n",
                b"HTTP/1.1 200 OK\r\nX-Empty:\r\nX-Tab:\tv\r\n\r\n",
            ],
            Surface::ChunkedBody => &[
                b"5\r\nhello\r\n0\r\n\r\n",
                b"0\r\n\r\n",
                b"3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n",
                b"5;ext=val\r\nhello\r\n0\r\n\r\n",
                b"a\r\n0123456789\r\n0\r\nTrailer: v\r\n\r\n",
                b"ffffffffffffffff\r\n",
                b"0000000000000005\r\nhello\r\n0\r\n\r\n",
                b"5\r\nhel",
                b"z\r\n",
                b"1\r\na\r\n1\r\nb\r\n1\r\nc\r\n0\r\n\r\n",
                // The shape the first campaign found: output already written,
                // then an error later in the same buffer.
                b"1\r\na\r\nz\r\n",
            ],
            Surface::Url => &[
                b"http://example.com/",
                b"https://example.com:8443/api?page=2#frag",
                b"http://[::1]:8080/path",
                b"http://[fe80::1%25eth0]/",
                b"https://example.com",
                b"http://example.com/a/../b/./c",
                b"http://example.com/?a=1&b=&c",
                b"http://user@example.com/",
                b"http://example.com:99999/",
                b"HtTpS://Example.COM/Path",
            ],
        }
    }

    /// Write this surface's seeds into `dir` as one file per input, named by
    /// content so that re-seeding an existing corpus overwrites rather than
    /// accumulating duplicates. Returns how many were written.
    ///
    /// # Errors
    ///
    /// If `dir` cannot be created or a seed cannot be written.
    pub fn write(surface: Surface, dir: &Path) -> io::Result<usize> {
        std::fs::create_dir_all(dir)?;
        let seeds = Self::for_surface(surface);
        for seed in seeds {
            std::fs::write(dir.join(Self::name(seed)), seed)?;
        }
        Ok(seeds.len())
    }

    fn name(seed: &[u8]) -> String {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in seed {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("seed-{hash:016x}")
    }
}
