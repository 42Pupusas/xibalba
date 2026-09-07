//! Replays a fixed corpus through the fuzz invariants on every `cargo test`.
//!
//! The libFuzzer targets under `fuzz/` need nightly and a time budget, so they
//! run on their own schedule. This runs the same invariants — from
//! `xibalba-fuzz`, so there is one definition of what the parsers promise —
//! against inputs chosen by hand plus a deterministic sweep.
//!
//! What this catches is a regression against shapes that already broke
//! something, and any *new* crash a fuzzer finds, once its input is added
//! below. What it cannot do is explore: that is the campaign's job. The two
//! are complementary, and both are cheap to keep.

use xibalba_fuzz::{Seeds, Surface};

/// A deterministic byte generator. Not a fuzzer — it explores nothing and
/// learns nothing — but it produces the malformed, truncated, and oddly
/// interleaved inputs that hand-written cases tend to miss, identically on
/// every machine and every run.
struct Prng {
    state: u64,
}

impl Prng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    const fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    /// A bounded index. Narrowing to `u32` first keeps the sequence identical
    /// on 32- and 64-bit targets, so a failure reproduces wherever it is run.
    const fn index(&mut self, modulus: usize) -> usize {
        (self.next_u64() & 0xffff_ffff) as usize % modulus
    }

    fn byte_from(&mut self, alphabet: &[u8]) -> u8 {
        alphabet[self.index(alphabet.len())]
    }

    /// Bytes drawn from a protocol-flavoured alphabet: structural characters
    /// appear far more often than they would at random, so the parser is
    /// pushed down real branches instead of failing at the first byte.
    fn protocol_noise(&mut self, len: usize) -> Vec<u8> {
        const ALPHABET: &[u8] = b"\r\n\t :;,0123456789abcdefHTTPX/.-_=%[]?&#\x00\x7f\xff";
        (0..len).map(|_| self.byte_from(ALPHABET)).collect()
    }

    /// A seed with some of its bytes replaced. Most corruptions of a valid
    /// message are still nearly valid, which is where the interesting
    /// branches are.
    fn mutate(&mut self, seed: &[u8]) -> Vec<u8> {
        let mut out = seed.to_vec();
        if out.is_empty() {
            return out;
        }
        let edits = 1 + self.index(4);
        for _ in 0..edits {
            // An earlier edit may have truncated the buffer to nothing, and an
            // empty input is already covered by its own case.
            if out.is_empty() {
                break;
            }
            let at = self.index(out.len());
            match self.next_u64() % 3 {
                0 => out[at] = self.byte_from(b"\r\n:; 0aF\x00\xff"),
                1 => out.truncate(at),
                _ => out.insert(at, self.byte_from(b"\r\n:; 0aF\x00\xff")),
            }
        }
        out
    }
}

/// The corpus each surface is checked against.
struct Corpus {
    surface: Surface,
    seeds: &'static [&'static [u8]],
}

impl Corpus {
    const ROUNDS: usize = 2_000;

    /// The seeds come from `xibalba-fuzz` rather than living here, so that a
    /// libFuzzer campaign starts from exactly the corpus this replays. An
    /// input added because a fuzzer found it would otherwise be checked by
    /// only one of the two.
    fn all() -> [Self; 3] {
        Surface::all().map(|surface| Self {
            surface,
            seeds: Seeds::for_surface(surface),
        })
    }

    /// Every seed, every truncation of every seed, and a sweep of mutated and
    /// synthetic inputs. Truncation is included wholesale because a parser fed
    /// by a socket sees every prefix of its input, and that is where the
    /// incomplete-versus-invalid distinction is decided.
    fn check(&self) {
        for seed in self.seeds {
            self.surface.check(seed);
            for end in 0..seed.len() {
                self.surface.check(&seed[..end]);
            }
        }

        let mut prng = Prng::new(0x5eed_1234_abcd_ef01);
        for round in 0..Self::ROUNDS {
            let seed = self.seeds[round % self.seeds.len()];
            self.surface.check(&prng.mutate(seed));

            let len = (round % 64) + 1;
            self.surface.check(&prng.protocol_noise(len));
        }
    }
}

#[test]
fn response_head_holds_its_invariants_across_the_corpus() {
    Corpus::all()[0].check();
}

#[test]
fn chunked_decoder_holds_its_invariants_across_the_corpus() {
    Corpus::all()[1].check();
}

#[test]
fn url_parsing_holds_its_invariants_across_the_corpus() {
    Corpus::all()[2].check();
}

#[test]
fn the_empty_input_is_handled_by_every_surface() {
    for surface in Surface::all() {
        surface.check(b"");
    }
}

/// The corpus is only worth what it exercises. A surface whose seeds all fail
/// at the first byte would pass every invariant while testing nothing, and the
/// failure would be silent.
#[test]
fn every_surface_has_seeds_that_parse() {
    for corpus in Corpus::all() {
        assert!(
            !corpus.seeds.is_empty(),
            "{} has no seeds",
            corpus.surface.name()
        );
    }
}

/// A surface with no libFuzzer target is explored by nobody, and a CI matrix
/// entry with no target fails the run. Both are silent until someone reads the
/// workflow, so the three lists are held together here.
#[test]
fn every_surface_has_a_fuzz_target_and_a_ci_job() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xibalba-proto sits one level below the workspace root")
        .to_path_buf();
    let workflow = std::fs::read_to_string(root.join(".github/workflows/ci.yml"))
        .unwrap_or_else(|e| panic!("read CI workflow: {e}"));

    for surface in Surface::all() {
        let target = root.join(format!("fuzz/fuzz_targets/{}.rs", surface.name()));
        assert!(
            target.exists(),
            "{} has no libFuzzer target at {}",
            surface.name(),
            target.display()
        );
        assert!(
            workflow.contains(surface.name()),
            "{} is not named in the CI fuzz matrix, so nothing ever runs it",
            surface.name()
        );
    }
}
