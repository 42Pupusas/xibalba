//! Properties the protocol parsers must hold for *any* input.
//!
//! These are written once and driven from two places: [`xibalba-proto`'s
//! `fuzz.rs` test][gate], which replays a fixed corpus on every `cargo test`,
//! and the libFuzzer targets under `fuzz/`, which explore new inputs on
//! nightly. Keeping one set of invariants is the point — if the cheap gate
//! checked weaker properties than the deep campaign, a green gate would say
//! nothing about what the campaign is expected to find.
//!
//! Every check here is total: it takes arbitrary bytes and must not panic,
//! whatever they are. A failure is a real defect, so each carries the reason
//! it is a defect rather than a bare assert.
//!
//! [gate]: ../../xibalba-proto/tests/fuzz.rs

mod chunked;
mod head;
mod seeds;
mod url;

pub use chunked::ChunkedInvariants;
pub use head::HeadInvariants;
pub use seeds::Seeds;
pub use url::UrlInvariants;

/// The three parser surfaces reachable from untrusted bytes.
///
/// A response head and a chunked body arrive from the peer; a URL arrives from
/// whatever the caller was handed, which for a client following redirects is
/// also the peer. Nothing else in the crate reads unvalidated input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    ResponseHead,
    ChunkedBody,
    Url,
}

impl Surface {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ResponseHead => "response_head",
            Self::ChunkedBody => "chunked_body",
            Self::Url => "url",
        }
    }

    /// Run every invariant this surface has against `data`.
    pub fn check(self, data: &[u8]) {
        match self {
            Self::ResponseHead => HeadInvariants::new(data).check_all(),
            Self::ChunkedBody => ChunkedInvariants::new(data).check_all(),
            Self::Url => UrlInvariants::new(data).check_all(),
        }
    }

    #[must_use]
    pub const fn all() -> [Self; 3] {
        [Self::ResponseHead, Self::ChunkedBody, Self::Url]
    }
}
